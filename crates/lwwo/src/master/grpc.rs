//! The gRPC service the workers talk to.
//!
//! It checks who is on the other side and passes the messages on: every
//! decision is taken by the master's loop. Only the logs skip the loop and
//! go straight to disk, since task output never passes through the master's
//! state.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::{Stream, StreamExt as _};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use super::logs::{LogKey, LogStore};
use super::state::{ApiError, Call, Msg, call};
use crate::proto;
use crate::security;

/// Messages for one worker that may wait in memory before the worker is
/// considered hopelessly behind.
const OUTBOX: usize = 1_024;

type Outbound<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The service behind `[workers] listen`.
#[derive(Debug)]
pub(crate) struct Service {
    pub(crate) tx: mpsc::Sender<Msg>,
    pub(crate) logs: Arc<LogStore>,
    /// The valid certificate of each worker, by name. `None` without TLS,
    /// in standalone, where the worker is this same process.
    pub(crate) serials: Option<Arc<RwLock<HashMap<String, String>>>>,
    pub(crate) sessions: AtomicU64,
    /// The database file: a worker may only write the logs of attempts
    /// that were assigned to it, and the database knows which those are.
    pub(crate) db: std::path::PathBuf,
    /// How often workers must say they are alive, from `[workers]
    /// heartbeat_interval`: they are told when their session opens.
    pub(crate) heartbeat_interval: Duration,
    /// After how long a silent worker is given up: its tasks are then
    /// assigned again, so a task whose worker stays away longer must stop.
    pub(crate) heartbeat_timeout: Duration,
}

/// The header of the session's answer that carries the heartbeat
/// interval, in milliseconds.
pub(crate) const HEARTBEAT_HEADER: &str = "lwwo-heartbeat-ms";

/// The header that carries the heartbeat timeout, in milliseconds.
pub(crate) const HEARTBEAT_TIMEOUT_HEADER: &str = "lwwo-heartbeat-timeout-ms";

fn status(error: ApiError) -> Status {
    match error.status {
        400 => Status::invalid_argument(error.message),
        401 => Status::unauthenticated(error.message),
        404 => Status::not_found(error.message),
        409 => Status::failed_precondition(error.message),
        503 => Status::unavailable(error.message),
        _ => Status::internal(error.message),
    }
}

fn reply(value: &serde_json::Value) -> proto::JoinReply {
    let text = |field: &str| value[field].as_str().unwrap_or_default().to_owned();
    proto::JoinReply {
        certificate_pem: text("certificate_pem"),
        ca_pem: text("ca_pem"),
    }
}

impl Service {
    /// The name in the certificate of whoever makes the request, checked
    /// against the certificates the master considers valid. `None` when
    /// the master runs without TLS.
    fn peer<T>(&self, request: &Request<T>) -> Result<Option<String>, Status> {
        let Some(serials) = &self.serials else {
            return Ok(None);
        };
        let certs = request
            .peer_certs()
            .filter(|certs| !certs.is_empty())
            .ok_or_else(|| Status::unauthenticated("a worker certificate is required"))?;
        let who = security::identity(&certs[0])
            .map_err(|error| Status::unauthenticated(format!("{error:#}")))?;
        let serials = serials.read().expect("the serials lock is never poisoned");
        if serials.get(&who.name) != Some(&who.serial) {
            return Err(Status::permission_denied(format!(
                "the certificate of worker `{}` was revoked or replaced",
                who.name
            )));
        }
        Ok(Some(who.name))
    }
}

#[tonic::async_trait]
impl proto::master_server::Master for Service {
    type SessionStream = Outbound<proto::MasterMessage>;
    type LogsStream = Outbound<proto::LogAck>;

    async fn join(
        &self,
        request: Request<proto::JoinRequest>,
    ) -> Result<Response<proto::JoinReply>, Status> {
        let request = request.into_inner();
        let value = call(
            &self.tx,
            Call::Join {
                token: request.secret,
                name: request.name,
                csr_pem: request.csr_pem,
            },
        )
        .await
        .map_err(status)?;
        Ok(Response::new(reply(&value)))
    }

    async fn renew(
        &self,
        request: Request<proto::RenewRequest>,
    ) -> Result<Response<proto::JoinReply>, Status> {
        let name = self
            .peer(&request)?
            .ok_or_else(|| Status::failed_precondition("this master has no certificates"))?;
        let csr_pem = request.into_inner().csr_pem;
        let value = call(&self.tx, Call::Renew { name, csr_pem })
            .await
            .map_err(status)?;
        Ok(Response::new(reply(&value)))
    }

    async fn session(
        &self,
        request: Request<Streaming<proto::WorkerMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let peer = self.peer(&request)?;
        let mut inbound = request.into_inner();
        let Some(proto::worker_message::Kind::Hello(hello)) =
            inbound.message().await?.and_then(|message| message.kind)
        else {
            return Err(Status::invalid_argument("the first message must be Hello"));
        };
        // The name of a worker is the one in its certificate.
        if peer.as_ref().is_some_and(|name| *name != hello.worker_id) {
            return Err(Status::permission_denied(format!(
                "the certificate is not of worker `{}`",
                hello.worker_id
            )));
        }
        if hello.protocol != proto::PROTOCOL {
            return Err(Status::failed_precondition(format!(
                "this worker speaks version {} of the lwwo protocol and the master version {}: \
                 run the same lwwo release on both",
                hello.protocol,
                proto::PROTOCOL
            )));
        }
        let name = hello.worker_id.clone();
        let session = self.sessions.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(OUTBOX);
        let unavailable = |_| Status::unavailable("the master is shutting down");
        self.tx
            .send(Msg::WorkerHello {
                name: name.clone(),
                session,
                hello,
                tx,
            })
            .await
            .map_err(unavailable)?;

        let to_master = self.tx.clone();
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(proto::WorkerMessage { kind: Some(kind) })) => {
                        let message = Msg::WorkerMessage {
                            name: name.clone(),
                            session,
                            kind,
                        };
                        if to_master.send(message).await.is_err() {
                            return;
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(error) => {
                        warn!("stream of worker {name} broke: {}", error.message());
                        break;
                    }
                }
            }
            let _ = to_master.send(Msg::WorkerGone { name, session }).await;
        });
        // When the master drops its end, the stream ends and the worker
        // reconnects.
        let mut response: Response<Self::SessionStream> =
            Response::new(Box::pin(ReceiverStream::new(rx).map(Ok)));
        let millis = |duration: Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let headers = response.metadata_mut();
        headers.insert(HEARTBEAT_HEADER, millis(self.heartbeat_interval).into());
        headers.insert(
            HEARTBEAT_TIMEOUT_HEADER,
            millis(self.heartbeat_timeout).into(),
        );
        Ok(response)
    }

    async fn logs(
        &self,
        request: Request<Streaming<proto::LogChunk>>,
    ) -> Result<Response<Self::LogsStream>, Status> {
        let worker = self.peer(&request)?;
        // Without certificates the only worker is this same process.
        let database = match &worker {
            Some(_) => Some(crate::store::open_read_only(&self.db).map_err(|error| {
                Status::internal(format!("cannot check the owner of the logs: {error:#}"))
            })?),
            None => None,
        };
        let mut inbound = request.into_inner();
        let logs = Arc::clone(&self.logs);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Ok(Some(chunk)) = inbound.message().await {
                let Some(task) = chunk.task else { continue };
                // The DAG in the path is the one the database says, and
                // the attempt must be one of this worker's.
                let dag = match (&worker, &database) {
                    (Some(worker), Some(database)) => crate::store::attempt_dag(
                        database,
                        &task.run_id,
                        &task.task,
                        task.attempt,
                        worker,
                    ),
                    _ => Ok(Some(chunk.dag)),
                };
                let ack = match dag {
                    Ok(Some(dag)) => {
                        let key = LogKey {
                            dag,
                            run: task.run_id.clone(),
                            task: task.task.clone(),
                            attempt: task.attempt,
                        };
                        // Names end up in paths: nothing may climb out of
                        // the log directory.
                        let safe = [&key.dag, &key.run, &key.task].iter().all(|part| {
                            !part.is_empty() && !part.contains(['/', '\\']) && *part != ".."
                        });
                        if safe {
                            logs.append(&key, chunk.offset, &chunk.data, chunk.last)
                                .map(|received| proto::LogAck {
                                    task: Some(task),
                                    received,
                                })
                                .map_err(|error| {
                                    Status::internal(format!("cannot write the log: {error}"))
                                })
                        } else {
                            Err(Status::invalid_argument("invalid names in the log chunk"))
                        }
                    }
                    // Not an attempt of this worker: nothing is written,
                    // and the worker is told to go on, so that it does not
                    // send the same chunk forever.
                    Ok(None) => {
                        warn!(
                            "dropped a log chunk for {}/{}/{}: not an attempt of this worker",
                            task.run_id, task.task, task.attempt
                        );
                        Ok(proto::LogAck {
                            received: chunk.offset + chunk.data.len() as u64,
                            task: Some(task),
                        })
                    }
                    Err(error) => Err(Status::internal(format!(
                        "cannot check the owner of the log: {error:#}"
                    ))),
                };
                let failed = ack.is_err();
                if tx.send(ack).await.is_err() || failed {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
