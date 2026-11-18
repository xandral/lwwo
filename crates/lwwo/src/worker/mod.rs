//! The worker: connects to the master, runs the tasks it is given and
//! reports how they went.
//!
//! One loop owns the state of the worker and reacts to three things:
//! messages from the master, events from the running tasks and the
//! heartbeat tick. The connection lives in a task of its own, so the
//! worker survives the master: tasks keep running while it is away, their
//! reports are kept until the master confirms them, and the connection is
//! tried again until it comes back.

mod cgroup;
mod journal;
pub(crate) mod shim;
mod ship;
mod spool;
mod wake;
mod watch;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tracing::{info, warn};

use self::ship::{Key, Shipper};
use self::wake::Wakeups;
use self::watch::{Shared, TaskEvent};
use crate::config::WorkerConfig;
use crate::proto::master_client::MasterClient;
use crate::proto::{self, master_message, worker_message};
use crate::security::{self, MASTER_NAME, Token};
use crate::util::now_ms;

/// How often the worker says it is alive, until the master says how often
/// it wants to hear.
const HEARTBEAT: Duration = Duration::from_secs(5);

/// How to reach the master.
#[derive(Debug, Clone)]
pub(crate) struct Connect {
    /// `host:port` of the master's gRPC service.
    address: String,
    /// Where `worker.key`, `worker.crt` and `ca.crt` are; `None` without
    /// TLS, in standalone.
    certs: Option<PathBuf>,
}

impl Connect {
    fn endpoint(&self, scheme: &str) -> Result<Endpoint> {
        Ok(
            Endpoint::from_shared(format!("{scheme}://{}", self.address))
                .with_context(|| format!("`{}` is not a valid master address", self.address))?
                .connect_timeout(Duration::from_secs(10))
                .http2_keep_alive_interval(Duration::from_secs(10))
                .keep_alive_while_idle(true),
        )
    }

    /// A client for the master. The certificate is read every time, so a
    /// renewed one is used from the next connection on.
    pub(crate) async fn client(&self) -> Result<MasterClient<Channel>> {
        let Some(certs) = &self.certs else {
            let channel = self.endpoint("http")?.connect().await?;
            return Ok(MasterClient::new(channel));
        };
        let read = |name: &str| {
            std::fs::read_to_string(certs.join(name))
                .with_context(|| format!("cannot read {}", certs.join(name).display()))
        };
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(read("ca.crt")?))
            // The master is recognised by its certificate, whatever its
            // address is.
            .domain_name(MASTER_NAME)
            .identity(Identity::from_pem(read("worker.crt")?, read("worker.key")?));
        let channel = self.endpoint("https")?.tls_config(tls)?.connect().await?;
        Ok(MasterClient::new(channel))
    }
}

fn save_certificate(dir: &Path, reply: &proto::JoinReply, key_pem: &str) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    security::write_private(&dir.join("worker.key"), key_pem)?;
    std::fs::write(dir.join("worker.crt"), &reply.certificate_pem)?;
    std::fs::write(dir.join("ca.crt"), &reply.ca_pem)?;
    Ok(())
}

/// Enters the master with the join token: gets the CA, checks it against
/// the fingerprint in the token, and only then sends the token to have a
/// certificate signed.
async fn join(address: &str, token: &str, name: &str, dir: &Path) -> Result<()> {
    let parsed = Token::parse(token)?;
    let ca_pem = security::fetch_ca(address, &parsed.fingerprint).await?;
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&ca_pem))
        .domain_name(MASTER_NAME);
    let channel = Endpoint::from_shared(format!("https://{address}"))?
        .connect_timeout(Duration::from_secs(10))
        .tls_config(tls)?
        .connect()
        .await
        .with_context(|| format!("cannot connect to the master at {address}"))?;
    let (csr_pem, key_pem) = security::worker_request(name)?;
    let reply = MasterClient::new(channel)
        .join(proto::JoinRequest {
            secret: token.trim().to_owned(),
            name: name.to_owned(),
            csr_pem,
        })
        .await
        .map_err(|status| anyhow::anyhow!("the master refused the join: {}", status.message()))?
        .into_inner();
    save_certificate(dir, &reply, &key_pem)?;
    info!("joined the master at {address} as {name}");
    Ok(())
}

/// Asks for a new certificate when the current one is past two thirds of
/// its life. The new one, with a new key, is used from the next
/// connection on.
async fn renew_when_due(connect: Connect, dir: PathBuf) {
    /// The longest sleep between two looks at the certificate.
    const LONGEST: Duration = Duration::from_secs(3_600);
    /// How long to wait after a renewal that failed.
    const AGAIN: Duration = Duration::from_secs(60);
    loop {
        let who = std::fs::read_to_string(dir.join("worker.crt"))
            .map_err(anyhow::Error::from)
            .and_then(|cert| security::identity_pem(&cert));
        let who = match who {
            Ok(who) => who,
            Err(error) => {
                warn!("cannot read the certificate: {error:#}");
                tokio::time::sleep(AGAIN).await;
                continue;
            }
        };
        let due = who.not_before + (who.not_after - who.not_before) / 3 * 2;
        let left = u64::try_from(due - now_ms()).unwrap_or(0);
        if left > 0 {
            tokio::time::sleep(Duration::from_millis(left).min(LONGEST)).await;
            continue;
        }
        let renewed: Result<()> = async {
            let (csr_pem, key_pem) = security::worker_request(&who.name)?;
            let reply = connect
                .client()
                .await?
                .renew(proto::RenewRequest { csr_pem })
                .await?
                .into_inner();
            save_certificate(&dir, &reply, &key_pem)
        }
        .await;
        match renewed {
            Ok(()) => info!("certificate renewed"),
            Err(error) => {
                warn!("cannot renew the certificate yet: {error:#}");
                tokio::time::sleep(AGAIN).await;
            }
        }
    }
}

/// What the worker's loop reacts to.
enum Input {
    /// A new session with the master: messages go out through here.
    Connected(mpsc::Sender<proto::WorkerMessage>),
    Disconnected,
    /// The master wants a heartbeat this often, and gives up a worker
    /// that is silent for this long.
    Heartbeat(Duration, Duration),
    /// Boxed: an `Assign` is much larger than the other inputs.
    Master(Box<master_message::Kind>),
}

/// Keeps a session open with the master, for ever.
async fn connection(connect: Connect, input: mpsc::Sender<Input>) {
    let mut wait = Duration::from_secs(1);
    loop {
        match connect.client().await {
            Ok(mut client) => {
                let (tx, rx) = mpsc::channel(256);
                // The loop sends Hello first: the master answers the call
                // only after reading it.
                if input.send(Input::Connected(tx)).await.is_err() {
                    return;
                }
                match client.session(ReceiverStream::new(rx)).await {
                    Ok(response) => {
                        info!("connected to the master");
                        wait = Duration::from_secs(1);
                        let header = |name: &str| {
                            response
                                .metadata()
                                .get(name)
                                .and_then(|value| value.to_str().ok()?.parse().ok())
                                .map(Duration::from_millis)
                        };
                        let interval = header(crate::master::grpc::HEARTBEAT_HEADER);
                        let timeout = header(crate::master::grpc::HEARTBEAT_TIMEOUT_HEADER);
                        if let (Some(interval), Some(timeout)) = (interval, timeout) {
                            let heartbeat = Input::Heartbeat(interval, timeout);
                            if input.send(heartbeat).await.is_err() {
                                return;
                            }
                        }
                        let mut inbound = response.into_inner();
                        while let Ok(Some(message)) = inbound.message().await {
                            if let Some(kind) = message.kind {
                                if input.send(Input::Master(Box::new(kind))).await.is_err() {
                                    return;
                                }
                            }
                        }
                        warn!("connection to the master lost: trying again");
                    }
                    Err(status) => warn!("the master refused the session: {}", status.message()),
                }
                if input.send(Input::Disconnected).await.is_err() {
                    return;
                }
            }
            Err(error) => warn!("cannot reach the master: {error:#}"),
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(15));
    }
}

struct Worker {
    name: String,
    shared: Arc<Shared>,
    /// Attempts in progress, each with the directory of its journal.
    running: HashMap<Key, PathBuf>,
    /// The journals of the attempts reported and not yet confirmed.
    reported: HashMap<Key, PathBuf>,
    /// Reports the master has not confirmed yet.
    unconfirmed: HashMap<Key, proto::Finished>,
    draining: bool,
    out: Option<mpsc::Sender<proto::WorkerMessage>>,
    events: mpsc::Sender<TaskEvent>,
}

fn task_ref(key: &Key) -> proto::TaskRef {
    proto::TaskRef {
        run_id: key.0.clone(),
        task: key.1.clone(),
        attempt: key.2,
    }
}

/// Total and available memory of the machine, in bytes; zeros where
/// `/proc/meminfo` does not exist.
fn memory() -> (u64, u64) {
    let Ok(info) = std::fs::read_to_string("/proc/meminfo") else {
        return (0, 0);
    };
    let field = |name: &str| {
        info.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| {
                rest.trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .map_or(0, |kilobytes| kilobytes * 1_024)
    };
    (field("MemTotal:"), field("MemAvailable:"))
}

/// Total and free bytes of the disk that holds `path`.
fn disk(path: &Path) -> (u64, u64) {
    rustix::fs::statvfs(path).map_or((0, 0), |stat| {
        (stat.f_blocks * stat.f_frsize, stat.f_bavail * stat.f_frsize)
    })
}

impl Worker {
    fn send(&mut self, kind: worker_message::Kind) {
        let Some(out) = &self.out else {
            return;
        };
        if out
            .try_send(proto::WorkerMessage { kind: Some(kind) })
            .is_err()
        {
            self.out = None;
        }
    }

    /// Running attempts, and finished ones the master may not know about.
    fn in_flight(&self) -> Vec<proto::TaskRef> {
        self.running
            .keys()
            .chain(self.unconfirmed.keys())
            .map(task_ref)
            .collect()
    }

    /// Whether the worker takes new tasks: not while it drains, and not
    /// when disk or memory are almost full.
    fn accepting(&self) -> (bool, u64, u64) {
        let config = &self.shared.config;
        let (disk_total, disk_free) = disk(&config.data_dir);
        let (memory_total, memory_free) = memory();
        let enough_disk = disk_total == 0 || disk_free >= config.min_free_disk.0.of(disk_total);
        let enough_memory =
            memory_total == 0 || memory_free >= config.min_free_memory.0.of(memory_total);
        (
            !self.draining && enough_disk && enough_memory,
            memory_free,
            disk_free,
        )
    }

    fn report(&mut self, key: &Key, finished: proto::Finished) {
        self.send(worker_message::Kind::Report(proto::Report {
            task: Some(task_ref(key)),
            kind: Some(proto::report::Kind::Finished(finished)),
        }));
    }

    fn connected(&mut self, out: mpsc::Sender<proto::WorkerMessage>) {
        self.out = Some(out);
        self.send(worker_message::Kind::Hello(proto::Hello {
            worker_id: self.name.clone(),
            slots: self.shared.config.slots(),
            labels: self.shared.config.labels.clone(),
            total_memory: memory().0,
            in_flight: self.in_flight(),
            protocol: proto::PROTOCOL,
        }));
        // What finished while the master was away, or was never confirmed.
        for (key, finished) in self.unconfirmed.clone() {
            self.report(&key, finished);
        }
    }

    fn assign(&mut self, assign: &proto::Assign) {
        let Some(task) = assign.task.clone() else {
            return;
        };
        let key: Key = (task.run_id.clone(), task.task.clone(), task.attempt);
        if self.running.contains_key(&key) || self.unconfirmed.contains_key(&key) {
            // The same assignment twice: it is already taken care of.
            return;
        }
        // Written down first, started second: see `journal`.
        let launched = watch::launch(&self.shared, assign, &key);
        self.send(worker_message::Kind::Ack(proto::Ack {
            task: Some(task),
            accepted: launched.is_ok(),
            reason: launched
                .as_ref()
                .err()
                .map(|error| format!("{error:#}"))
                .unwrap_or_default(),
        }));
        let Ok(dir) = launched else {
            // The master counts a refusal as a failed attempt.
            journal::forget(
                &self.shared.attempts(),
                &journal::attempt_dir(&self.shared.attempts(), &key),
            );
            return;
        };
        if let Some(spec) = journal::read::<journal::Spec>(&dir.join("spec.json")) {
            self.follow(spec, dir);
        }
    }

    /// Starts following an attempt, new or found on disk.
    fn follow(&mut self, spec: journal::Spec, dir: PathBuf) {
        self.running.insert(spec.key(), dir.clone());
        tokio::spawn(watch::follow(
            Arc::clone(&self.shared),
            spec,
            dir,
            self.events.clone(),
        ));
    }

    fn master(&mut self, kind: master_message::Kind) {
        match kind {
            master_message::Kind::Assign(assign) => self.assign(&assign),
            master_message::Kind::Cancel(cancel) => {
                let Some(task) = cancel.task else { return };
                for (key, dir) in &self.running {
                    let same_attempt = task.attempt == 0 || task.attempt == key.2;
                    if key.0 == task.run_id && key.1 == task.task && same_attempt {
                        watch::cancel(dir);
                    }
                }
            }
            master_message::Kind::Drain(_) => {
                info!("draining: finishing what is running, taking nothing more");
                self.draining = true;
            }
            master_message::Kind::Confirm(confirm) => {
                if let Some(task) = confirm.task {
                    let key = (task.run_id, task.task, task.attempt);
                    self.unconfirmed.remove(&key);
                    // The master has it: the journal of the attempt has
                    // nothing left to remember.
                    if let Some(dir) = self.reported.remove(&key) {
                        journal::forget(&self.shared.attempts(), &dir);
                    }
                }
            }
        }
    }

    fn task(&mut self, event: TaskEvent) {
        match event {
            TaskEvent::Started { key, at } => {
                self.send(worker_message::Kind::Report(proto::Report {
                    task: Some(task_ref(&key)),
                    kind: Some(proto::report::Kind::Started(proto::Started {
                        started_at: at,
                    })),
                }));
            }
            TaskEvent::Finished { key, finished } => {
                if let Some(dir) = self.running.remove(&key) {
                    self.reported.insert(key.clone(), dir);
                }
                // Kept until the master says it saved it.
                self.unconfirmed.insert(key.clone(), finished.clone());
                self.report(&key, finished);
            }
        }
    }

    fn heartbeat(&mut self) {
        self.shared.touch_alive();
        let (accepting, available_memory, free_disk) = self.accepting();
        self.send(worker_message::Kind::Heartbeat(proto::Heartbeat {
            in_flight: self.in_flight(),
            available_memory,
            free_disk,
            accepting,
        }));
    }
}

/// Runs a worker until the process ends. `secure` is false only in
/// standalone, where the master is this same process.
pub(crate) async fn run(config: WorkerConfig, address: String, secure: bool) -> Result<()> {
    crate::util::private_dir(&config.data_dir)?;
    // Held until the process ends: the journal has one reader.
    let _lock = crate::util::lock_dir(&config.data_dir, "worker")?;
    let mut name = config.name();
    let certs = if secure {
        let dir = config.data_dir.join("certs");
        if !dir.join("worker.crt").exists() {
            let Some(token) = config.join_token()? else {
                bail!(
                    "this worker has no certificate yet: give it the join token of the master \
                     (`lwwo token worker`) in a file with --token-file, or in LWWO_WORKER_TOKEN"
                );
            };
            join(&address, &token, &name, &dir).await?;
        }
        // The name of a worker is the one in its certificate.
        let cert = std::fs::read_to_string(dir.join("worker.crt"))?;
        name = security::identity_pem(&cert)?.name;
        Some(dir)
    } else {
        None
    };
    let connect = Connect { address, certs };
    if let Some(dir) = &connect.certs {
        tokio::spawn(renew_when_due(connect.clone(), dir.clone()));
    }

    let (input_tx, mut input) = mpsc::channel(1_024);
    let (events_tx, mut events) = mpsc::channel(1_024);
    let shared = Arc::new(Shared {
        shipper: Shipper::start(connect.clone()),
        cgroup_base: cgroup::prepare(config.cgroups),
        // Until the master says otherwise, its default.
        orphan_timeout_ms: AtomicU64::new(30_000),
        wakeups: Wakeups::start(),
        config,
    });
    // Before anything else: the shims of a previous worker are watching
    // this file to know whether to go on.
    shared.touch_alive();
    info!("worker {name}: {} slots", shared.config.slots());
    let mut worker = Worker {
        name,
        shared,
        running: HashMap::new(),
        reported: HashMap::new(),
        unconfirmed: HashMap::new(),
        draining: false,
        out: None,
        events: events_tx,
    };
    // What a previous worker left: tasks still running under their shims,
    // and outcomes the master has not confirmed. They are in the `Hello`,
    // so the master does not give them to anyone else.
    let found = watch::recover(&worker.shared);
    if !found.is_empty() {
        info!(
            "found {} attempts of a previous worker: picking them up",
            found.len()
        );
    }
    for (spec, dir) in found {
        worker.follow(spec, dir);
    }
    tokio::spawn(connection(connect, input_tx));

    let mut tick = tokio::time::interval(HEARTBEAT);
    loop {
        tokio::select! {
            message = input.recv() => match message {
                Some(Input::Connected(out)) => worker.connected(out),
                Some(Input::Disconnected) => worker.out = None,
                Some(Input::Heartbeat(interval, timeout)) => {
                    if interval != tick.period() && !interval.is_zero() {
                        tick = tokio::time::interval(interval);
                    }
                    let timeout = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
                    worker.shared.orphan_timeout_ms.store(timeout, Ordering::Relaxed);
                }
                Some(Input::Master(kind)) => worker.master(*kind),
                None => return Ok(()),
            },
            Some(event) = events.recv() => worker.task(event),
            _ = tick.tick() => worker.heartbeat(),
        }
    }
}
