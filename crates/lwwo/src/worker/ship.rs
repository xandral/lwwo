//! Sending task output to the master.
//!
//! Every attempt writes its output to a file on the worker's disk, the
//! spool. The shipper reads those files and sends them in chunks on the
//! `Logs` stream, one chunk per file at a time, each acknowledged with the
//! number of bytes the master has. If the master is away the files simply
//! grow, and shipping continues from the last acknowledged byte when it is
//! back: no line is lost, and none is sent to the master's file twice.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tracing::debug;

use super::Connect;
use crate::proto;

/// The most bytes in one chunk.
const CHUNK: usize = 64 * 1024;

/// One attempt: run, task and attempt number.
pub(crate) type Key = (String, String, u32);

enum Message {
    /// A new attempt writes to `path`.
    Track {
        key: Key,
        dag: String,
        path: PathBuf,
    },
    /// Some file grew.
    Grew,
    /// The attempt wrote its last line; `done` fires when the master has
    /// everything.
    Complete { key: Key, done: oneshot::Sender<()> },
}

/// A handle to the shipper task.
#[derive(Debug, Clone)]
pub(crate) struct Shipper(mpsc::UnboundedSender<Message>);

struct File {
    dag: String,
    path: PathBuf,
    /// Bytes the master has.
    acked: u64,
    /// A chunk is on its way and not yet acknowledged.
    waiting: bool,
    /// The chunk on its way is the last one.
    closing: bool,
    complete: Option<oneshot::Sender<()>>,
}

impl File {
    /// The next chunk to send, if there is something to send.
    fn next_chunk(&mut self, key: &Key) -> Option<proto::LogChunk> {
        if self.waiting {
            return None;
        }
        let mut data = Vec::new();
        let mut len = self.acked;
        if let Ok(mut file) = std::fs::File::open(&self.path) {
            len = file.metadata().map_or(self.acked, |meta| meta.len());
            if file.seek(SeekFrom::Start(self.acked)).is_ok() {
                let _ = file.take(CHUNK as u64).read_to_end(&mut data);
            }
        }
        let last = self.complete.is_some() && self.acked + data.len() as u64 >= len;
        if data.is_empty() && !last {
            return None;
        }
        self.waiting = true;
        self.closing = last;
        Some(proto::LogChunk {
            task: Some(proto::TaskRef {
                run_id: key.0.clone(),
                task: key.1.clone(),
                attempt: key.2,
            }),
            dag: self.dag.clone(),
            offset: self.acked,
            data,
            last,
        })
    }
}

impl Shipper {
    /// Starts the shipper task.
    pub(crate) fn start(connect: Connect) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(connect, rx));
        Self(tx)
    }

    /// Starts shipping the file an attempt writes to.
    pub(crate) fn track(&self, key: Key, dag: String, path: PathBuf) {
        let _ = self.0.send(Message::Track { key, dag, path });
    }

    /// Says that a tracked file grew.
    pub(crate) fn grew(&self) {
        let _ = self.0.send(Message::Grew);
    }

    /// Waits until the master has the whole output of an attempt.
    pub(crate) async fn finish(&self, key: Key) {
        let (done, finished) = oneshot::channel();
        if self.0.send(Message::Complete { key, done }).is_ok() {
            let _ = finished.await;
        }
    }
}

struct Stream {
    tx: mpsc::Sender<proto::LogChunk>,
    acks: Streaming<proto::LogAck>,
}

async fn open(connect: &Connect) -> Option<Stream> {
    let mut client = connect.client().await.ok()?;
    let (tx, rx) = mpsc::channel(16);
    let acks = client
        .logs(ReceiverStream::new(rx))
        .await
        .ok()?
        .into_inner();
    Some(Stream { tx, acks })
}

async fn run(connect: Connect, mut rx: mpsc::UnboundedReceiver<Message>) {
    let mut files: HashMap<Key, File> = HashMap::new();
    let mut stream: Option<Stream> = None;
    loop {
        if stream.is_none() && !files.is_empty() {
            stream = open(&connect).await;
            // Chunks sent on the old stream may never be acknowledged.
            for file in files.values_mut() {
                file.waiting = false;
            }
        }
        if let Some(open) = &stream {
            let mut broken = false;
            for (key, file) in &mut files {
                if let Some(chunk) = file.next_chunk(key) {
                    broken |= open.tx.send(chunk).await.is_err();
                }
            }
            if broken {
                stream = None;
            }
        }

        let idle = stream.is_none() && !files.is_empty();
        let ack = async {
            match &mut stream {
                Some(open) => open.acks.message().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            message = rx.recv() => match message {
                Some(Message::Track { key, dag, path }) => {
                    files.insert(key, File { dag, path, acked: 0, waiting: false, closing: false, complete: None });
                }
                Some(Message::Grew) => {}
                Some(Message::Complete { key, done }) => match files.get_mut(&key) {
                    Some(file) => file.complete = Some(done),
                    // Never tracked: nothing to wait for.
                    None => { let _ = done.send(()); }
                },
                None => return,
            },
            ack = ack => {
                let Ok(Some(ack)) = ack else {
                    debug!("log stream closed: reconnecting");
                    stream = None;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                };
                {
                    let Some(task) = ack.task else { continue };
                    let key = (task.run_id, task.task, task.attempt);
                    let Some(file) = files.get_mut(&key) else { continue };
                    let was_closing = file.closing;
                    file.acked = ack.received;
                    file.waiting = false;
                    file.closing = false;
                    let len = std::fs::metadata(&file.path).map_or(0, |meta| meta.len());
                    if was_closing && ack.received >= len {
                        // The master has it all and closed its file.
                        if let Some(file) = files.remove(&key) {
                            let _ = std::fs::remove_file(&file.path);
                            if let Some(done) = file.complete {
                                let _ = done.send(());
                            }
                        }
                    }
                }
            }
            // Without a stream, try again in a while.
            () = tokio::time::sleep(Duration::from_secs(2)), if idle => {}
        }
    }
}
