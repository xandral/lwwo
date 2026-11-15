//! The master: one event loop that owns all the state, the API in front
//! of it for people and the gRPC service for workers.

mod alerts;
mod api;
pub(crate) mod grpc;
mod logs;
mod state;
mod timer;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{error, info, warn};

use self::logs::LogStore;
use self::state::{Call, Master, Msg, call};
use self::timer::{Alarm, Ring};
use crate::config::{Auth, MasterConfig};
use crate::proto::master_server::MasterServer;
use crate::security::Ca;

/// Requests that may wait for the loop before senders are slowed down.
const INBOX: usize = 4_096;

/// A master that is running.
#[derive(Debug)]
pub(crate) struct Running {
    /// Where workers connect.
    pub(crate) workers: SocketAddr,
    /// Ends when the master stops, with the error that stopped it.
    pub(crate) done: JoinHandle<Result<()>>,
}

/// Starts a master. With `secure`, workers and API use the internal CA;
/// without, as in standalone, everything is plain and local.
#[expect(
    clippy::too_many_lines,
    reason = "the start of the master in the order it happens, top to bottom"
)]
pub(crate) async fn start(config: MasterConfig, secure: bool) -> Result<Running> {
    let data_dir = config.storage.data_dir.clone();
    crate::util::private_dir(&data_dir)?;
    let lock = crate::util::lock_dir(&data_dir, "master")?;
    let limit = config.logs.max_per_attempt.0.of(u64::MAX);
    let logs = Arc::new(LogStore::new(config.logs_dir(), limit));
    let ca = if secure {
        Some(Arc::new(Ca::load_or_create(&data_dir.join("ca"))?))
    } else {
        None
    };
    let serials = Arc::new(RwLock::new(HashMap::new()));
    let (changes, _) = broadcast::channel(16);
    let (tx, mut rx) = mpsc::channel(INBOX);

    let mut master = Master::new(
        config.clone(),
        Arc::clone(&logs),
        ca.clone(),
        Arc::clone(&serials),
        changes.clone(),
    )?;
    if let Some(path) = master.bootstrap_tokens()? {
        info!(
            "first start: an admin API token was written to {}; use it as LWWO_TOKEN",
            path.display()
        );
    }

    // The gRPC service for the workers.
    let listener = TcpListener::bind(config.workers.listen)
        .await
        .with_context(|| format!("cannot listen for workers on {}", config.workers.listen))?;
    let workers = listener.local_addr()?;
    let service = grpc::Service {
        tx: tx.clone(),
        logs: Arc::clone(&logs),
        serials: secure.then(|| Arc::clone(&serials)),
        sessions: AtomicU64::new(1),
        db: config.db_path(),
        heartbeat_interval: config.workers.heartbeat_interval.0,
        heartbeat_timeout: config.workers.heartbeat_timeout.0,
    };
    let mut server = Server::builder().http2_keepalive_interval(Some(Duration::from_secs(10)));
    // The certificate of the master, for workers and for the API.
    let mut master_identity = None;
    if let Some(ca) = &ca {
        let (cert, key) = ca.issue_master()?;
        // The CA goes along with the certificate: whoever has a token
        // recognises it by its fingerprint.
        let chain = format!("{cert}{}", ca.cert_pem());
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(&chain, &key))
            .client_ca_root(Certificate::from_pem(ca.cert_pem()))
            // `Join` comes without a certificate: that is what it asks for.
            .client_auth_optional(true);
        server = server.tls_config(tls)?;
        master_identity = Some((chain, key));
    }
    let grpc = server
        .add_service(MasterServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener));
    tokio::spawn(async move {
        if let Err(error) = grpc.await {
            error!("the gRPC server stopped: {error}");
        }
    });

    // The HTTP API.
    let auth = config.api.auth == Auth::Token;
    let app = api::router(api::Api {
        tx: tx.clone(),
        logs,
        changes,
        auth,
        db: config.db_path(),
        backup_dir: config.backup_dir(),
    });
    let listener = std::net::TcpListener::bind(config.api.listen)
        .with_context(|| format!("cannot listen for the API on {}", config.api.listen))?;
    listener.set_nonblocking(true)?;
    let api = listener.local_addr()?;
    let own_cert = match (&config.api.tls_cert, &config.api.tls_key) {
        (Some(cert), Some(key)) => Some((
            std::fs::read_to_string(cert)
                .with_context(|| format!("cannot read {}", cert.display()))?,
            std::fs::read_to_string(key)
                .with_context(|| format!("cannot read {}", key.display()))?,
        )),
        _ => None,
    };
    // With tokens the API is always behind TLS: a token in the clear is
    // a token for everybody.
    if let Some((cert, key)) = own_cert.or(master_identity.filter(|_| auth)) {
        let tls =
            axum_server::tls_rustls::RustlsConfig::from_pem(cert.into_bytes(), key.into_bytes())
                .await
                .context("invalid certificate or key for the API")?;
        let server = axum_server::from_tcp_rustls(listener, tls)?;
        tokio::spawn(async move {
            if let Err(error) = server.serve(app.into_make_service()).await {
                error!("the API server stopped: {error}");
            }
        });
    } else {
        let listener = TcpListener::from_std(listener)?;
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                error!("the API server stopped: {error}");
            }
        });
    }

    if let Some(dir) = config.dags.watch.clone() {
        tokio::spawn(watch(dir, tx.clone()));
    }

    // The loop: the only place where the state changes.
    let done = tokio::spawn(async move {
        // Held for as long as the loop runs; the kernel lets go of it when
        // the process ends, however it ends.
        let _lock = lock;
        let mut alarm = Alarm::new().context("cannot create the timer")?;
        loop {
            alarm.arm(master.next_wake())?;
            tokio::select! {
                ring = alarm.wait() => {
                    if ring? == Ring::ClockChanged {
                        info!("the system clock was moved: checking what is due");
                    }
                    master.on_alarm()?;
                }
                message = rx.recv() => match message {
                    Some(message) => master.on_message(message)?,
                    None => return Ok(()),
                },
            }
        }
    });
    info!("master ready: API on {api}, workers on {workers}");
    Ok(Running { workers, done })
}

/// Applies every YAML file of `dir` when it appears or changes. Handy in
/// development: in production DAGs come from `lwwo apply`.
async fn watch(dir: PathBuf, tx: mpsc::Sender<Msg>) {
    let mut seen: HashMap<PathBuf, SystemTime> = HashMap::new();
    info!("watching {} for DAGs", dir.display());
    loop {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            warn!("cannot read the watched directory {}", dir.display());
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_yaml = path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml");
            let Some(modified) = entry.metadata().ok().and_then(|meta| meta.modified().ok()) else {
                continue;
            };
            if !is_yaml || seen.get(&path) == Some(&modified) {
                continue;
            }
            seen.insert(path.clone(), modified);
            let Ok(yaml) = std::fs::read_to_string(&path) else {
                continue;
            };
            match call(&tx, Call::Apply { yaml }).await {
                Ok(applied) => info!("{}: applied {}", path.display(), applied["name"]),
                Err(error) => warn!("{}: {}", path.display(), error.message),
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
