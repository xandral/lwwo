//! Configuration of master and worker: a TOML file, overridden by
//! environment variables, overridden by flags.
//!
//! Every field has one name in three spellings: `[storage] db` in the file,
//! `LWWO_MASTER_STORAGE_DB` in the environment and `--storage-db` on the
//! command line. The three are merged into one TOML table, which is then
//! read into the typed structs, so a field is declared once.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer};
use toml::{Table, Value};

use crate::util::{Size, parse_duration, parse_size};

/// A duration written as `30s`, `5m`, `1h` or `30d`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Dur(pub(crate) Duration);

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_duration(&text)
            .map(Dur)
            .map_err(serde::de::Error::custom)
    }
}

/// A size written as `100MB`, `20GB` or `10%`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Amount(pub(crate) Size);

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_size(&text)
            .map(Amount)
            .map_err(serde::de::Error::custom)
    }
}

/// How the API checks who is calling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Auth {
    /// A bearer token, over TLS.
    Token,
    /// Nobody is checked: only allowed on a local address.
    None,
}

/// `[storage] durability`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Durability {
    /// `synchronous=NORMAL`: a power cut can lose the last transactions.
    Normal,
    /// `synchronous=FULL`: nothing is lost, writes are slower.
    Full,
}

/// `overlap`, in a DAG or in `[defaults]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OverlapKind {
    /// The new run waits for the active one.
    Queue,
    /// The fire is skipped.
    Skip,
    /// Runs overlap, up to `max_active`.
    Allow,
    /// The active run is cancelled.
    Replace,
}

/// `retry_backoff`, in a task or in `[defaults]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum BackoffKind {
    /// The wait doubles at every attempt.
    Exponential,
    /// The wait is always `retry_delay`.
    Fixed,
}

/// `cgroups` of a worker: whether each task gets a cgroup of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CgroupMode {
    /// Only when the worker's cgroup holds lwwo alone, as under a systemd
    /// service or in a container: then nothing else is moved.
    Auto,
    /// Always: whatever shares the worker's cgroup is moved below it.
    On,
    /// Never: tasks are stopped through their process group only.
    Off,
}

/// `[api]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ApiConfig {
    pub(crate) listen: SocketAddr,
    pub(crate) auth: Auth,
    pub(crate) tls_cert: Option<PathBuf>,
    pub(crate) tls_key: Option<PathBuf>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7070)),
            auth: Auth::Token,
            tls_cert: None,
            tls_key: None,
        }
    }
}

/// `[workers]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WorkersConfig {
    pub(crate) listen: SocketAddr,
    pub(crate) heartbeat_interval: Dur,
    pub(crate) heartbeat_timeout: Dur,
    /// How long a worker's certificate lasts.
    pub(crate) cert_lifetime: Dur,
}

impl Default for WorkersConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 7071)),
            heartbeat_interval: Dur(Duration::from_secs(5)),
            heartbeat_timeout: Dur(Duration::from_secs(30)),
            cert_lifetime: Dur(Duration::from_secs(90 * 86_400)),
        }
    }
}

/// `[storage]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct StorageConfig {
    pub(crate) data_dir: PathBuf,
    pub(crate) db: Option<PathBuf>,
    pub(crate) durability: Durability,
    pub(crate) retention: Dur,
    pub(crate) backup_dir: Option<PathBuf>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/lwwo"),
            db: None,
            durability: Durability::Normal,
            retention: Dur(Duration::from_secs(30 * 86_400)),
            backup_dir: None,
        }
    }
}

/// `[logs]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct LogsConfig {
    pub(crate) dir: Option<PathBuf>,
    pub(crate) max_per_attempt: Amount,
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            dir: None,
            max_per_attempt: Amount(Size::Bytes(100 << 20)),
        }
    }
}

/// `[dags]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct DagsConfig {
    pub(crate) default_timezone: String,
    pub(crate) watch: Option<PathBuf>,
}

impl Default for DagsConfig {
    fn default() -> Self {
        Self {
            default_timezone: "UTC".to_owned(),
            watch: None,
        }
    }
}

/// `[defaults]`: what DAGs and tasks get when they do not say.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Defaults {
    pub(crate) catchup: bool,
    pub(crate) overlap: OverlapKind,
    pub(crate) retries: u32,
    pub(crate) retry_delay: Dur,
    pub(crate) retry_backoff: BackoffKind,
    pub(crate) max_retry_delay: Option<Dur>,
    pub(crate) timeout: Option<Dur>,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            catchup: false,
            overlap: OverlapKind::Queue,
            retries: 0,
            retry_delay: Dur(Duration::from_secs(30)),
            retry_backoff: BackoffKind::Exponential,
            max_retry_delay: None,
            timeout: None,
        }
    }
}

/// `[alerts]`: off unless a webhook is given.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AlertsConfig {
    /// Where alerts are posted, as JSON. Without it there are no alerts.
    pub(crate) webhook: Option<String>,
    /// A task ready to run that has found no worker for this long is
    /// reported. Without it, waiting tasks are not reported.
    pub(crate) waiting_after: Option<Dur>,
    /// Report every run that ends as failed.
    pub(crate) on_failure: bool,
}

/// `master.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct MasterConfig {
    pub(crate) api: ApiConfig,
    pub(crate) workers: WorkersConfig,
    pub(crate) storage: StorageConfig,
    pub(crate) logs: LogsConfig,
    pub(crate) dags: DagsConfig,
    pub(crate) defaults: Defaults,
    pub(crate) alerts: AlertsConfig,
}

impl MasterConfig {
    /// The SQLite file.
    pub(crate) fn db_path(&self) -> PathBuf {
        self.storage
            .db
            .clone()
            .unwrap_or_else(|| self.storage.data_dir.join("lwwo.db"))
    }

    /// Where `lwwo backup` writes its copies, and nowhere else.
    pub(crate) fn backup_dir(&self) -> PathBuf {
        self.storage
            .backup_dir
            .clone()
            .unwrap_or_else(|| self.storage.data_dir.join("backups"))
    }

    /// The directory of the task logs.
    pub(crate) fn logs_dir(&self) -> PathBuf {
        self.logs
            .dir
            .clone()
            .unwrap_or_else(|| self.storage.data_dir.join("logs"))
    }
}

/// `worker.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WorkerConfig {
    /// The gRPC address of the master. Required, except in standalone.
    pub(crate) master: Option<String>,
    /// The join token itself: in the file or in `LWWO_WORKER_TOKEN`, never
    /// as a flag.
    pub(crate) token: Option<String>,
    /// A file holding the join token.
    pub(crate) token_file: Option<PathBuf>,
    pub(crate) name: Option<String>,
    pub(crate) data_dir: PathBuf,
    pub(crate) slots: Option<u32>,
    pub(crate) labels: Vec<String>,
    pub(crate) executors: Vec<String>,
    pub(crate) workdir: Option<PathBuf>,
    pub(crate) cache_dir: Option<PathBuf>,
    pub(crate) cache_max: Amount,
    pub(crate) min_free_disk: Amount,
    pub(crate) min_free_memory: Amount,
    pub(crate) kill_grace: Dur,
    pub(crate) log_spool_max: Amount,
    pub(crate) cgroups: CgroupMode,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            master: None,
            token: None,
            token_file: None,
            name: None,
            data_dir: PathBuf::from("/var/lib/lwwo-worker"),
            slots: None,
            labels: Vec::new(),
            executors: vec!["process".to_owned()],
            workdir: None,
            cache_dir: None,
            cache_max: Amount(Size::Bytes(20 << 30)),
            min_free_disk: Amount(Size::Percent(10)),
            min_free_memory: Amount(Size::Percent(10)),
            kill_grace: Dur(Duration::from_secs(30)),
            log_spool_max: Amount(Size::Bytes(1 << 30)),
            cgroups: CgroupMode::Auto,
        }
    }
}

impl WorkerConfig {
    /// The name of the worker: the configured one, or the machine's.
    pub(crate) fn name(&self) -> String {
        self.name.clone().unwrap_or_else(|| {
            rustix::system::uname()
                .nodename()
                .to_string_lossy()
                .into_owned()
        })
    }

    /// The join token, from `token` or from `token_file`.
    pub(crate) fn join_token(&self) -> Result<Option<String>> {
        if let Some(token) = &self.token {
            return Ok(Some(token.trim().to_owned()));
        }
        let Some(path) = &self.token_file else {
            return Ok(None);
        };
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read the join token from {}", path.display()))?;
        Ok(Some(text.trim().to_owned()))
    }

    /// Slots offered: the configured number, or one per CPU.
    pub(crate) fn slots(&self) -> u32 {
        self.slots.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map_or(1, |n| u32::try_from(n.get()).unwrap_or(u32::MAX))
        })
    }

    /// Where the private directories of the attempts live.
    pub(crate) fn workdir(&self) -> PathBuf {
        self.workdir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("work"))
    }

    /// Where git repositories are cached.
    pub(crate) fn cache_dir(&self) -> PathBuf {
        self.cache_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("cache"))
    }
}

/// What kind of TOML value a field takes, to convert the text of an
/// environment variable or a flag.
#[derive(Debug, Clone, Copy)]
enum Kind {
    Text,
    Integer,
    Boolean,
    List,
}

/// Every field of the master, as section, field and kind.
const MASTER_FIELDS: &[(&str, &str, Kind)] = &[
    ("api", "listen", Kind::Text),
    ("api", "auth", Kind::Text),
    ("api", "tls_cert", Kind::Text),
    ("api", "tls_key", Kind::Text),
    ("workers", "listen", Kind::Text),
    ("workers", "heartbeat_interval", Kind::Text),
    ("workers", "heartbeat_timeout", Kind::Text),
    ("workers", "cert_lifetime", Kind::Text),
    ("storage", "data_dir", Kind::Text),
    ("storage", "db", Kind::Text),
    ("storage", "durability", Kind::Text),
    ("storage", "retention", Kind::Text),
    ("storage", "backup_dir", Kind::Text),
    ("logs", "dir", Kind::Text),
    ("logs", "max_per_attempt", Kind::Text),
    ("dags", "default_timezone", Kind::Text),
    ("dags", "watch", Kind::Text),
    ("defaults", "catchup", Kind::Boolean),
    ("defaults", "overlap", Kind::Text),
    ("defaults", "retries", Kind::Integer),
    ("defaults", "retry_delay", Kind::Text),
    ("defaults", "retry_backoff", Kind::Text),
    ("defaults", "max_retry_delay", Kind::Text),
    ("defaults", "timeout", Kind::Text),
    ("alerts", "webhook", Kind::Text),
    ("alerts", "waiting_after", Kind::Text),
    ("alerts", "on_failure", Kind::Boolean),
];

/// Every field of the worker.
const WORKER_FIELDS: &[(&str, Kind)] = &[
    ("master", Kind::Text),
    ("token", Kind::Text),
    ("token_file", Kind::Text),
    ("name", Kind::Text),
    ("data_dir", Kind::Text),
    ("slots", Kind::Integer),
    ("labels", Kind::List),
    ("executors", Kind::List),
    ("workdir", Kind::Text),
    ("cache_dir", Kind::Text),
    ("cache_max", Kind::Text),
    ("min_free_disk", Kind::Text),
    ("min_free_memory", Kind::Text),
    ("kill_grace", Kind::Text),
    ("log_spool_max", Kind::Text),
    ("cgroups", Kind::Text),
];

fn to_value(text: &str, kind: Kind, name: &str) -> Result<Value> {
    Ok(match kind {
        Kind::Text => Value::String(text.to_owned()),
        Kind::Integer => Value::Integer(
            text.parse()
                .with_context(|| format!("`{name}` must be a number, not `{text}`"))?,
        ),
        Kind::Boolean => Value::Boolean(
            text.parse()
                .with_context(|| format!("`{name}` must be true or false, not `{text}`"))?,
        ),
        Kind::List => Value::Array(
            text.split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(|item| Value::String(item.to_owned()))
                .collect(),
        ),
    })
}

/// Splits `--storage-db x` and `--storage-db=x` into names and values.
fn parse_flags(flags: &[String]) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let mut flags = flags.iter();
    while let Some(flag) = flags.next() {
        let Some(name) = flag.strip_prefix("--") else {
            bail!("unexpected argument `{flag}`");
        };
        let (name, value) = match name.split_once('=') {
            Some((name, value)) => (name, value.to_owned()),
            None => (
                name,
                flags
                    .next()
                    .with_context(|| format!("`{flag}` needs a value"))?
                    .clone(),
            ),
        };
        out.insert(name.replace('-', "_"), value);
    }
    Ok(out)
}

fn absolute(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).with_context(|| format!("invalid path {}", path.display()))
}

fn read_table(path: Option<&Path>) -> Result<Table> {
    let Some(path) = path else {
        return Ok(Table::new());
    };
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read the configuration file {}", path.display()))?;
    text.parse::<Table>()
        .with_context(|| format!("{} is not valid TOML", path.display()))
}

fn section<'t>(table: &'t mut Table, name: &str) -> Result<&'t mut Table> {
    table
        .entry(name)
        .or_insert_with(|| Value::Table(Table::new()))
        .as_table_mut()
        .with_context(|| format!("`{name}` must be a section"))
}

/// Applies environment variables and flags of the worker fields to `table`,
/// the worker's table. `flag_prefix` is `worker_` in standalone, where the
/// worker's flags are spelled `--worker-slots`.
fn override_worker(
    table: &mut Table,
    env: &dyn Fn(&str) -> Option<String>,
    flags: &mut BTreeMap<String, String>,
    flag_prefix: &str,
) -> Result<()> {
    // On the command line every user of the machine can read the token,
    // with `ps`: it is not accepted there.
    if flags.contains_key(&format!("{flag_prefix}token")) {
        bail!(
            "the join token is not accepted as a flag: anyone on this machine could read it \
             with `ps`. Put it in a file and give --{}token-file, or set LWWO_WORKER_TOKEN",
            flag_prefix.replace('_', "-")
        );
    }
    for &(field, kind) in WORKER_FIELDS {
        let variable = format!("LWWO_WORKER_{}", field.to_ascii_uppercase());
        if let Some(text) = env(&variable) {
            table.insert(field.to_owned(), to_value(&text, kind, &variable)?);
        }
        if let Some(text) = flags.remove(&format!("{flag_prefix}{field}")) {
            table.insert(field.to_owned(), to_value(&text, kind, field)?);
        }
    }
    Ok(())
}

fn no_flags_left(flags: &BTreeMap<String, String>) -> Result<()> {
    if let Some(name) = flags.keys().next() {
        bail!("unknown option `--{}`", name.replace('_', "-"));
    }
    Ok(())
}

/// Reads the configuration of a master. In standalone the file may also
/// have a `[worker]` section, returned as the second value.
pub(crate) fn load_master(
    path: Option<&Path>,
    flags: &[String],
    standalone: bool,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<(MasterConfig, WorkerConfig)> {
    let mut table = read_table(path)?;
    let mut flags = parse_flags(flags)?;
    let mut worker = match table.remove("worker") {
        Some(Value::Table(worker)) if standalone => worker,
        Some(_) => bail!("the [worker] section is only read by `lwwo standalone`"),
        None => Table::new(),
    };

    if standalone {
        // Standalone is for trying lwwo out: everything in the current
        // directory, and a local API that asks for no token.
        let storage = section(&mut table, "storage")?;
        storage
            .entry("data_dir")
            .or_insert_with(|| Value::String("./lwwo-data".to_owned()));
        section(&mut table, "api")?
            .entry("auth")
            .or_insert_with(|| Value::String("none".to_owned()));
    }
    for &(section_name, field, kind) in MASTER_FIELDS {
        let variable = format!("LWWO_MASTER_{section_name}_{field}").to_ascii_uppercase();
        if let Some(text) = env(&variable) {
            section(&mut table, section_name)?
                .insert(field.to_owned(), to_value(&text, kind, &variable)?);
        }
        if let Some(text) = flags.remove(&format!("{section_name}_{field}")) {
            section(&mut table, section_name)?
                .insert(field.to_owned(), to_value(&text, kind, field)?);
        }
    }
    let mut master: MasterConfig = Value::Table(table)
        .try_into()
        .context("invalid master configuration")?;
    // Tasks and containers get these paths: they must not depend on the
    // directory the master was started from.
    master.storage.data_dir = absolute(&master.storage.data_dir)?;
    let alerts = &master.alerts;
    if alerts.webhook.is_none() && (alerts.on_failure || alerts.waiting_after.is_some()) {
        bail!("[alerts] needs a `webhook` to send the alerts to");
    }
    if master.workers.heartbeat_timeout.0 <= master.workers.heartbeat_interval.0 {
        bail!(
            "[workers] heartbeat_timeout must be longer than heartbeat_interval, \
             or every worker is lost between two heartbeats"
        );
    }
    if master.workers.cert_lifetime.0 < Duration::from_secs(10) {
        bail!("[workers] cert_lifetime must be at least 10 seconds");
    }
    if master.api.auth == Auth::None && !master.api.listen.ip().is_loopback() {
        bail!(
            "[api] auth = \"none\" is only allowed on a local address, not on {}",
            master.api.listen
        );
    }

    if standalone {
        override_worker(&mut worker, env, &mut flags, "worker_")?;
        worker.entry("data_dir").or_insert_with(|| {
            Value::String(master.storage.data_dir.join("worker").display().to_string())
        });
    }
    no_flags_left(&flags)?;
    let mut worker: WorkerConfig = Value::Table(worker)
        .try_into()
        .context("invalid [worker] section")?;
    worker.data_dir = absolute(&worker.data_dir)?;
    Ok((master, worker))
}

/// Reads the configuration of a worker.
pub(crate) fn load_worker(
    path: Option<&Path>,
    flags: &[String],
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<WorkerConfig> {
    let mut table = read_table(path)?;
    let mut flags = parse_flags(flags)?;
    override_worker(&mut table, env, &mut flags, "")?;
    no_flags_left(&flags)?;
    let mut worker: WorkerConfig = Value::Table(table)
        .try_into()
        .context("invalid worker configuration")?;
    worker.data_dir = absolute(&worker.data_dir)?;
    if worker.master.is_none() {
        bail!(
            "the worker needs the address of the master: set `master`, LWWO_WORKER_MASTER or --master"
        );
    }
    Ok(worker)
}

/// Where the CLI and the TUI find the master, and with which token.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct ClientConfig {
    pub(crate) api: Option<String>,
    pub(crate) token: Option<String>,
}

/// Reads `LWWO_API` and `LWWO_TOKEN`, then `~/.config/lwwo/config.toml`.
pub(crate) fn load_client(env: &dyn Fn(&str) -> Option<String>) -> Result<ClientConfig> {
    let mut config = ClientConfig::default();
    if let Some(home) = env("HOME") {
        let path = Path::new(&home).join(".config/lwwo/config.toml");
        if path.exists() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("cannot read {}", path.display()))?;
            config = toml::from_str(&text).with_context(|| {
                format!("{} is not a valid client configuration", path.display())
            })?;
        }
    }
    if let Some(api) = env("LWWO_API") {
        config.api = Some(api);
    }
    if let Some(token) = env("LWWO_TOKEN") {
        config.token = Some(token);
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn flags(list: &[&str]) -> Vec<String> {
        list.iter().map(|&f| f.to_owned()).collect()
    }

    #[test]
    fn defaults_are_those_of_the_spec() {
        let (master, _) = load_master(None, &[], false, &no_env).unwrap();
        assert_eq!(master.api.listen.to_string(), "127.0.0.1:7070");
        assert_eq!(master.api.auth, Auth::Token);
        assert_eq!(master.workers.heartbeat_timeout.0, Duration::from_secs(30));
        assert_eq!(master.db_path(), PathBuf::from("/var/lib/lwwo/lwwo.db"));
        assert_eq!(master.defaults.retries, 0);
        assert_eq!(master.dags.default_timezone, "UTC");
    }

    #[test]
    fn flags_beat_the_environment_which_beats_the_file() {
        let dir = std::env::temp_dir().join(format!("lwwo-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("master.toml");
        std::fs::write(
            &file,
            "[storage]\ndata_dir = \"/file\"\nretention = \"7d\"\n[defaults]\nretries = 1\n",
        )
        .unwrap();
        let env = |name: &str| match name {
            "LWWO_MASTER_STORAGE_DATA_DIR" => Some("/env".to_owned()),
            "LWWO_MASTER_DEFAULTS_RETRIES" => Some("2".to_owned()),
            _ => None,
        };
        let (master, _) = load_master(
            Some(&file),
            &flags(&["--defaults-retries", "3"]),
            false,
            &env,
        )
        .unwrap();
        assert_eq!(master.storage.data_dir, PathBuf::from("/env"));
        assert_eq!(master.defaults.retries, 3);
        assert_eq!(master.storage.retention.0, Duration::from_secs(7 * 86_400));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn alerts_are_off_unless_a_webhook_is_given() {
        let (master, _) = load_master(None, &[], false, &no_env).unwrap();
        assert!(master.alerts.webhook.is_none());
        let on = flags(&[
            "--alerts-webhook=https://hooks.example/x",
            "--alerts-waiting_after=15m",
            "--alerts-on_failure=true",
        ]);
        let (master, _) = load_master(None, &on, false, &no_env).unwrap();
        assert_eq!(
            master.alerts.waiting_after.map(|d| d.0),
            Some(Duration::from_secs(900))
        );
        assert!(master.alerts.on_failure);
        let half = flags(&["--alerts-on_failure=true"]);
        assert!(
            load_master(None, &half, false, &no_env).is_err(),
            "nowhere to send them"
        );
    }

    #[test]
    fn unknown_flags_and_fields_are_rejected() {
        assert!(load_master(None, &flags(&["--storage-bd", "x"]), false, &no_env).is_err());
        assert!(load_worker(None, &flags(&["--master=m:1", "--slot=2"]), &no_env).is_err());
    }

    #[test]
    fn no_auth_only_on_a_local_address() {
        let open = flags(&["--api-auth=none", "--api-listen=0.0.0.0:7070"]);
        assert!(load_master(None, &open, false, &no_env).is_err());
        let local = flags(&["--api-auth=none"]);
        assert!(load_master(None, &local, false, &no_env).is_ok());
    }

    #[test]
    fn the_heartbeat_timeout_must_leave_room_for_a_heartbeat() {
        let short = flags(&["--workers-heartbeat_timeout=5s"]);
        assert!(load_master(None, &short, false, &no_env).is_err());
        let fine = flags(&[
            "--workers-heartbeat_interval=1s",
            "--workers-heartbeat_timeout=5s",
        ]);
        assert!(load_master(None, &fine, false, &no_env).is_ok());
    }

    #[test]
    fn standalone_keeps_everything_in_the_current_directory() {
        let (master, worker) =
            load_master(None, &flags(&["--worker-slots", "3"]), true, &no_env).unwrap();
        let here = std::env::current_dir().unwrap();
        assert_eq!(master.storage.data_dir, here.join("lwwo-data"));
        assert_eq!(master.api.auth, Auth::None);
        assert_eq!(worker.slots(), 3);
        assert_eq!(worker.data_dir, here.join("lwwo-data/worker"));
    }

    #[test]
    fn the_join_token_is_never_a_flag() {
        let error = load_worker(None, &flags(&["--master=m:1", "--token=lwwo1_x"]), &no_env)
            .unwrap_err()
            .to_string();
        assert!(error.contains("not accepted as a flag"), "{error}");
        let error = load_master(None, &flags(&["--worker-token=lwwo1_x"]), true, &no_env)
            .unwrap_err()
            .to_string();
        assert!(error.contains("--worker-token-file"), "{error}");

        let env = |name: &str| (name == "LWWO_WORKER_TOKEN").then(|| "lwwo1_from_env".to_owned());
        let worker = load_worker(None, &flags(&["--master=m:1"]), &env).unwrap();
        assert_eq!(
            worker.join_token().unwrap().as_deref(),
            Some("lwwo1_from_env")
        );

        let file = std::env::temp_dir().join(format!("lwwo-token-{}", uuid::Uuid::new_v4()));
        std::fs::write(&file, "lwwo1_from_file\n").unwrap();
        let given = format!("--token-file={}", file.display());
        let worker = load_worker(None, &flags(&["--master=m:1", &given]), &no_env).unwrap();
        assert_eq!(
            worker.join_token().unwrap().as_deref(),
            Some("lwwo1_from_file")
        );
        std::fs::remove_file(file).unwrap();
    }

    #[test]
    fn worker_lists_come_separated_by_commas() {
        let env = |name: &str| (name == "LWWO_WORKER_LABELS").then(|| "big, gpu".to_owned());
        let worker = load_worker(None, &flags(&["--master", "m:7071"]), &env).unwrap();
        assert_eq!(worker.labels, ["big", "gpu"]);
        assert_eq!(worker.master.as_deref(), Some("m:7071"));
        assert!(
            load_worker(None, &[], &no_env).is_err(),
            "master is required"
        );
    }
}
