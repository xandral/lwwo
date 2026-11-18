//! The state of the master and what it does with every event.
//!
//! One task owns a [`Master`] and feeds it one [`Msg`] at a time, so there
//! are no locks. Every event goes the same way: the engine updates its
//! memory and answers with commands, the commands that save are written to
//! SQLite in one transaction, and only after the commit the messages for
//! the workers leave. A crash between the two loses no work: what was
//! saved is found again at the next start.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};

use std::time::Duration;

use anyhow::{Context, Result};
use lwwo_core::dag::{Graph, TaskId};
use lwwo_core::engine::{Command, Engine, EngineConfig, Event, Outcome};
use lwwo_core::run::{Rerun, RunId, RunOutcome, TaskSnapshot, TaskState};
use lwwo_core::time::Timestamp;
use lwwo_core::timetable::{Decision, Overlap, Trigger};
use lwwo_core::worker_selection::{Blocker, WorkerId};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info, warn};

use super::alerts::{self, Webhook};
use super::logs::{LogKey, LogStore};
use crate::config::MasterConfig;
use crate::dsl::{Dag, RunContext};
use crate::proto;
use crate::security::{Ca, Token, TokenKind};
use crate::store::{AttemptEnd, RunRow, Store};
use crate::util::{now, now_ms, parse_instant, parse_run_id, run_id_text};
use crate::zone::Zone;

/// An error for whoever called the API, with its HTTP status.
#[derive(Debug, Clone)]
pub(crate) struct ApiError {
    pub(crate) status: u16,
    pub(crate) message: String,
}

impl ApiError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(409, message)
    }

    fn bad_request(error: &anyhow::Error) -> Self {
        Self::new(400, format!("{error:#}"))
    }
}

impl From<anyhow::Error> for ApiError {
    /// An error nobody expected: the caller sees a 500.
    fn from(error: anyhow::Error) -> Self {
        Self::new(500, format!("{error:#}"))
    }
}

type ApiResult = Result<Value, ApiError>;

/// Which tasks of a run to run again, as asked through the API.
#[derive(Debug, Clone, Default)]
pub(crate) struct RerunRequest {
    /// One task; `None` for the failed ones, or all with `all`.
    pub(crate) task: Option<String>,
    pub(crate) all: bool,
    /// With `task`: only that task, not what depends on it.
    pub(crate) only: bool,
    /// Use the current version of the DAG, if its structure is the same.
    pub(crate) current: bool,
}

/// A rerun waiting for room, as stored in `dag_runs.rerun`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct QueuedRerun {
    task: Option<String>,
    all: bool,
    only: bool,
    /// The version to run with, chosen when the rerun was asked for.
    version: String,
    /// The state of the run before: it goes back to it if the rerun does
    /// not happen.
    was: String,
}

/// A request to the master, from the API or from the gRPC service.
#[derive(Debug)]
pub(crate) enum Call {
    Apply {
        yaml: String,
    },
    Dags,
    Dag(String),
    Pause {
        dag: String,
        paused: bool,
    },
    DeleteDag(String),
    Trigger {
        dag: String,
        at: Option<String>,
        params: BTreeMap<String, String>,
    },
    Runs {
        dag: Option<String>,
        limit: u32,
    },
    Run(String),
    Cancel(String),
    Rerun {
        run: String,
        request: RerunRequest,
    },
    /// Where the log of an attempt is, and whether the attempt is over.
    LogTarget {
        run: String,
        task: String,
        attempt: Option<u32>,
    },
    Workers,
    Drain(String),
    RemoveWorker(String),
    NewToken {
        kind: TokenKind,
        label: Option<String>,
        rotate: bool,
        /// How long an API token lasts; `None` for ever.
        expires: Option<Duration>,
    },
    /// Every token, without secrets.
    Tokens,
    /// Revokes a token by id.
    RevokeToken(String),
    /// Whether an API token is valid.
    CheckToken(String),
    /// A worker enters with the join token.
    Join {
        token: String,
        name: String,
        csr_pem: String,
    },
    /// A worker renews its certificate.
    Renew {
        name: String,
        csr_pem: String,
    },
    Status,
}

/// Everything that can wake the master, apart from its alarm.
#[derive(Debug)]
pub(crate) enum Msg {
    Call(Call, oneshot::Sender<ApiResult>),
    WorkerHello {
        name: String,
        session: u64,
        hello: proto::Hello,
        tx: mpsc::Sender<proto::MasterMessage>,
    },
    WorkerMessage {
        name: String,
        session: u64,
        kind: proto::worker_message::Kind,
    },
    WorkerGone {
        name: String,
        session: u64,
    },
}

/// Where a new run comes from.
#[derive(Debug, Clone, Copy)]
enum Origin {
    /// `lwwo run`, or the API.
    Manual,
    /// A fire of the cron. The row of the timetable moves to `next_fire`
    /// in the same transaction that creates the run.
    Timetable {
        /// `cron` or `catchup`.
        trigger: &'static str,
        next_fire: Option<i64>,
    },
}

/// A version of a DAG ready to run: parsed, with its graph.
#[derive(Debug)]
struct Loaded {
    dag: Dag,
    graph: Arc<Graph>,
    zone: Zone,
    ids: HashMap<String, TaskId>,
}

/// A run the engine is working on.
#[derive(Debug)]
struct OpenRun {
    id: String,
    dag: String,
    /// The version its attempts use.
    hash: String,
    loaded: Arc<Loaded>,
    logical_time: i64,
    params: BTreeMap<String, Value>,
}

/// A connected worker.
#[derive(Debug)]
struct Session {
    /// Which stream of the worker this is: messages of older ones are
    /// ignored.
    number: u64,
    tx: mpsc::Sender<proto::MasterMessage>,
    total_memory: u64,
    available_memory: u64,
    free_disk: u64,
    connected_at: i64,
    last_heartbeat: i64,
}

/// The state of a master.
pub(crate) struct Master {
    config: MasterConfig,
    store: Store,
    engine: Engine,
    logs: Arc<LogStore>,
    /// The CA, when workers and API use TLS; `None` in standalone.
    ca: Option<Arc<Ca>>,
    data_dir: PathBuf,
    open: HashMap<RunId, OpenRun>,
    /// Manual runs waiting for the overlap rule, oldest first, by DAG.
    queued: BTreeMap<String, VecDeque<String>>,
    /// Versions in memory: only while some run uses them.
    versions: HashMap<String, Weak<Loaded>>,
    worker_ids: HashMap<String, WorkerId>,
    worker_names: Vec<String>,
    sessions: HashMap<WorkerId, Session>,
    /// The name of every task that is on a worker, also for runs that are
    /// no longer open: a cancelled run still gets the reports of its tasks.
    inflight: HashMap<(RunId, TaskId), String>,
    /// The serial of the valid certificate of each worker, shared with the
    /// gRPC service so that checking a connection reads no database.
    serials: Arc<RwLock<HashMap<String, String>>>,
    /// The next cron fire: the only thing of the timetable kept in memory.
    next_cron: Option<i64>,
    next_cleanup: i64,
    /// DAGs with a run that just ended: their waiting fires may start.
    ended: Vec<String>,
    /// Events the master produced for itself.
    pending: VecDeque<Event>,
    changes: broadcast::Sender<()>,
    dirty: bool,
    /// Where alerts go, if they are on.
    webhook: Option<Webhook>,
    /// Since when each waiting task has been waiting, and whether it was
    /// reported already. Only kept when waiting tasks are reported.
    waiting_since: HashMap<(RunId, TaskId), (i64, bool)>,
}

fn state_text(state: TaskState) -> &'static str {
    match state {
        TaskState::Blocked => "blocked",
        TaskState::Assigned => "assigned",
        TaskState::Retrying => "retrying",
        TaskState::Success => "success",
        TaskState::Failed => "failed",
        TaskState::Skipped => "skipped",
        TaskState::Cancelled => "cancelled",
    }
}

fn parse_state(text: &str) -> Option<TaskState> {
    Some(match text {
        "blocked" => TaskState::Blocked,
        "assigned" => TaskState::Assigned,
        "retrying" => TaskState::Retrying,
        "success" => TaskState::Success,
        "failed" => TaskState::Failed,
        "skipped" => TaskState::Skipped,
        "cancelled" => TaskState::Cancelled,
        _ => return None,
    })
}

fn outcome_text(outcome: RunOutcome) -> &'static str {
    match outcome {
        RunOutcome::Success => "success",
        RunOutcome::Failed => "failed",
        RunOutcome::Cancelled => "cancelled",
    }
}

fn task_ref(run: RunId, task: &str, attempt: u32) -> proto::TaskRef {
    proto::TaskRef {
        run_id: run_id_text(run),
        task: task.to_owned(),
        attempt,
    }
}

fn message(kind: proto::master_message::Kind) -> proto::MasterMessage {
    proto::MasterMessage { kind: Some(kind) }
}

impl Master {
    /// Opens the database and loads back what was running.
    pub(crate) fn new(
        config: MasterConfig,
        logs: Arc<LogStore>,
        ca: Option<Arc<Ca>>,
        serials: Arc<RwLock<HashMap<String, String>>>,
        changes: broadcast::Sender<()>,
    ) -> Result<Self> {
        let store = Store::open(&config.db_path(), config.storage.durability)?;
        let engine = Engine::new(EngineConfig {
            heartbeat_timeout: config.workers.heartbeat_timeout.0,
        });
        let mut master = Self {
            data_dir: config.storage.data_dir.clone(),
            config,
            store,
            engine,
            logs,
            ca,
            open: HashMap::new(),
            queued: BTreeMap::new(),
            versions: HashMap::new(),
            worker_ids: HashMap::new(),
            worker_names: Vec::new(),
            sessions: HashMap::new(),
            inflight: HashMap::new(),
            serials,
            next_cron: None,
            next_cleanup: 0,
            ended: Vec::new(),
            pending: VecDeque::new(),
            changes,
            dirty: false,
            webhook: None,
            waiting_since: HashMap::new(),
        };
        master.webhook = master.config.alerts.webhook.clone().map(Webhook::new);
        master.restore()?;
        Ok(master)
    }

    /// The instant the alarm must be armed on: the nearest of the next
    /// cron fire, the engine's deadlines, the daily clean-up and the next
    /// waiting task to report.
    pub(crate) fn next_wake(&self) -> Option<i64> {
        [
            self.next_cron,
            self.engine.next_deadline().map(Timestamp::as_millis),
            Some(self.next_cleanup),
            self.next_waiting_alert(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// The alarm rang, or the clock moved: do whatever is due by now.
    pub(crate) fn on_alarm(&mut self) -> Result<()> {
        let now_ms = now_ms();
        if self.next_cron.is_some_and(|at| at <= now_ms) {
            self.fire(now_ms)?;
        }
        if self
            .engine
            .next_deadline()
            .is_some_and(|at| at.as_millis() <= now_ms)
        {
            self.engine_event(Event::Timer)?;
        }
        if self.next_cleanup <= now_ms {
            self.cleanup(now_ms)?;
        }
        self.settle()?;
        self.alert_waiting(now_ms);
        Ok(())
    }

    /// Handles one message.
    pub(crate) fn on_message(&mut self, msg: Msg) -> Result<()> {
        match msg {
            Msg::Call(call, reply) => {
                let result = self.call(call);
                if let Err(error) = &result {
                    if error.status >= 500 {
                        error!("request failed: {}", error.message);
                    }
                }
                // Whoever asked may have gone away: nothing to do then.
                let _ = reply.send(result);
            }
            Msg::WorkerHello {
                name,
                session,
                hello,
                tx,
            } => self.worker_hello(&name, session, hello, tx)?,
            Msg::WorkerMessage {
                name,
                session,
                kind,
            } => self.worker_message(&name, session, kind)?,
            Msg::WorkerGone { name, session } => self.worker_gone(&name, session)?,
        }
        self.settle()
    }

    /// Finishes what an event started: the master's own events, the fires
    /// that waited for a run to end, the sessions of workers given up.
    fn settle(&mut self) -> Result<()> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                self.engine_event(event)?;
            } else if let Some(dag) = self.ended.pop() {
                let started = self.after_run(&dag);
                contain(started, &format!("what waits for a run of `{dag}`"))?;
            } else {
                break;
            }
        }
        // A worker the engine gave up: closing its stream makes it
        // reconnect and say what it still runs.
        let engine = &self.engine;
        self.sessions
            .retain(|&id, _| engine.workers().get(id).is_some());
        self.inflight
            .retain(|&(run, task), _| engine.running(run, task).is_some());
        if self.dirty {
            self.dirty = false;
            // Nobody listening is fine.
            let _ = self.changes.send(());
        }
        self.track_waiting();
        Ok(())
    }

    // Alerts

    /// How long a task may wait for a worker before it is reported, if
    /// waiting tasks are reported at all.
    fn waiting_after(&self) -> Option<i64> {
        self.webhook.as_ref()?;
        let after = self.config.alerts.waiting_after?.0;
        Some(i64::try_from(after.as_millis()).unwrap_or(i64::MAX))
    }

    /// Notes which tasks wait for a worker, and since when. Called after
    /// every event: the ready queue is in memory, so this costs no I/O.
    fn track_waiting(&mut self) {
        if self.waiting_after().is_none() {
            return;
        }
        let now = now_ms();
        let waiting: std::collections::HashSet<(RunId, TaskId)> =
            self.engine.waiting().into_iter().collect();
        self.waiting_since.retain(|key, _| waiting.contains(key));
        for key in waiting {
            self.waiting_since.entry(key).or_insert((now, false));
        }
    }

    /// When the next waiting task has to be reported.
    fn next_waiting_alert(&self) -> Option<i64> {
        let after = self.waiting_after()?;
        self.waiting_since
            .values()
            .filter(|(_, reported)| !reported)
            .map(|(since, _)| since.saturating_add(after))
            .min()
    }

    /// Reports, in one alert, the tasks that have waited too long and were
    /// not reported yet. A task is reported once; if it starts and later
    /// waits again, that is a new wait.
    fn alert_waiting(&mut self, now: i64) {
        let (Some(after), Some(webhook)) = (self.waiting_after(), self.webhook.clone()) else {
            return;
        };
        let mut due: Vec<(RunId, TaskId, i64)> = self
            .waiting_since
            .iter_mut()
            .filter(|(_, (since, reported))| !*reported && now - *since >= after)
            .map(|(&(run, task), (since, reported))| {
                *reported = true;
                (run, task, *since)
            })
            .collect();
        if due.is_empty() {
            return;
        }
        due.sort_by_key(|&(_, _, since)| since);
        let mut lines = Vec::new();
        let mut tasks = Vec::new();
        for (run, task, since) in due {
            let Some(open) = self.open.get(&run) else {
                continue;
            };
            let name = open.loaded.graph.name(task);
            let reason = self.engine.blocker(run, task).map_or_else(
                || "a worker should take it now".to_owned(),
                |b| blocker_text(&b),
            );
            lines.push(format!("• {} / {name}: {reason}", open.dag));
            tasks.push(json!({
                "dag": open.dag,
                "run": open.id,
                "task": name,
                "waiting_since": since,
                "reason": reason,
            }));
        }
        if tasks.is_empty() {
            return;
        }
        let limit = alerts::duration_text(Duration::from_millis(u64::try_from(after).unwrap_or(0)));
        let count = tasks.len();
        let noun = if count == 1 { "task has" } else { "tasks have" };
        let text = format!(
            "lwwo: {count} {noun} been waiting for a worker for more than {limit}:\n{}",
            lines.join("\n")
        );
        warn!("{}", text.replace('\n', " "));
        webhook.send(json!({ "kind": "tasks_waiting", "text": text, "tasks": tasks }));
    }

    /// Reports a run that ended as failed, if failures are reported.
    fn alert_failed(&self, open: &OpenRun) {
        let Some(webhook) = self
            .webhook
            .as_ref()
            .filter(|_| self.config.alerts.on_failure)
        else {
            return;
        };
        let failed: Vec<String> = self
            .store
            .tasks(&open.id)
            .unwrap_or_default()
            .into_iter()
            .filter(|task| task.state == "failed")
            .map(|task| task.task)
            .collect();
        let when = open.loaded.zone.rfc3339(open.logical_time);
        let problem = self
            .store
            .run(&open.id)
            .ok()
            .flatten()
            .and_then(|row| row.problem);
        let text = match &problem {
            Some(problem) => format!(
                "lwwo: {} was stopped by lwwo (run {}, for {when}): {problem}",
                open.dag, open.id
            ),
            None => format!(
                "lwwo: {} failed (run {}, for {when}). Failed: {}.",
                open.dag,
                open.id,
                failed.join(", ")
            ),
        };
        webhook.send(json!({
            "kind": "run_failed",
            "text": text,
            "dag": open.dag,
            "run": open.id,
            "logical_time": open.logical_time,
            "failed_tasks": failed,
            "problem": problem,
        }));
    }

    // Versions and names

    /// A version ready to run, from memory or from its YAML.
    fn load(&mut self, hash: &str, yaml: &str) -> Result<Arc<Loaded>> {
        if let Some(loaded) = self.versions.get(hash).and_then(Weak::upgrade) {
            return Ok(loaded);
        }
        let dag = Dag::parse(yaml)?;
        let graph = Arc::new(dag.graph(&self.config.defaults)?);
        let ids = graph
            .tasks()
            .map(|task| (graph.name(task).to_owned(), task))
            .collect();
        let loaded = Arc::new(Loaded {
            zone: dag.zone(&self.config.dags.default_timezone)?,
            dag,
            graph,
            ids,
        });
        self.versions
            .retain(|_, version| version.strong_count() > 0);
        self.versions
            .insert(hash.to_owned(), Arc::downgrade(&loaded));
        Ok(loaded)
    }

    fn load_version(&mut self, hash: &str) -> Result<Arc<Loaded>> {
        let version = self
            .store
            .version(hash)?
            .with_context(|| format!("version {hash} is not in the database"))?;
        self.load(&version.hash, &version.yaml)
    }

    fn worker_id(&mut self, name: &str) -> WorkerId {
        if let Some(&id) = self.worker_ids.get(name) {
            return id;
        }
        let id = WorkerId::new(u32::try_from(self.worker_names.len()).unwrap_or(u32::MAX));
        self.worker_names.push(name.to_owned());
        self.worker_ids.insert(name.to_owned(), id);
        id
    }

    fn worker_name(&self, id: WorkerId) -> &str {
        self.worker_names
            .get(id.as_u32() as usize)
            .map_or("?", String::as_str)
    }

    fn task_name(&self, run: RunId, task: TaskId) -> Option<String> {
        self.open
            .get(&run)
            .map(|open| open.loaded.graph.name(task).to_owned())
            .or_else(|| self.inflight.get(&(run, task)).cloned())
    }

    fn task_id(&self, run: RunId, task: &str) -> Option<TaskId> {
        self.open
            .get(&run)
            .and_then(|open| open.loaded.ids.get(task).copied())
            .or_else(|| {
                self.inflight
                    .iter()
                    .find(|&(&(r, _), name)| r == run && name == task)
                    .map(|(&(_, id), _)| id)
            })
    }

    fn active(&self, dag: &str) -> Vec<RunId> {
        let mut runs: Vec<RunId> = self
            .open
            .iter()
            .filter(|(_, open)| open.dag == dag)
            .map(|(&id, _)| id)
            .collect();
        runs.sort_unstable();
        runs
    }

    fn active_count(&self, dag: &str) -> u32 {
        u32::try_from(self.active(dag).len()).unwrap_or(u32::MAX)
    }

    // Engine and commands

    /// Gives an event to the engine and carries out its commands. Returns
    /// whether there were any.
    fn engine_event(&mut self, event: Event) -> Result<bool> {
        let commands = self.engine.handle(event, now());
        let any = !commands.is_empty();
        self.execute(&commands)?;
        Ok(any)
    }

    /// Carries out the commands of one event: first everything that is
    /// saved, in one transaction, then the messages to the workers.
    #[expect(
        clippy::too_many_lines,
        reason = "one arm per command, and they belong in one transaction"
    )]
    fn execute(&mut self, commands: &[Command]) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }
        let now_ms = now_ms();
        let mut outbox: Vec<(WorkerId, proto::MasterMessage)> = Vec::new();
        let mut finished: Vec<(RunId, RunOutcome)> = Vec::new();
        let mut assigned: Vec<(RunId, TaskId, String)> = Vec::new();
        // Attempts lost with their worker: their logs will not grow.
        let mut lost_logs: Vec<(RunId, String, u32)> = Vec::new();
        // Assignments whose templates could not be rendered.
        let mut broken: Vec<Event> = Vec::new();

        self.store.transaction(|store| {
            for command in commands {
                match *command {
                    Command::SaveTask {
                        run,
                        task,
                        state,
                        attempts,
                    } => {
                        let Some(name) = self.task_name(run, task) else {
                            continue;
                        };
                        if state == TaskState::Blocked && attempts > 0 {
                            lost_logs.push((run, name.clone(), attempts));
                        }
                        let run = run_id_text(run);
                        store.save_task(&run, &name, state_text(state), attempts)?;
                        // An attempt still open on a task that is waiting
                        // again was lost with its worker; on a cancelled
                        // task, it was cancelled.
                        match state {
                            TaskState::Blocked => {
                                store.close_attempts(&run, &name, "lost", now_ms)?;
                            }
                            TaskState::Cancelled => {
                                store.close_attempts(&run, &name, "cancelled", now_ms)?;
                            }
                            _ => {}
                        }
                    }
                    Command::SaveRun { run, outcome } => {
                        store.finish_run(&run_id_text(run), outcome_text(outcome), now_ms)?;
                        finished.push((run, outcome));
                    }
                    Command::ReopenRun { run } => store.reopen_run(&run_id_text(run))?,
                    Command::Quarantine { run, ref reason } => {
                        let name = self.open.get(&run).map_or("?", |open| open.dag.as_str());
                        error!(
                            "run {} of {name} was stopped: {reason}. The other runs go on.",
                            run_id_text(run)
                        );
                        store.set_problem(&run_id_text(run), reason)?;
                    }
                    Command::Anomaly { ref message } => {
                        warn!("the engine corrected an inconsistency: {message}");
                    }
                    Command::Assign {
                        worker,
                        run,
                        task,
                        attempt,
                        timeout,
                    } => {
                        let open = self
                            .open
                            .get(&run)
                            .context("the engine assigned a task of a run that is not open")?;
                        let name = open.loaded.graph.name(task).to_owned();
                        store.insert_attempt(
                            &open.id,
                            &name,
                            attempt,
                            self.worker_name(worker),
                            &open.hash,
                            now_ms,
                        )?;
                        match assignment(open, task, attempt, timeout) {
                            Ok(assign) => {
                                outbox.push((
                                    worker,
                                    message(proto::master_message::Kind::Assign(assign)),
                                ));
                            }
                            Err(error) => {
                                warn!("{}/{name} cannot start: {error:#}", open.dag);
                                let end = AttemptEnd {
                                    outcome: "failed",
                                    exit_code: None,
                                    started_at: None,
                                    finished_at: now_ms,
                                    peak_memory: None,
                                    log_bytes: None,
                                    log_truncated: false,
                                };
                                store.finish_attempt(
                                    &open.id,
                                    &name,
                                    attempt,
                                    self.worker_name(worker),
                                    &end,
                                )?;
                                broken.push(Event::Report {
                                    worker,
                                    run,
                                    task,
                                    attempt,
                                    outcome: Outcome::Failed,
                                });
                            }
                        }
                        assigned.push((run, task, name));
                    }
                    Command::Cancel { worker, run, task } => {
                        if let Some(name) = self.task_name(run, task) {
                            let cancel = proto::Cancel {
                                task: Some(task_ref(run, &name, 0)),
                            };
                            outbox.push((
                                worker,
                                message(proto::master_message::Kind::Cancel(cancel)),
                            ));
                        }
                    }
                    Command::Confirm {
                        worker,
                        run,
                        task,
                        attempt,
                    } => {
                        if let Some(name) = self.task_name(run, task) {
                            let confirm = proto::Confirm {
                                task: Some(task_ref(run, &name, attempt)),
                            };
                            outbox.push((
                                worker,
                                message(proto::master_message::Kind::Confirm(confirm)),
                            ));
                        }
                    }
                }
            }
            Ok(())
        })?;

        // Committed: now, and only now, the workers hear about it.
        for (run, task, attempt) in lost_logs {
            if let Some(open) = self.open.get(&run) {
                // Closes the log, if it was still open; a
                // late piece from the worker is then acknowledged and
                // dropped.
                let _ = self.logs.take_stats(&LogKey {
                    dag: open.dag.clone(),
                    run: open.id.clone(),
                    task,
                    attempt,
                });
            }
        }
        for (run, task, name) in assigned {
            self.inflight.insert((run, task), name);
        }
        for (worker, message) in outbox {
            self.send(worker, message);
        }
        for (run, outcome) in finished {
            if let Some(open) = self.open.remove(&run) {
                info!(
                    "run {} of {} ended: {}",
                    open.id,
                    open.dag,
                    outcome_text(outcome)
                );
                if outcome == RunOutcome::Failed {
                    self.alert_failed(&open);
                }
                self.ended.push(open.dag);
            }
        }
        self.pending.extend(broken);
        self.dirty = true;
        Ok(())
    }

    fn send(&mut self, worker: WorkerId, message: proto::MasterMessage) {
        let Some(session) = self.sessions.get(&worker) else {
            return;
        };
        if session.tx.try_send(message).is_err() {
            // The stream is closed or hopelessly behind: the worker will
            // reconnect and say what it has.
            self.sessions.remove(&worker);
        }
    }

    // Starting runs

    /// Creates a run and starts it. `next_fire` is the new row of the
    /// timetable when the run comes from the cron: it is written in the
    /// same transaction as the run.
    fn create_run(
        &mut self,
        dag: &str,
        hash: &str,
        loaded: Arc<Loaded>,
        logical_time: i64,
        params: BTreeMap<String, Value>,
        origin: Origin,
    ) -> Result<String> {
        let trigger = match origin {
            Origin::Manual => "manual",
            Origin::Timetable { trigger, .. } => trigger,
        };
        let now_ms = now_ms();
        let run = crate::util::new_run_id();
        let row = RunRow {
            id: run_id_text(run),
            dag_name: dag.to_owned(),
            version_hash: hash.to_owned(),
            logical_time,
            trigger: trigger.to_owned(),
            params: serde_json::to_string(&params)?,
            state: "running".to_owned(),
            created_at: now_ms,
            started_at: Some(now_ms),
            finished_at: None,
            problem: None,
        };
        self.store.transaction(|store| {
            if let Origin::Timetable { next_fire, .. } = origin {
                store.set_next_fire(dag, next_fire)?;
            }
            store.insert_run(&row)?;
            store.insert_tasks(
                &row.id,
                dag,
                loaded.dag.tasks.iter().map(|t| t.name.as_str()),
            )
        })?;
        info!("run {} of {dag} started ({trigger})", row.id);
        let graph = Arc::clone(&loaded.graph);
        self.open_run(run, &row, loaded, params);
        self.engine_event(Event::StartRun { run, graph })?;
        self.dirty = true;
        Ok(row.id)
    }

    fn open_run(
        &mut self,
        run: RunId,
        row: &RunRow,
        loaded: Arc<Loaded>,
        params: BTreeMap<String, Value>,
    ) {
        self.open.insert(
            run,
            OpenRun {
                id: row.id.clone(),
                dag: row.dag_name.clone(),
                hash: row.version_hash.clone(),
                loaded,
                logical_time: row.logical_time,
                params,
            },
        );
    }

    /// Starts a manual run that was waiting in the queue.
    fn start_queued(&mut self, id: &str) -> Result<()> {
        let Some(row) = self.store.run(id)? else {
            return Ok(());
        };
        if row.state != "queued" {
            return Ok(());
        }
        if let Some(rerun) = self.store.rerun_request(&row.id)? {
            return self.start_queued_rerun(row, &rerun);
        }
        let Some(run) = parse_run_id(&row.id) else {
            return Ok(());
        };
        let loaded = self.load_version(&row.version_hash)?;
        self.store.transaction(|store| {
            store.start_run(&row.id, now_ms())?;
            store.insert_tasks(
                &row.id,
                &row.dag_name,
                loaded.dag.tasks.iter().map(|t| t.name.as_str()),
            )
        })?;
        info!("run {} of {} started (was queued)", row.id, row.dag_name);
        let params = serde_json::from_str(&row.params).unwrap_or_default();
        let graph = Arc::clone(&loaded.graph);
        self.open_run(run, &row, loaded, params);
        self.engine_event(Event::StartRun { run, graph })?;
        self.dirty = true;
        Ok(())
    }

    /// Whether one more run of the DAG may start now, by its overlap rule.
    fn has_room(&self, dag: &str, overlap: Overlap) -> bool {
        let active = self.active_count(dag);
        match overlap {
            Overlap::Allow { max_active } => active < max_active,
            _ => active == 0,
        }
    }

    /// A run of `dag` ended: what was waiting for it may start.
    fn after_run(&mut self, dag: &str) -> Result<()> {
        let Some(row) = self.store.dag(dag)? else {
            return Ok(());
        };
        let loaded = self.load(&row.current, &row.yaml)?;
        let overlap = loaded.dag.overlap(&self.config.defaults);
        while self.has_room(dag, overlap) {
            let Some(id) = self.queued.get_mut(dag).and_then(VecDeque::pop_front) else {
                break;
            };
            if let Err(error) = self.start_queued(&id) {
                if is_storage(&error) {
                    return Err(error);
                }
                // This run cannot start: it fails, with the reason, and
                // the queue moves on.
                let reason = format!("{error:#}");
                error!("queued run {id} of {dag} cannot start: {reason}");
                self.store.transaction(|store| {
                    store.finish_run(&id, "failed", now_ms())?;
                    store.set_problem(&id, &reason)
                })?;
                self.dirty = true;
            }
        }
        self.decide(dag, now_ms())
    }

    // The timetable

    /// The hour of some fire has come: start what is due, and remember
    /// when the next fire is.
    fn fire(&mut self, now_ms: i64) -> Result<()> {
        for dag in self.store.due(now_ms)? {
            let decided = self.decide(&dag, now_ms);
            contain(decided, &format!("the schedule of `{dag}`"))?;
        }
        self.next_cron = self.store.next_fire_after(now_ms)?;
        Ok(())
    }

    /// Asks the DAG's schedule what to do with its due fire, and does it,
    /// until the schedule says to wait.
    fn decide(&mut self, dag: &str, now_ms: i64) -> Result<()> {
        let Some(row) = self.store.dag(dag)? else {
            return Ok(());
        };
        let Some(mut next_fire) = row.next_fire else {
            return Ok(());
        };
        if row.paused || row.deleted_at.is_some() {
            return Ok(());
        }
        let loaded = self.load(&row.current, &row.yaml)?;
        let Some(schedule) = loaded
            .dag
            .schedule(&self.config.defaults, &self.config.dags.default_timezone)?
        else {
            return Ok(());
        };
        loop {
            let decision = schedule.decide(
                Timestamp::from_millis(next_fire),
                Timestamp::from_millis(now_ms),
                self.active_count(dag),
            );
            let following = match decision {
                Decision::Wait => break,
                Decision::Skip {
                    logical_time,
                    next_fire,
                } => {
                    let next_fire = next_fire.map(Timestamp::as_millis);
                    self.store.set_next_fire(dag, next_fire)?;
                    info!(
                        "fire of {dag} at {} skipped: a run is still active",
                        loaded.zone.rfc3339(logical_time.as_millis())
                    );
                    next_fire
                }
                Decision::Start {
                    logical_time,
                    trigger,
                    next_fire,
                    cancel_active,
                } => {
                    if cancel_active {
                        for run in self.active(dag) {
                            self.engine_event(Event::CancelRun { run })?;
                        }
                    }
                    let next_fire = next_fire.map(Timestamp::as_millis);
                    let trigger = match trigger {
                        Trigger::Cron => "cron",
                        Trigger::Catchup => "catchup",
                    };
                    let created = self.create_run(
                        dag,
                        &row.current,
                        Arc::clone(&loaded),
                        logical_time.as_millis(),
                        loaded.dag.params.clone(),
                        Origin::Timetable { trigger, next_fire },
                    );
                    if let Err(error) = created {
                        // For example a run for this fire exists already.
                        // The timetable must move on all the same.
                        warn!("fire of {dag} did not start a run: {error:#}");
                        self.store.set_next_fire(dag, next_fire)?;
                    }
                    next_fire
                }
            };
            match following {
                Some(following) => next_fire = following,
                None => break,
            }
        }
        self.dirty = true;
        Ok(())
    }

    fn refresh_next_cron(&mut self) -> Result<()> {
        let now_ms = now_ms();
        // A row that is already due makes the alarm ring at once.
        self.next_cron = if self.store.due(now_ms)?.is_empty() {
            self.store.next_fire_after(now_ms)?
        } else {
            Some(now_ms)
        };
        Ok(())
    }

    /// Deletes the runs older than `retention`, with their logs.
    fn cleanup(&mut self, now_ms: i64) -> Result<()> {
        let retention =
            i64::try_from(self.config.storage.retention.0.as_millis()).unwrap_or(i64::MAX);
        let deleted = self
            .store
            .delete_runs_before(now_ms.saturating_sub(retention))?;
        for (dag, run) in &deleted {
            self.logs.delete_run(dag, run);
        }
        if !deleted.is_empty() {
            info!("retention: deleted {} old runs", deleted.len());
        }
        self.next_cleanup = now_ms + 86_400_000;
        Ok(())
    }

    // Start of the master

    /// Loads the open runs back from the database and catches up with the
    /// timetable.
    fn restore(&mut self) -> Result<()> {
        for name in self.store.workers()? {
            if name.revoked_at.is_none() {
                self.serials
                    .write()
                    .expect("the serials lock is never poisoned")
                    .insert(name.name, name.cert_serial);
            }
        }
        for row in self.store.open_runs()? {
            if row.state == "queued" {
                self.queued
                    .entry(row.dag_name)
                    .or_default()
                    .push_back(row.id);
                continue;
            }
            if let Err(error) = self.restore_run(&row) {
                error!(
                    "run {} of {} cannot be loaded back: {error:#}",
                    row.id, row.dag_name
                );
                self.store.finish_run(&row.id, "failed", now_ms())?;
            }
        }
        let now_ms = now_ms();
        // Fires missed while the master was down: `catchup` decides.
        self.fire(now_ms)?;
        // And the manual runs that were queued.
        let waiting: Vec<String> = self.queued.keys().cloned().collect();
        self.ended.extend(waiting);
        self.settle()
    }

    fn restore_run(&mut self, row: &RunRow) -> Result<()> {
        let run = parse_run_id(&row.id).context("the id is not a UUID")?;
        let loaded = self.load_version(&row.version_hash)?;
        let rows = self.store.tasks(&row.id)?;
        let attempts = self.store.attempts(&row.id, None)?;
        let mut tasks = Vec::with_capacity(loaded.graph.len());
        let mut running = Vec::new();
        for task in loaded.graph.tasks() {
            let name = loaded.graph.name(task);
            let task_row = rows
                .iter()
                .find(|r| r.task == name)
                .with_context(|| format!("task {name} is missing from the database"))?;
            let state = parse_state(&task_row.state)
                .with_context(|| format!("task {name} has the unknown state {}", task_row.state))?;
            let mine = attempts.iter().filter(|a| a.task == name);
            // Failures since the task last succeeded or was cancelled. A
            // rerun between them is not recorded, so after one a task may
            // have fewer retries left than it should: the safe side.
            let failures = mine
                .clone()
                .rev()
                .take_while(|a| !matches!(a.outcome.as_deref(), Some("success" | "cancelled")))
                .filter(|a| matches!(a.outcome.as_deref(), Some("failed" | "timeout")))
                .count();
            if state == TaskState::Assigned {
                if let Some(attempt) = mine.clone().find(|a| a.finished_at.is_none()) {
                    let worker = self.worker_id(&attempt.worker);
                    running.push((task, worker));
                    self.inflight.insert((run, task), name.to_owned());
                }
            }
            tasks.push(TaskSnapshot {
                state,
                attempts: task_row.attempts,
                failures: u32::try_from(failures).unwrap_or(u32::MAX),
            });
        }
        let params = serde_json::from_str(&row.params).unwrap_or_default();
        let graph = Arc::clone(&loaded.graph);
        self.open_run(run, row, loaded, params);
        info!("run {} of {} loaded back", row.id, row.dag_name);
        self.engine_event(Event::RestoreRun {
            run,
            graph,
            tasks,
            running,
        })?;
        Ok(())
    }

    // Workers

    fn worker_hello(
        &mut self,
        name: &str,
        session: u64,
        hello: proto::Hello,
        tx: mpsc::Sender<proto::MasterMessage>,
    ) -> Result<()> {
        let worker = self.worker_id(name);
        let mut in_flight = Vec::new();
        let mut unknown = Vec::new();
        for task in hello.in_flight {
            let known = parse_run_id(&task.run_id)
                .and_then(|run| Some((run, self.task_id(run, &task.task)?)));
            match known {
                Some((run, id)) => in_flight.push((run, id, task.attempt)),
                None => unknown.push(task),
            }
        }
        let labels = hello.labels;
        let now_ms = now_ms();
        info!(
            "worker {name} connected: {} slots, {} tasks in flight",
            hello.slots,
            in_flight.len()
        );
        self.sessions.insert(
            worker,
            Session {
                number: session,
                tx,
                total_memory: hello.total_memory,
                available_memory: 0,
                free_disk: 0,
                connected_at: now_ms,
                last_heartbeat: now_ms,
            },
        );
        self.engine_event(Event::WorkerJoined {
            worker,
            slots: hello.slots,
            labels,
            in_flight,
        })?;
        // Attempts of runs the master no longer follows.
        for task in unknown {
            let cancel = proto::Cancel { task: Some(task) };
            self.send(worker, message(proto::master_message::Kind::Cancel(cancel)));
        }
        self.dirty = true;
        Ok(())
    }

    /// The worker behind a message, if the message is from its current
    /// session.
    fn current(&self, name: &str, session: u64) -> Option<WorkerId> {
        let worker = *self.worker_ids.get(name)?;
        (self.sessions.get(&worker)?.number == session).then_some(worker)
    }

    fn worker_gone(&mut self, name: &str, session: u64) -> Result<()> {
        let Some(worker) = self.current(name, session) else {
            return Ok(());
        };
        info!("worker {name} disconnected");
        self.sessions.remove(&worker);
        // Give it nothing more, but keep its tasks: it has one heartbeat
        // timeout to come back and say it still runs them.
        self.engine_event(Event::Heartbeat {
            worker,
            accepting: false,
        })?;
        self.dirty = true;
        Ok(())
    }

    fn worker_message(
        &mut self,
        name: &str,
        session: u64,
        kind: proto::worker_message::Kind,
    ) -> Result<()> {
        use proto::worker_message::Kind;
        let Some(worker) = self.current(name, session) else {
            return Ok(());
        };
        match kind {
            // A second Hello on the same stream means nothing.
            Kind::Hello(_) => {}
            Kind::Heartbeat(heartbeat) => {
                if let Some(session) = self.sessions.get_mut(&worker) {
                    session.available_memory = heartbeat.available_memory;
                    session.free_disk = heartbeat.free_disk;
                    session.last_heartbeat = now_ms();
                }
                self.engine_event(Event::Heartbeat {
                    worker,
                    accepting: heartbeat.accepting,
                })?;
            }
            Kind::Ack(ack) => {
                if !ack.accepted {
                    if let Some(task) = ack.task {
                        warn!("worker {name} refused {}: {}", task.task, ack.reason);
                        let finished = proto::Finished {
                            outcome: proto::Outcome::Failed.into(),
                            finished_at: now_ms(),
                            error: ack.reason,
                            ..Default::default()
                        };
                        self.report_finished(worker, &task, &finished)?;
                    }
                }
            }
            Kind::Report(report) => {
                let Some(task) = report.task else {
                    return Ok(());
                };
                match report.kind {
                    Some(proto::report::Kind::Started(started)) => {
                        self.store.start_attempt(
                            &task.run_id,
                            &task.task,
                            task.attempt,
                            started.started_at,
                        )?;
                        self.dirty = true;
                    }
                    Some(proto::report::Kind::Finished(finished)) => {
                        self.report_finished(worker, &task, &finished)?;
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    /// An attempt ended: save how, tell the engine, confirm to the worker.
    fn report_finished(
        &mut self,
        worker: WorkerId,
        task: &proto::TaskRef,
        finished: &proto::Finished,
    ) -> Result<()> {
        let (outcome, for_engine) = match finished.outcome() {
            proto::Outcome::Success => ("success", Outcome::Success),
            proto::Outcome::Timeout => ("timeout", Outcome::Timeout),
            proto::Outcome::Cancelled => ("cancelled", Outcome::Failed),
            proto::Outcome::Lost => ("lost", Outcome::Lost),
            proto::Outcome::Failed | proto::Outcome::Unspecified => ("failed", Outcome::Failed),
        };
        let run = parse_run_id(&task.run_id);
        let dag = match run.and_then(|run| self.open.get(&run)) {
            Some(open) => Some(open.dag.clone()),
            None => self.store.run(&task.run_id)?.map(|row| row.dag_name),
        };
        let stats = dag.and_then(|dag| {
            self.logs.take_stats(&LogKey {
                dag,
                run: task.run_id.clone(),
                task: task.task.clone(),
                attempt: task.attempt,
            })
        });
        if !finished.error.is_empty() {
            warn!(
                "{} attempt {} did not run: {}",
                task.task, task.attempt, finished.error
            );
        }
        let end = AttemptEnd {
            outcome,
            exit_code: finished.exit_code,
            started_at: (finished.started_at > 0).then_some(finished.started_at),
            finished_at: finished.finished_at,
            peak_memory: finished.peak_memory,
            log_bytes: stats.map(|(bytes, _)| bytes),
            log_truncated: stats.is_some_and(|(_, truncated)| truncated),
        };
        self.store.finish_attempt(
            &task.run_id,
            &task.task,
            task.attempt,
            self.worker_name(worker),
            &end,
        )?;
        self.dirty = true;

        if let Some((run, id)) = run.and_then(|run| Some((run, self.task_id(run, &task.task)?))) {
            // The engine confirms, after the task state is saved.
            self.engine_event(Event::Report {
                worker,
                run,
                task: id,
                attempt: task.attempt,
                outcome: for_engine,
            })?;
        } else {
            // A report the master no longer waits for: saved above, so
            // the worker can forget it.
            let confirm = proto::Confirm {
                task: Some(task.clone()),
            };
            self.send(
                worker,
                message(proto::master_message::Kind::Confirm(confirm)),
            );
        }
        Ok(())
    }

    // Requests

    fn call(&mut self, call: Call) -> ApiResult {
        match call {
            Call::Apply { yaml } => self.apply(&yaml),
            Call::Dags => Ok(json!(self.store.dags()?)),
            Call::Dag(name) => self.dag_detail(&name),
            Call::Pause { dag, paused } => self.pause(&dag, paused),
            Call::DeleteDag(name) => {
                self.existing_dag(&name)?;
                self.store.delete_dag(&name, now_ms())?;
                self.refresh_next_cron()?;
                self.dirty = true;
                Ok(json!({ "deleted": name }))
            }
            Call::Trigger { dag, at, params } => self.trigger(&dag, at.as_deref(), &params),
            Call::Runs { dag, limit } => self.runs(dag.as_deref(), limit),
            Call::Run(id) => self.run_detail(&id),
            Call::Cancel(id) => self.cancel(&id),
            Call::Rerun { run, request } => self.rerun(&run, &request),
            Call::LogTarget { run, task, attempt } => self.log_target(&run, &task, attempt),
            Call::Workers => Ok(self.workers()),
            Call::Drain(name) => self.drain(&name),
            Call::RemoveWorker(name) => self.remove_worker(&name),
            Call::NewToken {
                kind,
                label,
                rotate,
                expires,
            } => self.new_token(kind, label.as_deref(), rotate, expires),
            Call::Tokens => self.tokens(),
            Call::RevokeToken(id) => self.revoke_token(&id),
            Call::CheckToken(text) => Ok(json!(self.token_is_valid(&text, TokenKind::Api)?)),
            Call::Join {
                token,
                name,
                csr_pem,
            } => self.join(&token, &name, &csr_pem),
            Call::Renew { name, csr_pem } => self.issue(&name, &csr_pem),
            Call::Status => Ok(self.status()?),
        }
    }

    fn existing_dag(&self, name: &str) -> Result<crate::store::DagRow, ApiError> {
        self.store
            .dag(name)?
            .filter(|dag| dag.deleted_at.is_none())
            .ok_or_else(|| ApiError::not_found(format!("there is no DAG `{name}`")))
    }

    fn existing_run(&self, id: &str) -> Result<RunRow, ApiError> {
        self.store
            .run(id)?
            .ok_or_else(|| ApiError::not_found(format!("there is no run `{id}`")))
    }

    fn apply(&mut self, yaml: &str) -> ApiResult {
        let dag = Dag::parse(yaml).map_err(|e| ApiError::bad_request(&e))?;
        let schedule = dag
            .schedule(&self.config.defaults, &self.config.dags.default_timezone)
            .map_err(|e| ApiError::bad_request(&e))?;
        let hash = dag.hash();
        let before = self.store.dag(&dag.name)?;
        let paused = before.as_ref().is_some_and(|row| row.paused);
        let had_row = before.as_ref().is_some_and(|row| row.next_fire.is_some());
        let next_fire = schedule
            .filter(|_| !paused)
            .and_then(|schedule| schedule.next_fire(now()))
            .map(Timestamp::as_millis);
        let new_version = self.store.transaction(|store| {
            let new_version =
                store.apply_dag(&dag.name, &hash, yaml, &dag.structure(), now_ms())?;
            // A row that is waiting for a run to end keeps its fire,
            // unless the definition changed.
            if next_fire.is_none() || new_version || !had_row {
                store.set_next_fire(&dag.name, next_fire)?;
            }
            Ok(new_version)
        })?;
        self.refresh_next_cron()?;
        self.dirty = true;
        info!(
            "applied {} ({})",
            dag.name,
            if new_version {
                "new version"
            } else {
                "unchanged"
            }
        );
        Ok(json!({
            "name": dag.name,
            "hash": hash,
            "new_version": new_version,
            "next_fire": self.store.dag(&dag.name)?.and_then(|row| row.next_fire),
        }))
    }

    fn pause(&mut self, dag: &str, paused: bool) -> ApiResult {
        let row = self.existing_dag(dag)?;
        let next_fire = if paused {
            None
        } else {
            // The fires of the pause are not caught up: from now on.
            let loaded = self.load(&row.current, &row.yaml)?;
            loaded
                .dag
                .schedule(&self.config.defaults, &self.config.dags.default_timezone)?
                .and_then(|schedule| schedule.next_fire(now()))
                .map(Timestamp::as_millis)
        };
        self.store.transaction(|store| {
            store.set_paused(dag, paused)?;
            store.set_next_fire(dag, next_fire)
        })?;
        self.refresh_next_cron()?;
        self.dirty = true;
        Ok(json!({ "name": dag, "paused": paused, "next_fire": next_fire }))
    }

    fn dag_detail(&mut self, name: &str) -> ApiResult {
        let row = self.existing_dag(name)?;
        let loaded = self.load(&row.current, &row.yaml)?;
        Ok(json!({
            "dag": row,
            "yaml": row.yaml,
            "definition": loaded.dag,
            "active_runs": self.active_count(name),
            "queued_runs": self.queued.get(name).map_or(0, VecDeque::len),
        }))
    }

    fn trigger(
        &mut self,
        dag: &str,
        at: Option<&str>,
        overrides: &BTreeMap<String, String>,
    ) -> ApiResult {
        let row = self.existing_dag(dag)?;
        let loaded = self.load(&row.current, &row.yaml)?;
        let params = loaded
            .dag
            .resolve_params(overrides)
            .map_err(|e| ApiError::bad_request(&e))?;
        let logical_time = match at {
            Some(at) => {
                parse_instant(at, loaded.zone.inner()).map_err(|e| ApiError::bad_request(&e))?
            }
            None => now_ms(),
        };

        let overlap = loaded.dag.overlap(&self.config.defaults);
        let waiting = self.queued.get(dag).is_some_and(|queue| !queue.is_empty());
        if !self.has_room(dag, overlap) || waiting {
            match overlap {
                Overlap::Skip => {
                    return Err(ApiError::conflict(format!(
                        "a run of `{dag}` is active and its overlap rule is `skip`"
                    )));
                }
                Overlap::Replace => {
                    for run in self.active(dag) {
                        self.engine_event(Event::CancelRun { run })?;
                    }
                }
                Overlap::Queue | Overlap::Allow { .. } => {
                    let id = run_id_text(crate::util::new_run_id());
                    self.store.insert_run(&RunRow {
                        id: id.clone(),
                        dag_name: dag.to_owned(),
                        version_hash: row.current.clone(),
                        logical_time,
                        trigger: "manual".to_owned(),
                        params: serde_json::to_string(&params).map_err(anyhow::Error::from)?,
                        state: "queued".to_owned(),
                        created_at: now_ms(),
                        started_at: None,
                        finished_at: None,
                        problem: None,
                    })?;
                    self.queued
                        .entry(dag.to_owned())
                        .or_default()
                        .push_back(id.clone());
                    self.dirty = true;
                    return Ok(json!({ "id": id, "state": "queued" }));
                }
            }
        }
        let id = self.create_run(
            dag,
            &row.current,
            loaded,
            logical_time,
            params,
            Origin::Manual,
        )?;
        Ok(json!({ "id": id, "state": "running" }))
    }

    /// The latest runs, each with the counts of its tasks.
    fn runs(&self, dag: Option<&str>, limit: u32) -> ApiResult {
        let mut runs = Vec::new();
        for row in self.store.runs(dag, limit)? {
            let (total, success, failed, active) = self.store.task_counts(&row.id)?;
            let mut run = json!(row);
            run["tasks"] = json!({
                "total": total,
                "success": success,
                "failed": failed,
                "active": active,
            });
            runs.push(run);
        }
        Ok(json!(runs))
    }

    fn run_detail(&mut self, id: &str) -> ApiResult {
        let row = self.existing_run(id)?;
        let tasks = self.store.tasks(&row.id)?;
        let attempts = self.store.attempts(&row.id, None)?;
        // The graph as it was for this run, from its own version.
        let loaded = self.load_version(&row.version_hash).ok();
        let run = parse_run_id(&row.id);
        let waiting: std::collections::HashSet<(RunId, TaskId)> =
            self.engine.waiting().into_iter().collect();
        let tasks: Vec<Value> = match &loaded {
            Some(loaded) => loaded
                .dag
                .tasks
                .iter()
                .map(|def| {
                    let saved = tasks.iter().find(|t| t.task == def.name);
                    let worker = run
                        .zip(loaded.ids.get(&def.name))
                        .and_then(|(run, &task)| self.engine.running(run, task))
                        .map(|(worker, _)| self.worker_name(worker).to_owned());
                    // Ready, and no worker takes it: say why.
                    let waiting = run
                        .zip(loaded.ids.get(&def.name))
                        .filter(|&(run, &task)| waiting.contains(&(run, task)))
                        .map(|(run, &task)| {
                            self.engine.blocker(run, task).map_or_else(
                                || "a worker should take it now".to_owned(),
                                |b| blocker_text(&b),
                            )
                        });
                    json!({
                        "task": def.name,
                        "waiting": waiting,
                        "needs": def.needs,
                        "command": def.run,
                        // A queued run has no task rows yet.
                        "state": saved.map_or("blocked", |t| t.state.as_str()),
                        "attempts": saved.map_or(0, |t| t.attempts),
                        "worker": worker,
                    })
                })
                .collect(),
            None => tasks.iter().map(|t| json!(t)).collect(),
        };
        Ok(json!({ "run": row, "tasks": tasks, "attempts": attempts }))
    }

    fn cancel(&mut self, id: &str) -> ApiResult {
        let row = self.existing_run(id)?;
        match (row.state.as_str(), parse_run_id(&row.id)) {
            ("running", Some(run)) if self.open.contains_key(&run) => {
                self.engine_event(Event::CancelRun { run })?;
            }
            ("queued", _) => {
                if let Some(queue) = self.queued.get_mut(&row.dag_name) {
                    queue.retain(|queued| *queued != row.id);
                }
                self.dirty = true;
                if let Some(rerun) = self.store.rerun_request(&row.id)? {
                    // A finished run that waited to run again stays as it
                    // was.
                    let was = serde_json::from_str::<QueuedRerun>(&rerun)
                        .map_or_else(|_| "failed".to_owned(), |queued| queued.was);
                    self.store.drop_rerun(&row.id, &was)?;
                    return Ok(json!({ "id": row.id, "state": was }));
                }
                self.store.finish_run(&row.id, "cancelled", now_ms())?;
            }
            _ => {
                return Err(ApiError::conflict(format!(
                    "run {} is already {}",
                    row.id, row.state
                )));
            }
        }
        Ok(json!({ "id": row.id, "state": "cancelled" }))
    }

    fn rerun(&mut self, id: &str, request: &RerunRequest) -> ApiResult {
        let mut row = self.existing_run(id)?;
        if matches!(row.state.as_str(), "queued" | "running") {
            return Err(ApiError::conflict(format!(
                "run {} is {}: only a finished run can be run again",
                row.id, row.state
            )));
        }
        row.version_hash = self.rerun_version(&row, request)?;
        let loaded = self.load_version(&row.version_hash)?;
        let scope = rerun_scope(&row, &loaded, request)?;

        // A reopened run follows the overlap rule like a new one.
        let overlap = loaded.dag.overlap(&self.config.defaults);
        let dag = row.dag_name.clone();
        let waiting = self.queued.get(&dag).is_some_and(|queue| !queue.is_empty());
        if !self.has_room(&dag, overlap) || waiting {
            match overlap {
                Overlap::Skip => {
                    return Err(ApiError::conflict(format!(
                        "a run of `{dag}` is active and its overlap rule is `skip`"
                    )));
                }
                Overlap::Replace => {
                    for active in self.active(&dag) {
                        self.engine_event(Event::CancelRun { run: active })?;
                    }
                }
                Overlap::Queue | Overlap::Allow { .. } => {
                    let queued = QueuedRerun {
                        task: request.task.clone(),
                        all: request.all,
                        only: request.only,
                        version: row.version_hash.clone(),
                        was: row.state.clone(),
                    };
                    let queued = serde_json::to_string(&queued).map_err(anyhow::Error::from)?;
                    self.store.queue_rerun(&row.id, &queued)?;
                    self.queued
                        .entry(dag)
                        .or_default()
                        .push_back(row.id.clone());
                    self.dirty = true;
                    info!("run {} of {} waits to run again", row.id, row.dag_name);
                    return Ok(json!({ "id": row.id, "state": "queued" }));
                }
            }
        }
        self.reopen(&row, loaded, scope)
    }

    /// The version a rerun runs with: the run's own, or with `current` the
    /// DAG's current one if it has the same tasks and dependencies.
    fn rerun_version(&self, row: &RunRow, request: &RerunRequest) -> Result<String, ApiError> {
        if !request.current {
            return Ok(row.version_hash.clone());
        }
        let dag = self.existing_dag(&row.dag_name)?;
        if dag.current == row.version_hash {
            return Ok(dag.current);
        }
        let old = self
            .store
            .version(&row.version_hash)?
            .context("the run's version is gone")?;
        let new = self
            .store
            .version(&dag.current)?
            .context("the current version is gone")?;
        if old.structure != new.structure {
            return Err(ApiError::conflict(
                "the current version has different tasks or dependencies: start a new run",
            ));
        }
        Ok(dag.current)
    }

    /// A rerun that waited in the queue starts now, or goes back to the
    /// state of its run if there is nothing left to run.
    fn start_queued_rerun(&mut self, mut row: RunRow, text: &str) -> Result<()> {
        let queued: QueuedRerun =
            serde_json::from_str(text).context("the queued rerun cannot be read")?;
        let request = RerunRequest {
            task: queued.task.clone(),
            all: queued.all,
            only: queued.only,
            current: false,
        };
        row.version_hash.clone_from(&queued.version);
        let started = self
            .load_version(&row.version_hash)
            .map_err(ApiError::from)
            .and_then(|loaded| {
                let scope = rerun_scope(&row, &loaded, &request)?;
                self.reopen(&row, loaded, scope)
            });
        match started {
            Ok(_) => Ok(()),
            Err(error) if error.status >= 500 => Err(anyhow::anyhow!(error.message)),
            Err(error) => {
                warn!(
                    "run {} of {} does not run again: {}",
                    row.id, row.dag_name, error.message
                );
                self.store.drop_rerun(&row.id, &queued.was)?;
                self.dirty = true;
                Ok(())
            }
        }
    }

    /// Reopens a finished run on `row.version_hash` and starts what
    /// `scope` selects.
    fn reopen(&mut self, row: &RunRow, loaded: Arc<Loaded>, scope: Rerun) -> ApiResult {
        let run = parse_run_id(&row.id).context("the id is not a UUID")?;
        let saved = self.store.tasks(&row.id)?;
        let mut tasks = Vec::with_capacity(loaded.graph.len());
        for task in loaded.graph.tasks() {
            let name = loaded.graph.name(task);
            let state = saved
                .iter()
                .find(|t| t.task == name)
                .and_then(|t| Some((parse_state(&t.state)?, t.attempts)));
            let Some((state, attempts)) = state else {
                return Err(ApiError::conflict(format!(
                    "run {} has no task `{name}`: it never started",
                    row.id
                )));
            };
            tasks.push(TaskSnapshot {
                state,
                attempts,
                failures: 0,
            });
        }
        let graph = Arc::clone(&loaded.graph);
        let params = serde_json::from_str(&row.params).unwrap_or_default();
        self.open_run(run, row, loaded, params);
        let any = self.engine_event(Event::RerunRun {
            run,
            graph,
            tasks,
            scope,
        })?;
        if !any {
            self.open.remove(&run);
            return Err(ApiError::conflict(
                "nothing to run again: no task matches, or what it depends on did not succeed",
            ));
        }
        info!("run {} of {} reopened", row.id, row.dag_name);
        Ok(json!({ "id": row.id, "state": "running" }))
    }

    fn log_target(&self, run: &str, task: &str, attempt: Option<u32>) -> ApiResult {
        let row = self.existing_run(run)?;
        let attempts = self.store.attempts(&row.id, Some(task))?;
        let chosen = match attempt {
            Some(number) => attempts.iter().find(|a| a.attempt == number),
            None => attempts.last(),
        };
        let Some(chosen) = chosen else {
            return Err(ApiError::not_found(format!(
                "task `{task}` of run {} has no such attempt",
                row.id
            )));
        };
        Ok(json!({
            "dag": row.dag_name,
            "run": row.id,
            "task": task,
            "attempt": chosen.attempt,
            "attempts": attempts.len(),
            "finished": chosen.finished_at.is_some(),
        }))
    }

    fn workers(&self) -> Value {
        let now_ms = now_ms();
        let connected: Vec<Value> = self
            .engine
            .workers()
            .iter()
            .filter_map(|(id, worker)| {
                let session = self.sessions.get(&id)?;
                let labels = worker.labels();
                Some(json!({
                    "name": self.worker_name(id),
                    "slots": worker.slots(),
                    "used": worker.used(),
                    "accepting": worker.accepting(),
                    "labels": labels,
                    "total_memory": session.total_memory,
                    "available_memory": session.available_memory,
                    "free_disk": session.free_disk,
                    "connected_for_ms": now_ms - session.connected_at,
                    "heartbeat_age_ms": now_ms - session.last_heartbeat,
                }))
            })
            .collect();
        json!(connected)
    }

    fn session_of(&self, name: &str) -> Result<WorkerId, ApiError> {
        self.worker_ids
            .get(name)
            .copied()
            .filter(|id| self.sessions.contains_key(id))
            .ok_or_else(|| ApiError::not_found(format!("no worker `{name}` is connected")))
    }

    fn drain(&mut self, name: &str) -> ApiResult {
        let worker = self.session_of(name)?;
        self.send(
            worker,
            message(proto::master_message::Kind::Drain(proto::Drain {})),
        );
        // Stop choosing it at once, without waiting for its heartbeat.
        self.engine_event(Event::Heartbeat {
            worker,
            accepting: false,
        })?;
        self.dirty = true;
        Ok(json!({ "name": name, "accepting": false }))
    }

    fn remove_worker(&mut self, name: &str) -> ApiResult {
        let revoked = self.store.revoke_worker(name, now_ms())?;
        self.serials
            .write()
            .expect("the serials lock is never poisoned")
            .remove(name);
        let connected = self.session_of(name).ok();
        if let Some(worker) = connected {
            // Closing the stream is enough: its certificate is no longer
            // accepted, and its tasks are queued again when it stays silent.
            self.sessions.remove(&worker);
            self.engine_event(Event::Heartbeat {
                worker,
                accepting: false,
            })?;
        }
        if !revoked && connected.is_none() {
            return Err(ApiError::not_found(format!("there is no worker `{name}`")));
        }
        self.dirty = true;
        info!("worker {name} removed");
        Ok(json!({ "name": name, "removed": true }))
    }

    // Tokens and certificates

    fn ca(&self) -> Result<&Arc<Ca>, ApiError> {
        self.ca.as_ref().ok_or_else(|| {
            ApiError::conflict(
                "this master runs without TLS (standalone): it has no tokens or certificates",
            )
        })
    }

    fn token_is_valid(&self, text: &str, kind: TokenKind) -> Result<bool> {
        let Ok(token) = Token::parse(text) else {
            return Ok(false);
        };
        let Some(row) = self.store.token(&token.id)? else {
            return Ok(false);
        };
        Ok(row.kind == kind.as_str()
            && row.revoked_at.is_none()
            && row.expires_at.is_none_or(|at| at > now_ms())
            && crate::util::same_secret(&row.secret_hash, &token.secret_hash()))
    }

    fn join_token_path(&self) -> PathBuf {
        self.data_dir.join("join.token")
    }

    fn tokens(&self) -> ApiResult {
        self.ca()?;
        let now = now_ms();
        let tokens: Vec<Value> = self
            .store
            .tokens()?
            .into_iter()
            .map(|token| {
                let state = if token.revoked_at.is_some() {
                    "revoked"
                } else if token.expires_at.is_some_and(|at| at <= now) {
                    "expired"
                } else {
                    "valid"
                };
                json!({
                    "id": token.id,
                    "kind": token.kind,
                    "label": token.label,
                    "created_at": token.created_at,
                    "expires_at": token.expires_at,
                    "state": state,
                })
            })
            .collect();
        Ok(json!(tokens))
    }

    fn revoke_token(&mut self, id: &str) -> ApiResult {
        self.ca()?;
        if !self.store.revoke_token(id, now_ms())? {
            return Err(ApiError::not_found(format!(
                "there is no valid token `{id}`: `lwwo token list` shows them"
            )));
        }
        info!("token {id} revoked");
        Ok(json!({ "id": id, "state": "revoked" }))
    }

    fn new_token(
        &mut self,
        kind: TokenKind,
        label: Option<&str>,
        rotate: bool,
        expires: Option<Duration>,
    ) -> ApiResult {
        let fingerprint = self.ca()?.fingerprint()?;
        if kind == TokenKind::Join && !rotate {
            // The join token is the same for every worker: hand out the
            // one there is.
            if let Ok(text) = std::fs::read_to_string(self.join_token_path()) {
                if self.token_is_valid(&text, TokenKind::Join)? {
                    return Ok(json!({ "token": text.trim() }));
                }
            }
        }
        let token = Token::generate(&fingerprint);
        self.store.transaction(|store| {
            if kind == TokenKind::Join {
                store.revoke_tokens("join", now_ms())?;
            }
            store.insert_token(
                &token.id,
                kind.as_str(),
                label,
                &token.secret_hash(),
                now_ms(),
                expires.map(|after| {
                    now_ms().saturating_add(i64::try_from(after.as_millis()).unwrap_or(i64::MAX))
                }),
            )
        })?;
        if kind == TokenKind::Join {
            crate::security::write_private(&self.join_token_path(), &token.text())?;
        }
        Ok(json!({ "token": token.text() }))
    }

    /// Makes sure the first tokens exist: without an API token nobody
    /// could ask for one. Returns the path of the admin token if it was
    /// created now.
    pub(crate) fn bootstrap_tokens(&mut self) -> Result<Option<PathBuf>> {
        if self.ca.is_none() {
            return Ok(None);
        }
        let to_anyhow = |error: ApiError| anyhow::anyhow!(error.message);
        self.new_token(TokenKind::Join, None, false, None)
            .map_err(to_anyhow)?;
        if self.store.has_token("api")? {
            return Ok(None);
        }
        let token = self
            .new_token(
                TokenKind::Api,
                Some("admin, created at the first start"),
                false,
                None,
            )
            .map_err(to_anyhow)?;
        let path = self.data_dir.join("admin.token");
        crate::security::write_private(&path, token["token"].as_str().unwrap_or_default())?;
        Ok(Some(path))
    }

    fn join(&mut self, token: &str, name: &str, csr_pem: &str) -> ApiResult {
        if !self.token_is_valid(token, TokenKind::Join)? {
            return Err(ApiError::new(
                401,
                "the join token is not valid: it may have been rotated",
            ));
        }
        crate::security::check_worker_name(name).map_err(|e| ApiError::bad_request(&e))?;
        // The join token is shared by every worker: by itself it must not
        // be enough to take the name, and so the tasks, of another one.
        // Freeing a name takes an API token: `lwwo worker remove`.
        let name_in_use = self
            .store
            .workers()?
            .into_iter()
            .any(|worker| worker.name == name && worker.revoked_at.is_none());
        if name_in_use {
            return Err(ApiError::conflict(format!(
                "a worker named `{name}` is already registered. If this machine replaces it, \
                 remove the old one first: lwwo worker remove {name}"
            )));
        }
        self.issue(name, csr_pem)
    }

    /// Signs a certificate for a worker and makes it the only valid one
    /// for that name.
    fn issue(&mut self, name: &str, csr_pem: &str) -> ApiResult {
        let ca = Arc::clone(self.ca()?);
        let issued = ca
            .sign_worker(csr_pem, name, self.config.workers.cert_lifetime.0)
            .map_err(|e| ApiError::bad_request(&e))?;
        self.store.save_worker(name, &issued.serial, now_ms())?;
        self.serials
            .write()
            .expect("the serials lock is never poisoned")
            .insert(name.to_owned(), issued.serial.clone());
        info!("certificate {} issued to worker {name}", issued.serial);
        Ok(json!({ "certificate_pem": issued.cert_pem, "ca_pem": ca.cert_pem() }))
    }

    fn status(&self) -> Result<Value> {
        let (mut slots, mut used) = (0, 0);
        for (_, worker) in self.engine.workers().iter() {
            slots += worker.slots();
            used += worker.used();
        }
        Ok(json!({
            "version": env!("CARGO_PKG_VERSION"),
            "workers": self.sessions.len(),
            "slots": slots,
            "slots_used": used,
            "active_runs": self.open.len(),
            "queued_runs": self.queued.values().map(VecDeque::len).sum::<usize>(),
            "dags": self.store.dags()?.len(),
            "next_fire": self.next_cron,
        }))
    }
}

/// Sends a request to the master's loop and waits for its answer.
pub(crate) async fn call(tx: &mpsc::Sender<Msg>, call: Call) -> ApiResult {
    let (reply, answer) = oneshot::channel();
    let gone = || ApiError::new(503, "the master is shutting down");
    tx.send(Msg::Call(call, reply)).await.map_err(|_| gone())?;
    answer.await.map_err(|_| gone())?
}

/// Whether an error comes from the database. Those stop the master: a
/// master that cannot write cannot promise anything. Any other error is
/// about one DAG or one run, and stops only that.
/// Which tasks a rerun selects.
fn rerun_scope(row: &RunRow, loaded: &Loaded, request: &RerunRequest) -> Result<Rerun, ApiError> {
    Ok(match (&request.task, request.all) {
        (Some(name), _) => {
            let task = *loaded.ids.get(name).ok_or_else(|| {
                ApiError::not_found(format!("run {} has no task `{name}`", row.id))
            })?;
            Rerun::Task {
                task,
                downstream: !request.only,
            }
        }
        (None, true) => Rerun::All,
        (None, false) => Rerun::Failed,
    })
}

fn is_storage(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(<dyn std::error::Error>::is::<rusqlite::Error>)
}

/// Keeps an error about one DAG or run from stopping the master: it goes
/// to the log, and the rest goes on. Database errors still stop it.
fn contain(result: Result<()>, what: &str) -> Result<()> {
    match result {
        Err(error) if !is_storage(&error) => {
            error!("{what} cannot be handled: {error:#}. Everything else goes on.");
            Ok(())
        }
        other => other,
    }
}

/// Why a task finds no worker, in words.
fn blocker_text(blocker: &Blocker) -> String {
    match blocker {
        Blocker::NoWorkers => "no worker is connected".to_owned(),
        Blocker::MissingLabels(labels) => match labels.as_slice() {
            [label] => format!("no connected worker has the label \"{label}\""),
            labels => format!(
                "no connected worker has all the labels {}",
                labels
                    .iter()
                    .map(|label| format!("\"{label}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        },
        Blocker::TooFewSlots { needed, largest } => {
            format!("it needs {needed} slots and the largest worker that fits has {largest}")
        }
        Blocker::NotAccepting => {
            "the workers that fit take nothing new: they drain, or are short of disk or memory"
                .to_owned()
        }
        Blocker::Busy => "the workers that fit are full".to_owned(),
    }
}

/// The `Assign` message of one attempt, with its templates rendered.
fn assignment(
    open: &OpenRun,
    task: TaskId,
    attempt: u32,
    timeout: Option<std::time::Duration>,
) -> Result<proto::Assign> {
    let def = &open.loaded.dag.tasks[task.index()];
    let run_time = open.loaded.zone.rfc3339(open.logical_time);
    let run_date = open.loaded.zone.date(open.logical_time);
    let rendered = RunContext {
        dag: &open.dag,
        run_id: &open.id,
        run_time: &run_time,
        run_date: &run_date,
        params: &open.params,
    }
    .render(def, attempt)?;
    Ok(proto::Assign {
        task: Some(proto::TaskRef {
            run_id: open.id.clone(),
            task: def.name.clone(),
            attempt,
        }),
        dag: open.dag.clone(),
        command: rendered.command,
        env: rendered.env.into_iter().collect(),
        timeout_ms: timeout.map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX)),
        slots: def.slots,
    })
}
