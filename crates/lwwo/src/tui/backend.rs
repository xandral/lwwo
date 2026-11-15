//! What the TUI needs from the outside world.
//!
//! The screens only know this trait and the plain structs below, never the
//! HTTP API. [`Remote`](super::remote::Remote) implements it on top of a
//! master; [`Demo`](super::demo::Demo) implements it with made-up data, for
//! `lwwo tui --demo` and for the tests, which press keys and look at the
//! screen without any master.

use crate::client::LogLine;

/// The error text is shown to the user as it is.
pub(crate) type Result<T> = std::result::Result<T, String>;

/// The state of a run, as in `dag_runs.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunState {
    Queued,
    Running,
    Success,
    Failed,
    Cancelled,
}

impl RunState {
    /// Reads the text of the database; anything unknown counts as queued.
    pub(crate) fn parse(text: &str) -> Self {
        match text {
            "running" => Self::Running,
            "success" => Self::Success,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Queued,
        }
    }

    /// Whether the run is over.
    pub(crate) fn is_final(self) -> bool {
        matches!(self, Self::Success | Self::Failed | Self::Cancelled)
    }

    /// The word used in sentences.
    pub(crate) fn word(self) -> &'static str {
        match self {
            Self::Queued => "is queued",
            Self::Running => "is running",
            Self::Success => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "was cancelled",
        }
    }
}

/// The state of a task in a run, as in `task_runs.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskState {
    Blocked,
    Assigned,
    Retrying,
    Success,
    Failed,
    Skipped,
    Cancelled,
}

impl TaskState {
    /// Reads the text of the database; anything unknown counts as blocked.
    pub(crate) fn parse(text: &str) -> Self {
        match text {
            "assigned" => Self::Assigned,
            "retrying" => Self::Retrying,
            "success" => Self::Success,
            "failed" => Self::Failed,
            "skipped" => Self::Skipped,
            "cancelled" => Self::Cancelled,
            _ => Self::Blocked,
        }
    }

    /// Whether the task will not change any more in this run.
    pub(crate) fn is_final(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }
}

/// The line at the top: the master and its workers at a glance.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Status {
    pub(crate) address: String,
    pub(crate) workers: u32,
    pub(crate) slots: u32,
    pub(crate) slots_used: u32,
    pub(crate) active_runs: u32,
    pub(crate) queued_runs: u32,
    /// The next cron fire, UTC milliseconds.
    pub(crate) next_fire: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DagRow {
    pub(crate) name: String,
    pub(crate) paused: bool,
    pub(crate) next_fire: Option<i64>,
    /// The beginning of the hash of the current version.
    pub(crate) version: String,
}

/// How many tasks of a run are in each group of states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Counts {
    pub(crate) total: u32,
    pub(crate) succeeded: u32,
    pub(crate) failed: u32,
    /// On a worker right now.
    pub(crate) active: u32,
}

/// A run as shown in a list. Times are UTC milliseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunRow {
    pub(crate) id: String,
    pub(crate) dag: String,
    pub(crate) state: RunState,
    /// `cron`, `catchup` or `manual`.
    pub(crate) trigger: String,
    pub(crate) tasks: Counts,
    /// The instant the run is about: its fire, or `--at`.
    pub(crate) logical_time: i64,
    pub(crate) created_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) finished_at: Option<i64>,
}

/// One run with its tasks; or a DAG seen without running it, in which case
/// the tasks are as a new run would start them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunView {
    pub(crate) run: RunRow,
    /// The parameters of the run, as JSON text.
    pub(crate) params: String,
    /// Why lwwo stopped the run, if it did.
    pub(crate) problem: Option<String>,
    /// In name order; the other fields refer to tasks by position here.
    pub(crate) tasks: Vec<TaskRow>,
}

impl RunView {
    /// Tasks grouped by depth in the dependency graph, roots first: the
    /// level of a task is one more than the deepest task it needs.
    pub(crate) fn levels(&self) -> Vec<Vec<usize>> {
        let mut depth = vec![0usize; self.tasks.len()];
        // Needs may come in any order: repeat until nothing moves, which
        // takes at most as many rounds as there are tasks.
        for _ in 0..self.tasks.len() {
            let mut moved = false;
            for (index, task) in self.tasks.iter().enumerate() {
                let wanted = task
                    .needs
                    .iter()
                    .filter_map(|&need| depth.get(need))
                    .map(|below| below + 1)
                    .max()
                    .unwrap_or(0);
                if wanted > depth[index] {
                    depth[index] = wanted;
                    moved = true;
                }
            }
            if !moved {
                break;
            }
        }
        let mut levels = vec![Vec::new(); depth.iter().max().map_or(0, |deepest| deepest + 1)];
        for (index, &level) in depth.iter().enumerate() {
            levels[level].push(index);
        }
        levels
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskRow {
    pub(crate) name: String,
    pub(crate) state: TaskState,
    /// The command, as written in the DAG.
    pub(crate) command: String,
    /// The image it runs in, if it is a container.
    pub(crate) image: Option<String>,
    /// The tasks this one waits for, as positions in the run's tasks.
    pub(crate) needs: Vec<usize>,
    /// Ready to run, but no worker takes it: why, in words.
    pub(crate) waiting: Option<String>,
    /// Oldest first.
    pub(crate) attempts: Vec<AttemptRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttemptRow {
    /// 1 for the first attempt.
    pub(crate) number: u32,
    /// `success`, `failed`, `timeout`, `lost` or `cancelled`; `None` while
    /// it runs.
    pub(crate) outcome: Option<String>,
    pub(crate) worker: String,
    pub(crate) exit_code: Option<i64>,
    pub(crate) started_at: Option<i64>,
    pub(crate) finished_at: Option<i64>,
    pub(crate) peak_memory: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerRow {
    pub(crate) name: String,
    /// False while it drains, or when its disk or memory are almost full.
    pub(crate) accepting: bool,
    pub(crate) slots: u32,
    pub(crate) used: u32,
    pub(crate) labels: Vec<String>,
    pub(crate) executors: Vec<String>,
    pub(crate) available_memory: u64,
    pub(crate) free_disk: u64,
    /// How long ago it last said it was alive, in milliseconds.
    pub(crate) heartbeat_age_ms: i64,
}

/// Which tasks of a finished run to run again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Rerun {
    /// The tasks that did not succeed.
    Failed,
    /// One task and everything that depends on it.
    Task(String),
    /// Every task.
    All,
}

pub(crate) trait Backend {
    fn status(&mut self) -> Result<Status>;

    /// Every DAG, by name.
    fn dags(&mut self) -> Result<Vec<DagRow>>;

    /// The most recent runs, newest first, of one DAG or of all.
    fn runs(&mut self, dag: Option<&str>, limit: usize) -> Result<Vec<RunRow>>;

    /// One run with its tasks and attempts.
    fn run(&mut self, id: &str) -> Result<RunView>;

    /// The tasks of the current version of a DAG, as a new run would
    /// start them.
    fn dag(&mut self, name: &str) -> Result<RunView>;

    /// The connected workers, by name.
    fn workers(&mut self) -> Result<Vec<WorkerRow>>;

    /// The lines of the log of an attempt from line `from` on. Called again
    /// and again while the log is on screen: it returns what is new.
    fn log(&mut self, run: &str, task: &str, attempt: u32, from: usize) -> Result<Vec<LogLine>>;

    /// Starts a run of a DAG, with parameters as (name, value).
    fn start(&mut self, dag: &str, params: &[(String, String)]) -> Result<String>;

    fn cancel(&mut self, run: &str) -> Result<()>;

    fn rerun(&mut self, run: &str, what: &Rerun) -> Result<()>;

    /// Stops or restarts the schedule of a DAG.
    fn pause(&mut self, dag: &str, paused: bool) -> Result<()>;

    /// Tells a worker to finish what it has and take nothing more.
    fn drain(&mut self, worker: &str) -> Result<()>;

    /// Whether something changed on the master since the last call: the
    /// screen is then read again without waiting for the next refresh.
    fn changed(&mut self) -> bool;
}
