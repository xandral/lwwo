//! A made-up master, for `lwwo tui --demo` and for the tests.
//!
//! It holds a few DAGs, runs and workers in memory and answers like the
//! real thing, including the actions: starting, stopping, running again.
//! Nothing moves by itself, so a test sees the same screen every time.

use super::backend::{
    AttemptRow, Backend, Counts, DagRow, Rerun, Result, RunRow, RunState, RunView, Status, TaskRow,
    TaskState, WorkerRow,
};
use crate::client::LogLine;

const MINUTE: i64 = 60_000;

pub(crate) struct Demo {
    /// The instant everything is dated from (unix ms).
    now: i64,
    dags: Vec<DagRow>,
    runs: Vec<RunView>,
    workers: Vec<WorkerRow>,
    changed: bool,
}

/// A task of the demo: name, what it needs, its state, its attempts as
/// outcomes (`None` for one still running).
type Sketch = (
    &'static str,
    &'static [&'static str],
    TaskState,
    &'static [Option<&'static str>],
);

fn tasks(sketch: &[Sketch], now: i64) -> Vec<TaskRow> {
    let position = |name: &str| sketch.iter().position(|task| task.0 == name);
    sketch
        .iter()
        .map(|&(name, needs, state, attempts)| TaskRow {
            name: name.to_owned(),
            state,
            command: format!("python {name}.py --day {{{{ run.date }}}}"),
            waiting: None,
            needs: needs.iter().filter_map(|need| position(need)).collect(),
            attempts: attempts
                .iter()
                .zip(1..)
                .map(|(outcome, number)| AttemptRow {
                    number,
                    outcome: outcome.map(str::to_owned),
                    worker: if number % 2 == 0 { "w2" } else { "w1" }.to_owned(),
                    exit_code: outcome.map(|outcome| i64::from(outcome != "success")),
                    started_at: Some(now - 4 * MINUTE),
                    finished_at: outcome.map(|_| now - 3 * MINUTE),
                    peak_memory: outcome.map(|_| 180 << 20),
                })
                .collect(),
        })
        .collect()
}

fn counts(tasks: &[TaskRow]) -> Counts {
    let count = |wanted: TaskState| {
        u32::try_from(tasks.iter().filter(|task| task.state == wanted).count()).unwrap_or(0)
    };
    Counts {
        total: u32::try_from(tasks.len()).unwrap_or(0),
        succeeded: count(TaskState::Success),
        failed: count(TaskState::Failed),
        active: count(TaskState::Assigned),
    }
}

/// The DAG of the examples: a nightly ETL with a model on top.
fn etl(
    state: [TaskState; 8],
    attempts: [&'static [Option<&'static str>]; 8],
    now: i64,
) -> Vec<TaskRow> {
    let shape: [(&str, &[&str]); 8] = [
        ("clean", &["extract"]),
        ("check", &["extract"]),
        ("enrich", &["extract"]),
        ("extract", &[]),
        ("model-a", &["clean", "enrich"]),
        ("model-b", &["clean", "enrich"]),
        ("publish", &["report"]),
        ("report", &["check", "model-a", "model-b"]),
    ];
    let sketch: Vec<Sketch> = shape
        .iter()
        .zip(state)
        .zip(attempts)
        .map(|((&(name, needs), state), attempts)| (name, needs, state, attempts))
        .collect();
    tasks(&sketch, now)
}

impl Demo {
    /// The demo as it is at `now`.
    pub(crate) fn new(now: i64) -> Self {
        use TaskState::{Assigned, Blocked, Failed, Skipped, Success};
        const OK: &[Option<&str>] = &[Some("success")];
        const NONE: &[Option<&str>] = &[];
        const GOING: &[Option<&str>] = &[None];
        let run =
            |id: &str, dag: &str, state: RunState, trigger: &str, age: i64, tasks: Vec<TaskRow>| {
                RunView {
                    run: RunRow {
                        id: id.to_owned(),
                        dag: dag.to_owned(),
                        state,
                        trigger: trigger.to_owned(),
                        tasks: counts(&tasks),
                        logical_time: now - age * MINUTE,
                        created_at: now - age * MINUTE,
                        started_at: (state != RunState::Queued).then_some(now - age * MINUTE),
                        finished_at: state.is_final().then_some(now - (age - 3) * MINUTE),
                    },
                    problem: None,
                    params: if trigger == "manual" {
                        r#"{"country":"FR"}"#
                    } else {
                        "{}"
                    }
                    .to_owned(),
                    tasks,
                }
            };
        let running = etl(
            [
                Success, Success, Assigned, Success, Blocked, Blocked, Blocked, Blocked,
            ],
            [OK, OK, GOING, OK, NONE, NONE, NONE, NONE],
            now,
        );
        let failed = etl(
            [
                Success, Success, Success, Success, Failed, Success, Skipped, Skipped,
            ],
            [
                OK,
                OK,
                OK,
                OK,
                &[Some("failed"), Some("timeout")],
                OK,
                NONE,
                NONE,
            ],
            now,
        );
        let done = etl([Success; 8], [OK; 8], now);
        // A run stuck on a task that asks for a label no worker has.
        let mut stuck = tasks(
            &[
                ("load", &["train"], Blocked, NONE),
                ("train", &[], Blocked, NONE),
            ],
            now,
        );
        stuck[1].waiting = Some("no connected worker has the label \"gpu\"".to_owned());
        let waiting = tasks(
            &[
                ("load", &["train"], Blocked, NONE),
                ("train", &[], Blocked, NONE),
            ],
            now,
        );
        Self {
            now,
            dags: vec![
                DagRow {
                    name: "etl_sales".to_owned(),
                    paused: false,
                    next_fire: Some(now + 42 * MINUTE),
                    version: "9f2c41d07a3b".to_owned(),
                },
                DagRow {
                    name: "reports".to_owned(),
                    paused: true,
                    next_fire: None,
                    version: "51ab07c2e9d4".to_owned(),
                },
                DagRow {
                    name: "train_model".to_owned(),
                    paused: false,
                    next_fire: None,
                    version: "c0ffee123456".to_owned(),
                },
            ],
            runs: vec![
                run(
                    "0192f3a4-0004-7000-8000-00000000d4c1",
                    "train_model",
                    RunState::Queued,
                    "manual",
                    1,
                    waiting,
                ),
                run(
                    "0192f3a4-0003-7000-8000-00000000a2f1",
                    "etl_sales",
                    RunState::Running,
                    "cron",
                    4,
                    running,
                ),
                run(
                    "0192f3a4-0002-7000-8000-00000000a19e",
                    "etl_sales",
                    RunState::Failed,
                    "manual",
                    64,
                    failed,
                ),
                run(
                    "0192f3a4-0001-7000-8000-00000000a2c8",
                    "etl_sales",
                    RunState::Success,
                    "cron",
                    124,
                    done,
                ),
                run(
                    "0192f3a4-0000-7000-8000-00000000b7e2",
                    "train_model",
                    RunState::Running,
                    "manual",
                    300,
                    stuck,
                ),
            ],
            workers: vec![
                WorkerRow {
                    name: "w1".to_owned(),
                    accepting: true,
                    slots: 8,
                    used: 1,
                    labels: vec!["big".to_owned()],
                    available_memory: 12 << 30,
                    free_disk: 180 << 30,
                    heartbeat_age_ms: 2_000,
                },
                WorkerRow {
                    name: "w2".to_owned(),
                    accepting: true,
                    slots: 4,
                    used: 0,
                    labels: Vec::new(),
                    available_memory: 3 << 30,
                    free_disk: 40 << 30,
                    heartbeat_age_ms: 4_000,
                },
            ],
            changed: false,
        }
    }

    fn find(&mut self, id: &str) -> Result<&mut RunView> {
        self.runs
            .iter_mut()
            .find(|view| view.run.id == id)
            .ok_or_else(|| format!("there is no run `{id}`"))
    }
}

impl Backend for Demo {
    fn status(&mut self) -> Result<Status> {
        let count = |wanted: RunState| {
            u32::try_from(
                self.runs
                    .iter()
                    .filter(|view| view.run.state == wanted)
                    .count(),
            )
            .unwrap_or(0)
        };
        Ok(Status {
            address: "demo".to_owned(),
            workers: u32::try_from(self.workers.len()).unwrap_or(0),
            slots: self.workers.iter().map(|worker| worker.slots).sum(),
            slots_used: self.workers.iter().map(|worker| worker.used).sum(),
            active_runs: count(RunState::Running),
            queued_runs: count(RunState::Queued),
            next_fire: self.dags.iter().filter_map(|dag| dag.next_fire).min(),
        })
    }

    fn dags(&mut self) -> Result<Vec<DagRow>> {
        Ok(self.dags.clone())
    }

    fn runs(&mut self, dag: Option<&str>, limit: usize) -> Result<Vec<RunRow>> {
        Ok(self
            .runs
            .iter()
            .filter(|view| dag.is_none_or(|dag| view.run.dag == dag))
            .take(limit)
            .map(|view| view.run.clone())
            .collect())
    }

    fn run(&mut self, id: &str) -> Result<RunView> {
        self.find(id).cloned()
    }

    fn dag(&mut self, name: &str) -> Result<RunView> {
        let mut view = self
            .runs
            .iter()
            .find(|view| view.run.dag == name)
            .cloned()
            .ok_or_else(|| format!("there is no DAG `{name}`"))?;
        for task in &mut view.tasks {
            task.state = TaskState::Blocked;
            task.attempts.clear();
        }
        view.run.id = String::new();
        view.run.state = RunState::Queued;
        view.run.tasks = counts(&view.tasks);
        Ok(view)
    }

    fn workers(&mut self) -> Result<Vec<WorkerRow>> {
        Ok(self.workers.clone())
    }

    fn log(&mut self, _run: &str, task: &str, attempt: u32, from: usize) -> Result<Vec<LogLine>> {
        let at = self.now - 4 * MINUTE;
        let line = |offset: i64, stream: char, text: String| LogLine {
            at: at + offset * 1_000,
            stream,
            text,
        };
        let mut lines = vec![
            line(
                0,
                'O',
                format!("{task}: reading s3://data/2026-10-26/raw.parquet"),
            ),
            line(7, 'O', "18432 rows read".to_owned()),
            line(8, 'O', "dropping 112 rows without a customer id".to_owned()),
            line(
                13,
                'E',
                "timeout on the connection to the database, trying again".to_owned(),
            ),
            line(
                19,
                'O',
                format!("{task}: writing s3://data/2026-10-26/{task}.parquet"),
            ),
        ];
        if attempt > 1 {
            lines.push(line(25, 'S', "timeout: stopping the task".to_owned()));
        }
        Ok(lines.into_iter().skip(from).collect())
    }

    fn start(&mut self, dag: &str, params: &[(String, String)]) -> Result<String> {
        let mut view = self.dag(dag)?;
        let id = format!("0192f3a4-{:04}-7000-8000-0000000000ff", self.runs.len() + 1);
        view.run.id.clone_from(&id);
        view.run.state = RunState::Running;
        "manual".clone_into(&mut view.run.trigger);
        view.run.logical_time = self.now;
        view.run.created_at = self.now;
        view.run.started_at = Some(self.now);
        view.run.finished_at = None;
        let pairs: Vec<String> = params
            .iter()
            .map(|(k, v)| format!("\"{k}\":\"{v}\""))
            .collect();
        view.params = format!("{{{}}}", pairs.join(","));
        self.runs.insert(0, view);
        self.changed = true;
        Ok(id)
    }

    fn cancel(&mut self, run: &str) -> Result<()> {
        let view = self.find(run)?;
        if view.run.state.is_final() {
            return Err(format!("run {run} is already finished"));
        }
        view.run.state = RunState::Cancelled;
        for task in &mut view.tasks {
            if !task.state.is_final() {
                task.state = TaskState::Cancelled;
            }
        }
        view.run.tasks = counts(&view.tasks);
        self.changed = true;
        Ok(())
    }

    fn rerun(&mut self, run: &str, what: &Rerun) -> Result<()> {
        let view = self.find(run)?;
        if !view.run.state.is_final() {
            return Err("only a finished run can be run again".to_owned());
        }
        for task in &mut view.tasks {
            let again = match what {
                Rerun::Failed => task.state != TaskState::Success,
                Rerun::Task(name) => task.name == *name,
                Rerun::All => true,
            };
            if again {
                task.state = TaskState::Blocked;
            }
        }
        view.run.state = RunState::Running;
        view.run.finished_at = None;
        view.run.tasks = counts(&view.tasks);
        self.changed = true;
        Ok(())
    }

    fn pause(&mut self, dag: &str, paused: bool) -> Result<()> {
        let row = self
            .dags
            .iter_mut()
            .find(|row| row.name == dag)
            .ok_or_else(|| format!("there is no DAG `{dag}`"))?;
        row.paused = paused;
        self.changed = true;
        Ok(())
    }

    fn drain(&mut self, worker: &str) -> Result<()> {
        let row = self
            .workers
            .iter_mut()
            .find(|row| row.name == worker)
            .ok_or_else(|| format!("no worker `{worker}` is connected"))?;
        row.accepting = false;
        self.changed = true;
        Ok(())
    }

    fn changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }
}
