//! The TUI's view of a real master, through its HTTP API.
//!
//! The interface is a plain loop that asks and draws; the API client is
//! asynchronous. Each question blocks on the runtime the client lives in,
//! which is fine: a person is waiting for exactly that answer. Two things
//! arrive without being asked for, on tasks of their own: the master's
//! `/events`, which say when to look again, and the lines of the logs that
//! are open, followed like `tail -f`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use serde_json::{Value, json};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use super::backend::{
    AttemptRow, Backend, Counts, DagRow, Rerun, Result, RunRow, RunState, RunView, Status, TaskRow,
    TaskState, WorkerRow,
};
use crate::client::{Client, LogLine};

/// A log nobody asked about for this long is no longer on screen.
const FORGET_AFTER: Duration = Duration::from_secs(5);

/// The log of one attempt, filled as the master sends it.
struct Follow {
    lines: Arc<Mutex<Vec<LogLine>>>,
    task: JoinHandle<()>,
    asked: Instant,
}

pub(crate) struct Remote {
    client: Client,
    runtime: Handle,
    changed: Arc<AtomicBool>,
    /// The logs being followed, by run, task and attempt.
    logs: HashMap<(String, String, u32), Follow>,
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn number(value: &Value) -> u32 {
    u32::try_from(value.as_u64().unwrap_or(0)).unwrap_or(u32::MAX)
}

fn list(value: &Value) -> Vec<String> {
    value.as_array().into_iter().flatten().map(text).collect()
}

fn items(value: &Value) -> impl Iterator<Item = &Value> {
    value.as_array().into_iter().flatten()
}

fn run_row(run: &Value, tasks: Counts) -> RunRow {
    RunRow {
        id: text(&run["id"]),
        dag: text(&run["dag_name"]),
        state: RunState::parse(run["state"].as_str().unwrap_or_default()),
        trigger: text(&run["trigger"]),
        tasks,
        logical_time: run["logical_time"].as_i64().unwrap_or(0),
        created_at: run["created_at"].as_i64().unwrap_or(0),
        started_at: run["started_at"].as_i64(),
        finished_at: run["finished_at"].as_i64(),
    }
}

/// The tasks of a run or of a DAG: `named` gives the name of each, and
/// their needs become positions.
fn task_rows(
    tasks: &Value,
    name_field: &str,
    command_field: &str,
    attempts: &Value,
) -> Vec<TaskRow> {
    let names: Vec<String> = items(tasks).map(|task| text(&task[name_field])).collect();
    items(tasks)
        .map(|task| {
            let name = text(&task[name_field]);
            TaskRow {
                state: TaskState::parse(task["state"].as_str().unwrap_or_default()),
                command: list(&task[command_field]).join(" "),
                image: task["image"].as_str().map(str::to_owned),
                waiting: task["waiting"].as_str().map(str::to_owned),
                needs: items(&task["needs"])
                    .filter_map(|need| names.iter().position(|n| Some(n.as_str()) == need.as_str()))
                    .collect(),
                attempts: items(attempts)
                    .filter(|attempt| attempt["task"].as_str() == Some(name.as_str()))
                    .map(|attempt| AttemptRow {
                        number: number(&attempt["attempt"]),
                        outcome: attempt["outcome"].as_str().map(str::to_owned),
                        worker: text(&attempt["worker"]),
                        exit_code: attempt["exit_code"].as_i64(),
                        started_at: attempt["started_at"].as_i64(),
                        finished_at: attempt["finished_at"].as_i64(),
                        peak_memory: attempt["peak_memory"].as_u64(),
                    })
                    .collect(),
                name,
            }
        })
        .collect()
}

impl Remote {
    /// Starts listening to the master's events. `runtime` is the runtime
    /// `client` was made in.
    pub(crate) fn new(client: Client, runtime: Handle) -> Self {
        let changed = Arc::new(AtomicBool::new(true));
        let flag = Arc::clone(&changed);
        let events = client.clone();
        runtime.spawn(async move {
            loop {
                if let Ok(response) = events.stream("/events").await {
                    let mut body = response.bytes_stream();
                    while let Some(Ok(_)) = body.next().await {
                        flag.store(true, Ordering::Relaxed);
                    }
                }
                // The stream broke: the master may be restarting.
                tokio::time::sleep(Duration::from_secs(2)).await;
                flag.store(true, Ordering::Relaxed);
            }
        });
        Self {
            client,
            runtime,
            changed,
            logs: HashMap::new(),
        }
    }

    fn get(&self, path: &str) -> Result<Value> {
        self.runtime
            .block_on(self.client.get(path))
            .map_err(|error| format!("{error:#}"))
    }

    fn post(&self, path: &str, body: &Value) -> Result<Value> {
        self.runtime
            .block_on(self.client.post(path, body))
            .map_err(|error| format!("{error:#}"))
    }

    /// Starts following the log of an attempt.
    fn follow(&self, run: &str, task: &str, attempt: u32) -> Follow {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&lines);
        let client = self.client.clone();
        let path = format!("/runs/{run}/tasks/{task}/logs?follow=true&attempt={attempt}");
        let task = self.runtime.spawn(async move {
            let Ok(response) = client.stream(&path).await else {
                return;
            };
            let mut body = response.bytes_stream();
            // Pieces do not end where lines do: keep the unfinished one.
            let mut rest: Vec<u8> = Vec::new();
            while let Some(Ok(piece)) = body.next().await {
                rest.extend_from_slice(&piece);
                let mut new = Vec::new();
                while let Some(end) = rest.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = rest.drain(..=end).collect();
                    new.push(LogLine::parse(&String::from_utf8_lossy(&line[..end])));
                }
                sink.lock()
                    .expect("the log lock is never poisoned")
                    .extend(new);
            }
        });
        Follow {
            lines,
            task,
            asked: Instant::now(),
        }
    }
}

impl Backend for Remote {
    fn status(&mut self) -> Result<Status> {
        let status = self.get("/status")?;
        Ok(Status {
            address: self.client.address.clone(),
            workers: number(&status["workers"]),
            slots: number(&status["slots"]),
            slots_used: number(&status["slots_used"]),
            active_runs: number(&status["active_runs"]),
            queued_runs: number(&status["queued_runs"]),
            next_fire: status["next_fire"].as_i64(),
        })
    }

    fn dags(&mut self) -> Result<Vec<DagRow>> {
        let dags = self.get("/dags")?;
        Ok(items(&dags)
            .map(|dag| DagRow {
                name: text(&dag["name"]),
                paused: dag["paused"] == json!(true),
                next_fire: dag["next_fire"].as_i64(),
                version: text(&dag["current"]).chars().take(12).collect(),
            })
            .collect())
    }

    fn runs(&mut self, dag: Option<&str>, limit: usize) -> Result<Vec<RunRow>> {
        let filter = dag.map(|dag| format!("&dag={dag}")).unwrap_or_default();
        let runs = self.get(&format!("/runs?limit={limit}{filter}"))?;
        Ok(items(&runs)
            .map(|run| {
                let tasks = &run["tasks"];
                let counts = Counts {
                    total: number(&tasks["total"]),
                    succeeded: number(&tasks["success"]),
                    failed: number(&tasks["failed"]),
                    active: number(&tasks["active"]),
                };
                run_row(run, counts)
            })
            .collect())
    }

    fn run(&mut self, id: &str) -> Result<RunView> {
        let detail = self.get(&format!("/runs/{id}"))?;
        let tasks = task_rows(&detail["tasks"], "task", "command", &detail["attempts"]);
        let count = |wanted: TaskState| {
            u32::try_from(tasks.iter().filter(|task| task.state == wanted).count())
                .unwrap_or(u32::MAX)
        };
        let counts = Counts {
            total: u32::try_from(tasks.len()).unwrap_or(u32::MAX),
            succeeded: count(TaskState::Success),
            failed: count(TaskState::Failed),
            active: count(TaskState::Assigned),
        };
        Ok(RunView {
            run: run_row(&detail["run"], counts),
            params: text(&detail["run"]["params"]),
            problem: detail["run"]["problem"].as_str().map(str::to_owned),
            tasks,
        })
    }

    fn dag(&mut self, name: &str) -> Result<RunView> {
        let detail = self.get(&format!("/dags/{name}"))?;
        let tasks = task_rows(&detail["definition"]["tasks"], "name", "run", &Value::Null);
        Ok(RunView {
            run: RunRow {
                id: String::new(),
                dag: name.to_owned(),
                state: RunState::Queued,
                trigger: String::new(),
                tasks: Counts {
                    total: u32::try_from(tasks.len()).unwrap_or(u32::MAX),
                    ..Counts::default()
                },
                logical_time: 0,
                created_at: 0,
                started_at: None,
                finished_at: None,
            },
            params: "{}".to_owned(),
            problem: None,
            tasks,
        })
    }

    fn workers(&mut self) -> Result<Vec<WorkerRow>> {
        let workers = self.get("/workers")?;
        Ok(items(&workers)
            .map(|worker| WorkerRow {
                name: text(&worker["name"]),
                accepting: worker["accepting"] == json!(true),
                slots: number(&worker["slots"]),
                used: number(&worker["used"]),
                labels: list(&worker["labels"]),
                executors: list(&worker["executors"]),
                available_memory: worker["available_memory"].as_u64().unwrap_or(0),
                free_disk: worker["free_disk"].as_u64().unwrap_or(0),
                heartbeat_age_ms: worker["heartbeat_age_ms"].as_i64().unwrap_or(0),
            })
            .collect())
    }

    fn log(&mut self, run: &str, task: &str, attempt: u32, from: usize) -> Result<Vec<LogLine>> {
        let key = (run.to_owned(), task.to_owned(), attempt);
        if !self.logs.contains_key(&key) {
            let follow = self.follow(run, task, attempt);
            self.logs.insert(key.clone(), follow);
        }
        let follow = self.logs.get_mut(&key).expect("just inserted");
        follow.asked = Instant::now();
        let lines = follow.lines.lock().expect("the log lock is never poisoned");
        Ok(lines.get(from..).unwrap_or_default().to_vec())
    }

    fn start(&mut self, dag: &str, params: &[(String, String)]) -> Result<String> {
        let params: serde_json::Map<String, Value> = params
            .iter()
            .map(|(name, value)| (name.clone(), json!(value)))
            .collect();
        let started = self.post(&format!("/dags/{dag}/runs"), &json!({ "params": params }))?;
        Ok(text(&started["id"]))
    }

    fn cancel(&mut self, run: &str) -> Result<()> {
        self.post(&format!("/runs/{run}/cancel"), &json!({}))
            .map(drop)
    }

    fn rerun(&mut self, run: &str, what: &Rerun) -> Result<()> {
        let body = match what {
            Rerun::Failed => json!({}),
            Rerun::Task(task) => json!({ "task": task }),
            Rerun::All => json!({ "all": true }),
        };
        self.post(&format!("/runs/{run}/rerun"), &body).map(drop)
    }

    fn pause(&mut self, dag: &str, paused: bool) -> Result<()> {
        let verb = if paused { "pause" } else { "resume" };
        self.post(&format!("/dags/{dag}/{verb}"), &json!({}))
            .map(drop)
    }

    fn drain(&mut self, worker: &str) -> Result<()> {
        self.post(&format!("/workers/{worker}/drain"), &json!({}))
            .map(drop)
    }

    fn changed(&mut self) -> bool {
        // Called at every tick: a good moment to let go of the logs that
        // left the screen.
        self.logs.retain(|_, follow| {
            let keep = follow.asked.elapsed() < FORGET_AFTER;
            if !keep {
                follow.task.abort();
            }
            keep
        });
        self.changed.swap(false, Ordering::Relaxed)
    }
}
