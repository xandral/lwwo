//! The worker's side of running a task: start a shim, then watch the
//! journal of the attempt.
//!
//! The worker never holds the process of a task. It writes what to run,
//! starts `lwwo shim` on it and from then on only reads files: the spool
//! to send the output, `started.json` and `exit.json` to report. A worker
//! that starts finds the attempts of the one before it in the same
//! directories and picks them up where they are.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use rustix::process::{Pid, Signal, kill_process_group};
use tokio::sync::mpsc;
use tracing::warn;

use super::cgroup::Cgroup;
use super::journal::{self, Exit, Spec, Started};
use super::ship::{Key, Shipper};
use super::wake::Wakeups;
use crate::config::WorkerConfig;
use crate::proto;
use crate::util::now_ms;

/// A shim gets this long to take its lock before it is given up.
const SHIM_START: Duration = Duration::from_secs(5);

/// What an attempt tells the worker's loop.
#[derive(Debug)]
pub(crate) enum TaskEvent {
    Started { key: Key, at: i64 },
    Finished { key: Key, finished: proto::Finished },
}

/// What all attempts of a worker share.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) config: WorkerConfig,
    pub(crate) shipper: Shipper,
    /// Where task cgroups go, if the worker could prepare one.
    pub(crate) cgroup_base: Option<PathBuf>,
    /// The heartbeat timeout of the master, in milliseconds: a task whose
    /// worker is away longer than this stops by itself.
    pub(crate) orphan_timeout_ms: AtomicU64,
    /// Wakes the followers when the files of their attempt change.
    pub(crate) wakeups: Wakeups,
}

impl Shared {
    /// Where the journals of the attempts are.
    pub(crate) fn attempts(&self) -> PathBuf {
        self.config.data_dir.join("attempts")
    }

    /// The file that says the worker is alive.
    fn alive(&self) -> PathBuf {
        self.config.data_dir.join("alive")
    }

    /// Says "the worker is alive now" to the shims. Called at start and at
    /// every heartbeat.
    pub(crate) fn touch_alive(&self) {
        let path = self.alive();
        let temporary = path.with_extension("tmp");
        if std::fs::write(&temporary, now_ms().to_string()).is_ok() {
            let _ = std::fs::rename(temporary, path);
        }
    }
}

/// Writes down an assignment and starts its shim. Returns the journal
/// directory of the attempt.
pub(crate) fn launch(shared: &Shared, assign: &proto::Assign, key: &Key) -> Result<PathBuf> {
    let dir = journal::attempt_dir(&shared.attempts(), key);
    std::fs::create_dir_all(&dir).with_context(|| format!("cannot create {}", dir.display()))?;
    let config = &shared.config;
    let spec = Spec {
        run: key.0.clone(),
        task: key.1.clone(),
        attempt: key.2,
        dag: assign.dag.clone(),
        command: assign.command.clone(),
        env: assign.env.clone().into_iter().collect(),
        timeout_ms: assign.timeout_ms,
        workdir: journal::attempt_dir(&config.workdir(), key),
        kill_grace_ms: u64::try_from(config.kill_grace.0.as_millis()).unwrap_or(u64::MAX),
        cgroup_base: shared.cgroup_base.clone(),
        alive: shared.alive(),
        orphan_timeout_ms: shared.orphan_timeout_ms.load(Ordering::Relaxed),
        log_max: config.log_spool_max.0.of(u64::MAX),
    };
    // Written before anything starts: from here on a restarted worker
    // knows about this attempt.
    journal::write(&dir.join("spec.json"), &spec)?;

    let program = std::env::current_exe().context("cannot find the lwwo program itself")?;
    let errors = std::fs::File::create(dir.join("shim.err"))?;
    tokio::process::Command::new(program)
        .arg("shim")
        .arg(&dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(errors)
        // Its own process group: a Ctrl-C at the worker's terminal, or a
        // signal to the worker's group, does not reach it.
        .process_group(0)
        .spawn()
        .context("cannot start the shim of the task")?;
    // The handle is dropped on purpose: the shim outlives the worker.
    Ok(dir)
}

/// Asks the shim of an attempt to stop its task.
pub(crate) fn cancel(dir: &Path) {
    let _ = std::fs::write(dir.join("cancel"), "");
}

/// The attempts found on disk when the worker starts: those a previous
/// worker left running or unreported.
pub(crate) fn recover(shared: &Shared) -> Vec<(Spec, PathBuf)> {
    journal::scan(&shared.attempts())
}

/// The shim of an attempt died without saying how the task ended. Stops
/// whatever is left of the task and writes the attempt down as lost.
async fn clean_up_after_shim(shared: &Shared, spec: &Spec, dir: &Path) -> Exit {
    warn!(
        "the shim of {} attempt {} is gone: stopping its task",
        spec.task, spec.attempt
    );
    let started: Option<Started> = journal::read(&dir.join("started.json"));
    // The pid means this task only if the machine has not restarted since.
    let same_boot = started
        .as_ref()
        .and_then(|started| started.boot.as_ref())
        .is_some_and(|boot| journal::boot_id().as_ref() == Some(boot));
    if let Some(group) = started
        .as_ref()
        .filter(|_| same_boot)
        .and_then(|started| Pid::from_raw(i32::try_from(started.child_pid?).ok()?))
    {
        let _ = kill_process_group(group, Signal::KILL);
    }
    let cgroup = Cgroup::at(shared.cgroup_base.as_deref(), &spec.name());
    cgroup.kill();
    cgroup.remove();
    let _ = tokio::fs::remove_dir_all(&spec.workdir).await;
    let now = now_ms();
    let exit = Exit {
        outcome: "lost".to_owned(),
        code: None,
        started_at: started.map_or(now, |started| started.at),
        finished_at: now,
        peak_memory: None,
        error: "the shim watching the task died".to_owned(),
    };
    // Written down like any other outcome, so it survives a restart of
    // the worker until the master confirms it.
    if let Err(error) = journal::write(&dir.join("exit.json"), &exit) {
        warn!(
            "cannot write {}: {error:#}",
            dir.join("exit.json").display()
        );
    }
    exit
}

fn finished(exit: Exit) -> proto::Finished {
    let outcome = match exit.outcome.as_str() {
        "success" => proto::Outcome::Success,
        "timeout" => proto::Outcome::Timeout,
        "cancelled" => proto::Outcome::Cancelled,
        "lost" => proto::Outcome::Lost,
        _ => proto::Outcome::Failed,
    };
    proto::Finished {
        outcome: outcome.into(),
        exit_code: exit.code,
        started_at: exit.started_at,
        finished_at: exit.finished_at,
        peak_memory: exit.peak_memory,
        error: exit.error,
    }
}

/// How long ago the attempt was written down.
fn age(dir: &Path) -> Duration {
    std::fs::metadata(dir.join("spec.json"))
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|written| SystemTime::now().duration_since(written).ok())
        .unwrap_or(Duration::MAX)
}

/// Follows one attempt through its journal until it ends, sending its
/// output to the master on the way and its outcome at the end.
pub(crate) async fn follow(
    shared: Arc<Shared>,
    spec: Spec,
    dir: PathBuf,
    events: mpsc::Sender<TaskEvent>,
) {
    let key = spec.key();
    shared
        .shipper
        .track(key.clone(), spec.dag.clone(), dir.clone());
    // Watched before the first look, so no change is missed in between.
    let watch = shared.wakeups.watch(&dir);
    let mut told_started = false;
    let mut shim_missing = 0;
    let exit = loop {
        if !told_started {
            if let Some(started) = journal::read::<Started>(&dir.join("started.json")) {
                told_started = true;
                let event = TaskEvent::Started {
                    key: key.clone(),
                    at: started.at,
                };
                let _ = events.send(event).await;
            }
        }
        if let Some(exit) = journal::read::<Exit>(&dir.join("exit.json")) {
            break exit;
        }
        // No outcome and nobody holding the lock: the shim died. Twice in
        // a row, because a shim writes its outcome just before it ends;
        // and not while it may still be starting.
        if journal::shim_alive(&dir) || age(&dir) < SHIM_START {
            shim_missing = 0;
        } else {
            shim_missing += 1;
            if shim_missing >= 2 {
                break match journal::read::<Exit>(&dir.join("exit.json")) {
                    Some(exit) => exit,
                    None => clean_up_after_shim(&shared, &spec, &dir).await,
                };
            }
        }
        shared.shipper.grew();
        watch.changed().await;
    };
    drop(watch);
    // The report comes after the log, so the master can save both
    // together.
    shared.shipper.grew();
    shared.shipper.finish(key.clone()).await;
    let event = TaskEvent::Finished {
        key,
        finished: finished(exit),
    };
    let _ = events.send(event).await;
}
