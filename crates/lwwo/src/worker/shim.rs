//! The shim: a small process that stays beside one task.
//!
//! A process can only learn how its own children ended. If the worker ran
//! the tasks itself, a worker that stops, for an update or a crash, would
//! orphan them and lose their outcome. So the worker does not run tasks:
//! for each attempt it starts `lwwo shim <directory>` and forgets it. The
//! shim reads what to do from the attempt's journal, runs the task, writes
//! its output to the spool, enforces its timeout, and at the end writes how
//! it went. The worker only looks at files, so it can come and go.
//!
//! This is what `containerd-shim` and `conmon` do for containers. The shim
//! does little on purpose: no network, no state, nothing to update. Less
//! to go wrong is the whole point of it.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rustix::process::{Pid, Signal, kill_process_group};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use super::cgroup::Cgroup;
use super::journal::{self, Exit, Spec, Started};
use super::spool::Writer;
use crate::util::now_ms;

/// How often the shim looks for a request to stop.
const LOOK_EVERY: Duration = Duration::from_millis(300);

/// Why a task is stopped before it ends by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    Timeout,
    /// The master cancelled the run.
    Cancelled,
    /// The worker has been away too long: the master gave the task to
    /// someone else by now.
    Orphaned,
}

/// Where the output of an attempt goes: the spool in its journal, which
/// the worker sends to the master. One line of output is one line of the
/// spool: milliseconds, stream (`O`ut, `E`rr or `S`ystem) and text.
#[derive(Debug, Clone)]
struct Output {
    lines: mpsc::UnboundedSender<String>,
}

impl Output {
    fn line(&self, stream: char, text: &str) {
        let _ = self.lines.send(format!("{}\t{stream}\t{text}\n", now_ms()));
    }

    /// A line from lwwo itself, not from the task.
    fn system(&self, text: &str) {
        self.line('S', text);
    }
}

/// Copies the lines of a pipe to the output.
async fn copy_lines(pipe: impl AsyncRead + Unpin, stream: char, output: Output) {
    let mut reader = BufReader::new(pipe);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                let text = String::from_utf8_lossy(&line);
                output.line(stream, text.trim_end_matches(['\n', '\r']));
            }
        }
    }
}

/// The command that starts the task: a process on the worker's host.
fn command(spec: &Spec, cgroup: &Cgroup) -> Result<Command> {
    ensure!(!spec.command.is_empty(), "the task has no command");
    // The task enters its cgroup before it becomes the task: a shell
    // writes its own pid there and then turns into the command, so
    // nothing the task starts can be outside.
    let mut command = if let Some(procs) = cgroup.procs() {
        let mut shell = Command::new("/bin/sh");
        shell
            .args(["-c", "echo $$ > \"$0\"; exec \"$@\""])
            .arg(procs)
            .args(&spec.command);
        shell
    } else {
        let mut command = Command::new(&spec.command[0]);
        command.args(&spec.command[1..]);
        command
    };
    command.current_dir(&spec.workdir);
    // The task inherits the worker's environment, except lwwo's own
    // variables: its configuration, token included, is not for tasks.
    for (variable, _) in std::env::vars_os() {
        if variable.to_string_lossy().starts_with("LWWO_") {
            command.env_remove(variable);
        }
    }
    command.envs(&spec.env).env("LWWO_WORKDIR", &spec.workdir);
    Ok(command)
}

/// Stops the task: SIGTERM to its whole process group, then, if it is
/// still there after `kill_grace`, SIGKILL to the group and the cgroup.
async fn stop(spec: &Spec, child: &mut Child, cgroup: &Cgroup) {
    let group = child
        .id()
        .and_then(|pid| Pid::from_raw(i32::try_from(pid).ok()?));
    if let Some(group) = group {
        let _ = kill_process_group(group, Signal::TERM);
    }
    let grace = Duration::from_millis(spec.kill_grace_ms);
    if tokio::time::timeout(grace, child.wait()).await.is_ok() {
        cgroup.kill();
        return;
    }
    if let Some(group) = group {
        let _ = kill_process_group(group, Signal::KILL);
    }
    cgroup.kill();
    let _ = child.wait().await;
}

/// Decides when the task must be stopped, and says so on `stop`.
async fn watch_for_stop(
    spec: Spec,
    dir: PathBuf,
    started_at: i64,
    stop: watch::Sender<Option<Stop>>,
) {
    let deadline = spec
        .timeout_ms
        .map(|millis| started_at.saturating_add(i64::try_from(millis).unwrap_or(i64::MAX)));
    let orphan_after = i64::try_from(spec.orphan_timeout_ms).unwrap_or(i64::MAX);
    // A SIGTERM to the shim itself means the same as a cancel.
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    loop {
        let terminated = async {
            match &mut terminate {
                Some(signal) => signal.recv().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = tokio::time::sleep(LOOK_EVERY) => {}
            _ = terminated => {
                let _ = stop.send(Some(Stop::Cancelled));
                return;
            }
        }
        let now = now_ms();
        // The worker writes the time to `alive` at every heartbeat. A file
        // that cannot be read right now proves nothing: only an old time
        // counts.
        let worker_seen: Option<i64> = std::fs::read_to_string(&spec.alive)
            .ok()
            .and_then(|text| text.trim().parse().ok());
        let reason = if dir.join("cancel").exists() {
            Some(Stop::Cancelled)
        } else if deadline.is_some_and(|deadline| now >= deadline) {
            Some(Stop::Timeout)
        } else if orphan_after > 0 && worker_seen.is_some_and(|seen| now - seen > orphan_after) {
            Some(Stop::Orphaned)
        } else {
            None
        };
        if reason.is_some() {
            let _ = stop.send(reason);
            return;
        }
    }
}

/// Waits until the task must be stopped.
async fn stopped(stop: &mut watch::Receiver<Option<Stop>>) -> Stop {
    match stop.wait_for(Option::is_some).await {
        Ok(reason) => (*reason).unwrap_or(Stop::Cancelled),
        // Nobody is watching any more: nothing will ever ask to stop.
        Err(_) => std::future::pending().await,
    }
}

/// How an attempt went, before it is written down.
struct End {
    outcome: &'static str,
    exit_code: Option<i32>,
    peak_memory: Option<u64>,
    error: String,
}

impl End {
    fn stopped(reason: Stop, output: &Output) -> Self {
        let (outcome, said, error) = match reason {
            Stop::Timeout => ("timeout", "timeout: stopping the task", ""),
            Stop::Cancelled => ("cancelled", "cancelled: stopping the task", ""),
            Stop::Orphaned => (
                "cancelled",
                "the worker has been away too long: stopping the task",
                "the worker was away for longer than the heartbeat timeout",
            ),
        };
        output.system(said);
        Self {
            outcome,
            exit_code: None,
            peak_memory: None,
            error: error.to_owned(),
        }
    }
}

async fn attempt(
    spec: &Spec,
    dir: &Path,
    output: &Output,
    stop_rx: &mut watch::Receiver<Option<Stop>>,
) -> Result<End> {
    std::fs::create_dir_all(&spec.workdir).with_context(|| {
        format!(
            "cannot create the working directory {}",
            spec.workdir.display()
        )
    })?;
    let cgroup = Cgroup::at(spec.cgroup_base.as_deref(), &spec.name());
    let spawned = command(spec, &cgroup).and_then(|mut command| {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Its own process group, so that signals reach its children.
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("cannot start `{}`", spec.command[0]))
    });
    let mut child = match spawned {
        Ok(child) => child,
        Err(error) => {
            cgroup.remove();
            return Err(error);
        }
    };
    journal::write(
        &dir.join("started.json"),
        &Started {
            child_pid: child.id(),
            at: now_ms(),
            boot: journal::boot_id(),
        },
    )?;

    let stdout = child
        .stdout
        .take()
        .map(|pipe| tokio::spawn(copy_lines(pipe, 'O', output.clone())));
    let stderr = child
        .stderr
        .take()
        .map(|pipe| tokio::spawn(copy_lines(pipe, 'E', output.clone())));
    let end = tokio::select! {
        status = child.wait() => {
            let status = status.context("cannot wait for the task")?;
            End {
                outcome: if status.success() { "success" } else { "failed" },
                exit_code: status.code(),
                peak_memory: None,
                error: String::new(),
            }
        }
        reason = stopped(stop_rx) => {
            let end = End::stopped(reason, output);
            stop(spec, &mut child, &cgroup).await;
            end
        }
    };
    // Whatever the task left behind goes with it.
    cgroup.kill();
    // A process that outlived the task may hold the pipes open: do not
    // wait for it for ever.
    for mut reader in [stdout, stderr].into_iter().flatten() {
        if tokio::time::timeout(Duration::from_secs(5), &mut reader)
            .await
            .is_err()
        {
            reader.abort();
        }
    }
    let peak_memory = cgroup.peak_memory();
    cgroup.remove();
    Ok(End { peak_memory, ..end })
}

/// Runs the attempt whose journal is in `dir`, from beginning to end.
pub(crate) async fn run(dir: &Path) -> Result<()> {
    // First of all: this is what tells the worker that the shim is alive.
    let _alive = journal::lock_shim(dir)?;
    let spec: Spec = journal::read(&dir.join("spec.json"))
        .with_context(|| format!("{} has no readable spec.json", dir.display()))?;
    let started_at = now_ms();

    // One writer for the log, fed by stdout, stderr and lwwo itself.
    let (lines, mut written) = mpsc::unbounded_channel::<String>();
    let output = Output { lines };
    let mut log = Writer::new(dir, spec.log_max);
    // Plain file writes, on a thread of their own.
    let writer = tokio::task::spawn_blocking(move || {
        while let Some(line) = written.blocking_recv() {
            // A segment that cannot be written loses the line, not the
            // task.
            let _ = log.line(line.as_bytes());
        }
    });

    let (stop_tx, mut stop_rx) = watch::channel(None);
    let watcher = tokio::spawn(watch_for_stop(
        spec.clone(),
        dir.to_owned(),
        started_at,
        stop_tx,
    ));
    let end = attempt(&spec, dir, &output, &mut stop_rx)
        .await
        .unwrap_or_else(|error| {
            let error = format!("{error:#}");
            output.system(&error);
            End {
                outcome: "failed",
                exit_code: None,
                peak_memory: None,
                error,
            }
        });
    watcher.abort();
    let _ = tokio::fs::remove_dir_all(&spec.workdir).await;

    // The log is complete before the outcome exists: whoever sees
    // `exit.json` can send the whole log first.
    drop(output);
    let _ = writer.await;
    journal::write(
        &dir.join("exit.json"),
        &Exit {
            outcome: end.outcome.to_owned(),
            code: end.exit_code,
            started_at,
            finished_at: now_ms(),
            peak_memory: end.peak_memory,
            error: end.error,
        },
    )
}
