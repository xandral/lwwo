//! What a worker writes down about the tasks it runs, so that it can be
//! restarted without losing them.
//!
//! Every attempt has a directory, and the files in it are the journal. The
//! worker writes what to run *before* starting it, and how it ended is
//! written *before* it is reported, so after a crash the directory says
//! where things stood:
//!
//! ```text
//! attempts/<run>/<task>/<attempt>/
//!   spec.json     what to run            written by the worker, first
//!   shim.lock     held while the shim lives
//!   started.json  the process started    written by the shim
//!   log           the output so far      appended by the shim
//!   cancel        "stop it"              created by the worker
//!   exit.json     how it ended           written by the shim, last
//! ```
//!
//! The directory is removed when the master confirms the report: until
//! then it is the only place where the outcome exists.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::ship::Key;

/// What to run: the `Assign` of the master, plus what the shim needs to
/// know about the worker it runs on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Spec {
    pub(crate) run: String,
    pub(crate) task: String,
    pub(crate) attempt: u32,
    pub(crate) dag: String,
    pub(crate) command: Vec<String>,
    pub(crate) env: BTreeMap<String, String>,
    /// How long the attempt may take.
    pub(crate) timeout_ms: Option<u64>,
    /// The private working directory of the attempt.
    pub(crate) workdir: PathBuf,
    pub(crate) kill_grace_ms: u64,
    /// Where task cgroups go, if the worker can create them.
    pub(crate) cgroup_base: Option<PathBuf>,
    /// The file the worker keeps writing the time to while it is alive.
    pub(crate) alive: PathBuf,
    /// The task stops if the worker has been away this long: by then the
    /// master has given the task to someone else. Zero means never.
    pub(crate) orphan_timeout_ms: u64,
    /// The most output kept on the worker's disk for this attempt.
    #[serde(default = "no_log_limit")]
    pub(crate) log_max: u64,
}

fn no_log_limit() -> u64 {
    u64::MAX
}

impl Spec {
    pub(crate) fn key(&self) -> Key {
        (self.run.clone(), self.task.clone(), self.attempt)
    }

    /// A name for the cgroup of the attempt.
    pub(crate) fn name(&self) -> String {
        let run = self.run.replace('-', "");
        let tail = &run[run.len().saturating_sub(12)..];
        format!("{tail}-{}-{}", self.task, self.attempt)
    }
}

/// The process of the task started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Started {
    /// The process group of the task: what to stop if the shim is gone.
    pub(crate) child_pid: Option<u32>,
    /// When it started, UTC milliseconds.
    pub(crate) at: i64,
    /// Which boot of the machine the pid belongs to. After a reboot pids
    /// start again from the bottom, and the same number is someone else.
    #[serde(default)]
    pub(crate) boot: Option<String>,
}

/// What tells this boot of the machine from the others: the kernel's boot
/// id on Linux, the boot time on macOS. `None` where neither is known, and
/// then no pid is ever trusted across a restart.
pub(crate) fn boot_id() -> Option<String> {
    if let Ok(id) = std::fs::read_to_string("/proc/sys/kernel/random/boot_id") {
        return Some(id.trim().to_owned());
    }
    let output = std::process::Command::new("sysctl")
        .args(["-n", "kern.boottime"])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (output.status.success() && !text.is_empty()).then_some(text)
}

/// How the attempt ended.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Exit {
    /// `success`, `failed`, `timeout`, `cancelled` or `lost`.
    pub(crate) outcome: String,
    /// The exit code of the process, if it ended by itself.
    pub(crate) code: Option<i32>,
    pub(crate) started_at: i64,
    pub(crate) finished_at: i64,
    pub(crate) peak_memory: Option<u64>,
    /// Why it could not run, if it could not.
    pub(crate) error: String,
}

/// The directory of an attempt under `base`.
pub(crate) fn attempt_dir(base: &Path, key: &Key) -> PathBuf {
    base.join(&key.0).join(&key.1).join(key.2.to_string())
}

/// Writes `value` so that the file is either absent or complete: to a
/// temporary file first, then renamed, which is atomic.
pub(crate) fn write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let json = serde_json::to_vec(value)?;
    std::fs::write(&temporary, json)
        .with_context(|| format!("cannot write {}", temporary.display()))?;
    std::fs::rename(&temporary, path)
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

/// Reads a file of the journal; `None` if it is not there (yet).
pub(crate) fn read<T: DeserializeOwned>(path: &Path) -> Option<T> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

/// Every attempt with a complete `spec.json` under `base`. Directories
/// without one are leftovers of a crash before anything started, and are
/// removed.
pub(crate) fn scan(base: &Path) -> Vec<(Spec, PathBuf)> {
    let children = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
            .unwrap_or_default()
    };
    let mut found = Vec::new();
    for run in children(base) {
        for task in children(&run) {
            for attempt in children(&task) {
                match read::<Spec>(&attempt.join("spec.json")) {
                    Some(spec) => found.push((spec, attempt)),
                    None => {
                        let _ = std::fs::remove_dir_all(&attempt);
                    }
                }
            }
        }
    }
    found
}

/// Removes the directory of an attempt, and those above it if they are
/// left empty.
pub(crate) fn forget(base: &Path, dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    let mut parent = dir.parent();
    while let Some(up) = parent.filter(|up| *up != base && up.starts_with(base)) {
        // Fails, and stops, at the first directory that is not empty.
        if std::fs::remove_dir(up).is_err() {
            break;
        }
        parent = up.parent();
    }
}

/// A lock that says "the shim of this attempt is alive". The shim takes
/// it when it starts and the kernel releases it when the shim ends,
/// however it ends; unlike a pid, it cannot be mistaken for another
/// process.
///
/// [`shim_alive`] holds the lock for an instant to test it, and the
/// creation of the file wakes the worker to do just that, so a lock that
/// is taken is tried again for up to [`LOCK_PATIENCE`]: only one held
/// that long belongs to another shim.
pub(crate) fn lock_shim(dir: &Path) -> Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("shim.lock"))?;
    let start = std::time::Instant::now();
    while rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).is_err() {
        if start.elapsed() >= LOCK_PATIENCE {
            bail!("another shim holds this attempt");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(file)
}

/// How long a shim tries to take a lock that is held.
const LOCK_PATIENCE: Duration = Duration::from_secs(1);

/// Whether the shim of an attempt is alive: whether its lock is held.
pub(crate) fn shim_alive(dir: &Path) -> bool {
    let Ok(file) = std::fs::File::open(dir.join("shim.lock")) else {
        return false;
    };
    // Taking the lock means nobody held it; it is released at once, when
    // `file` is dropped.
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).is_err()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lwwo-journal-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn exit() -> Exit {
        Exit {
            outcome: "success".to_owned(),
            code: Some(0),
            started_at: 1,
            finished_at: 2,
            peak_memory: None,
            error: String::new(),
        }
    }

    #[test]
    fn a_file_is_absent_or_complete() {
        let dir = temp();
        let path = dir.join("exit.json");
        assert!(read::<Exit>(&path).is_none());
        write(&path, &exit()).unwrap();
        assert_eq!(read::<Exit>(&path).unwrap().outcome, "success");
        assert!(!dir.join("exit.tmp").exists());
        // Half a file is the same as no file.
        std::fs::write(&path, b"{\"outcome\": \"succ").unwrap();
        assert!(read::<Exit>(&path).is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_shim_waits_for_a_lock_held_for_an_instant() {
        let dir = temp();
        std::fs::write(dir.join("shim.lock"), "").unwrap();
        let checker = std::fs::File::open(dir.join("shim.lock")).unwrap();
        rustix::fs::flock(&checker, rustix::fs::FlockOperation::LockExclusive).unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(checker);
        });
        let _lock = lock_shim(&dir).expect("taken once the check lets go");
        release.join().unwrap();
        assert!(lock_shim(&dir).is_err(), "held by a shim: given up");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_lock_tells_whether_the_shim_is_alive() {
        let dir = temp();
        assert!(!shim_alive(&dir), "no lock file, no shim");
        let lock = lock_shim(&dir).unwrap();
        assert!(shim_alive(&dir));
        assert!(lock_shim(&dir).is_err(), "one shim per attempt");
        drop(lock);
        assert!(!shim_alive(&dir));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn forgetting_an_attempt_leaves_no_empty_directories() {
        let base = temp();
        let key: Key = ("run".to_owned(), "task".to_owned(), 1);
        let dir = attempt_dir(&base, &key);
        let other = attempt_dir(&base, &("run".to_owned(), "other".to_owned(), 1));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        // A directory without a spec is a leftover.
        assert!(scan(&base).is_empty());
        assert!(!dir.exists() && !other.exists());

        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        forget(&base, &dir);
        assert!(!base.join("run/task").exists());
        assert!(other.exists(), "the run still has another task");
        forget(&base, &other);
        assert!(base.exists() && !base.join("run").exists());
        std::fs::remove_dir_all(base).unwrap();
    }
}
