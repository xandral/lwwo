//! One cgroup per task, on Linux.
//!
//! A task may start other processes, which may leave its process group. A
//! cgroup holds them all: stopping the task stops the whole tree, and the
//! kernel says how much memory the tree used at its peak. Without cgroups
//! v2, or without the right to create them, the worker falls back to
//! process groups and reports no memory.

#[cfg(target_os = "linux")]
mod imp {
    use std::fs;
    use std::path::{Path, PathBuf};

    use tracing::{debug, info};

    use crate::config::CgroupMode;

    /// Whether every process in the cgroup at `base` is an lwwo process:
    /// this worker, or shims a previous one left. Then the cgroup is ours.
    fn only_lwwo_in(base: &Path) -> bool {
        let Ok(own) = std::env::current_exe() else {
            return false;
        };
        // After an update the shims of tasks still running hold the old
        // program, which the kernel shows as the same path, "(deleted)".
        let replaced = PathBuf::from(format!("{} (deleted)", own.display()));
        fs::read_to_string(base.join("cgroup.procs"))
            .unwrap_or_default()
            .lines()
            .all(|pid| {
                fs::read_link(format!("/proc/{pid}/exe"))
                    .is_ok_and(|exe| exe == own || exe == replaced)
            })
    }

    /// Finds the worker's own cgroup and prepares it to hold one child per
    /// task. The kernel only gives children a memory controller if the
    /// parent has no processes of its own, so the worker first moves
    /// itself, and whatever shares its cgroup, into a `supervisor` child.
    ///
    /// Moving other programs is not the worker's business: with
    /// [`CgroupMode::Auto`] it only goes ahead when the cgroup holds lwwo
    /// alone, as under a systemd service of its own or in a container.
    pub(crate) fn prepare(mode: CgroupMode) -> Option<PathBuf> {
        if mode == CgroupMode::Off {
            return None;
        }
        let own = fs::read_to_string("/proc/self/cgroup").ok()?;
        // cgroups v2 has a single line, `0::/some/path`.
        let path = own.lines().find_map(|line| line.strip_prefix("0::"))?;
        let base = PathBuf::from(format!("/sys/fs/cgroup{}", path.trim_end_matches('/')));
        if mode == CgroupMode::Auto && !only_lwwo_in(&base) {
            info!(
                "cgroups off: the worker shares its cgroup {} with other programs. Run it as a \
                 service of its own to get one cgroup per task, or set `cgroups = \"on\"`",
                base.display()
            );
            return None;
        }
        let supervisor = base.join("supervisor");
        fs::create_dir_all(&supervisor).ok()?;
        // Processes come and go while they are moved: try a few times, and
        // let the kernel say whether the cgroup is empty enough.
        let controllers = (0..5).any(|_| {
            for pid in fs::read_to_string(base.join("cgroup.procs"))
                .unwrap_or_default()
                .lines()
            {
                // One that just ended cannot be moved, and need not be.
                let _ = fs::write(supervisor.join("cgroup.procs"), pid);
            }
            fs::write(base.join("cgroup.subtree_control"), "+memory +pids").is_ok()
        });
        if controllers {
            info!(
                "cgroups: one per task under {}, with memory accounting",
                base.display()
            );
        } else {
            info!(
                "cgroups: one per task under {}, without memory accounting",
                base.display()
            );
        }
        Some(base)
    }

    #[derive(Debug)]
    pub(crate) struct Cgroup(Option<PathBuf>);

    impl Cgroup {
        /// The cgroup of a task under `base`, the directory the worker
        /// prepared; created if it is not there yet.
        pub(crate) fn at(base: Option<&Path>, id: &str) -> Self {
            let Some(base) = base else {
                return Self(None);
            };
            let path = base.join(format!("lwwo-{id}"));
            match fs::create_dir_all(&path) {
                Ok(()) => Self(Some(path)),
                Err(error) => {
                    debug!("no cgroup for {id}: {error}");
                    Self(None)
                }
            }
        }

        /// The file a process writes its pid to, to enter the cgroup.
        pub(crate) fn procs(&self) -> Option<PathBuf> {
            self.0.as_ref().map(|path| path.join("cgroup.procs"))
        }

        pub(crate) fn kill(&self) {
            if let Some(path) = &self.0 {
                // Kills every process in the cgroup at once, also those
                // that left the process group. Linux 5.14 or later.
                let _ = fs::write(path.join("cgroup.kill"), "1");
            }
        }

        pub(crate) fn peak_memory(&self) -> Option<u64> {
            let path = self.0.as_ref()?;
            fs::read_to_string(path.join("memory.peak"))
                .ok()?
                .trim()
                .parse()
                .ok()
        }

        pub(crate) fn remove(&self) {
            if let Some(path) = &self.0 {
                // Fails while processes are left; `kill` came first.
                let _ = fs::remove_dir(path);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    /// Without cgroups there is nothing to prepare.
    pub(crate) fn prepare(_mode: crate::config::CgroupMode) -> Option<std::path::PathBuf> {
        None
    }

    #[derive(Debug)]
    pub(crate) struct Cgroup;

    impl Cgroup {
        pub(crate) fn at(_base: Option<&std::path::Path>, _id: &str) -> Self {
            Self
        }

        #[expect(clippy::unused_self, reason = "same methods as on Linux")]
        pub(crate) fn procs(&self) -> Option<std::path::PathBuf> {
            None
        }

        #[expect(clippy::unused_self, reason = "same methods as on Linux")]
        pub(crate) fn kill(&self) {}

        #[expect(clippy::unused_self, reason = "same methods as on Linux")]
        pub(crate) fn peak_memory(&self) -> Option<u64> {
            None
        }

        #[expect(clippy::unused_self, reason = "same methods as on Linux")]
        pub(crate) fn remove(&self) {}
    }
}

pub(crate) use imp::{Cgroup, prepare};
