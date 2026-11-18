//! Waking the followers of attempts when their journal changes.
//!
//! The shim of an attempt talks to the worker only through files in the
//! attempt's directory. One kernel watcher (inotify on Linux, FSEvents on
//! macOS) covers all those directories, so a follower looks at its files
//! as soon as one of them changes instead of at fixed intervals. One
//! watcher for all attempts, because the kernel limits how many a user
//! may open.
//!
//! Some changes come with no file event, such as a shim that dies, so a
//! follower also looks every [`LOOK_ANYWAY`]. If the watcher cannot be
//! started, or cannot watch a directory, that attempt is looked at every
//! [`LOOK_WITHOUT_EVENTS`] instead.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::Notify;
use tracing::warn;

/// How often an attempt is looked at even without file events.
const LOOK_ANYWAY: Duration = Duration::from_secs(2);
/// How often an attempt is looked at when its directory is not watched.
const LOOK_WITHOUT_EVENTS: Duration = Duration::from_millis(250);

type Followers = Arc<Mutex<Vec<(PathBuf, Arc<Notify>)>>>;

/// The watcher shared by all attempts of a worker.
pub(crate) struct Wakeups {
    watcher: Option<Mutex<RecommendedWatcher>>,
    followers: Followers,
}

impl fmt::Debug for Wakeups {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Wakeups")
            .field("watching", &self.watcher.is_some())
            .finish_non_exhaustive()
    }
}

impl Wakeups {
    /// Starts the watcher; without one, every attempt is polled.
    pub(crate) fn start() -> Self {
        let followers: Followers = Arc::default();
        let wake = Arc::clone(&followers);
        let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let followers = wake.lock().unwrap_or_else(PoisonError::into_inner);
            let paths = event.map(|event| event.paths).unwrap_or_default();
            let mut woken = false;
            for (dir, notify) in followers.iter() {
                if paths.iter().any(|path| path.starts_with(dir)) {
                    notify.notify_one();
                    woken = true;
                }
            }
            // An error, or a path written differently from the watched
            // one: waking everybody costs one look each.
            if !woken {
                for (_, notify) in followers.iter() {
                    notify.notify_one();
                }
            }
        });
        let watcher = match watcher {
            Ok(watcher) => Some(Mutex::new(watcher)),
            Err(error) => {
                warn!(
                    "cannot watch attempt files, looking at them every {LOOK_WITHOUT_EVENTS:?}: {error}"
                );
                None
            }
        };
        Self { watcher, followers }
    }

    /// Starts watching the directory of an attempt, until the returned
    /// handle is dropped.
    pub(crate) fn watch(&self, dir: &Path) -> Watch<'_> {
        // Events carry the real path (on macOS `/var` is `/private/var`).
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        let notify = Arc::new(Notify::new());
        self.followers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((dir.clone(), Arc::clone(&notify)));
        let watched = self.watcher.as_ref().is_some_and(|watcher| {
            match watcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .watch(&dir, RecursiveMode::NonRecursive)
            {
                Ok(()) => true,
                Err(error) => {
                    warn!("cannot watch {}: {error}", dir.display());
                    false
                }
            }
        });
        Watch {
            wakeups: self,
            dir,
            notify,
            every: if watched {
                LOOK_ANYWAY
            } else {
                LOOK_WITHOUT_EVENTS
            },
        }
    }
}

/// One watched attempt directory.
pub(crate) struct Watch<'a> {
    wakeups: &'a Wakeups,
    dir: PathBuf,
    notify: Arc<Notify>,
    every: Duration,
}

impl Watch<'_> {
    /// Waits until a file of the attempt changes, or for a while.
    ///
    /// A change that comes while nobody waits is kept, so a change between
    /// two looks is never missed.
    pub(crate) async fn changed(&self) {
        tokio::select! {
            () = self.notify.notified() => {}
            () = tokio::time::sleep(self.every) => {}
        }
    }
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        if let Some(watcher) = &self.wakeups.watcher {
            // The directory may be gone already, and its watch with it.
            let _ = watcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .unwatch(&self.dir);
        }
        self.wakeups
            .followers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(_, notify)| !Arc::ptr_eq(notify, &self.notify));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_new_file_wakes_the_follower_at_once() {
        let dir = std::env::temp_dir().join(format!("lwwo-wake-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let wakeups = Wakeups::start();
        let watch = wakeups.watch(&dir);
        assert_eq!(watch.every, LOOK_ANYWAY, "the directory is watched");

        std::fs::write(dir.join("exit.json"), "{}").unwrap();
        tokio::time::timeout(Duration::from_secs(1), watch.changed())
            .await
            .expect("woken by the file, not by the two-second look");

        drop(watch);
        assert!(wakeups.followers.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
