//! Choosing which worker runs a ready task.
//!
//! Like Kubernetes' scheduler, it works in two steps:
//!
//! 1. **Filter:** keep the workers that accept tasks, have every label the
//!    task asks for, and have enough free slots.
//! 2. **Score:** among those, pick the least loaded in proportion *after*
//!    placing the task, as Kubernetes' `LeastAllocated` does. Comparing the
//!    load after placement keeps a big task off a small worker that it
//!    would fill completely.
//!
//! Ties are broken round-robin by worker id rather than at random, so the
//! core stays deterministic and simulations always give the same result.
//!
//! ```text
//! task needs 2 slots
//! w1: 0 of 2 used  → after: 2/2 = 100%
//! w2: 1 of 8 used  → after: 3/8 =  37%   ← chosen
//! ```

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

/// Identity of a connected worker, assigned by the binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerId(u32);

impl WorkerId {
    /// The worker with this id.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The id as a number.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// What the master knows about a worker when choosing: its capacity, its
/// labels and whether it takes new tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    slots: u32,
    used: u32,
    labels: Vec<String>,
    accepting: bool,
}

impl Worker {
    /// A worker with `slots` slots, all free, that accepts tasks.
    #[must_use]
    pub fn new(slots: u32, labels: Vec<String>) -> Self {
        Self {
            slots,
            used: 0,
            labels,
            accepting: true,
        }
    }

    /// Total slots offered.
    #[must_use]
    pub fn slots(&self) -> u32 {
        self.slots
    }

    /// Slots taken by running tasks.
    #[must_use]
    pub fn used(&self) -> u32 {
        self.used
    }

    /// Slots still free.
    #[must_use]
    pub fn free(&self) -> u32 {
        // A worker that came back with fewer slots than its tasks hold.
        self.slots.saturating_sub(self.used)
    }

    /// The labels of the worker.
    #[must_use]
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// Whether the worker takes new tasks: false while it drains.
    #[must_use]
    pub fn accepting(&self) -> bool {
        self.accepting
    }

    /// Whether a task needing `slots` slots and every label in `labels`
    /// can start on this worker now.
    #[must_use]
    pub fn can_host(&self, slots: u32, labels: &[String]) -> bool {
        self.accepting && self.free() >= slots && labels.iter().all(|l| self.labels.contains(l))
    }
}

/// Why no worker can take a task right now, from the most lasting reason
/// to the most passing one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Blocker {
    /// No worker is connected at all.
    NoWorkers,
    /// No connected worker has every label the task asks for. Holds the
    /// labels no worker has; if each one is somewhere but never all on the
    /// same worker, it holds them all.
    MissingLabels(Vec<String>),
    /// The workers with the labels are all smaller than the task.
    TooFewSlots {
        /// Slots the task takes.
        needed: u32,
        /// The most slots any worker with the labels has.
        largest: u32,
    },
    /// The workers that could host it take nothing new: they drain, or are
    /// short of disk or memory.
    NotAccepting,
    /// It fits, but the workers that fit are full for now.
    Busy,
}

/// Compares the load of two workers after placing a task of `slots` slots
/// on each: `Less` means `a` would be less loaded than `b`.
///
/// The load is `(used + slots) / total`. Comparing the two fractions by
/// cross-multiplication avoids floating point, whose rounding could make
/// equal loads look different.
fn compare_load(a: &Worker, b: &Worker, slots: u32) -> Ordering {
    let a_load = (u64::from(a.used) + u64::from(slots)) * u64::from(b.slots);
    let b_load = (u64::from(b.used) + u64::from(slots)) * u64::from(a.slots);
    a_load.cmp(&b_load)
}

/// Why a change to the pool was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerError {
    /// A worker with this id is already in the pool.
    AlreadyPresent(WorkerId),
    /// No worker with this id is in the pool.
    Unknown(WorkerId),
    /// More slots released than the worker has in use.
    ReleaseTooMany {
        /// The worker.
        worker: WorkerId,
        /// Slots in use.
        used: u32,
        /// Slots asked to release.
        release: u32,
    },
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyPresent(id) => write!(f, "worker {} is already connected", id.0),
            Self::Unknown(id) => write!(f, "worker {} is not connected", id.0),
            Self::ReleaseTooMany {
                worker,
                used,
                release,
            } => write!(
                f,
                "worker {} has {used} slots in use, cannot release {release}",
                worker.0
            ),
        }
    }
}

impl Error for WorkerError {}

/// The connected workers, and the round-robin position for ties.
#[derive(Debug, Clone, Default)]
pub struct WorkerPool {
    /// Workers by id; a `BTreeMap` keeps them in id order for round-robin.
    workers: BTreeMap<WorkerId, Worker>,
    /// The worker chosen last, where the next tie-break starts from.
    last: Option<WorkerId>,
}

impl WorkerPool {
    /// An empty pool.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The worker with this id, if connected.
    #[must_use]
    pub fn get(&self, id: WorkerId) -> Option<&Worker> {
        self.workers.get(&id)
    }

    /// Adds a worker that just connected.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::AlreadyPresent`] if the id is taken.
    pub fn add(&mut self, id: WorkerId, worker: Worker) -> Result<(), WorkerError> {
        if self.workers.contains_key(&id) {
            return Err(WorkerError::AlreadyPresent(id));
        }
        self.workers.insert(id, worker);
        Ok(())
    }

    /// Removes a worker that left, returning what was known about it.
    pub fn remove(&mut self, id: WorkerId) -> Option<Worker> {
        self.workers.remove(&id)
    }

    /// Sets whether a worker takes new tasks, from its heartbeat or a drain.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Unknown`] if the worker is not connected.
    pub fn set_accepting(&mut self, id: WorkerId, accepting: bool) -> Result<(), WorkerError> {
        let worker = self.workers.get_mut(&id).ok_or(WorkerError::Unknown(id))?;
        worker.accepting = accepting;
        Ok(())
    }

    /// Takes `slots` slots on a worker for a task it is already running,
    /// as a worker that reconnects says in its `Hello`. Unlike
    /// [`select`](Self::select) it does not check that they are free.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Unknown`] if the worker is not connected.
    pub fn reserve(&mut self, id: WorkerId, slots: u32) -> Result<(), WorkerError> {
        let worker = self.workers.get_mut(&id).ok_or(WorkerError::Unknown(id))?;
        worker.used = worker.used.saturating_add(slots);
        Ok(())
    }

    /// The connected workers, in id order.
    pub fn iter(&self) -> impl Iterator<Item = (WorkerId, &Worker)> {
        self.workers.iter().map(|(&id, worker)| (id, worker))
    }

    /// Frees `slots` slots on a worker when one of its tasks ends.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerError::Unknown`] if the worker is not connected, or
    /// [`WorkerError::ReleaseTooMany`] if fewer slots are in use.
    pub fn release(&mut self, id: WorkerId, slots: u32) -> Result<(), WorkerError> {
        let worker = self.workers.get_mut(&id).ok_or(WorkerError::Unknown(id))?;
        if slots > worker.used {
            return Err(WorkerError::ReleaseTooMany {
                worker: id,
                used: worker.used,
                release: slots,
            });
        }
        worker.used -= slots;
        Ok(())
    }

    /// Whether some worker takes new tasks and has a free slot: when not,
    /// no task can be placed and there is no point in trying.
    #[must_use]
    pub fn has_free_slots(&self) -> bool {
        self.workers.values().any(|w| w.accepting && w.free() > 0)
    }

    /// Why a task needing `slots` slots and `labels` cannot start on any
    /// worker now, or `None` if some worker can host it.
    #[must_use]
    pub fn blocker(&self, slots: u32, labels: &[String]) -> Option<Blocker> {
        if self.workers.is_empty() {
            return Some(Blocker::NoWorkers);
        }
        let with_labels: Vec<&Worker> = self
            .workers
            .values()
            .filter(|worker| labels.iter().all(|label| worker.labels.contains(label)))
            .collect();
        if with_labels.is_empty() {
            let mut missing: Vec<String> = labels
                .iter()
                .filter(|label| self.workers.values().all(|w| !w.labels.contains(label)))
                .cloned()
                .collect();
            if missing.is_empty() {
                missing = labels.to_vec();
            }
            return Some(Blocker::MissingLabels(missing));
        }
        let big_enough: Vec<&&Worker> = with_labels.iter().filter(|w| w.slots >= slots).collect();
        if big_enough.is_empty() {
            let largest = with_labels.iter().map(|w| w.slots).max().unwrap_or(0);
            return Some(Blocker::TooFewSlots {
                needed: slots,
                largest,
            });
        }
        if big_enough.iter().all(|w| !w.accepting) {
            return Some(Blocker::NotAccepting);
        }
        if big_enough.iter().any(|w| w.can_host(slots, labels)) {
            return None;
        }
        Some(Blocker::Busy)
    }

    /// Chooses a worker for a task needing `slots` slots and `labels`, and
    /// reserves the slots on it. Returns `None` if no worker can host it.
    ///
    /// # Panics
    ///
    /// Panics if the chosen worker is no longer in the pool, which would be
    /// a bug in this function.
    pub fn select(&mut self, slots: u32, labels: &[String]) -> Option<WorkerId> {
        let workers = self
            .workers
            .iter()
            .filter(|(_, w)| w.can_host(slots, labels));
        let mut best: Option<(WorkerId, &Worker)> = None;

        // Workers come in id order, so the first tied worker after `last`
        // replaces any tied worker at or before it.
        for (&id, worker) in workers {
            match best {
                None => best = Some((id, worker)),
                Some((best_id, best_worker)) => match compare_load(worker, best_worker, slots) {
                    Ordering::Less => best = Some((id, worker)),
                    Ordering::Equal => {
                        if let Some(last) = self.last {
                            if best_id <= last && id > last {
                                best = Some((id, worker));
                            }
                        }
                    }
                    Ordering::Greater => {}
                },
            }
        }

        let (id, _) = best?;
        self.workers
            .get_mut(&id)
            .expect("the chosen worker is in the pool")
            .used += slots;
        self.last = Some(id);
        Some(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(names: &[&str]) -> Vec<String> {
        names.iter().map(|&n| n.to_owned()).collect()
    }

    /// A worker with `used` of `slots` taken and the given labels.
    fn worker(slots: u32, used: u32, names: &[&str]) -> Worker {
        Worker {
            used,
            ..Worker::new(slots, labels(names))
        }
    }

    fn w(id: u32) -> WorkerId {
        WorkerId::new(id)
    }

    fn pool(workers: Vec<(u32, Worker)>) -> WorkerPool {
        let mut pool = WorkerPool::new();
        for (id, worker) in workers {
            pool.add(w(id), worker).unwrap();
        }
        pool
    }

    // Already working

    #[test]
    fn tasks_started_together_spread_over_unequal_workers() {
        let mut pool = pool(vec![(0, worker(2, 0, &[])), (1, worker(4, 0, &["big"]))]);
        let chosen: Vec<WorkerId> = (0..3).map(|_| pool.select(1, &[]).unwrap()).collect();
        assert_eq!(chosen, [w(1), w(0), w(1)]);
    }

    #[test]
    fn adding_the_same_worker_twice_is_an_error() {
        let mut pool = pool(vec![(1, worker(4, 0, &[]))]);
        assert_eq!(
            pool.add(w(1), worker(4, 0, &[])),
            Err(WorkerError::AlreadyPresent(w(1)))
        );
    }

    #[test]
    fn release_frees_slots() {
        let mut pool = pool(vec![(1, worker(4, 3, &[]))]);
        pool.release(w(1), 2).unwrap();
        assert_eq!(pool.get(w(1)).unwrap().free(), 3);
    }

    #[test]
    fn release_more_than_used_is_an_error() {
        let mut pool = pool(vec![(1, worker(4, 1, &[]))]);
        assert_eq!(
            pool.release(w(1), 2),
            Err(WorkerError::ReleaseTooMany {
                worker: w(1),
                used: 1,
                release: 2
            })
        );
    }

    #[test]
    fn unknown_worker_is_an_error() {
        let mut pool = WorkerPool::new();
        assert_eq!(pool.release(w(9), 1), Err(WorkerError::Unknown(w(9))));
        assert_eq!(
            pool.set_accepting(w(9), false),
            Err(WorkerError::Unknown(w(9)))
        );
    }

    // TASK 11

    #[test]
    fn can_host_with_free_slots_and_labels() {
        let w = worker(4, 1, &["big", "docker"]);
        assert!(w.can_host(3, &labels(&["big"])));
        assert!(w.can_host(1, &[]), "a task with no labels fits anywhere");
    }

    #[test]
    fn cannot_host_without_enough_free_slots() {
        assert!(!worker(4, 3, &[]).can_host(2, &[]));
    }

    #[test]
    fn cannot_host_without_every_label() {
        let w = worker(4, 0, &["big"]);
        assert!(!w.can_host(1, &labels(&["big", "gpu"])));
    }

    #[test]
    fn cannot_host_while_not_accepting() {
        let mut w = worker(4, 0, &[]);
        w.accepting = false;
        assert!(!w.can_host(1, &[]));
    }

    // TASK 12

    #[test]
    fn load_compares_proportions() {
        // After one more slot: 2/4 = 50% against 2/8 = 25%.
        assert_eq!(
            compare_load(&worker(4, 1, &[]), &worker(8, 1, &[]), 1),
            Ordering::Greater
        );
        assert_eq!(
            compare_load(&worker(8, 1, &[]), &worker(4, 1, &[]), 1),
            Ordering::Less
        );
    }

    #[test]
    fn load_counts_the_task_being_placed() {
        // Before: 0/2 against 1/8. After two slots: 2/2 against 3/8.
        assert_eq!(
            compare_load(&worker(2, 0, &[]), &worker(8, 1, &[]), 2),
            Ordering::Greater
        );
    }

    #[test]
    fn equal_proportions_are_equal() {
        // After one more slot: 1/2 and 2/4 are both 50%.
        assert_eq!(
            compare_load(&worker(2, 0, &[]), &worker(4, 1, &[]), 1),
            Ordering::Equal
        );
    }

    #[test]
    fn large_slot_counts_do_not_overflow() {
        let big = worker(u32::MAX, u32::MAX - 1, &[]);
        assert_eq!(compare_load(&big, &big, 1), Ordering::Equal);
    }

    // TASK 13 (needs 11 and 12)

    #[test]
    fn selects_the_least_loaded_and_reserves_slots() {
        let mut pool = pool(vec![(1, worker(2, 0, &[])), (2, worker(8, 1, &[]))]);
        assert_eq!(pool.select(2, &[]), Some(w(2)));
        assert_eq!(pool.get(w(2)).unwrap().used(), 3);
        assert_eq!(pool.get(w(1)).unwrap().used(), 0);
    }

    #[test]
    fn selects_only_workers_with_the_labels() {
        let mut pool = pool(vec![(1, worker(8, 0, &[])), (2, worker(8, 6, &["big"]))]);
        assert_eq!(pool.select(1, &labels(&["big"])), Some(w(2)));
    }

    #[test]
    fn selects_nothing_when_no_worker_fits() {
        let mut pool = pool(vec![(1, worker(2, 1, &[]))]);
        assert_eq!(pool.select(2, &[]), None);
        assert_eq!(WorkerPool::new().select(1, &[]), None);
    }

    #[test]
    fn ties_take_turns_by_id() {
        let mut pool = pool(vec![
            (1, worker(4, 0, &[])),
            (2, worker(4, 0, &[])),
            (3, worker(4, 0, &[])),
        ]);
        // Release after each choice, so the three stay equally loaded.
        let mut chosen = Vec::new();
        for _ in 0..4 {
            let id = pool.select(1, &[]).unwrap();
            pool.release(id, 1).unwrap();
            chosen.push(id);
        }
        assert_eq!(chosen, [w(1), w(2), w(3), w(1)]);
    }

    #[test]
    fn ties_skip_workers_that_are_not_tied() {
        // 2 is busier, so the turn goes from 1 straight to 3.
        let mut pool = pool(vec![
            (1, worker(4, 0, &[])),
            (2, worker(4, 2, &[])),
            (3, worker(4, 0, &[])),
        ]);
        assert_eq!(pool.select(1, &[]), Some(w(1)));
        pool.release(w(1), 1).unwrap();
        assert_eq!(pool.select(1, &[]), Some(w(3)));
    }

    #[test]
    fn the_blocker_says_why_nothing_fits() {
        let labels =
            |names: &[&str]| -> Vec<String> { names.iter().map(|&n| n.to_owned()).collect() };
        let mut pool = WorkerPool::new();
        assert_eq!(pool.blocker(1, &[]), Some(Blocker::NoWorkers));

        pool.add(WorkerId::new(1), Worker::new(2, labels(&["big"])))
            .unwrap();
        pool.add(WorkerId::new(2), Worker::new(4, labels(&["gpu"])))
            .unwrap();
        assert_eq!(pool.blocker(1, &labels(&["big"])), None);
        assert_eq!(
            pool.blocker(1, &labels(&["big", "tpu"])),
            Some(Blocker::MissingLabels(labels(&["tpu"]))),
            "the label nobody has"
        );
        assert_eq!(
            pool.blocker(1, &labels(&["big", "gpu"])),
            Some(Blocker::MissingLabels(labels(&["big", "gpu"]))),
            "each label is somewhere, never both together"
        );
        assert_eq!(
            pool.blocker(3, &labels(&["big"])),
            Some(Blocker::TooFewSlots {
                needed: 3,
                largest: 2
            })
        );

        assert_eq!(pool.select(2, &labels(&["big"])), Some(WorkerId::new(1)));
        assert_eq!(pool.blocker(1, &labels(&["big"])), Some(Blocker::Busy));
        pool.set_accepting(WorkerId::new(2), false).unwrap();
        assert_eq!(
            pool.blocker(1, &labels(&["gpu"])),
            Some(Blocker::NotAccepting)
        );
    }
}
