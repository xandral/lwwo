//! The in-memory alarms of the master: retries and heartbeats.
//!
//! Cron fires live in the database timetable; everything else that must
//! happen at an instant lives here, ordered by time. The master arms its
//! single timer on the earlier of [`Deadlines::next`] and the next cron
//! fire, and when it goes off takes everything due with
//! [`Deadlines::pop_due`].
//!
//! Task timeouts are not here: the worker enforces them, so they hold even
//! while the master is down.
//!
//! Each deadline is set at most once: setting it again moves it. A worker's
//! heartbeat deadline moves forward at every heartbeat, and a retry goes
//! away if its run is cancelled first.
//!
//! ```text
//! set Heartbeat(W1) at 10:00:30     by_time: [(10:00:30, W1)]
//! set Retry(A) at 10:05             by_time: [(10:00:30, W1), (10:05, A)]
//! set Heartbeat(W1) at 10:00:40     by_time: [(10:00:40, W1), (10:05, A)]
//! pop_due(10:01) → [Heartbeat(W1)]  by_time: [(10:05, A)]
//! ```

use std::collections::{BTreeSet, HashMap};

use crate::dag::TaskId;
use crate::run::RunId;
use crate::time::Timestamp;
use crate::worker_selection::WorkerId;

/// Something that must happen at an instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Deadline {
    /// A failed task may try again.
    Retry {
        /// The run.
        run: RunId,
        /// The task.
        task: TaskId,
    },
    /// A worker has not sent a heartbeat for too long.
    Heartbeat(WorkerId),
}

/// Deadlines ordered by instant, each set at most once.
#[derive(Debug, Clone, Default)]
pub struct Deadlines {
    /// Every deadline with its instant, earliest first. Ties come out in
    /// the order of [`Deadline`], so the order is always the same.
    by_time: BTreeSet<(Timestamp, Deadline)>,
    /// The instant of each deadline in `by_time`: a deadline is a key here
    /// exactly when it is in `by_time`, with the same instant.
    at: HashMap<Deadline, Timestamp>,
}

impl Deadlines {
    /// No deadlines.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of deadlines.
    #[must_use]
    pub fn len(&self) -> usize {
        self.at.len()
    }

    /// Whether there are no deadlines.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.at.is_empty()
    }

    /// When `deadline` is set for, if it is set.
    #[must_use]
    pub fn get(&self, deadline: Deadline) -> Option<Timestamp> {
        self.at.get(&deadline).copied()
    }

    /// The earliest instant of any deadline: where to arm the timer.
    #[must_use]
    pub fn next(&self) -> Option<Timestamp> {
        self.by_time.first().map(|&(instant, _)| instant)
    }

    /// Sets `deadline` for `instant`, moving it if it was already set.
    pub fn set(&mut self, deadline: Deadline, instant: Timestamp) {
        // A deadline that was already set leaves its old place first.
        if let Some(old) = self.at.insert(deadline, instant) {
            self.by_time.remove(&(old, deadline));
        }
        self.by_time.insert((instant, deadline));
    }

    /// Removes `deadline`, returning whether it was set.
    pub fn remove(&mut self, deadline: Deadline) -> bool {
        let Some(instant) = self.at.remove(&deadline) else {
            return false;
        };
        self.by_time.remove(&(instant, deadline));
        true
    }

    /// Removes and returns every deadline due at or before `now`, earliest
    /// first.
    pub fn pop_due(&mut self, now: Timestamp) -> Vec<Deadline> {
        let mut due = Vec::new();
        // by_time is ordered, so the first deadline after `now` ends the scan.
        while let Some(&(instant, deadline)) = self.by_time.first() {
            if instant > now {
                break;
            }
            self.by_time.pop_first();
            self.at.remove(&deadline);
            due.push(deadline);
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RUN: RunId = RunId::new(1);
    const W1: Deadline = Deadline::Heartbeat(WorkerId::new(1));
    const W2: Deadline = Deadline::Heartbeat(WorkerId::new(2));

    fn retry(task: u32) -> Deadline {
        Deadline::Retry {
            run: RUN,
            task: TaskId::new(task),
        }
    }

    fn t(millis: i64) -> Timestamp {
        Timestamp::from_millis(millis)
    }

    // Already working

    #[test]
    fn new_deadlines_are_empty() {
        let deadlines = Deadlines::new();
        assert!(deadlines.is_empty());
        assert_eq!(deadlines.next(), None);
    }

    // TASK 21

    #[test]
    fn next_is_the_earliest() {
        let mut deadlines = Deadlines::new();
        deadlines.set(retry(0), t(300));
        deadlines.set(W1, t(100));
        deadlines.set(W2, t(200));
        assert_eq!(deadlines.len(), 3);
        assert_eq!(deadlines.next(), Some(t(100)));
        assert_eq!(deadlines.get(W2), Some(t(200)));
    }

    #[test]
    fn setting_again_moves_the_deadline() {
        let mut deadlines = Deadlines::new();
        deadlines.set(W1, t(100));
        deadlines.set(W2, t(200));
        deadlines.set(W1, t(300));
        assert_eq!(deadlines.len(), 2);
        assert_eq!(deadlines.get(W1), Some(t(300)));
        assert_eq!(deadlines.next(), Some(t(200)));
    }

    // TASK 22 (needs 21)

    #[test]
    fn remove_takes_the_deadline_away() {
        let mut deadlines = Deadlines::new();
        deadlines.set(W1, t(100));
        deadlines.set(retry(0), t(200));
        assert!(deadlines.remove(W1));
        assert_eq!(deadlines.len(), 1);
        assert_eq!(deadlines.get(W1), None);
        assert_eq!(deadlines.next(), Some(t(200)));
    }

    #[test]
    fn removing_a_deadline_that_is_not_set_is_a_no_op() {
        let mut deadlines = Deadlines::new();
        deadlines.set(W1, t(100));
        assert!(!deadlines.remove(W2));
        assert_eq!(deadlines.len(), 1);
    }

    // TASK 23 (needs 21)

    #[test]
    fn pop_due_takes_everything_up_to_now_in_order() {
        let mut deadlines = Deadlines::new();
        deadlines.set(retry(0), t(300));
        deadlines.set(W2, t(200));
        deadlines.set(W1, t(100));
        assert_eq!(deadlines.pop_due(t(200)), [W1, W2]);
        assert_eq!(deadlines.len(), 1);
        assert_eq!(deadlines.get(W1), None);
        assert_eq!(deadlines.next(), Some(t(300)));
    }

    #[test]
    fn pop_due_before_anything_is_due_takes_nothing() {
        let mut deadlines = Deadlines::new();
        deadlines.set(W1, t(100));
        assert!(deadlines.pop_due(t(99)).is_empty());
        assert_eq!(deadlines.len(), 1);
    }

    #[test]
    fn a_moved_deadline_is_due_only_at_its_new_instant() {
        let mut deadlines = Deadlines::new();
        deadlines.set(W1, t(100));
        deadlines.set(W1, t(300));
        assert!(deadlines.pop_due(t(200)).is_empty());
        assert_eq!(deadlines.pop_due(t(300)), [W1]);
        assert!(deadlines.is_empty());
    }

    #[test]
    fn ties_come_out_in_a_fixed_order() {
        let mut deadlines = Deadlines::new();
        deadlines.set(W2, t(100));
        deadlines.set(retry(0), t(100));
        deadlines.set(W1, t(100));
        assert_eq!(deadlines.pop_due(t(100)), [retry(0), W1, W2]);
    }
}
