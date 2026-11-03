//! The queue of tasks ready to be sent to a worker, shared by all runs.
//!
//! Runs take turns: each `pop` serves the next run in line, and within a run
//! tasks come out in the order they became ready. A run with a thousand
//! ready tasks therefore cannot make a run with three wait behind all of
//! them.
//!
//! ```text
//! push  A1 A2 A3 B1         turns: [A, B]
//! pop → A1                  turns: [B, A]
//! pop → B1                  turns: [A]       B has nothing left
//! pop → A2, then A3
//! ```

use std::collections::{HashMap, VecDeque};

use crate::dag::TaskId;
use crate::run::RunId;

/// Ready tasks of all runs, served round-robin across runs.
#[derive(Debug, Clone, Default)]
pub struct ReadyQueue {
    /// Runs with at least one ready task, in the order they will be served.
    turns: VecDeque<RunId>,
    /// Ready tasks of each run in `turns`, oldest first. A run is a key here
    /// exactly when it is in `turns`.
    tasks: HashMap<RunId, VecDeque<TaskId>>,
    /// Total number of ready tasks.
    len: usize,
}

impl ReadyQueue {
    /// An empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of ready tasks, across all runs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no task is ready.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds a ready task of `run`.
    ///
    /// A run with no ready tasks joins the end of the line. Each task must
    /// be pushed once: [`Run::succeed`](crate::run::Run::succeed) returns
    /// every newly ready task exactly once.
    pub fn push(&mut self, run: RunId, task: TaskId) {
        let run_tasks = self.tasks.entry(run).or_default();
        if run_tasks.is_empty() {
            self.turns.push_back(run);
        }
        run_tasks.push_back(task);
        self.len += 1;
    }

    /// Takes the next ready task, from the run whose turn it is.
    ///
    /// The run goes to the end of the line if it still has ready tasks, and
    /// leaves the line otherwise.
    ///
    /// # Panics
    ///
    /// Panics if a run in `turns` has no ready tasks, which would be a bug
    /// in this module.
    pub fn pop(&mut self) -> Option<(RunId, TaskId)> {
        let run = self.turns.pop_front()?;
        let run_tasks = self
            .tasks
            .get_mut(&run)
            .expect("every run in turns has tasks");
        let task = run_tasks.pop_front().expect("every run in turns has tasks");

        if run_tasks.is_empty() {
            self.tasks.remove(&run);
        } else {
            self.turns.push_back(run);
        }

        self.len -= 1;
        Some((run, task))
    }

    /// Removes every ready task of `run`, for example when the run is
    /// cancelled, and returns them oldest first. Other runs keep their turn.
    pub fn remove_run(&mut self, run: RunId) -> Vec<TaskId> {
        // The run was not in the queue: nothing to remove.
        let Some(tasks) = self.tasks.remove(&run) else {
            return Vec::new();
        };

        self.turns.retain(|&r| r != run);
        self.len -= tasks.len();

        tasks.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: RunId = RunId::new(1);
    const B: RunId = RunId::new(2);
    const C: RunId = RunId::new(3);

    fn t(index: u32) -> TaskId {
        TaskId::new(index)
    }

    /// Pops everything left in the queue.
    fn drain(queue: &mut ReadyQueue) -> Vec<(RunId, TaskId)> {
        std::iter::from_fn(|| queue.pop()).collect()
    }

    // Already working

    #[test]
    fn new_queue_is_empty() {
        let queue = ReadyQueue::new();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
    }

    // TASK 8 and 9

    #[test]
    fn empty_queue_pops_nothing() {
        assert_eq!(ReadyQueue::new().pop(), None);
    }

    #[test]
    fn one_run_is_first_in_first_out() {
        let mut queue = ReadyQueue::new();
        queue.push(A, t(0));
        queue.push(A, t(1));
        queue.push(A, t(2));
        assert_eq!(queue.len(), 3);
        assert_eq!(drain(&mut queue), [(A, t(0)), (A, t(1)), (A, t(2))]);
        assert!(queue.is_empty());
    }

    #[test]
    fn runs_take_turns() {
        let mut queue = ReadyQueue::new();
        queue.push(A, t(0));
        queue.push(A, t(1));
        queue.push(A, t(2));
        queue.push(B, t(0));
        queue.push(C, t(0));
        queue.push(C, t(1));
        assert_eq!(
            drain(&mut queue),
            [
                (A, t(0)),
                (B, t(0)),
                (C, t(0)),
                (A, t(1)),
                (C, t(1)),
                (A, t(2)),
            ]
        );
    }

    #[test]
    fn run_that_ran_out_joins_the_end_of_the_line() {
        let mut queue = ReadyQueue::new();
        queue.push(A, t(0));
        queue.push(B, t(0));
        assert_eq!(queue.pop(), Some((A, t(0))));

        // A had nothing left, so it queues up again behind B.
        queue.push(A, t(1));
        assert_eq!(drain(&mut queue), [(B, t(0)), (A, t(1))]);
    }

    #[test]
    fn len_follows_push_and_pop() {
        let mut queue = ReadyQueue::new();
        queue.push(A, t(0));
        queue.push(B, t(0));
        assert_eq!(queue.len(), 2);
        queue.pop();
        assert_eq!(queue.len(), 1);
        queue.pop();
        assert!(queue.is_empty());
    }

    // TASK 10 (needs 8 and 9)

    #[test]
    fn remove_run_returns_its_tasks_and_keeps_the_others() {
        let mut queue = ReadyQueue::new();
        queue.push(A, t(0));
        queue.push(B, t(0));
        queue.push(A, t(1));
        queue.push(C, t(0));

        assert_eq!(queue.remove_run(A), [t(0), t(1)]);
        assert_eq!(queue.len(), 2);
        assert_eq!(drain(&mut queue), [(B, t(0)), (C, t(0))]);
    }

    #[test]
    fn remove_unknown_run_is_a_no_op() {
        let mut queue = ReadyQueue::new();
        queue.push(A, t(0));
        assert!(queue.remove_run(B).is_empty());
        assert_eq!(queue.len(), 1);
    }
}
