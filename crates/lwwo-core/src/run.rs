//! The state of one run: which tasks are waiting, running or done.
//!
//! A run is Kahn's algorithm spread over time. Each task has a counter of
//! missing dependencies; when a task succeeds its successors' counters drop
//! by one, and a task whose counter reaches zero is ready. When a task fails,
//! everything that depends on it, directly or not, is skipped, while
//! independent branches go on.
//!
//! A task with `retries` left does not fail at once: it waits in
//! `Retrying`, then goes back to `Blocked` to be assigned again.
//!
//! ```text
//! Blocked  ── assign ──→  Assigned  ── succeed ──→  Success
//!                         Assigned  ── fail, attempts left ──→  Retrying
//!                         Assigned  ── fail, none left ──→  Failed (descendants Skipped)
//! Assigned  ── requeue (worker lost) ──→  Blocked
//! Retrying  ── retry_due ──→  Blocked
//! any open task  ── cancel ──→  Cancelled
//! ```
//!
//! Only failures use up retries: an attempt lost with its worker is
//! counted in `attempts`, but the task goes back to the queue as it was.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::dag::{Graph, TaskId};

/// Identity of a run: a `UUIDv7` held as its 128-bit number.
///
/// The binary generates the UUID and converts it; the core only uses it as a
/// label, so it needs no UUID dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(u128);

impl RunId {
    /// The run with this 128-bit id.
    #[must_use]
    pub const fn new(id: u128) -> Self {
        Self(id)
    }

    /// The id as a 128-bit number.
    #[must_use]
    pub const fn as_u128(self) -> u128 {
        self.0
    }
}

/// Current state of a task within a run, as stored in `task_runs.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Waiting for its dependencies or for a free slot.
    Blocked,
    /// Sent to a worker: running or about to start.
    Assigned,
    /// The last attempt failed and another one will start after a delay.
    Retrying,
    /// Finished successfully.
    Success,
    /// Failed with no attempts left.
    Failed,
    /// Not run because a task it depends on failed.
    Skipped,
    /// Not finished because the run was cancelled.
    Cancelled,
}

impl TaskState {
    /// Whether the task will not change any more in this run.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }
}

/// How a finished run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Every task succeeded.
    Success,
    /// At least one task failed.
    Failed,
    /// The run was cancelled before it finished.
    Cancelled,
}

/// What a failed attempt leads to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// The task has attempts left: it is `Retrying`, and should be retried
    /// with [`Run::retry_due`] after this delay.
    Retry {
        /// How long to wait before the next attempt.
        delay: Duration,
    },
    /// The task failed for good.
    Final {
        /// The tasks skipped because they depend on it, in any order.
        skipped: Vec<TaskId>,
    },
}

/// A change that does not fit the current state of a task.
///
/// Delivery from workers is at-least-once, so the same report can arrive
/// twice: the caller is expected to log and ignore these, not to crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunError {
    /// The task is not ready to be assigned.
    NotReady {
        /// The task.
        task: TaskId,
        /// Its current state.
        state: TaskState,
    },
    /// The task is not running, so it cannot succeed or fail.
    NotAssigned {
        /// The task.
        task: TaskId,
        /// Its current state.
        state: TaskState,
    },
    /// The task is not waiting for a retry.
    NotRetrying {
        /// The task.
        task: TaskId,
        /// Its current state.
        state: TaskState,
    },
    /// The run still has open tasks, so it cannot be run again yet.
    NotFinished,
    /// A snapshot does not have one entry per task of the graph.
    WrongTaskCount {
        /// Tasks in the graph.
        expected: usize,
        /// Entries in the snapshot.
        found: usize,
    },
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReady { task, state } => {
                write!(f, "task {} is not ready: it is {state:?}", task.index())
            }
            Self::NotAssigned { task, state } => {
                write!(f, "task {} is not running: it is {state:?}", task.index())
            }
            Self::NotRetrying { task, state } => {
                write!(
                    f,
                    "task {} is not waiting to retry: it is {state:?}",
                    task.index()
                )
            }
            Self::NotFinished => f.write_str("the run is not finished"),
            Self::WrongTaskCount { expected, found } => {
                write!(f, "the graph has {expected} tasks, the snapshot {found}")
            }
        }
    }
}

impl Error for RunError {}

/// What the database knows of a task, for loading a run back into memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskSnapshot {
    /// `task_runs.state`.
    pub state: TaskState,
    /// `task_runs.attempts`.
    pub attempts: u32,
    /// Attempts that ended as failed or timed out since the task was last
    /// started from scratch: what retries are counted against.
    pub failures: u32,
}

/// Which tasks of a finished run to run again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rerun {
    /// The tasks that did not succeed: failed, skipped or cancelled.
    Failed,
    /// One task.
    Task {
        /// The task.
        task: TaskId,
        /// Also every task that depends on it, directly or not.
        downstream: bool,
    },
    /// Every task.
    All,
}

/// One run of a DAG version.
///
/// The [`Graph`] is shared by all runs of the same version through an
/// [`Arc`]: it stays in memory exactly as long as some run still uses it.
#[derive(Debug, Clone)]
pub struct Run {
    id: RunId,
    graph: Arc<Graph>,
    /// Dependencies not yet succeeded, indexed by task.
    missing: Vec<usize>,
    /// State of each task, indexed by task.
    states: Vec<TaskState>,
    /// Attempts made so far, indexed by task: `task_runs.attempts`.
    attempts: Vec<u32>,
    /// Failed attempts, indexed by task: what retries are counted against.
    failures: Vec<u32>,
    /// Tasks not yet in a final state.
    open: usize,
    /// Whether some task has failed.
    any_failed: bool,
    /// Whether the run was cancelled.
    cancelled: bool,
}

impl Run {
    /// Starts a run: every task is blocked and waits for its dependencies.
    #[must_use]
    pub fn new(id: RunId, graph: Arc<Graph>) -> Self {
        Self {
            id,
            missing: graph.in_degree().to_vec(),
            states: vec![TaskState::Blocked; graph.len()],
            attempts: vec![0; graph.len()],
            failures: vec![0; graph.len()],
            open: graph.len(),
            any_failed: false,
            cancelled: false,
            graph,
        }
    }

    /// Loads a run back from the database, after a restart of the master
    /// or to run a finished run again.
    ///
    /// The counters are computed again from the states. A blocked task
    /// behind one that did not succeed can never start, so it is skipped.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::WrongTaskCount`] if `tasks` does not have one
    /// entry per task of `graph`, in graph order.
    pub fn restore(id: RunId, graph: Arc<Graph>, tasks: &[TaskSnapshot]) -> Result<Self, RunError> {
        if tasks.len() != graph.len() {
            return Err(RunError::WrongTaskCount {
                expected: graph.len(),
                found: tasks.len(),
            });
        }
        let mut run = Self::new(id, graph);
        for (i, task) in tasks.iter().enumerate() {
            run.states[i] = task.state;
            run.attempts[i] = task.attempts;
            run.failures[i] = task.failures;
        }
        run.recount();
        Ok(run)
    }

    /// Computes `missing`, `open` and the two flags again from the states,
    /// and skips the blocked tasks that can no longer start.
    fn recount(&mut self) {
        // Skip what sits behind a task that did not succeed. An explicit
        // stack, as in `fail`.
        let mut stack: Vec<TaskId> = self
            .graph
            .tasks()
            .filter(|t| {
                let state = self.states[t.index()];
                state.is_final() && state != TaskState::Success
            })
            .collect();
        while let Some(u) = stack.pop() {
            for &succ in self.graph.successors(u) {
                if self.states[succ.index()] == TaskState::Blocked {
                    self.states[succ.index()] = TaskState::Skipped;
                    stack.push(succ);
                }
            }
        }

        self.missing.fill(0);
        for t in self.graph.tasks() {
            if self.states[t.index()] != TaskState::Success {
                for &succ in self.graph.successors(t) {
                    self.missing[succ.index()] += 1;
                }
            }
        }
        self.open = self.states.iter().filter(|s| !s.is_final()).count();
        self.any_failed = self.states.contains(&TaskState::Failed);
        self.cancelled = self.states.contains(&TaskState::Cancelled);
    }

    /// Tasks that can be assigned now: blocked, with no dependency missing.
    #[must_use]
    pub fn ready(&self) -> Vec<TaskId> {
        self.graph
            .tasks()
            .filter(|t| {
                self.states[t.index()] == TaskState::Blocked && self.missing[t.index()] == 0
            })
            .collect()
    }

    /// Failed attempts of task `t` that count against its retries.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    #[must_use]
    pub fn failures(&self, t: TaskId) -> u32 {
        self.failures[t.index()]
    }

    /// Reopens a finished run: the tasks chosen by `scope` go back to
    /// `Blocked` with their retries restored, and keep their attempt
    /// count, so the history stays in one run. Returns the tasks whose
    /// state changed, in ascending order; [`ready`](Self::ready) then says
    /// which can start.
    ///
    /// A task behind one that still has not succeeded stays skipped: with
    /// `Rerun::Task` on a task whose dependency failed, nothing can start.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::NotFinished`] if some task is still open.
    ///
    /// # Panics
    ///
    /// Panics if the task of `Rerun::Task` does not belong to this run's
    /// graph.
    pub fn rerun(&mut self, scope: Rerun) -> Result<Vec<TaskId>, RunError> {
        if !self.is_finished() {
            return Err(RunError::NotFinished);
        }
        let before = self.states.clone();
        let mut chosen = vec![false; self.graph.len()];
        match scope {
            Rerun::All => chosen.fill(true),
            Rerun::Failed => {
                for (i, state) in self.states.iter().enumerate() {
                    chosen[i] = *state != TaskState::Success;
                }
            }
            Rerun::Task { task, downstream } => {
                chosen[task.index()] = true;
                let mut stack = if downstream { vec![task] } else { Vec::new() };
                while let Some(u) = stack.pop() {
                    for &succ in self.graph.successors(u) {
                        if !chosen[succ.index()] {
                            chosen[succ.index()] = true;
                            stack.push(succ);
                        }
                    }
                }
            }
        }
        for (i, &chosen) in chosen.iter().enumerate() {
            if chosen {
                self.states[i] = TaskState::Blocked;
                self.failures[i] = 0;
            }
        }
        self.recount();
        Ok(self
            .graph
            .tasks()
            .filter(|t| self.states[t.index()] != before[t.index()])
            .collect())
    }

    /// The id of this run.
    #[must_use]
    pub fn id(&self) -> RunId {
        self.id
    }

    /// The graph this run follows.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// State of task `t`.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    #[must_use]
    pub fn state(&self, t: TaskId) -> TaskState {
        self.states[t.index()]
    }

    /// Attempts made so far for task `t`: 0 before it is first assigned.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    #[must_use]
    pub fn attempts(&self, t: TaskId) -> u32 {
        self.attempts[t.index()]
    }

    /// Tasks ready at the start of the run: those with no dependencies.
    #[must_use]
    pub fn initial_ready(&self) -> Vec<TaskId> {
        self.graph
            .tasks()
            .filter(|t| self.missing[t.index()] == 0)
            .collect()
    }

    /// Whether every task is in a final state.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.open == 0
    }

    /// How the run ended, or `None` while some task is still open.
    #[must_use]
    pub fn outcome(&self) -> Option<RunOutcome> {
        match (self.is_finished(), self.cancelled, self.any_failed) {
            (false, _, _) => None,
            (true, true, _) => Some(RunOutcome::Cancelled),
            (true, false, false) => Some(RunOutcome::Success),
            (true, false, true) => Some(RunOutcome::Failed),
        }
    }

    /// Marks a ready task as sent to a worker, starting a new attempt.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::NotReady`] if the task is not blocked or still
    /// has missing dependencies.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    pub fn assign(&mut self, t: TaskId) -> Result<(), RunError> {
        if self.state(t) != TaskState::Blocked || self.missing[t.index()] > 0 {
            return Err(RunError::NotReady {
                task: t,
                state: self.state(t),
            });
        }
        self.states[t.index()] = TaskState::Assigned;
        self.attempts[t.index()] += 1;
        Ok(())
    }

    /// Records that an assigned task succeeded, and returns the tasks that
    /// became ready because of it, in ascending order.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::NotAssigned`] if the task is not assigned, for
    /// example when the same report arrives twice.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    pub fn succeed(&mut self, t: TaskId) -> Result<Vec<TaskId>, RunError> {
        if self.state(t) != TaskState::Assigned {
            return Err(RunError::NotAssigned {
                task: t,
                state: self.state(t),
            });
        }
        self.states[t.index()] = TaskState::Success;
        self.open -= 1;

        let mut ready = Vec::new();
        for &succ in self.graph.successors(t) {
            let succ_index = succ.index();
            self.missing[succ_index] -= 1;
            if self.missing[succ_index] == 0 {
                ready.push(succ);
            }
        }
        Ok(ready)
    }

    /// Records that an attempt of an assigned task failed.
    ///
    /// With attempts left the task waits to retry. Otherwise it fails for
    /// good, and every task that depends on it, directly or not, is
    /// skipped.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::NotAssigned`] if the task is not assigned.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    pub fn fail(&mut self, t: TaskId) -> Result<Failure, RunError> {
        if self.state(t) != TaskState::Assigned {
            return Err(RunError::NotAssigned {
                task: t,
                state: self.state(t),
            });
        }
        if let Some(delay) = self.start_retry(t) {
            return Ok(Failure::Retry { delay });
        }
        self.states[t.index()] = TaskState::Failed;
        self.open -= 1;
        self.any_failed = true;

        // Walk the descendants with an explicit stack rather than recursion,
        // so a long chain of tasks cannot overflow the thread's stack.
        let mut skipped = Vec::new();
        let mut stack = vec![t];
        while let Some(u) = stack.pop() {
            for &succ in self.graph.successors(u) {
                let succ_index = succ.index();
                // Already skipped when reachable through more than one path.
                if !self.states[succ_index].is_final() {
                    self.states[succ_index] = TaskState::Skipped;
                    self.open -= 1;
                    skipped.push(succ);
                    stack.push(succ);
                }
            }
        }
        Ok(Failure::Final { skipped })
    }

    /// Puts a task whose attempt just failed in `Retrying`, if it has
    /// attempts left, and returns how long to wait before the next one.
    ///
    /// A task may fail `1 + retries` times, so with `retries: 2` the first
    /// two failures retry and the third is final.
    fn start_retry(&mut self, t: TaskId) -> Option<Duration> {
        self.failures[t.index()] += 1;
        let options = self.graph.options(t);
        let failures = self.failures[t.index()];
        if failures > options.retries {
            return None;
        }
        self.states[t.index()] = TaskState::Retrying;
        Some(options.retry_delay_after(failures))
    }

    /// Records that the wait of a retrying task is over: it is `Blocked`
    /// again, and ready to be assigned.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::NotRetrying`] if the task is not retrying, for
    /// example when the run was cancelled during the wait.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    pub fn retry_due(&mut self, t: TaskId) -> Result<(), RunError> {
        if self.state(t) != TaskState::Retrying {
            return Err(RunError::NotRetrying {
                task: t,
                state: self.state(t),
            });
        }
        self.states[t.index()] = TaskState::Blocked;
        Ok(())
    }

    /// Puts an assigned task back to `Blocked`, ready to be assigned again,
    /// because its worker was lost. The lost attempt stays counted in
    /// [`attempts`](Self::attempts) but does not use up a retry.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::NotAssigned`] if the task is not assigned.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    pub fn requeue(&mut self, t: TaskId) -> Result<(), RunError> {
        if self.state(t) != TaskState::Assigned {
            return Err(RunError::NotAssigned {
                task: t,
                state: self.state(t),
            });
        }
        self.states[t.index()] = TaskState::Blocked;
        Ok(())
    }

    /// Cancels the run: every task not yet in a final state becomes
    /// `Cancelled`, and is returned in ascending order. The run is then
    /// finished, with [`RunOutcome::Cancelled`].
    pub fn cancel(&mut self) -> Vec<TaskId> {
        let open: Vec<TaskId> = self
            .graph
            .tasks()
            .filter(|&t| !self.states[t.index()].is_final())
            .collect();
        for &t in &open {
            self.states[t.index()] = TaskState::Cancelled;
        }
        self.open = 0;
        self.cancelled = true;
        open
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{TaskDef, TaskOptions};

    fn task(name: &str, needs: &[&str]) -> TaskDef {
        TaskDef {
            name: name.to_owned(),
            needs: needs.iter().map(|&n| n.to_owned()).collect(),
            options: TaskOptions::default(),
        }
    }

    fn ids(indices: &[u32]) -> Vec<TaskId> {
        indices.iter().copied().map(TaskId::new).collect()
    }

    fn t(index: u32) -> TaskId {
        TaskId::new(index)
    }

    fn retried(name: &str, needs: &[&str], retries: u32) -> TaskDef {
        let mut def = task(name, needs);
        def.options.retries = retries;
        def
    }

    /// A run of `tasks`, with id 1.
    fn run_of(tasks: &[TaskDef]) -> Run {
        Run::new(RunId::new(1), Arc::new(Graph::build(tasks).unwrap()))
    }

    /// extract(0) -> clean(1), count(2) -> load(3)
    fn diamond() -> Run {
        run_of(&[
            task("extract", &[]),
            task("clean", &["extract"]),
            task("count", &["extract"]),
            task("load", &["clean", "count"]),
        ])
    }

    /// a(0) -> b(1) -> c(2), and an independent branch d(3) -> e(4).
    fn two_branches() -> Run {
        run_of(&[
            task("a", &[]),
            task("b", &["a"]),
            task("c", &["b"]),
            task("d", &[]),
            task("e", &["d"]),
        ])
    }

    // Already working

    #[test]
    fn new_run_is_all_blocked() {
        let run = diamond();
        assert_eq!(run.initial_ready(), ids(&[0]));
        assert!(
            run.graph()
                .tasks()
                .all(|t| run.state(t) == TaskState::Blocked)
        );
        assert!(!run.is_finished());
        assert_eq!(run.outcome(), None);
    }

    #[test]
    fn assign_counts_attempts() {
        let mut run = diamond();
        assert_eq!(run.attempts(t(0)), 0);
        run.assign(t(0)).unwrap();
        assert_eq!(run.attempts(t(0)), 1);
    }

    #[test]
    fn retry_due_of_a_task_that_is_not_retrying_is_an_error() {
        let mut run = diamond();
        assert_eq!(
            run.retry_due(t(0)),
            Err(RunError::NotRetrying {
                task: t(0),
                state: TaskState::Blocked
            })
        );
    }

    #[test]
    fn empty_run_is_finished_and_successful() {
        let run = run_of(&[]);
        assert!(run.is_finished());
        assert_eq!(run.outcome(), Some(RunOutcome::Success));
    }

    // TASK 5

    #[test]
    fn assign_ready_task() {
        let mut run = diamond();
        assert_eq!(run.assign(t(0)), Ok(()));
        assert_eq!(run.state(t(0)), TaskState::Assigned);
    }

    #[test]
    fn assign_task_with_missing_dependencies_is_an_error() {
        let mut run = diamond();
        assert_eq!(
            run.assign(t(3)),
            Err(RunError::NotReady {
                task: t(3),
                state: TaskState::Blocked
            })
        );
        assert_eq!(run.state(t(3)), TaskState::Blocked);
    }

    #[test]
    fn assign_twice_is_an_error() {
        let mut run = diamond();
        run.assign(t(0)).unwrap();
        assert_eq!(
            run.assign(t(0)),
            Err(RunError::NotReady {
                task: t(0),
                state: TaskState::Assigned
            })
        );
    }

    // TASK 6 (needs 5)

    #[test]
    fn success_releases_successors_in_order() {
        let mut run = diamond();
        run.assign(t(0)).unwrap();
        assert_eq!(run.succeed(t(0)), Ok(ids(&[1, 2])));

        run.assign(t(2)).unwrap();
        assert_eq!(run.succeed(t(2)), Ok(vec![]), "load still waits for clean");

        run.assign(t(1)).unwrap();
        assert_eq!(run.succeed(t(1)), Ok(ids(&[3])));

        run.assign(t(3)).unwrap();
        assert_eq!(run.succeed(t(3)), Ok(vec![]));

        assert!(run.is_finished());
        assert_eq!(run.outcome(), Some(RunOutcome::Success));
    }

    #[test]
    fn duplicate_success_report_is_an_error() {
        let mut run = diamond();
        run.assign(t(0)).unwrap();
        run.succeed(t(0)).unwrap();
        assert_eq!(
            run.succeed(t(0)),
            Err(RunError::NotAssigned {
                task: t(0),
                state: TaskState::Success
            })
        );
        // The successors were not released a second time.
        run.assign(t(1)).unwrap();
        assert_eq!(run.succeed(t(1)), Ok(vec![]));
    }

    #[test]
    fn success_of_unassigned_task_is_an_error() {
        let mut run = diamond();
        assert_eq!(
            run.succeed(t(0)),
            Err(RunError::NotAssigned {
                task: t(0),
                state: TaskState::Blocked
            })
        );
    }

    // TASK 7 (needs 5 and 6)

    #[test]
    fn failure_skips_descendants_and_spares_other_branches() {
        let mut run = two_branches();
        assert_eq!(run.initial_ready(), ids(&[0, 3]));

        run.assign(t(0)).unwrap();
        run.assign(t(3)).unwrap();
        assert_eq!(run.succeed(t(0)), Ok(ids(&[1])));

        run.assign(t(1)).unwrap();
        assert_eq!(run.fail(t(1)), Ok(Failure::Final { skipped: ids(&[2]) }));
        assert_eq!(run.state(t(1)), TaskState::Failed);
        assert_eq!(run.state(t(2)), TaskState::Skipped);
        assert_eq!(run.outcome(), None, "the other branch is still running");

        assert_eq!(run.succeed(t(3)), Ok(ids(&[4])));
        run.assign(t(4)).unwrap();
        run.succeed(t(4)).unwrap();

        assert!(run.is_finished());
        assert_eq!(run.outcome(), Some(RunOutcome::Failed));
    }

    #[test]
    fn failure_skips_each_descendant_once() {
        // load(3) is reachable from extract(0) through both clean and count.
        let mut run = diamond();
        run.assign(t(0)).unwrap();
        let Ok(Failure::Final { mut skipped }) = run.fail(t(0)) else {
            panic!("no retries, so the failure is final");
        };
        skipped.sort();
        assert_eq!(skipped, ids(&[1, 2, 3]));
        assert!(run.is_finished());
        assert_eq!(run.outcome(), Some(RunOutcome::Failed));
    }

    #[test]
    fn failure_of_unassigned_task_is_an_error() {
        let mut run = diamond();
        assert_eq!(
            run.fail(t(1)),
            Err(RunError::NotAssigned {
                task: t(1),
                state: TaskState::Blocked
            })
        );
    }

    // TASK 25 (needs 5 to 7)

    #[test]
    fn failure_with_retries_left_waits_to_retry() {
        let mut run = run_of(&[retried("a", &[], 2), task("b", &["a"])]);
        run.assign(t(0)).unwrap();
        assert_eq!(
            run.fail(t(0)),
            Ok(Failure::Retry {
                delay: Duration::from_secs(30)
            })
        );
        assert_eq!(run.state(t(0)), TaskState::Retrying);
        assert_eq!(run.state(t(1)), TaskState::Blocked, "b is not skipped");
        assert_eq!(run.outcome(), None);
    }

    #[test]
    fn retries_run_out_then_the_failure_is_final() {
        let mut run = run_of(&[retried("a", &[], 2), task("b", &["a"])]);
        for delay in [30, 60] {
            run.assign(t(0)).unwrap();
            assert_eq!(
                run.fail(t(0)),
                Ok(Failure::Retry {
                    delay: Duration::from_secs(delay)
                })
            );
            run.retry_due(t(0)).unwrap();
        }
        run.assign(t(0)).unwrap();
        assert_eq!(run.attempts(t(0)), 3);
        assert_eq!(run.fail(t(0)), Ok(Failure::Final { skipped: ids(&[1]) }));
        assert_eq!(run.outcome(), Some(RunOutcome::Failed));
    }

    #[test]
    fn a_retry_can_succeed() {
        let mut run = run_of(&[retried("a", &[], 1), task("b", &["a"])]);
        run.assign(t(0)).unwrap();
        run.fail(t(0)).unwrap();
        run.retry_due(t(0)).unwrap();
        run.assign(t(0)).unwrap();
        assert_eq!(run.succeed(t(0)), Ok(ids(&[1])));
        assert_eq!(run.attempts(t(0)), 2);
    }

    #[test]
    fn a_retrying_task_cannot_be_assigned_before_it_is_due() {
        let mut run = run_of(&[retried("a", &[], 1)]);
        run.assign(t(0)).unwrap();
        run.fail(t(0)).unwrap();
        assert_eq!(
            run.assign(t(0)),
            Err(RunError::NotReady {
                task: t(0),
                state: TaskState::Retrying
            })
        );
    }

    #[test]
    fn a_lost_attempt_does_not_use_up_a_retry() {
        // No retries: only one failure is allowed.
        let mut run = run_of(&[task("a", &[])]);
        run.assign(t(0)).unwrap();
        run.requeue(t(0)).unwrap();
        assert_eq!(run.state(t(0)), TaskState::Blocked);
        run.assign(t(0)).unwrap();
        assert_eq!(run.attempts(t(0)), 2);
        assert_eq!(run.fail(t(0)), Ok(Failure::Final { skipped: vec![] }));
    }

    #[test]
    fn requeue_of_a_task_that_is_not_assigned_is_an_error() {
        let mut run = diamond();
        assert_eq!(
            run.requeue(t(0)),
            Err(RunError::NotAssigned {
                task: t(0),
                state: TaskState::Blocked
            })
        );
    }

    #[test]
    fn cancel_closes_every_open_task() {
        let mut run = diamond();
        run.assign(t(0)).unwrap();
        run.succeed(t(0)).unwrap();
        run.assign(t(1)).unwrap();
        assert_eq!(run.cancel(), ids(&[1, 2, 3]));
        assert_eq!(run.state(t(0)), TaskState::Success);
        assert_eq!(run.state(t(1)), TaskState::Cancelled);
        assert!(run.is_finished());
        assert_eq!(run.outcome(), Some(RunOutcome::Cancelled));
    }

    // Restore and rerun

    fn snap(state: TaskState, attempts: u32) -> TaskSnapshot {
        TaskSnapshot {
            state,
            attempts,
            failures: 0,
        }
    }

    #[test]
    fn restore_recomputes_the_counters() {
        let graph = Arc::clone(&diamond().graph);
        let run = Run::restore(
            RunId::new(1),
            graph,
            &[
                snap(TaskState::Success, 1),
                snap(TaskState::Assigned, 1),
                snap(TaskState::Blocked, 0),
                snap(TaskState::Blocked, 0),
            ],
        )
        .unwrap();
        assert_eq!(run.ready(), ids(&[2]));
        assert_eq!(run.outcome(), None);
        assert_eq!(run.attempts(t(1)), 1);
    }

    #[test]
    fn restore_rejects_a_snapshot_of_the_wrong_size() {
        let graph = Arc::clone(&diamond().graph);
        assert_eq!(
            Run::restore(RunId::new(1), graph, &[]).unwrap_err(),
            RunError::WrongTaskCount {
                expected: 4,
                found: 0
            }
        );
    }

    #[test]
    fn rerun_failed_reopens_the_failed_and_the_skipped() {
        let mut run = diamond();
        run.assign(t(0)).unwrap();
        run.succeed(t(0)).unwrap();
        run.assign(t(1)).unwrap();
        run.assign(t(2)).unwrap();
        run.succeed(t(2)).unwrap();
        run.fail(t(1)).unwrap();
        assert_eq!(run.outcome(), Some(RunOutcome::Failed));

        assert_eq!(run.rerun(Rerun::Failed).unwrap(), ids(&[1, 3]));
        assert_eq!(run.outcome(), None);
        assert_eq!(run.ready(), ids(&[1]));
        run.assign(t(1)).unwrap();
        assert_eq!(run.attempts(t(1)), 2, "the history stays in the run");
        assert_eq!(run.succeed(t(1)).unwrap(), ids(&[3]));
        run.assign(t(3)).unwrap();
        run.succeed(t(3)).unwrap();
        assert_eq!(run.outcome(), Some(RunOutcome::Success));
    }

    #[test]
    fn rerun_of_an_open_run_is_an_error() {
        let mut run = diamond();
        assert_eq!(run.rerun(Rerun::All).unwrap_err(), RunError::NotFinished);
    }

    #[test]
    fn rerun_of_a_task_takes_its_descendants_only_when_asked() {
        let finished = || {
            let mut run = diamond();
            for i in 0..4 {
                run.assign(t(i)).unwrap();
                run.succeed(t(i)).unwrap();
            }
            run
        };
        let mut run = finished();
        let only = Rerun::Task {
            task: t(1),
            downstream: false,
        };
        assert_eq!(run.rerun(only).unwrap(), ids(&[1]));
        assert_eq!(run.ready(), ids(&[1]));

        let mut run = finished();
        let with = Rerun::Task {
            task: t(1),
            downstream: true,
        };
        assert_eq!(run.rerun(with).unwrap(), ids(&[1, 3]));
        assert_eq!(run.rerun(Rerun::All).unwrap_err(), RunError::NotFinished);
    }

    #[test]
    fn rerun_restores_the_retries() {
        let mut run = run_of(&[retried("a", &[], 1)]);
        run.assign(t(0)).unwrap();
        run.fail(t(0)).unwrap();
        run.retry_due(t(0)).unwrap();
        run.assign(t(0)).unwrap();
        assert!(matches!(run.fail(t(0)).unwrap(), Failure::Final { .. }));
        run.rerun(Rerun::Failed).unwrap();
        run.assign(t(0)).unwrap();
        assert!(matches!(run.fail(t(0)).unwrap(), Failure::Retry { .. }));
        assert_eq!(run.attempts(t(0)), 3);
    }

    #[test]
    fn a_task_behind_a_failed_one_stays_skipped() {
        let mut run = run_of(&[task("a", &[]), task("b", &["a"])]);
        run.assign(t(0)).unwrap();
        run.fail(t(0)).unwrap();
        let only_b = Rerun::Task {
            task: t(1),
            downstream: false,
        };
        assert!(run.rerun(only_b).unwrap().is_empty());
        assert_eq!(run.outcome(), Some(RunOutcome::Failed));
    }
}
