//! The state of one run: which tasks are waiting, running or done.
//!
//! A run is Kahn's algorithm spread over time. Each task has a counter of
//! missing dependencies; when a task succeeds its successors' counters drop
//! by one, and a task whose counter reaches zero is ready. When a task fails,
//! everything that depends on it, directly or not, is skipped, while
//! independent branches go on.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

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
}

impl TaskState {
    /// Whether the task will not change any more in this run.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(self, Self::Success | Self::Failed | Self::Skipped)
    }
}

/// How a finished run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Every task succeeded.
    Success,
    /// At least one task failed.
    Failed,
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
        }
    }
}

impl Error for RunError {}

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
    /// Tasks not yet in a final state.
    open: usize,
    /// Whether some task has failed.
    any_failed: bool,
}

impl Run {
    /// Starts a run: every task is blocked and waits for its dependencies.
    #[must_use]
    pub fn new(id: RunId, graph: Arc<Graph>) -> Self {
        Self {
            id,
            missing: graph.in_degree().to_vec(),
            states: vec![TaskState::Blocked; graph.len()],
            open: graph.len(),
            any_failed: false,
            graph,
        }
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
        match (self.is_finished(), self.any_failed) {
            (false, _) => None,
            (true, false) => Some(RunOutcome::Success),
            (true, true) => Some(RunOutcome::Failed),
        }
    }

    /// Marks a ready task as sent to a worker.
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

    /// Records that an assigned task failed for good, skips every task that
    /// depends on it directly or indirectly, and returns the skipped tasks
    /// in any order.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::NotAssigned`] if the task is not assigned.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this run's graph.
    pub fn fail(&mut self, t: TaskId) -> Result<Vec<TaskId>, RunError> {
        if self.state(t) != TaskState::Assigned {
            return Err(RunError::NotAssigned {
                task: t,
                state: self.state(t),
            });
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
        Ok(skipped)
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
        assert_eq!(run.fail(t(1)), Ok(ids(&[2])));
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
        let mut skipped = run.fail(t(0)).unwrap();
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
}
