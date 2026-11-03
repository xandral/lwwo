//! The graph of a DAG, computed from the `needs` of its tasks.
//!
//! Tasks are numbered from 0 in the order they appear in the DAG, and the
//! graph refers to them by these [`TaskId`]s instead of by name. The
//! successors of all tasks live in a single `succ` vector, and
//! `start[t]..start[t + 1]` is the slice belonging to task `t` (compressed
//! sparse row form):
//!
//! ```text
//! task:   extract=0  clean=1  count=2  load=3
//! succ:   [1, 2, 3, 3]
//! start:  [0, 2, 3, 4, 4]
//! ```

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

/// Position of a task within its DAG version: 0, 1, 2… in DAG order.
///
/// It is only meaningful together with the [`Graph`] it came from. A `u32`
/// halves the memory of the graph and of per-run state compared to `usize`,
/// and the newtype stops a task index from being mixed up with other numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(u32);

impl TaskId {
    /// The task at position `index`.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// The position as a `usize`, for indexing vectors.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// The task at position `index`, for indices produced inside this module.
    ///
    /// [`Graph::build`] rejects DAGs with more than `u32::MAX` tasks, so the
    /// conversion cannot fail for any index below `tasks.len()`.
    fn from_index(index: usize) -> Self {
        Self(u32::try_from(index).expect("task count checked in Graph::build"))
    }
}

/// A task as written in the DAG: its name and the names it depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDef {
    /// Unique name of the task within the DAG.
    pub name: String,
    /// Names of the tasks that must succeed before this one starts.
    pub needs: Vec<String>,
}

/// Why a DAG was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagError {
    /// The DAG has more tasks than a [`TaskId`] can number.
    TooManyTasks(usize),
    /// Two tasks share the same name.
    DuplicateTask(String),
    /// `task` lists in `needs` a name that is not in the DAG.
    UnknownDependency {
        /// The task with the bad dependency.
        task: String,
        /// The missing name.
        needs: String,
    },
    /// `task` lists the same name more than once in `needs`.
    DuplicateDependency {
        /// The task with the repeated dependency.
        task: String,
        /// The repeated name.
        needs: String,
    },
    /// Tasks that are part of a cycle or depend on one, in DAG order.
    Cycle(Vec<String>),
}

impl fmt::Display for DagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyTasks(n) => write!(f, "the DAG has {n} tasks, more than supported"),
            Self::DuplicateTask(name) => write!(f, "task `{name}` is defined more than once"),
            Self::UnknownDependency { task, needs } => {
                write!(f, "task `{task}` needs `{needs}`, which does not exist")
            }
            Self::DuplicateDependency { task, needs } => {
                write!(f, "task `{task}` lists `{needs}` more than once in `needs`")
            }
            Self::Cycle(tasks) => {
                write!(f, "tasks in or after a cycle: {}", tasks.join(", "))
            }
        }
    }
}

impl Error for DagError {}

/// The computed graph of a DAG, shared by all runs of the same version.
#[derive(Debug, Clone)]
pub struct Graph {
    names: Vec<String>,
    succ: Vec<TaskId>,
    start: Vec<usize>,
    in_degree: Vec<usize>,
}

impl Graph {
    /// Builds the graph from the tasks of a DAG.
    ///
    /// # Errors
    ///
    /// Returns a [`DagError`] if the DAG has too many tasks, if two tasks
    /// share a name, if a task lists the same dependency twice or a name
    /// that does not exist, or if the dependencies contain a cycle.
    pub fn build(tasks: &[TaskDef]) -> Result<Self, DagError> {
        if u32::try_from(tasks.len()).is_err() {
            return Err(DagError::TooManyTasks(tasks.len()));
        }

        // Name -> id, rejecting duplicates.
        let mut ids: HashMap<&str, TaskId> = HashMap::with_capacity(tasks.len());
        for (i, task) in tasks.iter().enumerate() {
            if ids.insert(&task.name, TaskId::from_index(i)).is_some() {
                return Err(DagError::DuplicateTask(task.name.clone()));
            }
        }

        // Each dependency at most once per task: a repeat is almost always a typo.
        let mut seen: HashSet<&str> = HashSet::new();
        for task in tasks {
            seen.clear();
            for need in &task.needs {
                if !seen.insert(need) {
                    return Err(DagError::DuplicateDependency {
                        task: task.name.clone(),
                        needs: need.clone(),
                    });
                }
            }
        }

        // `needs` from names to ids: preds[t] are the predecessors of t.
        let preds = tasks
            .iter()
            .map(|task| {
                task.needs
                    .iter()
                    .map(|need| {
                        ids.get(need.as_str())
                            .copied()
                            .ok_or_else(|| DagError::UnknownDependency {
                                task: task.name.clone(),
                                needs: need.clone(),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let (start, succ) = successors_csr(&preds);
        let graph = Self {
            names: tasks.iter().map(|t| t.name.clone()).collect(),
            succ,
            start,
            in_degree: in_degrees(&preds),
        };

        let stuck = graph.tasks_in_cycles();
        if !stuck.is_empty() {
            let names = stuck
                .into_iter()
                .map(|t| graph.name(t).to_owned())
                .collect();
            return Err(DagError::Cycle(names));
        }
        Ok(graph)
    }

    /// Number of tasks in the DAG.
    #[must_use]
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Whether the DAG has no tasks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// All task ids, in DAG order.
    pub fn tasks(&self) -> impl ExactSizeIterator<Item = TaskId> {
        (0..self.len()).map(TaskId::from_index)
    }

    /// Name of task `t`.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this graph.
    #[must_use]
    pub fn name(&self, t: TaskId) -> &str {
        &self.names[t.index()]
    }

    /// Number of dependencies of each task, indexed by [`TaskId::index`].
    #[must_use]
    pub fn in_degree(&self) -> &[usize] {
        &self.in_degree
    }

    /// Successors of task `t`: the tasks that depend on it, in ascending order.
    ///
    /// # Panics
    ///
    /// Panics if `t` does not belong to this graph.
    #[must_use]
    pub fn successors(&self, t: TaskId) -> &[TaskId] {
        let start = self.start[t.index()];
        let end = self.start[t.index() + 1];
        &self.succ[start..end]
    }

    /// Tasks that Kahn's algorithm never brings to zero, in ascending order.
    /// Empty if the graph has no cycle.
    fn tasks_in_cycles(&self) -> Vec<TaskId> {
        // Kahn's algorithm: start from the tasks with no dependencies and
        // "finish" them one by one, releasing their successors.
        let mut missing = self.in_degree.clone();
        let mut ready: Vec<TaskId> = self.tasks().filter(|t| missing[t.index()] == 0).collect();

        // Any order of extraction works, so a stack is enough.
        while let Some(t) = ready.pop() {
            for &s in self.successors(t) {
                let count = &mut missing[s.index()];
                *count -= 1;
                if *count == 0 {
                    ready.push(s);
                }
            }
        }

        // In a cycle every task waits for another one, so none reaches zero.
        self.tasks().filter(|t| missing[t.index()] > 0).collect()
    }
}

/// In-degree of each task: how many predecessors it has.
fn in_degrees(preds: &[Vec<TaskId>]) -> Vec<usize> {
    (0..preds.len()).map(|i| preds[i].len()).collect()
}

/// Turns predecessor lists into successors in CSR form, returning `(start, succ)`.
///
/// `start` has `preds.len() + 1` elements, and the successors of each task
/// are in ascending order.
fn successors_csr(preds: &[Vec<TaskId>]) -> (Vec<usize>, Vec<TaskId>) {
    // Count the successors of each task: every `p` in a predecessor list
    // is one more successor for `p`.
    let mut counts = vec![0; preds.len()];
    for list in preds {
        for p in list {
            counts[p.index()] += 1;
        }
    }

    // Each list starts where the previous one ends. The extra last element
    // is the total number of edges.
    let mut start = Vec::with_capacity(preds.len() + 1);
    start.push(0);
    for (i, &count) in counts.iter().enumerate() {
        start.push(start[i] + count);
    }

    // Write each task into the next free slot of each of its predecessors.
    // Visiting tasks in ascending order keeps every successor list sorted.
    let mut succ = vec![TaskId::new(0); start[preds.len()]];
    let mut next = start.clone();
    for (i, list) in preds.iter().enumerate() {
        let task = TaskId::from_index(i);
        for p in list {
            let slot = &mut next[p.index()];
            succ[*slot] = task;
            *slot += 1;
        }
    }

    (start, succ)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a `TaskDef` in one line: `task("load", &["clean", "count"])`.
    fn task(name: &str, needs: &[&str]) -> TaskDef {
        TaskDef {
            name: name.to_owned(),
            needs: needs.iter().map(|&n| n.to_owned()).collect(),
        }
    }

    /// Task ids from plain numbers: `ids(&[1, 2])`.
    fn ids(indices: &[u32]) -> Vec<TaskId> {
        indices.iter().copied().map(TaskId::new).collect()
    }

    /// Predecessor lists from plain numbers, one slice per task.
    fn preds(lists: &[&[u32]]) -> Vec<Vec<TaskId>> {
        lists.iter().map(|&l| ids(l)).collect()
    }

    /// The example DAG from the spec: extract -> clean, count -> load.
    fn example() -> Vec<TaskDef> {
        vec![
            task("extract", &[]),
            task("clean", &["extract"]),
            task("count", &["extract"]),
            task("load", &["clean", "count"]),
        ]
    }

    // TASK 1

    #[test]
    fn successors_of_example() {
        let g = Graph {
            names: ["a", "b", "c", "d"].map(String::from).to_vec(),
            succ: ids(&[1, 2, 3, 3]),
            start: vec![0, 2, 3, 4, 4],
            in_degree: vec![0, 1, 1, 2],
        };
        assert_eq!(g.successors(TaskId::new(0)), ids(&[1, 2]));
        assert_eq!(g.successors(TaskId::new(1)), ids(&[3]));
        assert_eq!(g.successors(TaskId::new(2)), ids(&[3]));
        assert!(g.successors(TaskId::new(3)).is_empty());
    }

    // TASK 2

    #[test]
    fn in_degrees_of_example() {
        let p = preds(&[&[], &[0], &[0], &[1, 2]]);
        assert_eq!(in_degrees(&p), [0, 1, 1, 2]);
    }

    #[test]
    fn in_degrees_empty() {
        assert!(in_degrees(&[]).is_empty());
    }

    // TASK 3

    #[test]
    fn csr_of_example() {
        let p = preds(&[&[], &[0], &[0], &[1, 2]]);
        let (start, succ) = successors_csr(&p);
        assert_eq!(start, [0, 2, 3, 4, 4]);
        assert_eq!(succ, ids(&[1, 2, 3, 3]));
    }

    #[test]
    fn csr_successors_are_sorted() {
        // 1 and 3 depend on 0, 2 depends on 1: the successors of 0 are [1, 3].
        let p = preds(&[&[], &[0], &[1], &[0]]);
        let (start, succ) = successors_csr(&p);
        assert_eq!(start, [0, 2, 3, 3, 3]);
        assert_eq!(succ, ids(&[1, 3, 2]));
    }

    #[test]
    fn csr_empty() {
        let (start, succ) = successors_csr(&[]);
        assert_eq!(start, [0]);
        assert!(succ.is_empty());
    }

    // TASK 4 (needs 1, 2 and 3 too)

    #[test]
    fn example_is_valid() {
        let g = Graph::build(&example()).unwrap();
        assert_eq!(g.len(), 4);
        assert_eq!(g.in_degree(), [0, 1, 1, 2]);
        assert_eq!(g.successors(TaskId::new(0)), ids(&[1, 2]));
        assert_eq!(g.name(TaskId::new(3)), "load");
        assert_eq!(g.tasks().collect::<Vec<_>>(), ids(&[0, 1, 2, 3]));
    }

    #[test]
    fn empty_dag_is_valid() {
        assert!(Graph::build(&[]).unwrap().is_empty());
    }

    #[test]
    fn cycle_is_rejected() {
        // a -> b -> c -> a, d depends on the cycle, e is free.
        let tasks = [
            task("a", &["c"]),
            task("b", &["a"]),
            task("c", &["b"]),
            task("d", &["c"]),
            task("e", &[]),
        ];
        assert_eq!(
            Graph::build(&tasks).unwrap_err(),
            DagError::Cycle(["a", "b", "c", "d"].map(String::from).to_vec())
        );
    }

    #[test]
    fn self_dependency_is_rejected() {
        assert_eq!(
            Graph::build(&[task("a", &["a"])]).unwrap_err(),
            DagError::Cycle(vec!["a".to_owned()])
        );
    }

    // Already working: the checks that run before the graph is built.

    #[test]
    fn duplicate_name_is_rejected() {
        assert_eq!(
            Graph::build(&[task("a", &[]), task("a", &[])]).unwrap_err(),
            DagError::DuplicateTask("a".to_owned())
        );
    }

    #[test]
    fn unknown_dependency_is_rejected() {
        assert_eq!(
            Graph::build(&[task("a", &["ghost"])]).unwrap_err(),
            DagError::UnknownDependency {
                task: "a".to_owned(),
                needs: "ghost".to_owned(),
            }
        );
    }

    #[test]
    fn duplicate_dependency_is_rejected() {
        let tasks = [task("a", &[]), task("b", &["a", "a"])];
        assert_eq!(
            Graph::build(&tasks).unwrap_err(),
            DagError::DuplicateDependency {
                task: "b".to_owned(),
                needs: "a".to_owned(),
            }
        );
    }

    #[test]
    fn errors_have_readable_messages() {
        let err = DagError::UnknownDependency {
            task: "a".to_owned(),
            needs: "ghost".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "task `a` needs `ghost`, which does not exist"
        );
    }
}
