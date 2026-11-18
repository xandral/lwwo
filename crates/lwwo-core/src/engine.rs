//! The engine: receives what happened and returns what to do.
//!
//! The master's event loop turns every timer, worker message and API call
//! into an [`Event`], passes it to [`Engine::handle`] with the current time,
//! and executes the [`Command`]s it gets back. The engine never does I/O
//! itself, so a test or a simulation can drive it with made-up events.
//!
//! ```text
//! a → b, one worker W1
//!
//! StartRun            → SaveTask(a, assigned), Assign(a → W1)
//! Report(a, success)  → SaveTask(a, success), SaveTask(b, assigned),
//!                       Assign(b → W1), Confirm(a)
//! Report(b, success)  → SaveTask(b, success), SaveRun(success), Confirm(b)
//! ```
//!
//! It puts together the other modules: [`Run`] knows which tasks are
//! ready, [`ReadyQueue`] decides their order, [`WorkerPool`] picks a worker
//! for each, and [`Deadlines`] holds the retries and heartbeats.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use crate::dag::{Graph, TaskId};
use crate::deadlines::{Deadline, Deadlines};
use crate::ready_queue::ReadyQueue;
use crate::run::{Failure, Rerun, Run, RunId, RunOutcome, TaskSnapshot, TaskState};
use crate::time::Timestamp;
use crate::worker_selection::{Blocker, Worker, WorkerError, WorkerId, WorkerPool};

/// Settings of the engine, from `master.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineConfig {
    /// How long a worker may stay silent before it is lost.
    pub heartbeat_timeout: Duration,
}

impl Default for EngineConfig {
    /// A 30 second heartbeat timeout, as in the spec.
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(30),
        }
    }
}

/// How an attempt ended, as reported by the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Exit code 0.
    Success,
    /// Any other exit code, or the process could not start.
    Failed,
    /// The worker stopped the process when its `timeout` ran out.
    Timeout,
    /// The worker lost track of the process and cannot say how it ended.
    /// Like an attempt lost with its worker, it does not use up a retry.
    Lost,
}

/// Something that happened, for the engine to react to.
#[derive(Debug, Clone)]
pub enum Event {
    /// A run was created, by the timetable or by hand.
    StartRun {
        /// The new run.
        run: RunId,
        /// The graph of the run's DAG version.
        graph: Arc<Graph>,
    },
    /// An open run is loaded back from the database, when the master
    /// starts.
    RestoreRun {
        /// The run.
        run: RunId,
        /// The graph of the run's DAG version.
        graph: Arc<Graph>,
        /// The state of every task, in graph order.
        tasks: Vec<TaskSnapshot>,
        /// The worker of every assigned task, from its open attempt.
        running: Vec<(TaskId, WorkerId)>,
    },
    /// A finished run is run again, in part or as a whole.
    RerunRun {
        /// The run.
        run: RunId,
        /// The graph of the run's DAG version.
        graph: Arc<Graph>,
        /// The state of every task, in graph order.
        tasks: Vec<TaskSnapshot>,
        /// Which tasks run again.
        scope: Rerun,
    },
    /// A run is cancelled, from the API or by `overlap: replace`.
    CancelRun {
        /// The run.
        run: RunId,
    },
    /// A worker connected and said `Hello`.
    WorkerJoined {
        /// The worker.
        worker: WorkerId,
        /// How many slots it has.
        slots: u32,
        /// Its labels.
        labels: Vec<String>,
        /// The attempts it is running or still has to report, when it
        /// reconnects: run, task and attempt.
        in_flight: Vec<(RunId, TaskId, u32)>,
    },
    /// A worker sent a `Heartbeat`.
    Heartbeat {
        /// The worker.
        worker: WorkerId,
        /// Whether it takes new tasks: false while it drains.
        accepting: bool,
    },
    /// A worker sent a `Report`: an attempt finished.
    Report {
        /// The worker.
        worker: WorkerId,
        /// The run.
        run: RunId,
        /// The task.
        task: TaskId,
        /// Which attempt, from 1.
        attempt: u32,
        /// How it ended.
        outcome: Outcome,
    },
    /// The timer went off.
    Timer,
}

/// Something to do, for the binary to carry out.
///
/// The binary writes every `Save*` command of one call in a single
/// transaction, and only after the commit sends the messages to workers
/// (`Assign`, `Cancel`, `Confirm`): a crash in between loses no work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Write the state of a task to `task_runs`.
    SaveTask {
        /// The run.
        run: RunId,
        /// The task.
        task: TaskId,
        /// Its new state.
        state: TaskState,
        /// Attempts made so far.
        attempts: u32,
    },
    /// Write to `dag_runs` that a finished run is running again.
    ReopenRun {
        /// The run.
        run: RunId,
    },
    /// Write how a run ended to `dag_runs`.
    SaveRun {
        /// The run.
        run: RunId,
        /// How it ended.
        outcome: RunOutcome,
    },
    /// Send `Assign` to a worker.
    Assign {
        /// The worker.
        worker: WorkerId,
        /// The run.
        run: RunId,
        /// The task.
        task: TaskId,
        /// Which attempt this is, from 1.
        attempt: u32,
        /// How long the worker lets it run.
        timeout: Option<Duration>,
    },
    /// Send `Cancel` to a worker.
    Cancel {
        /// The worker.
        worker: WorkerId,
        /// The run.
        run: RunId,
        /// The task.
        task: TaskId,
    },
    /// Send `Confirm` to a worker: its report is saved and can be dropped.
    Confirm {
        /// The worker.
        worker: WorkerId,
        /// The run.
        run: RunId,
        /// The task.
        task: TaskId,
        /// The attempt.
        attempt: u32,
    },
    /// Write down why a run was stopped by the engine itself. A
    /// `SaveRun` with [`RunOutcome::Failed`] follows.
    ///
    /// The engine never panics on what it is told: when an event does not
    /// fit the state of a run, which would be a bug or a database changed
    /// by hand, that run alone is stopped and the others go on.
    Quarantine {
        /// The run.
        run: RunId,
        /// What did not fit, for people.
        reason: String,
    },
    /// Something did not add up and was corrected: for the log.
    Anomaly {
        /// What, for people.
        message: String,
    },
}

/// An attempt sent to a worker and not yet reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Running {
    worker: WorkerId,
    attempt: u32,
    /// Slots taken on the worker, kept here so they can be released even
    /// after the run is gone.
    slots: u32,
}

/// The decision core of the master.
#[derive(Debug, Clone, Default)]
pub struct Engine {
    config: EngineConfig,
    /// Runs not yet finished.
    runs: HashMap<RunId, Run>,
    /// Tasks ready to be assigned.
    ready: ReadyQueue,
    /// Connected workers and their free slots.
    workers: WorkerPool,
    /// Retries and heartbeat expiries.
    deadlines: Deadlines,
    /// Attempts on workers, by run and task. A `BTreeMap`, so that walking
    /// it always gives the same order and so the same commands.
    running: BTreeMap<(RunId, TaskId), Running>,
    /// Attempts of runs that were closed while they ran, by run, task and
    /// attempt: their reports only free the slots, even if the run has
    /// been reopened since.
    closing: BTreeMap<(RunId, TaskId, u32), Running>,
}

impl Engine {
    /// An engine with no runs and no workers.
    #[must_use]
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// A run not yet finished.
    #[must_use]
    pub fn run(&self, run: RunId) -> Option<&Run> {
        self.runs.get(&run)
    }

    /// The runs not yet finished, in no particular order.
    pub fn runs(&self) -> impl Iterator<Item = &Run> {
        self.runs.values()
    }

    /// The worker and the attempt number of a task that is on a worker.
    #[must_use]
    pub fn running(&self, run: RunId, task: TaskId) -> Option<(WorkerId, u32)> {
        self.running
            .get(&(run, task))
            .map(|attempt| (attempt.worker, attempt.attempt))
    }

    /// Whether a task has an attempt on a worker: running, or of a run
    /// closed since and still to be reported.
    #[must_use]
    pub fn on_a_worker(&self, run: RunId, task: TaskId) -> bool {
        self.running.contains_key(&(run, task))
            || self
                .closing
                .range((run, task, 0)..=(run, task, u32::MAX))
                .next()
                .is_some()
    }

    /// The tasks that are ready, with every dependency done, and wait for
    /// a worker. Tasks of runs no longer open are left out.
    #[must_use]
    pub fn waiting(&self) -> Vec<(RunId, TaskId)> {
        self.ready
            .iter()
            .filter(|(run, _)| self.runs.contains_key(run))
            .collect()
    }

    /// Why a waiting task finds no worker; `None` if some worker can take
    /// it, or the task is not of an open run.
    #[must_use]
    pub fn blocker(&self, run: RunId, task: TaskId) -> Option<Blocker> {
        let options = self.runs.get(&run)?.graph().options(task);
        self.workers.blocker(options.slots, &options.labels)
    }

    /// The connected workers.
    #[must_use]
    pub fn workers(&self) -> &WorkerPool {
        &self.workers
    }

    /// The earliest retry or heartbeat expiry: the master arms its timer on
    /// the earlier of this and the next cron fire.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Timestamp> {
        self.deadlines.next()
    }

    /// Reacts to `event`, which happened at `now`, and returns what to do.
    pub fn handle(&mut self, event: Event, now: Timestamp) -> Vec<Command> {
        let mut out = Vec::new();
        match event {
            Event::StartRun { run, graph } => self.start_run(run, graph, &mut out),
            Event::RestoreRun {
                run,
                graph,
                tasks,
                running,
            } => self.restore_run(run, graph, &tasks, &running, now, &mut out),
            Event::RerunRun {
                run,
                graph,
                tasks,
                scope,
            } => self.rerun_run(run, graph, &tasks, scope, &mut out),
            Event::CancelRun { run } => self.cancel_run(run, &mut out),
            Event::WorkerJoined {
                worker,
                slots,
                labels,
                in_flight,
            } => self.worker_joined(worker, slots, labels, &in_flight, now, &mut out),
            Event::Heartbeat { worker, accepting } => self.heartbeat(worker, accepting, now),
            Event::Report {
                worker,
                run,
                task,
                attempt,
                outcome,
            } => self.report(worker, run, task, attempt, outcome, now, &mut out),
            Event::Timer => self.timer(now, &mut out),
        }
        // Whatever happened may have freed a slot or made a task ready.
        self.dispatch(&mut out);
        out
    }

    fn start_run(&mut self, run: RunId, graph: Arc<Graph>, out: &mut Vec<Command>) {
        // The same run started twice: keep the first.
        if self.runs.contains_key(&run) {
            return;
        }
        let state = Run::new(run, graph);
        for task in state.initial_ready() {
            self.ready.push(run, task);
        }
        if state.is_finished() {
            // A DAG with no tasks.
            out.push(Command::SaveRun {
                run,
                outcome: RunOutcome::Success,
            });
        } else {
            self.runs.insert(run, state);
        }
    }

    /// Loads an open run back after a restart of the master.
    ///
    /// Assigned tasks stay with their worker, which has one heartbeat
    /// timeout to reconnect and say in its `Hello` that it still has them.
    fn restore_run(
        &mut self,
        run: RunId,
        graph: Arc<Graph>,
        tasks: &[TaskSnapshot],
        running: &[(TaskId, WorkerId)],
        now: Timestamp,
        out: &mut Vec<Command>,
    ) {
        if self.runs.contains_key(&run) {
            return;
        }
        let Ok(mut state) = Run::restore(run, graph, tasks) else {
            // The snapshot belongs to another graph: nothing safe to do.
            return;
        };
        for task in state.graph().tasks().collect::<Vec<_>>() {
            // Restoring may have skipped it, or left it as it was.
            if state.state(task) != tasks[task.index()].state {
                out.push(save(&state, task));
            }
            match state.state(task) {
                TaskState::Retrying => {
                    let delay = state
                        .graph()
                        .options(task)
                        .retry_delay_after(state.failures(task));
                    self.deadlines
                        .set(Deadline::Retry { run, task }, now.saturating_add(delay));
                }
                TaskState::Assigned => {
                    let worker = running.iter().find(|(t, _)| *t == task).map(|&(_, w)| w);
                    if let Some(worker) = worker {
                        let slots = state.graph().options(task).slots;
                        self.running.insert(
                            (run, task),
                            Running {
                                worker,
                                attempt: state.attempts(task),
                                slots,
                            },
                        );
                        if self.workers.reserve(worker, slots).is_err()
                            && self.deadlines.get(Deadline::Heartbeat(worker)).is_none()
                        {
                            self.expect_heartbeat(worker, now);
                        }
                    } else if state.requeue(task).is_ok() {
                        // Assigned in the database, but to nobody.
                        out.push(save(&state, task));
                    }
                }
                _ => {}
            }
        }
        for task in state.ready() {
            self.ready.push(run, task);
        }
        if let Some(outcome) = state.outcome() {
            out.push(Command::SaveRun { run, outcome });
        } else {
            self.runs.insert(run, state);
        }
    }

    /// Reopens a finished run with the tasks chosen by `scope`.
    fn rerun_run(
        &mut self,
        run: RunId,
        graph: Arc<Graph>,
        tasks: &[TaskSnapshot],
        scope: Rerun,
        out: &mut Vec<Command>,
    ) {
        // Only finished runs are run again, and those are not in memory.
        if self.runs.contains_key(&run) {
            return;
        }
        let Ok(mut state) = Run::restore(run, graph, tasks) else {
            return;
        };
        let Ok(changed) = state.rerun(scope) else {
            return;
        };
        if state.is_finished() {
            // Nothing can start: the run stays as it ended.
            return;
        }
        out.push(Command::ReopenRun { run });
        for task in changed {
            out.push(save(&state, task));
        }
        for task in state.ready() {
            self.ready.push(run, task);
        }
        self.runs.insert(run, state);
    }

    fn cancel_run(&mut self, run: RunId, out: &mut Vec<Command>) {
        self.close_run(run, RunOutcome::Cancelled, out);
    }

    /// Stops a run the engine can no longer trust: see
    /// [`Command::Quarantine`].
    fn quarantine(&mut self, run: RunId, reason: String, out: &mut Vec<Command>) {
        out.push(Command::Quarantine { run, reason });
        self.close_run(run, RunOutcome::Failed, out);
    }

    /// Closes an open run before its tasks are done: whatever runs is
    /// stopped, and every open task becomes `Cancelled`.
    fn close_run(&mut self, run: RunId, outcome: RunOutcome, out: &mut Vec<Command>) {
        let Some(mut state) = self.runs.remove(&run) else {
            return;
        };
        self.ready.remove_run(run);
        for task in state.graph().tasks() {
            if state.state(task) == TaskState::Retrying {
                self.deadlines.remove(Deadline::Retry { run, task });
            }
        }
        // The attempts move to `closing`: their reports free the slots.
        let stopped: Vec<(TaskId, Running)> = self
            .running
            .range((run, TaskId::new(0))..)
            .take_while(|((r, _), _)| *r == run)
            .map(|(&(_, task), &attempt)| (task, attempt))
            .collect();
        for (task, attempt) in stopped {
            self.running.remove(&(run, task));
            self.closing.insert((run, task, attempt.attempt), attempt);
            out.push(Command::Cancel {
                worker: attempt.worker,
                run,
                task,
            });
        }
        for task in state.cancel() {
            out.push(save(&state, task));
        }
        out.push(Command::SaveRun { run, outcome });
    }

    fn worker_joined(
        &mut self,
        worker: WorkerId,
        slots: u32,
        labels: Vec<String>,
        in_flight: &[(RunId, TaskId, u32)],
        now: Timestamp,
        out: &mut Vec<Command>,
    ) {
        // A new session replaces the old one. The attempts the worker
        // still has stay with it; the others were lost on the way.
        self.workers.remove(worker);
        if let Err(error) = self.workers.add(worker, Worker::new(slots, labels)) {
            // Cannot happen: the old session was just removed.
            out.push(Command::Anomaly {
                message: format!("{error}"),
            });
            return;
        }
        self.expect_heartbeat(worker, now);

        // Attempts of closed runs still hold slots while the worker stops
        // them; those it no longer has are done.
        let closing: Vec<((RunId, TaskId, u32), Running)> = self
            .closing
            .iter()
            .filter(|(_, attempt)| attempt.worker == worker)
            .map(|(&key, &attempt)| (key, attempt))
            .collect();
        for (key, attempt) in closing {
            if in_flight.contains(&key) {
                if let Err(error) = self.workers.reserve(worker, attempt.slots) {
                    out.push(Command::Anomaly {
                        message: format!("{error}"),
                    });
                }
            } else {
                self.closing.remove(&key);
            }
        }
        let mine: Vec<((RunId, TaskId), Running)> = self
            .running
            .iter()
            .filter(|(_, attempt)| attempt.worker == worker)
            .map(|(&key, &attempt)| (key, attempt))
            .collect();
        for ((run, task), attempt) in mine {
            if in_flight.contains(&(run, task, attempt.attempt)) {
                if let Err(error) = self.workers.reserve(worker, attempt.slots) {
                    out.push(Command::Anomaly {
                        message: format!("{error}"),
                    });
                }
            } else {
                self.lose_attempt(run, task, out);
            }
        }
        // What the worker runs and the master no longer wants: attempts of
        // cancelled runs, or given up while the worker was away.
        for &(run, task, attempt) in in_flight {
            if self.running(run, task) != Some((worker, attempt)) {
                out.push(Command::Cancel { worker, run, task });
            }
        }
    }

    fn heartbeat(&mut self, worker: WorkerId, accepting: bool, now: Timestamp) {
        // A worker must say Hello before its heartbeats count.
        if self.workers.set_accepting(worker, accepting).is_ok() {
            self.expect_heartbeat(worker, now);
        }
    }

    fn expect_heartbeat(&mut self, worker: WorkerId, now: Timestamp) {
        self.deadlines.set(
            Deadline::Heartbeat(worker),
            now.saturating_add(self.config.heartbeat_timeout),
        );
    }

    fn timer(&mut self, now: Timestamp, out: &mut Vec<Command>) {
        for deadline in self.deadlines.pop_due(now) {
            match deadline {
                Deadline::Retry { run, task } => {
                    let Some(state) = self.runs.get_mut(&run) else {
                        continue;
                    };
                    if state.retry_due(task).is_ok() {
                        out.push(save(state, task));
                        self.ready.push(run, task);
                    }
                }
                Deadline::Heartbeat(worker) => self.worker_lost(worker, out),
            }
        }
    }

    /// Assigns ready tasks to workers, as long as some worker has room.
    ///
    /// A task that fits on no worker goes back to the queue without
    /// holding up the others: a task waiting for a busy GPU does not stop
    /// one that can run anywhere.
    fn dispatch(&mut self, out: &mut Vec<Command>) {
        // Tasks that fit on no worker for now.
        let mut unplaced = Vec::new();

        while self.workers.has_free_slots() {
            let Some((run, task)) = self.ready.pop() else {
                break;
            };
            // A run cancelled while its task was queued.
            let Some(state) = self.runs.get_mut(&run) else {
                continue;
            };

            // Copy what is needed out of `options` now: it borrows `state`,
            // and `assign` below changes it.
            let options = state.graph().options(task);
            let slots = options.slots;
            let timeout = options.timeout;
            let Some(worker) = self.workers.select(slots, &options.labels) else {
                unplaced.push((run, task));
                continue;
            };

            if let Err(error) = state.assign(task) {
                // A queued task is ready by construction: if it is not,
                // the run cannot be trusted. Give the slots back first.
                let reason = format!("task `{}` was queued but {error}", state.graph().name(task));
                let _ = self.workers.release(worker, slots);
                self.quarantine(run, reason, out);
                continue;
            }
            let attempt = state.attempts(task);
            self.running.insert(
                (run, task),
                Running {
                    worker,
                    attempt,
                    slots,
                },
            );
            out.push(save(state, task));
            out.push(Command::Assign {
                worker,
                run,
                task,
                attempt,
                timeout,
            });
        }

        for (run, task) in unplaced {
            self.ready.push(run, task);
        }
    }

    /// Handles the end of an attempt.
    ///
    /// Every report is confirmed, so that the worker can drop it, but only
    /// the report of the attempt in `running` changes anything: a duplicate,
    /// or a late report of an attempt already given up, is just confirmed.
    #[expect(
        clippy::too_many_arguments,
        reason = "the fields of Event::Report, spelled out"
    )]
    fn report(
        &mut self,
        worker: WorkerId,
        run: RunId,
        task: TaskId,
        attempt: u32,
        outcome: Outcome,
        now: Timestamp,
        out: &mut Vec<Command>,
    ) {
        out.push(Command::Confirm {
            worker,
            run,
            task,
            attempt,
        });

        // Only the attempt in `running` counts: anything else is a duplicate
        // or a late report of an attempt already given up.
        let key = (run, task);
        let current = self
            .running
            .get(&key)
            .copied()
            .filter(|current| current.worker == worker && current.attempt == attempt);
        let Some(current) = current else {
            // An attempt of a run closed while it ran: it frees its slots.
            let closed = self
                .closing
                .get(&(run, task, attempt))
                .is_some_and(|closing| closing.worker == worker);
            if let Some(closing) = closed
                .then(|| self.closing.remove(&(run, task, attempt)))
                .flatten()
            {
                self.release(worker, closing.slots, out);
            }
            return;
        };
        self.running.remove(&key);
        self.release(worker, current.slots, out);

        // A run cancelled while the task ran: only the slots mattered.
        let Some(state) = self.runs.get_mut(&run) else {
            return;
        };
        // A task on a worker is assigned by construction: an error here
        // means the run cannot be trusted.
        let problem = match outcome {
            Outcome::Success => match state.succeed(task) {
                Ok(ready) => {
                    out.push(save(state, task));
                    for next in ready {
                        self.ready.push(run, next);
                    }
                    None
                }
                Err(error) => Some(error),
            },
            Outcome::Lost => {
                if state.requeue(task).is_ok() {
                    out.push(save(state, task));
                    self.ready.push(run, task);
                }
                None
            }
            Outcome::Failed | Outcome::Timeout => match state.fail(task) {
                Ok(Failure::Retry { delay }) => {
                    self.deadlines
                        .set(Deadline::Retry { run, task }, now.saturating_add(delay));
                    out.push(save(state, task));
                    None
                }
                Ok(Failure::Final { skipped }) => {
                    out.push(save(state, task));
                    for other in skipped {
                        out.push(save(state, other));
                    }
                    None
                }
                Err(error) => Some(error),
            },
        };
        if let Some(error) = problem {
            let reason = format!(
                "the report of task `{}` did not fit its state: {error}",
                state.graph().name(task)
            );
            self.quarantine(run, reason, out);
            return;
        }

        if let Some(outcome) = state.outcome() {
            out.push(Command::SaveRun { run, outcome });
            self.runs.remove(&run);
        }
    }

    /// Forgets a worker that went silent or reconnected: its attempts are
    /// lost, and their tasks go back to the queue.
    fn worker_lost(&mut self, worker: WorkerId, out: &mut Vec<Command>) {
        // The worker may already be gone, when it reconnects after expiring.
        let _ = self.workers.remove(worker);
        self.deadlines.remove(Deadline::Heartbeat(worker));

        let lost: Vec<(RunId, TaskId)> = self
            .running
            .iter()
            .filter(|(_, attempt)| attempt.worker == worker)
            .map(|(&key, _)| key)
            .collect();
        for (run, task) in lost {
            self.lose_attempt(run, task, out);
        }
        self.closing.retain(|_, attempt| attempt.worker != worker);
    }

    /// Gives back the slots of an attempt that ended.
    fn release(&mut self, worker: WorkerId, slots: u32, out: &mut Vec<Command>) {
        if let Err(error) = self.workers.release(worker, slots) {
            // The count was wrong: what the worker really has in use is
            // not known, so the safe value is nothing in use.
            if let WorkerError::ReleaseTooMany { used, .. } = error {
                let _ = self.workers.release(worker, used);
            }
            out.push(Command::Anomaly {
                message: format!("{error}: corrected to no slots in use"),
            });
        }
    }

    /// Gives up the attempt of `task` on its worker and queues the task
    /// again.
    fn lose_attempt(&mut self, run: RunId, task: TaskId, out: &mut Vec<Command>) {
        self.running.remove(&(run, task));
        let Some(state) = self.runs.get_mut(&run) else {
            return;
        };
        if state.requeue(task).is_ok() {
            out.push(save(state, task));
            self.ready.push(run, task);
        }
    }
}

/// The command that saves the current state of `task` in `run`.
fn save(run: &Run, task: TaskId) -> Command {
    Command::SaveTask {
        run: run.id(),
        task,
        state: run.state(task),
        attempts: run.attempts(task),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{TaskDef, TaskOptions};

    const RUN: RunId = RunId::new(1);
    const W1: WorkerId = WorkerId::new(1);
    const W2: WorkerId = WorkerId::new(2);
    const SECOND: i64 = 1_000;

    fn t(index: u32) -> TaskId {
        TaskId::new(index)
    }

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_millis(seconds * SECOND)
    }

    fn task(name: &str, needs: &[&str]) -> TaskDef {
        TaskDef {
            name: name.to_owned(),
            needs: needs.iter().map(|&n| n.to_owned()).collect(),
            options: TaskOptions::default(),
        }
    }

    fn graph(tasks: &[TaskDef]) -> Arc<Graph> {
        Arc::new(Graph::build(tasks).expect("valid DAG in test"))
    }

    /// a(0) → b(1)
    fn chain() -> Arc<Graph> {
        graph(&[task("a", &[]), task("b", &["a"])])
    }

    fn join(engine: &mut Engine, worker: WorkerId, slots: u32, labels: &[&str]) -> Vec<Command> {
        let labels = labels.iter().map(|&l| l.to_owned()).collect();
        engine.handle(
            Event::WorkerJoined {
                worker,
                slots,
                labels,
                in_flight: vec![],
            },
            at(0),
        )
    }

    fn start(engine: &mut Engine, graph: Arc<Graph>) -> Vec<Command> {
        engine.handle(Event::StartRun { run: RUN, graph }, at(0))
    }

    fn report(
        engine: &mut Engine,
        worker: WorkerId,
        task: TaskId,
        attempt: u32,
        outcome: Outcome,
        now: Timestamp,
    ) -> Vec<Command> {
        engine.handle(
            Event::Report {
                worker,
                run: RUN,
                task,
                attempt,
                outcome,
            },
            now,
        )
    }

    /// The `Assign`s among `commands`, as (worker, task, attempt).
    fn assigns(commands: &[Command]) -> Vec<(WorkerId, TaskId, u32)> {
        commands
            .iter()
            .filter_map(|c| match *c {
                Command::Assign {
                    worker,
                    task,
                    attempt,
                    ..
                } => Some((worker, task, attempt)),
                _ => None,
            })
            .collect()
    }

    fn saved(run: RunId, task: TaskId, state: TaskState, attempts: u32) -> Command {
        Command::SaveTask {
            run,
            task,
            state,
            attempts,
        }
    }

    fn confirm(worker: WorkerId, task: TaskId, attempt: u32) -> Command {
        Command::Confirm {
            worker,
            run: RUN,
            task,
            attempt,
        }
    }

    // No task is ever ready in these

    #[test]
    fn an_empty_run_finishes_at_once() {
        let mut engine = Engine::default();
        assert_eq!(
            start(&mut engine, graph(&[])),
            [Command::SaveRun {
                run: RUN,
                outcome: RunOutcome::Success
            }]
        );
        assert!(engine.run(RUN).is_none());
    }

    #[test]
    fn heartbeats_move_the_deadline() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        assert_eq!(engine.next_deadline(), Some(at(30)));
        engine.handle(
            Event::Heartbeat {
                worker: W1,
                accepting: true,
            },
            at(10),
        );
        assert_eq!(engine.next_deadline(), Some(at(40)));
    }

    #[test]
    fn heartbeats_of_unknown_workers_are_ignored() {
        let mut engine = Engine::default();
        engine.handle(
            Event::Heartbeat {
                worker: W1,
                accepting: true,
            },
            at(0),
        );
        assert_eq!(engine.next_deadline(), None);
    }

    // Dispatch

    #[test]
    fn ready_tasks_go_to_workers() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 2, &[]);
        let out = start(&mut engine, chain());
        assert_eq!(
            out,
            [
                saved(RUN, t(0), TaskState::Assigned, 1),
                Command::Assign {
                    worker: W1,
                    run: RUN,
                    task: t(0),
                    attempt: 1,
                    timeout: None,
                },
            ]
        );
        assert_eq!(engine.workers().get(W1).map(Worker::used), Some(1));
    }

    #[test]
    fn tasks_wait_for_a_worker() {
        let mut engine = Engine::default();
        assert!(assigns(&start(&mut engine, chain())).is_empty());
        assert_eq!(assigns(&join(&mut engine, W1, 1, &[])), [(W1, t(0), 1)]);
    }

    #[test]
    fn a_task_that_does_not_fit_does_not_hold_up_the_others() {
        let mut gpu = task("train", &[]);
        gpu.options.labels = vec!["gpu".to_owned()];
        let mut engine = Engine::default();
        join(&mut engine, W1, 2, &[]);
        let out = start(&mut engine, graph(&[gpu, task("fetch", &[])]));
        assert_eq!(assigns(&out), [(W1, t(1), 1)]);
        // The GPU task is still waiting, and goes as soon as a GPU joins.
        assert_eq!(
            assigns(&join(&mut engine, W2, 1, &["gpu"])),
            [(W2, t(0), 1)]
        );
    }

    #[test]
    fn the_timeout_goes_with_the_assignment() {
        let mut slow = task("a", &[]);
        slow.options.timeout = Some(Duration::from_secs(60));
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        let out = start(&mut engine, graph(&[slow]));
        assert!(out.iter().any(|c| matches!(
            c,
            Command::Assign { timeout: Some(d), .. } if *d == Duration::from_secs(60)
        )));
    }

    // Reports and cancellation

    #[test]
    fn success_releases_the_next_task() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        let out = report(&mut engine, W1, t(0), 1, Outcome::Success, at(5));
        assert!(out.contains(&saved(RUN, t(0), TaskState::Success, 1)));
        assert!(out.contains(&confirm(W1, t(0), 1)));
        assert_eq!(assigns(&out), [(W1, t(1), 1)], "the slot was released");
    }

    #[test]
    fn a_run_finishes_when_its_last_task_does() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        report(&mut engine, W1, t(0), 1, Outcome::Success, at(5));
        let out = report(&mut engine, W1, t(1), 1, Outcome::Success, at(9));
        assert!(out.contains(&Command::SaveRun {
            run: RUN,
            outcome: RunOutcome::Success
        }));
        assert!(engine.run(RUN).is_none());
        assert_eq!(engine.workers().get(W1).map(Worker::used), Some(0));
    }

    #[test]
    fn a_duplicate_report_is_only_confirmed() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        report(&mut engine, W1, t(0), 1, Outcome::Success, at(5));
        let again = report(&mut engine, W1, t(0), 1, Outcome::Success, at(6));
        assert_eq!(again, [confirm(W1, t(0), 1)]);
    }

    #[test]
    fn a_failure_with_retries_waits_and_tries_again() {
        let mut flaky = task("a", &[]);
        flaky.options.retries = 1;
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, graph(&[flaky]));

        let out = report(&mut engine, W1, t(0), 1, Outcome::Failed, at(5));
        assert!(out.contains(&saved(RUN, t(0), TaskState::Retrying, 1)));
        assert!(assigns(&out).is_empty(), "it waits 30 seconds first");
        assert_eq!(engine.next_deadline(), Some(at(30)), "W1's heartbeat");

        // Keep W1 alive, then let the retry come due at 35 s.
        engine.handle(
            Event::Heartbeat {
                worker: W1,
                accepting: true,
            },
            at(20),
        );
        assert_eq!(engine.next_deadline(), Some(at(35)));
        let out = engine.handle(Event::Timer, at(35));
        assert_eq!(assigns(&out), [(W1, t(0), 2)]);
    }

    #[test]
    fn a_final_failure_skips_the_rest_and_fails_the_run() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        let out = report(&mut engine, W1, t(0), 1, Outcome::Timeout, at(5));
        assert!(out.contains(&saved(RUN, t(0), TaskState::Failed, 1)));
        assert!(out.contains(&saved(RUN, t(1), TaskState::Skipped, 0)));
        assert!(out.contains(&Command::SaveRun {
            run: RUN,
            outcome: RunOutcome::Failed
        }));
    }

    #[test]
    fn cancel_stops_the_running_tasks() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        let out = engine.handle(Event::CancelRun { run: RUN }, at(3));
        assert_eq!(
            out,
            [
                Command::Cancel {
                    worker: W1,
                    run: RUN,
                    task: t(0)
                },
                saved(RUN, t(0), TaskState::Cancelled, 1),
                saved(RUN, t(1), TaskState::Cancelled, 0),
                Command::SaveRun {
                    run: RUN,
                    outcome: RunOutcome::Cancelled
                },
            ]
        );
        // The worker reports the cancelled attempt: only its slot changes.
        let out = report(&mut engine, W1, t(0), 1, Outcome::Failed, at(4));
        assert_eq!(out, [confirm(W1, t(0), 1)]);
        assert_eq!(engine.workers().get(W1).map(Worker::used), Some(0));
    }

    // Lost workers

    #[test]
    fn a_silent_worker_is_lost_and_its_tasks_move() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        engine.handle(
            Event::WorkerJoined {
                worker: W2,
                slots: 1,
                labels: vec![],
                in_flight: vec![],
            },
            at(20),
        );

        // W1 says nothing for 30 seconds: a(0) moves to W2, as attempt 2.
        let out = engine.handle(Event::Timer, at(30));
        assert!(out.contains(&saved(RUN, t(0), TaskState::Blocked, 1)));
        assert_eq!(assigns(&out), [(W2, t(0), 2)]);
        assert!(engine.workers().get(W1).is_none());
    }

    #[test]
    fn a_lost_attempt_does_not_use_up_a_retry() {
        // One retry: if the lost attempt counted, the next failure would be final.
        let mut flaky = task("a", &[]);
        flaky.options.retries = 1;
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, graph(&[flaky]));

        let out = join(&mut engine, W1, 1, &[]); // W1 reconnects
        assert_eq!(assigns(&out), [(W1, t(0), 2)]);
        let out = report(&mut engine, W1, t(0), 2, Outcome::Failed, at(5));
        assert!(out.contains(&saved(RUN, t(0), TaskState::Retrying, 2)));
    }

    #[test]
    fn an_attempt_the_worker_lost_is_tried_again_without_using_a_retry() {
        // No retries: if losing the attempt counted, the task would fail.
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        let out = report(&mut engine, W1, t(0), 1, Outcome::Lost, at(5));
        assert!(out.contains(&saved(RUN, t(0), TaskState::Blocked, 1)));
        assert!(out.contains(&confirm(W1, t(0), 1)));
        assert_eq!(assigns(&out), [(W1, t(0), 2)], "the slot was released");
    }

    #[test]
    fn a_late_report_of_a_lost_attempt_is_only_confirmed() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        join(&mut engine, W1, 1, &[]); // attempt 2 on the new session
        let late = report(&mut engine, W1, t(0), 1, Outcome::Success, at(5));
        assert_eq!(late, [confirm(W1, t(0), 1)]);
        assert_eq!(
            engine.run(RUN).map(|r| r.state(t(0))),
            Some(TaskState::Assigned)
        );
    }

    // Restart of the master and reruns

    fn snap(state: TaskState, attempts: u32) -> TaskSnapshot {
        TaskSnapshot {
            state,
            attempts,
            failures: 0,
        }
    }

    fn restore_chain_with_a_on_w1(engine: &mut Engine) -> Vec<Command> {
        engine.handle(
            Event::RestoreRun {
                run: RUN,
                graph: chain(),
                tasks: vec![snap(TaskState::Assigned, 1), snap(TaskState::Blocked, 0)],
                running: vec![(t(0), W1)],
            },
            at(0),
        )
    }

    fn rejoin(engine: &mut Engine, in_flight: Vec<(RunId, TaskId, u32)>) -> Vec<Command> {
        engine.handle(
            Event::WorkerJoined {
                worker: W1,
                slots: 1,
                labels: vec![],
                in_flight,
            },
            at(5),
        )
    }

    #[test]
    fn after_a_restart_a_worker_keeps_what_it_is_running() {
        let mut engine = Engine::default();
        assert!(restore_chain_with_a_on_w1(&mut engine).is_empty());
        let out = rejoin(&mut engine, vec![(RUN, t(0), 1)]);
        assert!(out.is_empty(), "nothing runs twice: {out:?}");
        assert_eq!(engine.workers().get(W1).map(Worker::used), Some(1));
        let out = report(&mut engine, W1, t(0), 1, Outcome::Success, at(9));
        assert_eq!(assigns(&out), [(W1, t(1), 1)]);
    }

    #[test]
    fn after_a_restart_a_task_the_worker_never_got_is_queued_again() {
        let mut engine = Engine::default();
        restore_chain_with_a_on_w1(&mut engine);
        let out = rejoin(&mut engine, vec![]);
        assert!(out.contains(&saved(RUN, t(0), TaskState::Blocked, 1)));
        assert_eq!(assigns(&out), [(W1, t(0), 2)]);
    }

    #[test]
    fn after_a_restart_a_worker_that_never_returns_is_lost() {
        let mut engine = Engine::default();
        restore_chain_with_a_on_w1(&mut engine);
        assert_eq!(engine.next_deadline(), Some(at(30)));
        let out = engine.handle(Event::Timer, at(30));
        assert_eq!(out, [saved(RUN, t(0), TaskState::Blocked, 1)]);
        assert_eq!(assigns(&join(&mut engine, W2, 1, &[])), [(W2, t(0), 2)]);
    }

    #[test]
    fn restored_ready_tasks_are_pushed() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        let out = engine.handle(
            Event::RestoreRun {
                run: RUN,
                graph: chain(),
                tasks: vec![snap(TaskState::Success, 1), snap(TaskState::Blocked, 0)],
                running: vec![],
            },
            at(0),
        );
        assert_eq!(assigns(&out), [(W1, t(1), 1)]);
    }

    #[test]
    fn a_worker_is_told_to_stop_what_the_master_no_longer_wants() {
        let mut engine = Engine::default();
        let out = rejoin(&mut engine, vec![(RUN, t(0), 1)]);
        assert_eq!(
            out,
            [Command::Cancel {
                worker: W1,
                run: RUN,
                task: t(0)
            }]
        );
    }

    #[test]
    fn rerun_reopens_the_run_and_starts_the_failed_task() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        let out = engine.handle(
            Event::RerunRun {
                run: RUN,
                graph: chain(),
                tasks: vec![snap(TaskState::Failed, 1), snap(TaskState::Skipped, 0)],
                scope: Rerun::Failed,
            },
            at(0),
        );
        assert_eq!(out[0], Command::ReopenRun { run: RUN });
        assert!(out.contains(&saved(RUN, t(1), TaskState::Blocked, 0)));
        assert_eq!(assigns(&out), [(W1, t(0), 2)]);
    }

    #[test]
    fn a_rerun_right_after_a_cancel_waits_for_the_cancelled_attempt() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        engine.handle(Event::CancelRun { run: RUN }, at(1));
        // Rerun before the worker has stopped attempt 1: its slot is
        // still taken, so attempt 2 waits.
        let out = engine.handle(
            Event::RerunRun {
                run: RUN,
                graph: chain(),
                tasks: vec![snap(TaskState::Cancelled, 1), snap(TaskState::Cancelled, 0)],
                scope: Rerun::All,
            },
            at(2),
        );
        assert!(assigns(&out).is_empty());

        assert!(engine.on_a_worker(RUN, t(0)), "attempt 1 is still stopping");

        // The report of the cancelled attempt frees the slot, and only
        // that: the reopened run goes on with attempt 2.
        let out = report(&mut engine, W1, t(0), 1, Outcome::Failed, at(3));
        assert!(
            !out.iter()
                .any(|c| matches!(c, Command::Quarantine { .. } | Command::Anomaly { .. })),
            "{out:?}"
        );
        assert_eq!(assigns(&out), [(W1, t(0), 2)]);
        let out = report(&mut engine, W1, t(0), 2, Outcome::Success, at(4));
        assert_eq!(assigns(&out), [(W1, t(1), 1)]);
    }

    #[test]
    fn a_task_that_fits_nowhere_waits_and_says_why() {
        let mut gpu = task("train", &[]);
        gpu.options.labels = vec!["gpu".to_owned()];
        let mut engine = Engine::default();
        start(&mut engine, graph(&[gpu]));
        assert_eq!(engine.waiting(), [(RUN, t(0))]);
        assert_eq!(engine.blocker(RUN, t(0)), Some(Blocker::NoWorkers));
        join(&mut engine, W1, 1, &[]);
        assert_eq!(
            engine.blocker(RUN, t(0)),
            Some(Blocker::MissingLabels(vec!["gpu".to_owned()]))
        );
        join(&mut engine, W2, 1, &["gpu"]);
        assert!(engine.waiting().is_empty(), "it went to W2");
        engine.handle(Event::CancelRun { run: RUN }, at(1));
        assert!(engine.waiting().is_empty());
    }

    // When the state does not add up

    fn quarantined(commands: &[Command]) -> Vec<(RunId, String)> {
        commands
            .iter()
            .filter_map(|c| match c {
                Command::Quarantine { run, reason } => Some((*run, reason.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_queued_task_that_is_not_ready_stops_only_its_run() {
        let other = RunId::new(2);
        let mut engine = Engine::default();
        start(&mut engine, chain());
        engine.handle(
            Event::StartRun {
                run: other,
                graph: chain(),
            },
            at(0),
        );
        // Corrupt the queue: b(1) still needs a(0).
        engine.ready.push(RUN, t(1));
        let out = join(&mut engine, W1, 4, &[]);
        let stopped = quarantined(&out);
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].0, RUN);
        assert!(
            stopped[0].1.contains("task `b` was queued but"),
            "{}",
            stopped[0].1
        );
        assert!(out.contains(&Command::SaveRun {
            run: RUN,
            outcome: RunOutcome::Failed
        }));
        assert!(engine.run(RUN).is_none(), "the run left memory");
        // Its running task is stopped; the other run carries on.
        assert!(
            out.iter()
                .any(|c| matches!(c, Command::Cancel { run, .. } if *run == RUN))
        );
        assert!(assigns(&out).iter().any(|&(_, task, _)| task == t(0)));
        assert!(engine.run(other).is_some());
    }

    #[test]
    fn a_report_that_does_not_fit_stops_the_run_and_frees_the_slot() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        // Corrupt the run: its tasks are no longer what the engine sent.
        engine.runs.get_mut(&RUN).unwrap().cancel();
        let out = report(&mut engine, W1, t(0), 1, Outcome::Success, at(5));
        assert!(
            out.contains(&confirm(W1, t(0), 1)),
            "the worker can forget it"
        );
        assert_eq!(quarantined(&out).len(), 1);
        assert!(engine.run(RUN).is_none());
        assert_eq!(engine.workers().get(W1).map(Worker::used), Some(0));
    }

    #[test]
    fn a_wrong_slot_count_is_corrected_not_fatal() {
        let mut engine = Engine::default();
        join(&mut engine, W1, 1, &[]);
        start(&mut engine, chain());
        // Corrupt the count: the slot of a(0) is already free.
        engine.workers.release(W1, 1).unwrap();
        let out = report(&mut engine, W1, t(0), 1, Outcome::Success, at(5));
        assert!(
            out.iter().any(
                |c| matches!(c, Command::Anomaly { message } if message.contains("corrected"))
            )
        );
        assert!(quarantined(&out).is_empty(), "the run is fine");
        assert_eq!(assigns(&out), [(W1, t(1), 1)]);
    }
}
