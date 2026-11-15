//! Deterministic simulations of the engine.
//!
//! A made-up clock, made-up workers and a seeded random generator drive
//! [`Engine::handle`] through days of runs in a few milliseconds. Workers
//! take time, fail, time out and disappear; the checks are the promises of
//! the engine: every run ends, no task starts before its dependencies
//! succeeded, no worker gets more than its slots, and the same seed always
//! gives the same commands.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use lwwo_core::dag::{Graph, TaskDef, TaskId, TaskOptions};
use lwwo_core::engine::{Command, Engine, EngineConfig, Event, Outcome};
use lwwo_core::run::{RunId, RunOutcome, TaskState};
use lwwo_core::time::Timestamp;
use lwwo_core::worker_selection::WorkerId;

const SECOND: i64 = 1_000;
const HOUR: i64 = 3_600 * SECOND;
const HEARTBEAT_EVERY: i64 = 5 * SECOND;

/// A small xorshift generator: the simulations need repeatable numbers,
/// not good ones.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// True once in `n` times.
    fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }
}

/// A DAG of `len` tasks where each depends on up to three earlier ones.
fn random_graph(rng: &mut Rng, len: usize) -> Arc<Graph> {
    let tasks: Vec<TaskDef> = (0..len)
        .map(|i| {
            let mut needs = Vec::new();
            for _ in 0..rng.below(4).min(i as u64) {
                let need = format!("t{}", rng.below(i as u64));
                if !needs.contains(&need) {
                    needs.push(need);
                }
            }
            TaskDef {
                name: format!("t{i}"),
                needs,
                options: TaskOptions {
                    slots: 1 + u32::from(rng.one_in(5)),
                    labels: if rng.one_in(6) {
                        vec!["big".to_owned()]
                    } else {
                        Vec::new()
                    },
                    retries: u32::try_from(rng.below(3)).unwrap(),
                    retry_delay: Duration::from_secs(10),
                    timeout: Some(Duration::from_secs(600)),
                    ..TaskOptions::default()
                },
            }
        })
        .collect();
    Arc::new(Graph::build(&tasks).expect("needs point backwards, so no cycles"))
}

/// What the simulated world does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Step {
    Heartbeat(WorkerId),
    /// The attempt ends, if its worker session is still `session`.
    Finish {
        worker: WorkerId,
        session: u32,
        run: RunId,
        task: TaskId,
        attempt: u32,
        success: bool,
    },
    Crash(WorkerId),
    Rejoin(WorkerId),
    StartRun(RunId),
    /// The master's timer.
    Timer,
}

struct SimWorker {
    slots: u32,
    labels: Vec<String>,
    alive: bool,
    session: u32,
    used: u32,
}

struct Sim {
    rng: Rng,
    engine: Engine,
    /// Steps by time; the counter keeps equal times in insertion order.
    agenda: BinaryHeap<Reverse<(i64, u64, Step)>>,
    counter: u64,
    workers: BTreeMap<WorkerId, SimWorker>,
    graphs: HashMap<RunId, Arc<Graph>>,
    /// Task states as saved by the engine: what the database would hold.
    saved: HashMap<(RunId, TaskId), TaskState>,
    outcomes: HashMap<RunId, RunOutcome>,
    armed: Option<i64>,
    trace: Vec<String>,
    assignments: u64,
}

impl Sim {
    fn new(seed: u64) -> Self {
        Self {
            rng: Rng(seed),
            engine: Engine::new(EngineConfig::default()),
            agenda: BinaryHeap::new(),
            counter: 0,
            workers: BTreeMap::new(),
            graphs: HashMap::new(),
            saved: HashMap::new(),
            outcomes: HashMap::new(),
            armed: None,
            trace: Vec::new(),
            assignments: 0,
        }
    }

    fn at(&mut self, time: i64, step: Step) {
        self.counter += 1;
        self.agenda.push(Reverse((time, self.counter, step)));
    }

    fn add_worker(&mut self, id: u32, slots: u32, labels: &[&str]) {
        let id = WorkerId::new(id);
        self.workers.insert(
            id,
            SimWorker {
                slots,
                labels: labels.iter().map(|&l| l.to_owned()).collect(),
                alive: false,
                session: 0,
                used: 0,
            },
        );
        self.at(0, Step::Rejoin(id));
    }

    fn handle(&mut self, event: Event, now: i64) {
        let commands = self.engine.handle(event, Timestamp::from_millis(now));
        for command in commands {
            self.trace.push(format!("{now} {command:?}"));
            self.execute(&command, now);
        }
        // The master arms its one timer on the nearest deadline.
        let next = self.engine.next_deadline().map(Timestamp::as_millis);
        if next != self.armed {
            self.armed = next;
            if let Some(next) = next {
                self.at(next.max(now), Step::Timer);
            }
        }
    }

    fn execute(&mut self, command: &Command, now: i64) {
        match *command {
            Command::SaveTask {
                run, task, state, ..
            } => {
                self.saved.insert((run, task), state);
            }
            Command::SaveRun { run, outcome } => {
                assert!(
                    self.outcomes.insert(run, outcome).is_none(),
                    "run {run:?} ended twice"
                );
            }
            Command::Assign {
                worker,
                run,
                task,
                attempt,
                timeout,
            } => {
                self.assignments += 1;
                let graph = Arc::clone(&self.graphs[&run]);
                self.check_dependencies(&graph, run, task);
                let slots = graph.options(task).slots;
                let sim_worker = self
                    .workers
                    .get_mut(&worker)
                    .expect("assigned to a known worker");
                assert!(sim_worker.alive || self.engine.workers().get(worker).is_some());
                sim_worker.used += slots;
                assert!(
                    sim_worker.used <= sim_worker.slots,
                    "worker {worker:?} holds {} of {} slots",
                    sim_worker.used,
                    sim_worker.slots
                );
                for label in &graph.options(task).labels {
                    assert!(sim_worker.labels.contains(label), "label {label} missing");
                }
                assert!(timeout.is_some());
                // A worker that is already gone never answers.
                if sim_worker.alive {
                    let session = sim_worker.session;
                    let duration = 1 + i64::try_from(self.rng.below(120)).unwrap();
                    let success = !self.rng.one_in(8);
                    self.at(
                        now + duration * SECOND,
                        Step::Finish {
                            worker,
                            session,
                            run,
                            task,
                            attempt,
                            success,
                        },
                    );
                }
            }
            Command::Cancel { .. } | Command::Confirm { .. } | Command::ReopenRun { .. } => {}
            // The engine works around these instead of panicking, but in a
            // simulation, where nobody touched the state, they mean a bug.
            Command::Quarantine { ref reason, .. } => panic!("a run was quarantined: {reason}"),
            Command::Anomaly { ref message } => panic!("the engine corrected itself: {message}"),
        }
    }

    /// A task goes to a worker only after all it needs succeeded.
    fn check_dependencies(&self, graph: &Graph, run: RunId, task: TaskId) {
        for before in graph.tasks() {
            if graph.successors(before).contains(&task) {
                assert_eq!(
                    self.saved.get(&(run, before)),
                    Some(&TaskState::Success),
                    "{} of {run:?} assigned before {} succeeded",
                    graph.name(task),
                    graph.name(before)
                );
            }
        }
    }

    fn step(&mut self, now: i64, step: Step) {
        match step {
            Step::StartRun(run) => {
                let graph = Arc::clone(&self.graphs[&run]);
                self.handle(Event::StartRun { run, graph }, now);
            }
            Step::Rejoin(id) => {
                let worker = self.workers.get_mut(&id).unwrap();
                worker.alive = true;
                worker.session += 1;
                worker.used = 0;
                let event = Event::WorkerJoined {
                    worker: id,
                    slots: worker.slots,
                    labels: worker.labels.clone(),
                    in_flight: Vec::new(),
                };
                self.handle(event, now);
                self.at(now + HEARTBEAT_EVERY, Step::Heartbeat(id));
            }
            Step::Heartbeat(id) => {
                if !self.workers[&id].alive {
                    return;
                }
                self.handle(
                    Event::Heartbeat {
                        worker: id,
                        accepting: true,
                    },
                    now,
                );
                if self.rng.one_in(400) {
                    self.at(now + 1, Step::Crash(id));
                } else {
                    self.at(now + HEARTBEAT_EVERY, Step::Heartbeat(id));
                }
            }
            Step::Crash(id) => {
                // The worker goes silent, and comes back in a while as a
                // new session with nothing running.
                self.workers.get_mut(&id).unwrap().alive = false;
                let away = 20 + i64::try_from(self.rng.below(100)).unwrap();
                self.at(now + away * SECOND, Step::Rejoin(id));
            }
            Step::Finish {
                worker,
                session,
                run,
                task,
                attempt,
                success,
            } => {
                let slots = self.graphs[&run].options(task).slots;
                let sim_worker = self.workers.get_mut(&worker).unwrap();
                if !sim_worker.alive || sim_worker.session != session {
                    return; // The attempt died with its session.
                }
                sim_worker.used -= slots;
                let outcome = match (success, self.rng.one_in(3)) {
                    (true, _) => Outcome::Success,
                    (false, true) => Outcome::Timeout,
                    (false, false) => Outcome::Failed,
                };
                let event = Event::Report {
                    worker,
                    run,
                    task,
                    attempt,
                    outcome,
                };
                self.handle(event.clone(), now);
                // Delivery is at-least-once: now and then a report comes twice.
                if self.rng.one_in(10) {
                    self.handle(event, now);
                }
            }
            Step::Timer => {
                if self
                    .engine
                    .next_deadline()
                    .is_some_and(|d| d.as_millis() <= now)
                {
                    self.armed = None;
                    self.handle(Event::Timer, now);
                }
            }
        }
    }

    /// Runs until nothing is left to do or `until` is reached.
    fn run(&mut self, until: i64) {
        while let Some(Reverse((time, _, step))) = self.agenda.pop() {
            if time > until {
                break;
            }
            self.step(time, step);
        }
    }
}

/// A week of hourly runs of three DAGs on four unreliable workers.
fn week(seed: u64) -> Sim {
    let mut sim = Sim::new(seed);
    sim.add_worker(1, 4, &[]);
    sim.add_worker(2, 4, &[]);
    sim.add_worker(3, 8, &["big"]);
    sim.add_worker(4, 2, &["big"]);

    let dags: Vec<Arc<Graph>> = (0..3)
        .map(|_| {
            let len = 5 + usize::try_from(sim.rng.below(40)).unwrap();
            random_graph(&mut sim.rng, len)
        })
        .collect();
    let mut id = 0;
    for hour in 0..24 * 7 {
        for graph in &dags {
            id += 1;
            let run = RunId::new(id);
            sim.graphs.insert(run, Arc::clone(graph));
            sim.at(hour * HOUR, Step::StartRun(run));
        }
    }
    // One more day for the last runs to end.
    sim.run(24 * 8 * HOUR);
    sim
}

#[test]
fn a_week_of_runs_all_end() {
    let sim = week(0x5EED);
    assert_eq!(sim.outcomes.len(), sim.graphs.len(), "every run ended");
    assert!(sim.engine.runs().next().is_none(), "no run left in memory");
    for (&run, graph) in &sim.graphs {
        let states: Vec<TaskState> = graph.tasks().map(|t| sim.saved[&(run, t)]).collect();
        assert!(states.iter().all(|s| s.is_final()));
        let expected = if states.contains(&TaskState::Failed) {
            RunOutcome::Failed
        } else {
            RunOutcome::Success
        };
        assert_eq!(sim.outcomes[&run], expected);
        // A task is skipped only behind one that failed or was skipped.
        for t in graph.tasks() {
            if states[t.index()] == TaskState::Success {
                for &next in graph.successors(t) {
                    assert_ne!(states[next.index()], TaskState::Blocked);
                }
            }
        }
    }
    for (id, worker) in sim.engine.workers().iter() {
        assert_eq!(worker.used(), 0, "worker {id:?} still holds slots");
    }
    assert!(
        sim.assignments > 5_000,
        "only {} assignments",
        sim.assignments
    );
}

#[test]
fn the_same_seed_gives_the_same_commands() {
    let first = week(42);
    let second = week(42);
    assert_eq!(first.trace, second.trace);
    assert_ne!(first.trace, week(43).trace);
}

#[test]
fn many_seeds_keep_the_promises() {
    // The checks are the assertions inside the simulation.
    for seed in 1..=6 {
        let sim = week(seed);
        assert_eq!(sim.outcomes.len(), sim.graphs.len(), "seed {seed}");
    }
}
