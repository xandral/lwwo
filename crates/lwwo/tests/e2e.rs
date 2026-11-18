//! End-to-end tests: the real binary, real processes, real sockets.
//!
//! Every test starts its own master in a temporary directory and drives
//! it the way a person would, through the `lwwo` command line.

use std::cell::Cell;
use std::fmt::Write as _;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

const LWWO: &str = env!("CARGO_BIN_EXE_lwwo");

/// A background `lwwo` process, killed when the test ends.
struct Process(Child);

impl Process {
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}

/// A master, standalone or not, and how to talk to it.
struct Lab {
    dir: PathBuf,
    /// The ports the master got, read from its log: masters listen on
    /// port 0, so the system picks a free port and no two tests can race
    /// for the same one.
    api: Cell<u16>,
    workers: Cell<u16>,
    token: Option<String>,
    /// More flags for the master of a [`Cluster`], at every start.
    master_flags: Vec<String>,
}

impl Lab {
    fn new(name: &str) -> Self {
        // Under the system's temporary directory, which Docker can mount.
        let dir = std::env::temp_dir().join(format!("lwwo-e2e-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self {
            dir,
            api: Cell::new(0),
            workers: Cell::new(0),
            token: None,
            master_flags: Vec::new(),
        }
    }

    fn spawn(&self, log: &str, args: &[&str]) -> Process {
        let log = std::fs::File::create(self.dir.join(log)).unwrap();
        let child = Command::new(LWWO)
            .args(args)
            .current_dir(&self.dir)
            .env_remove("LWWO_TOKEN")
            .env("LWWO_LOG", "info")
            // Something a task must never see.
            .env("LWWO_WORKER_SECRET_THING", "leaked")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        Process(child)
    }

    /// Waits until the master logging to `log` is ready, and learns its
    /// ports from the line it writes then.
    fn learn_ports(&self, log: &str) {
        let path = self.dir.join(log);
        let mut found = None;
        self.wait_for("the master to be ready", || {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            found = text.lines().find_map(|line| {
                let rest = line.split("master ready: API on ").nth(1)?;
                let (api, workers) = rest.split_once(", workers on ")?;
                let port = |address: &str| address.trim().rsplit(':').next()?.parse::<u16>().ok();
                Some((port(api)?, port(workers)?))
            });
            found.is_some()
        });
        let (api, workers) = found.expect("found above");
        self.api.set(api);
        self.workers.set(workers);
    }

    fn standalone(&self, extra: &[&str]) -> Process {
        let listen = "--api-listen=127.0.0.1:0".to_owned();
        let mut args = vec![
            "standalone",
            &listen,
            "--worker-slots=4",
            "--worker-kill_grace=1s",
        ];
        args.extend(extra);
        let process = self.spawn("standalone.log", &args);
        self.learn_ports("standalone.log");
        self.wait_for("the worker to connect", || {
            self.api("GET", "/status", None)["workers"] == 1
        });
        process
    }

    /// Runs a client command and returns its output, or its error.
    fn try_cli(&self, args: &[&str]) -> Result<String, String> {
        let mut command = Command::new(LWWO);
        command
            .args(args)
            .current_dir(&self.dir)
            .env("LWWO_API", format!("127.0.0.1:{}", self.api.get()))
            .env("HOME", &self.dir)
            .env_remove("LWWO_TOKEN");
        if let Some(token) = &self.token {
            command.env("LWWO_TOKEN", token);
        }
        let output = command.output().unwrap();
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned())
        }
    }

    fn cli(&self, args: &[&str]) -> String {
        self.try_cli(args)
            .unwrap_or_else(|error| panic!("lwwo {args:?} failed: {error}\n{}", self.logs()))
    }

    /// Calls the API; `Null` while the master does not answer.
    fn api(&self, method: &str, path: &str, body: Option<&str>) -> Value {
        let mut args = vec!["api", method, path];
        args.extend(body);
        self.try_cli(&args)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or(Value::Null)
    }

    fn wait_for(&self, what: &str, mut ready: impl FnMut() -> bool) {
        let start = Instant::now();
        while !ready() {
            assert!(
                start.elapsed() < Duration::from_secs(90),
                "timed out waiting for {what}\n{}",
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    /// What the processes of this lab logged, for failure messages.
    fn logs(&self) -> String {
        let mut all = String::new();
        for entry in std::fs::read_dir(&self.dir).unwrap().flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "log") {
                let _ = writeln!(all, "--- {}", entry.path().display());
                all += &std::fs::read_to_string(entry.path()).unwrap_or_default();
            }
        }
        all
    }

    fn apply(&self, yaml: &str) {
        let file = self.dir.join("dag.yaml");
        std::fs::write(&file, yaml).unwrap();
        self.cli(&["apply", file.to_str().unwrap()]);
    }

    /// Starts a run by hand and returns its id.
    fn run(&self, dag: &str, extra: &[&str]) -> String {
        let mut args = vec!["run", dag];
        args.extend(extra);
        self.cli(&args)
            .split_whitespace()
            .next()
            .unwrap()
            .to_owned()
    }

    fn detail(&self, run: &str) -> Value {
        self.api("GET", &format!("/runs/{run}"), None)
    }

    fn run_state(&self, run: &str) -> String {
        self.detail(run)["run"]["state"]
            .as_str()
            .unwrap_or("?")
            .to_owned()
    }

    fn task(&self, run: &str, task: &str) -> Value {
        let detail = self.detail(run);
        detail["tasks"]
            .as_array()
            .and_then(|tasks| tasks.iter().find(|t| t["task"] == task).cloned())
            .unwrap_or(Value::Null)
    }

    /// Waits for the run to end and returns its final state.
    fn finished(&self, run: &str) -> String {
        // The state that ended the wait: reading it again could meet a
        // master too busy to answer.
        let mut state = String::new();
        self.wait_for("the run to end", || {
            state = self.run_state(run);
            matches!(state.as_str(), "success" | "failed" | "cancelled")
        });
        state
    }

    fn log(&self, run: &str, task: &str, attempt: u32) -> String {
        self.cli(&["logs", run, task, "--attempt", &attempt.to_string()])
    }

    fn outcomes(&self, run: &str, task: &str) -> Vec<String> {
        self.detail(run)["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["task"] == task)
            .map(|a| a["outcome"].as_str().unwrap_or("open").to_owned())
            .collect()
    }
}

impl Drop for Lab {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// Whether the process whose pid is written in `file` is still running.
fn alive(file: &Path) -> bool {
    let pid = std::fs::read_to_string(file).unwrap_or_default();
    Command::new("kill")
        .args(["-0", pid.trim()])
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

/// Whether a worker has no journal left: every attempt was confirmed.
fn journal_is_empty(worker_data: &Path) -> bool {
    std::fs::read_dir(worker_data.join("attempts"))
        .map_or(true, |mut entries| entries.next().is_none())
}

fn path_text(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[test]
fn a_run_goes_through_its_graph_with_retries_and_logs() {
    let lab = Lab::new("graph");
    let _master = lab.standalone(&[]);
    lab.apply(
        r#"
name: etl
params: {country: IT}
tasks:
  extract:
    run: ["sh", "-c", "echo extracting {{ params.country }} of {{ run.date }}"]
  clean:
    needs: [extract]
    retries: 1
    retry_delay: 1s
    run: ["sh", "-c", "echo attempt $LWWO_ATTEMPT; echo careful >&2; test $LWWO_ATTEMPT -ge 2"]
  count:
    needs: [extract]
    env: {LEVEL: "info-{{ task.name }}"}
    run: ["sh", "-c", "echo $LEVEL $LWWO_DAG $LWWO_TASK $LWWO_PARAM_COUNTRY; pwd; echo secret=$LWWO_WORKER_SECRET_THING"]
  load:
    needs: [clean, count]
    run: ["echo", "loading {{ run.id }}"]
"#,
    );
    let run = lab.run(
        "etl",
        &["--param", "country=FR", "--at", "2026-10-01T03:00:00Z"],
    );
    assert_eq!(lab.finished(&run), "success");

    assert_eq!(lab.task(&run, "clean")["attempts"], 2);
    assert_eq!(lab.outcomes(&run, "clean"), ["failed", "success"]);
    assert!(
        lab.log(&run, "extract", 1)
            .contains("extracting FR of 2026-10-01")
    );
    let first = lab.log(&run, "clean", 1);
    assert!(
        first.contains("attempt 1") && first.contains("ERR careful"),
        "{first}"
    );
    assert!(lab.log(&run, "clean", 2).contains("attempt 2"));
    let count = lab.log(&run, "count", 1);
    assert!(count.contains("info-count etl count FR"), "{count}");
    assert!(
        count.contains(&format!("/work/{run}/count/1")),
        "private directory: {count}"
    );
    assert!(
        count.contains("secret=\n") || count.ends_with("secret="),
        "LWWO_ variables are stripped: {count}"
    );
    assert!(lab.log(&run, "load", 1).contains(&format!("loading {run}")));

    // What the listings show.
    assert!(lab.cli(&["runs", "--dag", "etl"]).contains("success"));
    assert!(lab.cli(&["show", &run]).contains("clean, count"));
    assert!(lab.cli(&["dags"]).contains("etl"));
    assert!(lab.cli(&["workers"]).contains("0/4"));

    // Mistakes are refused with a reason.
    let unknown = lab
        .try_cli(&["run", "etl", "--param", "contry=FR"])
        .unwrap_err();
    assert!(unknown.contains("no parameter `contry`"), "{unknown}");
    let cycle = lab.dir.join("cycle.yaml");
    std::fs::write(
        &cycle,
        "name: bad\ntasks: {a: {run: [x], needs: [b]}, b: {run: [x], needs: [a]}}",
    )
    .unwrap();
    assert!(
        lab.try_cli(&["apply", path_text(&cycle)])
            .unwrap_err()
            .contains("cycle")
    );
}

#[test]
fn a_failure_skips_what_depends_on_it_and_a_rerun_continues_the_same_run() {
    let lab = Lab::new("rerun");
    let _master = lab.standalone(&[]);
    let marker = lab.dir.join("fixed");
    lab.apply(&format!(
        r#"
name: fragile
tasks:
  first:
    run: ["true"]
  breaks:
    needs: [first]
    run: ["test", "-e", "{}"]
  after:
    needs: [breaks]
    run: ["true"]
  beside:
    needs: [first]
    run: ["true"]
"#,
        marker.display()
    ));
    let run = lab.run("fragile", &[]);
    assert_eq!(lab.finished(&run), "failed");
    assert_eq!(lab.task(&run, "breaks")["state"], "failed");
    assert_eq!(lab.task(&run, "after")["state"], "skipped");
    assert_eq!(
        lab.task(&run, "beside")["state"],
        "success",
        "independent branches go on"
    );

    // Rerunning without fixing fails again; the history stays in the run.
    lab.cli(&["rerun", &run]);
    assert_eq!(lab.finished(&run), "failed");
    std::fs::write(&marker, "").unwrap();
    lab.cli(&["rerun", &run]);
    assert_eq!(lab.finished(&run), "success");
    assert_eq!(lab.task(&run, "breaks")["attempts"], 3);
    assert_eq!(
        lab.task(&run, "first")["attempts"],
        1,
        "what succeeded is not run again"
    );
    assert_eq!(lab.task(&run, "after")["attempts"], 1);

    // One task alone, then everything.
    lab.cli(&["rerun", &run, "--task", "first", "--only"]);
    assert_eq!(lab.finished(&run), "success");
    assert_eq!(lab.task(&run, "first")["attempts"], 2);
    assert_eq!(lab.task(&run, "beside")["attempts"], 1);
    lab.cli(&["rerun", &run, "--all"]);
    assert_eq!(lab.finished(&run), "success");
    assert_eq!(lab.task(&run, "beside")["attempts"], 2);
}

#[test]
fn cancel_and_timeout_stop_the_whole_process_tree() {
    let lab = Lab::new("stop");
    let _master = lab.standalone(&[]);
    let pids = lab.dir.join("pids");
    std::fs::create_dir_all(&pids).unwrap();
    // The task starts a child and waits: both must die.
    let script = format!(
        "sleep 300 & echo $! > {0}/$LWWO_TASK.child; echo $$ > {0}/$LWWO_TASK.parent; wait",
        pids.display()
    );
    lab.apply(&format!(
        r#"
name: slow
tasks:
  forever:
    run: ["sh", "-c", "{script}"]
  limited:
    timeout: 2s
    run: ["sh", "-c", "{script}"]
  never:
    needs: [forever]
    run: ["true"]
"#
    ));
    let run = lab.run("slow", &[]);
    lab.wait_for("the timeout", || {
        lab.task(&run, "limited")["state"] == "failed"
    });
    assert_eq!(lab.outcomes(&run, "limited"), ["timeout"]);
    assert!(lab.log(&run, "limited", 1).contains("lwwo: timeout"));

    lab.wait_for("the task to start", || pids.join("forever.child").exists());
    lab.cli(&["cancel", &run]);
    assert_eq!(lab.finished(&run), "cancelled");
    assert_eq!(lab.task(&run, "forever")["state"], "cancelled");
    assert_eq!(lab.task(&run, "never")["state"], "cancelled");
    lab.wait_for("the worker to report", || {
        lab.outcomes(&run, "forever") == ["cancelled"]
    });

    let alive = |file: &str| {
        let pid = std::fs::read_to_string(pids.join(file)).unwrap();
        Command::new("kill")
            .args(["-0", pid.trim()])
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    };
    lab.wait_for("the processes to die", || {
        !alive("forever.child") && !alive("forever.parent") && !alive("limited.child")
    });
    lab.wait_for("the slots to be free", || {
        lab.cli(&["workers"]).contains("0/4")
    });
}

#[test]
fn a_second_worker_on_the_same_data_refuses_to_start() {
    let (cluster, _master) = Cluster::new("worker-lock");
    let join = cluster.lab.cli(&["token", "worker"]);
    let _w1 = cluster.worker("w1", Some(join.trim()));
    cluster
        .lab
        .wait_for("w1 to join", || cluster.connected("w1"));
    let mut twin = cluster.worker("w1", None);
    assert!(!twin.0.wait().unwrap().success());
    let said = std::fs::read_to_string(cluster.lab.dir.join("w1.log")).unwrap();
    assert!(
        said.contains("another lwwo worker is already running"),
        "{said}"
    );
}

#[test]
fn a_second_master_on_the_same_data_refuses_to_start() {
    let lab = Lab::new("lock");
    let _master = lab.standalone(&[]);
    let other = "--api-listen=127.0.0.1:0".to_owned();
    let mut second = lab.spawn("second.log", &["standalone", &other]);
    assert!(!second.0.wait().unwrap().success());
    let said = std::fs::read_to_string(lab.dir.join("second.log")).unwrap();
    assert!(
        said.contains("another lwwo master is already running"),
        "{said}"
    );
    // The first one never noticed.
    assert_eq!(lab.api("GET", "/status", None)["workers"], 1);
}

#[test]
fn overlap_queue_makes_a_second_run_wait() {
    let lab = Lab::new("queue");
    let _master = lab.standalone(&[]);
    lab.apply("name: one_at_a_time\ntasks:\n  work:\n    run: [sleep, '2']\n");
    let first = lab.run("one_at_a_time", &[]);
    let second = lab.run("one_at_a_time", &[]);
    assert_eq!(lab.run_state(&first), "running");
    assert_eq!(lab.run_state(&second), "queued");
    assert_eq!(lab.finished(&first), "success");
    assert_eq!(lab.finished(&second), "success");
    let started = |run: &str| lab.detail(run)["run"]["started_at"].as_i64().unwrap();
    let ended = lab.detail(&first)["run"]["finished_at"].as_i64().unwrap();
    assert!(
        started(&second) >= ended,
        "the second started after the first ended"
    );

    // `skip` refuses, `replace` cancels the active run.
    lab.apply("name: skipper\noverlap: skip\ntasks:\n  work:\n    run: [sleep, '30']\n");
    let active = lab.run("skipper", &[]);
    assert!(
        lab.try_cli(&["run", "skipper"])
            .unwrap_err()
            .contains("skip")
    );
    lab.apply("name: skipper\noverlap: replace\ntasks:\n  work:\n    run: [sleep, '1']\n");
    let newer = lab.run("skipper", &[]);
    assert_eq!(lab.finished(&active), "cancelled");
    assert_eq!(lab.finished(&newer), "success");
}

#[test]
fn the_cron_starts_runs_and_pause_stops_it() {
    let lab = Lab::new("cron");
    let _master = lab.standalone(&[]);
    lab.apply("name: minutely\nschedule: '* * * * *'\ntasks:\n  tick:\n    run: [echo, '{{ run.time }}']\n");
    let runs = || lab.api("GET", "/runs?dag=minutely", None);
    lab.wait_for("the first fire", || {
        runs().as_array().is_some_and(|r| !r.is_empty())
    });
    let run = runs()[0].clone();
    assert_eq!(run["trigger"], "cron");
    let logical = run["logical_time"].as_i64().unwrap();
    assert_eq!(
        logical % 60_000,
        0,
        "the run carries the instant of its fire"
    );
    let late = run["created_at"].as_i64().unwrap() - logical;
    assert!((0..5_000).contains(&late), "woken {late} ms after the fire");

    lab.cli(&["pause", "minutely"]);
    assert_eq!(
        lab.api("GET", "/dags/minutely", None)["dag"]["next_fire"],
        Value::Null
    );
    assert!(lab.cli(&["resume", "minutely"]).contains("next run"));
    assert!(
        lab.api("GET", "/dags/minutely", None)["dag"]["next_fire"]
            .as_i64()
            .unwrap()
            > logical
    );
}

/// Master and workers as separate processes, with tokens and mutual TLS.
struct Cluster {
    lab: Lab,
}

impl Cluster {
    fn new(name: &str) -> (Self, Process) {
        Self::with(name, &[])
    }

    /// A cluster whose master gets `flags` too.
    fn with(name: &str, flags: &[&str]) -> (Self, Process) {
        let mut lab = Lab::new(name);
        lab.master_flags = flags.iter().map(|&flag| flag.to_owned()).collect();
        let master = Self::master(&lab);
        let token_file = lab.dir.join("master/admin.token");
        lab.wait_for("the admin token", || token_file.exists());
        lab.token = Some(std::fs::read_to_string(token_file).unwrap());
        lab.wait_for("the API", || {
            lab.api("GET", "/status", None)["workers"] == 0
        });
        (Self { lab }, master)
    }

    fn master(lab: &Lab) -> Process {
        // The first start takes any free ports; a restart takes the same
        // ones again, since the workers know that address.
        let api = format!("--api-listen=127.0.0.1:{}", lab.api.get());
        let workers = format!("--workers-listen=127.0.0.1:{}", lab.workers.get());
        let mut args = vec![
            "master",
            &api,
            &workers,
            "--storage-data_dir=master",
            "--workers-heartbeat_interval=1s",
            "--workers-heartbeat_timeout=4s",
        ];
        args.extend(lab.master_flags.iter().map(String::as_str));
        let process = lab.spawn("master.log", &args);
        lab.learn_ports("master.log");
        process
    }

    fn worker(&self, name: &str, token: Option<&str>) -> Process {
        let master = format!("--master=127.0.0.1:{}", self.lab.workers.get());
        let data = format!("--data_dir={name}");
        let name_flag = format!("--name={name}");
        let mut args = vec![
            "worker",
            &master,
            &data,
            &name_flag,
            "--slots=2",
            "--kill_grace=1s",
        ];
        // The token goes in a file: as a flag it would show in `ps`.
        let token = token.map(|token| {
            let file = self.lab.dir.join(format!("{name}.join"));
            std::fs::write(&file, token).unwrap();
            format!("--token-file={}", file.display())
        });
        args.extend(token.as_deref());
        self.lab.spawn(&format!("{name}.log"), &args)
    }

    fn connected(&self, name: &str) -> bool {
        self.lab
            .api("GET", "/workers", None)
            .as_array()
            .is_some_and(|workers| workers.iter().any(|w| w["name"] == name))
    }
}

#[test]
fn a_restart_of_the_master_reruns_nothing() {
    let (cluster, mut master) = Cluster::new("restart");
    let lab = &cluster.lab;
    let join = lab.cli(&["token", "worker"]);
    let _w1 = cluster.worker("w1", Some(join.trim()));
    lab.wait_for("w1 to join", || cluster.connected("w1"));

    let counter = lab.dir.join("starts");
    lab.apply(&format!(
        "name: steady\ntasks:\n  long:\n    run: [sh, -c, 'echo start >> {}; sleep 4; echo done']\n  next:\n    needs: [long]\n    run: [echo, after]\n",
        counter.display()
    ));
    let run = lab.run("steady", &[]);
    lab.wait_for("the task to start", || counter.exists());

    // The master dies while the task runs, and comes back.
    master.kill();
    let _master = Cluster::master(lab);
    assert_eq!(lab.finished(&run), "success");
    assert_eq!(
        std::fs::read_to_string(&counter).unwrap(),
        "start\n",
        "the task ran once"
    );
    assert_eq!(lab.task(&run, "long")["attempts"], 1);
    assert_eq!(lab.outcomes(&run, "long"), ["success"]);
    assert!(
        lab.log(&run, "long", 1).contains("done"),
        "the log survived the restart"
    );
    assert_eq!(lab.task(&run, "next")["state"], "success");
}

#[test]
fn workers_enter_with_the_token_and_a_lost_one_gives_its_tasks_back() {
    let (cluster, _master) = Cluster::new("cluster");
    let lab = &cluster.lab;

    // Nobody gets in without the right token, on either door.
    let mut anonymous = Command::new(LWWO);
    anonymous
        .args(["status"])
        .env("LWWO_API", format!("127.0.0.1:{}", lab.api.get()))
        .env("HOME", &lab.dir)
        .env_remove("LWWO_TOKEN");
    assert!(
        !anonymous.output().unwrap().status.success(),
        "the API needs a token"
    );
    let join = lab.cli(&["token", "worker"]).trim().to_owned();
    assert_eq!(
        lab.cli(&["token", "worker"]).trim(),
        join,
        "the join token is the same for all"
    );
    let mut wrong = cluster.worker("intruder", Some(&join.replace(".tok_", ".tok_x")));
    assert!(
        !wrong.0.wait().unwrap().success(),
        "a wrong token is refused"
    );
    assert!(!cluster.connected("intruder"));
    let mut api_token_as_join = cluster.worker("sneaky", lab.token.as_deref());
    assert!(
        !api_token_as_join.0.wait().unwrap().success(),
        "an API token is not a join token"
    );

    let mut w1 = cluster.worker("w1", Some(&join));
    lab.wait_for("w1 to join", || cluster.connected("w1"));
    assert!(lab.dir.join("w1/certs/worker.key").exists());

    let first_pid = lab.dir.join("pid.1");
    lab.apply(&format!(
        "name: moving\ntasks:\n  work:\n    run: [sh, -c, 'echo $$ > {}/pid.$LWWO_ATTEMPT; echo on attempt $LWWO_ATTEMPT; test $LWWO_ATTEMPT -ge 2 || sleep 300']\n",
        lab.dir.display()
    ));
    let run = lab.run("moving", &[]);
    lab.wait_for("the task to be assigned", || {
        lab.task(&run, "work")["worker"] == "w1"
    });

    // w1 disappears; w2 joins and gets the task as a new attempt.
    w1.kill();
    let _w2 = cluster.worker("w2", Some(&join));
    assert_eq!(lab.finished(&run), "success");
    assert_eq!(lab.outcomes(&run, "work"), ["lost", "success"]);
    let attempts = lab.detail(&run)["attempts"].clone();
    assert_eq!(attempts[1]["worker"], "w2");
    // The task w1 left behind does not go on for ever: its shim stops it
    // once the worker has been away longer than the heartbeat timeout,
    // since by then the master gave the task to someone else.
    lab.wait_for("the orphaned task to stop", || !alive(&first_pid));

    // w1 comes back with its certificate, without the token.
    let mut w1 = cluster.worker("w1", None);
    lab.wait_for("w1 to reconnect", || cluster.connected("w1"));

    // Rotating the token does not touch those who are in; removing a
    // worker locks it out for good.
    let rotated = lab.cli(&["token", "worker", "--rotate"]).trim().to_owned();
    assert_ne!(rotated, join);
    let mut late = cluster.worker("w3", Some(&join));
    assert!(
        !late.0.wait().unwrap().success(),
        "the old token stopped working"
    );
    // The join token alone cannot take the name of a worker that exists.
    let stolen = lab.dir.join("stolen.join");
    std::fs::write(&stolen, &rotated).unwrap();
    let impostor = Command::new(LWWO)
        .args([
            "worker",
            &format!("--master=127.0.0.1:{}", lab.workers.get()),
        ])
        .args([
            "--data_dir=impostor",
            "--name=w1",
            &format!("--token-file={}", stolen.display()),
        ])
        .current_dir(&lab.dir)
        .output()
        .unwrap();
    assert!(!impostor.status.success());
    let said = String::from_utf8_lossy(&impostor.stderr).into_owned();
    assert!(said.contains("already registered"), "{said}");
    assert!(cluster.connected("w1"), "the real w1 is untouched");

    lab.cli(&["worker", "remove", "w1"]);
    lab.wait_for("w1 to be dropped", || !cluster.connected("w1"));
    std::thread::sleep(Duration::from_secs(3));
    assert!(!cluster.connected("w1"), "a revoked certificate is refused");
    w1.kill();
    let _w3 = cluster.worker("w3", Some(&rotated));
    lab.wait_for("w3 to join", || cluster.connected("w3"));

    // Draining: the worker takes nothing more.
    lab.cli(&["drain", "w3"]);
    lab.wait_for("w3 to drain", || {
        lab.cli(&["workers"])
            .lines()
            .any(|l| l.starts_with("w3") && l.contains("not accepting"))
    });
}

/// Only where the worker can create cgroups: `scripts/test-linux.sh
/// --cgroups` sets the variable.
#[test]
fn a_cgroup_holds_the_whole_tree_and_measures_its_memory() {
    if std::env::var("LWWO_TEST_CGROUPS").as_deref() != Ok("1") {
        eprintln!("skipped: needs a Linux where the worker can create cgroups");
        return;
    }
    let lab = Lab::new("cgroup");
    // In the test container the worker shares its cgroup with cargo and
    // the tests: `auto` would rightly leave it alone.
    let _master = lab.standalone(&["--worker-cgroups=on"]);
    let pid_file = lab.dir.join("escaped");
    // `setsid` takes the child out of the process group of the task: only
    // the cgroup still holds it.
    lab.apply(&format!(
        "name: escape\ntasks:\n  quick:\n    run: [sh, -c, 'head -c 20000000 /dev/zero | tail -c 1 | wc -c']\n  runaway:\n    run: [sh, -c, 'setsid sleep 300 & echo $! > {}; sleep 300']\n",
        pid_file.display()
    ));
    let run = lab.run("escape", &[]);
    lab.wait_for("the quick task", || {
        lab.task(&run, "quick")["state"] == "success"
    });
    let attempts = lab.detail(&run)["attempts"].clone();
    let quick = attempts
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["task"] == "quick")
        .unwrap();
    assert!(
        quick["peak_memory"].as_u64().unwrap_or(0) > 0,
        "no peak memory: {quick}"
    );

    lab.wait_for("the runaway to start", || pid_file.exists());
    std::thread::sleep(Duration::from_millis(300));
    let pid = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .to_owned();
    assert!(Path::new(&format!("/proc/{pid}")).exists());
    lab.cli(&["cancel", &run]);
    lab.wait_for("the escaped process to die", || {
        // A killed process may linger as a zombie until it is collected.
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map_or(true, |stat| stat.contains(") Z "))
    });
}

#[test]
fn a_restart_of_the_worker_loses_nothing() {
    let (cluster, _master) = Cluster::new("worker-restart");
    let lab = &cluster.lab;
    let join = lab.cli(&["token", "worker"]);
    let mut w1 = cluster.worker("w1", Some(join.trim()));
    lab.wait_for("w1 to join", || cluster.connected("w1"));

    let counter = lab.dir.join("starts");
    lab.apply(&format!(
        "name: steady\ntasks:\n  long:\n    run: [sh, -c, 'echo start >> {}; sleep 4; echo done']\n  next:\n    needs: [long]\n    run: [echo, after]\n",
        counter.display()
    ));
    let run = lab.run("steady", &[]);
    lab.wait_for("the task to start", || counter.exists());

    // The worker dies while the task runs, and a new one takes its place:
    // the task goes on under its shim, and the new worker finds it.
    w1.kill();
    let _w1 = cluster.worker("w1", None);
    assert_eq!(lab.finished(&run), "success");
    assert_eq!(
        std::fs::read_to_string(&counter).unwrap(),
        "start\n",
        "the task ran once"
    );
    assert_eq!(lab.task(&run, "long")["attempts"], 1);
    assert_eq!(lab.outcomes(&run, "long"), ["success"]);
    assert!(
        lab.log(&run, "long", 1).contains("done"),
        "the log is whole"
    );
    assert_eq!(lab.task(&run, "next")["state"], "success");
    // Once the master confirmed, nothing is left on the worker's disk.
    lab.wait_for("the journal to be cleaned", || {
        journal_is_empty(&lab.dir.join("w1"))
    });
}

#[test]
fn a_task_whose_shim_dies_is_stopped_and_tried_again() {
    let lab = Lab::new("shim");
    let _master = lab.standalone(&[]);
    let pid = lab.dir.join("task.pid");
    // No retries: if a lost attempt counted as a failure, the run would fail.
    lab.apply(&format!(
        "name: guarded\ntasks:\n  work:\n    run: [sh, -c, 'test $LWWO_ATTEMPT -ge 2 && exit 0; echo $$ > {}; sleep 300']\n",
        pid.display()
    ));
    let run = lab.run("guarded", &[]);
    lab.wait_for("the task to start", || pid.exists());

    // Kill the shim of this test's attempt, and nothing else: its
    // command line carries the directory of this lab.
    let lab_name = lab.dir.file_name().unwrap().to_str().unwrap();
    let shim = format!("lwwo shim .*{lab_name}/");
    assert!(
        Command::new("pkill")
            .args(["-9", "-f", &shim])
            .status()
            .unwrap()
            .success()
    );

    assert_eq!(lab.finished(&run), "success");
    assert_eq!(lab.outcomes(&run, "work"), ["lost", "success"]);
    assert!(!alive(&pid), "the task the shim was watching was stopped");
    let worker_data = lab.dir.join("lwwo-data/worker");
    lab.wait_for("the journal to be cleaned", || {
        journal_is_empty(&worker_data)
    });
}

/// A webhook receiver: keeps the body of every POST it gets.
struct Hook {
    url: String,
    bodies: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Hook {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&bodies);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(&stream);
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0; length];
                if reader.read_exact(&mut body).is_ok() {
                    if let Ok(json) = serde_json::from_slice(&body) {
                        kept.lock().unwrap().push(json);
                    }
                }
                let _ = (&stream).write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Self { url, bodies }
    }

    fn of_kind(&self, kind: &str) -> Vec<serde_json::Value> {
        let bodies = self.bodies.lock().unwrap();
        bodies
            .iter()
            .filter(|body| body["kind"] == kind)
            .cloned()
            .collect()
    }
}

#[test]
fn alerts_tell_about_stuck_tasks_and_failed_runs() {
    let lab = Lab::new("alerts");
    let hook = Hook::start();
    let webhook = format!("--alerts-webhook={}", hook.url);
    let _master = lab.standalone(&[
        &webhook,
        "--alerts-waiting_after=2s",
        "--alerts-on_failure=true",
    ]);

    // A misspelled label: the task can never start.
    lab.apply("name: stuck\ntasks:\n  train:\n    labels: [gpuu]\n    run: ['true']\n");
    let stuck = lab.run("stuck", &[]);
    lab.wait_for("the waiting alert", || {
        !hook.of_kind("tasks_waiting").is_empty()
    });
    let alert = &hook.of_kind("tasks_waiting")[0];
    let text = alert["text"].as_str().unwrap();
    assert!(
        text.contains("1 task has been waiting for a worker for more than 2s"),
        "{text}"
    );
    assert!(
        text.contains("stuck / train: no connected worker has the label \"gpuu\""),
        "{text}"
    );
    assert_eq!(alert["tasks"][0]["run"], stuck.as_str());
    // The same wait is reported once.
    std::thread::sleep(Duration::from_secs(4));
    assert_eq!(hook.of_kind("tasks_waiting").len(), 1);
    // `lwwo show` says it too, alerts or not.
    // And a backup of the database meanwhile, with the master running.
    assert!(lab.cli(&["backup", "copy.db"]).contains("bytes"));
    assert!(lab.dir.join("lwwo-data/backups/copy.db").exists());
    let again = lab.try_cli(&["backup", "copy.db"]).unwrap_err();
    assert!(again.contains("exists already"), "{again}");
    // A name, never a path: a token cannot make the master write elsewhere.
    for path in ["/tmp/elsewhere.db", "../outside.db", ".hidden"] {
        let refused = lab.try_cli(&["backup", path]).unwrap_err();
        assert!(refused.contains("is not a name for a backup"), "{refused}");
    }
    // Without a name the copy is named after the time.
    assert!(lab.cli(&["backup"]).contains("/backups/lwwo-"));
    let shown = lab.cli(&["show", &stuck]);
    assert!(
        shown.contains("train waits for a worker: no connected worker has the label \"gpuu\""),
        "{shown}"
    );

    lab.apply("name: broken\ntasks:\n  fails:\n    run: ['false']\n  after:\n    needs: [fails]\n    run: ['true']\n");
    let broken = lab.run("broken", &[]);
    assert_eq!(lab.finished(&broken), "failed");
    lab.wait_for("the failure alert", || {
        !hook.of_kind("run_failed").is_empty()
    });
    let alert = &hook.of_kind("run_failed")[0];
    assert_eq!(alert["dag"], "broken");
    assert_eq!(alert["run"], broken.as_str());
    assert_eq!(alert["failed_tasks"], serde_json::json!(["fails"]));
    assert!(
        alert["text"]
            .as_str()
            .unwrap()
            .starts_with("lwwo: broken failed")
    );
}

#[test]
fn a_dag_broken_in_the_database_stops_only_itself() {
    let lab = Lab::new("contain");
    let mut master = lab.standalone(&[]);
    lab.apply("name: healthy\ntasks:\n  work:\n    run: ['true']\n");
    lab.apply("name: damaged\nschedule: '0 3 * * *'\ntasks:\n  work:\n    run: ['true']\n");
    master.kill();

    // Someone edits the database by hand: the stored YAML no longer reads,
    // and its fire is due right now.
    let db = rusqlite::Connection::open(lab.dir.join("lwwo-data/lwwo.db")).unwrap();
    db.execute(
        "UPDATE dag_versions SET yaml = 'name: [' WHERE dag_name = 'damaged'",
        [],
    )
    .unwrap();
    db.execute(
        "UPDATE timetable SET next_fire = 0 WHERE dag_name = 'damaged'",
        [],
    )
    .unwrap();
    drop(db);

    let _master = lab.standalone(&[]);
    let run = lab.run("healthy", &[]);
    assert_eq!(
        lab.finished(&run),
        "success",
        "the master is up and the rest works"
    );
    let log = std::fs::read_to_string(lab.dir.join("standalone.log")).unwrap();
    assert!(
        log.contains("the schedule of `damaged` cannot be handled"),
        "{log}"
    );
}

#[test]
fn tokens_can_be_listed_revoked_and_made_to_expire() {
    let (cluster, _master) = Cluster::new("tokens");
    let lab = &cluster.lab;
    let with = |token: &str| {
        Command::new(LWWO)
            .args(["status"])
            .env("LWWO_API", format!("127.0.0.1:{}", lab.api.get()))
            .env("HOME", &lab.dir)
            .env("LWWO_TOKEN", token)
            .output()
            .unwrap()
            .status
            .success()
    };
    let ci = lab
        .cli(&["token", "api", "--label", "ci"])
        .trim()
        .to_owned();
    let brief = lab
        .cli(&["token", "api", "--label", "brief", "--expires", "1s"])
        .trim()
        .to_owned();
    assert!(with(&ci) && with(&brief));

    // The id is the middle part of the token.
    let id = ci.split('.').nth(1).unwrap().to_owned();
    lab.cli(&["token", "revoke", &id]);
    assert!(!with(&ci), "a revoked token stops working at once");
    std::thread::sleep(Duration::from_millis(1_500));
    assert!(!with(&brief), "an expired token stops working");

    let list = lab.cli(&["token", "list"]);
    for wanted in ["revoked", "expired", "valid", "ci", "brief", "worker"] {
        assert!(list.contains(wanted), "`{wanted}` not in:\n{list}");
    }
    assert!(
        !list.contains(ci.split('.').nth(2).unwrap()),
        "no secret is shown"
    );
    let again = lab.try_cli(&["token", "revoke", &id]).unwrap_err();
    assert!(again.contains("no valid token"), "{again}");
}

#[test]
fn a_worker_renews_its_certificate_before_it_expires() {
    // Certificates of 12 seconds: valid from 6 seconds before they are
    // signed, so two thirds of their life is 6 seconds after.
    let (cluster, _master) = Cluster::with("renew", &["--workers-cert_lifetime=12s"]);
    let lab = &cluster.lab;
    let join = lab.cli(&["token", "worker"]);
    let mut w1 = cluster.worker("w1", Some(join.trim()));
    lab.wait_for("w1 to join", || cluster.connected("w1"));
    let joined = Instant::now();
    let cert = lab.dir.join("w1/certs/worker.crt");
    let first = std::fs::read_to_string(&cert).unwrap();

    // A copy of the first certificate and its key, as a thief would have.
    std::fs::create_dir_all(lab.dir.join("thief/certs")).unwrap();
    for file in ["worker.crt", "worker.key", "ca.crt"] {
        std::fs::copy(
            lab.dir.join("w1/certs").join(file),
            lab.dir.join("thief/certs").join(file),
        )
        .unwrap();
    }

    lab.wait_for("the certificate to be renewed", || {
        std::fs::read_to_string(&cert).is_ok_and(|now| now != first)
    });
    assert!(
        joined.elapsed() > Duration::from_secs(4),
        "renewed too early: after {:?}",
        joined.elapsed()
    );
    assert!(lab.logs().contains("certificate renewed"));
    assert!(cluster.connected("w1"), "the session went on meanwhile");

    // The certificate that was replaced no longer opens the door, though
    // it has not expired yet.
    let mut thief = cluster.worker("thief", None);
    lab.wait_for("the old certificate to be refused", || {
        std::fs::read_to_string(lab.dir.join("thief.log"))
            .unwrap_or_default()
            .contains("revoked or replaced")
    });
    thief.kill();

    // Past the life of the first certificate, a restarted worker gets in
    // with what it renewed since, and works.
    let left = Duration::from_secs(14).saturating_sub(joined.elapsed());
    std::thread::sleep(left);
    w1.kill();
    lab.wait_for("the master to notice", || !cluster.connected("w1"));
    let _w1 = cluster.worker("w1", None);
    lab.wait_for("w1 to come back", || cluster.connected("w1"));
    lab.apply("name: after\ntasks:\n  hello:\n    run: [echo, renewed]\n");
    let run = lab.run("after", &[]);
    assert_eq!(lab.finished(&run), "success");
    assert!(lab.log(&run, "hello", 1).contains("renewed"));
}

#[test]
fn a_watched_directory_applies_the_dags_put_in_it() {
    let lab = Lab::new("watch");
    let dags = lab.dir.join("dags");
    std::fs::create_dir_all(&dags).unwrap();
    let _standalone = lab.standalone(&[&format!("--dags-watch={}", dags.display())]);
    let hash = |name: &str| {
        lab.api("GET", &format!("/dags/{name}"), None)["dag"]["current"]
            .as_str()
            .map(str::to_owned)
    };

    std::fs::write(
        dags.join("seen.yaml"),
        "name: seen\ntasks:\n  one:\n    run: [echo, first]\n",
    )
    .unwrap();
    // Not YAML, and YAML that is not a DAG: neither stops the watching.
    std::fs::write(dags.join("notes.txt"), "name: notes\n").unwrap();
    std::fs::write(dags.join("broken.yaml"), "name: broken\ntasks: 3\n").unwrap();
    lab.wait_for("the DAG to be applied", || hash("seen").is_some());
    let first = hash("seen").unwrap();
    assert!(hash("notes").is_none());
    assert!(hash("broken").is_none());

    // A change is a new version, and runs use it.
    std::thread::sleep(Duration::from_millis(1_100));
    std::fs::write(
        dags.join("seen.yaml"),
        "name: seen\ntasks:\n  one:\n    run: [echo, second]\n",
    )
    .unwrap();
    lab.wait_for("the new version", || {
        hash("seen").is_some_and(|now| now != first)
    });
    let run = lab.run("seen", &[]);
    assert_eq!(lab.finished(&run), "success");
    assert!(lab.log(&run, "one", 1).contains("second"));
}

/// Sends a raw HTTP request to the API, as a browser or a script would,
/// and returns the status line and the body.
fn raw_http(port: u16, request: &str) -> String {
    use std::io::{Read as _, Write as _};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut answer = String::new();
    let _ = stream.read_to_string(&mut answer);
    answer
}

#[test]
fn without_tokens_the_api_refuses_web_pages_and_keeps_its_data_private() {
    use std::os::unix::fs::PermissionsExt as _;
    let lab = Lab::new("local");
    let _standalone = lab.standalone(&[]);
    let port = lab.api.get();
    let proof = lab.dir.join("proof");
    let yaml = format!(
        "name: pwn\ntasks:\n  x:\n    run: [touch, {}]\n",
        proof.display()
    );
    let send = |host: &str, origin: &str| {
        raw_http(
            port,
            &format!(
                "POST /dags HTTP/1.1\r\nHost: {host}\r\n{origin}Content-Type: text/plain\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{yaml}",
                yaml.len()
            ),
        )
    };

    // A page of another site: what a browser sends without asking first.
    let local = format!("127.0.0.1:{port}");
    let from_a_page = send(&local, "Origin: https://evil.example\r\n");
    assert!(from_a_page.starts_with("HTTP/1.1 403"), "{from_a_page}");
    // A name its owner pointed at 127.0.0.1.
    let rebound = send(&format!("evil.example:{port}"), "");
    assert!(rebound.starts_with("HTTP/1.1 403"), "{rebound}");
    assert_eq!(lab.api("GET", "/dags", None), serde_json::json!([]));

    // The same request from this machine, not from a browser, is taken.
    let direct = send(&local, "");
    assert!(direct.starts_with("HTTP/1.1 200"), "{direct}");
    let run = lab.run("pwn", &[]);
    assert_eq!(lab.finished(&run), "success");
    assert!(proof.exists());

    // What tasks print and the environment they get is for one user.
    for dir in ["lwwo-data", "lwwo-data/worker"] {
        let mode = std::fs::metadata(lab.dir.join(dir))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700, "{dir}");
    }
}

#[test]
fn a_worker_name_with_strange_characters_is_refused() {
    let (cluster, _master) = Cluster::new("names");
    let lab = &cluster.lab;
    let join = lab.cli(&["token", "worker"]);
    let _bad = cluster.worker("bad name", Some(join.trim()));
    lab.wait_for("the join to be refused", || {
        std::fs::read_to_string(lab.dir.join("bad name.log"))
            .unwrap_or_default()
            .contains("may only have letters")
    });
    assert_eq!(lab.api("GET", "/workers", None), serde_json::json!([]));

    // A host name is fine.
    let _good = cluster.worker("mac-mini.local", Some(join.trim()));
    lab.wait_for("the worker to join", || cluster.connected("mac-mini.local"));
}
