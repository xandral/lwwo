# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## State of the repo

Both crates exist and work end to end: `crates/lwwo-core` (decisions, no I/O) and `crates/lwwo` (the binary: master, worker, standalone, CLI, TUI). The design document `docs/design.md` is the source of truth for behaviour: read the relevant section before changing anything. `docs/configuration.md` lists every setting and `docs/database.md` the schema; a change in behaviour goes in these documents too. Check what actually exists (`git status`, `ls crates/`) before assuming.

Known gaps against the design are listed in `docs/status.md` under "Known limitations": raise them rather than silently filling them.

lwwo = Lightweight Workflow Orchestrator: a single Rust binary `lwwo` running as master, worker, `standalone` (master + worker in one process, data in `./lwwo-data`, API on `127.0.0.1:7070` without token — the easiest way to try it locally), TUI or CLI, with SQLite as the only store. Env vars use the `LWWO_` prefix. The project is meant to be published under the MIT licence: **all code, identifiers, comments, doc comments, test names, error messages and documentation are in English**. Documentation is written in the third person and states facts: no "we" or "you", no opinions, no references to proposals or discussions.

## Commands

Standard Cargo workspace (resolver 3, edition 2024), single `Cargo.lock` and `target/` at the root:

```bash
cargo build --workspace
cargo test --workspace
cargo test -p lwwo-core                      # core only: unit tests + tests/sim.rs simulations
cargo test -p lwwo-core <test_name>          # single test
cargo test -p lwwo --test e2e                # end-to-end with real processes
cargo clippy --workspace --all-targets -- -D warnings  # pedantic lints + the core I/O ban
cargo fmt --all
scripts/check-core-deps.sh                   # lwwo-core must have no deps outside the approved list (currently empty)
scripts/test-linux.sh                        # tests + clippy in a Linux container (timerfd, cgroups compile only there)
scripts/test-linux.sh --cgroups              # same, privileged: the cgroup test really runs
scripts/test-two-machines.sh                 # master, two workers and a client in separate containers on a Docker network
cargo run -p lwwo -- standalone              # try it: then `lwwo apply examples/dags/etl_sales.yaml`, `lwwo run etl_sales`, `lwwo tui`
```

`cargo run -p lwwo -- tui --demo` shows the TUI without a master. Its tests (`tui/tests.rs`) press keys on an `App<Demo>` and look at the screen as text: use `tui.shows(&[…])` for anything in the message box, which types messages out one after another.

When trying things by hand, never use the default ports or `pkill -f "lwwo standalone"`: a standalone may already be running on 7070 with data in `./lwwo-data`. Start trial masters with `--api-listen=127.0.0.1:<other port>` and a data dir under the system temp directory, and stop them by pid.

The e2e tests start real `lwwo` processes under the system temp dir, with masters listening on port 0 and the tests reading the ports from the master's `master ready` log line (`Lab::learn_ports`): picking a "free" port by hand raced with parallel tests and with Docker Desktop; the cron test waits for the next minute, so the suite takes up to about a minute. `lwwo api GET /runs` prints raw JSON and is what the tests use to look inside.

`scripts/` and `.github/` are local only: they are in `.gitignore` and not published (decided 2026-11-15), so the README and the docs do not mention them.

## Code style

Aim for the style of well-maintained Rust libraries. Workspace lints in the root `Cargo.toml` (every crate has `[lints] workspace = true`): `unsafe_code` forbidden, `missing_docs` and `unreachable_pub` warned, `clippy::pedantic` warned. So every public item gets a doc comment, fallible public functions document `# Errors` and panicking ones `# Panics`, and getters are `#[must_use]`. Indices get newtypes (`TaskId(u32)` in `dag.rs`) so they cannot be mixed up. Error types are enums that implement `Display` and `std::error::Error` by hand (no `thiserror` in the core: it has no dependencies). Unit tests live in a `#[cfg(test)] mod tests` at the bottom of each file.

The binary uses `anyhow` for errors and `pub(crate)` everywhere (nothing in a binary is public API). `crates/lwwo/clippy.toml` lists the product names that need no backticks in docs.

The timerfd wake-up is Linux-only; macOS falls back to a once-a-minute check, so clock/timer behaviour must be verified on Linux.

## Architecture (big picture)

**Two crates, decision vs. I/O.**
- `crates/lwwo-core`: a pure library with no I/O. Modules: `dag` (graph, `TaskId`), `run` (per-run state, `RunId`), `ready_queue`, `worker_selection`, `calendar` (civil dates), `cron` (parser + next fire in local time, no dependency), `timetable`, and `engine.rs`, which exposes `handle(event, now) -> commands`. It never reads the clock, DB, network, env, files or spawns threads — time is always an input. This is what makes `tests/sim.rs` able to simulate days of runs in milliseconds.
- `crates/lwwo`: the binary. Executes the core's commands (SQLite writes, gRPC sends, arming the timer) and feeds events back. Modules: `config`, `dsl/` (YAML + templates), `zone` (jiff behind the core's `TimeZone`), `store/` (SQLite), `security` (CA, certificates, tokens), `master/` (`state.rs` is the event loop's state and every decision around the engine; `api.rs` HTTP, `grpc.rs` workers, `logs.rs`, `timer.rs`), `worker/` (`mod.rs` session loop; `journal.rs` the per-attempt directory that is the worker's write-ahead log; `shim.rs` the `lwwo shim` process that runs one task and survives the worker; `watch.rs` launches shims and follows their files; `ship.rs` log shipping; `cgroup.rs`), `client` + `cli` (HTTP client and commands), `tui/` (three layers: `backend.rs` is the trait the screens see, with `remote.rs` over the HTTP API and `demo.rs` on made-up data; `app.rs` is state and keys, no terminal; `ui.rs`, `graph.rs`, `theme.rs`, `dialog.rs` draw). `build.rs` generates gRPC code from `proto/lwwo.proto` (needs `protoc`).
- **Names vs ids.** The core knows tasks as `TaskId` and workers as `WorkerId(u32)`; the database and the protocol use names. `master/state.rs` translates: tasks are in name order in every graph (the DSL sorts them), worker names are interned per process.
- **Core guard:** `crates/lwwo-core/clippy.toml` disallows `SystemTime::now`, `Instant::now`, `thread::spawn`, `env::var`, `fs::File`, `net::TcpStream`; CI also fails if `lwwo-core` gains any dependency not explicitly approved. Adding a core dependency is a design decision, not a routine change.

**Master: one event loop, no polling.** A single tokio task owns all state (no locks) and `select!`s on: the timer, worker messages, API requests, system clock changes. Every event is handled in the same order: update memory → write SQLite → push newly ready tasks to workers. Messages to workers (`Assign`, `Confirm`) are sent only **after** the commit they depend on — this ordering is what makes master crashes lose no work.

**One timer.** A single `timerfd` (absolute time, `TFD_TIMER_CANCEL_ON_SET`) is armed on the nearest of: next cron fire (from the DB `timetable`, never cached in memory beyond that one instant) and in-memory deadlines (retries, heartbeat expiry). Task timeouts are enforced by the worker, which gets the duration in `Assign`; retries are decided by the master, so they can move to another worker. On fire, two indexed queries on `timetable.next_fire`: who fires now, and when is the next one.

**Run engine = incremental Kahn.** Graph is rebuilt from the version's YAML when a run starts (not stored in the DB), kept in memory only while the DAG has open runs, shared by all runs of the same version, in CSR form (one successors vector + offsets). Per-run counters of missing deps; zero → single ready queue served round-robin across runs. Failure → successors become `skipped`. Re-runs reopen the *same* run with new attempts, using the run's DAG version.

**Worker durability.** The worker never holds a task's process: it writes `spec.json`, starts `lwwo shim <dir>` in its own process group and only reads files after that (`started.json`, the `log.<offset>` spool segments, `exit.json`), woken by a kernel file watcher. A restarted worker scans `data_dir/attempts` and picks up what it finds; a directory is removed only on `Confirm`. Shim liveness is an `flock` on `shim.lock`, never a pid. A shim stops its task when the worker's `alive` file is older than the master's heartbeat timeout, so a dead worker's task does not keep running beside its replacement (a worker cut off from the network still does, until it is back: delivery is at least once).

**Errors stop only what they are about.** The engine never panics on an event: a report or a queued task that does not fit a run's state emits `Command::Quarantine` (stored in `dag_runs.problem`, the run fails, its attempts are cancelled), a wrong shared count emits `Command::Anomaly` and is corrected. In the master, `contain()` keeps an error about one DAG or queued run from stopping the loop; only database errors (`is_storage`) stop it. Never catch a panic to go on. In `tests/sim.rs` a quarantine or anomaly is a failure.

**One process per data directory.** `util::lock_dir` takes an `flock` on `<data_dir>/lock` for the master and for the worker. The journal stores a boot id with every pid (`journal::boot_id`), and a pid from another boot is never signalled.

**Alerts.** `[alerts]` is off unless `webhook` is set (`master/alerts.rs`). The master tracks since when each ready-but-unplaced task waits (`Engine::waiting`, reasons from `WorkerPool::blocker` in the core), puts the nearest `waiting_after` deadline in the same alarm, and reports each wait once; `on_failure` posts when a run ends failed. Delivery is at most once, after the commit, never stored.

**Durability model.** Master is the only SQLite writer (WAL, `synchronous=NORMAL`) and the only party that talks to workers; task data never passes through it. Memory is rebuilt from SQLite on restart; workers reconnect with `Hello` listing in-flight tasks and hold each `Report` until `Confirm`. Delivery is at-least-once; tasks must be idempotent. Worker silent ~30s → its tasks are requeued.

**DB.** Eight `STRICT` tables (schema in `docs/database.md`, versioned via `PRAGMA user_version`, migrations embedded in the binary). Current task state in `task_runs`, history in `task_attempts`; every run and attempt references a `dag_versions.hash`. Times are UTC milliseconds in `INTEGER`; ids are UUIDv7 text.

**Interfaces.** Worker↔master: one bidirectional gRPC stream opened by the worker (8 messages) plus a separate log stream; mTLS via an internal CA, workers join once with a token (Docker Swarm model). CLI and TUI (ratatui, k9s style) use only the master's HTTP/JSON API with streaming — never SQLite directly.

**Task contract.** Exit code 0 = success. No data passing between tasks and no secret management; tasks get run identity via `LWWO_*` env vars and minijinja templates (`{{ run.date }}`, `{{ params.x }}`…). Inherited `LWWO_` vars are stripped from the task environment.

## Working on the design document

Decisions already taken are in `docs/decisions.md`: "Decisions" (with the rejected alternative) and "Alternatives considered" (with when to reconsider). Open items are in `docs/status.md`: "Known limitations" lists where the code does less than the design says, "Roadmap" what was put off. `docs/design.md` describes only how the system works, precisely enough to leave no ambiguity; rationale goes in decisions.md. Still open: releases. Decided: zero retries by default, with `retry_delay` (30s) doubling at each attempt by default (`retry_backoff: exponential|fixed`, optional `max_retry_delay` cap); pausing a DAG removes its `timetable` row, and resuming recomputes the next occurrence after now (occurrences during the pause are not caught up). DST: a cron time that does not exist runs at the first valid instant after it; one that happens twice runs only the first time. Don't silently resolve an open item in code — raise it.

`docs/images/run-events.svg` is generated by `scripts/run-events-diagram.py`.
