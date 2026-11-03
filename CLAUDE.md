# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## State of the repo

Code is being written module by module, starting from `crates/lwwo-core`. The source of truth is the design spec `lwwo — componenti e interazioni.md` (Italian). Read the relevant section before building anything; section names below refer to it. The layout, crates and commands described here are the **planned** ones from the spec's "Repository" section — check what actually exists (`git status`, `ls crates/`) before assuming any of it.

lwwo = Lightweight Workflow Orchestrator: a single Rust binary `lwwo` running as master, worker, `standalone` (master + worker in one process, data in `./lwwo-data`, API on `127.0.0.1:7070` without token — the easiest way to try it locally), TUI or CLI, with SQLite as the only store. Env vars use the `LWWO_` prefix. The spec is in Italian, but the project is meant to be published: **all code, identifiers, comments, doc comments, test names and error messages are in English**. Translate names from the spec when you write code. Protocol messages: `Hello`, `Heartbeat`, `Ack` stay as they are; `Esito` → `Report`, `Assegna` → `Assign`, `Annulla` → `Cancel`, `Svuota` → `Drain`, `Conferma` → `Confirm`.

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
cargo tree -p lwwo-core                      # must show no deps outside the approved list (currently empty)
```

## Code style

Aim for the style of well-maintained Rust libraries. Workspace lints in the root `Cargo.toml` (every crate has `[lints] workspace = true`): `unsafe_code` forbidden, `missing_docs` and `unreachable_pub` warned, `clippy::pedantic` warned. So every public item gets a doc comment, fallible public functions document `# Errors` and panicking ones `# Panics`, and getters are `#[must_use]`. Indices get newtypes (`TaskId(u32)` in `dag.rs`) so they cannot be mixed up. Error types are enums that implement `Display` and `std::error::Error` by hand (no `thiserror` in the core: it has no dependencies). Unit tests live in a `#[cfg(test)] mod tests` at the bottom of each file.

The user is learning Rust and builds the project with Claude: Claude writes the types, signatures and tests, and leaves some functions as `todo!("TASK n: …")` for the user. Never fill in a `todo!("TASK …")` unless asked; verify the tests against a reference solution in a scratch copy instead.

The timerfd wake-up is Linux-only; macOS falls back to a once-a-minute check, so clock/timer behaviour must be verified on Linux.

## Architecture (big picture)

**Two crates, decision vs. I/O.**
- `crates/lwwo-core`: a pure library with no I/O. Modules: `dag` (graph, `TaskId`), `run` (per-run state, `RunId`), `ready_queue`, `worker_selection`, `timetable`, and `engine.rs`, which exposes `handle(event, now) -> commands`. It never reads the clock, DB, network, env, files or spawns threads — time is always an input. This is what makes `tests/sim.rs` able to simulate days of runs in milliseconds. Its starting point is the run-engine prototype (crate `motore`, ~215 lines + 5 tests) described in the spec's "Prototipi" and "Il motore dei run" sections, with its benchmark numbers; the timerfd prototype's code is in "Prototipi" too.
- `crates/lwwo`: the binary. Executes the core's commands (SQLite writes, gRPC sends, arming the timer) and feeds events back. Modules: `config`, `dsl/` (YAML), `store/` (SQLite), `master/`, `worker/`, `tui/`. `build.rs` generates gRPC code from `proto/lwwo.proto`.
- **Core guard:** `crates/lwwo-core/clippy.toml` disallows `SystemTime::now`, `Instant::now`, `thread::spawn`, `env::var`, `fs::File`, `net::TcpStream`; CI also fails if `lwwo-core` gains any dependency not explicitly approved. Adding a core dependency is a design decision, not a routine change.

**Master: one event loop, no polling.** A single tokio task owns all state (no locks) and `select!`s on: the timer, worker messages, API requests, system clock changes. Every event is handled in the same order: update memory → write SQLite → push newly ready tasks to workers. Messages to workers (`Assign`, `Confirm`) are sent only **after** the commit they depend on — this ordering is what makes master crashes lose no work.

**One timer.** A single `timerfd` (absolute time, `TFD_TIMER_CANCEL_ON_SET`) is armed on the nearest of: next cron fire (from the DB `timetable`, never cached in memory beyond that one instant) and in-memory deadlines (retries, timeouts, heartbeat expiry). On fire, two indexed queries on `timetable.next_fire`: who fires now, and when is the next one.

**Run engine = incremental Kahn.** Graph is rebuilt from the version's YAML when a run starts (not stored in the DB), kept in memory only while the DAG has open runs, shared by all runs of the same version, in CSR form (one successors vector + offsets). Per-run counters of missing deps; zero → single ready queue served round-robin across runs. Failure → successors become `skipped`. Re-runs reopen the *same* run with new attempts, using the run's DAG version.

**Durability model.** Master is the only SQLite writer (WAL, `synchronous=NORMAL`) and the only party that talks to workers; task data never passes through it. Memory is rebuilt from SQLite on restart; workers reconnect with `Hello` listing in-flight tasks and hold each `Report` until `Confirm`. Delivery is at-least-once; tasks must be idempotent. Worker silent ~30s → its tasks are requeued.

**DB.** Eight `STRICT` tables (schema in the spec's "Database › Schema", versioned via `PRAGMA user_version`, migrations embedded in the binary). Current task state in `task_runs`, history in `task_attempts`; every run and attempt references a `dag_versions.hash`. Times are UTC milliseconds in `INTEGER`; ids are UUIDv7 text.

**Interfaces.** Worker↔master: one bidirectional gRPC stream opened by the worker (8 messages) plus a separate log stream; mTLS via an internal CA, workers join once with a token (Docker Swarm model). CLI and TUI (ratatui, k9s style) use only the master's HTTP/JSON API with streaming — never SQLite directly.

**Task contract.** Exit code 0 = success. No data passing between tasks and no secret management; tasks get run identity via `LWWO_*` env vars and minijinja templates (`{{ run.date }}`, `{{ params.x }}`…). Inherited `LWWO_` vars are stripped from the task environment.

## Working on the spec

Decisions already taken are in "Decisioni" (with the rejected alternative) and "Scelte scartate" (with when to reconsider). Open items are in "Aperto e rimandato" and "Ancora da decidere per la repo" (YAML crate — serde_yaml is unmaintained, cron/timezone crate with DST handling, CI, licence, binary deps). Two proposals in "Aperto e rimandato" still await confirmation: default zero retries with exponential backoff, 90-day worker certificates. Decided: pausing a DAG removes its `timetable` row, and resuming recomputes the next fire after now (fires during the pause are not caught up). Don't silently resolve an open item in code — raise it.
