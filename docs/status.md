# lwwo: status

What is implemented and how it is verified, the known limitations, and what is put off. How the system works is in [design.md](design.md).

## Implementation

The project is implemented: both crates exist and work end to end. It has not yet been used in production.

| Part | Status |
| --- | --- |
| Core (`lwwo-core`) | complete: graph, runs, retries, queue, worker selection, cron and time zones, timetable, engine, restore and rerun |
| Master | complete: event loop, timer, SQLite, HTTP API, gRPC, logs |
| Worker | complete: on-disk journal and one shim per task, processes on the host, cgroups, log shipping |
| Security | complete: internal CA, tokens, mTLS, revocation, certificate renewal |
| CLI and TUI | complete; the TUI also has a demo mode and its own tests |
| Alerts | webhook for waiting tasks and failed runs |
| Map and loop, metrics | not started: see "Roadmap" |

Verification:

- **Unit tests:** 159 in the core and 86 in the binary, 11 of which drive the TUI key by key.
- **Simulations:** a week of hourly runs on unreliable workers, about one second each, reproducible for a given seed.
- **End-to-end tests with real processes** (24): retries and logs; rerun; a rerun queued behind the active run; a rerun right after a cancel; a rerun on the current version across a master restart; a scheduled run that waited for a manual one, with the alarm armed after it; cancel and timeout on the whole process tree; queued runs; a worker spool over its limit while the master is down; scheduling; joining with a token; a lost worker; a master restart and a worker restart with no task run again; a killed shim; orphaned tasks stopped; a second master refused; alerts received by a local webhook; a DAG corrupted in the database that does not stop the master; a second worker on the same directory refused; revoked and expired tokens; a worker name that cannot be taken over; online backup; certificate renewal; a watched DAG directory.
- **Linux in a container:** the same tests, plus timerfd and cgroups, which do not exist on macOS.
- **Separate machines:** master, two workers and a client in separate containers on a Docker network: joining with a token, a worker cut off from the network, a master restart.
- **The TUI on a separate machine:** the same layout, with workers of different sizes and labels, and the real TUI running on the client in a terminal whose screens are read as text: a sales ETL with parameters, a retry, a task bound to a label and a log of 20,000 lines; a timeout and a cancel; the runs, workers and DAGs screens, the detail of a run and the logs of each attempt; a run started from the TUI and its log followed live; a master restart and a stopped worker while the TUI is open.

Measurements on the release binary, on a laptop:

| Measure | Value |
| --- | --- |
| Size of the binary | 17 MB |
| Master and worker at rest | 10 MB of memory, 0% CPU |
| 1,000 independent empty tasks on 16 slots | 8.9 s, about 110 tasks per second |
| A chain of 300 empty tasks | 3.7 s, about 12 ms per task |
| Memory after those 1,300 tasks | 18 MB |

## Known limitations

Points where the implementation does less than [design.md](design.md) states.

- [ ] **Skipped occurrences:** with `overlap: skip` the skipped occurrence appears only in the master's log. The schema has no place to record it; a column or an additional state is needed.
- [ ] **Master epoch:** a number that increases at every start and is sent to the workers would let them refuse the orders of a stale master. It is postponed together with high availability, since after a restore from backup the number would go backwards.
- [ ] **Token roles:** every API token currently grants full access. A read-only token belongs to the postponed topic of users and permissions.
- [ ] **systemd executor:** on Linux with systemd the guardian of a task could be process 1, which never dies (`systemd-run`); at present the shim is always used.
- [ ] **Custom API certificate:** the master serves it, but CLI and TUI can verify only the internal CA.
- [ ] **Retries after a restart:** a task requeued after a master restart may have fewer retries left than it should, because the database does not record when it was requeued.
- [ ] **Peak memory** is available only with cgroups, hence only on Linux and with permission to create them.
- [ ] **Scheduling test:** it failed once and the failure was not reproduced. The likely cause is the port collision between parallel tests that was fixed later; this is not proven.
- [ ] **Interrupted renewal:** if the worker stops between the master's response to `Renew` and saving the new certificate, its old certificate is no longer valid and it must join again after `lwwo worker remove`. The master could accept the previous certificate until the new one is first used.
- [ ] **A worker cut off from the network keeps its tasks** (see "Surviving a worker restart"). Stopping them after `heartbeat_timeout` without contact with the master would also stop every task whenever the master is down for longer than that.

- [ ] **Live log over half the limit:** while an attempt runs, `follow` shows its output only up to half of `max_per_attempt`; the kept end appears when the log is closed. Reading the `.tail` files while the attempt runs would remove the gap.
- [ ] **Very long lines:** the shim holds one line in memory until its newline, so a task that writes gigabytes without a newline makes the shim use that much memory. Cutting lines at a fixed length would bound it.

## Roadmap

Put off on purpose, in no particular order.

- [ ] Map and loop for agentic workflows. The implementation comes later, but the engine must account from the start for a graph that grows during the run.
- [ ] Required parameters, without a default.
- [ ] A compiled graph stored in the database, or a cache of the graphs of frequently scheduled DAGs: only if building from the YAML becomes a visible cost.
- [ ] Triggers between DAGs: B starts when A ends.
- [ ] Metrics for Prometheus.
- [ ] Users and permissions for CLI and TUI.
- [ ] Logs on S3 instead of the master's disk.
- [ ] Memory-based scheduling, with evictions and task classes.
- [ ] Lineage between DAGs and data, with SQLite's recursive queries or GraphQLite on the same file.
- [ ] Fail-fast runs: stopping a run at its first failure, as a per-DAG option.
- [ ] Replaying a run step by step from the stored events of the core.
- [ ] Simulating future runs with the core, a simulated clock and historical durations.
- [ ] Reusable tasks, defined once and referenced from several DAGs.
- [ ] Releases: static binaries for Linux x86\_64 and arm64, and a Docker image.
