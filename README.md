# lwwo

**lwwo** (Lightweight Workflow Orchestrator) runs workflows: a workflow
declares a set of tasks, their dependencies and a schedule, and lwwo runs
each task at the right moment, on a machine with room for it, and records
the outcome.

It belongs to the same family as Airflow, Dagster and Argo Workflows, at the
small end of it: **one binary, one SQLite file, nothing else to install.**

```text
  lwwo apply · run · tui              worker            worker
        │                           ┌────────┐        ┌────────┐
        │ HTTP                      │ tasks  │        │ tasks  │
        ▼                           └───┬────┘        └───┬────┘
  ┌───────────┐   pushes tasks ▶        │  gRPC stream    │
  │  master   │◀────────────────────────┴─────────────────┘
  └─────┬─────┘   ◀ results, logs
        ▼
     SQLite
```

> **Status: experimental.** lwwo works end to end and is tested at several
> levels, but it has not been used in production. See
> [Project status](#project-status).

## Design principles

Most orchestrators consist of several services: a scheduler, a web server, a
database, a message queue and workers. lwwo is built on the following
principles.

- **One binary.** The same `lwwo` program is the master that decides, the
  worker that executes, the command line and the terminal interface. It is
  17 MB and takes 10 MB of memory at rest.
- **One file of data.** All state lives in a SQLite database next to the
  master. There is no Postgres to provision and no Redis to operate.
- **Event-driven, no polling.** The master computes when the next thing is
  due and asks the kernel to wake it then; until that moment it uses no CPU.
  A result from a worker or a request to the API wakes it earlier.
- **Work is pushed.** Each worker opens one gRPC stream to the master, and
  tasks arrive on it the moment they are ready. Workers need no open ports,
  and nothing waits for a polling round.
- **A crash loses nothing, on either side.** The master commits to SQLite
  before it sends anything, so after a restart it resumes where it was and
  no task runs twice. A worker hands each task to a small guardian process
  and keeps a journal on disk, so it can be restarted or upgraded while its
  tasks keep running.
- **Decisions are separate from I/O.** Every decision (which task is ready,
  which worker gets it, what a failure means, when a schedule fires) is in a
  library, `lwwo-core`, with no input or output and no dependencies. It
  receives an event and the current time and returns commands. A week of
  runs on failing workers is simulated in about a second.
- **Security without ceremony.** A worker joins with one token, receives a
  certificate from the master's own authority, and from then on all traffic
  is mutual TLS. No certificate is created or copied by hand.
- **A terminal interface** in the style of k9s: runs, graphs and live logs,
  usable over SSH, with no web server.

Deliberately out of scope: passing data between tasks, storing secrets, a
web interface, and a second master. Tasks write their results to a location
derived from the date of the run, and secrets come from the service already
in use for them. While the single master is down nothing new starts, and
nothing is lost.

The complete design, with every decision and the alternatives rejected, is
in [docs/design.md](docs/design.md).

## Requirements

- **Rust**, to build. The exact version is pinned in `rust-toolchain.toml`
  and installed by `rustup` on the first `cargo` command
  (<https://rustup.rs>).
- **protoc**, the Protocol Buffers compiler, used during the build:
  `brew install protobuf` on macOS, `apt install protobuf-compiler` on
  Debian and Ubuntu.
- **Docker or Podman**, only for tasks that run in containers.
- **git**, only for tasks that take their code from a repository.

lwwo runs on Linux and macOS. Linux is the target platform: waking exactly
on time when the system clock is changed (timerfd) and stopping every
process a task has started (cgroups) rely on Linux features. On macOS both
have a simpler fallback.

## Building

```sh
git clone <this repository>
cd lwwo
cargo build --release
```

The result is one file, `target/release/lwwo`. The examples below call it
`lwwo`, which assumes it is on the `PATH` or aliased:

```sh
alias lwwo="$PWD/target/release/lwwo"
```

## Quick start

The `standalone` mode runs the master and one worker in a single process,
keeps its data in `./lwwo-data` and asks for no token:

```sh
lwwo standalone
```

In a second terminal, apply the example workflow, a nightly ETL made of
shell commands:

```sh
lwwo apply examples/dags/etl_sales.yaml
```

`apply` validates the file and stores it. An unknown field, a dependency
cycle or a template that names a value that does not exist is rejected at
this point, not when the run starts. The example is scheduled for 03:00
every night; a run can also be started by hand:

```sh
lwwo run etl_sales
```

To follow it:

```sh
lwwo runs                     # the latest runs
lwwo show <run>               # one run and its tasks; a prefix of the id is enough
lwwo logs <run> clean         # the output of a task
lwwo logs <run> clean -f      # the same, live
```

The task `clean` fails once and succeeds on its second attempt: the example
does so on purpose, to show a retry.

### Terminal interface

```sh
lwwo tui
```

The TUI opens on the list of runs, each with a bar that fills as its tasks
succeed. `Enter` opens a run and shows its tasks by level; `Tab` switches to
the full graph with every dependency drawn; `Enter` on a task shows its
output. From the list, `Tab` cycles through DAGs and workers. The message
box describes the item under the cursor and reports what just happened, the
last line lists the keys available in the current screen, and `?` lists
them all.

`lwwo tui --demo` opens the same interface on sample data, without a master.

## Defining a workflow

A workflow, or DAG, is a YAML file:

```yaml
name: etl_sales
schedule: "0 3 * * *"          # every day at 03:00
timezone: Europe/Rome

params:
  country: IT

defaults:
  retries: 2

tasks:
  extract:
    run: ["python", "extract.py", "--country", "{{ params.country }}"]

  clean:
    needs: [extract]
    run: ["python", "clean.py", "--day", "{{ run.date }}"]

  load:
    needs: [clean]
    image: ghcr.io/acme/etl:1.4.2
    run: ["python", "load.py", "--day", "{{ run.date }}"]
    timeout: 1h
```

- **A task is a command.** `run` is a program and its arguments. lwwo starts
  it and considers only its exit code: zero is success, anything else is
  failure. There is no library to import and no task API: a Python script, a
  compiled program, `dbt` or a shell one-liner (`["sh", "-c", "..."]`) are
  all tasks.
- **Where it runs.** By default, as a process on the worker. With `image`,
  in a container, so the worker needs only Docker or Podman. With `source`,
  the worker first checks out a git repository into the task's working
  directory.
- **Dependencies.** `needs` lists the tasks that must succeed first. lwwo
  derives the order and runs in parallel whatever can be. When a task fails,
  the tasks that depend on it are skipped and independent branches continue.
- **Run identity.** A task learns which run it belongs to from the
  `{{ ... }}` templates in its command and from environment variables such
  as `LWWO_RUN_DATE` and `LWWO_PARAM_COUNTRY`. A task that derives its
  output path from these values overwrites the same file when it runs again,
  which is what makes retries and reruns safe.
- **Parameters.** `params` are values with defaults that a single run can
  override:

  ```sh
  lwwo run etl_sales --param country=FR
  lwwo run etl_sales --at 2026-10-01T03:00     # a run for that instant
  ```

  Past days are reprocessed with a loop of `lwwo run --at`.
- **Schedule policies.** `overlap` decides what happens when a run is due
  while the previous one is still active: wait (`queue`, the default),
  `skip`, run alongside (`allow`) or `replace` it. `catchup` decides what
  happens after downtime: only the latest missed occurrence runs (the
  default), or all of them.

Every field is listed in [docs/configuration.md](docs/configuration.md).

### Data between tasks

**lwwo does not pass data between tasks.** It does not capture, store or
transfer task output, and a task returns nothing but its exit code. Each
task receives the identity of its run in the environment, and tasks exchange
data themselves, by writing to and reading from a location derived from it:

```yaml
tasks:
  extract:
    run: [sh, -c, 'python extract.py > /data/$LWWO_RUN_ID/raw.json']
  clean:
    needs: [extract]
    run: [sh, -c, 'python clean.py < /data/$LWWO_RUN_ID/raw.json']
```

| Variable | Content |
| --- | --- |
| `LWWO_RUN_ID` | unique id of the run |
| `LWWO_RUN_DATE`, `LWWO_RUN_TIME` | day and instant the run is for, in the DAG's time zone |
| `LWWO_DAG`, `LWWO_TASK` | names of the DAG and of the task |
| `LWWO_ATTEMPT` | attempt number, from 1 |
| `LWWO_PARAM_<NAME>` | the parameters of the run |
| `LWWO_WORKDIR` | private working directory of the attempt |

- The location must be reachable from every worker that can run the tasks:
  a bucket, a network file system or a database. On a single machine a local
  directory is enough.
- A path built from `LWWO_RUN_ID` is unique per run, and a rerun of the same
  run overwrites the same files. A path built from `LWWO_RUN_DATE` is shared
  by all runs for that day, which suits daily jobs.
- `LWWO_ATTEMPT` does not belong in an output path: a retry would write
  elsewhere, and the next task could not know which attempt to read.
- `LWWO_WORKDIR` is created for one attempt and removed when the attempt
  ends. It is for temporary files, not for data shared between tasks.

## Failures and reruns

A failed task is retried as many times as `retries` says, with an increasing
delay. When the retries are exhausted the run ends as failed, and once the
cause is fixed it can be resumed instead of restarted:

```sh
lwwo rerun <run>                  # the failed tasks, and those skipped because of them
lwwo rerun <run> --task clean     # one task and everything downstream of it
lwwo rerun <run> --all
```

A rerun adds new attempts to the same run, so its whole history stays in one
place.

When a worker disappears and does not return, its tasks are assigned to
another worker after the heartbeat timeout (30 seconds by default), without
using up a retry. `lwwo cancel <run>` stops a run and every process it
started.

## Alerts

Alerts are off by default. With a webhook configured, lwwo reports two
situations, each enabled separately:

```toml
[alerts]
webhook = "https://hooks.slack.com/services/..."
waiting_after = "15m"
on_failure = true
```

- `on_failure` reports every run that ends as failed, with the failed tasks.
- `waiting_after` reports a task that is ready but that no worker takes, for
  example because of a misspelled label or because the only matching machine
  is off. Nothing fails in that case and the run stays open, so lwwo
  reports the task once it has waited longer than the configured time, with
  the reason:

```text
lwwo: 1 task has been waiting for a worker for more than 15m:
• etl_sales / train: no connected worker has the label "gpuu"
```

Each alert is a JSON POST with a readable `text` field, which is what Slack
displays, and the structured details beside it. The reason a task is waiting
is also shown by `lwwo show` and in the TUI, whether or not alerts are
enabled.

## Deployment

In production the master and the workers are separate processes, usually on
separate machines, and all traffic between them is encrypted.

### Master

```sh
lwwo master --config examples/master.toml
```

On its first start the master creates its certificate authority and writes
an administrator token to its data directory. The command line and the TUI
use that token:

```sh
export LWWO_API=master.internal:7070
export LWWO_TOKEN=$(cat /var/lib/lwwo/admin.token)
lwwo status
```

### Workers

A worker joins with the join token printed by the master:

```sh
lwwo token worker                 # on the master: prints lwwo1_...

# on the worker machine
echo 'lwwo1_...' > /etc/lwwo/join.token && chmod 600 /etc/lwwo/join.token
lwwo worker --master master.internal:7071 --token-file /etc/lwwo/join.token --slots 8
```

- The token is needed only for the first connection. With it the worker
  obtains a certificate, and from then on master and worker authenticate
  each other by certificate. The worker renews its certificate by itself.
- A token carries the fingerprint of the master's certificate authority, so
  it is never sent to a machine that is not the right master.
- The join token is not accepted as a command-line flag, where `ps` would
  expose it: it is given in a file or in `LWWO_WORKER_TOKEN`.
- The join token alone cannot take the name of a registered worker. A name
  is released with `lwwo worker remove <name>`, which requires an API token
  and also revokes the worker's certificate.

A worker offers a number of **slots**, the tasks it runs at once, and
optional **labels** such as `gpu` or `big`. A task that asks for a label is
assigned only to workers that have it. This is the whole resource model.

### Security model

- An API token is an administrator credential: a DAG is a command, so whoever
  holds a token can run commands on every worker, as the worker's user.
- Tasks run as the worker's user and are not isolated from each other or
  from the worker: a task can read the worker's key and the files of other
  tasks. Containers see only what is mounted and passed to them.
- `standalone` has no authentication: every process on the machine can use
  its API. It refuses requests coming from web pages. It is meant for trying
  lwwo on a personal machine, not for shared hosts.
- Data directories are created for their owner only (mode `0700`).

The full table is in [docs/design.md](docs/design.md#security-model).

### Restarting and removing workers

A worker can be stopped and started again, for an upgrade or after a crash,
without disturbing its tasks. Each task is watched by its own guardian
process, and the worker writes what it is about to do to disk before acting,
so a restarted worker finds its tasks where the previous one left them. Two
consequences:

- To remove a worker, drain it first with `lwwo drain <name>`. If it
  disappears, its tasks stop by themselves after the heartbeat timeout and
  the master assigns them to another worker.
- Under systemd the unit needs `KillMode=process`, otherwise stopping the
  service also stops the tasks:

```ini
[Service]
ExecStart=/usr/local/bin/lwwo worker --config /etc/lwwo/worker.toml
Restart=always
KillMode=process
```

### Tokens and backup

API tokens can be created with an expiry, listed and revoked:

```sh
lwwo token api --label ci --expires 90d
lwwo token list
lwwo token revoke tok_1a2b3c4d5e6f
```

`lwwo backup` writes a consistent copy of the database to the master's
backup directory while the master runs.

### Availability

There is a single master by design, and it locks its data directory: a
second master started on the same directory refuses to run, because both
would assign the same tasks. When the master process dies, running tasks
continue and nothing is lost; a supervisor should restart it (`Restart=always`
in a systemd unit). Surviving the loss of the whole machine requires
replicating the SQLite file elsewhere, for instance with Litestream.

### Configuration

Every setting can be written in the configuration file, given as an
environment variable or passed as a flag, in increasing order of priority:
`[storage] data_dir` in the file is `LWWO_MASTER_STORAGE_DATA_DIR` in the
environment and `--storage-data-dir` on the command line.
[docs/configuration.md](docs/configuration.md) lists every setting with its
default; `examples/master.toml` and `examples/worker.toml` are commented
starting points.

## Project status

lwwo is experimental. It works end to end and is tested at four levels: the
decision core on its own, including simulations of days of activity; the
individual parts of the program; the real program started as real processes,
with failing tasks, vanishing workers and a master killed in the middle of a
run; and master, workers and client in separate containers on a network.

Known limits:

- It has not been used in production, and its security has not been reviewed
  by a third party (see [Security model](#security-model)).
- A single master: while it is down nothing new starts.
- A worker cut off from the network keeps its tasks running while the master
  reassigns them, so a task can run twice until the network returns. Tasks
  must be idempotent.
- Throughput is bounded by a single SQLite writer. Measured on a laptop:
  about 60 empty tasks per second on 16 slots.
- The items not yet finished are listed in the design document under
  "To fix".

## Development

```sh
cargo test --workspace                                   # every test
cargo clippy --workspace --all-targets -- -D warnings    # the lints
cargo fmt --all --check                                  # formatting
```

The code is two crates. `crates/lwwo-core` is the decision core: no
dependencies, no input or output, and lint rules that keep it so.
`crates/lwwo` is the program around it: configuration, YAML, SQLite, master,
worker, command line and TUI.

## Documentation

- [docs/design.md](docs/design.md): the complete design, with every decision
  taken and every alternative rejected.
- [docs/configuration.md](docs/configuration.md): every setting of master,
  worker, client and DAG file.
- [docs/database.md](docs/database.md): the SQLite schema, table by table.

## Licence

MIT. See [LICENSE](LICENSE).
