# lwwo configuration reference

Every setting of the master, of the worker, of the client and of a DAG file,
with its default and what it does.

- [How settings are given](#how-settings-are-given)
- [Master](#master)
- [Worker](#worker)
- [Standalone](#standalone)
- [Client: CLI and TUI](#client-cli-and-tui)
- [DAG file](#dag-file)
- [Where a value comes from](#where-a-value-comes-from)

## How settings are given

The master and the worker read a TOML file, then environment variables, then
flags. The last one wins. Every setting has one name in three spellings:

| Where | Master, `[storage] db` | Worker, `slots` |
| --- | --- | --- |
| File (`--config file.toml`) | `[storage]`<br>`db = "/data/lwwo.db"` | `slots = 8` |
| Environment | `LWWO_MASTER_STORAGE_DB=/data/lwwo.db` | `LWWO_WORKER_SLOTS=8` |
| Flag | `--storage-db=/data/lwwo.db` | `--slots=8` |

- A flag takes its value as `--name=value` or `--name value`.
- Lists are comma-separated in the environment and in flags: `--labels=big,gpu`.
- An unknown setting is an error, in all three places: a typo never passes
  silently.

**Units**

| Kind | Written as | Examples |
| --- | --- | --- |
| Duration | a whole number and a unit: `ms`, `s`, `m`, `h`, `d` | `500ms`, `30s`, `15m`, `1h`, `30d` |
| Size | a whole number and `B`, `KB`, `MB`, `GB`, `TB` (powers of 1024), or `%` of the total | `100MB`, `20GB`, `10%` |

## Master

`lwwo master [--config master.toml] [--section-field=value]…`

### `[api]`: the HTTP API, for CLI and TUI

| Setting | Default | Meaning |
| --- | --- | --- |
| `listen` | `127.0.0.1:7070` | Address and port of the API. |
| `auth` | `token` | `token`: every request needs an API token, and the API is served over TLS. `none`: nobody is checked; accepted only when `listen` is a loopback address. |
| `tls_cert` | none | A custom certificate for the API, PEM file. Needs `tls_key`. Without the pair, the API uses a certificate signed by lwwo's internal CA. |
| `tls_key` | none | The private key of `tls_cert`, PEM file. |

Note: the lwwo CLI and TUI only verify the internal CA, so with `tls_cert`
they cannot connect yet; the setting is for other HTTP clients.

### `[workers]`: the gRPC port workers connect to

| Setting | Default | Meaning |
| --- | --- | --- |
| `listen` | `0.0.0.0:7071` | Address and port for the workers. Always mutual TLS. |
| `heartbeat_interval` | `5s` | How often workers say they are alive. Workers learn it from the master when they connect. |
| `heartbeat_timeout` | `30s` | A worker silent for this long is given up: its tasks are assigned again. Must be longer than `heartbeat_interval`. Also the time after which a task stops by itself when its worker process is gone. |
| `cert_lifetime` | `90d` | How long a worker's certificate lasts. Workers renew by themselves at about two thirds of it. At least `10s`. |

### `[storage]`: the database

| Setting | Default | Meaning |
| --- | --- | --- |
| `data_dir` | `/var/lib/lwwo` | Everything the master keeps: database, CA, tokens, logs, backups. Created with mode `0700`. One master per directory: a second one refuses to start. |
| `db` | `<data_dir>/lwwo.db` | The SQLite file. |
| `durability` | `normal` | `normal`: a power cut can lose the last few transactions (a crash of lwwo alone loses nothing). `full`: every transaction reaches the disk before the master goes on; slower. |
| `retention` | `30d` | Runs older than this are deleted, with their logs. |
| `backup_dir` | `<data_dir>/backups` | The only place `lwwo backup` writes to. |

### `[logs]`: task output

| Setting | Default | Meaning |
| --- | --- | --- |
| `dir` | `<data_dir>/logs` | Where task logs are kept, one plain-text file per attempt. |
| `max_per_attempt` | `100MB` | Above this the master keeps the beginning and the end of an attempt's log and marks the cut. |

### `[dags]`

| Setting | Default | Meaning |
| --- | --- | --- |
| `default_timezone` | `UTC` | Time zone of the schedules of DAGs that name none. An IANA name, like `Europe/Rome`. |
| `watch` | none | A directory: every `.yaml` or `.yml` file in it is applied when it appears or changes (checked every 2 seconds). For development; in production use `lwwo apply`. |

### `[defaults]`: what DAGs and tasks get when they do not say

| Setting | Default | Meaning |
| --- | --- | --- |
| `catchup` | `false` | After the master was down: `false` starts only the last missed fire, `true` starts all of them, in order. |
| `overlap` | `queue` | What happens when a run should start while another of the same DAG is active: `queue` (wait), `skip` (drop the new one), `allow` (run together, up to the DAG's `max_active`), `replace` (cancel the active one). |
| `retries` | `0` | How many times a failed task is tried again. |
| `retry_delay` | `30s` | Wait before the first retry. |
| `retry_backoff` | `exponential` | `exponential`: the wait doubles at every retry. `fixed`: always `retry_delay`. |
| `max_retry_delay` | none | Upper limit for the wait between retries. |
| `timeout` | none | A task running longer is stopped and counts as failed (`timeout`). Without it tasks have no time limit. |

### `[alerts]`: off unless `webhook` is set

| Setting | Default | Meaning |
| --- | --- | --- |
| `webhook` | none | URL that receives each alert as a JSON `POST`. The `text` field is a readable sentence (what Slack shows); `kind` is `tasks_waiting` or `run_failed`. |
| `waiting_after` | none | A task ready to run that has found no worker for this long is reported, once, with the reason (no worker has the label, too few slots, …). |
| `on_failure` | `false` | Report every run that ends as failed. |

Setting `waiting_after` or `on_failure` without `webhook` is an error.

### Example

```toml
[api]
listen = "0.0.0.0:7070"

[workers]
listen = "0.0.0.0:7071"

[storage]
data_dir = "/var/lib/lwwo"
retention = "30d"

[dags]
default_timezone = "Europe/Rome"

[defaults]
retries = 2
timeout = "2h"

[alerts]
webhook = "https://hooks.slack.com/services/..."
waiting_after = "15m"
on_failure = true
```

## Worker

`lwwo worker [--config worker.toml] [--field=value]…`

The worker file has no sections.

| Setting | Default | Meaning |
| --- | --- | --- |
| `master` | required | Address of the master's worker port, `host:7071`. |
| `token_file` | none | A file with the join token (`lwwo token worker`). Needed only the first time: after joining, the worker has its certificate in `data_dir`. |
| `token` | none | The join token itself. In the file or as `LWWO_WORKER_TOKEN`; **not accepted as a flag**, where `ps` would show it. |
| `name` | the host name | The name of the worker, unique in the cluster: letters, digits, `_`, `-` and `.`, at most 64 characters. After joining, the name in the certificate counts. |
| `data_dir` | `/var/lib/lwwo-worker` | Certificates, the journal of the attempts in progress, working directories. Created with mode `0700`. One worker per directory. |
| `slots` | number of CPUs | How many slots the worker offers. A task takes `slots` of them (1 unless the DAG says more). |
| `labels` | `[]` | Labels of this worker. A task with `labels` runs only on a worker that has them all. |
| `workdir` | `<data_dir>/work` | Where each attempt gets its own working directory, removed when the attempt ends. |
| `min_free_disk` | `10%` | With less free disk the worker takes no new tasks (running ones go on). |
| `min_free_memory` | `10%` | The same for memory. |
| `kill_grace` | `30s` | To stop a task the worker sends SIGTERM to its whole process group, and SIGKILL after this long. |
| `log_spool_max` | `1GB` | Limit of the output kept per attempt on the worker until the master has it. Above it the rest of the output is dropped, with a line that says so. |
| `cgroups` | `auto` | Linux only. Each task in a cgroup of its own, so its whole process tree can be stopped and its peak memory measured. `auto`: only when the worker's cgroup holds lwwo alone (a systemd service, a container). `on`: always; whatever shares the worker's cgroup is moved. `off`: never. |

### Example

```toml
master = "master.internal:7071"
token_file = "/etc/lwwo/join.token"
slots = 8
labels = ["big"]
```

## Standalone

`lwwo standalone [--config file.toml] [flags]` runs a master and a worker in
one process, to try lwwo on one machine.

- It reads the **master** settings, with two different defaults:
  `[storage] data_dir = "./lwwo-data"` and `[api] auth = "none"`.
- The worker's settings go in a `[worker]` section of the same file, as
  `LWWO_WORKER_*` variables, or as flags with the `worker-` prefix:
  `--worker-slots=4`.
- The worker's `data_dir` defaults to `<data_dir>/worker`.
- Master and worker talk on a local port without certificates;
  `[workers] listen` is ignored.

## Client: CLI and TUI

Every other command (`lwwo apply`, `run`, `tui`, …) talks to the master's
API and needs only two values.

| Setting | Environment | File `~/.config/lwwo/config.toml` | Default |
| --- | --- | --- | --- |
| Address of the API | `LWWO_API` | `api = "master.internal:7070"` | `127.0.0.1:7070` |
| API token | `LWWO_TOKEN` | `token = "lwwo1_…"` | none: plain HTTP, for a local standalone |

The environment wins over the file. The token carries the fingerprint of the
master's CA, so the client checks it is talking to the right master before
sending anything; there is no certificate to install.

`LWWO_LOG` sets the log level of any lwwo process (`error`, `warn`, `info`,
`debug`).

## DAG file

A DAG is one YAML file, given to the master with `lwwo apply file.yaml`.
Unknown fields are rejected.

```yaml
name: etl_sales
schedule: "0 3 * * *"
timezone: Europe/Rome
params:
  country: it
defaults:
  retries: 2
  timeout: 1h
tasks:
  extract:
    run: [python, extract.py, "--day", "{{ run.date }}", "--country", "{{ params.country }}"]
  load:
    needs: [extract]
    run: [python, load.py]
    labels: [big]
    slots: 2
```

### DAG fields

| Field | Default | Meaning |
| --- | --- | --- |
| `name` | required | Name of the DAG: letters, digits, `_` and `-`. |
| `tasks` | required | The tasks, by name (same characters as above). At least one. |
| `schedule` | none | A cron expression (see below). Without it the DAG runs only by hand. |
| `timezone` | master's `default_timezone` | IANA time zone the schedule is read in. |
| `catchup` | master's `[defaults]` | See `[defaults] catchup`. |
| `overlap` | master's `[defaults]` | `queue`, `skip`, `allow` or `replace`. Applies to manual runs too. |
| `max_active` | `10` | With `overlap: allow`, how many runs may be active together. |
| `params` | `{}` | Parameters with their defaults: text, number or `true`/`false`. Overridden per run with `lwwo run <dag> --param name=value`. |
| `defaults` | `{}` | Any task field except `run` and `needs`, for every task that does not set it. |

**Schedule.** Five fields: minute, hour, day of month, month, day of week.
Each is a comma-separated list of `*`, `n` or `a-b`, optionally with a step
`/s`. In the day of week both 0 and 7 are Sunday. When both day fields are
restricted, a day matches if either does.

| Expression | Fires |
| --- | --- |
| `0 3 * * *` | every day at 03:00 |
| `*/15 * * * *` | every 15 minutes |
| `30 8 * * 1-5` | Monday to Friday at 08:30 |
| `0 0 1 * *` | the first of every month at midnight |

On a daylight-saving change, a time that does not exist runs at the first
valid instant after it; a time that happens twice runs only the first time.

### Task fields

| Field | Default | Meaning |
| --- | --- | --- |
| `run` | required | The command, as a list: program and arguments, run as a process on the worker's host. No shell is involved; for one, write `[sh, -c, "…"]`. Exit code 0 is success. Containers and code checkouts are part of the command when a task needs them (`[docker, run, …]`). |
| `needs` | `[]` | Tasks that must succeed first. If one fails, this task is skipped. |
| `env` | `{}` | Environment variables for the task. Values may use templates. Names starting with `LWWO_` are reserved. The DAG's `defaults.env` and the task's are merged. |
| `labels` | `[]` | The task runs only on a worker that has all these labels. |
| `slots` | `1` | How many of the worker's slots the task takes. |
| `retries` | master's `[defaults]` | See `[defaults]`. |
| `retry_delay` | master's `[defaults]` | |
| `retry_backoff` | master's `[defaults]` | `exponential` or `fixed`. |
| `max_retry_delay` | master's `[defaults]` | |
| `timeout` | master's `[defaults]` | |

### Templates and variables

`run` arguments and `env` values may contain `{{ … }}` (Jinja syntax). A
misspelled name is rejected by `lwwo apply`. The same values reach the task
as environment variables.

| Template | Variable | Value |
| --- | --- | --- |
| `{{ dag.name }}` | `LWWO_DAG` | name of the DAG |
| `{{ run.id }}` | `LWWO_RUN_ID` | id of the run |
| `{{ run.time }}` | `LWWO_RUN_TIME` | the instant the run is for, RFC 3339 in the DAG's time zone |
| `{{ run.date }}` | `LWWO_RUN_DATE` | its day, `YYYY-MM-DD`, in the DAG's time zone |
| `{{ task.name }}` | `LWWO_TASK` | name of the task |
| `{{ task.attempt }}` | `LWWO_ATTEMPT` | 1 for the first attempt |
| `{{ params.x }}` | `LWWO_PARAM_X` | parameter `x` (the variable name is upper case, `-` becomes `_`) |
| | `LWWO_WORKDIR` | the private working directory of the attempt, removed when the attempt ends |

lwwo does not pass data between tasks: a task returns only its exit code.
Tasks exchange data through a location they derive from these values, such
as `/data/$LWWO_RUN_ID/raw.json`, reachable from every worker that can run
them.

What a task sees of the environment: the worker's environment, minus every
`LWWO_*` variable of the worker itself (its configuration and token), plus
`env` and the variables above. A task that starts a container passes what
the container needs on its own command line.

## Where a value comes from

For a setting that exists at several levels, the nearest one wins:

```text
task field  →  DAG `defaults`  →  master `[defaults]`  →  built-in default
```

This holds for `retries`, `retry_delay`, `retry_backoff`, `max_retry_delay`
and `timeout`. `catchup` and `overlap` go from the DAG straight to the
master's `[defaults]`. The other task fields (`env`, `labels`, `slots`) stop
at the DAG's `defaults`.

A run keeps the DAG version it started with: applying a new version changes
the next runs, not the ones in progress.
