# lwwo database reference

The SQLite schema of the master, table by table. Why it is shaped this way is in [design.md](design.md#database).

## Schema

Verified on SQLite 3.53. A second run for the same fire, a wrong state, an unknown trigger and a task with a DAG different from its run's are rejected. Deleting a run also removes its tasks and attempts.

```sql
CREATE TABLE dags (
  name        TEXT PRIMARY KEY,
  current     TEXT REFERENCES dag_versions(hash),   -- active version
  paused      INTEGER NOT NULL DEFAULT 0,
  created_at  INTEGER NOT NULL,
  deleted_at  INTEGER
) STRICT;

CREATE TABLE dag_versions (
  hash        TEXT PRIMARY KEY,                     -- hash of the definition
  dag_name    TEXT NOT NULL REFERENCES dags(name),
  yaml        TEXT NOT NULL,                        -- the source
  structure   TEXT NOT NULL,                        -- hash of tasks and dependencies only
  created_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE timetable (
  dag_name    TEXT PRIMARY KEY REFERENCES dags(name),
  next_fire   INTEGER NOT NULL                      -- next fire
) STRICT;
CREATE INDEX timetable_by_time ON timetable(next_fire);

CREATE TABLE dag_runs (
  id            TEXT PRIMARY KEY,                   -- UUIDv7
  dag_name      TEXT NOT NULL REFERENCES dags(name),
  version_hash  TEXT NOT NULL REFERENCES dag_versions(hash),
  logical_time  INTEGER NOT NULL,                   -- instant of the run
  trigger       TEXT NOT NULL CHECK (trigger IN ('cron', 'catchup', 'manual')),
  params        TEXT NOT NULL DEFAULT '{}',         -- JSON, final values
  state         TEXT NOT NULL CHECK (state IN ('queued', 'running', 'success', 'failed', 'cancelled')),
  created_at    INTEGER NOT NULL,
  started_at    INTEGER,
  finished_at   INTEGER,
  problem       TEXT,                               -- why lwwo stopped it, since version 2
  rerun         TEXT,                               -- a rerun waiting for room, since version 3
  UNIQUE (id, dag_name)                             -- target of the key of task_runs
) STRICT;
CREATE UNIQUE INDEX dag_runs_one_per_tick ON dag_runs(dag_name, logical_time) WHERE trigger <> 'manual';
CREATE INDEX dag_runs_by_dag ON dag_runs(dag_name, logical_time DESC);
CREATE INDEX dag_runs_open ON dag_runs(state) WHERE state IN ('queued', 'running');

CREATE TABLE task_runs (
  run_id      TEXT NOT NULL,
  dag_name    TEXT NOT NULL REFERENCES dags(name),  -- direct reference to the DAG
  task        TEXT NOT NULL,                        -- name of the task
  state       TEXT NOT NULL CHECK (state IN ('blocked', 'assigned', 'retrying', 'success', 'failed', 'skipped', 'cancelled')),
  attempts    INTEGER NOT NULL DEFAULT 0,           -- attempts made so far
  PRIMARY KEY (run_id, task),
  FOREIGN KEY (run_id, dag_name) REFERENCES dag_runs(id, dag_name) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;
CREATE INDEX task_runs_by_task ON task_runs(dag_name, task, run_id DESC);

CREATE TABLE task_attempts (
  run_id        TEXT NOT NULL,
  task          TEXT NOT NULL,
  attempt       INTEGER NOT NULL,                   -- 1, 2, ...
  worker        TEXT NOT NULL,
  version_hash  TEXT NOT NULL,                      -- version used by this attempt
  assigned_at   INTEGER NOT NULL,                   -- first write
  started_at    INTEGER,                            -- second write, with the outcome
  finished_at   INTEGER,
  outcome       TEXT CHECK (outcome IN ('success', 'failed', 'timeout', 'lost', 'cancelled')),
  exit_code     INTEGER,
  peak_memory   INTEGER,                            -- bytes, informative only
  log_bytes     INTEGER,
  log_truncated INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, task, attempt),
  FOREIGN KEY (run_id, task) REFERENCES task_runs(run_id, task) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;
CREATE INDEX task_attempts_in_flight ON task_attempts(worker) WHERE finished_at IS NULL;

CREATE TABLE workers (
  name         TEXT PRIMARY KEY,
  cert_serial  TEXT NOT NULL,                       -- certificate issued by the internal CA
  joined_at    INTEGER NOT NULL,
  revoked_at   INTEGER
) STRICT;

CREATE TABLE tokens (
  id           TEXT PRIMARY KEY,
  kind         TEXT NOT NULL CHECK (kind IN ('api', 'join')),
  label        TEXT,
  secret_hash  TEXT NOT NULL,                       -- never the token in the clear
  created_at   INTEGER NOT NULL,
  expires_at   INTEGER,
  revoked_at   INTEGER
) STRICT;

PRAGMA user_version = 3;
```

## Tables and columns

For each table: what it is for and what each column holds. Times are UTC milliseconds; the examples show the corresponding time in the Europe/Rome zone. Optional columns may stay empty.

### `dags`: a workflow

One row per DAG, created at the first `lwwo apply`. A removed DAG stays, with `deleted_at`, as long as it has runs in history.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `name` | text, key | name of the DAG, from the `name` field of the YAML | `sales` |
| `current` | text | hash of the active version, in `dag_versions`; empty only during the first apply | `9f2c41…` |
| `paused` | integer | 1 if the DAG is paused: the cron creates no new runs | `0` |
| `created_at` | integer | first apply | `1792527300000` (Oct 20, 22:15) |
| `deleted_at` | integer, optional | when the DAG was removed; empty while it exists | empty |

### `dag_versions`: a version of a DAG

One row for every different content applied. Applying the same file again adds no rows.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `hash` | text, key | SHA-256 of the definition as understood: it changes if the content changes, not for spaces or comments | `9f2c41…` |
| `dag_name` | text | the DAG it belongs to | `sales` |
| `yaml` | text | the YAML as it was applied: the source of truth of the version | `name: sales …` |
| `structure` | text | hash of tasks and dependencies only: if two versions have the same, a rerun can move to the new version | `51ab07…` |
| `created_at` | integer | when it was applied | `1793051400000` (Oct 26, 22:50) |

### `timetable`: the next fire

One row for every DAG with a `schedule`. The master updates it at every fire, in the same transaction in which it creates the run. It is not loaded into memory: when the time of a fire comes, the master queries it with these two queries, which use the `timetable_by_time` index.

```sql
-- who must start now, with the YAML of the current version
SELECT t.dag_name, v.yaml
FROM timetable t
JOIN dags d ON d.name = t.dag_name
JOIN dag_versions v ON v.hash = d.current
WHERE t.next_fire <= :now;

-- when the next fire is
SELECT min(next_fire) FROM timetable WHERE next_fire > :now;
```

The second query looks only at the future: a row left behind waits for the end of a run, not for the clock.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `dag_name` | text, key | the DAG | `sales` |
| `next_fire` | integer | instant of the next fire; if at start it is already past, there are missed fires to catch up according to `catchup` | `1793066400000` (Oct 27, 03:00) |

### `dag_runs`: a run of the workflow

One row per run, created by the cron, by a catch-up or by `lwwo run`.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `id` | text, key | UUIDv7: unique and already in time order | `0192f3a4-…` |
| `dag_name` | text | the DAG | `sales` |
| `version_hash` | text | the version the run runs with | `9f2c41…` |
| `logical_time` | integer | the instant of the run: the cron fire, or `--at` in manual runs. Tasks see it as `run.time` and `run.date` | `1792980000000` (Oct 26, 03:00) |
| `trigger` | text | `cron`, `catchup` (a run started more than a minute late on its fire, because of an interruption or because it waited for the previous run) or `manual` | `cron` |
| `params` | JSON text | the final values of the parameters, defaults included | `{"country": "IT", "threshold": 100}` |
| `state` | text | `queued` (waiting because of the overlap rule, as a new run or as a rerun), `running`, `success`, `failed` or `cancelled` | `running` |
| `problem` | text, optional | why `lwwo` stopped the run by itself: what it was told did not fit the state of the run. Almost always empty; cleared when the run is rerun | empty |
| `rerun` | JSON text, optional | set only while a finished run waits to be rerun, with state `queued`: the tasks to run again (`task`, `all`, `only`), the `version` to use and the state the run had (`was`). Cleared when the rerun starts or is cancelled | `{"task":null,"all":false,"only":false,"version":"9f2c41…","was":"failed"}` |
| `created_at` | integer | when the run was created | `1792980000000` (Oct 26, 03:00) |
| `started_at` | integer, optional | when the run started, that is when its tasks were created; empty while it is queued | `1792980001000` (Oct 26, 03:00:01) |
| `finished_at` | integer, optional | when the run ended or was cancelled | empty |

Two constraints: the same DAG cannot have two `cron` or `catchup` runs with the same `logical_time`, and the pair `id` and `dag_name` is unique, because the key of `task_runs` uses it.

### `task_runs`: a task inside a run

It is the bridge table between runs and tasks. The rows are all created together when the run starts, one per task; a queued run has none, unless it is a finished run waiting to be rerun, which keeps its rows.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `run_id` | text, key | the run | `0192f3a4-…` |
| `dag_name` | text | the DAG of the run, repeated for direct queries; the composite key keeps it equal to the run's | `sales` |
| `task` | text, key | name of the task in the YAML | `load` |
| `state` | text | current state of the task, explained in the table below | `success` |
| `attempts` | integer | attempts made so far, reruns included | `2` |

| State | Meaning |
| --- | --- |
| `blocked` | waiting for dependencies or for a free slot |
| `assigned` | sent to a worker: running or about to start |
| `retrying` | the last attempt failed, and the next one starts after the wait computed from `retry_delay` and `retry_backoff` |
| `success` | succeeded |
| `failed` | failed, with no attempts left |
| `skipped` | skipped because a predecessor failed |
| `cancelled` | not finished because the run was cancelled |

The history of a task is read without joins:

```sql
SELECT run_id, state, attempts
FROM task_runs
WHERE dag_name = 'sales' AND task = 'load'
ORDER BY run_id DESC          -- UUIDv7s are already in time order
LIMIT 20;
```

### `task_attempts`: an attempt of a task

It is the history: one row for every time a task was sent to a worker. It is written twice, at assignment and at the end.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `run_id`, `task` | text, key | the task of the run | `0192f3a4-…`, `load` |
| `attempt` | integer, key | number of the attempt, from 1, reruns included | `2` |
| `worker` | text | the worker that ran it | `w2` |
| `version_hash` | text | the version used: usually the run's, the current one if the rerun asked for it | `9f2c41…` |
| `assigned_at` | integer | when the master assigned it: the first write | `1792980001000` (03:00:01) |
| `started_at` | integer, optional | when the process really started; it arrives with the outcome, in the second write | `1792980004000` (03:00:04) |
| `finished_at` | integer, optional | end of the attempt; empty while it runs | `1792982482000` (03:41:22) |
| `outcome` | text, optional | `success`, `failed`, `timeout`, `lost` (the worker disappeared) or `cancelled` | `success` |
| `exit_code` | integer, optional | exit code of the process | `0` |
| `peak_memory` | integer, optional | peak memory in bytes, informative only | `1610612736` (1.5 GB) |
| `log_bytes` | integer, optional | size of the attempt's log | `48213` |
| `log_truncated` | integer | 1 if the log went over the limit and the master kept only the beginning and the end | `0` |

The log file has no column: the path is derived from DAG, run, task and attempt, for example `logs/sales/0192f3a4-…/load/2.log`.

### `workers`: the registry of workers

One row for every worker that joined with the token. Slots, labels and sessions stay in memory: here is only what is needed to recognise a worker and to revoke it.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `name` | text, key | name of the worker, unique | `w2` |
| `cert_serial` | text | serial number of the certificate issued by the internal CA | `4f:1a:…` |
| `joined_at` | integer | when it joined with the token | `1792527300000` (Oct 20, 22:15) |
| `revoked_at` | integer, optional | when it was removed: from then on its certificate is refused | empty |

### `tokens`: the access tokens

One row per token. The token in the clear is seen only when it is created: only its hash stays in the database.

| Column | Type | Meaning | Example |
| --- | --- | --- | --- |
| `id` | text, key | id of the token | `tok_7c2e…` |
| `kind` | text | `api` for CLI, TUI and scripts; `join` for workers joining | `api` |
| `label` | text, optional | a name to recognise it by | `production CI` |
| `secret_hash` | text | hash of the secret | `sha256:b41f…` |
| `created_at` | integer | when it was created | `1793051400000` (Oct 26, 22:50) |
| `expires_at` | integer, optional | expiry; empty if it does not expire | empty |
| `revoked_at` | integer, optional | when it was revoked | empty |
