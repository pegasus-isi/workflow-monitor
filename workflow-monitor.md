# Live workflow events from `pegasus-monitord`

This document describes the **live event feed** that lets `workflow-monitor`
obtain Pegasus workflow progress directly from a running `pegasus-monitord`
process — **the instant monitord parses an event from the DAGMan log** — instead
of waiting for it to be committed to, and then polled back out of, the stampede
SQLite database.

It covers: the motivation, the architecture, every file that changed in both
repos, how to enable and use it, the event schema/translation, and exactly how
parity with the existing database path was tested.

---

## 1. Motivation

Today `workflow-monitor` reconstructs progress by **polling the stampede SQLite
database** (`StampedeDB.get_events_since()` in `src/workflow_monitor/db.py`).
Every event therefore travels a long path:

```
DAGMan .dag.dagman.out  →  monitord parse  →  batch INSERT into stampede.db
                        →  workflow-monitor SQL poll  →  workflow-events.jsonl
```

The DB batch-commit and the poll interval add latency, and the consumer depends
on the database being present and unlocked.

**Goal:** extract events the moment monitord parses them — *before* the DB —
while preserving the existing `workflow-events.jsonl` schema so all downstream
consumers (`replay.py`, `remote.py`, `display.py`) keep working unchanged.
HTCondor data continues to be collected by `workflow-monitor`'s own poller and
is appended to the **same** `workflow-events.jsonl`.

**Key enabler:** monitord already dispatches structured "stampede" events through
a pluggable, multiplexable `EventSink` layer
(`Pegasus/monitoring/event_output.py`). We add a new first-class sink there that
translates those events into `workflow-monitor`'s native record schema and writes
them live to a file. Full parity is achievable — every field the consumer needs
(`type_desc`, `state`, `exitcode`, `maxrss`, `stdout/stderr_file`) is present in
the live stream.

---

## 2. Architecture

```
pegasus-monitord  (auto-launched by pegasus-run; parses .dag.dagman.out)
   │  output_to_db(event, kwargs)              [Workflow in workflow.py]
   ▼
MultiplexEventSink                              [event_output.py]
   ├─► DBEventSink ─────────────► <dag>.stampede.db          (UNCHANGED)
   └─► WorkflowMonitorEventSink ─► <submit_dir>/monitord-events.jsonl   (NEW)
          translates stampede kwargs → native records:
          workflow_start · jobs_init · workflow_state · job_state
                              │  file transport (append-only JSONL)
                              ▼
workflow-monitor  --source live   (SINGLE writer of the final file)
   ├─ StampedeStreamReader: tail monitord-events.jsonl (byte-offset resume) ─┐
   ├─ htcondor_poll.py: condor_q / history / status ─► htcondor_poll/… ──────┤
   └─ EventLogger (one fh; dedup, disk-guard, resume) ◄──────────────────────┘
          ▼
   <submit_dir>/workflow-events.jsonl   (pegasus + htcondor, interleaved)
```

Three design decisions shape this:

1. **First-class Pegasus sink** — the translation lives in Pegasus
   (`WorkflowMonitorEventSink`), emitting `workflow-monitor`'s native schema.
2. **File transport** — the sink appends `monitord-events.jsonl`; the consumer
   tails it.
3. **Single writer** — `workflow-monitor` (one process) tails the sink's file
   *and* runs the HTCondor poller, writing the final merged
   `workflow-events.jsonl` through one `EventLogger`. No cross-process locking.

The two-stage file reconciles all three: monitord (process A) writes Pegasus
records in native schema to `monitord-events.jsonl`; `workflow-monitor`
(process B, the single writer of the final artifact) merges those with HTCondor
events into `workflow-events.jsonl`.

Because the new sink owns its own JSON serialization, it also sidesteps
monitord's global per-invocation `--encoding` setting — the DB sink keeps `bp`,
the new sink emits native JSON independently.

---

## 3. What changed — Pegasus repo

Path prefix: `packages/pegasus-python/src/Pegasus/`

### 3a. `monitoring/event_output.py` — new `WorkflowMonitorEventSink`

A new `EventSink` subclass (sibling of `FileEventSink`). It receives every
`(event, kwargs)` via `send()` — the same path the DB sink uses — and emits
`workflow-monitor` native JSONL (one JSON object per line). As a long-lived
object for the monitord run, it holds the small correlation state needed to
replace the joins the stampede DB would otherwise perform:

| State held | Built from | Replaces |
|---|---|---|
| `type_desc` per job | `stampede.job.info` | the `job` table |
| `transformation`/`argv` per job | `stampede.task.info` + `stampede.wf.map.task_job` | `LEFT JOIN task` |
| carried-forward `maxrss` | `stampede.inv.end` | `invocation.maxrss` |
| carried-forward `exitcode`/`stdout`/`stderr` | `job_inst.main.*`/`post.*` | `job_instance` columns |
| synthetic integer `job_id` | a per-name counter | the `job` PK (stream has names only) |

**Key conventions handled:**

- Event keys arrive in two formats — dynamic events (from `workflow.py`) use `__`
  separators (`xwf__id`, `job__id`); static-bp events have `.id`→`__id` remapped
  (`job__id`) with dotted non-id keys (`dax.label`). The sink normalizes both with
  `k.replace(".", "_").replace("__", "_")`.
- **State strings are copied verbatim** from the DB loader
  (`db/workflow_loader.py`'s `jobstate` `states` dict), applied as
  `states[event][int(status)+1]` (callers only ever pass status `-1`, `0`, or
  none → success/normal variant). This guarantees the emitted `state` values are
  identical to the stampede `jobstate` column the consumer reads today — including
  exact spellings like `POST_SCRIPT_FAILED` (not `…FAILURE`).

The sink emits **only** the Pegasus-derived records (`workflow_start`,
`jobs_init`, `workflow_state`, `job_state`). The HTCondor records and the
end-of-run aggregates (`workflow_stats`/`workflow_end`) are added by
`workflow-monitor`, the single writer of the final file.

`restart=True` (monitord `--replay`/recovery) opens the file in truncate mode,
matching how monitord rotates the stampede DB on replay.

### 3b. `monitoring/event_output.py` — new `wfmonitor://` URL scheme

`create_wf_event_sink()` gains a branch:

```python
elif url.scheme == "wfmonitor":
    # wfmonitor:///path/to/monitord-events.jsonl  (owns its own JSON encoding)
    sink = WorkflowMonitorEventSink(url.path, **kw)
```

### 3c. `cli/pegasus-monitord.py` — `pegasus.monitord.wfmonitor.url`

Just before the sink is constructed, monitord reads the new property and injects
it as a **named workflow sink** so the existing multiplex machinery builds it
next to the default (DB) sink:

```python
wfmonitor_prop = props.property("pegasus.monitord.wfmonitor.url")
if wfmonitor_prop is not None and not no_events:
    # "true"/"yes"/"on"/"1"/"" -> default <run>/monitord-events.jsonl
    # a bare path        -> wfmonitor://<abspath>
    # a full wfmonitor:// URL -> used as-is
    props.property("pegasus.catalog.workflow.wfmonitor.url", wfmonitor_url)
```

The presence of a second `pegasus.catalog.workflow.*.url` triggers
`MultiplexEventSink`, which builds both the DB sink (as `default`) and the
`wfmonitor` sink. **DB population is unaffected.**

### 3d. `test/test_wfmonitor_event_sink.py` — new regression tests

Nine tests drive the sink exactly as `Workflow.output_to_db` does and assert the
emitted records (header, `jobs_init` enrichment, workflow-state transitions, every
job-state string, enrichment carry-forward, restart truncation). Runs under the
normal `ant test-python` / `tox -e py310`.

---

## 4. What changed — workflow-monitor repo

Path prefix: `src/workflow_monitor/`

### 4a. `stampede_stream.py` (new) — `StampedeStreamReader`

A **drop-in `StampedeDB` facade**. It tails `monitord-events.jsonl` incrementally
(byte-offset resume, partial-line buffering, truncation detection) and exposes
exactly the subset of the `StampedeDB` interface the loops and `EventLogger` rely
on:

- `snapshot()` → `WorkflowSnapshot` (rebuilds the per-job roster the DB join produces)
- `get_events_since(after_ts)` → DB-shaped `job_state` rows (same `> after_ts` semantics)
- `get_workflow_times()` → `{start, end}`
- `connect()` / `close()` / context-manager protocol

Because of this, **`event_log.py`, `server.py`, and `display.py` needed no
changes** — they are already polymorphic over `db`. The same `EventLogger`
writes the merged `workflow-events.jsonl`, so its dedup, disk-exhaustion guard,
and resume logic are inherited unchanged.

> Note on `end_time`: the reader marks a job's `end_time` using **only**
> `{JOB_TERMINATED, JOB_SUCCESS, JOB_FAILURE}` — matching `db.get_jobs()` exactly
> (POST_SCRIPT states deliberately do not count), so durations are identical.

### 4b. `cli.py` — `--source {db,live,auto}`

```
--source live   tail monitord-events.jsonl (events as monitord parses them)
--source db     poll the stampede SQLite database (previous behavior)
--source auto   (default) use the live stream if monitord-events.jsonl exists,
                else fall back to the database
```

A small `make_source()` helper opens a `StampedeStreamReader` or a `StampedeDB`
and is used by every mode (TUI, `--serve`, `--why-idle`). The live path needs no
database; the DB path needs no live stream.

### 4c. `braindump.py` — discovery accessors

```python
WorkflowInfo.monitord_events_path  # <submit_dir>/monitord-events.jsonl (always)
WorkflowInfo.monitord_events       # the path if it exists, else None (for auto)
```

---

## 5. Usage

### 5a. Enable the live sink in monitord

`pegasus-monitord` is normally started automatically by `pegasus-run`. To make it
emit the extra JSONL alongside the DB, add to the workflow's
`pegasus.properties` (picked up from the braindump):

```properties
# Easiest: a bare boolean writes <submit_dir>/monitord-events.jsonl
pegasus.monitord.wfmonitor.url = true
```

Other accepted forms:

```properties
# explicit path
pegasus.monitord.wfmonitor.url = /scratch/run0001/monitord-events.jsonl
# explicit URL
pegasus.monitord.wfmonitor.url = wfmonitor:///scratch/run0001/monitord-events.jsonl
```

The stampede DB is still written exactly as before — this only **adds** a sink.

### 5b. Run workflow-monitor against the live stream

```bash
# Auto (default): live if monitord-events.jsonl exists, else DB
workflow-monitor /path/to/submit_dir

# Force the live stream
workflow-monitor --source live /path/to/submit_dir

# Headless writer producing the merged workflow-events.jsonl (the common case)
workflow-monitor --serve-foreground --source live /path/to/submit_dir

# Force the legacy DB path
workflow-monitor --source db /path/to/submit_dir
```

The merged `workflow-events.jsonl` written by `--serve`/`--log` is the same
artifact `--replay` and `--remote` already consume — no consumer changes needed.

### 5c. Regenerate the stream from a finished run (offline)

```bash
pegasus-monitord --replay \
  -d 'wfmonitor:///abs/run/monitord-events.jsonl' \
  /abs/run/<dag>.dag.dagman.out
```

---

## 6. Event schema & translation

`monitord-events.jsonl` lines are the **same record schema** the DB path already
produces (so the consumer is unchanged):

| Record | Emitted when | Key fields |
|---|---|---|
| `workflow_start` | `stampede.wf.plan` | `dax_label`, `user`, `planner_version`, `submit_dir`, `wf_uuid` |
| `jobs_init` | first `static.end`/`xwf.start`/job_state | `total_jobs`, `jobs[]` (`job_id`, `exec_job_id`, `type_desc`, `transformation`, `task_argv`) |
| `workflow_state` | `stampede.xwf.start` / `.end` | `state` (`WORKFLOW_STARTED`/`WORKFLOW_TERMINATED`), `status`, `wf_start`/`wf_end` |
| `job_state` | any `stampede.job_inst.*` transition | `exec_job_id`, `type_desc`, `state`, `job_id`, optional `exitcode`/`stdout_file`/`stderr_file`/`maxrss` |

The `state` string mapping (verbatim from `db/workflow_loader.py`,
`states[event][int(status)+1]`):

| Stampede event | status 0 (success) | status -1 (fail) |
|---|---|---|
| `job_inst.submit.end` | `SUBMIT` | `SUBMIT_FAILED` |
| `job_inst.main.start` | `EXECUTE` | `EXECUTE` |
| `job_inst.main.term` | `JOB_TERMINATED` | `JOB_EVICTED` |
| `job_inst.main.end` | `JOB_SUCCESS` | `JOB_FAILURE` |
| `job_inst.pre.end` | `PRE_SCRIPT_SUCCESS` | `PRE_SCRIPT_FAILED` |
| `job_inst.post.end` | `POST_SCRIPT_SUCCESS` | `POST_SCRIPT_FAILED` |
| `job_inst.held.start` / `.end` | `JOB_HELD` / `JOB_RELEASED` | — |
| `job_inst.{grid,globus}.submit.end` | `GRID_SUBMIT`/`GLOBUS_SUBMIT` | `…_SUBMIT_FAILED` |

(`pre.start`, `post.start`, `image.info`, `abort.info` are status-less and map to
`PRE_SCRIPT_STARTED`, `POST_SCRIPT_STARTED`, `IMAGE_SIZE`, `JOB_ABORTED`.)

---

## 7. Parity — what's identical and how it was tested

### 7a. The one semantic difference

Both paths are faithful, but they differ in *enrichment timing* on **intermediate**
rows:

- **DB path** — `get_events_since` joins each `jobstate` row to the job_instance
  and invocation, so a job's **final** `exitcode`/`maxrss`/`stdout_file` are
  stamped onto **every** transition row (a denormalized snapshot view).
- **Live path** — carries enrichment forward as monitord learns it, so a row
  shows what was actually known when that transition occurred (a true event
  stream). The terminal rows (`JOB_SUCCESS`/`POST_SCRIPT_SUCCESS`) carry the full
  enrichment.

This does not affect consumers: `workflow-monitor` uses the **latest row per job**
for final disposition, which is identical in both paths.

### 7b. Test layers

| Layer | What it proves |
|---|---|
| **Sink unit tests** (`test/test_wfmonitor_event_sink.py`, 9 tests) | Sink emits the correct native records, state strings, enrichment, restart-truncation. |
| **Reader round-trip** | Sink output → `StampedeStreamReader` reconstructs snapshot, events, timing, maxrss, completion (incl. incremental tail). |
| **Consumer end-to-end** | Reader + `EventLogger` produce a merged `workflow-events.jsonl` with Pegasus + HTCondor events interleaved by a single writer; `workflow_stats`/`workflow_end` computed from the reconstructed snapshot. |
| **Real `pegasus-monitord --replay`** | The actual dev monitord runs the `wfmonitor` sink and produces `monitord-events.jsonl`. |
| **DB-vs-live parity** | Compares the live stream against the DB path for the same run. |
| **CLI smoke** | `--source db` and `--source live` render identical statistics. |

### 7c. Reproducing the real parity test

Generate the stream with the dev monitord against a finished run that has both a
`*.dag.dagman.out` and a `*.static.bp` (e.g.
`packages/pegasus-common/test/client/analyzer_samples_dir/process_wf_success`):

```bash
# 1. (one-time) dev venv with modern deps for the dev tree
python3 -m venv /tmp/pegdev
/tmp/pegdev/bin/pip install "sqlalchemy>=1.4,<2" pyyaml "flask>1.1,<2.3" \
    "werkzeug<2.3" "markupsafe<2.1"

# 2. copy a finished run; clear stale monitord state (gdbm-format shelve)
cp -r .../analyzer_samples_dir/process_wf_success /tmp/replaytest
rm -f /tmp/replaytest/monitord.subwf* /tmp/replaytest/monitord.{done,info,started}

# 3. run the DEV monitord with the wfmonitor sink
REPO=/path/to/pegasus
SRC="$REPO/packages/pegasus-python/src:$REPO/packages/pegasus-common/src:\
$REPO/packages/pegasus-api/src:$REPO/packages/pegasus-worker/src"
PYTHONPATH="$SRC" PEGASUS_HOME=/opt/homebrew /tmp/pegdev/bin/python \
  "$REPO/packages/pegasus-python/src/Pegasus/cli/pegasus-monitord.py" --replay \
  -d 'wfmonitor:////tmp/replaytest/monitord-events.jsonl' \
  /tmp/replaytest/process-0.dag.dagman.out

# 4. compare statistics produced by both sources
workflow-monitor --once --source db   /tmp/replaytest   # baseline (stampede.db)
workflow-monitor --once --source live /tmp/replaytest   # live (monitord-events.jsonl)
```

### 7d. Observed results (process_wf_success, 5 jobs)

- **State-transition skeleton identical:** 35/35 transitions, same per-state
  counts, **0** differences in `(exec_job_id, type_desc, state)`.
- **Final per-job disposition byte-identical** across all 5 jobs and every job
  type (`compute`, `create-dir`, `cleanup`, `registration`, `stage-out-tx`):
  same `state`, `exitcode`, `maxrss`, and resolved `stdout_file` (`…out.000`).
- **Identical computed statistics:** jobs (1 compute / 4 infra), wall time
  (1m17s), compute time (5s), duration distribution, peak memory (2.9M, `ls`),
  pool (1 machine, 12 CPUs).
- `--serve-foreground --source live` produced a 41-record merged
  `workflow-events.jsonl` (`workflow_start` first, `workflow_end` last, 35
  `job_state`, plus a real `condor_status` `pool_status` interleaved by the single
  writer).

**Conclusion:** parity with `workflow-events.jsonl` is achieved. The only
divergence is the intermediate-row enrichment timing described in §7a, which is
invisible to consumers.

---

## 8. Operational notes

- **Opt-in & safe:** the feature does nothing unless
  `pegasus.monitord.wfmonitor.url` is set; the stampede DB is always written as
  before.
- **Restart/replay:** on monitord `--replay`/recovery the sink truncates
  `monitord-events.jsonl`; the reader detects the shrink and resets its state.
- **Consumer resume:** stopping/restarting `workflow-monitor` resumes from the
  reader's byte offset and the `EventLogger`'s existing log resume — no duplicate
  header, no re-emitted events.
- **Disk safety:** the merged `workflow-events.jsonl` inherits the existing
  disk-exhaustion guard (`--min-free-mb` / `--max-log-mb`, `log_paused`/
  `log_resumed` markers).
- **`--why-idle`, `--source live`** uses the reconstructed snapshot; it still runs
  its own HTCondor queries.

---

## 9. File reference

| Repo | File | Change |
|---|---|---|
| pegasus | `packages/pegasus-python/src/Pegasus/monitoring/event_output.py` | `WorkflowMonitorEventSink` + `wfmonitor://` scheme |
| pegasus | `packages/pegasus-python/src/Pegasus/cli/pegasus-monitord.py` | read/wire `pegasus.monitord.wfmonitor.url` |
| pegasus | `packages/pegasus-python/test/test_wfmonitor_event_sink.py` | sink regression tests (new) |
| workflow-monitor | `src/workflow_monitor/stampede_stream.py` | `StampedeStreamReader` facade (new) |
| workflow-monitor | `src/workflow_monitor/cli.py` | `--source {db,live,auto}` + source selection |
| workflow-monitor | `src/workflow_monitor/braindump.py` | `monitord_events` / `monitord_events_path` accessors |

Unchanged but central (facade keeps them untouched):
`src/workflow_monitor/event_log.py`, `server.py`, `display.py`.
