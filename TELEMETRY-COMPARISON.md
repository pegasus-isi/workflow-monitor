# `pegasus-monitord` (→ Elasticsearch) vs. `workflow-monitor` (→ JSONL)

A field-level comparison of the ways workflow telemetry leaves a Pegasus run:

1. **`pegasus-monitord`** — Pegasus's own monitoring daemon, which can stream *NetLogger
   "stampede" events* to an AMQP broker (RabbitMQ) for ingestion into Elasticsearch.
2. **`workflow-monitor`** — this project, which polls the artifacts monitord produces and
   writes its own `workflow-events.jsonl` (optionally shipped to Elasticsearch by a
   downstream Vector pipeline).
3. **the `wfmonitor` monitord plugin** *(added 2026-06)* — this project again, but running
   **inside** monitord via the `pegasus.monitord.plugins` entry-point system: the same
   workflow-monitor JSONL schema, produced event-driven at the source (and, optionally,
   the HTCondor polling too) — see [§12](#12-the-third-path-the-wfmonitor-monitord-plugin)
   and the usage guide in [`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md).

All can land in indices with similar names, but the **document shapes of paths 1 and 2 are
not interchangeable** (path 3 deliberately reuses path 2's shapes). This document
enumerates every difference and weighs the trade-offs.

> **Provenance.** The monitord details below are extracted from the version-matched local
> install, **Pegasus 5.1.2-dev.0**:
> - Event emission: `Pegasus/monitoring/workflow.py`
> - Encoders + sinks (BP / BSON / JSON / AMQP): `Pegasus/monitoring/event_output.py`
> - BP key formatting: `Pegasus/netlogger/nlapi.py`
> - DB column mapping (the consumer): `Pegasus/db/workflow_loader.py`
>
> The `workflow-monitor` details are from `src/workflow_monitor/event_log.py` and
> `DATA_SOURCES.md` §9 in this repo.

---

## Contents

| § | Section | What it covers |
|---|---|---|
| 0 | [TL;DR](#0-tldr) | At-a-glance comparison table and the one-sentence summary |
| 1 | [Architectural positioning](#1-architectural-positioning) | Producer vs. consumer; the pipeline diagram |
| 2 | [monitord's output formats and sinks](#2-monitords-output-formats-and-sinks) | BP/BSON/JSON encodings, the AMQP sink, enabling the ES path |
| 3 | [The key-naming transformation](#3-the-key-naming-transformation-the-single-biggest-formatting-difference) | How the same datum becomes `xwf.id` / `xwf_id` / `wf_uuid` |
| 4 | [Complete monitord stampede event catalog](#4-complete-monitord-stampede-event-catalog-with-fields) | All 43 events with their fields |
| 5 | [The round-trip](#5-the-round-trip-same-transition-two-representations) | One transition shown in both representations |
| 6 | [workflow-monitor JSONL event catalog](#6-workflow-monitor-jsonl-event-catalog-summary) | The 10 coarse `event_type`s |
| 7 | [Enumerated differences](#7-enumerated-differences) | 17 concrete, itemized differences |
| 8 | [Strengths & weaknesses](#8-strengths--weaknesses) | Trade-offs of each approach |
| 9 | [Safeguards: how workflow-monitor avoids perturbing the running workflow](#9-safeguards-how-workflow-monitor-avoids-perturbing-the-running-workflow) | Why it is safe to attach to a live production run |
| 9.1 | &nbsp;&nbsp;[Read-only access to the stampede database](#91-read-only-access-to-the-stampede-database) | `mode=ro`, SELECT-only, no write locks |
| 9.2 | &nbsp;&nbsp;[Read-only access to the HTCondor scheduler](#92-read-only-access-to-the-htcondor-scheduler) | Query-only commands and bindings; no mutation |
| 9.3 | &nbsp;&nbsp;[Bounded, polite polling](#93-bounded-polite-polling) | Timeouts, tiered cadence, fingerprint dedup |
| 9.4 | &nbsp;&nbsp;[Failure isolation: degrade, never disrupt](#94-failure-isolation-degrade-never-disrupt) | Swallow errors, return empty, no retry storm |
| 9.5 | &nbsp;&nbsp;[Writes stay in the monitor's own lane](#95-writes-stay-in-the-monitors-own-lane) | Only sidecar files; never touches workflow artifacts |
| 9.6 | &nbsp;&nbsp;[Diagnostics are advisory, never actuating](#96-diagnostics-are-advisory-never-actuating) | Suggestions are text, never executed |
| 9.7 | &nbsp;&nbsp;[Non-invasive daemon and remote modes](#97-non-invasive-daemon-and-remote-modes) | Daemon signals itself only; SSH is read-only |
| 9.8 | &nbsp;&nbsp;[Safeguard summary](#98-safeguard-summary) | One table: surface → mechanism → guarantee |
| 10 | [Practical Elasticsearch implications](#10-practical-elasticsearch-implications) | Field-name and index-collision gotchas |
| 11 | [When to use which](#11-when-to-use-which) | Decision guidance; how they compose |
| 12 | [The third path: the wfmonitor monitord plugin](#12-the-third-path-the-wfmonitor-monitord-plugin) | Push instead of poll — same JSONL schema, emitted from inside monitord; measured equivalence |
| — | [Sources](#sources) | Pegasus source files, repo files, and docs |

---

## 0. TL;DR

| | **monitord → Elasticsearch** | **workflow-monitor → JSONL** |
|---|---|---|
| Role in pipeline | **Producer** (parses condor/DAGMan logs) | **Consumer** of monitord's output (`stampede.db`) + live HTCondor |
| Event model | ~43 fine-grained NetLogger `stampede.*` events | 10 coarse `event_type`s (+ a diagnostics sidecar) |
| One job's lifecycle | many events (`submit.start`, `main.start`, `main.term`, `main.end`, `post.*`, `inv.end`, `host.info`, `composite`…) | folded into one `job_state` per transition, plus embedded ClassAds |
| Field naming | NetLogger dotted keys, **flattened to `_` for JSON/ES** (`xwf.id` → `xwf_id`) | hand-chosen flat snake_case (`wf_uuid`, `exec_job_id`) |
| Correlation key(s) | `xwf_id` + `root_xwf_id` + `parent_xwf_id` (root/sub-workflow aware) | single `wf_uuid` |
| Transport | AMQP topic exchange → Logstash → ES | local append-only file → (optional) Vector → ES |
| Emission trigger | event-driven, as logs are parsed | poll-driven, **fingerprint-deduped** |
| Default scope to AMQP | **only 4 event types** unless widened | all events it knows about |
| Derived analytics | none (faithful recorder) | `workflow_stats`, `pool_status`, stall/hold/idle diagnostics |
| Raw HTCondor ClassAds | no | yes (embedded verbatim) |
| Impact on the running workflow | in-process producer (part of the run) | **observe-only, read-only, bounded** — see [§9](#9-safeguards-how-workflow-monitor-avoids-perturbing-the-running-workflow) |

**One sentence:** monitord emits many small, precise, NetLogger-schema events (dotted keys
that become underscores in ES) straight from the condor logs; workflow-monitor re-reads the
DB monitord wrote *plus* live HTCondor and emits fewer, coarser, hand-named snake_case events
that collapse several lifecycle phases into one, embed raw ClassAds, dedup by fingerprint, and
add derived stats and interpretive diagnostics that monitord never produces.

---

## 1. Architectural positioning

```
                 ┌────────────────────────────────────────────────────────────┐
                 │  condor logs, *.dagman.out, *.out.NNN kickstart records    │
                 └───────────────┬────────────────────────────────────────────┘
                                 │ parses (event-driven)
                                 ▼
                         pegasus-monitord
                                 │ emits stampede.* NetLogger events
        ┌────────────────────────┼────────────────────────────────────┐
        │ BP/BSON encode         │ JSON encode                        │
        ▼                        ▼                                    ▼
   <dag>.stampede.db        AMQP exchange (RabbitMQ) ──▶ Logstash ──▶ Elasticsearch
   (SQLite/MySQL)                                          [pegasus-* / workflow-events-*]
        │
        │  read-only poll (?mode=ro)  +  condor_q / condor_history / condor_status
        ▼
   workflow-monitor  ──▶  workflow-events.jsonl  ──(optional Vector tail)──▶ Elasticsearch
        │                 diagnostics-events.jsonl                          [workflow-events-* / workflow-diag-*]
        ▼
   Rich TUI / --serve / --remote / --replay

   (third path, 2026: the wfmonitor PLUGIN runs inside pegasus-monitord itself and
    writes monitord-events.jsonl — workflow-monitor's schema, event-driven, with
    optional in-plugin condor polling via the host's tick(); see §12)
```

The crucial structural fact: **monitord is upstream of `stampede.db`; workflow-monitor is
downstream of it.** workflow-monitor's `job_state` events are therefore a *second-order*
re-materialization of the very events monitord emitted — see the round-trip in §5. (The
§12 plugin removes that second order: it materializes the same records *first-order*,
from the live event stream, before the DB round-trip.)

---

## 2. monitord's output formats and sinks

monitord routes every event through a `MultiplexEventSink` to one or more destinations. The
*encoding* and the *sink* are chosen independently.

### Encodings (`event_output.py`)

| Encoding | Selector | Used for | Key format on the wire |
|---|---|---|---|
| **BP** (NetLogger Best-Practices) | default / `bp` | SQLite/MySQL stampede DB, file, TCP | space-separated `key=value`, dotted keys (`xwf.id`) |
| **BSON** | `bson` | binary file/TCP | dotted keys |
| **JSON** | `json` | **AMQP → Elasticsearch** | flat JSON object, **underscored keys** (`xwf_id`) |

### Sinks (`event_output.py`)

`DBEventSink`, `FileEventSink`, `TCPEventSink`, `AMQPEventSink`, all fronted by
`MultiplexEventSink`.

### Enabling the Elasticsearch path

```properties
pegasus.monitord.encoding              = json
pegasus.catalog.workflow.amqp.url      = amqp://USER:PASS@host:5672/<exchange>
pegasus.catalog.workflow.amqp.events   = *           # else only 4 event types are sent
```

- The AMQP exchange is a **durable topic exchange** on virtual host `pegasus`
  (`AMQPEventSink.EXCH_OPTS`).
- The **routing key** of each message is the full event name, e.g.
  `stampede.job_inst.main.end` (`AMQPEventSink.send` → `STAMPEDE_NS + event`).
- **Default filter is restrictive.** With no `amqp.events` set, monitord publishes **only**
  (`AMQPEventSink.configure_filters`, lines 449-452):
  - `stampede.job_inst.composite`
  - `stampede.job_inst.tag`
  - `stampede.inv.end`
  - `stampede.wf.plan`

  You must set `amqp.events = *` (or a comma-list / globs) to get the full stream.
- From RabbitMQ, a **Logstash** pipeline inserts into Elasticsearch. The index name is
  **set by the operator's Logstash config, not by Pegasus** — common choices are
  `pegasus-composite-events-YYYY.MM` (the `dibbs-data-collection-setup` reference stack) or
  `workflow-events-%{+YYYY.MM.dd}` (the example pipeline in the Pegasus monitoring guide).

---

## 3. The key-naming transformation (the single biggest formatting difference)

Pegasus events are authored once but serialized three different ways. Knowing this chain is
the key to reading anything in Elasticsearch.

```
emitter writes:        kwargs["xwf__id"]        (double underscore)     workflow.py
                              │
        ┌─────────────────────┼──────────────────────────────┐
        │ BP / DB path                                       │ JSON / AMQP / ES path
        ▼                                                    ▼
 nlapi._append / DBEventSink.send:                    json_encode():
   k.replace("__", ".")                                 k.replace(".", "_")
        │  → "xwf.id"                                    then .replace("__","_")
        ▼                                                    │  → "xwf_id"
 stampede DB loader (workflow_loader.py):                    ▼
   k.replace(".", "_")  +  attr_remap            Elasticsearch document field:
        │  → DB column "wf_uuid"                       "xwf_id": "<uuid>"
        ▼
   SQLite column: workflow.wf_uuid
```

So the **same datum** appears as:

| Stage | Key |
|---|---|
| Emitter kwarg | `xwf__id` |
| BP line / DB sink intermediate | `xwf.id` |
| **Elasticsearch field (JSON)** | **`xwf_id`** |
| stampede.db column | `wf_uuid` |

`json_encode` (`event_output.py:585-589`) collapses **both** `.` and `__` to a single `_`,
because Elasticsearch treats a literal `.` in a key as object nesting. The **event name
itself keeps its dots** (`"event": "stampede.job_inst.main.end"`); only the *field keys* are
underscored.

Representative ES field names after this transform: `xwf_id`, `root_xwf_id`, `parent_xwf_id`,
`job_id`, `job_inst_id`, `js_id`, `sched_id`, `submit_hostname`, `submit_dir`, `dax_label`,
`planner_version`, `local_dur`, `cluster_dur`, `cluster_start`, `stdout_file`, `stderr_file`,
`inv_id`, `task_id`, `remote_cpu_time`, `total_memory`.

> **Contrast:** workflow-monitor never does any of this. It picks flat snake_case names by
> hand (`event_type`, `wf_uuid`, `exec_job_id`, `type_desc`, `exitcode`, `stdout_file`) and
> JSON-serializes them directly with no NetLogger conformance and no key mangling.

---

## 4. Complete monitord stampede event catalog (with fields)

All 43 event names monitord knows about (`event_output.py` `_acceptedEvents` /
`workflow_loader.py` `eventMap`). Field names shown in **BP dotted form**; the ES/JSON form
replaces every `.` and `__` with `_`.

### Common envelope (on essentially every event)
`ts` (timestamp), `event` (`stampede.<name>`, dots preserved), `level` (`Info`/`Error`),
`xwf.id` (workflow UUID).

### Workflow-level

| Event | Emitter | Notable fields (BP form) |
|---|---|---|
| `stampede.wf.plan` ¹ | `db_send_wf_info` | `xwf.id`, `dax.label`, `dax.version`, `dax.index`, `dax.file`, `dag.file.name`, `submit.hostname`, `submit.dir`, `argv`, `user`, `grid_dn`, `planner.version`, `parent.xwf.id`, `root.xwf.id`, `ts` |
| `stampede.xwf.start` | `db_send_wf_state` | `xwf.id`, `ts`, `reason`, `restart_count` |
| `stampede.xwf.end` | `db_send_wf_state` | `xwf.id`, `ts`, `reason`, `restart_count`, `status`, `level=Error` (if failed) |
| `stampede.xwf.map.subwf_job` | `db_send_subwf_link` | `xwf.id` (parent), `subwf.id`, `job.id`, `job_inst.id`, `ts` |
| `stampede.xwf.meta` | `workflow_meta` | `xwf.id`, `key`, `value` |

### Static (abstract DAG structure)

`stampede.static.start`, `stampede.static.end`, `stampede.static.meta.start`,
`stampede.static.meta.end`, `stampede.task.info`, `stampede.task.edge`,
`stampede.task.meta`, `stampede.job.info`, `stampede.job.edge`,
`stampede.wf.map.task_job`, `stampede.wf.map.file`, `stampede.rc.meta`, `stampede.rc.pfn`.
Job/task identity fields here: `job.id` (= exec_job_id / DAG node), `task.id` (abstract task
id), `child.job.id`/`parent.job.id`, `child.task.id`/`parent.task.id`.

### Job-instance lifecycle (per job attempt)

All carry `xwf.id`, `job.id`, `job_inst.id` (submit sequence), `js.id` (jobstate sequence),
`ts`, and `sched.id` (the HTCondor cluster id) when known.

| Event | Emitter | Extra fields | Materialized jobstate ² |
|---|---|---|---|
| `job_inst.pre.start` | `db_send_job_brief` | `reason` | PRE_SCRIPT_STARTED |
| `job_inst.pre.term` | brief | | PRE_SCRIPT_TERMINATED |
| `job_inst.pre.end` | brief | `exitcode`, `status` | PRE_SCRIPT_SUCCESS/FAILED |
| `job_inst.submit.start` | brief | `site` | *(seeds job_instance row)* |
| `job_inst.submit.end` | brief | `status` | SUBMIT / SUBMIT_FAILED |
| `job_inst.main.start` | `db_send_job_start` | `stdin.file`, `stdout.file`, `stderr.file` | EXECUTE |
| `job_inst.main.term` | brief | `status` | JOB_TERMINATED / JOB_EVICTED |
| `job_inst.main.end` | `db_send_job_end` | `site`, `user`, `local.dur`, `multiplier_factor`, `exitcode`, `status`, `work_dir`, `cluster.start`, `cluster.dur`, `stdout.text`, `stderr.text` | JOB_SUCCESS / JOB_FAILURE |
| `job_inst.post.start` | brief | | POST_SCRIPT_STARTED |
| `job_inst.post.term` | brief | | POST_SCRIPT_TERMINATED |
| `job_inst.post.end` | brief | `exitcode`, `status` | POST_SCRIPT_SUCCESS/FAILED |
| `job_inst.held.start` | brief | `reason` (hold reason) | JOB_HELD |
| `job_inst.held.end` | brief | | JOB_RELEASED |
| `job_inst.image.info` | brief | (image size) | IMAGE_SIZE |
| `job_inst.abort.info` | brief | | JOB_ABORTED |
| `job_inst.grid.submit.end` | brief | `status` | GRID_SUBMIT / _FAILED |
| `job_inst.globus.submit.end` | brief | `status` | GLOBUS_SUBMIT / _FAILED |
| `job_inst.host.info` | `db_send_host_info` | `hostname`, `ip`, `site`, `total_memory`, `uname` | *(host table)* |
| `job_inst.composite` ¹ | `create_composite_job_event` | merged summary of `main.end` (one-event-per-job rollup; the event ELK dashboards key on) | — |
| `job_inst.tag` ¹ | `tag` | integrity tag fields | — |

### Invocation / metrics

| Event | Emitter | Notable fields (kickstart record) |
|---|---|---|
| `stampede.inv.start` | `db_send_task_start` | `xwf.id`, `job.id`, `job_inst.id`, `inv.id`, `ts` *(noop in DB loader)* |
| `stampede.inv.end` ¹ | `db_send_task_end` | `xwf.id`, `job.id`, `job_inst.id`, `inv.id`, `task.id`, `transformation`, `executable`, `argv`, `start_time`, `dur`, `remote_cpu_time`, `avg_cpu`, `maxrss`, `exitcode`, `ts`, `level` |
| `stampede.int.metric` | `int_metric` | integrity-check metrics per job instance |
| `stampede.task.monitoring` | *(passthrough)* | user-supplied online-monitoring payload (the basis of Panorama time-series) |

¹ One of the **four events sent to AMQP by default**.
² From the loader's `states` map (`workflow_loader.py:549-582`): the `status` field (-1/0)
picks FAILED vs SUCCESS. This is the column value that lands in `stampede.db` and that
workflow-monitor later reads back — see §5.

---

## 5. The round-trip: same transition, two representations

A single compute job's "it started running" fact travels like this:

```
monitord emits:  stampede.job_inst.main.start   (xwf.id, job.id, job_inst.id, js.id, stdout.file, …)
       │  loader maps status→state, writes row
       ▼
stampede.db:     jobstate.state = "EXECUTE"      (workflow_loader states map)
       │  workflow-monitor polls get_events_since()
       ▼
workflow-monitor emits:  {"event_type":"job_state","state":"EXECUTE","exec_job_id":"…","job_id":2,"stdout_file":"…"}
```

So workflow-monitor's **`state` enum values are literally monitord's materialized jobstate
strings** (`SUBMIT`, `EXECUTE`, `JOB_TERMINATED`, `JOB_SUCCESS`, `POST_SCRIPT_SUCCESS`,
`JOB_HELD`, …). One workflow-monitor `job_state` line can stand in for what monitord split
across `submit.end` + `main.start` + `main.term` + `main.end`. workflow-monitor **does not
reconstruct** the separate `inv.end`, `host.info`, or `composite` events — that richness
(CPU time, maxrss, executable, argv, host uname) is collapsed into a handful of optional
fields (`exitcode`, `maxrss`, `stdout_file`, `stderr_file`) plus the separately-embedded
HTCondor ClassAds.

### Event-name correspondence

| Concept | monitord (→ ES field `event`) | workflow-monitor (`event_type`) |
|---|---|---|
| Workflow header / plan | `stampede.wf.plan` | `workflow_start` + `jobs_init` |
| Workflow running/done | `stampede.xwf.start` / `stampede.xwf.end` | `workflow_state` |
| Job roster (static) | `stampede.job.info` / `stampede.job.edge` | `jobs_init` (compute roster only) |
| Per-job transition | `stampede.job_inst.*` (≈16 events) | one `job_state` per transition |
| Kickstart performance | `stampede.inv.end` | partial: `maxrss`/`exitcode` on `job_state`; rollups in `workflow_stats` |
| Execution host | `stampede.job_inst.host.info` | inside `htcondor_poll` (`RemoteHost`) / `workflow_stats.hosts` |
| Per-job rollup | `stampede.job_inst.composite` | `workflow_stats` (workflow-level, not per-job) |
| Live queue snapshot | *(none — monitord is log-driven)* | **`htcondor_poll`** (raw ClassAds) |
| Completed-job condor metrics | *(none)* | **`htcondor_history`** (raw ClassAds) |
| Pool/slot inventory | *(none)* | **`pool_status`** |
| Stall / hold / idle analysis | *(none)* | **diagnostics sidecar** (`stall_detected`, `hold_diagnosis`, `idle_diagnosis`, …) |

---

## 6. workflow-monitor JSONL event catalog (summary)

Full schema in `DATA_SOURCES.md` §9; here for side-by-side reference. Every line carries
`event_type`, `timestamp` (unix float), `wf_uuid`.

| `event_type` | When | Key fields |
|---|---|---|
| `workflow_start` | once (header) | `dax_label`, `user`, `planner_version`, `submit_dir`, `wf_start` |
| `jobs_init` | once (first snapshot) | `total_jobs`, `jobs[]` (`job_id`, `exec_job_id`, `type_desc`, `transformation`, `task_argv`) |
| `workflow_state` | on state change | `state`, `status`, `wf_start`/`wf_end` |
| `job_state` | per transition (incremental) | `exec_job_id`, `type_desc`, `state`, `job_id`, opt `exitcode`/`stdout_file`/`stderr_file`/`maxrss` |
| `htcondor_poll` | queue fingerprint changes | `jobs[]` = **raw condor_q ClassAds** |
| `htcondor_history` | new completed clusters | `jobs[]` = **raw condor_history ClassAds** |
| `pool_status` | pool fingerprint changes | `pool{}` (slots, cpus, memory, gpus, machines, load) |
| `workflow_stats` | before end | `stats{}` (parallelism, cpu/mem efficiency, duration distribution, queue wait, transfer bytes) |
| `workflow_end` | final | `wf_state`, `wf_status`, `wf_end`, `total_jobs`, `done`, `failed`, `elapsed` |
| *(sidecar)* `diag_*`, `stall_detected`, `hold_diagnosis`, `idle_diagnosis`, `failure_diagnosis`, `stall_resolved` | `--diagnose` | interpretive findings + suggestions |

---

## 7. Enumerated differences

1. **Producer vs. consumer.** monitord *generates* the canonical event stream from condor
   logs; workflow-monitor *observes* the DB monitord wrote (plus live HTCondor). One is the
   source of truth; the other is a derived view.

2. **Event granularity.** monitord: ~43 fine-grained events, many per job. workflow-monitor:
   10 coarse events; one `job_state` subsumes several monitord events.

3. **Field naming.** monitord: NetLogger dotted keys, **flattened to `_`** for ES
   (`xwf_id`, `job_inst_id`, `local_dur`). workflow-monitor: hand-named flat snake_case
   (`wf_uuid`, `exec_job_id`, `type_desc`). No shared field-naming convention.

4. **Correlation identity.** monitord is **root/sub-workflow aware**: `xwf_id`,
   `root_xwf_id`, `parent_xwf_id`, and per-attempt `job_inst_id` + `js_id`.
   workflow-monitor uses a single `wf_uuid` (no hierarchical workflow modeling) and an
   integer `job_id`/`exec_job_id`.

5. **Job state representation.** monitord encodes each transition as a *distinct event name*
   (`main.start`, `main.end`, …) with a numeric `status`. workflow-monitor encodes it as a
   *string `state` enum* inside one `job_state` event — the same strings monitord's loader
   materialized into the DB.

6. **Transport & framing.** monitord: AMQP topic exchange, routing key = event name,
   per-message JSON, fan-out to Logstash. workflow-monitor: one local append-only JSONL
   file, byte-offset tailing, optional Vector shipper.

7. **Default scope.** monitord sends **only 4 event types** to AMQP unless `amqp.events`
   is widened — so a default ES deployment is dominated by `composite`/`inv.end`/`wf.plan`.
   workflow-monitor always writes everything it collects.

8. **Embedded raw ClassAds.** workflow-monitor embeds full HTCondor `condor_q`/
   `condor_history` ClassAds verbatim (`htcondor_poll`, `htcondor_history`). monitord has no
   equivalent — condor specifics appear only as already-normalized fields (`sched_id`,
   `exitcode`, `multiplier_factor`).

9. **Resource / pool telemetry.** workflow-monitor emits `pool_status` (slot/cpu/memory/gpu
   inventory from `condor_status`). monitord has nothing comparable.

10. **Derived analytics.** workflow-monitor computes `workflow_stats` (efficiency,
    parallelism, duration distribution, queue wait). monitord is a faithful recorder and
    derives nothing — analytics are left to `pegasus-statistics` over the DB.

11. **Interpretive diagnostics.** workflow-monitor's sidecar emits stall detection and
    hold/idle/failure remediation *suggestions*. monitord emits facts only.

12. **Emission cadence & dedup.** monitord is event-driven (one message per real
    transition, no dedup). workflow-monitor is poll-driven with **content fingerprinting** —
    `htcondor_poll`/`pool_status` are emitted only when a fingerprint changes; `job_state`
    via an incremental high-water-mark query.

13. **Timestamps.** monitord `ts` is the *event's* time parsed from logs (ISO or float UTC).
    workflow-monitor `timestamp` is the logger's wall clock at poll time; the *true* DB
    timestamps are carried separately (`wf_start`, `wf_end`, `job_state.timestamp`).

14. **Resume semantics.** workflow-monitor truncates a trailing `workflow_end` and recovers
    high-water marks so stop/restart yields one continuous series. monitord's resume is at
    the DB/log level, not in the event stream.

15. **Kickstart depth.** monitord's `inv.end` exposes `remote_cpu_time`, `avg_cpu`,
    `maxrss`, `executable`, `argv`, `transformation` per invocation. workflow-monitor keeps
    only `maxrss`/`exitcode` on `job_state` and rolls the rest into `workflow_stats`.

16. **User monitoring events.** monitord can carry `stampede.task.monitoring` (the basis of
    Pegasus *Panorama* online time-series). workflow-monitor has no channel for
    user-injected application metrics.

17. **Online-ness.** monitord's AMQP stream is genuinely real-time (pushed as parsed).
    workflow-monitor's timeliness is bounded by its poll interval and by how fast monitord
    updates the DB it reads.

---

## 8. Strengths & weaknesses

### `pegasus-monitord` → Elasticsearch

**Strengths**
- **Canonical & complete.** It *is* the source of truth; nothing is lost to polling.
- **Fine-grained.** Every lifecycle phase, retry, hold, and invocation is its own event —
  ideal for precise timelines and per-attempt forensics.
- **Root/sub-workflow aware.** `root_xwf_id`/`parent_xwf_id` model hierarchical workflows
  correctly across many runs in one index.
- **Schema-stable.** Field names track the documented stampede schema; downstream mappings
  are predictable across Pegasus versions.
- **Real-time push.** No polling latency; AMQP fan-out scales to many submit hosts.
- **Carries kickstart depth & user metrics** (`inv.end`, `task.monitoring`) out of the box.

**Weaknesses**
- **Deployment burden.** Needs RabbitMQ + Logstash + Elasticsearch and `encoding=json`;
  the index name and mappings are the operator's responsibility.
- **Restrictive default filter** (4 event types) is a silent footgun — a "working" pipeline
  can be missing almost everything until `amqp.events=*`.
- **No HTCondor-native view.** No live queue snapshot, no pool/slot inventory, no
  `condor_history` efficiency — monitord only knows what's in the logs it parses.
- **No analytics or diagnostics.** Raw events only; "why is this idle / stalled?" is not
  answered.
- **High event volume.** Many events per job; the `composite` event exists precisely to give
  dashboards a manageable one-per-job rollup.
- **Key-flattening gotchas.** `.`/`__` → `_` and event names that *retain* dots can surprise
  anyone writing ES queries by hand.

### `workflow-monitor` → JSONL

**Strengths**
- **Zero extra infrastructure for capture.** Writes a local file; ship later with anything
  (Vector, Filebeat, `scp`).
- **HTCondor-complete.** Embeds raw `condor_q`/`condor_history` ClassAds and `condor_status`
  pool inventory that monitord never sees — the full scheduler-side picture.
- **Derived analytics built in** (`workflow_stats`) and **interpretive diagnostics**
  (stall/hold/idle) — actionable, not just factual.
- **Compact & deduped.** Fingerprinting keeps the file small; one `job_state` per transition
  is easy to scan/replay.
- **Self-describing & replayable.** `--replay`/`--remote` operate directly on the file;
  resume logic gives one continuous series across restarts.
- **Stable contract owned here.** The schema is this repo's (`DATA_SOURCES.md` §9), so
  downstream consumers (Vector/ES templates) have one authority to track.

**Weaknesses**
- **Second-order & poll-bounded.** Only as timely/complete as the DB it reads; sub-poll
  transitions can be coalesced or missed.
- **Lossy vs. monitord.** Collapses many lifecycle events into one `job_state`; drops
  per-invocation depth (`remote_cpu_time`, `avg_cpu`, `executable`, `argv`).
- **No sub-workflow hierarchy.** Single `wf_uuid`; nested/sub-workflows aren't modeled as a
  tree the way `root_xwf_id`/`parent_xwf_id` do.
- **Heavy `htcondor_poll` payloads.** Embedding full ClassAds is the largest contributor to
  file size (an open question in the Vector work: whether to ship them at all).
- **Not the canonical record.** For audit/provenance, the stampede DB / monitord stream is
  authoritative; this is a monitoring/observability layer.
- **Bespoke schema.** Doesn't conform to NetLogger/stampede, so it won't drop into
  Pegasus's own ES dashboards without a translation layer.

---

## 9. Safeguards: how workflow-monitor avoids perturbing the running workflow

Because workflow-monitor sits **downstream** of `stampede.db` (see §1), it can be — and is —
built as a strictly *observe-only* layer. It never writes to the workflow's state, never
issues a scheduler command that changes a job, and never holds a lock or a process the
workflow depends on. Every touch point with live state — the SQLite DB monitord is actively
writing, the HTCondor `schedd`/`collector`, and the submit directory on disk — is read-only,
time-bounded, and failure-isolated. The mechanisms below are what make it safe to attach to a
production run.

> **Provenance.** File:line citations in this section are from this repo:
> `src/workflow_monitor/db.py`, `htcondor_poll.py`, `why_idle.py`, `event_log.py`,
> `diag_log.py`, `server.py`, `remote.py`, `diagnostics.py`, `diagnostics_engine.py`,
> `stall_detector.py`, `braindump.py`, `cli.py`.

### 9.1 Read-only access to the stampede database

The one resource the monitor shares with `pegasus-monitord` is the SQLite file monitord is
actively writing. The monitor opens it in **SQLite read-only URI mode**, so the SQLite layer
itself forbids any write:

```python
# db.py:244-248
uri = f"file:{self.db_path}?mode=ro"
self._conn = sqlite3.connect(
    uri, uri=True, timeout=5.0, check_same_thread=False
)
```

- **`mode=ro` is a hard guarantee, not a convention.** A read-only connection cannot acquire
  SQLite's `RESERVED`/`EXCLUSIVE` write locks, so the monitor can never block monitord's
  writes to the DB. Every query method issues `SELECT` only — there is no
  `INSERT`/`UPDATE`/`DELETE`/`CREATE` and no `commit()`/`rollback()` anywhere in the codebase.
- **`timeout=5.0`** bounds how long a read waits on SQLite's shared lock before giving up, so
  a momentary writer lock can't wedge the monitor.
- **Discovery is read-only too.** `braindump.yml` is opened in read mode and parsed with
  `yaml.safe_load` (`braindump.py`); the DB path is only `.exists()`-checked, never created.

### 9.2 Read-only access to the HTCondor scheduler

The monitor's entire view of live HTCondor state comes from **five query commands**, every
one of them read-only:

| Surface | Command / binding | File:line |
|---|---|---|
| Live queue | `condor_q -json` / `schedd.query(...)` | `htcondor_poll.py:121`, `:93` |
| Completed jobs | `condor_history -json` / `schedd.history(...)` | `htcondor_poll.py:261`, `:246` |
| Pool / slot inventory | `condor_status -json` / `Collector.query(Startd)` | `htcondor_poll.py:688`, `:672` |
| Fair-share priority | `condor_userprio -long` | `why_idle.py:58` |
| Negotiator metrics | `condor_status -negotiator -json` | `why_idle.py:112` |

When the Python bindings are available it calls only `Schedd.query`, `Schedd.history`,
`Collector.query`, and `Collector.locate`; when they are not, it falls back to the equivalent
read-only CLI. **Both paths are read-only.** There is no call to `act()`, `edit()`,
`submit()`, or `remove()` on a `Schedd`, and no
`condor_rm`/`condor_hold`/`condor_release`/`condor_submit`/`condor_qedit` anywhere — those
strings appear *only* as suggestion text rendered to the user by the diagnostics layer
(§9.6), never as executed commands. **No code path can modify, remove, hold, or release a
job.**

### 9.3 Bounded, polite polling

Read-only is not enough on its own — a tight poll loop could still hammer the `schedd`. The
monitor is throttled on four axes:

- **Subprocess timeouts.** Every shelled-out condor query runs with `capture_output=True` and
  an explicit timeout (10 s for `condor_q`/`condor_userprio`, 15 s for
  `condor_history`/`condor_status`), so a hung or slow daemon can never block the monitor or
  pile up zombie queries, and output is captured rather than leaking onto the workflow's
  terminal (`htcondor_poll.py`).
- **Tiered cadence.** The live queue is polled every `poll_interval` (default 2 s);
  `condor_history` is throttled to `max(poll_interval × 3, 10 s)` and `condor_status` to
  `max(poll_interval × 5, 15 s)` (`server.py`). The expensive, rarely-changing queries run
  far less often than the cheap one.
- **Deduplication.** Queue and pool snapshots are content-fingerprinted and `condor_history`
  results are de-duplicated by `ClusterId`, so repeated polls do almost no work and emit
  nothing when nothing changed (`server.py`; see §7 item 12).
- **Adaptive back-off (live TUI *and* `--serve`).** A shared `CondorBackoff` **gate** throttles
  the condor poll when the `schedd` is down or overloaded — a query hard-fails (timeout,
  non-zero exit, missing binary). The gate reopens only on an *exponentially* growing interval
  (2 s → 4 → 8 … capped at 60 s) with **jitter** (no lock-step across monitors), and snaps back
  to base cadence the moment a query succeeds. Crucially it gates **only the condor query, not
  the loop**: local `stampede.db` and the TUI keep refreshing every cycle, so the view never
  freezes during an outage; a gated cycle reuses the **last-good** queue (so the fingerprint
  dedup emits nothing new), and while failing, `condor_history`/`condor_status` are suppressed
  too — leaving the backed-off `condor_q` probe as the *only* traffic to a struggling scheduler
  (`htcondor_poll.py`, `CondorBackoff`; failure signal `query_queue(raise_on_error=True)` →
  `CondorQueryError`). An *empty but valid* queue is never a failure, so an idle workflow does
  not trigger back-off. The daemon logs back-off to its `.pid.log`; the live TUI shows a
  **`SCHEDD?`** header badge instead.

### 9.4 Failure isolation: degrade, never disrupt

The monitor is designed so that *its own* failures never propagate outward:

- A momentarily-locked or missing DB raises `sqlite3.OperationalError`, which is caught; the
  monitor returns a minimal "UNKNOWN" snapshot and keeps polling rather than crashing
  (`db.py:462-483`).
- The condor query functions **never raise by default** — timeouts, a missing binary, or
  unparseable JSON are swallowed and the function returns `[]`/`None` (`htcondor_poll.py`,
  `query_queue`/`query_slots`). Callers that need to *act* on failure opt in explicitly
  (`raise_on_error=True`); the live display and one-shot modes do not, so they degrade exactly
  as before.
- There are **no aggressive retry storms**: a failed query reuses the last-good result for that
  cycle, and the condor poll then *backs off* (in both the live TUI and `--serve`) rather than
  retrying at full rate (§9.3). The workflow never sees back-pressure from the observer — the
  load it can place on a struggling scheduler *decreases* as failures persist, while the
  monitor's own local DB/UI refresh keeps running at full cadence.

### 9.5 Writes stay in the monitor's own lane

The monitor writes only files it owns, and they are **new sidecar files**, not edits to
anything Pegasus created:

| File | Purpose | Mode | Location |
|---|---|---|---|
| `workflow-events.jsonl` | replay event stream | append (`"a"`) | `submit_dir/` by default, or `--log PATH` |
| `diagnostics-events.jsonl` | `--diagnose` sidecar | append (`"a"`) | sibling of the event log |
| `workflow-events.pid` / `.pid.log` | `--serve` daemon bookkeeping | write / truncate | sibling of the event log |

The default home for the event log is *inside* the submit directory (`event_log.py`), but it
is a **distinct filename that Pegasus and monitord neither read nor own** — adding it cannot
change how the workflow runs. The monitor never writes to `stampede.db`, `braindump.yml`, the
`.dag`, `*.dagman.out`, condor logs, or kickstart `*.out.NNN` records; those are opened
read-only when parsed for diagnostics. `--log` redirects the event stream entirely out of the
submit tree if desired.

**Disk-exhaustion guard.** Because that default log shares the workflow's filesystem and is
append-only, an unbounded log could in principle fill the volume `pegasus-monitord` and
HTCondor depend on — taking the *workflow* down. `EventLogger` therefore watches the log's
filesystem and **pauses appending** when free space falls below a floor (`--min-free-mb`,
default 200 MB) or the file exceeds an optional cap (`--max-log-mb`, default unlimited),
emitting a `log_paused` control event; it **resumes** (with 1.5× hysteresis) once space
recovers, emitting `log_resumed` with the count of dropped events. The `statvfs` is throttled
to once per 5 s, and a write that fails with `OSError` mid-interval forces an immediate pause —
so even a disk that fills between checks cannot crash the monitor or let it pile onto a full
volume. The guard fails *safe*: it drops its own telemetry rather than ever competing with the
workflow for the last megabytes (`event_log.py`, `_disk_ok`/`_force_pause`/`_resume`; control
events documented in `DATA_SOURCES.md` §9). Set `--min-free-mb 0` (with no cap) to disable it.

### 9.6 Diagnostics are advisory, never actuating

The `--diagnose` layer (`diagnostics.py`, `diagnostics_engine.py`, `stall_detector.py`) only
*describes* problems. Each finding is a text record — `severity`, `summary`, `reason`, and a
list of `suggestions` — emitted to the sidecar JSONL and the TUI. A suggestion such as
`"Try 'condor_release <job_id>' to release the job"` is a **string printed for a human**; the
diagnostics modules contain no `subprocess`, `os.system`, or binding call that would carry it
out. Stall/hold/idle/failure detection is reporting only, and is bounded: already-diagnosed
jobs are de-duplicated and the layer runs at the existing poll cadence, adding no extra
scheduler load.

### 9.7 Non-invasive daemon and remote modes

- **`--serve` (daemon).** A standard double-fork detaches the daemon from the terminal; its
  only signal handling is `SIGTERM`/`SIGINT` setting a `shutdown` flag to break *its own* poll
  loop (`server.py`). `--stop-server` sends `SIGTERM` to the **daemon's** PID from the `.pid`
  file — never to a workflow job or to monitord.
- **`--remote` (SSH).** The remote side is touched only by `cat` and `tail -c +<offset>`
  against the JSONL file for incremental, read-only fetching; results land in a local temp
  dir that is cleaned up on exit (`remote.py`). There is no `scp`/`rsync`/`put` back to the
  remote host and nothing that mutates the remote workflow.
- **No actuating flags exist.** Across every mode (live, `--once`, `--why-idle`, `--serve`,
  `--remote`, `--replay`) the CLI exposes no `--rm`/`--hold`/`--release`/`--kill`/`--clean`
  option (`cli.py`). The tool is observe-only by construction.

### 9.8 Safeguard summary

| Surface the workflow depends on | What the monitor does | Mechanism | Guarantee |
|---|---|---|---|
| `stampede.db` (monitord is writing it) | SELECT-only reads | `file:…?mode=ro`, `timeout=5.0` (`db.py:245`) | Cannot take a write lock or block monitord |
| HTCondor `schedd` / `collector` | 5 read-only queries | `condor_q`/`history`/`status`/`userprio`; `query`/`history`/`locate` only | Cannot modify / remove / hold / release a job |
| Scheduler load | Throttled + adaptive polling | timeouts + tiered cadence + fingerprint dedup + **`CondorBackoff` gate (exponential + jitter, both TUI & `--serve`); local DB/UI unaffected** | Bounded rate that *decreases* when the scheduler struggles; UI never freezes |
| The monitor's own faults | Degrade quietly | catch `OperationalError`; never raises by default; back off instead of retry-storm | Observer failure never reaches the workflow |
| Submit-directory **filesystem** | Bounded, append-only sidecars | new `*.jsonl`/`.pid` files; **pause below `--min-free-mb` / above `--max-log-mb`**; `--log` to relocate | No Pegasus artifact modified; cannot fill the workflow's volume |
| Remediation | Advice only | text `suggestions`; no exec of condor actions | Nothing is actuated automatically |
| Daemon / remote | Self-scoped only | daemon signals itself; SSH `cat`/`tail` read-only | No signal or write reaches the workflow |

---

## 10. Practical Elasticsearch implications

- **Field names differ by design.** A query for monitord data uses `xwf_id`, `job_inst_id`,
  `local_dur`; the same concepts in workflow-monitor docs are `wf_uuid`, `job_id`,
  (computed) `wall_time`. Don't expect one Kibana index pattern to serve both.
- **The join key is the workflow UUID** in both — monitord's `xwf_id` ⇔ workflow-monitor's
  `wf_uuid` — which is the only field reliable for correlating the two streams.
- **Event discriminator field differs.** monitord uses `event` (value keeps dots, e.g.
  `stampede.job_inst.main.end`); workflow-monitor uses `event_type` (e.g. `job_state`).
- **Index naming can collide.** Both ecosystems gravitate to `workflow-events-*`. If you run
  both into one cluster, **namespace them** (e.g. `pegasus-stampede-*` vs.
  `workflow-monitor-*`) or dynamic mapping conflicts will bite (`status` is an int in one,
  `state`/`wf_status` semantics differ; `jobs` is an array of ClassAds in workflow-monitor
  with no monitord analog).
- **Dynamic-mapping risk.** workflow-monitor's embedded ClassAds introduce many open-ended
  fields; the downstream templates use `strings_as_keyword` dynamic templates to contain
  this. monitord's stream is closed-schema and maps cleanly.

---

## 11. When to use which

- **Use monitord → ES** when you need the authoritative, complete, fine-grained provenance
  record across many (possibly hierarchical) workflows, you're standing up shared
  observability for a facility, and you can run the RabbitMQ/Logstash/ES stack. Remember to
  set `amqp.events=*`.
- **Use workflow-monitor → JSONL** when you want an at-a-glance live TUI, scheduler-side
  insight (queue, pool, efficiency, *why* a job is idle/stalled), zero-infra local capture,
  and easy replay — accepting that it's a derived, poll-bounded view.
- **Use the wfmonitor monitord plugin (§12)** when your Pegasus has the monitord plugin
  system and you want workflow-monitor's JSONL **without the poll latency and without a
  second process**: the same records, emitted event-driven from inside monitord, with the
  HTCondor polling optionally folded into the same plugin thread. Setup:
  [`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md).
- **They compose.** Run monitord for the canonical record and workflow-monitor (either the
  polling monitor or the plugin) for the HTCondor-native + diagnostic layer; correlate on
  the workflow UUID (`xwf_id` ⇔ `wf_uuid`). The plugin and the standalone monitor also
  compose with each other — just pass `--no-condor-poll` to `--serve` when the plugin owns
  the condor polling, so the schedd is queried once, not twice.

---

## 12. The third path: the wfmonitor monitord plugin

The 2026-06 `pegasus.monitord.plugins` entry-point system in monitord (branch
`monitord-plugin-system`) made a third path possible, and this repo ships it:
`workflow_monitor/monitord_plugin.py` registers `wfmonitor`, which runs on a dedicated
thread **inside monitord** and translates the live stampede event stream into the *same*
JSONL records §6 catalogs (`workflow_start`, `jobs_init`, `job_state`, `workflow_state`),
written to `monitord-events.jsonl`. With `condor_poll = true` it also emits
`htcondor_poll`/`htcondor_history`/`pool_status` from the plugin host's `tick()` — the
scheduler-side layer of §0's right-hand column, without the polling process.

How it shifts this document's trade-off table:

- **Emission trigger:** event-driven (like monitord's own sinks), not poll-bounded —
  `job_state` records appear as monitord parses the transition, seconds before the polling
  path can see the same row land in `stampede.db` and read it back out.
- **Document shape:** identical to workflow-monitor's (path 2) by construction — the
  §3 key-naming discussion and §10's index-collision warnings apply between paths 1 and
  2/3 exactly as written.
- **Safeguards (§9):** the read-only/bounded-polling guarantees carry over, now enforced
  by the plugin host as well: bounded queue (drop-on-overflow), daemon worker thread,
  exception isolation, bounded shutdown join. The plugin never blocks monitord; condor
  queries stay constraint-scoped, backoff-gated, and fingerprint-deduped.
- **What it does *not* replace:** the TUI, `--remote`, diagnostics, and `workflow_stats`
  still come from the standalone monitor (which can run alongside; see §11), and the
  stampede DB remains the canonical record — plugin delivery is best-effort by design.

**Measured equivalence** (FABRIC testbed, 2026-06-12, plugin tick 5s vs. `--serve` 2s
loop, both paths capturing the same runs):

| Run | Path | `job_state`-bearing events | `htcondor_poll` | `htcondor_history` | `pool_status` |
|---|---|---|---|---|---|
| diamond (12 jobs) | plugin | 88 + condor | 7 | 3 | 5 |
| diamond (12 jobs) | polling | — | 8 | 3 | 5 |
| earthquake (19 containerized jobs) | plugin | 137 + condor | 15 | 5 | 7 |
| earthquake (19 containerized jobs) | polling | — | 20 | 5 | 10 |

Identical history capture; the 2s polling loop buys only ~25–30% more queue/pool
snapshots than the 5s in-monitord tick — i.e., the single-process plugin configuration
loses very little scheduler-side fidelity.

Setup and configuration: [`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md).

---

## Sources

- Pegasus 5.1.2-dev.0 source (local install):
  `Pegasus/monitoring/workflow.py`, `Pegasus/monitoring/event_output.py`,
  `Pegasus/netlogger/nlapi.py`, `Pegasus/db/workflow_loader.py`
- This repo: `src/workflow_monitor/event_log.py`, `DATA_SOURCES.md` §9 / §9b / §10
- This repo (safeguards, §9): `src/workflow_monitor/db.py`, `htcondor_poll.py`,
  `why_idle.py`, `server.py`, `remote.py`, `diagnostics.py`, `diagnostics_engine.py`,
  `stall_detector.py`, `diag_log.py`, `braindump.py`, `cli.py`
- Pegasus docs: [Monitoring reference](https://pegasus.isi.edu/documentation/reference-guide/monitoring.html),
  [pegasus-monitord manpage](https://pegasus.isi.edu/documentation/manpages/pegasus-monitord.html),
  [dibbs-data-collection-setup (RabbitMQ + ELK)](https://github.com/pegasus-isi/dibbs-data-collection-setup)
