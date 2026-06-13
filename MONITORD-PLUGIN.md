# Using workflow-monitor as a `pegasus-monitord` plugin

workflow-monitor ships a **monitord event plugin** (`wfmonitor`) that runs
*inside* `pegasus-monitord` and writes workflow-monitor's native JSONL records
**live, as monitord parses the run** — no polling the stampede database, no
separate monitoring process. Optionally, the same plugin also performs the
HTCondor queue/history/pool polling that `workflow-monitor --serve` would
otherwise do, from monitord's own plugin thread.

This guide is for a user on a **vanilla Pegasus install that includes the
monitord plugin system** (the `pegasus.monitord.plugins` entry-point host; see
*Requirements* below). Nothing here requires any particular testbed or
shipping pipeline — the output is a local JSONL file you can tail, replay in
the TUI, or forward however you like.

```
                       pegasus-monitord (runs your workflow's logs)
                            │ stampede events, in-process, event-driven
                            ▼
                  wfmonitor plugin (this package, on its own thread)
                            │                        │ optional tick():
                            │                        │ condor_q / condor_history /
                            │                        │ condor_status
                            ▼                        ▼
                  <submit-dir>/monitord-events.jsonl   (same schema as
                                                        workflow-events.jsonl)
```

---

## Requirements

- **Pegasus with the monitord plugin system.** The entry-point plugin host is
  not yet in a Pegasus release; it lives on the
  [`monitord-plugin-system`](https://github.com/pegasus-isi/pegasus/tree/monitord-plugin-system)
  branch (pure-Python: `Pegasus/monitoring/plugin.py` plus small changes to
  `event_output.py` and the `pegasus-monitord` CLI). Verify your install has
  it:

  ```bash
  python3 -c "from Pegasus.monitoring.plugin import MonitordEventPlugin; print('plugin host OK')"
  ```

  If that import fails, your Pegasus predates the plugin system. (The three
  files are overlayable onto an existing 5.1.x install; the in-plugin condor
  polling additionally needs the `tick()` hook, commit `25b37965e` or later.)
  The plugin host's user documentation is in the Pegasus reference guide
  under *Monitoring → pegasus-monitord → Event Plugins*.

- **workflow-monitor installed where monitord runs** (the submit host), and
  **visible to the Python that monitord uses**. The `pegasus-monitord`
  wrapper resolves `which python3`, and a `pip install --user` into that
  interpreter's user site is sufficient:

  ```bash
  python3 -m pip install --user \
      "git+https://github.com/pegasus-isi/workflow-monitor.git@monitord-plugin-adapter"
  ```

  Then verify monitord will discover the plugin — **with the same
  `python3`**:

  ```bash
  python3 -c "from importlib.metadata import entry_points; \
    eps = {e.name: e for e in entry_points(group='pegasus.monitord.plugins')}; \
    eps['wfmonitor'].load(); print('wfmonitor OK')"
  ```

  If this prints anything but `wfmonitor OK`, monitord would silently run
  without the plugin — fix the install before going further (most common
  cause: installing into a virtualenv or interpreter that is not the
  `python3` monitord's wrapper picks).

- For the optional condor polling: the `condor_q` / `condor_history` /
  `condor_status` CLI tools on `PATH` (or the `htcondor` Python bindings —
  the plugin tries the bindings first and falls back to the CLI), with
  whatever read access your pool's security configuration grants the
  submitting user. On a default local pool, no credentials are needed.

---

## Configuration

Plugins are configured purely through **Pegasus properties**, in the
`pegasus.monitord.plugins.wfmonitor.*` namespace. Put them in the properties
file your workflow is **planned** with (e.g. the `pegasus.properties` you pass
to `pegasus-plan --conf`) so they are baked into the submit directory that
monitord reads:

```properties
# minimum: turn the plugin on and pin the output path
pegasus.monitord.plugins.wfmonitor.enabled = true
pegasus.monitord.plugins.wfmonitor.events_path = /path/to/run/monitord-events.jsonl

# optional: also poll HTCondor from inside monitord
pegasus.monitord.plugins.wfmonitor.tick_interval = 5
pegasus.monitord.plugins.wfmonitor.condor_poll = true
```

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | must be `true` or monitord never starts the plugin |
| `events_path` | `./monitord-events.jsonl` | output JSONL, line-buffered; use an **absolute path** (the default is relative to monitord's working directory) |
| `restart` | `false` | truncate the output file instead of appending |
| `tick_interval` | `0` | read by the **plugin host**: >0 enables periodic `tick()` callbacks (seconds); required for condor polling |
| `condor_poll` | `false` | poll condor from `tick()` and emit `htcondor_poll` / `htcondor_history` / `pool_status` events |
| `schedd`, `collector`, `token_path`, `cert_path`, `key_path`, `password_file` | unset | optional passthroughs to the condor queries, for non-default pools (same meaning as the `workflow-monitor` CLI flags) |

Host-level tuning also lives in the same namespace (`queue_size`,
`join_timeout`) — see the Pegasus reference guide; the defaults are fine.

Notes on the condor polling:

- `tick_interval` is the base cadence. The plugin polls the queue every due
  tick (behind the same adaptive backoff `--serve` uses, so a struggling
  schedd is never hammered), `condor_history` at ≥ 3× the base (min 10 s),
  and `condor_status` at ≥ 5× (min 15 s), with a final flush at shutdown to
  capture terminal ClassAds.
- Queries are **scoped to your workflow** (a `Cmd` prefix match on the
  planner-recorded submit directory, identical to the standalone monitor);
  the plugin never issues an unconstrained `condor_q`, and never polls before
  the `wf.plan` event supplies the submit directory.
- Emission is fingerprint-deduped exactly like the standalone monitor: a
  poll that observes no change writes nothing.

---

## Running and verifying

Nothing else changes about how you run the workflow — plan and submit as
usual. When monitord starts, its log (`<submit-dir>/monitord.log`) shows the
plugin coming up:

```
... Enabling monitord event plugin host (plugins:// sink)
... wfmonitor plugin writing events to /path/to/run/monitord-events.jsonl
... wfmonitor condor polling enabled (base=5.0s history>=15.0s pool>=25.0s)   # if condor_poll
... started monitord event plugin 'wfmonitor'
```

The output file fills live (line-buffered — `tail -f` works) with
workflow-monitor's native schema (authoritative reference:
[`DATA_SOURCES.md`](DATA_SOURCES.md) §9):

- `workflow_start` — header, once
- `jobs_init` — the job roster, once
- `job_state` — one record per job state transition
- `workflow_state` — `WORKFLOW_STARTED` / `WORKFLOW_TERMINATED` (+ status)
- with `condor_poll`: `htcondor_poll` (raw ClassAds), `htcondor_history`
  (cumulative completed-job ClassAds), `pool_status` (slot/cpu/memory
  summary)

Quick health checks after a run:

```bash
# event-type census
python3 -c "import json,sys,collections; print(collections.Counter(
 json.loads(l)['event_type'] for l in open(sys.argv[1])))" monitord-events.jsonl

# every timestamp should be a sane epoch
python3 -c "import json,sys; bad=[e for e in map(json.loads, open(sys.argv[1]))
 if not (isinstance(e.get('timestamp'),(int,float)) and e['timestamp']>1e9)];
print('bad-timestamp events:', len(bad))" monitord-events.jsonl
```

The file is a first-class workflow-monitor log — replay it in the TUI:

```bash
workflow-monitor --replay /path/to/run/monitord-events.jsonl
```

---

## Coexisting with the standalone monitor

The plugin and the standalone monitor are independent and can run together —
useful for comparison, redundant capture, or because you want the TUI/
diagnostics layer too. One rule: **don't double-poll the schedd.** If the
plugin has `condor_poll = true` and you also run the headless server, disable
the server's own condor polling:

```bash
workflow-monitor <submit-dir> --serve --diagnose --log --no-condor-poll
```

(The stampede-DB snapshotting, TUI, and diagnostics are unaffected by
`--no-condor-poll`; only the `condor_q`/`history`/`status` calls are skipped.)

Conversely, with `condor_poll` unset the plugin emits only the four
pegasus-event types and the standalone monitor behaves exactly as it always
has.

---

## Behavior and caveats

- **Best-effort delivery, never interference.** The plugin host gives the
  plugin a bounded queue and its own daemon thread: a slow or crashing
  plugin drops/logs rather than ever stalling monitord or the workflow.
  Treat the JSONL as telemetry, not as an exactly-once ledger (the stampede
  database remains the canonical record).
- **One monitord, one workflow.** The plugin assumes the single-workflow
  case. Hierarchical workflows spawn one monitord per sub-workflow; each
  would start its own plugin instance, and pointing them at one shared
  `events_path` is untested.
- **Planner roster timestamps.** The events replayed from the planner's
  `<dag>.static.bp` file can carry non-wall-clock `ts` values (a known
  planner quirk: 1970-era stamps). The plugin guards against this
  internally — roster-derived records fall back to wall-clock time — so you
  should never see pre-2001 timestamps in the output; if you do, file a bug.
- **ClassAd values.** `htcondor_poll`/`htcondor_history` embed raw ClassAds;
  non-JSON-native values (e.g. unevaluated `classad.ExprTree` from the
  Python bindings) are stringified, matching the standalone monitor's
  serialization.

## See also

- [`DATA_SOURCES.md`](DATA_SOURCES.md) — the authoritative event schema.
- [`TELEMETRY-COMPARISON.md`](TELEMETRY-COMPARISON.md) — how this push path
  relates to monitord's AMQP→ES pipeline and the polling path, with measured
  side-by-side numbers.
- Pegasus reference guide → *Monitoring → Event Plugins* — the plugin host's
  contract (threading, properties, delivery semantics).
