# Changelog

All notable changes to `workflow-monitor` are documented here.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added — workflow-safety safeguards

Two safeguards that strengthen `workflow-monitor`'s core promise: as an
**external, observe-only** layer it must never burden the Pegasus workflow it
watches. Both follow from the producer-vs-consumer analysis in
[`MONITORD-v-WORKFLOW-MONITOR.md`](MONITORD-v-WORKFLOW-MONITOR.md) §9; they close
the two remaining ways an external observer could still hurt a live run — filling
the workflow's disk, and hammering a struggling scheduler.

#### 1. Disk-exhaustion guard for the JSONL event log

**Problem.** The event log is append-only and, by default, written *inside* the
workflow's submit directory (`<submit_dir>/workflow-events.jsonl`) — the same
filesystem `pegasus-monitord`'s DB writes, HTCondor's logs, and the jobs'
outputs all share. With no size bound, a long `--serve` run on a verbose
workflow (embedded `htcondor_poll` ClassAds are the dominant size contributor)
could fill that volume, at which point monitord's DB flush and condor logging
fail and **the workflow itself stalls or dies.** The observer would have caused
the outage.

**Fix.** `EventLogger` now watches the log's filesystem and **pauses appending**
before it can exhaust the volume:

- Pauses when free space drops below a floor (`--min-free-mb`, default **200 MB**)
  or when the file exceeds an optional cap (`--max-log-mb`, default **unlimited**).
- Emits a `log_paused` control event on pause and `log_resumed` (with a count of
  dropped events) on recovery. Events produced while paused are **dropped, not
  buffered** — the guard never trades the workflow's disk for its own telemetry.
- Resumes only with **1.5× hysteresis** above the floor, to avoid flapping.
- The `statvfs` check is **throttled to once per 5 s** (negligible overhead at
  the normal poll cadence). A write that fails with `OSError` *between* checks
  (disk filled mid-interval) forces an immediate pause instead of crashing.
- **Fails safe** everywhere: if even the `log_paused`/`log_resumed` marker or the
  final file close can't be written, the error is swallowed.
- Disable entirely with `--min-free-mb 0` and no `--max-log-mb`.

The guard lives in `EventLogger`, so it protects **every** logging path — the
`--serve` daemon and the live-TUI `--log` mode alike. Defaults are on, so the
protection applies even if a caller does not wire the CLI flags.

New CLI flags (group **“Logging safeguards”**):

| Flag | Default | Effect |
|---|---|---|
| `--min-free-mb MB` | `200` | Pause logging below this much free space (`0` disables) |
| `--max-log-mb MB` | unlimited | Pause logging when the JSONL grows past this size |

New JSONL control events (schema in `DATA_SOURCES.md` §9): `log_paused`,
`log_resumed`. These describe the *log*, not the workflow; replay/remote
consumers may ignore them. A `dropped > 0` on `log_resumed` flags a gap.

#### 2. Adaptive back-off when the HTCondor scheduler is unreachable

**Problem.** The `--serve` poll loop ran at a fixed cadence regardless of
failures. When the `schedd` was down or overloaded, every cycle still fired a
query that burned its full 10–15 s timeout — i.e. the monitor hit the scheduler
*hardest exactly when it was most fragile*.

**Fix.** The daemon loop now backs off when condor queries hard-fail:

- On a hard failure (timeout, non-zero exit, missing binary, unparseable output)
  the sleep grows **exponentially** — `2 s → 4 → 8 → … capped at 60 s` — and
  **snaps back** to the base interval the instant a query succeeds.
- Every idle sleep carries a little **jitter** so multiple monitors sharing one
  `schedd` never poll in lock-step (thundering-herd avoidance).
- An **empty-but-valid** queue (exit 0, no jobs matched) is *not* a failure, so an
  idle or between-stages workflow never triggers back-off.
- **Scope is deliberate.** Back-off keys on the *scheduler* (the shared, remote,
  fragile resource). Local `stampede.db` polling is left at full cadence: its
  transient `mode=ro` lock contention is normal and self-resolves in
  milliseconds, so backing off on it would only make the monitor sluggish for no
  benefit. When the main loop backs off, the already-throttled history/pool
  polls naturally slow with it.

Mechanism: `query_queue(raise_on_error=True)` now distinguishes a hard failure
(raises the new `CondorQueryError`) from a valid empty result (returns `[]`).
The default contract is unchanged — `raise_on_error` defaults to `False`, so the
live display, `--once`, and `--why-idle` paths behave exactly as before. The
daemon counts consecutive failures and feeds `server._backoff_sleep()`. Entering
and leaving back-off is logged to the daemon's `.pid.log` for visibility.

### Changed

- `EventLogger.__init__` gains `min_free_mb` / `max_log_mb` parameters
  (defaults 200 / None). `_emit` now routes through `_disk_ok()`; raw writes are
  factored into `_write_raw`.
- `htcondor_poll.query_queue` / `_query_via_subprocess` gain `raise_on_error`
  (default `False`); empty stdout with exit 0 is now explicitly a valid empty
  result, never an error.
- `server.run_server`, `display.run_monitor` gain `min_free_mb` / `max_log_mb`
  pass-through; `server`'s main-loop sleep is now `_backoff_sleep(...)`.

### Files touched

| File | Change |
|---|---|
| `src/workflow_monitor/event_log.py` | Disk-exhaustion guard (`_disk_ok`, `_force_pause`, `_resume`, `_write_raw`); resilient `close()` |
| `src/workflow_monitor/htcondor_poll.py` | `CondorQueryError`; `raise_on_error` on `query_queue` / `_query_via_subprocess` |
| `src/workflow_monitor/server.py` | `_backoff_sleep`, `_MAX_BACKOFF_SECONDS`, failure-streak tracking in `_poll_condor`, back-off sleep; guard pass-through |
| `src/workflow_monitor/display.py` | Disk-guard pass-through to `EventLogger` |
| `src/workflow_monitor/cli.py` | `--min-free-mb` / `--max-log-mb` flags; wired into serve + live modes |
| `DATA_SOURCES.md` | Documented `log_paused` / `log_resumed` control events (§9) |
| `MONITORD-v-WORKFLOW-MONITOR.md` | Updated safeguards §9.3–9.5, §9.8 to reflect the implemented guards |

### Compatibility

- **Backward compatible.** All new parameters and flags have safe defaults; no
  existing call site or CLI invocation changes behavior unless the new flags are
  used. The default contract of the public `query_*` functions (“never raises”)
  is preserved for every caller that does not opt in.
- **JSONL consumers** that switch on `event_type` simply ignore the two new
  control events; existing event shapes are unchanged.

### Testing

Verified with standalone checks (no live workflow required):

- `_backoff_sleep`: base+jitter at streak 0; exponential `4→8→16`; cap at 60 s;
  jitter bounds hold across streaks 0–24.
- `CondorQueryError`: raised on timeout / non-zero exit / unparseable JSON only
  when `raise_on_error=True`; an empty queue (exit 0) **never** raises; valid
  JSON returns the parsed list.
- Disk guard: floor pause drops events and writes `log_paused`; recovery writes
  `log_resumed` with the correct `dropped` count; size cap triggers pause; a
  disabled guard (`--min-free-mb 0`, no cap) never pauses; an `OSError` mid-write
  forces a pause without crashing.
- `--min-free-mb` / `--max-log-mb` parse and surface in `--help`; all five
  touched modules byte-compile.
