"""JSONL event logger for workflow replay."""

from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .braindump import WorkflowInfo
from .db import StampedeDB, WorkflowSnapshot
from .htcondor_poll import PoolSummary
from .stats import compute_workflow_stats


class EventLogger:
    """Writes workflow events to a JSONL file for later replay.

    Each line is a self-contained JSON object with at minimum:
        {"event_type": "...", "timestamp": <unix_float>, "wf_uuid": "..."}

    If an existing log file is detected for the same workflow, the logger
    resumes from where it left off — no duplicate header, no re-emitted
    events — so stop/restart of the monitor produces a single continuous
    series.
    """

    def __init__(
        self,
        info: WorkflowInfo,
        db: StampedeDB,
        log_path: Optional[Path] = None,
        min_free_mb: float = 200.0,
        max_log_mb: Optional[float] = None,
    ) -> None:
        self._info = info
        self._db = db
        self._wf_uuid = info.wf_uuid

        if log_path is None:
            log_path = info.submit_dir / "workflow-events.jsonl"
        self._path = log_path

        # ── Disk-exhaustion guard ────────────────────────────────────────────
        # The event log is append-only and unbounded, and by default lives on
        # the same filesystem as the workflow's submit dir.  To guarantee the
        # monitor can never fill that filesystem out from under pegasus-monitord
        # or HTCondor, appending pauses when free space drops below a floor (or
        # an optional size cap is exceeded) and resumes if space recovers.
        self._min_free_bytes: int = int(max(min_free_mb, 0.0) * 1024 * 1024)
        self._max_log_bytes: Optional[int] = (
            int(max_log_mb * 1024 * 1024) if max_log_mb and max_log_mb > 0 else None
        )
        self._paused: bool = False
        self._dropped_events: int = 0
        self._disk_last_check: float = 0.0
        self._disk_check_interval: float = 5.0

        self._high_water_ts: float = 0.0
        self._last_wf_state: Optional[str] = None
        self._last_condor_fingerprint: frozenset[tuple] = frozenset()
        self._last_history_fingerprint: frozenset[tuple] = frozenset()
        self._last_pool_fingerprint: Optional[str] = None
        self._jobs_init_emitted: bool = False
        self._resumed: bool = False

        # Try to resume from an existing log before opening for append
        self._try_resume()

        self._fh = open(self._path, "a")

        if not self._resumed:
            self._write_header()

    # ── Resume from existing log ──────────────────────────────────────────────

    def _try_resume(self) -> None:
        """Scan an existing log file to recover state for seamless continuation.

        If the log ends with a ``workflow_end`` event (from a previous server
        session), the file is truncated to remove it so that new events
        append cleanly and clients never see a premature end marker.
        """
        if not self._path.exists() or self._path.stat().st_size == 0:
            return

        header_uuid: Optional[str] = None
        last_end_offset: Optional[int] = 0  # byte offset of last workflow_end line

        try:
            with open(self._path) as fh:
                offset = 0
                for line in fh:
                    raw = line.strip()
                    if not raw:
                        offset = fh.tell()
                        continue
                    ev = json.loads(raw)
                    etype = ev.get("event_type")

                    if etype == "workflow_start":
                        header_uuid = ev.get("wf_uuid")

                    elif etype == "workflow_state":
                        self._last_wf_state = ev.get("state")

                    elif etype == "job_state":
                        ts = ev.get("timestamp", 0)
                        if ts > self._high_water_ts:
                            self._high_water_ts = ts

                    elif etype == "jobs_init":
                        self._jobs_init_emitted = True

                    elif etype == "htcondor_poll":
                        jobs = ev.get("jobs", [])
                        self._last_condor_fingerprint = self._condor_fingerprint(jobs)

                    elif etype == "htcondor_history":
                        jobs = ev.get("jobs", [])
                        self._last_history_fingerprint = self._history_fingerprint(jobs)

                    elif etype == "pool_status":
                        self._last_pool_fingerprint = self._pool_fingerprint(
                            ev.get("pool", {})
                        )

                    elif etype == "workflow_end":
                        last_end_offset = offset

                    offset = fh.tell()
        except (json.JSONDecodeError, OSError):
            # Corrupted or unreadable — start fresh
            return

        # Only resume if the existing log belongs to the same workflow
        if header_uuid is not None and header_uuid == self._wf_uuid:
            self._resumed = True
            # Truncate any trailing workflow_end so new events append cleanly
            if last_end_offset is not None and last_end_offset > 0:
                with open(self._path, "r+") as fh:
                    fh.seek(last_end_offset)
                    rest = fh.read().strip()
                    # Only truncate if workflow_end is the last event
                    try:
                        ev = json.loads(rest)
                        if ev.get("event_type") == "workflow_end":
                            fh.seek(last_end_offset)
                            fh.truncate()
                    except (json.JSONDecodeError, ValueError):
                        pass

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _write_raw(self, event: Dict[str, Any]) -> None:
        """Serialize and append one event, bypassing the disk-exhaustion guard.

        Used both by ``_emit`` (after the guard approves) and by the guard
        itself to write its own ``log_paused``/``log_resumed`` control markers.
        """
        event.setdefault("wf_uuid", self._wf_uuid)
        self._fh.write(json.dumps(event, default=str) + "\n")
        self._fh.flush()

    def _emit(self, event: Dict[str, Any]) -> None:
        if not self._disk_ok():
            self._dropped_events += 1
            return
        try:
            self._write_raw(event)
        except OSError as exc:
            # Filesystem filled between periodic checks — stop writing now so we
            # never contribute to exhausting the volume the workflow runs on.
            self._force_pause(f"write failed: {exc}")
            self._dropped_events += 1

    # ── Disk-exhaustion guard ─────────────────────────────────────────────────

    def _disk_ok(self) -> bool:
        """Return True if it is safe to append another event.

        Pauses logging when free space on the log's filesystem drops below the
        configured floor, or when an optional size cap is exceeded; resumes once
        free space recovers (with 1.5x hysteresis to avoid flapping).  The
        underlying ``statvfs`` is throttled to once per ``_disk_check_interval``
        seconds so the guard adds negligible overhead at the normal poll cadence.
        """
        if self._min_free_bytes <= 0 and self._max_log_bytes is None:
            return True  # guard fully disabled

        now = time.time()
        if now - self._disk_last_check < self._disk_check_interval:
            return not self._paused
        self._disk_last_check = now

        try:
            free = shutil.disk_usage(self._path.parent).free
        except OSError:
            return not self._paused  # can't stat — leave state unchanged
        size = 0
        if self._max_log_bytes is not None:
            try:
                size = self._path.stat().st_size
            except OSError:
                size = 0

        low_space = self._min_free_bytes > 0 and free < self._min_free_bytes
        too_big = self._max_log_bytes is not None and size > self._max_log_bytes

        if not self._paused and (low_space or too_big):
            if low_space:
                reason = (
                    f"free space {free // (1024 * 1024)} MB below floor "
                    f"{self._min_free_bytes // (1024 * 1024)} MB"
                )
            else:
                reason = (
                    f"log size {size // (1024 * 1024)} MB exceeds cap "
                    f"{self._max_log_bytes // (1024 * 1024)} MB"
                )
            self._force_pause(reason, free=free, size=size)
        elif self._paused:
            # Resume only with real headroom (1.5x the floor) and under the cap.
            recovered_space = (
                self._min_free_bytes <= 0 or free > self._min_free_bytes * 1.5
            )
            under_cap = self._max_log_bytes is None or size <= self._max_log_bytes
            if recovered_space and under_cap:
                self._resume(free=free, size=size)

        return not self._paused

    def _force_pause(self, reason: str, free: int = 0, size: int = 0) -> None:
        """Enter the paused state and write a best-effort ``log_paused`` marker."""
        if self._paused:
            return
        self._paused = True
        print(
            f"[disk-guard] pausing event log — {reason}; further events dropped "
            f"until space recovers",
            file=sys.stderr,
        )
        try:
            self._write_raw(
                {
                    "event_type": "log_paused",
                    "timestamp": time.time(),
                    "reason": reason,
                    "free_mb": round(free / (1024 * 1024), 1),
                    "log_mb": round(size / (1024 * 1024), 1),
                }
            )
        except OSError:
            pass  # disk truly full — nothing more we can do

    def _resume(self, free: int = 0, size: int = 0) -> None:
        """Leave the paused state and write a ``log_resumed`` marker."""
        dropped = self._dropped_events
        try:
            self._write_raw(
                {
                    "event_type": "log_resumed",
                    "timestamp": time.time(),
                    "free_mb": round(free / (1024 * 1024), 1),
                    "log_mb": round(size / (1024 * 1024), 1),
                    "dropped": dropped,
                }
            )
        except OSError:
            return  # couldn't even write the resume marker — stay paused
        self._paused = False
        self._dropped_events = 0
        print(
            f"[disk-guard] resuming event log — free space recovered "
            f"({free // (1024 * 1024)} MB); {dropped} event(s) dropped while paused",
            file=sys.stderr,
        )

    def _write_header(self) -> None:
        wf_times = self._db.get_workflow_times()
        self._emit(
            {
                "event_type": "workflow_start",
                "timestamp": time.time(),
                "dax_label": self._info.dax_label,
                "user": self._info.user,
                "planner_version": self._info.planner_version,
                "submit_dir": str(self._info.submit_dir),
                "wf_start": wf_times.get("start"),
            }
        )

    # ── Public API ───────────────────────────────────────────────────────────

    def record(
        self,
        snapshot: WorkflowSnapshot,
        condor_jobs: Optional[List[Dict]] = None,
        condor_history: Optional[List[Dict]] = None,
        pool_status: Optional[PoolSummary] = None,
    ) -> None:
        """Record new events from the latest poll cycle."""
        if not self._jobs_init_emitted:
            self._emit_jobs_init(snapshot)
            self._jobs_init_emitted = True
        self._record_workflow_state(snapshot)
        self._record_job_events()
        self._record_condor_poll(condor_jobs)
        self._record_condor_history(condor_history)
        self._record_pool_status(pool_status)

    def close(
        self,
        snapshot: Optional[WorkflowSnapshot] = None,
        condor_history: Optional[List[Dict]] = None,
        pool_status: Optional[PoolSummary] = None,
    ) -> None:
        """Emit workflow_stats and workflow_end events, then close the file."""
        # Emit analytics summary before the end marker
        if snapshot is not None:
            wf_stats = compute_workflow_stats(snapshot, condor_history, pool_status)
            self._emit(
                {
                    "event_type": "workflow_stats",
                    "timestamp": time.time(),
                    "stats": wf_stats.to_dict(),
                }
            )

        end_event: Dict[str, Any] = {
            "event_type": "workflow_end",
            "timestamp": time.time(),
        }
        if snapshot is not None:
            end_event["wf_state"] = snapshot.wf_state
            end_event["wf_status"] = snapshot.wf_status
            end_event["wf_end"] = snapshot.wf_end
            end_event["total_jobs"] = snapshot.total_jobs()
            end_event["done"] = snapshot.done_count()
            end_event["failed"] = snapshot.failed_count()
            end_event["elapsed"] = snapshot.elapsed
        self._emit(end_event)
        try:
            self._fh.close()
        except OSError:
            pass  # closing flushes buffered data; ignore a full-disk failure

    @property
    def path(self) -> Path:
        return self._path

    @property
    def high_water_ts(self) -> float:
        """Timestamp of the most recent job_state event processed."""
        return self._high_water_ts

    @property
    def resumed(self) -> bool:
        return self._resumed

    # ── Event detection ──────────────────────────────────────────────────────

    def _emit_jobs_init(self, snapshot: WorkflowSnapshot) -> None:
        """Emit a jobs_init event listing the full job roster from the first snapshot."""
        jobs = []
        for j in snapshot.jobs:
            entry: dict = {
                "job_id": j.job_id,
                "exec_job_id": j.exec_job_id,
                "type_desc": j.type_desc,
            }
            if j.transformation:
                entry["transformation"] = j.transformation
            if j.task_argv:
                entry["task_argv"] = j.task_argv
            jobs.append(entry)
        self._emit(
            {
                "event_type": "jobs_init",
                "timestamp": time.time(),
                "total_jobs": len(jobs),
                "jobs": jobs,
            }
        )

    def _record_workflow_state(self, snapshot: WorkflowSnapshot) -> None:
        if snapshot.wf_state != self._last_wf_state:
            ev: Dict[str, Any] = {
                "event_type": "workflow_state",
                "timestamp": time.time(),
                "state": snapshot.wf_state,
                "status": snapshot.wf_status,
            }
            # Include actual DB timestamps so remote clients can compute
            # accurate elapsed time (time.time() is logger wall clock).
            if snapshot.wf_state == "WORKFLOW_STARTED" and snapshot.wf_start:
                ev["wf_start"] = snapshot.wf_start
            elif snapshot.wf_state == "WORKFLOW_TERMINATED" and snapshot.wf_end:
                ev["wf_end"] = snapshot.wf_end
            self._emit(ev)
            self._last_wf_state = snapshot.wf_state

    def _record_job_events(self) -> None:
        events = self._db.get_events_since(self._high_water_ts)
        for ev in events:
            record: Dict[str, Any] = {
                "event_type": "job_state",
                "timestamp": ev["timestamp"],
                "exec_job_id": ev["exec_job_id"],
                "type_desc": ev["type_desc"],
                "state": ev["state"],
                "job_id": ev["job_id"],
            }
            if ev.get("exitcode") is not None:
                record["exitcode"] = ev["exitcode"]
            if ev.get("stdout_file"):
                record["stdout_file"] = ev["stdout_file"]
            if ev.get("stderr_file"):
                record["stderr_file"] = ev["stderr_file"]
            if ev.get("maxrss") is not None:
                record["maxrss"] = ev["maxrss"]
            self._emit(record)
            if ev["timestamp"] > self._high_water_ts:
                self._high_water_ts = ev["timestamp"]

    @staticmethod
    def _condor_fingerprint(jobs: List[Dict]) -> frozenset:
        """Build a fingerprint that captures job identity AND key attributes.

        This ensures an htcondor_poll event is emitted when a job's status,
        hold reason, host assignment, or transfer progress changes.
        """
        parts = []
        for cj in jobs:
            key = cj.get("ClusterId", cj.get("DAGNodeName", ""))
            status = cj.get("JobStatus", "")
            hold = cj.get("HoldReason", "")
            host = cj.get("RemoteHost", "")
            sent = str(cj.get("BytesSent", ""))
            recvd = str(cj.get("BytesRecvd", ""))
            parts.append((str(key), str(status), hold, host, sent, recvd))
        return frozenset(parts)

    def _record_condor_poll(self, condor_jobs: Optional[List[Dict]]) -> None:
        if condor_jobs is None:
            return
        fp = self._condor_fingerprint(condor_jobs)
        if fp != self._last_condor_fingerprint:
            self._emit(
                {
                    "event_type": "htcondor_poll",
                    "timestamp": time.time(),
                    "jobs": condor_jobs,
                }
            )
            self._last_condor_fingerprint = fp

    @staticmethod
    def _history_fingerprint(jobs: List[Dict]) -> frozenset:
        """Build a fingerprint for history records (keyed by ClusterId)."""
        parts = []
        for hj in jobs:
            key = str(hj.get("ClusterId", ""))
            parts.append(key)
        return frozenset(parts)

    def _record_condor_history(self, condor_history: Optional[List[Dict]]) -> None:
        if not condor_history:
            return
        fp = self._history_fingerprint(condor_history)
        if fp != self._last_history_fingerprint:
            self._emit(
                {
                    "event_type": "htcondor_history",
                    "timestamp": time.time(),
                    "jobs": condor_history,
                }
            )
            self._last_history_fingerprint = fp

    @staticmethod
    def _pool_fingerprint(pool_dict: Dict) -> str:
        """Build a fingerprint for pool status based on slot counts."""
        return (
            f"{pool_dict.get('total_slots', 0)}:"
            f"{pool_dict.get('claimed_slots', 0)}:"
            f"{pool_dict.get('idle_slots', 0)}:"
            f"{pool_dict.get('total_cpus', 0)}:"
            f"{pool_dict.get('idle_cpus', 0)}"
        )

    def _record_pool_status(self, pool: Optional["PoolSummary"]) -> None:
        if pool is None:
            return
        pool_dict = pool.to_dict()
        fp = self._pool_fingerprint(pool_dict)
        if fp != self._last_pool_fingerprint:
            self._emit(
                {
                    "event_type": "pool_status",
                    "timestamp": time.time(),
                    "pool": pool_dict,
                }
            )
            self._last_pool_fingerprint = fp
