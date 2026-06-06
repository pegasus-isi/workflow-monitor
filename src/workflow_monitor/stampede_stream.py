"""Live workflow event source: tail pegasus-monitord's native JSONL sink.

This is a drop-in replacement for :class:`~workflow_monitor.db.StampedeDB` that
sources workflow progress from the ``monitord-events.jsonl`` file written by
Pegasus' ``WorkflowMonitorEventSink`` (enabled with
``pegasus.monitord.wfmonitor.url``) instead of polling the stampede SQLite
database.  Because monitord's sink emits records the instant it parses them
from the DAGMan log, workflow-monitor sees progress *before* it is committed to
and polled back out of the DB.

It implements exactly the subset of the ``StampedeDB`` interface the monitoring
loops and :class:`~workflow_monitor.event_log.EventLogger` rely on
(``snapshot``, ``get_events_since``, ``get_workflow_times`` and the
connect/close context-manager protocol), so it slots into both the live TUI
(``display.run_monitor``) and the headless server (``server.run_server``) with
no changes to the loop or logging code.  The same ``EventLogger`` then writes
the merged ``workflow-events.jsonl`` (these Pegasus events interleaved with the
HTCondor events the existing poller collects) — a single writer, so no locking.
"""

from __future__ import annotations

import json
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .db import JobRecord, WorkflowSnapshot

# Jobstate strings that mark a job's end_time. Must match db.get_jobs()'s
# end_time definition exactly — MAX(timestamp) over these states only — so
# the live path computes the same job durations as the DB path (the POST_SCRIPT
# states deliberately do NOT count toward end_time, matching the SQL).
_END_STATES = {"JOB_TERMINATED", "JOB_SUCCESS", "JOB_FAILURE"}


class StampedeStreamReader:
    """Tail ``monitord-events.jsonl`` and expose a StampedeDB-compatible view."""

    def __init__(self, events_path: Path, wf_uuid: Optional[str] = None) -> None:
        self._path = Path(events_path)
        self._wf_uuid = wf_uuid
        self._offset = 0
        self._buf = ""  # carry an unterminated trailing line between drains

        # Per-job roster (rebuilds the joins db.get_jobs() performs)
        self._jobs: Dict[str, dict] = {}
        self._order: List[str] = []
        self._seq_counter = 0

        # Accumulated job-state transitions, DB-shaped, for get_events_since()
        self._job_events: List[dict] = []
        self._recent: deque = deque(maxlen=200)

        # Workflow-level state
        self._wf_state = "UNKNOWN"
        self._wf_status: Optional[int] = None
        self._wf_start: Optional[float] = None
        self._wf_end: Optional[float] = None
        self._header: dict = {}

    # ── StampedeDB-compatible lifecycle ───────────────────────────────────────

    def connect(self) -> None:
        self._drain()

    def close(self) -> None:
        pass

    def __enter__(self) -> "StampedeStreamReader":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ── Tailing ───────────────────────────────────────────────────────────────

    def _drain(self) -> None:
        """Read any newly-appended complete lines and fold them into state.

        Incremental: only the bytes appended since the last drain are read, so
        calling this on every public method (and several times per loop cycle)
        is cheap.  A truncation (monitord replay/recovery rewrites the file)
        is detected via a shrunk size and resets the accumulated state.
        """
        chunk = ""
        try:
            if not self._path.exists():
                return
            size = self._path.stat().st_size
            if size < self._offset:
                # File was truncated (monitord --replay / recovery) — start over.
                self._offset = 0
                self._buf = ""
                self._reset_state()
            with open(self._path) as fh:
                fh.seek(self._offset)
                chunk = fh.read()
                self._offset = fh.tell()
        except OSError:
            return
        if not chunk:
            return

        self._buf += chunk
        lines = self._buf.split("\n")
        self._buf = (
            lines.pop()
        )  # last element is the trailing partial line ("" if complete)
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            self._apply(rec)

    def _reset_state(self) -> None:
        self._jobs.clear()
        self._order.clear()
        self._job_events.clear()
        self._recent.clear()
        self._seq_counter = 0
        self._wf_state = "UNKNOWN"
        self._wf_status = None
        self._wf_start = None
        self._wf_end = None

    def _new_entry(self, name: str) -> dict:
        self._seq_counter += 1
        return {
            "job_id": self._seq_counter,
            "exec_job_id": name,
            "type_desc": None,
            "raw_state": None,
            "exitcode": None,
            "site": None,
            "submit_time": None,
            "start_time": None,
            "end_time": None,
            "transformation": None,
            "task_argv": None,
            "stdout_file": None,
            "stderr_file": None,
            "maxrss": None,
        }

    def _entry_for(self, name: str) -> dict:
        entry = self._jobs.get(name)
        if entry is None:
            entry = self._new_entry(name)
            self._jobs[name] = entry
            self._order.append(name)
        return entry

    def _apply(self, rec: dict) -> None:
        et = rec.get("event_type")
        if et == "workflow_start":
            self._header = rec
            if self._wf_uuid is None:
                self._wf_uuid = rec.get("wf_uuid")
            if rec.get("wf_start") is not None:
                self._wf_start = rec["wf_start"]
        elif et == "jobs_init":
            for j in rec.get("jobs", []):
                name = j.get("exec_job_id")
                if not name:
                    continue
                entry = self._entry_for(name)
                if j.get("job_id") is not None:
                    entry["job_id"] = j["job_id"]
                entry["type_desc"] = j.get("type_desc")
                entry["transformation"] = j.get("transformation")
                entry["task_argv"] = j.get("task_argv")
        elif et == "workflow_state":
            self._wf_state = rec.get("state", self._wf_state)
            if rec.get("status") is not None:
                self._wf_status = rec["status"]
            if rec.get("wf_start") is not None:
                self._wf_start = rec["wf_start"]
            if rec.get("wf_end") is not None:
                self._wf_end = rec["wf_end"]
        elif et == "job_state":
            self._apply_job_state(rec)
        # htcondor_*/pool_status/workflow_stats/workflow_end records that may be
        # present (e.g. if pointed at a merged log) are ignored here — this
        # reader only reconstructs the Pegasus view.

    def _apply_job_state(self, rec: dict) -> None:
        name = rec.get("exec_job_id")
        if not name:
            return
        entry = self._entry_for(name)
        if rec.get("type_desc") is not None:
            entry["type_desc"] = rec["type_desc"]
        if rec.get("job_id") is not None:
            entry["job_id"] = rec["job_id"]

        state = rec.get("state")
        ts = rec.get("timestamp")
        entry["raw_state"] = state  # latest transition wins (chronological stream)
        if rec.get("exitcode") is not None:
            entry["exitcode"] = rec["exitcode"]
        if rec.get("stdout_file"):
            entry["stdout_file"] = rec["stdout_file"]
        if rec.get("stderr_file"):
            entry["stderr_file"] = rec["stderr_file"]
        if rec.get("maxrss") is not None:
            entry["maxrss"] = rec["maxrss"]

        # Timing, mirroring db.get_jobs(): MIN(SUBMIT), MIN(EXECUTE), MAX(terminal).
        if ts is not None:
            if state == "SUBMIT":
                if entry["submit_time"] is None or ts < entry["submit_time"]:
                    entry["submit_time"] = ts
            elif state == "EXECUTE":
                if entry["start_time"] is None or ts < entry["start_time"]:
                    entry["start_time"] = ts
            elif state in _END_STATES:
                if entry["end_time"] is None or ts > entry["end_time"]:
                    entry["end_time"] = ts

        # DB-shaped row for get_events_since() (keys match db.get_events_since).
        self._job_events.append(
            {
                "exec_job_id": name,
                "type_desc": entry["type_desc"],
                "state": state,
                "timestamp": ts,
                "job_id": entry["job_id"],
                "exitcode": rec.get("exitcode"),
                "stdout_file": rec.get("stdout_file"),
                "stderr_file": rec.get("stderr_file"),
                "maxrss": rec.get("maxrss"),
            }
        )
        self._recent.append(
            {
                "exec_job_id": name,
                "type_desc": entry["type_desc"],
                "state": state,
                "timestamp": ts,
            }
        )

    # ── StampedeDB-compatible queries ─────────────────────────────────────────

    def get_workflow_times(self) -> Dict:
        self._drain()
        return {"start": self._wf_start, "end": self._wf_end}

    def get_events_since(self, after_ts: float) -> List[Dict]:
        """Job-state transitions with timestamp strictly after *after_ts*.

        Same ``> after_ts`` semantics as :meth:`StampedeDB.get_events_since`, so
        EventLogger's high-water-mark advancement behaves identically.
        """
        self._drain()
        return [
            dict(ev)
            for ev in self._job_events
            if ev["timestamp"] is not None and ev["timestamp"] > after_ts
        ]

    def snapshot(self) -> WorkflowSnapshot:
        self._drain()
        now = datetime.now().timestamp()
        jobs: List[JobRecord] = []
        for name in self._order:
            e = self._jobs[name]
            jobs.append(
                JobRecord(
                    job_id=e["job_id"],
                    exec_job_id=e["exec_job_id"],
                    type_desc=e["type_desc"] or "",
                    raw_state=e["raw_state"],
                    exitcode=e["exitcode"],
                    site=e["site"],
                    submit_time=e["submit_time"],
                    start_time=e["start_time"],
                    end_time=e["end_time"],
                    _now=now,
                    transformation=e["transformation"],
                    task_argv=e["task_argv"],
                    stdout_file=e["stdout_file"],
                    stderr_file=e["stderr_file"],
                    maxrss=e["maxrss"],
                )
            )
        # Most recent first, matching db.get_recent_events()'s DESC ordering.
        recent = list(self._recent)[-20:][::-1]
        return WorkflowSnapshot(
            wf_state=self._wf_state,
            wf_status=self._wf_status,
            wf_start=self._wf_start,
            wf_end=self._wf_end,
            jobs=jobs,
            recent_events=recent,
            poll_time=now,
        )
