"""JSONL replay engine for workflow monitor."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.live import Live

from .braindump import WorkflowInfo
from .db import JobRecord, WorkflowSnapshot
from .display import build_layout, _print_final_summary
from .htcondor_poll import PoolSummary
from .stats import WorkflowStats, compute_workflow_stats


class ReplayEngine:
    """Loads a JSONL event log and replays it through the Rich TUI."""

    def __init__(self, path: Path, speed: float = 1.0, events_n: int = 15) -> None:
        self._path = path
        self._speed = speed
        self._events_n = events_n

        self._events: List[Dict[str, Any]] = []
        self._info: Optional[WorkflowInfo] = None
        self._header_wf_start: Optional[float] = None

        self._load()

    # ── Loading ──────────────────────────────────────────────────────────────

    def _load(self) -> None:
        raw: List[Dict[str, Any]] = []
        with open(self._path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    raw.append(json.loads(line))

        if not raw:
            raise ValueError(f"Empty event log: {self._path}")

        # Extract workflow info from header
        header = raw[0]
        if header.get("event_type") != "workflow_start":
            raise ValueError(
                f"First event must be workflow_start, got: {header.get('event_type')}"
            )

        self._info = WorkflowInfo(
            wf_uuid=header.get("wf_uuid", "unknown"),
            root_wf_uuid=header.get("wf_uuid", "unknown"),
            dax_label=header.get("dax_label", "unknown"),
            submit_dir=Path(header.get("submit_dir", ".")),
            user=header.get("user", "unknown"),
            planner_version=header.get("planner_version", "?"),
            dag_file="replay.dag",
            condor_log="replay.log",
            timestamp=str(header.get("timestamp", "")),
            basedir=Path(header.get("submit_dir", ".")),
        )

        # Capture wf_start from header if logged (for stable elapsed time)
        self._header_wf_start = header.get("wf_start")

        # Keep all events except headers for replay.
        # Old logs from stop/restart sessions may contain multiple
        # workflow_start/jobs_init/workflow_end markers.  We strip
        # extra headers and intermediate workflow_end events so the
        # replay sees a single continuous series.
        body = raw[1:]

        # Filter out extra session artifacts
        cleaned: List[Dict[str, Any]] = []
        for ev in body:
            etype = ev.get("event_type")
            if etype == "workflow_start":
                # Extra header from a resumed session — skip
                continue
            if (
                etype == "jobs_init"
                and cleaned
                and any(e.get("event_type") == "jobs_init" for e in cleaned)
            ):
                # Duplicate jobs_init — skip
                continue
            cleaned.append(ev)

        # Remove intermediate workflow_end events (keep only the last one)
        final_end_idx = None
        for i in range(len(cleaned) - 1, -1, -1):
            if cleaned[i].get("event_type") == "workflow_end":
                final_end_idx = i
                break
        if final_end_idx is not None:
            cleaned = [
                ev
                for i, ev in enumerate(cleaned)
                if ev.get("event_type") != "workflow_end" or i == final_end_idx
            ]

        # Deduplicate job_state events (old multi-session logs re-emit them)
        seen_job_states: set[tuple] = set()
        deduped: List[Dict[str, Any]] = []
        for ev in cleaned:
            if ev.get("event_type") == "job_state":
                if (
                    ev.get("job_instance_id") is not None
                    and ev.get("jobstate_submit_seq") is not None
                ):
                    key = (
                        "row",
                        ev.get("timestamp"),
                        ev.get("job_instance_id"),
                        ev.get("jobstate_submit_seq"),
                    )
                else:
                    key = (
                        "legacy",
                        ev.get("job_id"),
                        ev.get("state"),
                        ev.get("timestamp"),
                    )
                if key in seen_job_states:
                    continue
                seen_job_states.add(key)
            deduped.append(ev)

        # Sort by timestamp so multi-session events replay in chronological
        # order.  Use a stable sort; non-job events (jobs_init, workflow_state,
        # workflow_end) keep their relative position within the same second.
        deduped.sort(key=lambda ev: ev.get("timestamp", 0))

        self._events = deduped

        # Pre-scan for the full job roster (used when no jobs_init event exists)
        self._prescan_jobs = self._prescan_job_roster()

    def _prescan_job_roster(self) -> Dict[int, Dict[str, str]]:
        """Scan all events to discover the complete set of job IDs and metadata."""
        jobs: Dict[int, Dict[str, str]] = {}
        for ev in self._events:
            if ev.get("event_type") == "jobs_init":
                # If a jobs_init event exists, use it and stop scanning
                for j in ev.get("jobs", []):
                    jid = j.get("job_id")
                    if jid is not None:
                        entry: dict = {
                            "exec_job_id": j.get("exec_job_id", ""),
                            "type_desc": j.get("type_desc", "compute"),
                        }
                        if j.get("transformation"):
                            entry["transformation"] = j["transformation"]
                        if j.get("task_argv"):
                            entry["task_argv"] = j["task_argv"]
                        jobs[jid] = entry
                return jobs
            if ev.get("event_type") == "job_state":
                jid = ev.get("job_id")
                if jid is not None and jid not in jobs:
                    jobs[jid] = {
                        "exec_job_id": ev.get("exec_job_id", ""),
                        "type_desc": ev.get("type_desc", "compute"),
                    }
        return jobs

    @property
    def info(self) -> WorkflowInfo:
        assert self._info is not None
        return self._info

    # ── Frame grouping ───────────────────────────────────────────────────────

    def _build_frames(self) -> List[List[Dict[str, Any]]]:
        """Group events into frames by integer-second timestamp."""
        if not self._events:
            return []

        frames: List[List[Dict[str, Any]]] = []
        current_frame: List[Dict[str, Any]] = []
        current_ts: Optional[int] = None

        for ev in self._events:
            ts = int(ev.get("timestamp", 0))
            if current_ts is None or ts == current_ts:
                current_frame.append(ev)
                current_ts = ts
            else:
                if current_frame:
                    frames.append(current_frame)
                current_frame = [ev]
                current_ts = ts

        if current_frame:
            frames.append(current_frame)

        return frames

    # ── Snapshot reconstruction ──────────────────────────────────────────────

    def _reconstruct(
        self,
        frame_events: List[Dict[str, Any]],
        job_state: Dict[int, Dict[str, Any]],
        wf_state: str,
        wf_status: Optional[int],
        wf_start: Optional[float],
        wf_end: Optional[float],
        recent_events: List[Dict],
        frame_ts: float,
    ) -> tuple:
        """Apply a frame's events and return updated state + snapshot."""
        for ev in frame_events:
            etype = ev.get("event_type")

            if etype == "workflow_state":
                wf_state = ev.get("state", wf_state)
                wf_status = ev.get("status")
                if wf_state == "WORKFLOW_STARTED" and wf_start is None:
                    wf_start = ev.get("timestamp")
                elif wf_state == "WORKFLOW_TERMINATED":
                    wf_end = ev.get("timestamp")

            elif etype == "jobs_init":
                # Pre-populate all jobs as UNSUBMITTED for stable total count
                for j in ev.get("jobs", []):
                    jid = j.get("job_id")
                    if jid is not None and jid not in job_state:
                        job_state[jid] = {
                            "exec_job_id": j.get("exec_job_id", ""),
                            "type_desc": j.get("type_desc", "compute"),
                            "raw_state": None,
                            "exitcode": None,
                            "site": None,
                            "submit_time": None,
                            "start_time": None,
                            "end_time": None,
                            "transformation": j.get("transformation"),
                            "task_argv": j.get("task_argv"),
                            "stdout_file": None,
                            "stderr_file": None,
                            "maxrss": None,
                        }

            elif etype == "job_state":
                jid = ev.get("job_id")
                if jid is not None:
                    if jid not in job_state:
                        job_state[jid] = {
                            "exec_job_id": ev.get("exec_job_id", ""),
                            "type_desc": ev.get("type_desc", "compute"),
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
                    js = job_state[jid]
                    state = ev.get("state")
                    js["raw_state"] = state
                    ts = ev.get("timestamp")
                    # Capture runtime metadata when present
                    if ev.get("stdout_file"):
                        js["stdout_file"] = ev["stdout_file"]
                    if ev.get("stderr_file"):
                        js["stderr_file"] = ev["stderr_file"]
                    if ev.get("maxrss") is not None:
                        js["maxrss"] = ev["maxrss"]
                    if ev.get("exitcode") is not None:
                        js["exitcode"] = ev["exitcode"]

                    if state == "SUBMIT" and js["submit_time"] is None:
                        js["submit_time"] = ts
                    elif state == "EXECUTE" and js["start_time"] is None:
                        js["start_time"] = ts
                    elif state in (
                        "JOB_TERMINATED",
                        "JOB_SUCCESS",
                        "JOB_FAILURE",
                    ):
                        js["end_time"] = ts

                    # Add to recent events list
                    recent_events.append(
                        {
                            "exec_job_id": ev.get("exec_job_id"),
                            "type_desc": ev.get("type_desc"),
                            "state": state,
                            "timestamp": ts,
                        }
                    )

            elif etype == "workflow_end":
                wf_state = ev.get("wf_state", wf_state)
                wf_status = ev.get("wf_status", wf_status)
                if ev.get("wf_state") == "WORKFLOW_TERMINATED" and wf_end is None:
                    wf_end = ev.get("timestamp")

        # Use wf_start from header if not yet set from events
        if wf_start is None and self._header_wf_start is not None:
            wf_start = self._header_wf_start

        # Trim recent events to last N
        recent_events[:] = recent_events[-self._events_n :]

        # Build snapshot with frame_ts as poll_time and _now on each JobRecord
        jobs = [
            JobRecord(
                job_id=jid,
                exec_job_id=js["exec_job_id"],
                type_desc=js["type_desc"],
                raw_state=js["raw_state"],
                exitcode=js["exitcode"],
                site=js["site"],
                submit_time=js["submit_time"],
                start_time=js["start_time"],
                end_time=js["end_time"],
                _now=frame_ts,
                transformation=js.get("transformation"),
                task_argv=js.get("task_argv"),
                stdout_file=js.get("stdout_file"),
                stderr_file=js.get("stderr_file"),
                maxrss=js.get("maxrss"),
            )
            for jid, js in sorted(job_state.items())
        ]

        snap = WorkflowSnapshot(
            wf_state=wf_state,
            wf_status=wf_status,
            wf_start=wf_start,
            wf_end=wf_end,
            jobs=jobs,
            # Recent events in reverse order (newest first) to match db.get_recent_events
            recent_events=list(reversed(recent_events)),
            poll_time=frame_ts,
        )

        return snap, wf_state, wf_status, wf_start, wf_end

    # ── Replay loop ──────────────────────────────────────────────────────────

    def run(self, show_all: bool = False, sort_by_activity: bool = True) -> None:
        """Run the replay in the terminal."""
        frames = self._build_frames()
        if not frames:
            print("No events to replay.")
            return

        info = self.info
        console = Console()

        replay_info = {"speed": self._speed}

        # Pre-populate all jobs as UNSUBMITTED for stable total count
        job_state: Dict[int, Dict[str, Any]] = {}
        for jid, jmeta in self._prescan_jobs.items():
            job_state[jid] = {
                "exec_job_id": jmeta["exec_job_id"],
                "type_desc": jmeta["type_desc"],
                "raw_state": None,
                "exitcode": None,
                "site": None,
                "submit_time": None,
                "start_time": None,
                "end_time": None,
                "transformation": jmeta.get("transformation"),
                "task_argv": jmeta.get("task_argv"),
                "stdout_file": None,
                "stderr_file": None,
                "maxrss": None,
            }
        wf_state = "UNKNOWN"
        wf_status: Optional[int] = None
        wf_start: Optional[float] = None
        wf_end: Optional[float] = None
        recent_events: List[Dict] = []
        condor_jobs: Optional[List[Dict]] = None
        condor_history: Optional[List[Dict]] = None
        pool_status: Optional[PoolSummary] = None
        workflow_stats: Optional[WorkflowStats] = None

        with Live(
            console=console,
            screen=True,
            refresh_per_second=4,
            redirect_stderr=False,
        ) as live:
            try:
                for i, frame in enumerate(frames):
                    frame_ts = float(frame[0].get("timestamp", time.time()))

                    # Extract condor data from events in this frame
                    for ev in frame:
                        if ev.get("event_type") == "htcondor_poll":
                            condor_jobs = ev.get("jobs")
                        elif ev.get("event_type") == "htcondor_history":
                            condor_history = ev.get("jobs")
                        elif ev.get("event_type") == "pool_status":
                            pool_status = PoolSummary.from_dict(ev.get("pool", {}))
                        elif ev.get("event_type") == "workflow_stats":
                            workflow_stats = WorkflowStats.from_dict(
                                ev.get("stats", {})
                            )

                    snap, wf_state, wf_status, wf_start, wf_end = self._reconstruct(
                        frame,
                        job_state,
                        wf_state,
                        wf_status,
                        wf_start,
                        wf_end,
                        recent_events,
                        frame_ts,
                    )

                    layout = build_layout(
                        info,
                        snap,
                        show_all,
                        condor_jobs,
                        self._events_n,
                        frame_ts,
                        replay_info=replay_info,
                        condor_history=condor_history,
                        pool_status=pool_status,
                        sort_by_activity=sort_by_activity,
                    )
                    live.update(layout)

                    # Sleep between frames
                    if i < len(frames) - 1:
                        next_ts = frames[i + 1][0].get("timestamp", frame_ts)
                        delta = max(0, float(next_ts) - frame_ts)
                        if self._speed > 0:
                            time.sleep(delta / self._speed)

                # Hold final frame for 2 seconds
                time.sleep(2.0)

            except KeyboardInterrupt:
                pass

        # Final summary — reuse the same stats display as live mode
        if workflow_stats is None:
            workflow_stats = compute_workflow_stats(
                snap,
                condor_history=condor_history,
                pool_status=pool_status,
            )
        _print_final_summary(console, snap, stats=workflow_stats)
