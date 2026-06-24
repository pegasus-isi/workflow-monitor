"""Query the Pegasus stampede SQLite database for workflow monitoring data."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


# ─── State helpers ────────────────────────────────────────────────────────────

# Map raw pegasus jobstate -> simplified display category
_STATE_MAP: Dict[str, str] = {
    "POST_SCRIPT_SUCCESS": "SUCCESS",
    "JOB_SUCCESS": "SUCCESS",
    "POST_SCRIPT_FAILURE": "FAILED",
    "POST_SCRIPT_FAILED": "FAILED",
    "JOB_FAILURE": "FAILED",
    "JOB_FAILED": "FAILED",
    "EXECUTE": "RUNNING",
    "SUBMIT": "QUEUED",
    "PRE_SCRIPT_STARTED": "PRE",
    "PRE_SCRIPT_SUCCESS": "PRE",
    "POST_SCRIPT_STARTED": "POST",
    "POST_SCRIPT_TERMINATED": "POST",
    "JOB_TERMINATED": "DONE",
    "JOB_HELD": "HELD",
    "JOB_EVICTED": "HELD",
}

# Rich color per display state
STATE_STYLE: Dict[str, str] = {
    "SUCCESS": "bold green",
    "FAILED": "bold red",
    "RUNNING": "bold cyan",
    "QUEUED": "yellow",
    "PRE": "blue",
    "POST": "blue",
    "HELD": "magenta",
    "DONE": "green",
    "UNKNOWN": "dim",
}

# Job type labels for display
JOB_TYPE_LABEL: Dict[str, str] = {
    "compute": "compute",
    "stage-in-tx": "stage-in",
    "stage-out-tx": "stage-out",
    "create-dir": "dir-create",
    "stage-worker": "stage-worker",
    "cleanup": "cleanup",
    "registration": "register",
}


def display_state(raw_state: Optional[str]) -> str:
    if raw_state is None:
        return "UNSUBMITTED"
    return _STATE_MAP.get(raw_state, raw_state)


def fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0:
        return "-"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s"


def fmt_timestamp(ts: Optional[float]) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def real_exitcode(raw: Optional[int]) -> Optional[int]:
    """Decode a raw POSIX ``wait()`` status into a conventional exit code.

    The stampede ``job_instance.exitcode`` column stores the raw wait status
    (see DATA_SOURCES.md), not the final code:

    * Normal termination puts the exit status in the high byte, so ``exit(N)``
      is stored as ``N << 8`` (``exit(1)`` -> 256, ``exit(127)`` -> 32512).
    * Termination by a signal puts the signal number in the low 7 bits, with
      bit ``0x80`` set when a core was dumped — so SIGKILL is 9 or 137 and
      SIGSEGV is 11 or 139.

    We return the exit status for a normal exit and the shell convention
    ``128 + signal`` for a signal kill, matching the codes the failure
    diagnostics key on (137 = OOM/SIGKILL, 139 = SIGSEGV). The previous
    ``raw >> 8`` shortcut mis-decoded core-dump statuses (137/139 -> 0) and
    reported bare signal numbers for kills without a core. ``None`` passes
    through; a negative sentinel is returned unchanged.
    """
    if raw is None:
        return None
    if raw < 0:
        return raw
    sig = raw & 0x7F
    # sig == 0 is WIFEXITED; sig == 0x7F is WIFSTOPPED (never seen for a reaped
    # job) — decode both from the high byte rather than as a phantom signal.
    if sig in (0, 0x7F):
        return (raw >> 8) & 0xFF
    return 128 + sig


def fmt_memory(maxrss_kb: Optional[int]) -> str:
    """Format maxrss (KB) as human-readable."""
    if maxrss_kb is None:
        return "-"
    mb = maxrss_kb / 1024.0
    if mb < 1024:
        return f"{mb:.1f}M"
    return f"{mb / 1024:.2f}G"


# ─── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class JobRecord:
    job_id: int
    exec_job_id: str
    type_desc: str
    raw_state: Optional[str]
    exitcode: Optional[int]
    site: Optional[str]
    submit_time: Optional[float]
    start_time: Optional[float]
    end_time: Optional[float]
    _now: Optional[float] = field(default=None, repr=False)
    # Enriched metadata
    transformation: Optional[str] = None
    task_argv: Optional[str] = None
    stdout_file: Optional[str] = None
    stderr_file: Optional[str] = None
    maxrss: Optional[int] = None  # peak RSS in KB

    @property
    def disp_state(self) -> str:
        return display_state(self.raw_state)

    @property
    def duration(self) -> Optional[float]:
        if self.start_time is not None and self.end_time is not None:
            return self.end_time - self.start_time
        if self.start_time is not None and self.disp_state == "RUNNING":
            now = self._now if self._now is not None else datetime.now().timestamp()
            return now - self.start_time
        return None

    @property
    def is_compute(self) -> bool:
        return self.type_desc == "compute"

    @property
    def short_name(self) -> str:
        """Strip the run-specific ID suffix for cleaner display."""
        name = self.exec_job_id
        # e.g. preprocess_ID0000001 -> preprocess_ID0000001 (keep as-is)
        return name

    @property
    def display_name(self) -> str:
        """Prefer transformation name for compute jobs, else exec_job_id."""
        if self.transformation and self.is_compute:
            return self.transformation
        return self.exec_job_id


@dataclass
class WorkflowSnapshot:
    wf_state: str  # WORKFLOW_STARTED | WORKFLOW_TERMINATED | UNKNOWN
    wf_status: Optional[int]  # exit status (0=success)
    wf_start: Optional[float]
    wf_end: Optional[float]
    jobs: List[JobRecord]
    recent_events: List[Dict]
    poll_time: float = field(default_factory=lambda: datetime.now().timestamp())

    @property
    def is_running(self) -> bool:
        return self.wf_state == "WORKFLOW_STARTED"

    @property
    def is_complete(self) -> bool:
        return self.wf_state == "WORKFLOW_TERMINATED"

    @property
    def succeeded(self) -> bool:
        return self.is_complete and self.wf_status == 0

    @property
    def failed(self) -> bool:
        return self.is_complete and self.wf_status != 0

    @property
    def elapsed(self) -> Optional[float]:
        if self.wf_start is None:
            return None
        end = self.wf_end if self.wf_end is not None else self.poll_time
        return end - self.wf_start

    def job_counts(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for job in self.jobs:
            s = job.disp_state
            counts[s] = counts.get(s, 0) + 1
        return counts

    def compute_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if j.is_compute]

    def infra_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if not j.is_compute]

    def total_jobs(self) -> int:
        return len(self.jobs)

    def done_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state == "SUCCESS")

    def failed_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state == "FAILED")

    def held_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state == "HELD")

    def held_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if j.disp_state == "HELD"]

    def failed_jobs(self) -> List[JobRecord]:
        return [j for j in self.jobs if j.disp_state == "FAILED"]

    def running_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state == "RUNNING")

    def queued_count(self) -> int:
        return sum(1 for j in self.jobs if j.disp_state in ("QUEUED", "PRE", "POST"))

    def progress_pct(self) -> float:
        total = self.total_jobs()
        if total == 0:
            return 0.0
        return 100.0 * self.done_count() / total


# ─── Database class ───────────────────────────────────────────────────────────


class StampedeDB:
    """Read-only interface to the Pegasus stampede SQLite database."""

    def __init__(self, db_path: Path, wf_uuid: Optional[str] = None):
        self.db_path = db_path
        self._wf_uuid = wf_uuid
        self._wf_id: Optional[int] = None
        self._conn: Optional[sqlite3.Connection] = None

    # ── Connection management ─────────────────────────────────────────────────

    def connect(self) -> None:
        uri = f"file:{self.db_path}?mode=ro"
        self._conn = sqlite3.connect(
            uri, uri=True, timeout=5.0, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._resolve_wf_id()

    def _resolve_wf_id(self) -> None:
        """Look up the integer wf_id for the configured wf_uuid."""
        if self._wf_uuid is None or self._conn is None:
            return
        cur = self._conn.cursor()
        cur.execute("SELECT wf_id FROM workflow WHERE wf_uuid = ?", (self._wf_uuid,))
        row = cur.fetchone()
        if row:
            self._wf_id = row["wf_id"]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "StampedeDB":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _conn_or_raise(self) -> sqlite3.Connection:
        if self._conn is None:
            self.connect()
        return self._conn  # type: ignore[return-value]

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_workflow_state(self) -> Dict:
        cur = self._conn_or_raise().cursor()
        if self._wf_id is not None:
            cur.execute(
                """
                SELECT state, timestamp, status
                FROM workflowstate
                WHERE wf_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (self._wf_id,),
            )
        else:
            cur.execute(
                """
                SELECT state, timestamp, status
                FROM workflowstate
                ORDER BY timestamp DESC
                LIMIT 1
                """
            )
        row = cur.fetchone()
        if not row:
            return {"state": "UNKNOWN", "timestamp": None, "status": None}
        return dict(row)

    def get_workflow_times(self) -> Dict:
        cur = self._conn_or_raise().cursor()
        if self._wf_id is not None:
            cur.execute(
                "SELECT state, timestamp FROM workflowstate WHERE wf_id = ? ORDER BY timestamp",
                (self._wf_id,),
            )
        else:
            cur.execute("SELECT state, timestamp FROM workflowstate ORDER BY timestamp")
        start = end = None
        for row in cur.fetchall():
            if row["state"] == "WORKFLOW_STARTED":
                start = row["timestamp"]
            elif row["state"] == "WORKFLOW_TERMINATED":
                end = row["timestamp"]
        return {"start": start, "end": end}

    def get_jobs(self) -> List[JobRecord]:
        """Return all jobs with their latest observed state and metadata."""
        cur = self._conn_or_raise().cursor()
        cur.execute(
            """
            WITH latest_ji AS (
                SELECT job_id, job_instance_id, exitcode, site,
                       stdout_file, stderr_file
                FROM job_instance
                WHERE (job_id, job_submit_seq) IN (
                    SELECT job_id, MAX(job_submit_seq)
                    FROM job_instance
                    GROUP BY job_id
                )
            )
            SELECT
                j.job_id,
                j.exec_job_id,
                j.type_desc,
                (
                    SELECT js.state
                    FROM jobstate js
                    WHERE js.job_instance_id = lji.job_instance_id
                    ORDER BY js.timestamp DESC, js.jobstate_submit_seq DESC
                    LIMIT 1
                ) AS current_state,
                lji.exitcode,
                lji.site,
                (
                    SELECT MIN(js.timestamp)
                    FROM jobstate js
                    JOIN job_instance ji2 ON js.job_instance_id = ji2.job_instance_id
                    WHERE ji2.job_id = j.job_id AND js.state = 'SUBMIT'
                ) AS submit_time,
                (
                    SELECT MIN(js.timestamp)
                    FROM jobstate js
                    JOIN job_instance ji2 ON js.job_instance_id = ji2.job_instance_id
                    WHERE ji2.job_id = j.job_id AND js.state = 'EXECUTE'
                ) AS start_time,
                (
                    SELECT MAX(js.timestamp)
                    FROM jobstate js
                    JOIN job_instance ji2 ON js.job_instance_id = ji2.job_instance_id
                    WHERE ji2.job_id = j.job_id
                      AND js.state IN ('JOB_TERMINATED', 'JOB_SUCCESS', 'JOB_FAILURE')
                ) AS end_time,
                t.transformation,
                t.argv AS task_argv,
                lji.stdout_file,
                lji.stderr_file,
                (SELECT MAX(inv.maxrss) FROM invocation inv
                 WHERE inv.job_instance_id = lji.job_instance_id
                   AND inv.task_submit_seq >= 0) AS maxrss
            FROM job j
            LEFT JOIN latest_ji lji ON j.job_id = lji.job_id
            LEFT JOIN task t ON t.job_id = j.job_id
            WHERE (? IS NULL OR j.wf_id = ?)
            ORDER BY j.job_id
            """,
            (self._wf_id, self._wf_id),
        )
        jobs = []
        for row in cur.fetchall():
            jobs.append(
                JobRecord(
                    job_id=row["job_id"],
                    exec_job_id=row["exec_job_id"],
                    type_desc=row["type_desc"],
                    raw_state=row["current_state"],
                    exitcode=row["exitcode"],
                    site=row["site"],
                    submit_time=row["submit_time"],
                    start_time=row["start_time"],
                    end_time=row["end_time"],
                    transformation=row["transformation"],
                    task_argv=row["task_argv"],
                    stdout_file=row["stdout_file"],
                    stderr_file=row["stderr_file"],
                    maxrss=row["maxrss"],
                )
            )
        return jobs

    def get_events_since(self, after_ts: float) -> List[Dict]:
        """Return all job-state transitions after *after_ts*, ordered ASC."""
        cur = self._conn_or_raise().cursor()
        cur.execute(
            """
            SELECT
                j.exec_job_id,
                j.type_desc,
                js.state,
                js.timestamp,
                j.job_id,
                ji.exitcode,
                ji.stdout_file,
                ji.stderr_file,
                (SELECT MAX(inv.maxrss) FROM invocation inv
                 WHERE inv.job_instance_id = ji.job_instance_id
                   AND inv.task_submit_seq >= 0) AS maxrss
            FROM job j
            JOIN job_instance ji ON j.job_id = ji.job_id
            JOIN jobstate js ON ji.job_instance_id = js.job_instance_id
            WHERE js.timestamp > ?
              AND (? IS NULL OR j.wf_id = ?)
            ORDER BY js.timestamp ASC, js.jobstate_submit_seq ASC
            """,
            (after_ts, self._wf_id, self._wf_id),
        )
        return [dict(row) for row in cur.fetchall()]

    def get_recent_events(self, limit: int = 20) -> List[Dict]:
        """Return the *limit* most recent job-state events."""
        cur = self._conn_or_raise().cursor()
        cur.execute(
            """
            SELECT
                j.exec_job_id,
                j.type_desc,
                js.state,
                js.timestamp
            FROM job j
            JOIN job_instance ji ON j.job_id = ji.job_id
            JOIN jobstate js ON ji.job_instance_id = js.job_instance_id
            WHERE (? IS NULL OR j.wf_id = ?)
            ORDER BY js.timestamp DESC, js.jobstate_submit_seq DESC
            LIMIT ?
            """,
            (self._wf_id, self._wf_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]

    def snapshot(self) -> WorkflowSnapshot:
        """Capture a full workflow snapshot in one call."""
        try:
            wf_row = self.get_workflow_state()
            wf_times = self.get_workflow_times()
            jobs = self.get_jobs()
            events = self.get_recent_events()
        except sqlite3.OperationalError:
            # DB may be locked momentarily; return a minimal snapshot
            wf_row = {"state": "UNKNOWN", "timestamp": None, "status": None}
            wf_times = {"start": None, "end": None}
            jobs = []
            events = []

        return WorkflowSnapshot(
            wf_state=wf_row.get("state", "UNKNOWN"),
            wf_status=wf_row.get("status"),
            wf_start=wf_times.get("start"),
            wf_end=wf_times.get("end"),
            jobs=jobs,
            recent_events=events,
        )
