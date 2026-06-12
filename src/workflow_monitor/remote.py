"""SSH client engine for remote workflow monitoring.

Periodically fetches a JSONL event log from a remote server via SSH and
displays it in the Rich TUI, following new events as they arrive.

Usage (from cli.py):
    workflow-monitor --remote user@host:/path/to/workflow-events.jsonl
"""

from __future__ import annotations

import json
import shlex
import subprocess
import tempfile
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


_REMOTE_TRUNCATED_EXIT = 75


def _parse_remote_spec(spec: str) -> tuple[str, str]:
    """Parse 'user@host:/path/to/file' into (ssh_target, remote_path).

    Handles IPv6 addresses in square brackets:
        user@[2001:db8::1]:/path/to/file

    Returns (ssh_host_part, remote_path) where ssh_host_part is
    everything before the path-separating colon.
    """
    # IPv6 in brackets: find the closing ']' then expect ':' after it
    if "[" in spec:
        bracket_close = spec.find("]")
        if bracket_close == -1:
            raise ValueError(
                f"Invalid remote spec: {spec!r}\n"
                "Unclosed bracket in IPv6 address.\n"
                "Expected format: user@[IPv6]:/path/to/workflow-events.jsonl"
            )
        # The colon separating host from path comes after ']'
        colon_pos = spec.find(":", bracket_close + 1)
        if colon_pos == -1:
            raise ValueError(
                f"Invalid remote spec: {spec!r}\n"
                "Expected format: user@[IPv6]:/path/to/workflow-events.jsonl"
            )
        host_part = spec[:colon_pos]
        remote_path = spec[colon_pos + 1 :]
    else:
        if ":" not in spec:
            raise ValueError(
                f"Invalid remote spec: {spec!r}\n"
                "Expected format: user@host:/path/to/workflow-events.jsonl"
            )
        host_part, remote_path = spec.split(":", 1)

    if not host_part or not remote_path:
        raise ValueError(
            f"Invalid remote spec: {spec!r}\n"
            "Expected format: user@host:/path/to/workflow-events.jsonl"
        )
    return host_part, remote_path


def _build_ssh_base(
    ssh_config: Optional[str] = None,
    ssh_identity: Optional[str] = None,
) -> List[str]:
    """Build the base SSH command as a list of arguments."""
    parts = ["ssh"]
    if ssh_config:
        parts.extend(["-F", ssh_config])
    if ssh_identity:
        parts.extend(["-i", ssh_identity])
    return parts


def _strip_brackets(host: str) -> str:
    """Strip square brackets from an IPv6 host for SSH.

    Remote specs use brackets for IPv6 (user@[ipv6]:path) but SSH
    expects the raw address (user@ipv6).
    """
    # Extract user@ prefix if present
    if "@" in host:
        user, addr = host.rsplit("@", 1)
        addr = addr.strip("[]")
        return f"{user}@{addr}"
    return host.strip("[]")


def _fetch_file(
    host: str,
    remote_path: str,
    local_path: Path,
    ssh_base: List[str],
    byte_offset: int = 0,
) -> tuple[bool, str, int]:
    """Fetch a remote file via SSH, streaming directly to disk.

    When *byte_offset* > 0, uses ``tail -c +<offset>`` to download only
    new bytes appended since the last fetch.  Falls back to a full
    download when *byte_offset* is 0 (first fetch) or if the remote file
    shrank (e.g. server restarted with a new log).

    Returns (success, stderr_text, new_byte_offset).
    """
    ssh_host = _strip_brackets(host)
    quoted_remote_path = shlex.quote(remote_path)

    if byte_offset > 0:
        # tail -c +N outputs bytes starting at byte N (1-indexed).  Check
        # remote size first because tail beyond EOF succeeds with no output,
        # which would otherwise hide a truncated/recreated event log forever.
        remote_cmd = (
            f"size=$(wc -c < {quoted_remote_path}) || exit 2; "
            f'if [ "$size" -lt {byte_offset} ]; then '
            f"exit {_REMOTE_TRUNCATED_EXIT}; "
            "fi; "
            f"tail -c +{byte_offset + 1} -- {quoted_remote_path}"
        )
        write_mode = "ab"
    else:
        remote_cmd = f"cat -- {quoted_remote_path}"
        write_mode = "wb"

    cmd = ssh_base + [ssh_host, remote_cmd]
    try:
        with open(local_path, write_mode) as fh:
            result = subprocess.run(
                cmd,
                stdout=fh,
                stderr=subprocess.PIPE,
                timeout=60,
            )
        if result.returncode == 0:
            new_size = local_path.stat().st_size
            return True, "", new_size
        if result.returncode == _REMOTE_TRUNCATED_EXIT:
            return True, "", 0
        # On failure with incremental fetch, the local file may have
        # garbage appended — truncate back to the previous offset.
        if byte_offset > 0:
            with open(local_path, "ab") as fh:
                fh.truncate(byte_offset)
        return False, result.stderr.decode(errors="replace").strip(), byte_offset
    except subprocess.TimeoutExpired:
        if byte_offset > 0:
            with open(local_path, "ab") as fh:
                fh.truncate(byte_offset)
        return False, "ssh timed out", byte_offset
    except FileNotFoundError:
        return False, "ssh not found in PATH", byte_offset


class RemoteEngine:
    """Rsyncs a remote JSONL log and displays it in the TUI."""

    def __init__(
        self,
        remote_spec: str,
        sync_interval: float = 5.0,
        events_n: int = 15,
        ssh_config: Optional[str] = None,
        ssh_identity: Optional[str] = None,
    ) -> None:
        self._remote_spec = remote_spec
        self._sync_interval = sync_interval
        self._events_n = events_n

        # Parse and validate remote spec
        self._host, self._remote_path = _parse_remote_spec(remote_spec)

        # Build base SSH command
        self._ssh_base = _build_ssh_base(ssh_config, ssh_identity)

        # Local temp file for the synced JSONL
        self._tmpdir = tempfile.mkdtemp(prefix="wfmon-remote-")
        filename = Path(self._remote_path).name
        self._local_path = Path(self._tmpdir) / filename

        # Incremental fetch state (byte offset into the remote file)
        self._remote_offset: int = 0

        # Diagnostic sidecar — fetched best-effort, may not exist
        self._diag_remote_path: str = str(
            Path(self._remote_path).parent / "diagnostics-events.jsonl"
        )
        self._diag_local_path: Path = Path(self._tmpdir) / "diagnostics-events.jsonl"
        self._diag_remote_offset: int = 0
        self._diag_processed_lines: int = 0
        self._diag_active_alerts: List[Dict[str, Any]] = []
        self._diag_seen: bool = False

        # Replay state
        self._info: Optional[WorkflowInfo] = None
        self._job_state: Dict[int, Dict[str, Any]] = {}
        self._wf_state: str = "UNKNOWN"
        self._wf_status: Optional[int] = None
        self._wf_start: Optional[float] = None
        self._wf_end: Optional[float] = None
        self._recent_events: List[Dict] = []
        self._header_wf_start: Optional[float] = None
        self._prescan_jobs: Dict[int, Dict[str, str]] = {}
        self._processed_lines: int = 0
        self._workflow_complete: bool = False
        self._condor_jobs: Optional[List[Dict]] = None
        self._condor_history: Optional[List[Dict]] = None
        self._pool_status: Optional[PoolSummary] = None
        self._workflow_stats: Optional[WorkflowStats] = None
        self._pending_workflow_end: Optional[Dict[str, Any]] = None

    def _do_sync(self) -> tuple[bool, str]:
        """Fetch the remote file via SSH (incremental after first sync)."""
        ok, err, new_offset = _fetch_file(
            self._host,
            self._remote_path,
            self._local_path,
            self._ssh_base,
            byte_offset=self._remote_offset,
        )
        if ok:
            # If the file shrank (server restarted), re-fetch from scratch
            if new_offset < self._remote_offset:
                self._remote_offset = 0
                self._processed_lines = 0
                return self._do_sync()
            self._remote_offset = new_offset
        return ok, err

    def _sync_diagnostics(self) -> None:
        """Best-effort fetch of the diagnostics sidecar. Failures are silent."""
        ok, _err, new_offset = _fetch_file(
            self._host,
            self._diag_remote_path,
            self._diag_local_path,
            self._ssh_base,
            byte_offset=self._diag_remote_offset,
        )
        if not ok:
            return
        self._diag_seen = True
        if new_offset < self._diag_remote_offset:
            # File shrank — reset and refetch from scratch
            self._diag_remote_offset = 0
            self._diag_processed_lines = 0
            self._diag_active_alerts.clear()
            ok, _err, new_offset = _fetch_file(
                self._host,
                self._diag_remote_path,
                self._diag_local_path,
                self._ssh_base,
                byte_offset=0,
            )
            if not ok:
                return
        self._diag_remote_offset = new_offset

    def _load_new_diag_events(self) -> None:
        """Read new lines from the local diag sidecar and update active alerts."""
        if not self._diag_local_path.exists():
            return
        try:
            with open(self._diag_local_path) as fh:
                total = 0
                for i, line in enumerate(fh):
                    total = i + 1
                    if i < self._diag_processed_lines:
                        continue
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    et = ev.get("event_type")
                    if et == "diag_start":
                        # New daemon session — reset alert state
                        self._diag_active_alerts.clear()
                    elif et == "stall_resolved":
                        self._diag_active_alerts.clear()
                    elif et in (
                        "stall_detected",
                        "idle_diagnosis",
                        "hold_diagnosis",
                        "failure_diagnosis",
                    ):
                        self._diag_active_alerts.append(ev)
                self._diag_processed_lines = total
        except OSError:
            pass

    def _load_new_events(self) -> List[Dict[str, Any]]:
        """Read any new lines from the local JSONL file since last read."""
        if not self._local_path.exists():
            return []

        new_events: List[Dict[str, Any]] = []
        try:
            with open(self._local_path) as fh:
                total_lines = 0
                for i, line in enumerate(fh):
                    total_lines = i + 1
                    if i < self._processed_lines:
                        continue
                    line = line.strip()
                    if line:
                        new_events.append(json.loads(line))
                self._processed_lines = total_lines
        except (json.JSONDecodeError, OSError):
            pass

        return new_events

    def _process_header(self, ev: Dict[str, Any]) -> None:
        """Extract WorkflowInfo from a workflow_start event."""
        self._info = WorkflowInfo(
            wf_uuid=ev.get("wf_uuid", "unknown"),
            root_wf_uuid=ev.get("wf_uuid", "unknown"),
            dax_label=ev.get("dax_label", "unknown"),
            submit_dir=Path(ev.get("submit_dir", ".")),
            user=ev.get("user", "unknown"),
            planner_version=ev.get("planner_version", "?"),
            dag_file="remote.dag",
            condor_log="remote.log",
            timestamp=str(ev.get("timestamp", "")),
            basedir=Path(ev.get("submit_dir", ".")),
        )
        self._header_wf_start = ev.get("wf_start")

    def _apply_event(self, ev: Dict[str, Any]) -> None:
        """Apply a single event to the running state."""
        etype = ev.get("event_type")

        if etype == "workflow_start":
            if self._info is None:
                self._process_header(ev)
            return

        if etype == "workflow_state":
            self._wf_state = ev.get("state", self._wf_state)
            self._wf_status = ev.get("status")
            if self._wf_state == "WORKFLOW_STARTED" and self._wf_start is None:
                # Prefer actual DB start time over logger wall clock
                self._wf_start = ev.get("wf_start") or ev.get("timestamp")
            elif self._wf_state == "WORKFLOW_TERMINATED":
                # Prefer actual DB end time over logger wall clock
                self._wf_end = ev.get("wf_end") or ev.get("timestamp")

        elif etype == "jobs_init":
            for j in ev.get("jobs", []):
                jid = j.get("job_id")
                if jid is not None and jid not in self._job_state:
                    self._job_state[jid] = {
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
                if jid not in self._job_state:
                    self._job_state[jid] = {
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
                js = self._job_state[jid]
                state = ev.get("state")
                js["raw_state"] = state
                ts = ev.get("timestamp")
                if ev.get("exitcode") is not None:
                    js["exitcode"] = ev["exitcode"]
                # Capture runtime metadata when present
                if ev.get("stdout_file"):
                    js["stdout_file"] = ev["stdout_file"]
                if ev.get("stderr_file"):
                    js["stderr_file"] = ev["stderr_file"]
                if ev.get("maxrss") is not None:
                    js["maxrss"] = ev["maxrss"]

                if state == "SUBMIT" and js["submit_time"] is None:
                    js["submit_time"] = ts
                elif state == "EXECUTE" and js["start_time"] is None:
                    js["start_time"] = ts
                elif state in ("JOB_TERMINATED", "JOB_SUCCESS", "JOB_FAILURE"):
                    js["end_time"] = ts

                self._recent_events.append(
                    {
                        "exec_job_id": ev.get("exec_job_id"),
                        "type_desc": ev.get("type_desc"),
                        "state": state,
                        "timestamp": ts,
                    }
                )

        elif etype == "htcondor_poll":
            self._condor_jobs = ev.get("jobs")

        elif etype == "htcondor_history":
            self._condor_history = ev.get("jobs")

        elif etype == "pool_status":
            self._pool_status = PoolSummary.from_dict(ev.get("pool", {}))

        elif etype == "workflow_stats":
            self._workflow_stats = WorkflowStats.from_dict(ev.get("stats", {}))

        elif etype == "workflow_end":
            # Defer marking complete — a resumed server may append events
            # after a prior workflow_end.  We mark complete only in
            # _finalize_batch() after all current events are processed.
            self._pending_workflow_end = ev
            return  # skip the clearing logic below

        # Any non-end event after a workflow_end means the server resumed
        if self._pending_workflow_end is not None and etype in (
            "job_state",
            "workflow_state",
            "htcondor_poll",
        ):
            self._pending_workflow_end = None
            self._workflow_complete = False

        # Use header wf_start if not yet set from events
        if self._wf_start is None and self._header_wf_start is not None:
            self._wf_start = self._header_wf_start

        # Trim recent events
        self._recent_events = self._recent_events[-self._events_n :]

    def _finalize_batch(self) -> None:
        """After processing all events from a sync, resolve any pending
        workflow_end.  If a workflow_end was followed by newer events
        (server resume), it was already cleared by those events."""
        ev = self._pending_workflow_end
        if ev is None:
            return
        self._wf_state = ev.get("wf_state", self._wf_state)
        self._wf_status = ev.get("wf_status", self._wf_status)
        if ev.get("wf_state") == "WORKFLOW_TERMINATED":
            # Prefer the actual DB end time over the logger's wall clock
            wf_end = ev.get("wf_end") or ev.get("timestamp")
            if self._wf_end is None:
                self._wf_end = wf_end
            self._workflow_complete = True
        self._pending_workflow_end = None

    def _build_snapshot(self) -> WorkflowSnapshot:
        """Build a WorkflowSnapshot from the current accumulated state."""
        now = time.time()
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
                _now=now,
                transformation=js.get("transformation"),
                task_argv=js.get("task_argv"),
                stdout_file=js.get("stdout_file"),
                stderr_file=js.get("stderr_file"),
                maxrss=js.get("maxrss"),
            )
            for jid, js in sorted(self._job_state.items())
        ]

        return WorkflowSnapshot(
            wf_state=self._wf_state,
            wf_status=self._wf_status,
            wf_start=self._wf_start,
            wf_end=self._wf_end,
            jobs=jobs,
            recent_events=list(reversed(self._recent_events)),
            poll_time=now,
        )

    def run(
        self, show_all: bool = False, once: bool = False, sort_by_activity: bool = True
    ) -> None:
        """Run the remote monitoring TUI."""
        console = Console()

        console.print(f"[dim]Connecting to {self._host}...[/dim]")

        # Initial sync — must succeed to get header
        ok, err = self._do_sync()
        if not ok:
            console.print(
                f"[bold red]Failed to fetch from {self._remote_spec}[/bold red]"
            )
            if err:
                console.print(f"[red]{err}[/red]")
            console.print(
                "[dim]Check that SSH access is configured and the file exists.[/dim]"
            )
            return

        # Load initial events
        events = self._load_new_events()
        if not events:
            console.print("[bold red]No events found in remote log file.[/bold red]")
            return

        for ev in events:
            self._apply_event(ev)
        self._finalize_batch()

        # Best-effort initial diagnostics sync
        self._sync_diagnostics()
        self._load_new_diag_events()

        if self._info is None:
            console.print(
                "[bold red]No workflow_start header found in remote log.[/bold red]"
            )
            return

        info = self._info
        remote_info = {"host": self._host}

        snap = self._build_snapshot()

        if once:
            from .display import (
                _make_header,
                _make_status_bar,
                _make_diagnostics_panel,
                _make_job_table,
                _make_infra_summary,
                _make_pool_panel,
                _make_events_panel,
            )

            console.print(
                _make_header(info, snap, snap.poll_time, remote_info=remote_info)
            )
            console.print(_make_status_bar(snap))
            if snap.held_count() > 0 or snap.failed_count() > 0:
                console.print(
                    _make_diagnostics_panel(snap, condor_jobs=self._condor_jobs)
                )
            console.print(
                _make_job_table(
                    snap,
                    show_all=show_all,
                    condor_jobs=self._condor_jobs,
                    condor_history=self._condor_history,
                    sort_by_activity=sort_by_activity,
                )
            )
            if snap.infra_jobs():
                console.print(_make_infra_summary(snap))
            if self._pool_status is not None:
                console.print(_make_pool_panel(self._pool_status))
            console.print(_make_events_panel(snap, n=self._events_n))
            stats = self._workflow_stats or compute_workflow_stats(
                snap,
                condor_history=self._condor_history,
                pool_status=self._pool_status,
            )
            _print_final_summary(
                console, snap, condor_jobs=self._condor_jobs, stats=stats
            )
            self._cleanup()
            return

        console.print(
            f"[dim]Monitoring {info.dax_label} via SSH "
            f"(sync every {self._sync_interval:.0f}s)[/dim]"
        )

        with Live(
            console=console,
            screen=True,
            refresh_per_second=2,
            redirect_stderr=False,
        ) as live:
            try:
                last_sync = time.time()

                while True:
                    # Update display with current state
                    snap = self._build_snapshot()
                    layout = build_layout(
                        info,
                        snap,
                        show_all,
                        self._condor_jobs,
                        self._events_n,
                        snap.poll_time,
                        remote_info=remote_info,
                        condor_history=self._condor_history,
                        pool_status=self._pool_status,
                        diag_alerts=list(self._diag_active_alerts)
                        if self._diag_active_alerts
                        else None,
                        diag_path=(
                            f"{self._host}:{self._diag_remote_path}"
                            if self._diag_seen
                            else None
                        ),
                        sort_by_activity=sort_by_activity,
                    )
                    live.update(layout)

                    if self._workflow_complete:
                        # Hold final state briefly then exit
                        time.sleep(2.0)
                        break

                    # Sleep a short interval, then check if sync is due
                    time.sleep(1.0)

                    if time.time() - last_sync >= self._sync_interval:
                        self._do_sync()  # ignore errors during live sync
                        new_events = self._load_new_events()
                        for ev in new_events:
                            self._apply_event(ev)
                        self._finalize_batch()
                        # Best-effort diagnostics sidecar fetch
                        self._sync_diagnostics()
                        self._load_new_diag_events()
                        last_sync = time.time()

            except KeyboardInterrupt:
                pass

        # Final summary — use unified stats display
        stats = self._workflow_stats or compute_workflow_stats(
            snap,
            condor_history=self._condor_history,
            pool_status=self._pool_status,
        )
        _print_final_summary(console, snap, condor_jobs=self._condor_jobs, stats=stats)

        self._cleanup()

    def _cleanup(self) -> None:
        """Remove temporary files."""
        try:
            self._local_path.unlink(missing_ok=True)
            self._diag_local_path.unlink(missing_ok=True)
            Path(self._tmpdir).rmdir()
        except OSError:
            pass
