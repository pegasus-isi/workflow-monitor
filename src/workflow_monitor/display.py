"""Rich-based terminal dashboard for live workflow monitoring."""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .braindump import WorkflowInfo
from .db import (
    STATE_STYLE,
    JOB_TYPE_LABEL,
    StampedeDB,
    WorkflowSnapshot,
    fmt_duration,
    fmt_memory,
    fmt_timestamp,
    real_exitcode,
)
from .diagnostics import collect_diagnostics
from .event_log import EventLogger
from .stats import WorkflowStats, compute_workflow_stats
from .htcondor_poll import (
    CondorBackoff,
    query_queue,
    query_history,
    query_slots,
    format_job_status,
    format_resources,
    format_transfer,
    format_efficiency,
    format_disk_usage,
    queue_wait_seconds,
    cpu_efficiency,
    memory_efficiency,
    PoolSummary,
)


# ─── Workflow state styling ───────────────────────────────────────────────────


def _wf_state_text(snap: WorkflowSnapshot) -> Text:
    if snap.is_running:
        return Text("● RUNNING", style="bold cyan")
    if snap.succeeded:
        return Text("✔ SUCCESS", style="bold green")
    if snap.failed:
        return Text("✖ FAILED", style="bold red")
    return Text(f"◌ {snap.wf_state}", style="dim")


# ─── Header panel ─────────────────────────────────────────────────────────────


def _make_header(
    info: WorkflowInfo,
    snap: WorkflowSnapshot,
    refresh_ts: float,
    replay_info: Optional[dict] = None,
    remote_info: Optional[dict] = None,
    stall_active: bool = False,
    condor_offline: bool = False,
) -> Panel:
    grid = Table.grid(expand=True, padding=(0, 0))
    grid.add_column(ratio=1)
    grid.add_column(justify="right", no_wrap=True)

    # Row 1: title + mode badge
    title = Text()
    title.append("Pegasus Workflow Monitor", style="bold white")
    if replay_info is not None:
        title.append("  ")
        title.append(" REPLAY ", style="bold white on dark_orange3")
        speed = replay_info.get("speed", 1.0)
        speed_str = f"{speed:g}x" if speed != int(speed) else f"{int(speed)}x"
        title.append(f" {speed_str}", style="bold dark_orange3")
    elif remote_info is not None:
        title.append("  ")
        title.append(" SSH ", style="bold white on blue")
        host = remote_info.get("host", "")
        if host:
            title.append(f" {host}", style="bold blue")
    else:
        title.append("  ")
        title.append(" LIVE ", style="bold white on green")
    if stall_active:
        title.append("  ")
        title.append(" STALL ", style="bold white on red")
    if condor_offline:
        # Scheduler is unreachable; condor-derived columns may be stale while
        # the monitor backs off and retries. DB-driven state keeps updating.
        title.append("  ")
        title.append(" SCHEDD? ", style="bold black on yellow")

    right_col = Text(no_wrap=True)
    if replay_info is not None or remote_info is not None:
        right_col.append(
            datetime.fromtimestamp(refresh_ts).strftime("%H:%M:%S"), style="dim white"
        )
    else:
        right_col.append("Refreshed ", style="dim")
        right_col.append(
            datetime.fromtimestamp(refresh_ts).strftime("%H:%M:%S"), style="white"
        )

    # Row 2: workflow details
    details = Text()
    details.append("workflow: ", style="dim")
    details.append(info.dax_label, style="bold yellow")
    details.append("  uuid: ", style="dim")
    details.append(info.wf_uuid[:8] + "…", style="cyan")
    details.append("  user: ", style="dim")
    details.append(info.user, style="cyan")

    version = Text(no_wrap=True)
    version.append("pegasus ", style="dim")
    version.append(info.planner_version, style="dim")

    grid.add_row(title, right_col)
    grid.add_row(details, version)
    return Panel(grid, style="on grey7", padding=(0, 1))


# ─── Status summary bar ───────────────────────────────────────────────────────


def _make_status_bar(snap: WorkflowSnapshot) -> Panel:
    done = snap.done_count()
    total = snap.total_jobs()
    pct = snap.progress_pct()
    elapsed = fmt_duration(snap.elapsed)
    failed = snap.failed_count()
    held = snap.held_count()
    running = snap.running_count()
    queued = snap.queued_count()
    unsubmitted = sum(1 for j in snap.jobs if j.disp_state == "UNSUBMITTED")

    # Progress bar (manual Rich progress widget would need a separate task;
    # we render a simple bar using block characters instead)
    bar_width = 40
    filled = int(bar_width * pct / 100)
    bar_text = Text()
    bar_text.append("[", style="dim")
    bar_text.append("█" * filled, style="bold green")
    if failed > 0:
        fail_filled = max(1, int(bar_width * failed / total)) if total else 0
        bar_text.append("█" * min(fail_filled, bar_width - filled), style="bold red")
        bar_text.append("░" * max(0, bar_width - filled - fail_filled), style="dim")
    else:
        bar_text.append("░" * (bar_width - filled), style="dim")
    bar_text.append("]", style="dim")

    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(no_wrap=True)
    grid.add_column(no_wrap=True)
    grid.add_column(no_wrap=True)
    grid.add_column(no_wrap=True, ratio=1)

    state_cell = _wf_state_text(snap)
    elapsed_cell = Text(f"Elapsed: {elapsed}", style="dim white")
    pct_cell = Text(f"{pct:.1f}%  {done}/{total} done", style="white")

    counts = Text()
    counts.append(f"Done:{done} ", style="green")
    counts.append(f"Run:{running} ", style="cyan")
    counts.append(f"Queued:{queued} ", style="yellow")
    if unsubmitted:
        counts.append(f"Wait:{unsubmitted} ", style="dim")
    if held:
        counts.append(f"Held:{held} ", style="bold magenta")
    if failed:
        counts.append(f"Fail:{failed}", style="bold red")

    grid.add_row(state_cell, elapsed_cell, pct_cell, counts)

    progress_row = Table.grid(expand=True)
    progress_row.add_column(ratio=1)
    progress_row.add_column(no_wrap=True)
    progress_row.add_row(bar_text, Text())

    combined = Table.grid(expand=True)
    combined.add_column()
    combined.add_row(grid)
    combined.add_row(progress_row)

    return Panel(combined, title="[bold]Workflow Status[/bold]", padding=(0, 1))


# ─── Job details table ────────────────────────────────────────────────────────


def _activity_sort_key(job) -> tuple:
    """Stable two-tier sort key for the job table.

    Tier 0: RUNNING jobs at the top (most recently started first).
    Tier 1: jobs with any activity, by most-recent timestamp.
    Tier 2: UNSUBMITTED / no-timestamp jobs at the bottom (original order
    preserved by Python's stable sort).
    """
    if job.disp_state == "RUNNING":
        return (0, -(job.start_time or job.submit_time or 0))
    ts = job.end_time or job.start_time or job.submit_time
    if ts is None:
        return (2, 0)
    return (1, -ts)


def _make_job_table(
    snap: WorkflowSnapshot,
    show_all: bool = False,
    condor_jobs: Optional[List] = None,
    condor_history: Optional[List] = None,
    sort_by_activity: bool = True,
) -> Panel:
    # Build a condor lookup by DAGNodeName -> status
    condor_map: dict = {}
    if condor_jobs:
        for cj in condor_jobs:
            node = cj.get("DAGNodeName", "")
            if node:
                condor_map[node] = cj

    # Build a history lookup by DAGNodeName -> ClassAd
    history_map: dict = {}
    if condor_history:
        for hj in condor_history:
            node = hj.get("DAGNodeName", "")
            if node:
                history_map[node] = hj

    jobs_to_show = snap.jobs if show_all else snap.compute_jobs()
    if sort_by_activity:
        jobs_to_show = sorted(jobs_to_show, key=_activity_sort_key)

    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Job", style="white", ratio=3, no_wrap=True)
    table.add_column("Type", style="dim", ratio=1, no_wrap=True)
    table.add_column("State", ratio=1, no_wrap=True)
    table.add_column("Exit", justify="right", no_wrap=True, ratio=0)
    table.add_column("Duration", justify="right", no_wrap=True, ratio=1)
    table.add_column("Args", style="dim", ratio=2, no_wrap=True, max_width=40)
    table.add_column("Mem", justify="right", style="dim", no_wrap=True, width=7)
    table.add_column("Req", style="dim", no_wrap=True, width=12)
    table.add_column("Live", style="dim", ratio=1, no_wrap=True)

    for job in jobs_to_show:
        state_style = STATE_STYLE.get(job.disp_state, "")
        state_cell = Text(job.disp_state, style=state_style)
        ec = real_exitcode(job.exitcode)
        exit_cell = (
            Text(str(ec), style="green" if ec == 0 else "red")
            if ec is not None
            else Text("-", style="dim")
        )
        dur_cell = Text(fmt_duration(job.duration), style="dim")

        # Task arguments (truncated)
        argv = job.task_argv or ""
        if len(argv) > 37:
            argv = argv[:37] + "..."
        argv_cell = Text(argv, style="dim")

        mem_cell = Text(fmt_memory(job.maxrss), style="dim")

        # Condor live info (queue) or history (completed)
        condor_info = condor_map.get(job.exec_job_id, {})
        hist_info = history_map.get(job.exec_job_id, {})
        req_cell = Text("", style="dim")
        if condor_info:
            # Job is still in the queue — show live status
            live_parts = []
            live_parts.append(format_job_status(condor_info.get("JobStatus")))
            host = condor_info.get("RemoteHost", "")
            if host:
                short_host = host.split(".")[0] if "." in host else host
                live_parts.append(short_host)
            restarts = condor_info.get("NumJobStarts")
            if restarts is not None and int(restarts) > 1:
                live_parts.append(f"try#{int(restarts)}")
            wait = queue_wait_seconds(condor_info)
            if wait is not None and wait > 0:
                live_parts.append(f"wait:{fmt_duration(wait)}")
            xfer = format_transfer(condor_info)
            if xfer:
                live_parts.append(f"io:{xfer}")
            live_cell = Text(" ".join(live_parts), style="cyan")
            req_cell = Text(format_resources(condor_info), style="dim")
        elif hist_info:
            # Job completed — show post-completion metrics from history
            live_parts = []
            host = hist_info.get("LastRemoteHost", "")
            if host:
                short_host = host.split(".")[0] if "." in host else host
                live_parts.append(short_host)
            eff = cpu_efficiency(hist_info)
            if eff is not None:
                eff_style = "green" if eff >= 0.7 else "yellow" if eff >= 0.3 else "red"
                live_parts.append(f"cpu:{format_efficiency(eff)}")
            mem_eff = memory_efficiency(hist_info)
            if mem_eff is not None:
                live_parts.append(f"mem:{format_efficiency(mem_eff)}")
            disk = hist_info.get("DiskUsage")
            if disk:
                live_parts.append(f"disk:{format_disk_usage(disk)}")
            xfer = format_transfer(hist_info)
            if xfer:
                live_parts.append(f"io:{xfer}")
            restarts = hist_info.get("NumJobStarts")
            if restarts is not None and int(restarts) > 1:
                live_parts.append(f"try#{int(restarts)}")
            live_cell = Text(" ".join(live_parts), style="dim green")
            req_cell = Text(format_resources(hist_info), style="dim")
        else:
            live_cell = Text("", style="dim")

        type_label = JOB_TYPE_LABEL.get(job.type_desc, job.type_desc)

        table.add_row(
            job.display_name,
            type_label,
            state_cell,
            exit_cell,
            dur_cell,
            argv_cell,
            mem_cell,
            req_cell,
            live_cell,
        )

    if not jobs_to_show:
        table.add_row(
            Text("(no jobs yet)", style="dim italic"),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        )

    title = "[bold]Compute Jobs[/bold]" if not show_all else "[bold]All Jobs[/bold]"
    return Panel(table, title=title, padding=(0, 0))


# ─── Recent events panel ──────────────────────────────────────────────────────


def _make_events_panel(snap: WorkflowSnapshot, n: int = 15) -> Panel:
    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        expand=True,
        padding=(0, 1),
    )
    table.add_column("Job", style="white", no_wrap=True, ratio=3)
    table.add_column("State", no_wrap=True, ratio=1)
    table.add_column("Start", style="dim", no_wrap=True, width=10)
    table.add_column("End", style="dim", no_wrap=True, width=10)
    table.add_column("Duration", justify="right", style="dim", no_wrap=True, width=9)
    table.add_column("Mem", justify="right", style="dim", no_wrap=True, width=7)

    # Show jobs sorted by most recent activity (end_time or start_time)
    active_jobs = [
        j
        for j in snap.jobs
        if j.raw_state is not None  # only jobs that have been submitted
    ]
    active_jobs.sort(
        key=lambda j: j.end_time or j.start_time or j.submit_time or 0,
        reverse=True,
    )

    for job in active_jobs[:n]:
        state_style = STATE_STYLE.get(job.disp_state, "dim")
        state_cell = Text(job.disp_state, style=state_style)
        start_cell = Text(fmt_timestamp(job.start_time or job.submit_time), style="dim")
        end_cell = Text(
            fmt_timestamp(job.end_time) if job.end_time else "-", style="dim"
        )
        dur_cell = Text(fmt_duration(job.duration), style="dim")
        mem_cell = Text(fmt_memory(job.maxrss), style="dim")

        table.add_row(
            job.display_name,
            state_cell,
            start_cell,
            end_cell,
            dur_cell,
            mem_cell,
        )

    if not active_jobs:
        table.add_row(
            Text("(no activity yet)", style="dim italic"),
            "",
            "",
            "",
            "",
            "",
        )

    return Panel(table, title="[bold]Recent Events[/bold]", padding=(0, 0))


# ─── Infrastructure summary (compact) ────────────────────────────────────────


def _make_infra_summary(snap: WorkflowSnapshot) -> Panel:
    infra = snap.infra_jobs()
    counts: dict = {}
    for job in infra:
        t = JOB_TYPE_LABEL.get(job.type_desc, job.type_desc)
        s = job.disp_state
        key = (t, s)
        counts[key] = counts.get(key, 0) + 1

    table = Table(box=None, show_header=False, padding=(0, 1))
    table.add_column("Type", style="dim")
    table.add_column("State", style="dim")
    table.add_column("Count", justify="right", style="dim")

    for (t, s), count in sorted(counts.items()):
        style = STATE_STYLE.get(s, "dim")
        table.add_row(t, Text(s, style=style), str(count))

    if not counts:
        table.add_row("(none)", "", "")

    return Panel(table, title="[bold]Auxiliary Jobs[/bold]", padding=(0, 0))


# ─── Pool resources panel ────────────────────────────────────────────────


def _make_pool_panel(pool: PoolSummary) -> Panel:
    table = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    table.add_column("Label", style="dim", no_wrap=True)
    table.add_column("Value", style="white", no_wrap=True, justify="right")

    # Machines
    table.add_row("Machines", str(pool.machines))

    # Slots: claimed/idle/total
    slot_text = Text()
    if pool.claimed_slots:
        slot_text.append(f"{pool.claimed_slots}", style="cyan")
        slot_text.append("/", style="dim")
    slot_text.append(f"{pool.total_slots}", style="white")
    if pool.idle_slots:
        slot_text.append(f" ({pool.idle_slots} idle)", style="dim green")
    table.add_row("Slots", slot_text)

    # CPUs
    cpu_text = Text()
    used = pool.total_cpus - pool.idle_cpus
    if used > 0:
        cpu_text.append(f"{used}", style="cyan")
        cpu_text.append("/", style="dim")
    cpu_text.append(f"{pool.total_cpus}", style="white")
    if pool.idle_cpus > 0:
        cpu_text.append(f" ({pool.idle_cpus} idle)", style="dim green")
    table.add_row("CPUs", cpu_text)

    # Memory
    total_gb = pool.total_memory_mb / 1024
    idle_gb = pool.idle_memory_mb / 1024
    mem_text = Text()
    used_gb = total_gb - idle_gb
    if used_gb > 0.1:
        mem_text.append(f"{used_gb:.1f}", style="cyan")
        mem_text.append("/", style="dim")
    mem_text.append(f"{total_gb:.1f}G", style="white")
    if idle_gb > 0.1:
        mem_text.append(f" ({idle_gb:.1f}G free)", style="dim green")
    table.add_row("Memory", mem_text)

    # GPUs (only if any exist)
    if pool.total_gpus > 0:
        gpu_text = Text()
        used_gpus = pool.total_gpus - pool.idle_gpus
        if used_gpus > 0:
            gpu_text.append(f"{used_gpus}", style="cyan")
            gpu_text.append("/", style="dim")
        gpu_text.append(f"{pool.total_gpus}", style="white")
        if pool.idle_gpus > 0:
            gpu_text.append(f" ({pool.idle_gpus} idle)", style="dim green")
        table.add_row("GPUs", gpu_text)

    # OS/Arch
    if pool.os_arch:
        table.add_row("Platform", Text(pool.os_arch, style="dim"))

    return Panel(table, title="[bold]Pool Resources[/bold]", padding=(0, 0))


# ─── Diagnostics panel ────────────────────────────────────────────────────


def _make_diagnostics_panel(
    snap: WorkflowSnapshot,
    condor_jobs: Optional[List] = None,
    submit_dir: Optional[Path] = None,
) -> Panel:
    held = snap.held_jobs()
    failed = snap.failed_jobs()
    diags = collect_diagnostics(held, failed, condor_jobs, submit_dir=submit_dir)

    grid = Table.grid(expand=True, padding=(0, 1))
    grid.add_column(ratio=1)

    for diag in diags:
        severity_style = "bold magenta" if diag.severity == "held" else "bold red"
        icon = "⊘" if diag.severity == "held" else "✖"

        header = Text()
        header.append(f" {icon} ", style=severity_style)
        header.append(diag.job_name, style="bold white")
        header.append(f"  {diag.summary}", style=severity_style)
        grid.add_row(header)

        if diag.reason and diag.reason != f"Exit code: {None}":
            # When suggestions already carry structured findings (e.g. "Missing: ..."
            # from stderr analysis), skip the raw stderr dump to avoid noise.
            has_structured = any(
                s.startswith("Missing:") or s.startswith("Permission denied:")
                for s in diag.suggestions
            )
            reason_parts = diag.reason.split(" | ")
            for part in reason_parts:
                # Skip the huge raw stderr when structured findings cover it
                if has_structured and part.startswith("stderr:"):
                    continue
                reason_text = Text()
                reason_text.append("   ", style="dim")
                if part.startswith("stdout:"):
                    reason_text.append("stdout: ", style="dim")
                    msg = part[len("stdout:") :].strip()
                    if len(msg) > 120:
                        msg = msg[:117] + "..."
                    reason_text.append(msg, style="bold white")
                elif part.startswith("stderr:"):
                    reason_text.append("stderr: ", style="dim")
                    msg = part[len("stderr:") :].strip()
                    if len(msg) > 120:
                        msg = msg[:117] + "..."
                    reason_text.append(msg, style="bold yellow")
                elif part.startswith("executable:"):
                    reason_text.append("exec:   ", style="dim")
                    reason_text.append(
                        part[len("executable:") :].strip(), style="dim white"
                    )
                else:
                    reason_text.append(part, style="dim white")
                grid.add_row(reason_text)

        for suggestion in diag.suggestions[:5]:
            sug_text = Text()
            if suggestion.startswith("Missing:"):
                sug_text.append("   ✗ ", style="bold red")
                sug_text.append(suggestion[len("Missing:") :].strip(), style="bold red")
            elif suggestion.startswith("Permission denied:"):
                sug_text.append("   ✗ ", style="bold red")
                sug_text.append(suggestion, style="bold red")
            else:
                sug_text.append("   → ", style="yellow")
                sug_text.append(suggestion, style="yellow")
            grid.add_row(sug_text)

        grid.add_row(Text(""))  # spacer between diagnostics

    if not diags:
        grid.add_row(Text("  No issues detected", style="dim green"))

    return Panel(
        grid,
        title="[bold yellow]Diagnostics[/bold yellow]",
        border_style="yellow",
        padding=(0, 0),
    )


# ─── Stall alert panel ───────────────────────────────────────────────────────


def _make_stall_alert_panel(
    alerts: List[Dict],
    diag_path: Optional[str] = None,
) -> Panel:
    """Render a compact alert panel summarizing active stall diagnostics.

    *alerts* is the list of recent diagnostic events (stall_detected,
    idle_diagnosis, hold_diagnosis, failure_diagnosis) collected since the
    last stall_resolved.
    """
    body = Text()

    # Pull the most recent stall_detected for the headline
    stall = next(
        (a for a in reversed(alerts) if a.get("event_type") == "stall_detected"),
        None,
    )
    if stall is not None:
        stype = stall.get("stall_type", "unknown")
        details = stall.get("details", "")
        body.append("Stall type: ", style="bold")
        body.append(f"{stype}\n", style="bold yellow")
        if details:
            body.append(f"  {details}\n", style="white")

    # Most recent idle diagnosis
    idle = next(
        (a for a in reversed(alerts) if a.get("event_type") == "idle_diagnosis"),
        None,
    )
    if idle is not None:
        findings = idle.get("findings") or []
        suggestions = idle.get("suggestions") or []
        for f in findings[:3]:
            body.append("  • ", style="yellow")
            body.append(f"{f}\n", style="white")
        for s in suggestions[:2]:
            body.append("  → ", style="green")
            body.append(f"{s}\n", style="white")

    # Hold/failure diagnoses (one line each, latest few)
    held_failed = [
        a
        for a in alerts
        if a.get("event_type") in ("hold_diagnosis", "failure_diagnosis")
    ]
    for d in held_failed[-3:]:
        body.append("  ! ", style="red")
        body.append(f"{d.get('job_name', '?')}: ", style="bold red")
        body.append(f"{d.get('summary', '')}\n", style="white")

    if diag_path:
        body.append("Sidecar: ", style="dim")
        body.append(f"{diag_path}", style="dim cyan")

    if len(body) == 0:
        body.append("Diagnostics active", style="bold yellow")

    return Panel(
        body,
        title="[bold red]STALL DETECTED[/bold red]",
        border_style="bold red",
        padding=(0, 1),
    )


# ─── Full layout assembly ─────────────────────────────────────────────────────


def build_layout(
    info: WorkflowInfo,
    snap: WorkflowSnapshot,
    show_all: bool,
    condor_jobs: Optional[List],
    events_n: int,
    refresh_ts: float,
    replay_info: Optional[dict] = None,
    remote_info: Optional[dict] = None,
    submit_dir: Optional[Path] = None,
    condor_history: Optional[List] = None,
    pool_status: Optional[PoolSummary] = None,
    diag_alerts: Optional[List[Dict]] = None,
    diag_path: Optional[str] = None,
    sort_by_activity: bool = True,
    condor_offline: bool = False,
) -> Layout:
    has_issues = snap.held_count() > 0 or snap.failed_count() > 0
    has_alerts = bool(diag_alerts)

    # Calculate diagnostics panel height based on number of issues
    diag_height = 0
    if has_issues:
        n_issues = snap.held_count() + snap.failed_count()
        # ~7 lines per issue (header + exitcode + stdout + stderr + exec + suggestions + spacer)
        diag_height = min(3 + n_issues * 7, 25)

    layout = Layout()
    parts = [
        Layout(name="header", size=4),
        Layout(name="status", size=5),
    ]
    if has_alerts:
        # 3 fixed lines + per-finding/suggestion/job lines, capped
        n_lines = 3
        idle = next(
            (
                a
                for a in reversed(diag_alerts)
                if a.get("event_type") == "idle_diagnosis"
            ),
            None,
        )
        if idle:
            n_lines += min(3, len(idle.get("findings") or []))
            n_lines += min(2, len(idle.get("suggestions") or []))
        n_lines += min(
            3,
            sum(
                1
                for a in diag_alerts
                if a.get("event_type") in ("hold_diagnosis", "failure_diagnosis")
            ),
        )
        alert_height = max(5, min(n_lines + 2, 12))
        parts.append(Layout(name="stall_alert", size=alert_height))
    if has_issues:
        parts.append(Layout(name="diagnostics", size=diag_height))
    parts.append(Layout(name="main"))
    parts.append(Layout(name="events", size=events_n + 3))
    layout.split_column(*parts)

    # Build the right-side column: infra summary + optional pool resources
    if pool_status is not None:
        right_col = Layout(name="right_col")
        right_col.split_column(
            Layout(name="infra", ratio=1),
            Layout(name="pool", size=pool_status.total_gpus > 0 and 9 or 8),
        )
        layout["main"].split_row(
            Layout(name="jobs", ratio=3),
            right_col,
        )
        right_col["infra"].update(_make_infra_summary(snap))
        right_col["pool"].update(_make_pool_panel(pool_status))
    else:
        layout["main"].split_row(
            Layout(name="jobs", ratio=3),
            Layout(name="infra", ratio=1),
        )
        layout["infra"].update(_make_infra_summary(snap))

    layout["header"].update(
        _make_header(
            info,
            snap,
            refresh_ts,
            replay_info=replay_info,
            remote_info=remote_info,
            stall_active=has_alerts,
            condor_offline=condor_offline,
        )
    )
    layout["status"].update(_make_status_bar(snap))
    if has_alerts:
        layout["stall_alert"].update(
            _make_stall_alert_panel(diag_alerts, diag_path=diag_path)
        )
    if has_issues:
        layout["diagnostics"].update(
            _make_diagnostics_panel(
                snap, condor_jobs=condor_jobs, submit_dir=submit_dir
            )
        )
    layout["jobs"].update(
        _make_job_table(
            snap,
            show_all=show_all,
            condor_jobs=condor_jobs,
            condor_history=condor_history,
            sort_by_activity=sort_by_activity,
        )
    )
    layout["events"].update(_make_events_panel(snap, n=events_n))

    return layout


# ─── Monitor loop ─────────────────────────────────────────────────────────────


def run_monitor(
    info: WorkflowInfo,
    db: StampedeDB,
    poll_interval: float = 2.0,
    show_all: bool = False,
    events_n: int = 15,
    condor_constraint: Optional[str] = None,
    condor_kwargs: Optional[dict] = None,
    once: bool = False,
    log_path: Optional[Path] = None,
    diagnose: bool = False,
    sort_by_activity: bool = True,
    min_free_mb: float = 200.0,
    max_log_mb: Optional[float] = None,
) -> None:
    """Run the live terminal dashboard.

    Parameters
    ----------
    info:              WorkflowInfo from braindump.yml.
    db:                Connected StampedeDB instance.
    poll_interval:     Seconds between stampede.db refreshes.
    show_all:          When True, show all job types not just compute.
    events_n:          Number of recent events to show.
    condor_constraint: Optional HTCondor ClassAd constraint for live queue.
    condor_kwargs:     Extra kwargs forwarded to htcondor_poll.query_queue().
    once:              Print status once and exit (non-interactive).
    log_path:          If set, write JSONL event log to this path.
    min_free_mb:       Pause event logging below this much free space (MB).
    max_log_mb:        Optional hard cap on the JSONL size (MB); None = unbounded.
    """
    ck = condor_kwargs or {}

    console = Console()

    logger: Optional[EventLogger] = None
    if log_path is not None:
        logger = EventLogger(
            info, db, log_path=log_path, min_free_mb=min_free_mb, max_log_mb=max_log_mb
        )
        if logger.resumed:
            console.print(f"[dim]Resuming event log at {logger.path}[/dim]")
        else:
            console.print(f"[dim]Logging events to {logger.path}[/dim]")

    diag_engine = None
    diag_active_alerts: List[Dict] = []
    if diagnose:
        from .diag_log import DiagnosticLogger
        from .diagnostics_engine import DiagnosticsEngine

        if logger is not None:
            diag_path = DiagnosticLogger.path_from_event_log(logger.path)
        else:
            diag_path = info.submit_dir / "diagnostics-events.jsonl"
        diag_engine = DiagnosticsEngine(
            info=info,
            diag_log_path=diag_path,
            poll_interval=poll_interval,
            condor_constraint=condor_constraint,
            condor_kwargs=ck,
        )
        console.print(f"[dim]Diagnostics sidecar: {diag_path}[/dim]")

    # Adaptive gate: the TUI keeps refreshing local DB state every cycle, but
    # the live condor queue is polled less often when the scheduler is failing
    # so a struggling schedd is never hammered and the UI never freezes. The
    # last good queue is reused while gated/failed (no stderr output here — it
    # would corrupt the full-screen Live; the state is shown via a header badge).
    condor_backoff = CondorBackoff(poll_interval)
    _last_condor_jobs: List = []

    def _poll_condor() -> List:
        nonlocal _last_condor_jobs
        now = time.time()
        if not condor_backoff.due(now):
            return _last_condor_jobs
        try:
            jobs = query_queue(constraint=condor_constraint, raise_on_error=True, **ck)
        except Exception:
            condor_backoff.record(False, now)
            return _last_condor_jobs
        condor_backoff.record(True, now)
        _last_condor_jobs = jobs
        return jobs

    # History cache: accumulates completed job records, refreshed every
    # few poll cycles to avoid hammering condor_history each second.
    _history_cache: List = []
    _history_last_poll: float = 0.0
    _HISTORY_INTERVAL = max(poll_interval * 3, 10.0)  # at least 10s

    def _poll_history() -> List:
        nonlocal _history_cache, _history_last_poll
        now = time.time()
        # While the scheduler is failing, skip history too (don't add load).
        if condor_backoff.fail_streak > 0:
            return _history_cache
        if now - _history_last_poll < _HISTORY_INTERVAL:
            return _history_cache
        try:
            result = query_history(constraint=condor_constraint, **ck)
            if result:
                # Merge into cache (dedup by ClusterId)
                seen = {h.get("ClusterId") for h in _history_cache}
                for h in result:
                    cid = h.get("ClusterId")
                    if cid not in seen:
                        _history_cache.append(h)
                        seen.add(cid)
        except Exception:
            pass
        _history_last_poll = now
        return _history_cache

    # Pool status cache: polled less frequently (~15s)
    _pool_cache: Optional[PoolSummary] = None
    _pool_last_poll: float = 0.0
    _POOL_INTERVAL = max(poll_interval * 5, 15.0)

    def _poll_pool() -> Optional[PoolSummary]:
        nonlocal _pool_cache, _pool_last_poll
        now = time.time()
        if condor_backoff.fail_streak > 0:
            return _pool_cache  # scheduler failing — skip pool queries too
        if now - _pool_last_poll < _POOL_INTERVAL:
            return _pool_cache
        try:
            # Pool query uses collector_host but no per-workflow constraint
            pool_kwargs = {}
            if ck.get("collector_host"):
                pool_kwargs["collector_host"] = ck["collector_host"]
            if ck.get("token_path"):
                pool_kwargs["token_path"] = ck["token_path"]
            if ck.get("cert_path"):
                pool_kwargs["cert_path"] = ck["cert_path"]
            if ck.get("key_path"):
                pool_kwargs["key_path"] = ck["key_path"]
            if ck.get("password_file"):
                pool_kwargs["password_file"] = ck["password_file"]
            _pool_cache = query_slots(**pool_kwargs)
        except Exception:
            pass
        _pool_last_poll = now
        return _pool_cache

    def _refresh() -> tuple:
        snap = db.snapshot()
        condor_jobs = _poll_condor()
        history = _poll_history()
        pool = _poll_pool()
        ts = time.time()
        if logger is not None:
            logger.record(snap, condor_jobs, history, pool)
        if diag_engine is not None:
            try:
                hwts = logger.high_water_ts if logger is not None else 0.0
                events = diag_engine.tick(snap, condor_jobs, pool, hwts)
                for ev in events:
                    et = ev.get("event_type")
                    if et == "stall_resolved":
                        diag_active_alerts.clear()
                    elif et in (
                        "stall_detected",
                        "idle_diagnosis",
                        "hold_diagnosis",
                        "failure_diagnosis",
                    ):
                        diag_active_alerts.append(ev)
            except Exception:
                pass
        return snap, condor_jobs, history, pool, ts

    if once:
        snap, condor_jobs, history, pool, ts = _refresh()
        console.print(_make_header(info, snap, ts))
        console.print(_make_status_bar(snap))
        if snap.held_count() > 0 or snap.failed_count() > 0:
            console.print(
                _make_diagnostics_panel(
                    snap, condor_jobs=condor_jobs, submit_dir=info.submit_dir
                )
            )
        console.print(
            _make_job_table(
                snap,
                show_all=show_all,
                condor_jobs=condor_jobs,
                condor_history=history,
                sort_by_activity=sort_by_activity,
            )
        )
        if snap.infra_jobs():
            console.print(_make_infra_summary(snap))
        if pool is not None:
            console.print(_make_pool_panel(pool))
        console.print(_make_events_panel(snap, n=events_n))
        wf_stats = compute_workflow_stats(
            snap, condor_history=history, pool_status=pool
        )
        _print_final_summary(
            console,
            snap,
            condor_jobs=condor_jobs,
            submit_dir=info.submit_dir,
            stats=wf_stats,
        )
        if logger is not None:
            logger.close(snap, condor_history=history, pool_status=pool)
        if diag_engine is not None:
            try:
                diag_engine.close()
            except Exception:
                pass
        return

    with Live(
        console=console,
        screen=True,
        refresh_per_second=2,
        redirect_stderr=False,
    ) as live:
        try:
            while True:
                snap, condor_jobs, history, pool, ts = _refresh()
                layout = build_layout(
                    info,
                    snap,
                    show_all,
                    condor_jobs,
                    events_n,
                    ts,
                    submit_dir=info.submit_dir,
                    condor_history=history,
                    pool_status=pool,
                    diag_alerts=list(diag_active_alerts)
                    if diag_active_alerts
                    else None,
                    diag_path=str(diag_engine.path) if diag_engine else None,
                    sort_by_activity=sort_by_activity,
                    condor_offline=condor_backoff.fail_streak > 0,
                )
                live.update(layout)

                if snap.is_complete and not snap.is_running:
                    # Give one extra beat for final DB flush from monitord
                    time.sleep(poll_interval)
                    snap, condor_jobs, history, pool, ts = _refresh()
                    live.update(
                        build_layout(
                            info,
                            snap,
                            show_all,
                            condor_jobs,
                            events_n,
                            ts,
                            submit_dir=info.submit_dir,
                            condor_history=history,
                            pool_status=pool,
                            diag_alerts=list(diag_active_alerts)
                            if diag_active_alerts
                            else None,
                            diag_path=str(diag_engine.path) if diag_engine else None,
                            sort_by_activity=sort_by_activity,
                            condor_offline=condor_backoff.fail_streak > 0,
                        )
                    )
                    break

                time.sleep(poll_interval)

        except KeyboardInterrupt:
            pass

    # After live session ends, print a brief final summary
    wf_stats = compute_workflow_stats(snap, condor_history=history, pool_status=pool)
    _print_final_summary(
        console,
        snap,
        condor_jobs=condor_jobs,
        submit_dir=info.submit_dir,
        stats=wf_stats,
    )
    if logger is not None:
        logger.close(snap, condor_history=history, pool_status=pool)
    if diag_engine is not None:
        try:
            diag_engine.close()
        except Exception:
            pass


def _format_eff_range(
    lo: Optional[float], hi: Optional[float], mean: Optional[float]
) -> str:
    """Format an efficiency range like '81% (42%–98%)'."""
    if mean is None:
        return ""
    mean_s = f"{mean * 100:.0f}%"
    if lo is not None and hi is not None and lo != hi:
        return f"{mean_s} ({lo * 100:.0f}%–{hi * 100:.0f}%)"
    return mean_s


def _print_stats_block(console: Console, stats: WorkflowStats) -> None:
    """Print a human-readable statistics block."""
    table = Table(
        box=box.SIMPLE_HEAVY,
        show_header=False,
        padding=(0, 2),
        expand=False,
        title="[bold]Workflow Statistics[/bold]",
        title_style="dim",
    )
    table.add_column("Label", style="dim", no_wrap=True)
    table.add_column("Value", style="white", no_wrap=True)

    # Job counts
    parts = []
    if stats.compute_jobs:
        parts.append(f"{stats.compute_jobs} compute")
    if stats.infra_jobs:
        parts.append(f"{stats.infra_jobs} infra")
    job_line = ", ".join(parts) + f"  ({stats.total_jobs} total"
    if stats.failed:
        job_line += f", {stats.failed} failed"
    if stats.held:
        job_line += f", {stats.held} held"
    job_line += ")"
    table.add_row("Jobs", job_line)

    # Wall time
    if stats.wall_time is not None:
        table.add_row("Wall time", fmt_duration(stats.wall_time))

    # Compute time + parallelism
    if stats.total_compute_time is not None:
        ct = fmt_duration(stats.total_compute_time)
        if stats.parallelism is not None and stats.parallelism > 1.01:
            ct += f"  ({stats.parallelism:.1f}x parallelism)"
        table.add_row("Compute time", ct)

    # Duration range
    if stats.dur_min is not None and stats.dur_max is not None:
        dur_line = (
            f"min: {fmt_duration(stats.dur_min)}  max: {fmt_duration(stats.dur_max)}"
        )
        if stats.dur_mean is not None:
            dur_line += f"  mean: {fmt_duration(stats.dur_mean)}"
        if stats.dur_median is not None:
            dur_line += f"  median: {fmt_duration(stats.dur_median)}"
        table.add_row("Duration", dur_line)

    # Longest / shortest job
    if stats.longest_job_name and stats.dur_max is not None:
        table.add_row(
            "Longest job", f"{stats.longest_job_name} ({fmt_duration(stats.dur_max)})"
        )
    if (
        stats.shortest_job_name
        and stats.dur_min is not None
        and stats.shortest_job_name != stats.longest_job_name
    ):
        table.add_row(
            "Shortest job", f"{stats.shortest_job_name} ({fmt_duration(stats.dur_min)})"
        )

    # Memory
    if stats.peak_maxrss_kb is not None:
        mem_line = f"peak: {fmt_memory(stats.peak_maxrss_kb)}"
        if stats.peak_maxrss_job:
            mem_line += f" ({stats.peak_maxrss_job})"
        if stats.mean_maxrss_kb is not None:
            mem_line += f"  avg: {fmt_memory(int(stats.mean_maxrss_kb))}"
        table.add_row("Memory", mem_line)

    # CPU efficiency
    eff_line = _format_eff_range(
        stats.cpu_eff_min, stats.cpu_eff_max, stats.cpu_eff_mean
    )
    if eff_line:
        table.add_row("CPU efficiency", eff_line)

    # Memory efficiency
    mem_eff_line = _format_eff_range(
        stats.mem_eff_min, stats.mem_eff_max, stats.mem_eff_mean
    )
    if mem_eff_line:
        table.add_row("Mem efficiency", mem_eff_line)

    # Queue wait
    if stats.wait_mean is not None:
        wait_line = f"mean: {fmt_duration(stats.wait_mean)}"
        if stats.wait_min is not None and stats.wait_max is not None:
            wait_line += (
                f"  ({fmt_duration(stats.wait_min)}–{fmt_duration(stats.wait_max)})"
            )
        table.add_row("Queue wait", wait_line)

    # CPU-hours
    if stats.cpu_seconds is not None:
        hours = stats.cpu_seconds / 3600
        if hours >= 1.0:
            table.add_row("CPU consumed", f"{hours:.2f} hours")
        else:
            table.add_row("CPU consumed", f"{stats.cpu_seconds:.1f}s")

    # Transfer
    if stats.transfer_bytes is not None and stats.transfer_bytes > 0:
        from .htcondor_poll import format_transfer

        table.add_row(
            "Data transfer",
            format_transfer({"BytesSent": stats.transfer_bytes, "BytesRecvd": 0}),
        )

    # Retries
    if stats.retry_count is not None and stats.retry_count > 0:
        table.add_row("Retries", str(stats.retry_count))

    # Hosts
    if stats.hosts:
        table.add_row("Hosts", ", ".join(stats.hosts[:5]))

    # Pool
    pool_parts = []
    if stats.pool_machines is not None:
        pool_parts.append(f"{stats.pool_machines} machines")
    if stats.pool_total_cpus is not None:
        pool_parts.append(f"{stats.pool_total_cpus} CPUs")
    if stats.pool_total_gpus is not None:
        pool_parts.append(f"{stats.pool_total_gpus} GPUs")
    if pool_parts:
        table.add_row("Pool", ", ".join(pool_parts))

    console.print(table)


def _print_final_summary(
    console: Console,
    snap: WorkflowSnapshot,
    condor_jobs: Optional[List] = None,
    submit_dir: Optional[Path] = None,
    stats: Optional[WorkflowStats] = None,
) -> None:
    console.print()
    if snap.succeeded:
        console.print(
            f"[bold green]✔ Workflow completed successfully[/bold green]  "
            f"(elapsed: {fmt_duration(snap.elapsed)})"
        )
    elif snap.failed:
        console.print(
            f"[bold red]✖ Workflow FAILED[/bold red]  "
            f"(elapsed: {fmt_duration(snap.elapsed)}, "
            f"failed jobs: {snap.failed_count()}"
            f"{f', held jobs: {snap.held_count()}' if snap.held_count() else ''})"
        )
        # Print diagnostics summary
        held = snap.held_jobs()
        failed = snap.failed_jobs()
        if held or failed:
            diags = collect_diagnostics(
                held, failed, condor_jobs, submit_dir=submit_dir
            )
            for diag in diags:
                icon = "⊘" if diag.severity == "held" else "✖"
                style = "magenta" if diag.severity == "held" else "red"
                console.print(
                    f"  [{style}]{icon} {diag.job_name}[/{style}]: {diag.summary}"
                )
                has_structured = any(
                    s.startswith("Missing:") or s.startswith("Permission denied:")
                    for s in diag.suggestions
                )
                if diag.reason:
                    for part in diag.reason.split(" | "):
                        if has_structured and part.startswith("stderr:"):
                            continue
                        if part.startswith("stdout:"):
                            console.print(
                                f"    [white]stdout: {part[7:].strip()}[/white]"
                            )
                        elif part.startswith("stderr:"):
                            msg = part[7:].strip()
                            if len(msg) > 200:
                                msg = msg[:197] + "..."
                            console.print(f"    [yellow]stderr: {msg}[/yellow]")
                        elif part.startswith("executable:"):
                            console.print(f"    [dim]exec: {part[11:].strip()}[/dim]")
                        else:
                            console.print(f"    [dim]{part}[/dim]")
                for sug in diag.suggestions[:4]:
                    if sug.startswith("Missing:"):
                        console.print(f"    [bold red]✗ {sug[8:].strip()}[/bold red]")
                    elif sug.startswith("Permission denied:"):
                        console.print(f"    [bold red]✗ {sug}[/bold red]")
                    else:
                        console.print(f"    [yellow]→ {sug}[/yellow]")
    else:
        held = snap.held_count()
        msg = f"[yellow]◌ Monitoring stopped[/yellow]  (state: {snap.wf_state}"
        if held:
            msg += f", held: {held}"
        msg += ")"
        console.print(msg)
        if held:
            diags = collect_diagnostics(
                snap.held_jobs(), [], condor_jobs, submit_dir=submit_dir
            )
            for diag in diags:
                console.print(f"  [magenta]⊘ {diag.job_name}[/magenta]: {diag.summary}")
                for sug in diag.suggestions[:2]:
                    console.print(f"    [yellow]→ {sug}[/yellow]")

    # Print statistics block if available
    if stats is not None:
        console.print()
        _print_stats_block(console, stats)

    console.print()
