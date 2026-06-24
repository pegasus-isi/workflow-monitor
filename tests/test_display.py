"""Unit tests for workflow_monitor.display.

Display is Rich-rendering. We never start the Live TUI (``run_monitor``); we
exercise the *pure builders* and confirm each returns a valid Rich renderable
that renders to text without error and contains expected substrings.

Render helper: a fixed-width Console writing to a StringIO, ``force_terminal=
False`` so no ANSI is emitted and we can assert on plain substrings.
"""

from __future__ import annotations

import io
import time

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel

from workflow_monitor import display
from workflow_monitor.braindump import WorkflowInfo
from workflow_monitor.db import StampedeDB
from workflow_monitor.htcondor_poll import PoolSummary
from workflow_monitor.stats import WorkflowStats, compute_workflow_stats


# ─────────────────────────────────────────────────────────────────────────────
# Render helpers
# ─────────────────────────────────────────────────────────────────────────────


def render(renderable, width: int = 120) -> str:
    """Render *renderable* to plain text via a captured Console."""
    con = Console(file=io.StringIO(), width=width, force_terminal=False)
    con.print(renderable)
    return con.file.getvalue()


def make_info(submit_dir="/submit/run0001", **overrides) -> WorkflowInfo:
    from pathlib import Path

    defaults = dict(
        wf_uuid="abcdef1234567890",
        root_wf_uuid="abcdef1234567890",
        dax_label="diamond",
        submit_dir=Path(submit_dir),
        user="tester",
        planner_version="5.1.2",
        dag_file="diamond-0.dag",
        condor_log="diamond-0.log",
        timestamp="20260101T000000+0000",
        basedir=Path(submit_dir).parent,
    )
    defaults.update(overrides)
    return WorkflowInfo(**defaults)


def make_pool(**overrides) -> PoolSummary:
    defaults = dict(
        total_slots=8,
        idle_slots=2,
        claimed_slots=6,
        total_cpus=16,
        idle_cpus=4,
        total_memory_mb=32768,
        idle_memory_mb=8192,
        total_gpus=0,
        idle_gpus=0,
        machines=2,
        os_arch="LINUX/X86_64",
    )
    defaults.update(overrides)
    return PoolSummary(**defaults)


# ─────────────────────────────────────────────────────────────────────────────
# _wf_state_text
# ─────────────────────────────────────────────────────────────────────────────


def test_wf_state_text_running(make_snapshot):
    snap = make_snapshot(states=["RUNNING"], wf_state="WORKFLOW_STARTED")
    out = render(display._wf_state_text(snap))
    assert "RUNNING" in out


def test_wf_state_text_success(make_snapshot):
    snap = make_snapshot(
        states=["SUCCESS"], wf_state="WORKFLOW_TERMINATED", wf_status=0
    )
    out = render(display._wf_state_text(snap))
    assert "SUCCESS" in out


def test_wf_state_text_failed(make_snapshot):
    snap = make_snapshot(states=["FAILED"], wf_state="WORKFLOW_TERMINATED", wf_status=1)
    out = render(display._wf_state_text(snap))
    assert "FAILED" in out


def test_wf_state_text_other_shows_raw_state(make_snapshot):
    # Neither running, succeeded, nor failed -> dim "◌ <wf_state>"
    snap = make_snapshot(states=[], wf_state="UNKNOWN", wf_status=None)
    out = render(display._wf_state_text(snap))
    assert "UNKNOWN" in out


# ─────────────────────────────────────────────────────────────────────────────
# _activity_sort_key
# ─────────────────────────────────────────────────────────────────────────────


def test_activity_sort_key_running_first(make_job):
    running = make_job(raw_state="EXECUTE", start_time=200.0, end_time=None)
    terminal = make_job(raw_state="JOB_SUCCESS", start_time=100.0, end_time=150.0)
    unsub = make_job(raw_state=None, submit_time=None, start_time=None, end_time=None)
    # RUNNING -> tier 0, terminal -> tier 1, unsubmitted -> tier 2
    assert display._activity_sort_key(running)[0] == 0
    assert display._activity_sort_key(terminal)[0] == 1
    assert display._activity_sort_key(unsub)[0] == 2


def test_activity_sort_orders_running_before_terminal_before_unsubmitted(make_job):
    jobs = [
        make_job(
            job_id=1, raw_state=None, submit_time=None, start_time=None, end_time=None
        ),
        make_job(job_id=2, raw_state="JOB_SUCCESS", start_time=100.0, end_time=150.0),
        make_job(job_id=3, raw_state="EXECUTE", start_time=300.0, end_time=None),
    ]
    ordered = sorted(jobs, key=display._activity_sort_key)
    states = [j.disp_state for j in ordered]
    assert states[0] == "RUNNING"
    assert states[-1] == "UNSUBMITTED"


def test_activity_sort_running_most_recent_start_first(make_job):
    older = make_job(job_id=1, raw_state="EXECUTE", start_time=100.0, end_time=None)
    newer = make_job(job_id=2, raw_state="EXECUTE", start_time=500.0, end_time=None)
    ordered = sorted([older, newer], key=display._activity_sort_key)
    # Most recently started running job sorts first (key uses -start_time)
    assert ordered[0] is newer


def test_activity_sort_terminal_most_recent_first(make_job):
    older = make_job(job_id=1, raw_state="JOB_SUCCESS", start_time=10.0, end_time=20.0)
    newer = make_job(
        job_id=2, raw_state="JOB_SUCCESS", start_time=100.0, end_time=200.0
    )
    ordered = sorted([older, newer], key=display._activity_sort_key)
    assert ordered[0] is newer


# ─────────────────────────────────────────────────────────────────────────────
# _format_eff_range
# ─────────────────────────────────────────────────────────────────────────────


def test_format_eff_range_none_mean():
    assert display._format_eff_range(0.4, 0.9, None) == ""


def test_format_eff_range_with_min_max():
    out = display._format_eff_range(0.42, 0.98, 0.81)
    assert out == "81% (42%–98%)"


def test_format_eff_range_equal_lo_hi_just_mean():
    # When lo == hi, only the mean is shown (no parenthetical range)
    assert display._format_eff_range(0.5, 0.5, 0.5) == "50%"


def test_format_eff_range_lo_hi_none_just_mean():
    assert display._format_eff_range(None, None, 0.73) == "73%"


# ─────────────────────────────────────────────────────────────────────────────
# _make_header
# ─────────────────────────────────────────────────────────────────────────────


def test_make_header_live_badge(make_snapshot):
    info = make_info()
    snap = make_snapshot(states=["RUNNING"])
    panel = display._make_header(info, snap, refresh_ts=1_700_000_000.0)
    assert isinstance(panel, Panel)
    out = render(panel)
    assert "Pegasus Workflow Monitor" in out
    assert "LIVE" in out
    assert "diamond" in out
    assert "tester" in out


def test_make_header_replay_badge(make_snapshot):
    info = make_info()
    snap = make_snapshot(states=["RUNNING"])
    panel = display._make_header(
        info, snap, refresh_ts=1_700_000_000.0, replay_info={"speed": 4.0}
    )
    out = render(panel)
    assert "REPLAY" in out
    assert "4x" in out


def test_make_header_remote_badge(make_snapshot):
    info = make_info()
    snap = make_snapshot(states=["RUNNING"])
    panel = display._make_header(
        info, snap, refresh_ts=1_700_000_000.0, remote_info={"host": "myhost"}
    )
    out = render(panel)
    assert "SSH" in out
    assert "myhost" in out


def test_make_header_stall_and_offline_badges(make_snapshot):
    info = make_info()
    snap = make_snapshot(states=["RUNNING"])
    panel = display._make_header(
        info,
        snap,
        refresh_ts=1_700_000_000.0,
        stall_active=True,
        condor_offline=True,
    )
    out = render(panel)
    assert "STALL" in out
    assert "SCHEDD?" in out


# ─────────────────────────────────────────────────────────────────────────────
# _make_status_bar
# ─────────────────────────────────────────────────────────────────────────────


def test_make_status_bar_running_mix(make_snapshot):
    snap = make_snapshot(
        states=["SUCCESS", "RUNNING", "QUEUED", "UNSUBMITTED"],
        wf_state="WORKFLOW_STARTED",
    )
    panel = display._make_status_bar(snap)
    assert isinstance(panel, Panel)
    out = render(panel)
    assert "Workflow Status" in out
    assert "Done:1" in out
    assert "Run:1" in out
    assert "Queued:1" in out
    assert "Wait:1" in out


def test_make_status_bar_with_failures_and_holds(make_snapshot):
    snap = make_snapshot(
        states=["SUCCESS", "FAILED", "HELD"],
        wf_state="WORKFLOW_TERMINATED",
        wf_status=1,
    )
    out = render(display._make_status_bar(snap))
    assert "Fail:1" in out
    assert "Held:1" in out


def test_make_status_bar_empty_roster(make_snapshot):
    # No jobs: 0/0 done, 0.0% — must render without ZeroDivision
    snap = make_snapshot(states=[])
    out = render(display._make_status_bar(snap))
    assert "0/0 done" in out


# ─────────────────────────────────────────────────────────────────────────────
# _make_job_table
# ─────────────────────────────────────────────────────────────────────────────


def test_make_job_table_compute_only(make_snapshot, make_job):
    jobs = [
        make_job(job_id=1, exec_job_id="preprocess_ID1", type_desc="compute"),
        make_job(job_id=2, exec_job_id="create_dir", type_desc="create-dir"),
    ]
    snap = make_snapshot(jobs=jobs)
    panel = display._make_job_table(snap, show_all=False)
    out = render(panel)
    assert "Compute Jobs" in out
    assert "preprocess_ID1" in out
    # The infra job should NOT appear in compute-only view
    assert "create_dir" not in out


def test_make_job_table_show_all_title(make_snapshot, make_job):
    jobs = [make_job(job_id=1, exec_job_id="create_dir", type_desc="create-dir")]
    snap = make_snapshot(jobs=jobs)
    out = render(display._make_job_table(snap, show_all=True))
    assert "All Jobs" in out
    assert "create_dir" in out


def test_make_job_table_empty_placeholder(make_snapshot):
    snap = make_snapshot(jobs=[])
    out = render(display._make_job_table(snap, show_all=False))
    assert "no jobs yet" in out


def test_make_job_table_with_condor_live_info(
    make_snapshot, make_job, condor_queue_ads
):
    job = make_job(
        job_id=1,
        exec_job_id="preprocess_ID0000001",
        type_desc="compute",
        raw_state="EXECUTE",
        end_time=None,
    )
    snap = make_snapshot(jobs=[job])
    # The job table has many no-wrap columns; the Live column only fits content
    # at a wide terminal, so render extra-wide to surface the host name.
    out = render(
        display._make_job_table(snap, show_all=False, condor_jobs=condor_queue_ads),
        width=300,
    )
    # live RemoteHost short name shows up in the Live column
    assert "worker1" in out


def test_make_job_table_with_history_info(make_snapshot, make_job, condor_history_ads):
    job = make_job(
        job_id=1,
        exec_job_id="preprocess_ID0000001",
        type_desc="compute",
        raw_state="JOB_SUCCESS",
    )
    snap = make_snapshot(jobs=[job])
    out = render(
        display._make_job_table(
            snap, show_all=False, condor_history=condor_history_ads
        ),
        width=300,
    )
    # completed history shows host short name
    assert "worker1" in out


def test_make_job_table_uses_transformation_display_name(make_snapshot, make_job):
    job = make_job(
        job_id=1,
        exec_job_id="preprocess_ID0000001",
        type_desc="compute",
        transformation="pegasus::preprocess",
    )
    snap = make_snapshot(jobs=[job])
    out = render(display._make_job_table(snap, show_all=False))
    assert "pegasus::preprocess" in out


# ─────────────────────────────────────────────────────────────────────────────
# _make_events_panel
# ─────────────────────────────────────────────────────────────────────────────


def test_make_events_panel_lists_submitted_jobs(make_snapshot, make_job):
    jobs = [
        make_job(job_id=1, exec_job_id="preprocess_ID1", raw_state="JOB_SUCCESS"),
        make_job(
            job_id=2,
            exec_job_id="unsub",
            raw_state=None,
            submit_time=None,
            start_time=None,
            end_time=None,
        ),
    ]
    snap = make_snapshot(jobs=jobs)
    out = render(display._make_events_panel(snap, n=15))
    assert "Recent Events" in out
    assert "preprocess_ID1" in out
    # Unsubmitted (raw_state None) jobs are excluded from the events panel
    assert "unsub" not in out


def test_make_events_panel_empty(make_snapshot):
    snap = make_snapshot(jobs=[])
    out = render(display._make_events_panel(snap, n=15))
    assert "no activity yet" in out


def test_make_events_panel_respects_n_limit(make_snapshot, make_job):
    jobs = [
        make_job(
            job_id=i,
            exec_job_id=f"job_{i}",
            raw_state="JOB_SUCCESS",
            end_time=100.0 + i,
        )
        for i in range(1, 6)
    ]
    snap = make_snapshot(jobs=jobs)
    out = render(display._make_events_panel(snap, n=2))
    # Only the 2 most recent (highest end_time) jobs: job_5 and job_4
    assert "job_5" in out
    assert "job_4" in out
    assert "job_1" not in out


# ─────────────────────────────────────────────────────────────────────────────
# _make_infra_summary
# ─────────────────────────────────────────────────────────────────────────────


def test_make_infra_summary_counts(make_snapshot, make_job):
    jobs = [
        make_job(job_id=1, exec_job_id="c1", type_desc="create-dir"),
        make_job(job_id=2, exec_job_id="c2", type_desc="create-dir"),
        make_job(job_id=3, exec_job_id="comp", type_desc="compute"),
    ]
    snap = make_snapshot(jobs=jobs)
    panel = display._make_infra_summary(snap)
    out = render(panel)
    assert "Auxiliary Jobs" in out
    assert "dir-create" in out  # JOB_TYPE_LABEL maps create-dir -> dir-create
    assert "2" in out


def test_make_infra_summary_none(make_snapshot, make_job):
    jobs = [make_job(job_id=1, exec_job_id="comp", type_desc="compute")]
    snap = make_snapshot(jobs=jobs)
    out = render(display._make_infra_summary(snap))
    assert "(none)" in out


# ─────────────────────────────────────────────────────────────────────────────
# _make_pool_panel
# ─────────────────────────────────────────────────────────────────────────────


def test_make_pool_panel_basic():
    panel = display._make_pool_panel(make_pool())
    assert isinstance(panel, Panel)
    out = render(panel)
    assert "Pool Resources" in out
    assert "Machines" in out
    assert "Slots" in out
    assert "CPUs" in out
    assert "Memory" in out
    assert "LINUX/X86_64" in out
    # No GPUs row when total_gpus == 0
    assert "GPUs" not in out


def test_make_pool_panel_with_gpus():
    out = render(display._make_pool_panel(make_pool(total_gpus=4, idle_gpus=1)))
    assert "GPUs" in out


# ─────────────────────────────────────────────────────────────────────────────
# _make_diagnostics_panel
# ─────────────────────────────────────────────────────────────────────────────


def test_make_diagnostics_panel_no_issues(make_snapshot):
    snap = make_snapshot(states=["SUCCESS", "RUNNING"])
    panel = display._make_diagnostics_panel(snap)
    assert isinstance(panel, Panel)
    out = render(panel)
    assert "Diagnostics" in out
    assert "No issues detected" in out


def test_make_diagnostics_panel_with_held_job(make_snapshot, make_job):
    held = make_job(
        job_id=1,
        exec_job_id="analyze_ID0000004",
        type_desc="compute",
        raw_state="JOB_HELD",
    )
    snap = make_snapshot(jobs=[held])
    out = render(display._make_diagnostics_panel(snap))
    assert "Diagnostics" in out
    assert "analyze_ID0000004" in out


def test_make_diagnostics_panel_with_failed_job(make_snapshot, make_job):
    failed = make_job(
        job_id=1,
        exec_job_id="analyze_ID0000004",
        type_desc="compute",
        raw_state="JOB_FAILURE",
        exitcode=1,
    )
    snap = make_snapshot(jobs=[failed])
    out = render(display._make_diagnostics_panel(snap))
    assert "analyze_ID0000004" in out


# ─────────────────────────────────────────────────────────────────────────────
# _make_stall_alert_panel
# ─────────────────────────────────────────────────────────────────────────────


def test_make_stall_alert_panel_with_stall_event():
    alerts = [
        {
            "event_type": "stall_detected",
            "stall_type": "no_running_jobs",
            "details": "no jobs progressing for 120s",
        },
        {
            "event_type": "idle_diagnosis",
            "findings": ["No machines match RequestMemory", "Negotiator idle"],
            "suggestions": ["Reduce RequestMemory"],
        },
        {
            "event_type": "hold_diagnosis",
            "job_name": "analyze_ID0000004",
            "summary": "transfer output failure",
        },
    ]
    panel = display._make_stall_alert_panel(alerts, diag_path="/tmp/diag.jsonl")
    assert isinstance(panel, Panel)
    out = render(panel)
    assert "STALL DETECTED" in out
    assert "no_running_jobs" in out
    assert "No machines match RequestMemory" in out
    assert "Reduce RequestMemory" in out
    assert "analyze_ID0000004" in out
    assert "/tmp/diag.jsonl" in out


def test_make_stall_alert_panel_empty_fallback():
    out = render(display._make_stall_alert_panel([]))
    # With no usable content, the body falls back to "Diagnostics active"
    assert "Diagnostics active" in out


# ─────────────────────────────────────────────────────────────────────────────
# build_layout
# ─────────────────────────────────────────────────────────────────────────────


def test_build_layout_running_snapshot(make_snapshot):
    info = make_info()
    snap = make_snapshot(
        states=["SUCCESS", "RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED"
    )
    layout = display.build_layout(
        info,
        snap,
        show_all=False,
        condor_jobs=None,
        events_n=10,
        refresh_ts=time.time(),
    )
    assert isinstance(layout, Layout)
    out = render(layout)
    assert "Pegasus Workflow Monitor" in out
    assert "Workflow Status" in out


def test_build_layout_completed_snapshot_with_pool(make_snapshot, condor_history_ads):
    info = make_info()
    snap = make_snapshot(
        states=["SUCCESS", "SUCCESS"],
        wf_state="WORKFLOW_TERMINATED",
        wf_status=0,
    )
    layout = display.build_layout(
        info,
        snap,
        show_all=True,
        condor_jobs=None,
        events_n=8,
        refresh_ts=time.time(),
        condor_history=condor_history_ads,
        pool_status=make_pool(),
    )
    assert isinstance(layout, Layout)
    out = render(layout)
    assert "Pool Resources" in out


def test_build_layout_with_issues_and_alerts(make_snapshot, make_job):
    info = make_info()
    held = make_job(
        job_id=1, exec_job_id="analyze_ID4", type_desc="compute", raw_state="JOB_HELD"
    )
    snap = make_snapshot(jobs=[held], wf_state="WORKFLOW_STARTED")
    alerts = [
        {
            "event_type": "stall_detected",
            "stall_type": "held_jobs",
            "details": "1 job held",
        }
    ]
    layout = display.build_layout(
        info,
        snap,
        show_all=False,
        condor_jobs=None,
        events_n=5,
        refresh_ts=time.time(),
        diag_alerts=alerts,
        diag_path="/tmp/diag.jsonl",
        condor_offline=True,
    )
    out = render(layout)
    # Both the stall alert panel and the diagnostics panel should appear
    assert "STALL DETECTED" in out
    assert "Diagnostics" in out


# ─────────────────────────────────────────────────────────────────────────────
# _print_stats_block / _print_final_summary
# ─────────────────────────────────────────────────────────────────────────────


def _diamond_stats(diamond_db) -> WorkflowStats:
    with StampedeDB(diamond_db, wf_uuid="test-wf-uuid") as db:
        snap = db.snapshot()
    return compute_workflow_stats(snap)


def test_print_stats_block_labels(diamond_db):
    stats = _diamond_stats(diamond_db)
    con = Console(file=io.StringIO(), width=120, force_terminal=False)
    display._print_stats_block(con, stats)
    out = con.file.getvalue()
    assert "Workflow Statistics" in out
    assert "Jobs" in out
    assert "compute" in out


def test_print_stats_block_handcrafted_fields():
    stats = WorkflowStats(
        total_jobs=4,
        compute_jobs=4,
        infra_jobs=0,
        succeeded=4,
        wall_time=206.0,
        total_compute_time=140.0,
        parallelism=2.0,
        dur_min=30.0,
        dur_max=50.0,
        dur_mean=40.0,
        dur_median=40.0,
        longest_job_name="analyze",
        shortest_job_name="findrange",
        peak_maxrss_kb=60000,
        peak_maxrss_job="analyze",
        mean_maxrss_kb=49290.0,
        cpu_eff_min=0.5,
        cpu_eff_max=0.95,
        cpu_eff_mean=0.8,
        wait_mean=5.0,
        wait_min=2.0,
        wait_max=10.0,
        cpu_seconds=7200.0,
        retry_count=1,
        hosts=["worker1", "worker2"],
        pool_machines=2,
        pool_total_cpus=16,
    )
    con = Console(file=io.StringIO(), width=120, force_terminal=False)
    display._print_stats_block(con, stats)
    out = con.file.getvalue()
    assert "Wall time" in out
    assert "Compute time" in out
    assert "parallelism" in out
    assert "Longest job" in out
    assert "analyze" in out
    assert "CPU efficiency" in out
    assert "80%" in out
    assert "Queue wait" in out
    assert "CPU consumed" in out
    assert "2.00 hours" in out
    assert "Retries" in out
    assert "Hosts" in out
    assert "Pool" in out


def test_print_final_summary_success(diamond_db):
    with StampedeDB(diamond_db, wf_uuid="test-wf-uuid") as db:
        snap = db.snapshot()
    stats = compute_workflow_stats(snap)
    con = Console(file=io.StringIO(), width=120, force_terminal=False)
    display._print_final_summary(con, snap, stats=stats)
    out = con.file.getvalue()
    assert "completed successfully" in out
    assert "Workflow Statistics" in out


def test_print_final_summary_failed(make_snapshot, make_job):
    failed = make_job(
        job_id=1,
        exec_job_id="analyze_ID4",
        type_desc="compute",
        raw_state="JOB_FAILURE",
        exitcode=1,
    )
    snap = make_snapshot(jobs=[failed], wf_state="WORKFLOW_TERMINATED", wf_status=1)
    con = Console(file=io.StringIO(), width=120, force_terminal=False)
    display._print_final_summary(con, snap)
    out = con.file.getvalue()
    assert "FAILED" in out
    assert "analyze_ID4" in out


def test_print_final_summary_monitoring_stopped(make_snapshot, make_job):
    # Workflow not complete (still WORKFLOW_STARTED) -> "Monitoring stopped"
    held = make_job(
        job_id=1, exec_job_id="held_job", type_desc="compute", raw_state="JOB_HELD"
    )
    snap = make_snapshot(jobs=[held], wf_state="WORKFLOW_STARTED")
    con = Console(file=io.StringIO(), width=120, force_terminal=False)
    display._print_final_summary(con, snap)
    out = con.file.getvalue()
    assert "Monitoring stopped" in out
    assert "held" in out.lower()
