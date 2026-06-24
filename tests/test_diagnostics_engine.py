"""Unit tests for :mod:`workflow_monitor.diagnostics_engine`.

The engine owns a :class:`StallDetector` and a :class:`DiagnosticLogger`
writing to a JSONL sidecar. We construct a minimal :class:`WorkflowInfo`
directly (no real braindump needed) and point the log at a temp path.

Strategy for the stall-driven ``_auto_diagnose`` branch: rather than drive
the time-based detector to a confirmed stall, we monkeypatch
``engine._detector.check`` to return a synthetic ``stall_detected`` dict. The
spec explicitly endorses this as a clean way to deterministically exercise the
auto-diagnose path. The ``why_idle`` helpers are monkeypatched so no real
condor is contacted.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pytest

from workflow_monitor.braindump import WorkflowInfo
from workflow_monitor.diagnostics_engine import DiagnosticsEngine


# ─────────────────────────────────────────────────────────────────────────────
# Helpers / fixtures
# ─────────────────────────────────────────────────────────────────────────────


def make_info(submit_dir: Path) -> WorkflowInfo:
    """A minimal WorkflowInfo sufficient for the engine."""
    return WorkflowInfo(
        wf_uuid="engine-wf-uuid",
        root_wf_uuid="engine-wf-uuid",
        dax_label="diamond",
        submit_dir=submit_dir,
        user="tester",
        planner_version="5.1.2",
        dag_file="diamond-0.dag",
        condor_log="diamond-0.log",
        timestamp="20260101T000000+0000",
        basedir=submit_dir.parent,
    )


@pytest.fixture
def engine(tmp_path):
    """A DiagnosticsEngine writing to a temp sidecar. Closed on teardown."""
    log_path = tmp_path / "diagnostics-events.jsonl"
    info = make_info(tmp_path)
    eng = DiagnosticsEngine(info, diag_log_path=log_path, poll_interval=2.0)
    yield eng
    try:
        eng.close()
    except Exception:
        pass


def read_events(path: Path) -> List[dict]:
    """Parse every JSONL line in the sidecar."""
    lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
    return [json.loads(ln) for ln in lines]


# ─────────────────────────────────────────────────────────────────────────────
# Construction / path / logger lifecycle
# ─────────────────────────────────────────────────────────────────────────────


def test_path_property_points_at_log(engine, tmp_path):
    assert engine.path == tmp_path / "diagnostics-events.jsonl"
    assert engine.path.exists()


def test_construction_writes_diag_start(engine):
    events = read_events(engine.path)
    assert events[0]["event_type"] == "diag_start"
    assert events[0]["wf_uuid"] == "engine-wf-uuid"
    # engine_config mirrors the detector's knobs.
    assert events[0]["engine_config"]["poll_interval"] == 2.0
    assert "no_transition_seconds" in events[0]["engine_config"]


def test_close_writes_diag_end(tmp_path):
    log_path = tmp_path / "diagnostics-events.jsonl"
    eng = DiagnosticsEngine(make_info(tmp_path), diag_log_path=log_path)
    eng.close()
    events = read_events(log_path)
    assert events[-1]["event_type"] == "diag_end"
    assert events[-1]["wf_uuid"] == "engine-wf-uuid"


# ─────────────────────────────────────────────────────────────────────────────
# tick: held/failed diagnostics (always runs)
# ─────────────────────────────────────────────────────────────────────────────


def test_tick_no_problems_returns_empty(engine, make_snapshot):
    snap = make_snapshot(states=["RUNNING", "SUCCESS"], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None, high_water_ts=0.0)
    assert out == []


def test_tick_emits_hold_diagnosis_and_writes_sidecar(engine, make_snapshot):
    snap = make_snapshot(states=["HELD"], wf_state="WORKFLOW_STARTED")
    condor_jobs = [
        {
            "DAGNodeName": "job_ID0000001",
            "HoldReason": "Error from slot1@w2: Transfer output files failure",
        }
    ]
    out = engine.tick(snap, condor_jobs=condor_jobs, pool=None, high_water_ts=0.0)
    assert len(out) == 1
    ev = out[0]
    assert ev["event_type"] == "hold_diagnosis"
    assert ev["severity"] == "held"
    assert ev["job_name"] == "job_ID0000001"
    assert ev["summary"] == "Output file transfer failed"
    assert "Transfer output files failure" in ev["reason"]
    assert isinstance(ev["suggestions"], list) and ev["suggestions"]

    # Event was also persisted to the sidecar.
    persisted = read_events(engine.path)
    assert any(e["event_type"] == "hold_diagnosis" for e in persisted)


def test_tick_emits_failure_diagnosis(engine, make_snapshot, make_job):
    failed = make_job(
        job_id=7,
        exec_job_id="analyze_ID0000007",
        raw_state="JOB_FAILURE",
        exitcode=256,  # raw wait status -> real exit code 1
    )
    snap = make_snapshot(jobs=[failed], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None, high_water_ts=0.0)
    assert len(out) == 1
    ev = out[0]
    assert ev["event_type"] == "failure_diagnosis"
    assert ev["severity"] == "failed"
    assert ev["job_name"] == "analyze_ID0000007"
    assert "exit code 1" in ev["summary"].lower()


def test_held_failed_dedup_across_ticks(engine, make_snapshot):
    snap = make_snapshot(states=["HELD"], wf_state="WORKFLOW_STARTED")
    condor_jobs = [
        {"DAGNodeName": "job_ID0000001", "HoldReason": "memory usage exceeded"}
    ]
    first = engine.tick(snap, condor_jobs=condor_jobs, pool=None)
    assert len(first) == 1
    # Same held job on the next tick is NOT re-emitted (dedup key held:name).
    second = engine.tick(snap, condor_jobs=condor_jobs, pool=None)
    assert second == []
    assert "held:job_ID0000001" in engine._detector.state.diagnosed_jobs


def test_held_and_failed_both_diagnosed(engine, make_snapshot, make_job):
    held = make_job(job_id=1, exec_job_id="h_ID0000001", raw_state="JOB_HELD")
    failed = make_job(
        job_id=2, exec_job_id="f_ID0000002", raw_state="JOB_FAILURE", exitcode=256
    )
    snap = make_snapshot(jobs=[held, failed], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None)
    by_type = {e["event_type"] for e in out}
    assert by_type == {"hold_diagnosis", "failure_diagnosis"}


def test_held_failed_error_emits_diag_error(engine, make_snapshot, monkeypatch):
    def boom(**kwargs):
        raise RuntimeError("collect blew up")

    # The engine does `from .diagnostics import collect_diagnostics` lazily,
    # so patch the name on the diagnostics module itself.
    import workflow_monitor.diagnostics as diag_mod

    monkeypatch.setattr(diag_mod, "collect_diagnostics", boom)
    snap = make_snapshot(states=["HELD"], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None)
    assert len(out) == 1
    ev = out[0]
    assert ev["event_type"] == "diag_error"
    assert ev["stage"] == "held_failed"
    assert "collect blew up" in ev["error"]


# ─────────────────────────────────────────────────────────────────────────────
# tick: stall detection plumbing
# ─────────────────────────────────────────────────────────────────────────────


def _stall_event() -> dict:
    return {
        "event_type": "stall_detected",
        "timestamp": 1234.0,
        "stall_type": "idle_too_long",
        "details": "synthetic",
        "total_jobs": 2,
        "done_jobs": 0,
        "running_jobs": 0,
        "queued_jobs": 2,
        "held_jobs": 0,
        "failed_jobs": 0,
        "seconds_since_progress": 30.0,
    }


def test_stall_resolved_is_logged_but_no_autodiagnose(
    engine, make_snapshot, monkeypatch
):
    """A stall_resolved event is logged/returned but must NOT trigger
    _auto_diagnose (only stall_detected does)."""
    resolved = {
        "event_type": "stall_resolved",
        "timestamp": 1.0,
        "stall_type": "idle_too_long",
        "stall_duration_seconds": 5.0,
        "resolution": "Progress resumed",
    }
    monkeypatch.setattr(engine._detector, "check", lambda snap, hw: resolved)

    called = {"auto": False}

    def spy(*a, **k):
        called["auto"] = True
        return []

    monkeypatch.setattr(engine, "_auto_diagnose", spy)

    snap = make_snapshot(states=["QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None)
    assert resolved in out
    assert called["auto"] is False
    persisted = read_events(engine.path)
    assert any(e["event_type"] == "stall_resolved" for e in persisted)


def test_stall_detected_triggers_autodiagnose(engine, make_snapshot, monkeypatch):
    monkeypatch.setattr(engine._detector, "check", lambda snap, hw: _stall_event())

    # Stub the why_idle queries so no real condor is contacted, and have
    # _analyze run on the snapshot to produce a real IdleDiagnosis.
    import workflow_monitor.why_idle as wi

    monkeypatch.setattr(wi, "_get_idle_job_details", lambda **k: [])
    monkeypatch.setattr(wi, "_query_userprio", lambda **k: [])
    monkeypatch.setattr(wi, "_query_negotiator", lambda **k: None)

    # 2 queued jobs so queued_count() > 0 and _analyze finds idle jobs.
    snap = make_snapshot(states=["QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None)

    types = [e["event_type"] for e in out]
    assert "stall_detected" in types
    assert "idle_diagnosis" in types
    # Order: stall first, then auto-diagnosis.
    assert types.index("stall_detected") < types.index("idle_diagnosis")

    idle = next(e for e in out if e["event_type"] == "idle_diagnosis")
    assert idle["idle_job_count"] == 2
    assert "findings" in idle and "suggestions" in idle
    assert "requirement_mismatches" in idle

    persisted = read_events(engine.path)
    assert any(e["event_type"] == "idle_diagnosis" for e in persisted)


def test_autodiagnose_skipped_when_no_queued(engine, make_snapshot, monkeypatch):
    """_auto_diagnose returns early (no idle_diagnosis) when queued_count==0."""
    monkeypatch.setattr(engine._detector, "check", lambda snap, hw: _stall_event())

    import workflow_monitor.why_idle as wi

    # If _auto_diagnose did proceed, this would be called; assert it isn't.
    def fail_if_called(**k):
        raise AssertionError("_get_idle_job_details should not be called")

    monkeypatch.setattr(wi, "_get_idle_job_details", fail_if_called)

    # All running -> queued_count() == 0.
    snap = make_snapshot(states=["RUNNING", "RUNNING"], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None)
    types = [e["event_type"] for e in out]
    assert "stall_detected" in types
    assert "idle_diagnosis" not in types


def test_autodiagnose_error_emits_diag_error(engine, make_snapshot, monkeypatch):
    monkeypatch.setattr(engine._detector, "check", lambda snap, hw: _stall_event())

    import workflow_monitor.why_idle as wi

    def boom(**k):
        raise RuntimeError("userprio failed")

    monkeypatch.setattr(wi, "_get_idle_job_details", lambda **k: [])
    monkeypatch.setattr(wi, "_query_userprio", boom)
    monkeypatch.setattr(wi, "_query_negotiator", lambda **k: None)

    snap = make_snapshot(states=["QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED")
    out = engine.tick(snap, condor_jobs=None, pool=None)
    types = [e["event_type"] for e in out]
    assert "stall_detected" in types
    err = next(e for e in out if e["event_type"] == "diag_error")
    assert err["stage"] == "idle_diagnosis"
    assert "userprio failed" in err["error"]


def test_autodiagnose_passes_kwargs_to_queries(tmp_path, make_snapshot, monkeypatch):
    """condor_constraint / condor_kwargs flow into the why_idle query fns."""
    log_path = tmp_path / "diagnostics-events.jsonl"
    info = make_info(tmp_path)
    eng = DiagnosticsEngine(
        info,
        diag_log_path=log_path,
        condor_constraint="DAGManJobId == 42",
        condor_kwargs={"schedd_name": "schedd1", "collector_host": "coll1"},
    )
    try:
        monkeypatch.setattr(eng._detector, "check", lambda snap, hw: _stall_event())

        import workflow_monitor.why_idle as wi

        captured = {}

        def cap_idle(constraint=None, schedd_name=None):
            captured["constraint"] = constraint
            captured["schedd_name"] = schedd_name
            return []

        def cap_userprio(collector_host=None):
            captured["userprio_collector"] = collector_host
            return []

        def cap_negotiator(collector_host=None):
            captured["negotiator_collector"] = collector_host
            return None

        monkeypatch.setattr(wi, "_get_idle_job_details", cap_idle)
        monkeypatch.setattr(wi, "_query_userprio", cap_userprio)
        monkeypatch.setattr(wi, "_query_negotiator", cap_negotiator)

        snap = make_snapshot(states=["QUEUED"], wf_state="WORKFLOW_STARTED")
        eng.tick(snap, condor_jobs=None, pool=None)

        assert captured["constraint"] == "DAGManJobId == 42"
        assert captured["schedd_name"] == "schedd1"
        assert captured["userprio_collector"] == "coll1"
        assert captured["negotiator_collector"] == "coll1"
    finally:
        eng.close()


# ─────────────────────────────────────────────────────────────────────────────
# tick: stall + held/failed interplay in one cycle
# ─────────────────────────────────────────────────────────────────────────────


def test_tick_combines_stall_and_held_failed(engine, make_snapshot, monkeypatch):
    """One tick can emit a stall event AND a held/failed diagnosis."""
    monkeypatch.setattr(engine._detector, "check", lambda snap, hw: _stall_event())

    import workflow_monitor.why_idle as wi

    monkeypatch.setattr(wi, "_get_idle_job_details", lambda **k: [])
    monkeypatch.setattr(wi, "_query_userprio", lambda **k: [])
    monkeypatch.setattr(wi, "_query_negotiator", lambda **k: None)

    # One queued (for auto-diagnose) + one held (for held/failed diagnosis).
    snap = make_snapshot(states=["QUEUED", "HELD"], wf_state="WORKFLOW_STARTED")
    condor_jobs = [
        {"DAGNodeName": "job_ID0000002", "HoldReason": "disk usage exceeded"}
    ]
    out = engine.tick(snap, condor_jobs=condor_jobs, pool=None)
    types = [e["event_type"] for e in out]
    assert "stall_detected" in types
    assert "idle_diagnosis" in types
    assert "hold_diagnosis" in types
