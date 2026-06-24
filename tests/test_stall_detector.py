"""Unit tests for :mod:`workflow_monitor.stall_detector`.

The detector is time-driven: it reads ``time.time()`` once per ``check()``
call. We control the clock with a mutable holder and monkeypatch
``stall_detector.time.time`` so every assertion is deterministic.

Conventions used throughout
---------------------------
* ``startup_grace_seconds=0`` disables the grace window (``now - start < 0``
  is False once the clock advances), so the second ``check()`` can already
  trigger a heuristic.
* The FIRST ``check()`` call always returns None and *initializes* baselines
  (``workflow_start_time``, ``last_progress_time``, ``last_high_water_ts``,
  ``last_done_count``). We treat it as the "init" call.
* To hold "no progress", keep the roster (hence ``done_count``) flat and do
  not bump ``high_water_ts`` between calls.
* The two-strike machine needs the SAME triggered type on two consecutive
  calls before ``stall_detected`` is emitted.
"""

from __future__ import annotations

import pytest

from workflow_monitor import stall_detector
from workflow_monitor.stall_detector import StallDetector


# ─────────────────────────────────────────────────────────────────────────────
# Clock control
# ─────────────────────────────────────────────────────────────────────────────


class Clock:
    """Mutable now-holder; mutate ``.t`` between ``check()`` calls."""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock(monkeypatch):
    c = Clock(1000.0)
    monkeypatch.setattr(stall_detector.time, "time", c)
    return c


def make_detector(**overrides) -> StallDetector:
    """A detector with small, test-friendly thresholds by default."""
    cfg = dict(
        poll_interval=2.0,
        cooldown_seconds=100.0,
        no_transition_seconds=10.0,
        progress_plateau_seconds=20.0,
        idle_threshold_seconds=15.0,
        startup_grace_seconds=0.0,
    )
    cfg.update(overrides)
    return StallDetector(**cfg)


# ─────────────────────────────────────────────────────────────────────────────
# config / is_stalled / first-call init
# ─────────────────────────────────────────────────────────────────────────────


def test_config_exposes_all_knobs():
    det = StallDetector(
        poll_interval=3.0,
        cooldown_seconds=11.0,
        no_transition_seconds=22.0,
        progress_plateau_seconds=33.0,
        idle_threshold_seconds=44.0,
        startup_grace_seconds=55.0,
    )
    assert det.config == {
        "poll_interval": 3.0,
        "cooldown_seconds": 11.0,
        "no_transition_seconds": 22.0,
        "progress_plateau_seconds": 33.0,
        "idle_threshold_seconds": 44.0,
        "startup_grace_seconds": 55.0,
    }


def test_first_check_returns_none_and_initializes(clock, make_snapshot):
    det = make_detector()
    snap = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, high_water_ts=500.0) is None
    # Baselines were set from the first call.
    assert det.state.workflow_start_time == 1000.0
    assert det.state.last_progress_time == 1000.0
    assert det.state.last_high_water_ts == 500.0
    assert det.state.last_done_count == snap.done_count()
    assert det.is_stalled() is False


# ─────────────────────────────────────────────────────────────────────────────
# False-positive guards
# ─────────────────────────────────────────────────────────────────────────────


def test_complete_workflow_returns_none_when_not_confirmed(clock, make_snapshot):
    det = make_detector()
    init = make_snapshot(states=["RUNNING"], wf_state="WORKFLOW_STARTED")
    assert det.check(init, 0.0) is None  # init call
    clock.t += 100
    done = make_snapshot(
        states=["SUCCESS"], wf_state="WORKFLOW_TERMINATED", wf_status=0
    )
    # Complete + not confirmed -> _maybe_resolve resets silently, returns None.
    assert det.check(done, 0.0) is None
    assert det.is_stalled() is False


def test_total_zero_returns_none(clock, make_snapshot):
    det = make_detector()
    empty = make_snapshot(states=[], wf_state="WORKFLOW_STARTED")
    assert det.check(empty, 0.0) is None  # init
    clock.t += 1000
    assert det.check(empty, 0.0) is None  # total == 0 guard


def test_all_unsubmitted_returns_none(clock, make_snapshot):
    det = make_detector()
    snap = make_snapshot(
        states=["UNSUBMITTED", "UNSUBMITTED"], wf_state="WORKFLOW_STARTED"
    )
    assert det.check(snap, 0.0) is None  # init
    clock.t += 1000
    # unsubmitted == total guard fires before any heuristic.
    assert det.check(snap, 0.0) is None
    assert det.is_stalled() is False


# ─────────────────────────────────────────────────────────────────────────────
# Startup grace
# ─────────────────────────────────────────────────────────────────────────────


def test_startup_grace_suppresses_detection(clock, make_snapshot):
    det = make_detector(startup_grace_seconds=60.0)
    snap = make_snapshot(states=["QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init at t=1000
    # Advance well past no_transition threshold (10s) but stay inside grace.
    clock.t += 30  # t=1030, still < start+60
    assert det.check(snap, 0.0) is None
    clock.t += 20  # t=1050, still < start+60
    assert det.check(snap, 0.0) is None
    assert det.is_stalled() is False


def test_startup_grace_advances_progress_markers(clock, make_snapshot):
    det = make_detector(startup_grace_seconds=60.0)
    snap = make_snapshot(states=["QUEUED", "RUNNING"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 100.0) is None  # init: last_progress_time=1000
    clock.t += 20  # t=1020, inside grace
    # high_water bumps -> markers advance even though we return None in grace.
    assert det.check(snap, 200.0) is None
    assert det.state.last_progress_time == 1020.0
    assert det.state.last_high_water_ts == 200.0


# ─────────────────────────────────────────────────────────────────────────────
# Progress detection / resolution
# ─────────────────────────────────────────────────────────────────────────────


def test_progress_via_done_increase_returns_none_and_updates_markers(
    clock, make_snapshot
):
    det = make_detector()
    s0 = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(s0, 0.0) is None  # init, last_done=0
    clock.t += 5
    s1 = make_snapshot(states=["SUCCESS", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(s1, 0.0) is None  # done went 0 -> 1
    assert det.state.last_done_count == 1
    assert det.state.last_progress_time == 1005.0


def test_progress_via_high_water_bump_returns_none(clock, make_snapshot):
    det = make_detector()
    snap = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 100.0) is None  # init
    clock.t += 5
    assert det.check(snap, 150.0) is None  # high_water bumped -> progressed
    assert det.state.last_high_water_ts == 150.0
    assert det.state.last_progress_time == 1005.0


def test_progress_resumed_resolves_confirmed_stall(clock, make_snapshot):
    """A confirmed stall resolves to ``stall_resolved`` when progress resumes."""
    det = make_detector()
    snap = make_snapshot(
        states=["RUNNING", "QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED"
    )
    assert det.check(snap, 0.0) is None  # init at t=1000

    clock.t += 11  # t=1011, since_progress=11 >= 10 -> no_transitions strike 1
    assert det.check(snap, 0.0) is None
    clock.t += 1  # t=1012, strike 2 -> confirm
    ev = det.check(snap, 0.0)
    assert ev is not None and ev["event_type"] == "stall_detected"
    assert det.is_stalled() is True
    since = det.state.since

    # Now progress: a job completes.
    clock.t += 5  # t=1017
    progressed = make_snapshot(
        states=["SUCCESS", "QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED"
    )
    res = det.check(progressed, 0.0)
    assert res is not None
    assert res["event_type"] == "stall_resolved"
    assert res["stall_type"] == "no_transitions"
    assert res["resolution"] == "Progress resumed"
    assert res["stall_duration_seconds"] == round(1017.0 - since, 1)
    assert det.is_stalled() is False


# ─────────────────────────────────────────────────────────────────────────────
# Two-strike state machine
# ─────────────────────────────────────────────────────────────────────────────


def test_two_strike_confirms_on_second_call(clock, make_snapshot):
    det = make_detector()
    snap = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init at t=1000

    clock.t += 12  # since_progress=12 >= 10 -> first strike (armed, None)
    assert det.check(snap, 0.0) is None
    assert det.is_stalled() is False

    clock.t += 1  # second strike -> confirm
    ev = det.check(snap, 0.0)
    assert ev is not None
    assert ev["event_type"] == "stall_detected"
    assert ev["stall_type"] == "no_transitions"
    assert det.is_stalled() is True


def test_changing_triggered_type_rearms(clock, make_snapshot):
    """A different triggered type on the second call re-arms (returns None)."""
    det = make_detector(no_transition_seconds=10.0)
    # Roster with running+queued -> no_transitions can trigger.
    nt = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(nt, 0.0) is None  # init

    clock.t += 12  # first strike: no_transitions
    assert det.check(nt, 0.0) is None
    assert det._suspected_type == "no_transitions"

    # Now switch the roster so all active jobs are HELD -> all_held triggers.
    clock.t += 1
    held = make_snapshot(states=["HELD"], wf_state="WORKFLOW_STARTED")
    assert det.check(held, 0.0) is None  # type changed -> re-arm, None
    assert det._suspected_type == "all_held"
    assert det.is_stalled() is False

    # Second consecutive all_held -> confirm.
    clock.t += 1
    ev = det.check(held, 0.0)
    assert ev is not None and ev["stall_type"] == "all_held"


# ─────────────────────────────────────────────────────────────────────────────
# Heuristics in isolation
# ─────────────────────────────────────────────────────────────────────────────


def _confirm(det, snap, clock, first_gap, second_gap=1):
    """Drive two consecutive strikes; return the emitted event."""
    clock.t += first_gap
    assert det.check(snap, 0.0) is None  # strike 1
    clock.t += second_gap
    return det.check(snap, 0.0)  # strike 2 -> confirm


def test_all_held_heuristic(clock, make_snapshot):
    det = make_detector()
    # 1 done, 2 held, 0 failed -> active = 3 - 1 - 0 = 2 == held.
    snap = make_snapshot(
        states=["SUCCESS", "HELD", "HELD"], wf_state="WORKFLOW_STARTED"
    )
    assert det.check(snap, 0.0) is None  # init
    # all_held does not depend on time; just need two strikes.
    ev = _confirm(det, snap, clock, first_gap=1)
    assert ev is not None
    assert ev["stall_type"] == "all_held"
    assert ev["details"] == "All 2 active job(s) are HELD"
    assert ev["held_jobs"] == 2
    assert ev["done_jobs"] == 1
    assert ev["total_jobs"] == 3


def test_all_held_not_triggered_when_a_job_still_running(clock, make_snapshot):
    det = make_detector()
    # held=1 but active = 2 (held + running) so held != active.
    snap = make_snapshot(states=["HELD", "RUNNING"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init
    clock.t += 1
    # Not all_held; running>0 but since_progress (1s) < 10 -> nothing triggers.
    assert det.check(snap, 0.0) is None
    assert det._suspected_type is None


def test_no_transitions_heuristic(clock, make_snapshot):
    det = make_detector(no_transition_seconds=10.0)
    snap = make_snapshot(states=["QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init at t=1000
    ev = _confirm(det, snap, clock, first_gap=11)  # t=1011 then t=1012
    assert ev is not None
    assert ev["stall_type"] == "no_transitions"
    # seconds_since_progress at confirm = 1012 - 1000 = 12.
    assert ev["seconds_since_progress"] == 12.0
    assert "No job state transitions" in ev["details"]


def test_no_transitions_not_triggered_before_threshold(clock, make_snapshot):
    det = make_detector(no_transition_seconds=10.0)
    snap = make_snapshot(states=["QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init
    clock.t += 9  # below threshold
    assert det.check(snap, 0.0) is None
    assert det._suspected_type is None
    assert det.is_stalled() is False


def test_progress_plateau_heuristic(clock, make_snapshot):
    # Set no_transition far away so plateau wins (running>0 but we want the
    # plateau branch, which requires no_transition NOT to fire first).
    det = make_detector(no_transition_seconds=1000.0, progress_plateau_seconds=20.0)
    snap = make_snapshot(states=["RUNNING", "RUNNING"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init at t=1000
    ev = _confirm(det, snap, clock, first_gap=21)  # t=1021 then t=1022
    assert ev is not None
    assert ev["stall_type"] == "progress_plateau"
    assert ev["running_jobs"] == 2
    assert "Done count flat" in ev["details"]


def test_idle_too_long_heuristic(clock, make_snapshot):
    # queued>0, running==0. Keep no_transition large so idle_too_long branch
    # is the one that fires (no_transitions would otherwise pre-empt it).
    det = make_detector(no_transition_seconds=1000.0, idle_threshold_seconds=15.0)
    snap = make_snapshot(states=["QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init at t=1000
    ev = _confirm(det, snap, clock, first_gap=16)  # t=1016 then t=1017
    assert ev is not None
    assert ev["stall_type"] == "idle_too_long"
    assert ev["queued_jobs"] == 2
    assert ev["running_jobs"] == 0
    assert "queued with 0 running" in ev["details"]


def test_heuristic_priority_no_transitions_beats_idle(clock, make_snapshot):
    """When both no_transitions and idle_too_long would match, the higher
    priority (no_transitions) is chosen."""
    det = make_detector(no_transition_seconds=10.0, idle_threshold_seconds=10.0)
    snap = make_snapshot(states=["QUEUED", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init
    ev = _confirm(det, snap, clock, first_gap=11)
    assert ev is not None
    assert ev["stall_type"] == "no_transitions"


def test_event_payload_has_all_keys(clock, make_snapshot):
    det = make_detector()
    snap = make_snapshot(
        states=["SUCCESS", "RUNNING", "QUEUED", "HELD", "FAILED"],
        wf_state="WORKFLOW_STARTED",
    )
    assert det.check(snap, 0.0) is None  # init
    ev = _confirm(det, snap, clock, first_gap=11)
    assert ev is not None
    expected_keys = {
        "event_type",
        "timestamp",
        "stall_type",
        "details",
        "total_jobs",
        "done_jobs",
        "running_jobs",
        "queued_jobs",
        "held_jobs",
        "failed_jobs",
        "seconds_since_progress",
    }
    assert set(ev.keys()) == expected_keys
    assert ev["total_jobs"] == 5
    assert ev["done_jobs"] == 1
    assert ev["running_jobs"] == 1
    assert ev["queued_jobs"] == 1
    assert ev["held_jobs"] == 1
    assert ev["failed_jobs"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Cooldown
# ─────────────────────────────────────────────────────────────────────────────


def test_cooldown_suppresses_reemission_of_same_type(clock, make_snapshot):
    det = make_detector(no_transition_seconds=10.0, cooldown_seconds=100.0)
    snap = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init at t=1000

    clock.t += 11  # strike 1
    assert det.check(snap, 0.0) is None
    clock.t += 1  # strike 2 -> confirm at t=1012
    ev = det.check(snap, 0.0)
    assert ev is not None and ev["event_type"] == "stall_detected"
    emit_time = det.state.last_emit_time
    assert emit_time == 1012.0

    # Subsequent same-type matches within cooldown are suppressed.
    clock.t += 50  # t=1062, within 100s cooldown
    assert det.check(snap, 0.0) is None
    assert det.state.last_emit_time == emit_time  # unchanged
    clock.t += 30  # t=1092, still within cooldown
    assert det.check(snap, 0.0) is None
    assert det.state.last_emit_time == emit_time


def test_cooldown_expiry_allows_reemission(clock, make_snapshot):
    det = make_detector(no_transition_seconds=10.0, cooldown_seconds=100.0)
    snap = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(snap, 0.0) is None  # init at t=1000
    clock.t += 11
    assert det.check(snap, 0.0) is None  # strike 1
    clock.t += 1
    assert det.check(snap, 0.0) is not None  # confirm at t=1012

    # Past cooldown -> same-type stall re-emits.
    clock.t += 101  # t=1113, 101s since last emit (>= 100)
    ev = det.check(snap, 0.0)
    assert ev is not None
    assert ev["event_type"] == "stall_detected"
    assert ev["stall_type"] == "no_transitions"
    assert det.state.last_emit_time == 1113.0


# ─────────────────────────────────────────────────────────────────────────────
# _maybe_resolve direct behavior
# ─────────────────────────────────────────────────────────────────────────────


def test_maybe_resolve_silent_reset_when_not_confirmed(clock):
    det = make_detector()
    det._suspected_type = "no_transitions"
    det._suspected_since = 5.0
    det.state.status = "suspected"
    out = det._maybe_resolve(2000.0, "noop")
    assert out is None
    assert det.state.status == "normal"
    assert det.state.stall_type is None
    assert det._suspected_type is None


def test_maybe_resolve_clears_diagnosed_jobs_on_resolution(clock):
    det = make_detector()
    det.state.status = "confirmed"
    det.state.stall_type = "all_held"
    det.state.since = 1000.0
    det.state.diagnosed_jobs.add("held:job_X")
    out = det._maybe_resolve(1010.0, "Workflow completed")
    assert out is not None
    assert out["event_type"] == "stall_resolved"
    assert out["stall_type"] == "all_held"
    assert out["stall_duration_seconds"] == 10.0
    assert out["resolution"] == "Workflow completed"
    assert det.state.diagnosed_jobs == set()
    assert det.state.status == "normal"


def test_complete_resolves_confirmed_stall_via_check(clock, make_snapshot):
    """End-to-end: confirm a stall, then a complete snapshot resolves it."""
    det = make_detector(no_transition_seconds=10.0)
    running = make_snapshot(states=["RUNNING", "QUEUED"], wf_state="WORKFLOW_STARTED")
    assert det.check(running, 0.0) is None  # init
    clock.t += 11
    assert det.check(running, 0.0) is None  # strike 1
    clock.t += 1
    assert det.check(running, 0.0) is not None  # confirm

    clock.t += 5
    complete = make_snapshot(
        states=["SUCCESS", "SUCCESS"], wf_state="WORKFLOW_TERMINATED", wf_status=0
    )
    res = det.check(complete, 0.0)
    assert res is not None
    assert res["event_type"] == "stall_resolved"
    assert res["resolution"] == "Workflow completed"
    assert det.is_stalled() is False
