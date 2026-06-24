"""Regression coverage driven by REAL captured event logs in ``example-logs/``.

The six fixtures here are not synthetic — they were captured from real Pegasus
runs and exercise scale (Montage at 325 jobs), real failures (mask-error), and
every event type the hand-built fixtures in ``test_replay.py`` / ``test_remote.py``
cannot reproduce.  Each log is reconstructed through BOTH independent readers —
:class:`ReplayEngine` (offline replay) and :class:`RemoteEngine` (SSH client) —
and the resulting :class:`WorkflowSnapshot` is asserted against known invariants.

No network, no Rich ``Live`` TUI:  ``ReplayEngine.run()`` / ``RemoteEngine.run()``
are never called.  Instead we drive the pure load/reconstruct primitives exactly
the way ``run()`` seeds and folds them, so the totals are faithful.

The expected per-file numbers below were measured against the current source and
are treated as the regression baseline.  A divergence here is a real source
regression, not a flaky assertion.
"""

from __future__ import annotations

import io
import json
import pathlib
from typing import Any, Dict, List, Optional

import pytest

from rich.console import Console

from workflow_monitor.display import build_layout
from workflow_monitor.remote import RemoteEngine
from workflow_monitor.replay import ReplayEngine


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures location + expected reconstructed final-snapshot stats
# ─────────────────────────────────────────────────────────────────────────────

EXAMPLE_DIR = pathlib.Path(__file__).resolve().parent.parent / "example-logs"

# NOTE: the astro filename is misspelled "worklfow" on disk — use the real name.
EXPECTED: Dict[str, Dict[str, Any]] = {
    "ai-lung-workflow-events.jsonl": {
        "total_jobs": 34,
        "done": 34,
        "failed": 0,
        "held": 0,
        "wf_state": "WORKFLOW_TERMINATED",
    },
    "astro-montage-worklfow-events.jsonl": {
        "total_jobs": 325,
        "done": 318,
        "failed": 0,
        "held": 0,
        "wf_state": "WORKFLOW_TERMINATED",
    },
    "eq-workflow-events.jsonl": {
        "total_jobs": 24,
        "done": 24,
        "failed": 0,
        "held": 0,
        "wf_state": "WORKFLOW_TERMINATED",
    },
    "mask-error-workflow-events.jsonl": {
        "total_jobs": 31,
        "done": 16,
        "failed": 1,
        "held": 0,
        "wf_state": "WORKFLOW_TERMINATED",
    },
    "ml-crop-workflow-events.jsonl": {
        "total_jobs": 17,
        "done": 17,
        "failed": 0,
        "held": 0,
        "wf_state": "WORKFLOW_TERMINATED",
    },
    "qs-workflow-events.jsonl": {
        "total_jobs": 12,
        "done": 12,
        "failed": 0,
        "held": 0,
        "wf_state": "WORKFLOW_TERMINATED",
    },
}

ALL_FILES = sorted(EXPECTED)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _require(name: str) -> pathlib.Path:
    """Return the path to a fixture, skipping gracefully if absent."""
    path = EXAMPLE_DIR / name
    if not path.exists():
        pytest.skip(f"example log not present: {path}")
    return path


def reconstruct_replay(path: pathlib.Path):
    """Fold every frame through ReplayEngine._reconstruct() exactly the way
    ReplayEngine.run() does — seeding job_state from the pre-scanned roster —
    but without Rich Live or sleeps.  Returns the final WorkflowSnapshot.
    """
    eng = ReplayEngine(path, speed=0)
    frames = eng._build_frames()

    # Seed job_state from the pre-scanned roster, mirroring run().
    job_state: Dict[int, Dict[str, Any]] = {}
    for jid, jmeta in eng._prescan_jobs.items():
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
    snap = None

    for frame in frames:
        frame_ts = float(frame[0].get("timestamp", 0.0))
        snap, wf_state, wf_status, wf_start, wf_end = eng._reconstruct(
            frame,
            job_state,
            wf_state,
            wf_status,
            wf_start,
            wf_end,
            recent_events,
            frame_ts,
        )

    assert snap is not None, f"no frames reconstructed for {path}"
    return eng, snap


def reconstruct_remote(path: pathlib.Path):
    """Feed a log through RemoteEngine's pure event-folding primitives.

    SSH and tempfile setup are bypassed entirely (no __init__, no network):
    we instantiate via __new__, set the state __init__ would have set, then
    drive _process_header (first event) + _apply_event (rest) + _finalize_batch
    exactly as run() does after a sync.  Returns the final WorkflowSnapshot.
    """
    eng = RemoteEngine.__new__(RemoteEngine)
    # Replicate the slice of __init__ state the folding primitives touch.
    eng._events_n = 15
    eng._info = None
    eng._job_state = {}
    eng._wf_state = "UNKNOWN"
    eng._wf_status = None
    eng._wf_start = None
    eng._wf_end = None
    eng._recent_events = []
    eng._header_wf_start = None
    eng._prescan_jobs = {}
    eng._processed_lines = 0
    eng._workflow_complete = False
    eng._condor_jobs = None
    eng._condor_history = None
    eng._pool_status = None
    eng._workflow_stats = None
    eng._pending_workflow_end = None

    with open(path) as fh:
        events = [json.loads(line) for line in fh if line.strip()]

    assert events, f"empty log: {path}"
    assert events[0].get("event_type") == "workflow_start"

    # First event is the header → _process_header; the rest → _apply_event.
    eng._process_header(events[0])
    for ev in events[1:]:
        eng._apply_event(ev)
    eng._finalize_batch()

    return eng, eng._build_snapshot()


def _stats(snap) -> Dict[str, Any]:
    return {
        "total_jobs": snap.total_jobs(),
        "done": snap.done_count(),
        "failed": snap.failed_count(),
        "held": snap.held_count(),
        "wf_state": snap.wf_state,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1. Parametrized load + replay reconstruction over all 6 files
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", ALL_FILES)
def test_replay_reconstruction_matches_table(name):
    """Each real log folds via ReplayEngine to its known final-snapshot stats."""
    path = _require(name)
    _eng, snap = reconstruct_replay(path)

    expected = EXPECTED[name]
    assert snap.total_jobs() == expected["total_jobs"], (
        f"{name}: total_jobs {snap.total_jobs()} != {expected['total_jobs']}"
    )
    assert snap.done_count() == expected["done"], (
        f"{name}: done {snap.done_count()} != {expected['done']}"
    )
    assert snap.failed_count() == expected["failed"], (
        f"{name}: failed {snap.failed_count()} != {expected['failed']}"
    )
    assert snap.held_count() == expected["held"], (
        f"{name}: held {snap.held_count()} != {expected['held']}"
    )
    assert snap.wf_state == expected["wf_state"], (
        f"{name}: wf_state {snap.wf_state!r} != {expected['wf_state']!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Replay and remote agree on real data
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", ALL_FILES)
def test_replay_and_remote_agree(name):
    """The two independent readers must reconstruct identical totals from the
    same real log.  This cross-checks ReplayEngine vs RemoteEngine end-to-end."""
    path = _require(name)

    _r_eng, replay_snap = reconstruct_replay(path)
    _x_eng, remote_snap = reconstruct_remote(path)

    replay = _stats(replay_snap)
    remote = _stats(remote_snap)

    for key in ("total_jobs", "done", "failed", "held"):
        assert replay[key] == remote[key], (
            f"{name}: replay {key}={replay[key]} != remote {key}={remote[key]}"
        )

    # And both must still match the recorded baseline.
    expected = EXPECTED[name]
    for key in ("total_jobs", "done", "failed", "held", "wf_state"):
        assert replay[key] == expected[key], (
            f"{name}: replay {key}={replay[key]} != expected {expected[key]}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 3. mask-error failure detail — the failure path on real data
# ─────────────────────────────────────────────────────────────────────────────


def test_mask_error_has_exactly_one_failed_job():
    """The mask-error workflow records exactly one FAILED job; locate it."""
    path = _require("mask-error-workflow-events.jsonl")

    _eng, snap = reconstruct_replay(path)

    failed = [j for j in snap.jobs if j.disp_state == "FAILED"]
    assert len(failed) == 1, (
        f"expected exactly 1 FAILED job, got {len(failed)}: "
        f"{[j.exec_job_id for j in failed]}"
    )
    # snap.failed_jobs() must agree with the manual scan.
    assert len(snap.failed_jobs()) == 1
    assert snap.failed_count() == 1

    failed_job = failed[0]
    assert failed_job.exec_job_id, "failed job has no exec_job_id"
    assert failed_job.disp_state == "FAILED"

    # Remote reader must surface the same single failure.
    _x_eng, remote_snap = reconstruct_remote(path)
    remote_failed = [j for j in remote_snap.jobs if j.disp_state == "FAILED"]
    assert len(remote_failed) == 1
    assert remote_failed[0].exec_job_id == failed_job.exec_job_id


# ─────────────────────────────────────────────────────────────────────────────
# 4. Header / info parse
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", ALL_FILES)
def test_header_info_parsed(name):
    """The workflow_start header yields a non-empty wf_uuid and dax_label."""
    path = _require(name)
    eng = ReplayEngine(path, speed=0)
    info = eng.info

    assert info.wf_uuid, f"{name}: empty wf_uuid"
    assert info.wf_uuid != "unknown", f"{name}: wf_uuid not parsed from header"
    assert info.dax_label, f"{name}: empty dax_label"
    assert info.dax_label != "unknown", f"{name}: dax_label not parsed from header"

    # RemoteEngine._process_header must derive the same identity.
    remote_eng, _snap = reconstruct_remote(path)
    assert remote_eng._info is not None
    assert remote_eng._info.wf_uuid == info.wf_uuid
    assert remote_eng._info.dax_label == info.dax_label


# ─────────────────────────────────────────────────────────────────────────────
# 5. Render smoke at scale (astro-montage, 325 jobs)
# ─────────────────────────────────────────────────────────────────────────────


def test_render_layout_at_scale_does_not_raise():
    """Rendering the reconstructed large-workflow snapshot must not raise."""
    path = _require("astro-montage-worklfow-events.jsonl")

    eng, snap = reconstruct_replay(path)
    info = eng.info

    console = Console(file=io.StringIO(), width=120)
    layout = build_layout(
        info,
        snap,
        show_all=False,
        condor_jobs=None,
        events_n=15,
        refresh_ts=snap.poll_time,
    )
    console.print(layout)  # must not raise on real data at scale

    output = console.file.getvalue()
    assert output, "build_layout rendered no output for the Montage workflow"

    # Also smoke the show_all path (renders the infrastructure jobs too).
    console2 = Console(file=io.StringIO(), width=120)
    layout2 = build_layout(
        info,
        snap,
        show_all=True,
        condor_jobs=None,
        events_n=15,
        refresh_ts=snap.poll_time,
    )
    console2.print(layout2)
    assert console2.file.getvalue()
