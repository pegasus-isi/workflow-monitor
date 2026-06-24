"""Round-trip contract tests: writer (EventLogger) -> readers (Replay/Remote).

The JSONL event log is a contract between the *writer* (:class:`EventLogger`,
fed by a real :class:`StampedeDB`) and the two *readers* that reconstruct a
``WorkflowSnapshot`` from it offline: :class:`ReplayEngine` (file replay) and
:class:`RemoteEngine` (SSH-fetched log). Elsewhere these three are tested in
isolation; here we prove they agree end-to-end — data the writer emits is
reconstructed identically by both readers.

What we assert is the *observable* workflow state that drives the TUI and
diagnostics: roster (set of ``exec_job_id``), the count buckets
(done/failed/held/running/queued/total), per-job ``disp_state``, and the
terminal ``wf_state``. We do NOT call ``ReplayEngine.run()`` or
``RemoteEngine.run()`` (they start a Rich Live TUI / loop); instead we drive
the pure load/fold/reconstruct methods directly. No network: the RemoteEngine
is fed events in memory via ``_process_header`` / ``_apply_event`` and never
performs an SSH fetch.

Fields that do NOT round-trip through the log, and why (documented, not
asserted as equal):

* ``site`` — the logger's ``job_state`` records never carry ``site`` (see
  ``EventLogger._record_job_events``), so both readers reconstruct ``site=None``
  regardless of the DB value. Not part of the observable contract under test.
* ``submit_time`` / ``start_time`` / ``end_time`` / ``duration`` — derived from
  per-event timestamps. They survive in shape (SUBMIT->submit_time,
  EXECUTE->start_time, terminal->end_time) and we assert ``duration`` for
  completed compute jobs where it is well-defined, but absolute timestamps are
  the DB's, not the logger's wall clock, so we compare durations not instants.
* ``maxrss`` — emitted only on ``job_state`` events that carry it; for a
  completed job the DB attaches maxrss to the terminal transition, so it does
  survive and we spot-check it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from workflow_monitor.braindump import WorkflowInfo
from workflow_monitor.db import StampedeDB, WorkflowSnapshot
from workflow_monitor.event_log import EventLogger
from workflow_monitor.remote import RemoteEngine
from workflow_monitor.replay import ReplayEngine


WF_UUID = "test-wf-uuid"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _make_info(submit_dir: Path) -> WorkflowInfo:
    """A WorkflowInfo whose wf_uuid matches the DB built by the fixtures."""
    return WorkflowInfo(
        wf_uuid=WF_UUID,
        root_wf_uuid=WF_UUID,
        dax_label="diamond",
        submit_dir=submit_dir,
        user="tester",
        planner_version="5.1.2",
        dag_file="diamond-0.dag",
        condor_log="diamond-0.log",
        timestamp="20260101T000000+0000",
        basedir=submit_dir.parent,
    )


def _open_db(path: Path) -> StampedeDB:
    db = StampedeDB(path, wf_uuid=WF_UUID)
    db.connect()
    return db


def _write_log(db_path: Path, log_path: Path) -> WorkflowSnapshot:
    """Drive the real writer once (record + close) against the real DB.

    Returns the source snapshot the writer was fed, so callers can compare the
    readers' reconstruction against the ground truth.
    """
    db = _open_db(db_path)
    try:
        info = _make_info(log_path.parent)
        db_snap = db.snapshot()
        logger = EventLogger(info, db, log_path=log_path, min_free_mb=0)
        logger.record(db_snap)
        logger.close(db_snap)
        return db_snap
    finally:
        db.close()


def _parse_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _replay_snapshot(log_path: Path) -> WorkflowSnapshot:
    """Fold ALL frames through ReplayEngine, mirroring run() WITHOUT Live/sleep.

    This reproduces exactly what ``ReplayEngine.run()`` does to build the final
    on-screen snapshot: it seeds ``job_state`` from the pre-scanned roster, then
    folds every frame via ``_reconstruct``. We just drop the Rich/Live/sleep.
    """
    eng = ReplayEngine(log_path, speed=0)
    frames = eng._build_frames()
    assert frames, "expected at least one frame in the log"

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

    snap: Optional[WorkflowSnapshot] = None
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
    assert snap is not None
    return snap


def _remote_snapshot(events: List[Dict[str, Any]]) -> WorkflowSnapshot:
    """Feed events through RemoteEngine in order, then build the snapshot.

    No SSH ever runs: we construct the engine against a dummy spec purely to
    get its in-memory reconstruction state, then push events through the same
    methods ``run()`` uses (``_process_header`` for the header, ``_apply_event``
    for the rest), finalize the batch, and read back ``_build_snapshot()``.
    """
    eng = RemoteEngine("user@host:/path/to/workflow-events.jsonl")
    assert events and events[0].get("event_type") == "workflow_start"
    eng._process_header(events[0])
    for ev in events:
        eng._apply_event(ev)
    eng._finalize_batch()
    return eng._build_snapshot()


def _disp_by_exec(snap: WorkflowSnapshot) -> Dict[str, str]:
    """Map exec_job_id -> disp_state for every job in the snapshot."""
    return {j.exec_job_id: j.disp_state for j in snap.jobs}


def _counts(snap: WorkflowSnapshot) -> Dict[str, int]:
    return {
        "total": snap.total_jobs(),
        "done": snap.done_count(),
        "failed": snap.failed_count(),
        "held": snap.held_count(),
        "running": snap.running_count(),
        "queued": snap.queued_count(),
    }


def _roster(snap: WorkflowSnapshot) -> set:
    return {j.exec_job_id for j in snap.jobs}


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures: a workflow DB with a mix of outcomes
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mixed_db(db_factory, state_seqs):
    """A completed workflow with one of every diagnostics-relevant outcome.

    Roster (by exec_job_id):
      preprocess  -> SUCCESS  (compute, has transformation + maxrss)
      findrange   -> FAILED   (compute)
      analyze     -> HELD     (compute)
      hold_runner -> RUNNING  (compute)
      create-dir  -> SUCCESS  (infra)
    Workflow: WORKFLOW_STARTED + WORKFLOW_TERMINATED (terminal).
    """
    jobs = [
        {
            "job_id": 1,
            "exec_job_id": "preprocess_ID0000001",
            "type_desc": "compute",
            "states": state_seqs["SUCCESS"],
            "exitcode": 0,
            "site": "condorpool",
            "transformation": "pegasus::preprocess",
            "maxrss": 51200,
        },
        {
            "job_id": 2,
            "exec_job_id": "findrange_ID0000002",
            "type_desc": "compute",
            "states": state_seqs["FAILED"],
            "exitcode": 256,  # raw wait status for exit(1)
            "site": "condorpool",
            "transformation": "pegasus::findrange",
        },
        {
            "job_id": 3,
            "exec_job_id": "analyze_ID0000004",
            "type_desc": "compute",
            "states": state_seqs["HELD"],
            "site": "condorpool",
            "transformation": "pegasus::analyze",
        },
        {
            "job_id": 4,
            "exec_job_id": "running_ID0000005",
            "type_desc": "compute",
            "states": state_seqs["RUNNING"],
            "site": "condorpool",
            "transformation": "pegasus::running",
        },
        {
            "job_id": 5,
            "exec_job_id": "create_dir_diamond_0_local",
            "type_desc": "create-dir",
            "states": state_seqs["SUCCESS"],
            "exitcode": 0,
            "site": "local",
        },
    ]
    wf_states = [
        {"state": "WORKFLOW_STARTED", "timestamp": 49.0, "status": None},
        {"state": "WORKFLOW_TERMINATED", "timestamp": 255.0, "status": 1},
    ]
    return db_factory(jobs=jobs, wf_states=wf_states, dax_label="diamond")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Completed-workflow round trip: writer -> replay
# ─────────────────────────────────────────────────────────────────────────────


def test_completed_roundtrip_writer_to_replay(mixed_db, tmp_path):
    log_path = tmp_path / "workflow-events.jsonl"
    db_snap = _write_log(mixed_db, log_path)
    replay_snap = _replay_snapshot(log_path)

    # Roster of jobs must match exactly.
    assert _roster(replay_snap) == _roster(db_snap)

    # Count buckets must match.
    assert _counts(replay_snap) == _counts(db_snap)
    # Sanity: the mix really did contain each outcome.
    assert _counts(db_snap) == {
        "total": 5,
        "done": 2,
        "failed": 1,
        "held": 1,
        "running": 1,
        "queued": 0,
    }

    # Per-job display state, keyed by exec_job_id.
    assert _disp_by_exec(replay_snap) == _disp_by_exec(db_snap)

    # Workflow reached its terminal state.
    assert replay_snap.wf_state == "WORKFLOW_TERMINATED"
    assert replay_snap.is_complete

    # Metadata the logger emits (transformation via jobs_init) survives the trip.
    src_xform = {j.exec_job_id: j.transformation for j in db_snap.jobs}
    rep_xform = {j.exec_job_id: j.transformation for j in replay_snap.jobs}
    assert rep_xform == src_xform

    # Duration round-trips for completed compute jobs (start->end deltas, which
    # are invariant even though absolute timestamps belong to the DB clock).
    src_dur = {
        j.exec_job_id: j.duration
        for j in db_snap.jobs
        if j.disp_state in ("SUCCESS", "FAILED")
    }
    rep_dur = {
        j.exec_job_id: j.duration
        for j in replay_snap.jobs
        if j.disp_state in ("SUCCESS", "FAILED")
    }
    assert rep_dur == src_dur

    # maxrss survives for the job that carried it (preprocess).
    rep_maxrss = {j.exec_job_id: j.maxrss for j in replay_snap.jobs}
    assert rep_maxrss["preprocess_ID0000001"] == 51200


# ─────────────────────────────────────────────────────────────────────────────
# 2. Completed-workflow round trip: writer -> remote
# ─────────────────────────────────────────────────────────────────────────────


def test_completed_roundtrip_writer_to_remote(mixed_db, tmp_path):
    log_path = tmp_path / "workflow-events.jsonl"
    db_snap = _write_log(mixed_db, log_path)
    events = _parse_jsonl(log_path)
    remote_snap = _remote_snapshot(events)

    assert _roster(remote_snap) == _roster(db_snap)
    assert _counts(remote_snap) == _counts(db_snap)
    assert _disp_by_exec(remote_snap) == _disp_by_exec(db_snap)
    assert remote_snap.wf_state == "WORKFLOW_TERMINATED"
    assert remote_snap.is_complete


# ─────────────────────────────────────────────────────────────────────────────
# 3. The two readers agree on the identical log
# ─────────────────────────────────────────────────────────────────────────────


def test_readers_agree(mixed_db, tmp_path):
    log_path = tmp_path / "workflow-events.jsonl"
    _write_log(mixed_db, log_path)

    events = _parse_jsonl(log_path)
    replay_snap = _replay_snapshot(log_path)
    remote_snap = _remote_snapshot(events)

    assert _roster(replay_snap) == _roster(remote_snap)
    assert _counts(replay_snap) == _counts(remote_snap)
    assert _disp_by_exec(replay_snap) == _disp_by_exec(remote_snap)
    assert replay_snap.wf_state == remote_snap.wf_state


# ─────────────────────────────────────────────────────────────────────────────
# 4. Failure / held states specifically survive both readers
# ─────────────────────────────────────────────────────────────────────────────


def test_failed_and_held_survive_roundtrip(mixed_db, tmp_path):
    log_path = tmp_path / "workflow-events.jsonl"
    _write_log(mixed_db, log_path)
    events = _parse_jsonl(log_path)

    replay_snap = _replay_snapshot(log_path)
    remote_snap = _remote_snapshot(events)

    for snap, label in ((replay_snap, "replay"), (remote_snap, "remote")):
        disp = _disp_by_exec(snap)
        assert disp["findrange_ID0000002"] == "FAILED", f"{label}: failed lost"
        assert disp["analyze_ID0000004"] == "HELD", f"{label}: held lost"
        assert snap.failed_count() == 1, f"{label}: failed_count"
        assert snap.held_count() == 1, f"{label}: held_count"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Resume continuity: multi-session log still reconstructs correctly
# ─────────────────────────────────────────────────────────────────────────────


def test_resume_multisession_roundtrip(mixed_db, tmp_path):
    """Record/close twice on the same log_path (second logger resumes), then
    confirm the replay engine's multi-session cleanup (extra-header / extra-end
    / job_state dedup) cooperates with the writer's resume to reconstruct the
    same final snapshot as a single-session log."""
    log_path = tmp_path / "workflow-events.jsonl"

    # Session 1
    db_snap = _write_log(mixed_db, log_path)

    # Session 2 — a brand-new logger on the same path must resume, not re-header.
    db = _open_db(mixed_db)
    try:
        info = _make_info(log_path.parent)
        db_snap2 = db.snapshot()
        logger2 = EventLogger(info, db, log_path=log_path, min_free_mb=0)
        assert logger2.resumed, "second EventLogger should resume the existing log"
        logger2.record(db_snap2)
        logger2.close(db_snap2)
    finally:
        db.close()

    # The multi-session log must still have exactly one header and parse.
    events = _parse_jsonl(log_path)
    headers = [e for e in events if e.get("event_type") == "workflow_start"]
    assert len(headers) == 1, "resume must not duplicate the workflow_start header"

    replay_snap = _replay_snapshot(log_path)

    assert _roster(replay_snap) == _roster(db_snap)
    assert _counts(replay_snap) == _counts(db_snap)
    assert _disp_by_exec(replay_snap) == _disp_by_exec(db_snap)
    assert replay_snap.wf_state == "WORKFLOW_TERMINATED"

    # And the remote reader agrees on the multi-session log too.
    remote_snap = _remote_snapshot(events)
    assert _disp_by_exec(remote_snap) == _disp_by_exec(db_snap)
    assert _counts(remote_snap) == _counts(db_snap)
