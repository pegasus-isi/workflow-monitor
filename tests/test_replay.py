"""Unit tests for :mod:`workflow_monitor.replay`.

Exercises the pure loading and reconstruction logic of :class:`ReplayEngine`:
``_load`` (header parsing + multi-session cleanup), ``_prescan_job_roster``,
``_build_frames``, and ``_reconstruct``.  ``ReplayEngine.run()`` is never
called — it starts a Rich Live TUI and sleeps — but the end-to-end test mirrors
``run()``'s frame loop manually (no Live, no sleep) to get strong coverage of
the reconstruction path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from workflow_monitor.db import JobRecord, WorkflowSnapshot
from workflow_monitor.replay import ReplayEngine


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _write_log(path: Path, events: List[Dict[str, Any]]) -> Path:
    """Write a list of event dicts as JSONL to *path*."""
    with open(path, "w") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")
    return path


def _header(wf_start: float = 100.0, ts: float = 100.0, **kw) -> Dict[str, Any]:
    base = {
        "event_type": "workflow_start",
        "wf_uuid": "uuid-123",
        "dax_label": "diamond",
        "submit_dir": "/submit/run0001",
        "user": "tester",
        "planner_version": "5.1.2",
        "timestamp": ts,
        "wf_start": wf_start,
    }
    base.update(kw)
    return base


def _engine(tmp_path: Path, events: List[Dict[str, Any]], **kw) -> ReplayEngine:
    path = _write_log(tmp_path / "events.jsonl", events)
    return ReplayEngine(path, **kw)


# ─────────────────────────────────────────────────────────────────────────────
# _load: error cases
# ─────────────────────────────────────────────────────────────────────────────


def test_empty_file_raises(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_text("")
    with pytest.raises(ValueError, match="Empty event log"):
        ReplayEngine(path)


def test_blank_lines_only_raises(tmp_path):
    path = tmp_path / "blank.jsonl"
    path.write_text("\n   \n\n")
    with pytest.raises(ValueError, match="Empty event log"):
        ReplayEngine(path)


def test_first_event_not_workflow_start_raises(tmp_path):
    events = [
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 1.0}
    ]
    with pytest.raises(ValueError, match="First event must be workflow_start"):
        _engine(tmp_path, events)


# ─────────────────────────────────────────────────────────────────────────────
# _load: header / info
# ─────────────────────────────────────────────────────────────────────────────


def test_header_populates_info(tmp_path):
    eng = _engine(tmp_path, [_header(wf_start=99.0)])
    info = eng.info
    assert info.wf_uuid == "uuid-123"
    assert info.dax_label == "diamond"
    assert info.submit_dir == Path("/submit/run0001")
    assert info.user == "tester"
    assert info.planner_version == "5.1.2"
    # _header_wf_start captured from header
    assert eng._header_wf_start == 99.0


def test_info_property_returns_workflow_info(tmp_path):
    eng = _engine(tmp_path, [_header()])
    # `info` asserts non-None internally; just confirm a usable object.
    assert eng.info is not None
    assert eng.info.wf_uuid == "uuid-123"


def test_header_defaults_when_fields_missing(tmp_path):
    eng = _engine(tmp_path, [{"event_type": "workflow_start"}])
    info = eng.info
    assert info.wf_uuid == "unknown"
    assert info.dax_label == "unknown"
    assert info.planner_version == "?"
    assert eng._header_wf_start is None


# ─────────────────────────────────────────────────────────────────────────────
# _load: multi-session cleanup
# ─────────────────────────────────────────────────────────────────────────────


def test_load_strips_extra_headers_dedups_and_keeps_last_end(tmp_path):
    events = [
        _header(ts=100.0),  # consumed as header (raw[0])
        {
            "event_type": "jobs_init",
            "timestamp": 100.0,
            "jobs": [{"job_id": 1, "exec_job_id": "j1"}],
        },
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 101.0},
        # duplicate job_state (same job_id, state, timestamp) -> dropped
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 101.0},
        {
            "event_type": "workflow_end",
            "wf_state": "WORKFLOW_TERMINATED",
            "wf_status": 0,
            "timestamp": 102.0,
        },
        # resumed session: extra header (stripped), duplicate jobs_init (dropped),
        # intermediate workflow_end (removed, only last kept)
        {"event_type": "workflow_start", "timestamp": 103.0},
        {
            "event_type": "jobs_init",
            "timestamp": 103.0,
            "jobs": [{"job_id": 1, "exec_job_id": "j1"}],
        },
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "EXECUTE",
            "timestamp": 104.0,
        },
        {
            "event_type": "workflow_end",
            "wf_state": "WORKFLOW_TERMINATED",
            "wf_status": 0,
            "timestamp": 105.0,
        },
    ]
    eng = _engine(tmp_path, events)
    body = eng._events

    etypes = [e["event_type"] for e in body]
    # No workflow_start in body (header consumed; extra stripped)
    assert "workflow_start" not in etypes
    # Exactly one jobs_init
    assert etypes.count("jobs_init") == 1
    # Exactly one workflow_end (the LAST one, at ts 105)
    assert etypes.count("workflow_end") == 1
    end_ev = next(e for e in body if e["event_type"] == "workflow_end")
    assert end_ev["timestamp"] == 105.0
    # Duplicate SUBMIT job_state deduplicated -> only one SUBMIT for job 1
    submit_evs = [
        e for e in body if e["event_type"] == "job_state" and e.get("state") == "SUBMIT"
    ]
    assert len(submit_evs) == 1


def test_load_sorts_events_by_timestamp_stable(tmp_path):
    events = [
        _header(ts=100.0),
        {"event_type": "job_state", "job_id": 2, "state": "SUBMIT", "timestamp": 105.0},
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 101.0},
        # two events at the same timestamp keep insertion order (stable sort)
        {"event_type": "job_state", "job_id": 3, "state": "SUBMIT", "timestamp": 103.0},
        {
            "event_type": "job_state",
            "job_id": 4,
            "state": "EXECUTE",
            "timestamp": 103.0,
        },
    ]
    eng = _engine(tmp_path, events)
    ts_order = [e["timestamp"] for e in eng._events]
    assert ts_order == sorted(ts_order)
    # Stable: job 3 (SUBMIT) before job 4 (EXECUTE) within ts 103
    at_103 = [e for e in eng._events if e["timestamp"] == 103.0]
    assert [e["job_id"] for e in at_103] == [3, 4]


# ─────────────────────────────────────────────────────────────────────────────
# _prescan_job_roster
# ─────────────────────────────────────────────────────────────────────────────


def test_prescan_uses_jobs_init_when_present(tmp_path):
    events = [
        _header(),
        {
            "event_type": "jobs_init",
            "timestamp": 100.0,
            "jobs": [
                {
                    "job_id": 1,
                    "exec_job_id": "pre",
                    "type_desc": "compute",
                    "transformation": "pegasus::preprocess",
                    "task_argv": "-a top",
                },
                {"job_id": 2, "exec_job_id": "dir", "type_desc": "create-dir"},
            ],
        },
        # a job_state for an id NOT in jobs_init must be ignored (jobs_init wins/stops)
        {
            "event_type": "job_state",
            "job_id": 99,
            "state": "SUBMIT",
            "exec_job_id": "ghost",
            "timestamp": 101.0,
        },
    ]
    eng = _engine(tmp_path, events)
    roster = eng._prescan_jobs
    assert set(roster) == {1, 2}
    assert roster[1]["transformation"] == "pegasus::preprocess"
    assert roster[1]["task_argv"] == "-a top"
    assert roster[1]["type_desc"] == "compute"
    assert roster[2]["type_desc"] == "create-dir"
    # No transformation key when not provided
    assert "transformation" not in roster[2]


def test_prescan_derives_roster_from_job_states(tmp_path):
    events = [
        _header(),
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "SUBMIT",
            "exec_job_id": "j1",
            "type_desc": "compute",
            "timestamp": 101.0,
        },
        {
            "event_type": "job_state",
            "job_id": 2,
            "state": "SUBMIT",
            "exec_job_id": "j2",
            "type_desc": "stage-in-tx",
            "timestamp": 102.0,
        },
        # second occurrence of job 1 -> first wins, not overwritten
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "EXECUTE",
            "exec_job_id": "j1",
            "type_desc": "compute",
            "timestamp": 103.0,
        },
    ]
    eng = _engine(tmp_path, events)
    roster = eng._prescan_jobs
    assert set(roster) == {1, 2}
    assert roster[1]["exec_job_id"] == "j1"
    assert roster[2]["type_desc"] == "stage-in-tx"


# ─────────────────────────────────────────────────────────────────────────────
# _build_frames
# ─────────────────────────────────────────────────────────────────────────────


def test_build_frames_groups_by_integer_second(tmp_path):
    events = [
        _header(ts=100.4),
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 100.4},
        {"event_type": "job_state", "job_id": 2, "state": "SUBMIT", "timestamp": 100.9},
        {"event_type": "job_state", "job_id": 3, "state": "SUBMIT", "timestamp": 101.2},
    ]
    eng = _engine(tmp_path, events)
    # Header is consumed; body has the three job_state events.
    frames = eng._build_frames()
    assert len(frames) == 2
    # Frame 1: int(100.4)==100 and int(100.9)==100 -> two events
    assert [e["job_id"] for e in frames[0]] == [1, 2]
    # Frame 2: int(101.2)==101 -> one event
    assert [e["job_id"] for e in frames[1]] == [3]


def test_build_frames_empty_when_no_body_events(tmp_path):
    eng = _engine(tmp_path, [_header()])
    assert eng._build_frames() == []


# ─────────────────────────────────────────────────────────────────────────────
# _reconstruct: direct drive
# ─────────────────────────────────────────────────────────────────────────────


def _fresh_engine(tmp_path) -> ReplayEngine:
    """An engine whose body is irrelevant; used only to call _reconstruct."""
    return _engine(tmp_path, [_header(wf_start=100.0)])


def test_reconstruct_workflow_state_sets_start(tmp_path):
    eng = _fresh_engine(tmp_path)
    frame = [
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_STARTED",
            "status": None,
            "timestamp": 200.0,
        }
    ]
    snap, wf_state, wf_status, wf_start, wf_end = eng._reconstruct(
        frame,
        {},
        "UNKNOWN",
        None,
        None,
        None,
        [],
        200.0,
    )
    assert wf_state == "WORKFLOW_STARTED"
    assert wf_start == 200.0
    assert wf_end is None
    assert snap.wf_state == "WORKFLOW_STARTED"


def test_reconstruct_workflow_state_terminated_sets_end(tmp_path):
    eng = _fresh_engine(tmp_path)
    frame = [
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_TERMINATED",
            "status": 0,
            "timestamp": 300.0,
        }
    ]
    snap, wf_state, wf_status, wf_start, wf_end = eng._reconstruct(
        frame,
        {},
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        [],
        300.0,
    )
    assert wf_state == "WORKFLOW_TERMINATED"
    assert wf_status == 0
    assert wf_end == 300.0


def test_reconstruct_jobs_init_prepopulates_unsubmitted(tmp_path):
    eng = _fresh_engine(tmp_path)
    frame = [
        {
            "event_type": "jobs_init",
            "jobs": [
                {
                    "job_id": 2,
                    "exec_job_id": "j2",
                    "type_desc": "compute",
                    "transformation": "pegasus::analyze",
                },
                {"job_id": 1, "exec_job_id": "j1", "type_desc": "create-dir"},
            ],
            "timestamp": 100.0,
        }
    ]
    job_state: Dict[int, Dict[str, Any]] = {}
    snap, *_ = eng._reconstruct(
        frame,
        job_state,
        "UNKNOWN",
        None,
        100.0,
        None,
        [],
        100.0,
    )
    # Pre-populated, sorted by job_id
    assert [j.job_id for j in snap.jobs] == [1, 2]
    j2 = next(j for j in snap.jobs if j.job_id == 2)
    assert j2.raw_state is None  # UNSUBMITTED
    assert j2.disp_state == "UNSUBMITTED"
    assert j2.transformation == "pegasus::analyze"
    assert j2.type_desc == "compute"
    # _now propagated to each JobRecord
    assert all(j._now == 100.0 for j in snap.jobs)


def test_reconstruct_job_state_transitions_set_times(tmp_path):
    eng = _fresh_engine(tmp_path)
    job_state: Dict[int, Dict[str, Any]] = {}
    recent: List[Dict] = []

    # SUBMIT -> submit_time
    eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": "SUBMIT",
                "exec_job_id": "j1",
                "type_desc": "compute",
                "timestamp": 110.0,
            }
        ],
        job_state,
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        recent,
        110.0,
    )
    assert job_state[1]["submit_time"] == 110.0
    assert job_state[1]["raw_state"] == "SUBMIT"

    # EXECUTE -> start_time
    eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": "EXECUTE",
                "timestamp": 120.0,
            }
        ],
        job_state,
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        recent,
        120.0,
    )
    assert job_state[1]["start_time"] == 120.0

    # JOB_TERMINATED -> end_time
    snap, *_ = eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": "JOB_TERMINATED",
                "timestamp": 150.0,
            }
        ],
        job_state,
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        recent,
        150.0,
    )
    assert job_state[1]["end_time"] == 150.0

    j1 = snap.jobs[0]
    assert j1.submit_time == 110.0
    assert j1.start_time == 120.0
    assert j1.end_time == 150.0
    assert j1.duration == 30.0
    # recent_events accumulated one entry per job_state event (3 total)
    assert len(recent) == 3


@pytest.mark.parametrize("end_state", ["JOB_TERMINATED", "JOB_SUCCESS", "JOB_FAILURE"])
def test_reconstruct_terminal_states_set_end_time(tmp_path, end_state):
    eng = _fresh_engine(tmp_path)
    job_state: Dict[int, Dict[str, Any]] = {}
    eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": end_state,
                "timestamp": 199.0,
            }
        ],
        job_state,
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        [],
        199.0,
    )
    assert job_state[1]["end_time"] == 199.0


def test_reconstruct_captures_runtime_metadata(tmp_path):
    eng = _fresh_engine(tmp_path)
    job_state: Dict[int, Dict[str, Any]] = {}
    eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": "EXECUTE",
                "stdout_file": "out.txt",
                "stderr_file": "err.txt",
                "maxrss": 4096,
                "timestamp": 120.0,
            }
        ],
        job_state,
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        [],
        120.0,
    )
    assert job_state[1]["stdout_file"] == "out.txt"
    assert job_state[1]["stderr_file"] == "err.txt"
    assert job_state[1]["maxrss"] == 4096


def test_reconstruct_workflow_end_sets_state_status_end(tmp_path):
    eng = _fresh_engine(tmp_path)
    frame = [
        {
            "event_type": "workflow_end",
            "wf_state": "WORKFLOW_TERMINATED",
            "wf_status": 1,
            "timestamp": 400.0,
        }
    ]
    snap, wf_state, wf_status, wf_start, wf_end = eng._reconstruct(
        frame,
        {},
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        [],
        400.0,
    )
    assert wf_state == "WORKFLOW_TERMINATED"
    assert wf_status == 1
    assert wf_end == 400.0


def test_reconstruct_wf_start_falls_back_to_header(tmp_path):
    # header wf_start = 100.0; no workflow_state event sets it.
    eng = _engine(tmp_path, [_header(wf_start=100.0)])
    snap, wf_state, wf_status, wf_start, wf_end = eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": "SUBMIT",
                "timestamp": 150.0,
            }
        ],
        {},
        "UNKNOWN",
        None,
        None,
        None,
        [],
        150.0,
    )
    assert wf_start == 100.0
    assert snap.wf_start == 100.0


def test_reconstruct_recent_events_trimmed_and_reversed(tmp_path):
    eng = _engine(tmp_path, [_header(wf_start=100.0)], events_n=2)
    recent: List[Dict] = []
    frame = [
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 100.0},
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "EXECUTE",
            "timestamp": 100.0,
        },
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_TERMINATED",
            "timestamp": 100.0,
        },
    ]
    snap, *_ = eng._reconstruct(
        frame,
        {},
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        recent,
        100.0,
    )
    # recent list trimmed to last events_n (=2)
    assert len(recent) == 2
    assert [e["state"] for e in recent] == ["EXECUTE", "JOB_TERMINATED"]
    # snapshot recent_events reversed (newest first)
    assert [e["state"] for e in snap.recent_events] == ["JOB_TERMINATED", "EXECUTE"]


def test_reconstruct_unknown_job_id_autocreated(tmp_path):
    eng = _fresh_engine(tmp_path)
    job_state: Dict[int, Dict[str, Any]] = {}
    # job_id seen for the first time via job_state (no jobs_init)
    snap, *_ = eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 7,
                "state": "SUBMIT",
                "exec_job_id": "j7",
                "type_desc": "compute",
                "timestamp": 110.0,
            }
        ],
        job_state,
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        [],
        110.0,
    )
    assert 7 in job_state
    assert snap.jobs[0].job_id == 7
    assert snap.jobs[0].exec_job_id == "j7"


def test_reconstruct_poll_time_is_frame_ts(tmp_path):
    eng = _fresh_engine(tmp_path)
    snap, *_ = eng._reconstruct(
        [
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": "SUBMIT",
                "timestamp": 130.0,
            }
        ],
        {},
        "WORKFLOW_STARTED",
        None,
        100.0,
        None,
        [],
        130.0,
    )
    assert snap.poll_time == 130.0


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end: mirror run()'s frame loop WITHOUT the Live TUI / sleeps
# ─────────────────────────────────────────────────────────────────────────────


def test_end_to_end_reconstruction(tmp_path):
    events = [
        _header(wf_start=100.0, ts=100.0),
        {
            "event_type": "jobs_init",
            "timestamp": 100.0,
            "jobs": [
                {
                    "job_id": 1,
                    "exec_job_id": "pre",
                    "type_desc": "compute",
                    "transformation": "pegasus::preprocess",
                },
                {
                    "job_id": 2,
                    "exec_job_id": "ana",
                    "type_desc": "compute",
                    "transformation": "pegasus::analyze",
                },
            ],
        },
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_STARTED",
            "status": None,
            "timestamp": 100.0,
        },
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 101.0},
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "EXECUTE",
            "timestamp": 110.0,
        },
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_TERMINATED",
            "timestamp": 150.0,
        },
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_SUCCESS",
            "timestamp": 151.0,
        },
        {"event_type": "job_state", "job_id": 2, "state": "SUBMIT", "timestamp": 152.0},
        {
            "event_type": "job_state",
            "job_id": 2,
            "state": "EXECUTE",
            "timestamp": 160.0,
        },
        {
            "event_type": "job_state",
            "job_id": 2,
            "state": "JOB_TERMINATED",
            "timestamp": 200.0,
        },
        {
            "event_type": "job_state",
            "job_id": 2,
            "state": "JOB_SUCCESS",
            "timestamp": 201.0,
        },
        {
            "event_type": "workflow_end",
            "wf_state": "WORKFLOW_TERMINATED",
            "wf_status": 0,
            "timestamp": 205.0,
        },
    ]
    eng = _engine(tmp_path, events)

    # Mirror run(): seed job_state from prescan roster, iterate frames through
    # _reconstruct.  No Live, no sleep.
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
    wf_status = None
    wf_start = None
    wf_end = None
    recent: List[Dict] = []

    snap: WorkflowSnapshot | None = None
    for frame in eng._build_frames():
        frame_ts = float(frame[0].get("timestamp"))
        snap, wf_state, wf_status, wf_start, wf_end = eng._reconstruct(
            frame,
            job_state,
            wf_state,
            wf_status,
            wf_start,
            wf_end,
            recent,
            frame_ts,
        )

    assert snap is not None
    # Final workflow state
    assert wf_state == "WORKFLOW_TERMINATED"
    assert wf_status == 0
    assert wf_start == 100.0
    assert wf_end == 205.0
    assert snap.is_complete and snap.succeeded

    # Both compute jobs present, sorted, terminal
    assert [j.job_id for j in snap.jobs] == [1, 2]
    j1, j2 = snap.jobs
    assert j1.disp_state == "SUCCESS"
    assert j2.disp_state == "SUCCESS"
    assert j1.submit_time == 101.0
    assert j1.start_time == 110.0
    # end_time is set on each terminal event; last-applied wins (JOB_SUCCESS @151)
    assert j1.end_time == 151.0
    assert j1.duration == pytest.approx(41.0)  # 151 - 110
    assert j2.end_time == 201.0
    assert j2.duration == pytest.approx(41.0)  # 201 - 160
    # transformation carried from jobs_init roster
    assert j1.transformation == "pegasus::preprocess"
    assert j2.transformation == "pegasus::analyze"

    # done_count reflects both successes
    assert snap.done_count() == 2
    assert isinstance(j1, JobRecord)
