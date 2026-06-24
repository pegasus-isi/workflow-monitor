"""Unit tests for :mod:`workflow_monitor.event_log`.

The EventLogger writes a JSONL event stream for later replay. These tests
exercise: the workflow_start header, jobs_init / workflow_state / job_state
emission, condor/history/pool fingerprint de-duplication, close() (stats +
end marker), resume logic, and the disk-exhaustion guard.

All tests are deterministic: time.time() is frozen / stepped via monkeypatch,
and the disk guard's shutil.disk_usage is monkeypatched to a controllable
SimpleNamespace. No network, no real HTCondor.
"""

from __future__ import annotations

import json
import types
from pathlib import Path
from typing import List

import pytest

import workflow_monitor.event_log as event_log
from workflow_monitor.braindump import WorkflowInfo
from workflow_monitor.db import StampedeDB
from workflow_monitor.event_log import EventLogger
from workflow_monitor.htcondor_poll import PoolSummary


# ─────────────────────────────────────────────────────────────────────────────
# helpers / fixtures
# ─────────────────────────────────────────────────────────────────────────────


def read_events(path: Path) -> List[dict]:
    """Parse a JSONL file into a list of dicts (one per non-empty line)."""
    text = path.read_text()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def read_valid_events(path: Path) -> List[dict]:
    """Like :func:`read_events` but skip non-JSON (garbage) lines."""
    out: List[dict] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def types_of(events: List[dict]) -> List[str]:
    return [e["event_type"] for e in events]


@pytest.fixture
def clock(monkeypatch):
    """A controllable wall clock for event_log.time.time().

    Returns a mutable dict ``{"t": <float>}``. Mutate ``clock["t"]`` to step
    time forward (used to bypass the 5s disk-guard throttle).
    """
    state = {"t": 1000.0}
    monkeypatch.setattr(event_log.time, "time", lambda: state["t"])
    return state


@pytest.fixture
def info(tmp_path) -> WorkflowInfo:
    """A WorkflowInfo whose submit_dir is the temp dir."""
    return WorkflowInfo(
        wf_uuid="test-wf-uuid",
        root_wf_uuid="test-wf-uuid",
        dax_label="diamond",
        submit_dir=tmp_path,
        user="tester",
        planner_version="5.1.2",
        dag_file="diamond-0.dag",
        condor_log="diamond-0.log",
        timestamp="20260101T000000+0000",
        basedir=tmp_path.parent,
    )


@pytest.fixture
def open_db(db_factory):
    """Build a stampede DB and return an opened StampedeDB.

    Usage: ``db = open_db(jobs=[...], wf_states=[...])``.
    """
    opened: List[StampedeDB] = []

    def _open(**kwargs) -> StampedeDB:
        wf_uuid = kwargs.get("wf_uuid", "test-wf-uuid")
        path = db_factory(**kwargs)
        db = StampedeDB(path, wf_uuid=wf_uuid)
        db.connect()
        opened.append(db)
        return db

    yield _open
    for db in opened:
        db.close()


# A simple stampede DB used by most tests: one started workflow, one job.
def _basic_jobs():
    return [
        {
            "job_id": 1,
            "exec_job_id": "preprocess_ID0000001",
            "type_desc": "compute",
            "states": [("SUBMIT", 100.0), ("EXECUTE", 110.0)],
            "transformation": "pegasus::preprocess",
            "argv": "-a top",
            "stdout_file": "00/00/pre.out",
            "stderr_file": "00/00/pre.err",
            "maxrss": 51200,
        },
    ]


def _started_wf_states():
    return [{"state": "WORKFLOW_STARTED", "timestamp": 49.0, "status": None}]


# ─────────────────────────────────────────────────────────────────────────────
# 1. Fresh start / header
# ─────────────────────────────────────────────────────────────────────────────


def test_fresh_start_writes_header(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    log_path = tmp_path / "events.jsonl"
    logger = EventLogger(info, db, log_path=log_path)

    assert logger.resumed is False
    assert logger.path == log_path

    events = read_events(log_path)
    assert len(events) == 1
    hdr = events[0]
    assert hdr["event_type"] == "workflow_start"
    assert hdr["dax_label"] == "diamond"
    assert hdr["user"] == "tester"
    assert hdr["planner_version"] == "5.1.2"
    assert hdr["submit_dir"] == str(tmp_path)
    assert hdr["wf_uuid"] == "test-wf-uuid"
    # wf_start comes from db.get_workflow_times()["start"]
    assert hdr["wf_start"] == 49.0
    assert hdr["timestamp"] == 1000.0


def test_default_log_path_is_submit_dir(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = EventLogger(info, db)  # no log_path
    assert logger.path == tmp_path / "workflow-events.jsonl"
    assert logger.path.exists()


def test_header_wf_start_none_when_not_started(info, open_db, clock, tmp_path):
    # No workflowstate rows -> get_workflow_times returns start=None
    db = open_db(jobs=_basic_jobs(), wf_states=[])
    logger = EventLogger(info, db, log_path=tmp_path / "events.jsonl")
    hdr = read_events(logger.path)[0]
    assert hdr["wf_start"] is None


# ─────────────────────────────────────────────────────────────────────────────
# 2. record(): jobs_init, workflow_state, job_state, fingerprints
# ─────────────────────────────────────────────────────────────────────────────


def _make_logger(info, db, tmp_path, **kw):
    return EventLogger(info, db, log_path=tmp_path / "events.jsonl", **kw)


def test_jobs_init_emitted_once(info, open_db, clock, tmp_path, make_snapshot):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    snap = db.snapshot()

    logger.record(snap)
    logger.record(snap)  # second record must NOT re-emit jobs_init

    events = read_events(logger.path)
    init_events = [e for e in events if e["event_type"] == "jobs_init"]
    assert len(init_events) == 1

    init = init_events[0]
    assert init["total_jobs"] == 1
    assert len(init["jobs"]) == 1
    j = init["jobs"][0]
    assert j["job_id"] == 1
    assert j["exec_job_id"] == "preprocess_ID0000001"
    assert j["type_desc"] == "compute"
    assert j["transformation"] == "pegasus::preprocess"
    assert j["task_argv"] == "-a top"


def test_jobs_init_omits_optional_when_absent(info, open_db, clock, tmp_path):
    # A job with no transformation/argv -> entry has only the 3 required keys
    jobs = [
        {
            "job_id": 5,
            "exec_job_id": "create_dir_local",
            "type_desc": "create-dir",
            "states": [("SUBMIT", 50.0)],
        }
    ]
    db = open_db(jobs=jobs, wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    logger.record(db.snapshot())
    init = [e for e in read_events(logger.path) if e["event_type"] == "jobs_init"][0]
    entry = init["jobs"][0]
    assert set(entry.keys()) == {"job_id", "exec_job_id", "type_desc"}


def test_workflow_state_emitted_on_change_only(
    info, open_db, clock, tmp_path, make_snapshot
):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)

    started = make_snapshot(states=[], wf_state="WORKFLOW_STARTED", wf_start=49.0)
    logger.record(started)
    logger.record(started)  # same state -> no new workflow_state

    terminated = make_snapshot(
        states=[],
        wf_state="WORKFLOW_TERMINATED",
        wf_status=0,
        wf_start=49.0,
        wf_end=255.0,
    )
    logger.record(terminated)

    ws = [e for e in read_events(logger.path) if e["event_type"] == "workflow_state"]
    assert len(ws) == 2
    assert ws[0]["state"] == "WORKFLOW_STARTED"
    assert ws[0]["wf_start"] == 49.0
    assert "wf_end" not in ws[0]
    assert ws[1]["state"] == "WORKFLOW_TERMINATED"
    assert ws[1]["wf_end"] == 255.0
    assert "wf_start" not in ws[1]


def test_job_state_events_only_new(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    snap = db.snapshot()

    logger.record(snap)
    # Two transitions exist: SUBMIT@100, EXECUTE@110
    js1 = [e for e in read_events(logger.path) if e["event_type"] == "job_state"]
    assert [e["state"] for e in js1] == ["SUBMIT", "EXECUTE"]
    assert logger.high_water_ts == 110.0

    # A second record() with no new DB rows emits nothing new
    logger.record(snap)
    js2 = [e for e in read_events(logger.path) if e["event_type"] == "job_state"]
    assert len(js2) == 2  # unchanged

    # Each job_state carries identity + state
    submit_ev = js1[0]
    assert submit_ev["exec_job_id"] == "preprocess_ID0000001"
    assert submit_ev["type_desc"] == "compute"
    assert submit_ev["job_id"] == 1
    assert submit_ev["timestamp"] == 100.0


def test_job_state_advances_high_water_across_calls(db_factory, info, clock, tmp_path):
    # First DB has only the SUBMIT/EXECUTE transitions.
    path = db_factory(
        jobs=_basic_jobs(), wf_states=_started_wf_states(), name="wf.stampede.db"
    )
    db = StampedeDB(path, wf_uuid="test-wf-uuid")
    db.connect()
    logger = _make_logger(info, db, tmp_path)
    logger.record(db.snapshot())
    assert logger.high_water_ts == 110.0
    db.close()

    # Append later transitions to the SAME db file, reopen, record again.
    import sqlite3

    conn = sqlite3.connect(str(path))
    # job_instance_id 1, jobstate_submit_seq continues
    conn.execute(
        "INSERT INTO jobstate (job_instance_id, state, timestamp, jobstate_submit_seq)"
        " VALUES (1, 'JOB_TERMINATED', 150.0, 100)"
    )
    conn.execute(
        "INSERT INTO jobstate (job_instance_id, state, timestamp, jobstate_submit_seq)"
        " VALUES (1, 'JOB_SUCCESS', 151.0, 101)"
    )
    conn.commit()
    conn.close()

    db2 = StampedeDB(path, wf_uuid="test-wf-uuid")
    db2.connect()
    # Reuse the same logger object (its high_water_ts is 110.0).
    logger._db = db2
    logger.record(db2.snapshot())
    db2.close()

    js = [
        e["state"] for e in read_events(logger.path) if e["event_type"] == "job_state"
    ]
    assert js == ["SUBMIT", "EXECUTE", "JOB_TERMINATED", "JOB_SUCCESS"]
    assert logger.high_water_ts == 151.0


def test_job_state_optional_fields(info, open_db, clock, tmp_path):
    # A fully-attributed terminal job: exitcode, stdout/stderr, maxrss present.
    jobs = [
        {
            "job_id": 1,
            "exec_job_id": "analyze_ID0000004",
            "type_desc": "compute",
            "states": [
                ("SUBMIT", 100.0),
                ("EXECUTE", 110.0),
                ("JOB_TERMINATED", 150.0),
            ],
            "exitcode": 0,
            "site": "condorpool",
            "stdout_file": "00/00/an.out",
            "stderr_file": "00/00/an.err",
            "maxrss": 60000,
        }
    ]
    db = open_db(jobs=jobs, wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    logger.record(db.snapshot())

    js = [e for e in read_events(logger.path) if e["event_type"] == "job_state"]
    # Every transition row joins to the same job_instance, so optional fields
    # appear on all of them (the DB attaches exitcode/stdout/etc per instance).
    last = js[-1]
    assert last["state"] == "JOB_TERMINATED"
    assert last["exitcode"] == 0
    assert last["stdout_file"] == "00/00/an.out"
    assert last["stderr_file"] == "00/00/an.err"
    assert last["maxrss"] == 60000


# ── condor poll fingerprint ──────────────────────────────────────────────────


def test_condor_poll_none_never_emitted(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    logger.record(db.snapshot(), condor_jobs=None)
    assert not [
        e for e in read_events(logger.path) if e["event_type"] == "htcondor_poll"
    ]


def test_condor_poll_dedup_and_change(info, open_db, clock, tmp_path, condor_queue_ads):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    snap = db.snapshot()

    logger.record(snap, condor_jobs=condor_queue_ads)
    logger.record(snap, condor_jobs=condor_queue_ads)  # identical -> no new event
    polls = [e for e in read_events(logger.path) if e["event_type"] == "htcondor_poll"]
    assert len(polls) == 1
    assert len(polls[0]["jobs"]) == 2

    # Change JobStatus on one job -> a new fingerprint -> a new event.
    changed = [dict(ad) for ad in condor_queue_ads]
    changed[0]["JobStatus"] = 4  # was 2
    logger.record(snap, condor_jobs=changed)
    polls = [e for e in read_events(logger.path) if e["event_type"] == "htcondor_poll"]
    assert len(polls) == 2


# ── history fingerprint ──────────────────────────────────────────────────────


def test_history_none_and_empty_not_emitted(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    snap = db.snapshot()
    logger.record(snap, condor_history=None)
    logger.record(snap, condor_history=[])
    assert not [
        e for e in read_events(logger.path) if e["event_type"] == "htcondor_history"
    ]


def test_history_dedup_and_change(info, open_db, clock, tmp_path, condor_history_ads):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    snap = db.snapshot()

    logger.record(snap, condor_history=condor_history_ads)
    logger.record(snap, condor_history=condor_history_ads)  # same ClusterId set
    hist = [
        e for e in read_events(logger.path) if e["event_type"] == "htcondor_history"
    ]
    assert len(hist) == 1

    # Add a new ClusterId -> set changes -> new event.
    more = condor_history_ads + [{"ClusterId": 999, "ExitCode": 0}]
    logger.record(snap, condor_history=more)
    hist = [
        e for e in read_events(logger.path) if e["event_type"] == "htcondor_history"
    ]
    assert len(hist) == 2


# ── pool fingerprint ─────────────────────────────────────────────────────────


def test_pool_none_not_emitted(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    logger.record(db.snapshot(), pool_status=None)
    assert not [e for e in read_events(logger.path) if e["event_type"] == "pool_status"]


def test_pool_dedup_and_change(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    snap = db.snapshot()

    pool = PoolSummary(
        total_slots=8, claimed_slots=4, idle_slots=4, total_cpus=16, idle_cpus=8
    )
    logger.record(snap, pool_status=pool)
    logger.record(snap, pool_status=pool)  # identical counts -> no new event
    ps = [e for e in read_events(logger.path) if e["event_type"] == "pool_status"]
    assert len(ps) == 1
    assert ps[0]["pool"]["total_slots"] == 8

    # Change a count that participates in the fingerprint.
    pool2 = PoolSummary(
        total_slots=8, claimed_slots=6, idle_slots=2, total_cpus=16, idle_cpus=4
    )
    logger.record(snap, pool_status=pool2)
    ps = [e for e in read_events(logger.path) if e["event_type"] == "pool_status"]
    assert len(ps) == 2


# ─────────────────────────────────────────────────────────────────────────────
# 3. close()
# ─────────────────────────────────────────────────────────────────────────────


def test_close_with_snapshot_emits_stats_then_end(
    info, open_db, clock, tmp_path, make_snapshot
):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)

    snap = make_snapshot(
        states=["SUCCESS", "FAILED"],
        wf_state="WORKFLOW_TERMINATED",
        wf_status=1,
        wf_start=49.0,
        wf_end=255.0,
    )
    logger.close(snapshot=snap)

    events = read_events(logger.path)
    # workflow_stats immediately precedes workflow_end.
    assert types_of(events)[-2:] == ["workflow_stats", "workflow_end"]

    stats_ev = events[-2]
    assert "stats" in stats_ev
    assert stats_ev["stats"]["total_jobs"] == 2

    end = events[-1]
    assert end["wf_state"] == "WORKFLOW_TERMINATED"
    assert end["wf_status"] == 1
    assert end["wf_end"] == 255.0
    assert end["total_jobs"] == 2
    assert end["done"] == 1
    assert end["failed"] == 1
    assert end["elapsed"] == pytest.approx(255.0 - 49.0)


def test_close_without_snapshot_minimal_end(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    logger.close()  # no snapshot

    events = read_events(logger.path)
    assert "workflow_stats" not in types_of(events)
    end = events[-1]
    assert end["event_type"] == "workflow_end"
    # Minimal end carries only event_type/timestamp/wf_uuid.
    assert set(end.keys()) == {"event_type", "timestamp", "wf_uuid"}


def test_close_end_is_last_line(info, open_db, clock, tmp_path, make_snapshot):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = _make_logger(info, db, tmp_path)
    logger.record(db.snapshot())
    snap = make_snapshot(
        states=["SUCCESS"],
        wf_state="WORKFLOW_TERMINATED",
        wf_status=0,
        wf_start=49.0,
        wf_end=255.0,
    )
    logger.close(snapshot=snap)
    events = read_events(logger.path)
    assert events[-1]["event_type"] == "workflow_end"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Resume logic
# ─────────────────────────────────────────────────────────────────────────────


def _write_prior_log(path: Path, *, wf_uuid: str, max_ts: float = 130.0):
    """Write a complete prior session log ending in workflow_end."""
    lines = [
        {
            "event_type": "workflow_start",
            "timestamp": 900.0,
            "wf_uuid": wf_uuid,
            "dax_label": "diamond",
            "wf_start": 49.0,
        },
        {
            "event_type": "jobs_init",
            "timestamp": 901.0,
            "wf_uuid": wf_uuid,
            "total_jobs": 1,
            "jobs": [
                {
                    "job_id": 1,
                    "exec_job_id": "preprocess_ID0000001",
                    "type_desc": "compute",
                }
            ],
        },
        {
            "event_type": "workflow_state",
            "timestamp": 902.0,
            "wf_uuid": wf_uuid,
            "state": "WORKFLOW_STARTED",
            "status": None,
        },
        {
            "event_type": "job_state",
            "timestamp": 100.0,
            "wf_uuid": wf_uuid,
            "state": "SUBMIT",
            "exec_job_id": "preprocess_ID0000001",
            "type_desc": "compute",
            "job_id": 1,
        },
        {
            "event_type": "job_state",
            "timestamp": max_ts,
            "wf_uuid": wf_uuid,
            "state": "EXECUTE",
            "exec_job_id": "preprocess_ID0000001",
            "type_desc": "compute",
            "job_id": 1,
        },
        {
            "event_type": "htcondor_poll",
            "timestamp": 903.0,
            "wf_uuid": wf_uuid,
            "jobs": [{"ClusterId": 101, "JobStatus": 2}],
        },
        {
            "event_type": "workflow_end",
            "timestamp": 904.0,
            "wf_uuid": wf_uuid,
            "wf_state": "WORKFLOW_STARTED",
        },
    ]
    path.write_text("".join(json.dumps(li) + "\n" for li in lines))


def test_resume_matching_uuid_truncates_end(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    log_path = tmp_path / "events.jsonl"
    _write_prior_log(log_path, wf_uuid="test-wf-uuid", max_ts=130.0)

    logger = EventLogger(info, db, log_path=log_path)

    assert logger.resumed is True
    # State recovered from the prior log.
    assert logger.high_water_ts == 130.0
    assert logger._last_wf_state == "WORKFLOW_STARTED"
    assert logger._jobs_init_emitted is True
    # condor fingerprint recovered (one cluster, status 2)
    assert logger._last_condor_fingerprint == EventLogger._condor_fingerprint(
        [{"ClusterId": 101, "JobStatus": 2}]
    )

    # Trailing workflow_end has been truncated.
    events = read_events(log_path)
    assert "workflow_end" not in types_of(events)
    # No fresh workflow_start rewritten (still exactly one).
    assert types_of(events).count("workflow_start") == 1


def test_resume_does_not_reemit_jobs_init(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    log_path = tmp_path / "events.jsonl"
    _write_prior_log(log_path, wf_uuid="test-wf-uuid")
    logger = EventLogger(info, db, log_path=log_path)

    before = types_of(read_events(log_path)).count("jobs_init")
    logger.record(db.snapshot())  # would emit jobs_init if flag were not set
    after = types_of(read_events(log_path)).count("jobs_init")
    assert before == after == 1


def test_resume_uuid_mismatch_starts_fresh(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    log_path = tmp_path / "events.jsonl"
    _write_prior_log(log_path, wf_uuid="OTHER-uuid")

    logger = EventLogger(info, db, log_path=log_path)

    assert logger.resumed is False
    events = read_events(log_path)
    # workflow_end NOT truncated (the prior log is left intact)...
    assert "workflow_end" in types_of(events)
    # ...and a fresh header was appended.
    starts = [e for e in events if e["event_type"] == "workflow_start"]
    assert len(starts) == 2
    assert starts[-1]["wf_uuid"] == "test-wf-uuid"


def test_resume_corrupt_log_starts_fresh(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    log_path = tmp_path / "events.jsonl"
    log_path.write_text("this is not json{{{\n{bad\n")

    logger = EventLogger(info, db, log_path=log_path)

    assert logger.resumed is False
    # A fresh header is appended after the garbage (which is left in place).
    starts = [
        e
        for e in read_valid_events(log_path)
        if e.get("event_type") == "workflow_start"
    ]
    assert len(starts) == 1
    assert starts[0]["wf_uuid"] == "test-wf-uuid"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Disk-exhaustion guard
# ─────────────────────────────────────────────────────────────────────────────


def _set_free(monkeypatch, free_bytes: int):
    """Monkeypatch event_log.shutil.disk_usage to report a given free space."""
    ns = types.SimpleNamespace(total=free_bytes * 10, used=0, free=free_bytes)
    monkeypatch.setattr(event_log.shutil, "disk_usage", lambda _p: ns)


MB = 1024 * 1024


def test_guard_disabled_when_no_floor_and_no_cap(info, open_db, clock, tmp_path):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    logger = EventLogger(
        info, db, log_path=tmp_path / "events.jsonl", min_free_mb=0.0, max_log_mb=None
    )
    # _disk_ok short-circuits to True without ever calling disk_usage.
    assert logger._disk_ok() is True


def test_low_space_pauses_then_recovers(
    info, open_db, clock, tmp_path, monkeypatch, make_snapshot
):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    # Plenty of space at construction so the header writes cleanly.
    _set_free(monkeypatch, 1000 * MB)
    logger = EventLogger(
        info, db, log_path=tmp_path / "events.jsonl", min_free_mb=100.0
    )
    assert read_events(logger.path)[0]["event_type"] == "workflow_start"

    # Each record() carries a distinct condor_jobs fingerprint so it always
    # *attempts* an emit (and therefore re-runs the disk guard). Without this,
    # an unchanged poll cycle would emit nothing and never re-check the disk.
    snap = make_snapshot(states=["RUNNING"], wf_state="WORKFLOW_STARTED")

    def cjobs(status):
        return [{"ClusterId": 101, "JobStatus": status}]

    # Drop free space below the 100 MB floor and step past the 5s throttle.
    _set_free(monkeypatch, 10 * MB)
    clock["t"] += 10.0
    logger.record(snap, condor_jobs=cjobs(2))  # first emit re-checks disk -> pauses

    events = read_events(logger.path)
    assert "log_paused" in types_of(events)
    assert logger._paused is True
    paused_ev = [e for e in events if e["event_type"] == "log_paused"][0]
    assert "below floor" in paused_ev["reason"]

    # While paused, further emits are dropped (counted).
    n_before = len(read_events(logger.path))
    clock["t"] += 10.0
    logger.record(snap, condor_jobs=cjobs(3))  # would emit, but dropped
    assert logger._dropped_events >= 1
    # No new event lines appended while paused.
    assert len(read_events(logger.path)) == n_before

    # Recover: free space above 1.5x the floor (>150 MB) and step the clock.
    _set_free(monkeypatch, 500 * MB)
    clock["t"] += 10.0
    logger.record(snap, condor_jobs=cjobs(4))  # re-checks disk -> resumes

    events = read_events(logger.path)
    assert "log_resumed" in types_of(events)
    resumed_ev = [e for e in events if e["event_type"] == "log_resumed"][0]
    assert resumed_ev["dropped"] >= 1
    assert logger._paused is False
    assert logger._dropped_events == 0


def test_max_log_cap_pauses(info, open_db, clock, tmp_path, monkeypatch, make_snapshot):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    _set_free(monkeypatch, 100000 * MB)  # disk never the limiting factor
    # Tiny cap so any content trips it. min_free_mb=0 -> only the cap is active.
    logger = EventLogger(
        info,
        db,
        log_path=tmp_path / "events.jsonl",
        min_free_mb=0.0,
        max_log_mb=0.000001,  # ~1 byte cap
    )
    # Header already written (construction emit happened before file grew, but
    # the cap is checked on the next emit). Step time and record.
    clock["t"] += 10.0
    logger.record(make_snapshot(states=["RUNNING"]))

    events = read_events(logger.path)
    assert "log_paused" in types_of(events)
    paused = [e for e in events if e["event_type"] == "log_paused"][0]
    assert "exceeds cap" in paused["reason"]


def test_disk_check_throttled(
    info, open_db, clock, tmp_path, monkeypatch, make_snapshot
):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    _set_free(monkeypatch, 1000 * MB)
    logger = EventLogger(
        info, db, log_path=tmp_path / "events.jsonl", min_free_mb=100.0
    )

    # Drop space below floor but DO NOT advance the clock past the 5s interval.
    _set_free(monkeypatch, 1 * MB)
    clock["t"] += 1.0  # < 5s throttle -> guard returns cached (not paused)
    logger.record(make_snapshot(states=["RUNNING"]))
    assert logger._paused is False
    assert "log_paused" not in types_of(read_events(logger.path))


def test_emit_oserror_forces_pause(
    info, open_db, clock, tmp_path, monkeypatch, make_snapshot
):
    db = open_db(jobs=_basic_jobs(), wf_states=_started_wf_states())
    _set_free(monkeypatch, 1000 * MB)
    logger = EventLogger(
        info, db, log_path=tmp_path / "events.jsonl", min_free_mb=100.0
    )

    # Make the next _write_raw raise OSError exactly once (simulate ENOSPC).
    real_write = logger._fh.write
    calls = {"n": 0}

    def flaky_write(s):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("No space left on device")
        return real_write(s)

    monkeypatch.setattr(logger._fh, "write", flaky_write)
    clock["t"] += 10.0  # ensure disk check is fresh; disk is fine (1000 MB)

    snap = make_snapshot(states=["RUNNING"])
    logger.record(snap)  # first emit -> jobs_init triggers write -> OSError

    # _emit caught the OSError, force-paused, and dropped the event.
    assert logger._paused is True
    assert logger._dropped_events >= 1
    # The force_pause marker is written via _write_raw (now succeeds).
    assert "log_paused" in types_of(read_events(logger.path))
    pause = [e for e in read_events(logger.path) if e["event_type"] == "log_paused"][0]
    assert "write failed" in pause["reason"]
