"""Unit tests for :mod:`workflow_monitor.db`.

Covers the pure module-level helpers, the ``JobRecord`` / ``WorkflowSnapshot``
dataclasses, and the ``StampedeDB`` SQLite interface exercised against real
temporary databases built by the shared ``db_factory`` / ``diamond_db``
fixtures.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

import pytest

from workflow_monitor import db as dbmod
from workflow_monitor.db import (
    StampedeDB,
    WorkflowSnapshot,
    display_state,
    fmt_duration,
    fmt_memory,
    fmt_timestamp,
    real_exitcode,
)

# Minimal schema for the hand-built retry DB (multiple job_instances per
# job_id, which the shared db_factory cannot model). Mirrors the subset of
# columns get_jobs() queries.
_RETRY_SCHEMA = """
CREATE TABLE workflow (
    wf_id INTEGER PRIMARY KEY, wf_uuid TEXT, dax_label TEXT
);
CREATE TABLE job (
    job_id INTEGER PRIMARY KEY, wf_id INTEGER, exec_job_id TEXT, type_desc TEXT
);
CREATE TABLE job_instance (
    job_instance_id INTEGER PRIMARY KEY, job_id INTEGER, job_submit_seq INTEGER,
    exitcode INTEGER, site TEXT, stdout_file TEXT, stderr_file TEXT
);
CREATE TABLE jobstate (
    job_instance_id INTEGER, state TEXT, timestamp REAL, jobstate_submit_seq INTEGER
);
CREATE TABLE task (
    task_id INTEGER PRIMARY KEY, job_id INTEGER, transformation TEXT, argv TEXT
);
CREATE TABLE invocation (
    invocation_id INTEGER PRIMARY KEY, job_instance_id INTEGER,
    task_submit_seq INTEGER, maxrss INTEGER
);
"""


# ─────────────────────────────────────────────────────────────────────────────
# display_state
# ─────────────────────────────────────────────────────────────────────────────


def test_display_state_none():
    assert display_state(None) == "UNSUBMITTED"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("POST_SCRIPT_SUCCESS", "SUCCESS"),
        ("JOB_SUCCESS", "SUCCESS"),
        ("POST_SCRIPT_FAILURE", "FAILED"),
        ("POST_SCRIPT_FAILED", "FAILED"),
        ("JOB_FAILURE", "FAILED"),
        ("JOB_FAILED", "FAILED"),
        ("EXECUTE", "RUNNING"),
        ("SUBMIT", "QUEUED"),
        ("PRE_SCRIPT_STARTED", "PRE"),
        ("PRE_SCRIPT_SUCCESS", "PRE"),
        ("POST_SCRIPT_STARTED", "POST"),
        ("POST_SCRIPT_TERMINATED", "POST"),
        ("JOB_TERMINATED", "DONE"),
        ("JOB_HELD", "HELD"),
        ("JOB_EVICTED", "HELD"),
    ],
)
def test_display_state_mapped(raw, expected):
    assert display_state(raw) == expected


def test_display_state_unknown_passthrough():
    # Unrecognized states are returned verbatim.
    assert display_state("SOMETHING_WEIRD") == "SOMETHING_WEIRD"
    assert display_state("") == ""


# ─────────────────────────────────────────────────────────────────────────────
# fmt_duration
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (None, "-"),
        (-1, "-"),
        (-0.5, "-"),  # negative float
        (0, "0s"),
        (0.9, "0s"),  # truncates toward zero (int())
        (1, "1s"),
        (59, "59s"),
        (59.9, "59s"),
        (60, "1m00s"),
        (61, "1m01s"),
        (125, "2m05s"),
        (3599, "59m59s"),
        (3600, "1h00m00s"),
        (3661, "1h01m01s"),
        (7325, "2h02m05s"),
    ],
)
def test_fmt_duration(seconds, expected):
    assert fmt_duration(seconds) == expected


# ─────────────────────────────────────────────────────────────────────────────
# fmt_timestamp
# ─────────────────────────────────────────────────────────────────────────────


def test_fmt_timestamp_none():
    assert fmt_timestamp(None) == "-"


@pytest.mark.parametrize("ts", [0.0, 1_700_000_000.0, 1_000_000_000.5])
def test_fmt_timestamp_matches_local_strftime(ts):
    # The function uses local-time fromtimestamp; assert against the same.
    expected = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
    got = fmt_timestamp(ts)
    assert got == expected
    assert len(got) == 8
    assert got[2] == ":" and got[5] == ":"


# ─────────────────────────────────────────────────────────────────────────────
# real_exitcode
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, None),
        # ── Normal termination: exit status in the high byte (N << 8) ──────
        (0, 0),  # exit(0)
        (256, 1),  # exit(1)
        (512, 2),  # exit(2)
        (126 << 8, 126),  # exit(126) == 32256
        (32512, 127),  # exit(127)
        (128 << 8, 128),  # exit(128) == 32768
        # ── Signal termination: 128 + signal (low 7 bits), core bit ignored ─
        (9, 137),  # SIGKILL, no core
        (137, 137),  # SIGKILL with core (0x89) — previously mis-decoded to 0
        (11, 139),  # SIGSEGV, no core
        (139, 139),  # SIGSEGV with core (0x8B) — previously mis-decoded to 0
        (15, 143),  # SIGTERM
        (129, 129),  # SIGHUP with core (0x81) — previously mis-decoded to 0
        # ── Degenerate boundary values ────────────────────────────────────
        (-1, -1),  # negative sentinel passes through
        (128, 0),  # 0x80: core bit only, no signal -> WIFEXITED high byte (0)
    ],
)
def test_real_exitcode(raw, expected):
    assert real_exitcode(raw) == expected


# ─────────────────────────────────────────────────────────────────────────────
# fmt_memory
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "kb,expected",
    [
        (None, "-"),
        (0, "0.0M"),
        (1024, "1.0M"),  # 1 MB
        (51200, "50.0M"),  # 50 MB
        (1024 * 1023, "1023.0M"),  # just under 1 GB stays in MB
        (1024 * 1024, "1.00G"),  # exactly 1 GB switches to G
        (1024 * 1024 * 2, "2.00G"),
        (1024 * 1024 + 1024 * 512, "1.50G"),  # 1.5 GB
    ],
)
def test_fmt_memory(kb, expected):
    assert fmt_memory(kb) == expected


# ─────────────────────────────────────────────────────────────────────────────
# JobRecord properties
# ─────────────────────────────────────────────────────────────────────────────


def test_jobrecord_disp_state(make_job):
    assert make_job(raw_state="JOB_SUCCESS").disp_state == "SUCCESS"
    assert make_job(raw_state="EXECUTE").disp_state == "RUNNING"
    assert make_job(raw_state=None).disp_state == "UNSUBMITTED"


def test_jobrecord_duration_start_and_end(make_job):
    j = make_job(start_time=110.0, end_time=150.0, raw_state="JOB_SUCCESS")
    assert j.duration == 40.0


def test_jobrecord_duration_running_uses_now_override(make_job):
    j = make_job(
        raw_state="EXECUTE",
        start_time=110.0,
        end_time=None,
    )
    j._now = 200.0
    assert j.duration == 90.0


def test_jobrecord_duration_running_falls_back_to_wallclock(make_job, monkeypatch):
    j = make_job(raw_state="EXECUTE", start_time=110.0, end_time=None)
    j._now = None

    class _FakeDateTime:
        @staticmethod
        def now():
            class _T:
                @staticmethod
                def timestamp():
                    return 300.0

            return _T()

    monkeypatch.setattr(dbmod, "datetime", _FakeDateTime)
    assert j.duration == 190.0


def test_jobrecord_duration_running_with_end_prefers_end(make_job):
    # If both start and end are set, the end-time branch wins even for RUNNING.
    j = make_job(raw_state="EXECUTE", start_time=110.0, end_time=140.0)
    assert j.duration == 30.0


def test_jobrecord_duration_missing_start(make_job):
    assert make_job(start_time=None, end_time=150.0).duration is None


def test_jobrecord_duration_not_running_no_end(make_job):
    # start set but no end, and not RUNNING -> None
    j = make_job(raw_state="JOB_TERMINATED", start_time=110.0, end_time=None)
    assert j.duration is None


def test_jobrecord_duration_zero_start_time_honored(make_job):
    # A start_time of exactly 0.0 (epoch) is a real timestamp, not "missing":
    # the guard uses `is not None`, so duration is computed normally.
    j = make_job(start_time=0.0, end_time=10.0, raw_state="JOB_SUCCESS")
    assert j.duration == 10.0


def test_jobrecord_is_compute(make_job):
    assert make_job(type_desc="compute").is_compute is True
    assert make_job(type_desc="create-dir").is_compute is False
    assert make_job(type_desc="stage-in-tx").is_compute is False


def test_jobrecord_display_name_compute_uses_transformation(make_job):
    j = make_job(
        type_desc="compute",
        exec_job_id="preprocess_ID0000001",
        transformation="pegasus::preprocess",
    )
    assert j.display_name == "pegasus::preprocess"


def test_jobrecord_display_name_compute_without_transformation(make_job):
    j = make_job(
        type_desc="compute", exec_job_id="preprocess_ID0000001", transformation=None
    )
    assert j.display_name == "preprocess_ID0000001"


def test_jobrecord_display_name_noncompute_ignores_transformation(make_job):
    # Non-compute jobs always use exec_job_id even if a transformation is set.
    j = make_job(
        type_desc="stage-in-tx",
        exec_job_id="stage_in_local_0",
        transformation="pegasus::transfer",
    )
    assert j.display_name == "stage_in_local_0"


def test_jobrecord_short_name(make_job):
    j = make_job(exec_job_id="preprocess_ID0000001")
    assert j.short_name == "preprocess_ID0000001"


# ─────────────────────────────────────────────────────────────────────────────
# WorkflowSnapshot status flags
# ─────────────────────────────────────────────────────────────────────────────


def test_snapshot_is_running(make_snapshot):
    snap = make_snapshot(wf_state="WORKFLOW_STARTED")
    assert snap.is_running is True
    assert snap.is_complete is False
    assert snap.succeeded is False
    assert snap.failed is False


def test_snapshot_succeeded(make_snapshot):
    snap = make_snapshot(wf_state="WORKFLOW_TERMINATED", wf_status=0)
    assert snap.is_complete is True
    assert snap.is_running is False
    assert snap.succeeded is True
    assert snap.failed is False


def test_snapshot_failed(make_snapshot):
    snap = make_snapshot(wf_state="WORKFLOW_TERMINATED", wf_status=1)
    assert snap.is_complete is True
    assert snap.succeeded is False
    assert snap.failed is True


def test_snapshot_terminated_none_status_is_failed(make_snapshot):
    # wf_status None != 0 -> failed() returns True when terminated.
    snap = make_snapshot(wf_state="WORKFLOW_TERMINATED", wf_status=None)
    assert snap.failed is True
    assert snap.succeeded is False


def test_snapshot_unknown_state(make_snapshot):
    snap = make_snapshot(wf_state="UNKNOWN", wf_status=None)
    assert snap.is_running is False
    assert snap.is_complete is False
    assert snap.succeeded is False
    assert snap.failed is False


# ─────────────────────────────────────────────────────────────────────────────
# WorkflowSnapshot.elapsed
# ─────────────────────────────────────────────────────────────────────────────


def test_snapshot_elapsed_uses_wf_end(make_snapshot):
    snap = make_snapshot(wf_start=49.0, wf_end=255.0, poll_time=10_000.0)
    assert snap.elapsed == 206.0


def test_snapshot_elapsed_running_uses_poll_time(make_snapshot):
    snap = make_snapshot(wf_start=49.0, wf_end=None, poll_time=149.0)
    assert snap.elapsed == 100.0


def test_snapshot_elapsed_none_when_no_start(make_snapshot):
    snap = make_snapshot(wf_start=None, wf_end=255.0, poll_time=10_000.0)
    assert snap.elapsed is None


def test_snapshot_elapsed_zero_wf_end_honored(make_snapshot):
    # wf_end == 0.0 is a real end timestamp (guard uses `is not None`), so
    # elapsed uses it rather than falling back to poll_time. With wf_start 0.0
    # this yields 0.0; the old truthiness guard would have returned 149.0.
    snap = make_snapshot(wf_start=0.0, wf_end=0.0, poll_time=149.0)
    assert snap.elapsed == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# WorkflowSnapshot counts
# ─────────────────────────────────────────────────────────────────────────────


def test_snapshot_counts_mixed_roster(make_snapshot):
    states = [
        "SUCCESS",
        "SUCCESS",
        "FAILED",
        "RUNNING",
        "HELD",
        "QUEUED",
        "PRE",
        "POST",
    ]
    snap = make_snapshot(states=states)

    assert snap.total_jobs() == 8
    assert snap.done_count() == 2
    assert snap.failed_count() == 1
    assert snap.held_count() == 1
    assert snap.running_count() == 1
    # queued_count includes QUEUED + PRE + POST
    assert snap.queued_count() == 3

    counts = snap.job_counts()
    assert counts == {
        "SUCCESS": 2,
        "FAILED": 1,
        "RUNNING": 1,
        "HELD": 1,
        "QUEUED": 1,
        "PRE": 1,
        "POST": 1,
    }


def test_snapshot_held_and_failed_job_lists(make_snapshot):
    snap = make_snapshot(states=["SUCCESS", "HELD", "FAILED", "HELD"])
    held = snap.held_jobs()
    failed = snap.failed_jobs()
    assert len(held) == 2
    assert all(j.disp_state == "HELD" for j in held)
    assert len(failed) == 1
    assert failed[0].disp_state == "FAILED"


def test_snapshot_compute_vs_infra(make_job):
    jobs = [
        make_job(job_id=1, type_desc="compute"),
        make_job(job_id=2, type_desc="compute"),
        make_job(job_id=3, type_desc="create-dir"),
        make_job(job_id=4, type_desc="stage-in-tx"),
    ]
    snap = WorkflowSnapshot(
        wf_state="WORKFLOW_STARTED",
        wf_status=None,
        wf_start=49.0,
        wf_end=None,
        jobs=jobs,
        recent_events=[],
    )
    assert len(snap.compute_jobs()) == 2
    assert len(snap.infra_jobs()) == 2
    assert {j.job_id for j in snap.compute_jobs()} == {1, 2}
    assert {j.job_id for j in snap.infra_jobs()} == {3, 4}


def test_snapshot_progress_pct(make_snapshot):
    snap = make_snapshot(states=["SUCCESS", "SUCCESS", "RUNNING", "QUEUED"])
    assert snap.progress_pct() == pytest.approx(50.0)


def test_snapshot_progress_pct_all_done(make_snapshot):
    snap = make_snapshot(states=["SUCCESS", "SUCCESS"])
    assert snap.progress_pct() == pytest.approx(100.0)


def test_snapshot_progress_pct_zero_jobs(make_snapshot):
    snap = make_snapshot(states=[])
    assert snap.total_jobs() == 0
    assert snap.progress_pct() == 0.0
    assert snap.job_counts() == {}


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB: connection management
# ─────────────────────────────────────────────────────────────────────────────


def test_connect_close(diamond_db):
    db = StampedeDB(diamond_db)
    assert db._conn is None
    db.connect()
    assert db._conn is not None
    db.close()
    assert db._conn is None


def test_close_is_idempotent(diamond_db):
    db = StampedeDB(diamond_db)
    db.close()  # no connection yet -> no error
    db.connect()
    db.close()
    db.close()  # second close -> no error
    assert db._conn is None


def test_context_manager(diamond_db):
    with StampedeDB(diamond_db) as db:
        assert db._conn is not None
        snap = db.snapshot()
        assert isinstance(snap, WorkflowSnapshot)
    assert db._conn is None


def test_conn_or_raise_autoconnects(diamond_db):
    db = StampedeDB(diamond_db)
    # No explicit connect(); a query should lazily connect.
    state = db.get_workflow_state()
    assert db._conn is not None
    assert state["state"] == "WORKFLOW_TERMINATED"
    db.close()


def test_connect_opens_readonly(diamond_db):
    # The read-only URI must reject writes.
    with StampedeDB(diamond_db) as db:
        with pytest.raises(sqlite3.OperationalError):
            db._conn.execute("INSERT INTO workflow (wf_id) VALUES (999)")


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB: wf_uuid resolution
# ─────────────────────────────────────────────────────────────────────────────


def test_resolve_wf_id_with_known_uuid(diamond_db):
    with StampedeDB(diamond_db, wf_uuid="test-wf-uuid") as db:
        assert db._wf_id == 1


def test_resolve_wf_id_unknown_uuid_stays_none(diamond_db):
    with StampedeDB(diamond_db, wf_uuid="does-not-exist") as db:
        assert db._wf_id is None
        # Falls back to the IS-NULL branch, which still returns data.
        assert db.get_workflow_state()["state"] == "WORKFLOW_TERMINATED"


def test_no_uuid_uses_isnull_branch(diamond_db):
    with StampedeDB(diamond_db) as db:
        assert db._wf_uuid is None
        assert db._wf_id is None
        jobs = db.get_jobs()
        assert len(jobs) == 5


def test_uuid_branch_and_isnull_branch_agree(diamond_db):
    with StampedeDB(diamond_db, wf_uuid="test-wf-uuid") as db1:
        jobs_with = db1.get_jobs()
    with StampedeDB(diamond_db) as db2:
        jobs_without = db2.get_jobs()
    assert [j.job_id for j in jobs_with] == [j.job_id for j in jobs_without]


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB: get_workflow_state
# ─────────────────────────────────────────────────────────────────────────────


def test_get_workflow_state_latest_row(db_factory):
    path = db_factory(
        jobs=[],
        wf_states=[
            {"state": "WORKFLOW_STARTED", "timestamp": 10.0, "status": None},
            {"state": "WORKFLOW_TERMINATED", "timestamp": 20.0, "status": 0},
        ],
    )
    with StampedeDB(path) as db:
        st = db.get_workflow_state()
        assert st["state"] == "WORKFLOW_TERMINATED"
        assert st["timestamp"] == 20.0
        assert st["status"] == 0


def test_get_workflow_state_no_rows(db_factory):
    path = db_factory(jobs=[], wf_states=[])
    with StampedeDB(path) as db:
        st = db.get_workflow_state()
        assert st == {"state": "UNKNOWN", "timestamp": None, "status": None}


def test_get_workflow_state_with_uuid_scoped(db_factory):
    path = db_factory(
        wf_states=[
            {"state": "WORKFLOW_STARTED", "timestamp": 5.0, "status": None},
        ]
    )
    with StampedeDB(path, wf_uuid="test-wf-uuid") as db:
        assert db._wf_id == 1
        assert db.get_workflow_state()["state"] == "WORKFLOW_STARTED"


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB: get_workflow_times
# ─────────────────────────────────────────────────────────────────────────────


def test_get_workflow_times(diamond_db):
    with StampedeDB(diamond_db) as db:
        times = db.get_workflow_times()
        assert times == {"start": 49.0, "end": 255.0}


def test_get_workflow_times_running_only(db_factory):
    path = db_factory(
        wf_states=[{"state": "WORKFLOW_STARTED", "timestamp": 49.0, "status": None}]
    )
    with StampedeDB(path) as db:
        times = db.get_workflow_times()
        assert times == {"start": 49.0, "end": None}


def test_get_workflow_times_empty(db_factory):
    path = db_factory(wf_states=[])
    with StampedeDB(path) as db:
        assert db.get_workflow_times() == {"start": None, "end": None}


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB: get_jobs
# ─────────────────────────────────────────────────────────────────────────────


def test_get_jobs_count_and_order(diamond_db):
    with StampedeDB(diamond_db) as db:
        jobs = db.get_jobs()
    assert [j.job_id for j in jobs] == [1, 2, 3, 4, 5]


def test_get_jobs_state_and_times(diamond_db):
    with StampedeDB(diamond_db) as db:
        jobs = {j.job_id: j for j in db.get_jobs()}

    j1 = jobs[1]
    assert j1.exec_job_id == "preprocess_ID0000001"
    assert j1.type_desc == "compute"
    assert j1.raw_state == "POST_SCRIPT_SUCCESS"
    assert j1.disp_state == "SUCCESS"
    assert j1.exitcode == 0
    assert j1.site == "condorpool"
    assert j1.submit_time == 100.0
    assert j1.start_time == 110.0
    # end_time = MAX over JOB_TERMINATED/JOB_SUCCESS/JOB_FAILURE -> 151.0
    assert j1.end_time == 151.0
    assert j1.duration == pytest.approx(41.0)
    assert j1.transformation == "pegasus::preprocess"
    assert j1.maxrss == 51200


def test_get_jobs_enrichment_metadata(diamond_db):
    with StampedeDB(diamond_db) as db:
        jobs = {j.job_id: j for j in db.get_jobs()}
    # compute jobs carry transformation + maxrss
    assert jobs[4].transformation == "pegasus::analyze"
    assert jobs[4].maxrss == 60000
    # the create-dir job has no task row -> no transformation, no maxrss
    assert jobs[5].transformation is None
    assert jobs[5].maxrss is None
    assert jobs[5].type_desc == "create-dir"
    assert jobs[5].is_compute is False


def test_get_jobs_latest_instance_selection(tmp_path):
    # Two job_instances for the SAME job_id: a failed first attempt (seq 0) and
    # a successful retry (seq 1). get_jobs must pick the latest (MAX seq = 1).
    # The shared db_factory keys job_id as a PK and inserts one job row per
    # spec, so it cannot model retries; build the DB directly here instead.
    path = tmp_path / "retry.stampede.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_RETRY_SCHEMA)
        conn.execute(
            "INSERT INTO workflow (wf_id, wf_uuid, dax_label) VALUES (1, ?, 'x')",
            ("test-wf-uuid",),
        )
        conn.execute(
            "INSERT INTO job (job_id, wf_id, exec_job_id, type_desc) "
            "VALUES (1, 1, 'retry_job', 'compute')"
        )
        # Attempt 0 (failed)
        conn.execute(
            "INSERT INTO job_instance (job_instance_id, job_id, job_submit_seq, "
            "exitcode, site) VALUES (10, 1, 0, 1, 'siteA')"
        )
        # Attempt 1 (succeeded) -- the latest by job_submit_seq
        conn.execute(
            "INSERT INTO job_instance (job_instance_id, job_id, job_submit_seq, "
            "exitcode, site) VALUES (11, 1, 1, 0, 'condorpool')"
        )
        js = [
            (10, "SUBMIT", 100.0, 1),
            (10, "EXECUTE", 110.0, 2),
            (10, "JOB_FAILURE", 120.0, 3),
            (11, "SUBMIT", 130.0, 4),
            (11, "EXECUTE", 140.0, 5),
            (11, "JOB_TERMINATED", 150.0, 6),
            (11, "JOB_SUCCESS", 151.0, 7),
        ]
        conn.executemany(
            "INSERT INTO jobstate (job_instance_id, state, timestamp, "
            "jobstate_submit_seq) VALUES (?,?,?,?)",
            js,
        )
        conn.commit()
    finally:
        conn.close()

    with StampedeDB(path) as db:
        jobs = db.get_jobs()
    assert len(jobs) == 1
    j = jobs[0]
    # Latest instance (seq 1) has exitcode 0, site condorpool, SUCCESS state.
    assert j.exitcode == 0
    assert j.site == "condorpool"
    assert j.raw_state == "JOB_SUCCESS"
    assert j.disp_state == "SUCCESS"
    # submit_time = MIN SUBMIT across both instances of this job_id -> 100.0
    assert j.submit_time == 100.0
    # start_time = MIN EXECUTE -> 110.0; end_time = MAX terminal -> 151.0
    assert j.start_time == 110.0
    assert j.end_time == 151.0


def test_get_jobs_maxrss_ignores_negative_task_seq(db_factory):
    # An invocation with task_submit_seq < 0 (e.g. clustering wrapper) must be
    # excluded from the maxrss MAX().
    path = db_factory(
        jobs=[
            {
                "job_id": 1,
                "exec_job_id": "job1",
                "type_desc": "compute",
                "states": [
                    ("SUBMIT", 100.0),
                    ("EXECUTE", 110.0),
                    ("JOB_SUCCESS", 120.0),
                ],
                "transformation": "pegasus::x",
                "maxrss": 12345,
                "task_submit_seq": -1,  # excluded by the WHERE task_submit_seq >= 0
            }
        ]
    )
    with StampedeDB(path) as db:
        j = db.get_jobs()[0]
    assert j.maxrss is None


def test_get_jobs_argv_via_task(db_factory):
    path = db_factory(
        jobs=[
            {
                "job_id": 1,
                "exec_job_id": "job1",
                "type_desc": "compute",
                "states": [("SUBMIT", 100.0)],
                "transformation": "pegasus::x",
                "argv": "-i input.txt -o out.txt",
            }
        ]
    )
    with StampedeDB(path) as db:
        j = db.get_jobs()[0]
    assert j.task_argv == "-i input.txt -o out.txt"
    assert j.transformation == "pegasus::x"


def test_get_jobs_queued_only_has_no_start_or_end(db_factory):
    path = db_factory(
        jobs=[
            {
                "job_id": 1,
                "exec_job_id": "queued_job",
                "type_desc": "compute",
                "states": [("SUBMIT", 100.0)],
            }
        ]
    )
    with StampedeDB(path) as db:
        j = db.get_jobs()[0]
    assert j.submit_time == 100.0
    assert j.start_time is None
    assert j.end_time is None
    assert j.duration is None
    assert j.disp_state == "QUEUED"


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB: get_recent_events / get_events_since
# ─────────────────────────────────────────────────────────────────────────────


def test_get_recent_events_ordering_and_limit(diamond_db):
    with StampedeDB(diamond_db) as db:
        events = db.get_recent_events(limit=3)
    assert len(events) == 3
    # Descending by timestamp: the latest diamond event is analyze JOB_SUCCESS@251.
    ts = [e["timestamp"] for e in events]
    assert ts == sorted(ts, reverse=True)
    assert events[0]["timestamp"] == 251.0
    assert events[0]["state"] == "JOB_SUCCESS"
    assert events[0]["exec_job_id"] == "analyze_ID0000004"


def test_get_recent_events_default_limit_caps(db_factory):
    # Build a workflow with > 20 jobstate rows and confirm default limit=20.
    jobs = []
    for i in range(1, 11):  # 10 jobs * 3 states = 30 rows
        jobs.append(
            {
                "job_id": i,
                "exec_job_id": f"job{i}",
                "type_desc": "compute",
                "states": [
                    ("SUBMIT", 100.0 + i),
                    ("EXECUTE", 200.0 + i),
                    ("JOB_SUCCESS", 300.0 + i),
                ],
            }
        )
    path = db_factory(jobs=jobs)
    with StampedeDB(path) as db:
        events = db.get_recent_events()
    assert len(events) == 20


def test_get_recent_events_keys(diamond_db):
    with StampedeDB(diamond_db) as db:
        events = db.get_recent_events(limit=1)
    assert set(events[0].keys()) == {"exec_job_id", "type_desc", "state", "timestamp"}


def test_get_events_since_filters_and_orders(diamond_db):
    with StampedeDB(diamond_db) as db:
        events = db.get_events_since(200.0)
    # All returned timestamps strictly > 200.0
    assert all(e["timestamp"] > 200.0 for e in events)
    # Ascending order
    ts = [e["timestamp"] for e in events]
    assert ts == sorted(ts)
    # Diamond events strictly after 200.0:
    #   job3 JOB_SUCCESS@201, job4 SUBMIT@205, EXECUTE@210,
    #   JOB_TERMINATED@250, JOB_SUCCESS@251 -> 5 events.
    assert ts == [201.0, 205.0, 210.0, 250.0, 251.0]


def test_get_events_since_boundary_is_exclusive(db_factory):
    path = db_factory(
        jobs=[
            {
                "job_id": 1,
                "exec_job_id": "j1",
                "type_desc": "compute",
                "states": [("SUBMIT", 100.0), ("EXECUTE", 110.0)],
            }
        ]
    )
    with StampedeDB(path) as db:
        # after_ts == 100.0 -> SUBMIT@100 excluded, EXECUTE@110 included
        events = db.get_events_since(100.0)
    assert [e["state"] for e in events] == ["EXECUTE"]


def test_get_events_since_returns_all_when_before_everything(diamond_db):
    with StampedeDB(diamond_db) as db:
        events = db.get_events_since(0.0)
    # All jobstate rows for the diamond: 5 jobs.
    # job1: 7 states, job2-4: 4 each, job5: 4 -> 7 + 4*4 = 23
    assert len(events) == 23
    assert "exitcode" in events[0]
    assert "maxrss" in events[0]


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB.snapshot end-to-end
# ─────────────────────────────────────────────────────────────────────────────


def test_snapshot_end_to_end_diamond(diamond_db):
    with StampedeDB(diamond_db) as db:
        snap = db.snapshot()

    assert isinstance(snap, WorkflowSnapshot)
    assert snap.wf_state == "WORKFLOW_TERMINATED"
    assert snap.wf_status == 0
    assert snap.wf_start == 49.0
    assert snap.wf_end == 255.0
    assert snap.is_complete is True
    assert snap.succeeded is True
    assert snap.elapsed == 206.0

    assert snap.total_jobs() == 5
    assert snap.done_count() == 5  # all 5 jobs in SUCCESS state
    assert snap.failed_count() == 0
    assert snap.progress_pct() == pytest.approx(100.0)
    assert len(snap.compute_jobs()) == 4
    assert len(snap.infra_jobs()) == 1

    # recent_events populated (default limit 20, diamond has 23 rows -> 20)
    assert len(snap.recent_events) == 20


def test_snapshot_running_workflow(db_factory):
    path = db_factory(
        jobs=[
            {
                "job_id": 1,
                "exec_job_id": "p1",
                "type_desc": "compute",
                "states": [
                    ("SUBMIT", 100.0),
                    ("EXECUTE", 110.0),
                    ("JOB_SUCCESS", 120.0),
                ],
                "exitcode": 0,
                "transformation": "pegasus::p1",
            },
            {
                "job_id": 2,
                "exec_job_id": "p2",
                "type_desc": "compute",
                "states": [("SUBMIT", 105.0), ("EXECUTE", 115.0)],
            },
        ],
        wf_states=[{"state": "WORKFLOW_STARTED", "timestamp": 50.0, "status": None}],
    )
    with StampedeDB(path) as db:
        snap = db.snapshot()
    assert snap.is_running is True
    assert snap.is_complete is False
    assert snap.wf_end is None
    assert snap.done_count() == 1
    assert snap.running_count() == 1
    assert snap.progress_pct() == pytest.approx(50.0)


def test_snapshot_empty_db(db_factory):
    path = db_factory(jobs=[], wf_states=[])
    with StampedeDB(path) as db:
        snap = db.snapshot()
    assert snap.wf_state == "UNKNOWN"
    assert snap.wf_status is None
    assert snap.wf_start is None
    assert snap.wf_end is None
    assert snap.elapsed is None
    assert snap.jobs == []
    assert snap.recent_events == []
    assert snap.progress_pct() == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# StampedeDB.snapshot: OperationalError swallowing
# ─────────────────────────────────────────────────────────────────────────────


def test_snapshot_swallows_operational_error(diamond_db, monkeypatch):
    with StampedeDB(diamond_db) as db:

        def _boom():
            raise sqlite3.OperationalError("database is locked")

        # Patch the first call inside snapshot() to raise.
        monkeypatch.setattr(db, "get_workflow_state", _boom)
        snap = db.snapshot()

    # Falls back to a minimal, well-formed snapshot.
    assert snap.wf_state == "UNKNOWN"
    assert snap.wf_status is None
    assert snap.wf_start is None
    assert snap.wf_end is None
    assert snap.jobs == []
    assert snap.recent_events == []
    assert snap.elapsed is None


def test_snapshot_operational_error_midway(diamond_db, monkeypatch):
    # Even if the error happens after some queries succeed, the whole snapshot
    # falls back to minimal because the try/except wraps all four queries.
    with StampedeDB(diamond_db) as db:

        def _boom():
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(db, "get_jobs", _boom)
        snap = db.snapshot()

    assert snap.wf_state == "UNKNOWN"
    assert snap.jobs == []
    assert snap.recent_events == []


def test_snapshot_does_not_swallow_other_errors(diamond_db, monkeypatch):
    # A non-OperationalError must propagate (the except is narrow).
    with StampedeDB(diamond_db) as db:

        def _boom():
            raise ValueError("not an operational error")

        monkeypatch.setattr(db, "get_workflow_state", _boom)
        with pytest.raises(ValueError):
            db.snapshot()
