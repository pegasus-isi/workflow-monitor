"""Shared pytest fixtures for the workflow-monitor test suite.

The most important fixture here is :func:`build_stampede_db`, a factory that
constructs a temporary SQLite database mirroring the subset of the Pegasus
``stampede.db`` schema that :mod:`workflow_monitor.db` actually queries. Tests
that need realistic ``WorkflowSnapshot`` data should build a DB with this
factory and read it back through :class:`workflow_monitor.db.StampedeDB`, which
exercises the real SQL.

Lighter-weight factories (``make_job``, ``make_snapshot``) build the dataclasses
directly for tests that only care about the in-memory model.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pytest

from workflow_monitor.db import JobRecord, WorkflowSnapshot


# ─────────────────────────────────────────────────────────────────────────────
# stampede.db builder
# ─────────────────────────────────────────────────────────────────────────────

# The schema below is intentionally a *subset* of the real Pegasus stampede
# schema — just the tables and columns referenced by workflow_monitor.db. Each
# table keeps the column names the production queries use so the SQL exercises
# the same joins it would against a real database.
_SCHEMA = """
CREATE TABLE workflow (
    wf_id           INTEGER PRIMARY KEY,
    wf_uuid         TEXT,
    root_wf_uuid    TEXT,
    dax_label       TEXT,
    planner_version TEXT,
    submit_dir      TEXT
);
CREATE TABLE workflowstate (
    wf_id     INTEGER,
    state     TEXT,
    timestamp REAL,
    status    INTEGER
);
CREATE TABLE job (
    job_id      INTEGER PRIMARY KEY,
    wf_id       INTEGER,
    exec_job_id TEXT,
    type_desc   TEXT
);
CREATE TABLE job_instance (
    job_instance_id INTEGER PRIMARY KEY,
    job_id          INTEGER,
    job_submit_seq  INTEGER,
    exitcode        INTEGER,
    site            TEXT,
    stdout_file     TEXT,
    stderr_file     TEXT
);
CREATE TABLE jobstate (
    job_instance_id      INTEGER,
    state                TEXT,
    timestamp            REAL,
    jobstate_submit_seq  INTEGER
);
CREATE TABLE task (
    task_id        INTEGER PRIMARY KEY,
    job_id         INTEGER,
    transformation TEXT,
    argv           TEXT
);
CREATE TABLE invocation (
    invocation_id   INTEGER PRIMARY KEY,
    job_instance_id INTEGER,
    task_submit_seq INTEGER,
    maxrss          INTEGER
);
"""


def build_stampede_db(
    path: Path,
    *,
    jobs: Sequence[Dict[str, Any]] = (),
    wf_states: Sequence[Dict[str, Any]] = (),
    wf_uuid: str = "test-wf-uuid",
    wf_id: int = 1,
    dax_label: str = "test_dax",
    planner_version: str = "5.1.2",
    submit_dir: str = "/submit/run0001",
    root_wf_uuid: Optional[str] = None,
) -> Path:
    """Create and populate a stampede-like SQLite DB at *path*.

    Parameters
    ----------
    jobs:
        Each job spec is a dict::

            {
                "job_id": 1,
                "exec_job_id": "preprocess_ID0000001",
                "type_desc": "compute",          # default "compute"
                "states": [("SUBMIT", 100.0), ("EXECUTE", 110.0),
                           ("JOB_TERMINATED", 150.0), ("JOB_SUCCESS", 151.0)],
                "exitcode": 0,                   # optional
                "site": "condorpool",            # optional
                "stdout_file": "00/00/foo.out.000",  # optional
                "stderr_file": "00/00/foo.err",  # optional
                "transformation": "pegasus::preprocess",  # optional
                "argv": "-a top",                # optional
                "maxrss": 51200,                 # optional, KB
            }

        ``states`` is the ordered list of (jobstate, timestamp) transitions.
        The job's current_state is the last transition (highest seq). A single
        job_instance (job_submit_seq=0) is created per job.

    wf_states:
        Each spec is ``{"state": "WORKFLOW_STARTED", "timestamp": 50.0,
        "status": None}``. The latest by timestamp drives current state.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_SCHEMA)
        conn.execute(
            "INSERT INTO workflow (wf_id, wf_uuid, root_wf_uuid, dax_label, "
            "planner_version, submit_dir) VALUES (?,?,?,?,?,?)",
            (
                wf_id,
                wf_uuid,
                root_wf_uuid or wf_uuid,
                dax_label,
                planner_version,
                submit_dir,
            ),
        )
        for ws in wf_states:
            conn.execute(
                "INSERT INTO workflowstate (wf_id, state, timestamp, status) "
                "VALUES (?,?,?,?)",
                (wf_id, ws["state"], ws["timestamp"], ws.get("status")),
            )

        ji_id = 0
        js_seq = 0
        inv_id = 0
        task_id = 0
        for spec in jobs:
            job_id = spec["job_id"]
            conn.execute(
                "INSERT INTO job (job_id, wf_id, exec_job_id, type_desc) "
                "VALUES (?,?,?,?)",
                (job_id, wf_id, spec["exec_job_id"], spec.get("type_desc", "compute")),
            )
            ji_id += 1
            this_ji = ji_id
            conn.execute(
                "INSERT INTO job_instance (job_instance_id, job_id, "
                "job_submit_seq, exitcode, site, stdout_file, stderr_file) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    this_ji,
                    job_id,
                    spec.get("job_submit_seq", 0),
                    spec.get("exitcode"),
                    spec.get("site"),
                    spec.get("stdout_file"),
                    spec.get("stderr_file"),
                ),
            )
            for state, ts in spec.get("states", []):
                js_seq += 1
                conn.execute(
                    "INSERT INTO jobstate (job_instance_id, state, timestamp, "
                    "jobstate_submit_seq) VALUES (?,?,?,?)",
                    (this_ji, state, ts, js_seq),
                )
            if spec.get("transformation") is not None or spec.get("argv") is not None:
                task_id += 1
                conn.execute(
                    "INSERT INTO task (task_id, job_id, transformation, argv) "
                    "VALUES (?,?,?,?)",
                    (task_id, job_id, spec.get("transformation"), spec.get("argv")),
                )
            if spec.get("maxrss") is not None:
                inv_id += 1
                conn.execute(
                    "INSERT INTO invocation (invocation_id, job_instance_id, "
                    "task_submit_seq, maxrss) VALUES (?,?,?,?)",
                    (inv_id, this_ji, spec.get("task_submit_seq", 1), spec["maxrss"]),
                )
        conn.commit()
    finally:
        conn.close()
    return path


# Common job-state transition sequences ───────────────────────────────────────
SUCCESS_STATES = [
    ("SUBMIT", 100.0),
    ("EXECUTE", 110.0),
    ("JOB_TERMINATED", 150.0),
    ("JOB_SUCCESS", 151.0),
    ("POST_SCRIPT_STARTED", 152.0),
    ("POST_SCRIPT_TERMINATED", 153.0),
    ("POST_SCRIPT_SUCCESS", 154.0),
]
FAILED_STATES = [
    ("SUBMIT", 100.0),
    ("EXECUTE", 110.0),
    ("JOB_TERMINATED", 130.0),
    ("JOB_FAILURE", 131.0),
]
RUNNING_STATES = [("SUBMIT", 100.0), ("EXECUTE", 110.0)]
HELD_STATES = [("SUBMIT", 100.0), ("JOB_HELD", 105.0)]
QUEUED_STATES = [("SUBMIT", 100.0)]


@pytest.fixture
def state_seqs():
    """Common job-state transition sequences, as a dict (no import needed)."""
    return {
        "SUCCESS": list(SUCCESS_STATES),
        "FAILED": list(FAILED_STATES),
        "RUNNING": list(RUNNING_STATES),
        "HELD": list(HELD_STATES),
        "QUEUED": list(QUEUED_STATES),
    }


@pytest.fixture
def db_factory(tmp_path):
    """Return a callable that builds a stampede DB under a temp dir.

    Usage::

        db_path = db_factory(jobs=[...], wf_states=[...])
        from workflow_monitor.db import StampedeDB
        with StampedeDB(db_path) as db:
            snap = db.snapshot()
    """
    counter = {"n": 0}

    def _make(**kwargs) -> Path:
        counter["n"] += 1
        name = kwargs.pop("name", f"wf{counter['n']}.stampede.db")
        path = tmp_path / name
        return build_stampede_db(path, **kwargs)

    return _make


@pytest.fixture
def diamond_db(db_factory):
    """A realistic completed 4-node diamond workflow DB path."""
    jobs = [
        {
            "job_id": 1,
            "exec_job_id": "preprocess_ID0000001",
            "type_desc": "compute",
            "states": SUCCESS_STATES,
            "exitcode": 0,
            "site": "condorpool",
            "transformation": "pegasus::preprocess",
            "maxrss": 51200,
        },
        {
            "job_id": 2,
            "exec_job_id": "findrange_ID0000002",
            "type_desc": "compute",
            "states": [
                ("SUBMIT", 155.0),
                ("EXECUTE", 160.0),
                ("JOB_TERMINATED", 190.0),
                ("JOB_SUCCESS", 191.0),
            ],
            "exitcode": 0,
            "site": "condorpool",
            "transformation": "pegasus::findrange",
            "maxrss": 40960,
        },
        {
            "job_id": 3,
            "exec_job_id": "findrange_ID0000003",
            "type_desc": "compute",
            "states": [
                ("SUBMIT", 155.0),
                ("EXECUTE", 162.0),
                ("JOB_TERMINATED", 200.0),
                ("JOB_SUCCESS", 201.0),
            ],
            "exitcode": 0,
            "site": "condorpool",
            "transformation": "pegasus::findrange",
            "maxrss": 45000,
        },
        {
            "job_id": 4,
            "exec_job_id": "analyze_ID0000004",
            "type_desc": "compute",
            "states": [
                ("SUBMIT", 205.0),
                ("EXECUTE", 210.0),
                ("JOB_TERMINATED", 250.0),
                ("JOB_SUCCESS", 251.0),
            ],
            "exitcode": 0,
            "site": "condorpool",
            "transformation": "pegasus::analyze",
            "maxrss": 60000,
        },
        {
            "job_id": 5,
            "exec_job_id": "create_dir_diamond_0_local",
            "type_desc": "create-dir",
            "states": [
                ("SUBMIT", 50.0),
                ("EXECUTE", 55.0),
                ("JOB_TERMINATED", 60.0),
                ("JOB_SUCCESS", 61.0),
            ],
            "exitcode": 0,
            "site": "local",
        },
    ]
    wf_states = [
        {"state": "WORKFLOW_STARTED", "timestamp": 49.0, "status": None},
        {"state": "WORKFLOW_TERMINATED", "timestamp": 255.0, "status": 0},
    ]
    return db_factory(jobs=jobs, wf_states=wf_states, dax_label="diamond")


# ─────────────────────────────────────────────────────────────────────────────
# In-memory model factories
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def make_job():
    """Factory for JobRecord with sensible defaults."""

    def _make(**kwargs) -> JobRecord:
        defaults: Dict[str, Any] = dict(
            job_id=1,
            exec_job_id="job_ID0000001",
            type_desc="compute",
            raw_state="JOB_SUCCESS",
            exitcode=0,
            site="condorpool",
            submit_time=100.0,
            start_time=110.0,
            end_time=150.0,
        )
        defaults.update(kwargs)
        return JobRecord(**defaults)

    return _make


@pytest.fixture
def make_snapshot(make_job):
    """Factory for WorkflowSnapshot.

    Pass ``states=["SUCCESS", "RUNNING", ...]`` to quickly build a roster of
    jobs in the given display states, or pass ``jobs=[JobRecord, ...]``.
    """
    _raw_for_disp = {
        "SUCCESS": "JOB_SUCCESS",
        "FAILED": "JOB_FAILURE",
        "RUNNING": "EXECUTE",
        "QUEUED": "SUBMIT",
        "HELD": "JOB_HELD",
        "DONE": "JOB_TERMINATED",
        "POST": "POST_SCRIPT_STARTED",
        "PRE": "PRE_SCRIPT_STARTED",
        "UNSUBMITTED": None,
    }

    def _make(
        states: Optional[Sequence[str]] = None,
        jobs: Optional[List[JobRecord]] = None,
        wf_state: str = "WORKFLOW_STARTED",
        wf_status: Optional[int] = None,
        wf_start: Optional[float] = 49.0,
        wf_end: Optional[float] = None,
        recent_events: Optional[List[Dict]] = None,
        **snap_kwargs,
    ) -> WorkflowSnapshot:
        if jobs is None:
            jobs = []
            for i, disp in enumerate(states or [], start=1):
                jobs.append(
                    make_job(
                        job_id=i,
                        exec_job_id=f"job_ID{i:07d}",
                        raw_state=_raw_for_disp.get(disp, disp),
                    )
                )
        return WorkflowSnapshot(
            wf_state=wf_state,
            wf_status=wf_status,
            wf_start=wf_start,
            wf_end=wf_end,
            jobs=jobs,
            recent_events=recent_events or [],
            **snap_kwargs,
        )

    return _make


# ─────────────────────────────────────────────────────────────────────────────
# braindump.yml writer
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def write_braindump():
    """Write a braindump.yml into a directory and return its Path.

    Usage: ``bd = write_braindump(tmp_path, submit_dir=str(tmp_path))``
    """
    import yaml

    def _write(directory: Path, **overrides) -> Path:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        data = {
            "wf_uuid": "test-wf-uuid",
            "root_wf_uuid": "test-wf-uuid",
            "dax_label": "diamond",
            "submit_dir": str(directory),
            "basedir": str(directory.parent),
            "user": "tester",
            "planner_version": "5.1.2",
            "dag": "diamond-0.dag",
            "condor_log": "diamond-0.log",
            "timestamp": "20260101T000000+0000",
        }
        data.update(overrides)
        bd = directory / "braindump.yml"
        bd.write_text(yaml.safe_dump(data))
        return bd

    return _write


# ─────────────────────────────────────────────────────────────────────────────
# Sample HTCondor ClassAd dicts
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def condor_queue_ads() -> List[Dict[str, Any]]:
    """A couple of live-queue ClassAd dicts as returned by condor_q -json."""
    return [
        {
            "ClusterId": 101,
            "ProcId": 0,
            "JobStatus": 2,
            "Cmd": "/bin/preprocess",
            "DAGNodeName": "preprocess_ID0000001",
            "RemoteHost": "slot1@worker1.example.org",
            "QDate": 100,
            "JobStartDate": 110,
            "RequestCpus": 1,
            "RequestMemory": 2048,
            "RequestDisk": 1048576,
            "Owner": "tester",
            "BytesSent": 1024,
            "BytesRecvd": 2048,
        },
        {
            "ClusterId": 102,
            "ProcId": 0,
            "JobStatus": 5,
            "Cmd": "/bin/analyze",
            "DAGNodeName": "analyze_ID0000004",
            "HoldReason": "Error from slot1@w2: Transfer output files failure",
            "HoldReasonCode": 12,
            "QDate": 200,
            "RequestCpus": 2,
            "RequestMemory": 4096,
            "Owner": "tester",
        },
    ]


@pytest.fixture
def condor_history_ads() -> List[Dict[str, Any]]:
    """History ClassAd dicts as returned by condor_history -json."""
    return [
        {
            "ClusterId": 101,
            "ProcId": 0,
            "DAGNodeName": "preprocess_ID0000001",
            "ExitCode": 0,
            "JobStatus": 4,
            "RemoteWallClockTime": 40.0,
            "RemoteUserCpu": 38.0,
            "RequestCpus": 1,
            "RequestMemory": 2048,
            "ImageSize": 1048576,
            "DiskUsage": 524288,
            "LastRemoteHost": "slot1@worker1.example.org",
            "BytesSent": 1024,
            "BytesRecvd": 2048,
            "NumJobStarts": 1,
            "QDate": 100,
            "JobStartDate": 110,
        },
        {
            "ClusterId": 102,
            "ProcId": 0,
            "DAGNodeName": "findrange_ID0000002",
            "ExitCode": 0,
            "JobStatus": 4,
            "RemoteWallClockTime": 30.0,
            "RemoteUserCpu": 15.0,
            "RequestCpus": 1,
            "RequestMemory": 2048,
            "ImageSize": 2097152,
            "DiskUsage": 262144,
            "LastRemoteHost": "slot2@worker2.example.org",
            "BytesSent": 512,
            "BytesRecvd": 1024,
            "NumJobStarts": 2,
            "QDate": 100,
            "JobStartDate": 150,
        },
    ]


@pytest.fixture
def condor_slot_ads() -> List[Dict[str, Any]]:
    """Slot ClassAd dicts as returned by condor_status -json."""
    return [
        {
            "Name": "slot1@worker1",
            "Machine": "worker1.example.org",
            "SlotType": "Static",
            "Cpus": 4,
            "Memory": 8192,
            "Disk": 1048576,
            "Activity": "Busy",
            "State": "Claimed",
            "OpSys": "LINUX",
            "Arch": "X86_64",
            "GPUs": 0,
            "TotalLoadAvg": 3.5,
        },
        {
            "Name": "slot2@worker2",
            "Machine": "worker2.example.org",
            "SlotType": "Static",
            "Cpus": 4,
            "Memory": 8192,
            "Disk": 1048576,
            "Activity": "Idle",
            "State": "Unclaimed",
            "OpSys": "LINUX",
            "Arch": "X86_64",
            "GPUs": 0,
            "TotalLoadAvg": 0.1,
        },
    ]
