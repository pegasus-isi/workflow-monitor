"""Smoke test that the shared fixtures produce data the real code can read.

If this file passes, the stampede.db builder and snapshot factories are sound
and the per-module test files can rely on them.
"""

from workflow_monitor.db import StampedeDB


def test_diamond_db_roundtrip(diamond_db):
    with StampedeDB(diamond_db, wf_uuid="test-wf-uuid") as db:
        snap = db.snapshot()
    assert snap.total_jobs() == 5
    assert snap.is_complete
    assert snap.succeeded
    assert snap.done_count() == 5
    assert len(snap.compute_jobs()) == 4
    assert len(snap.infra_jobs()) == 1
    # preprocess ran first; verify metadata enrichment came through
    pre = next(j for j in snap.jobs if j.exec_job_id == "preprocess_ID0000001")
    assert pre.transformation == "pegasus::preprocess"
    assert pre.maxrss == 51200
    # end_time is MAX over JOB_TERMINATED/JOB_SUCCESS/JOB_FAILURE => 151.0
    assert pre.duration == 41.0  # 151 - 110
    assert pre.display_name == "pegasus::preprocess"


def test_db_factory_states(db_factory, state_seqs):
    path = db_factory(
        jobs=[
            {"job_id": 1, "exec_job_id": "a", "states": state_seqs["RUNNING"]},
            {"job_id": 2, "exec_job_id": "b", "states": state_seqs["HELD"]},
            {
                "job_id": 3,
                "exec_job_id": "c",
                "states": state_seqs["FAILED"],
                "exitcode": 256,
            },
        ],
        wf_states=[{"state": "WORKFLOW_STARTED", "timestamp": 49.0}],
    )
    with StampedeDB(path) as db:  # no wf_uuid -> IS NULL branch
        snap = db.snapshot()
    counts = snap.job_counts()
    assert counts.get("RUNNING") == 1
    assert counts.get("HELD") == 1
    assert counts.get("FAILED") == 1
    assert snap.is_running


def test_make_snapshot_factory(make_snapshot):
    snap = make_snapshot(states=["SUCCESS", "SUCCESS", "RUNNING", "HELD"])
    assert snap.total_jobs() == 4
    assert snap.done_count() == 2
    assert snap.running_count() == 1
    assert snap.held_count() == 1


def test_write_braindump_loads(write_braindump, tmp_path):
    from workflow_monitor.braindump import load_braindump

    run_dir = tmp_path / "run0001"
    write_braindump(run_dir, submit_dir=str(run_dir))
    info = load_braindump(run_dir)
    assert info.wf_uuid == "test-wf-uuid"
    assert info.dax_label == "diamond"
    assert info.submit_dir == run_dir
