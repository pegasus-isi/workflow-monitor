"""Unit tests for :mod:`workflow_monitor.stats`.

Covers :func:`compute_workflow_stats` (job counts, timing/duration
distribution, memory aggregation, ``condor_history`` aggregation, and pool
status) plus :meth:`WorkflowStats.to_dict` / :meth:`WorkflowStats.from_dict`
serialization.

All inputs are built directly from the shared in-memory factories
(``make_snapshot`` / ``make_job``) so the SQL layer is not exercised here.
"""

from __future__ import annotations

from statistics import median

import pytest

from workflow_monitor.htcondor_poll import PoolSummary
from workflow_monitor.stats import WorkflowStats, compute_workflow_stats


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _compute_job(make_job, *, job_id, name, start, end, **kw):
    """A compute JobRecord with an explicit, positive duration window."""
    kw.setdefault("raw_state", "JOB_SUCCESS")
    return make_job(
        job_id=job_id,
        exec_job_id=name,
        type_desc="compute",
        start_time=start,
        end_time=end,
        **kw,
    )


def _infra_job(make_job, *, job_id, name, type_desc, **kw):
    return make_job(
        job_id=job_id,
        exec_job_id=name,
        type_desc=type_desc,
        raw_state="JOB_SUCCESS",
        **kw,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Job counts
# ─────────────────────────────────────────────────────────────────────────────


def test_job_counts_mixed_snapshot(make_job, make_snapshot):
    jobs = [
        # compute: two succeeded, one failed, one held
        _compute_job(make_job, job_id=1, name="c1", start=100.0, end=110.0),
        _compute_job(make_job, job_id=2, name="c2", start=100.0, end=120.0),
        _compute_job(
            make_job,
            job_id=3,
            name="c3",
            start=100.0,
            end=130.0,
            raw_state="JOB_FAILURE",
        ),
        _compute_job(
            make_job,
            job_id=4,
            name="c4",
            start=100.0,
            end=140.0,
            raw_state="JOB_HELD",
        ),
        # infra jobs
        _infra_job(make_job, job_id=5, name="create_dir", type_desc="create-dir"),
        _infra_job(make_job, job_id=6, name="stage_in", type_desc="stage-in-tx"),
    ]
    snap = make_snapshot(jobs=jobs)
    s = compute_workflow_stats(snap)

    assert s.total_jobs == 6
    assert s.compute_jobs == 4
    assert s.infra_jobs == 2
    # succeeded counts SUCCESS disp_state across ALL jobs (compute + infra)
    assert s.succeeded == 4  # c1, c2, and both infra jobs (JOB_SUCCESS)
    assert s.failed == 1
    assert s.held == 1


# ─────────────────────────────────────────────────────────────────────────────
# Timing / duration distribution
# ─────────────────────────────────────────────────────────────────────────────


def test_timing_and_duration_distribution(make_job, make_snapshot):
    # durations: 10, 20, 40  -> min 10, max 40, sum 70, mean 70/3, median 20
    jobs = [
        _compute_job(make_job, job_id=1, name="short_job", start=100.0, end=110.0),
        _compute_job(make_job, job_id=2, name="mid_job", start=100.0, end=120.0),
        _compute_job(make_job, job_id=3, name="long_job", start=100.0, end=140.0),
    ]
    snap = make_snapshot(jobs=jobs, wf_start=0.0, wf_end=100.0)
    s = compute_workflow_stats(snap)

    assert s.wall_time == 100.0  # snapshot.elapsed (wf_end - wf_start)
    assert s.total_compute_time == 70.0
    assert s.dur_min == 10.0
    assert s.dur_max == 40.0
    assert s.dur_mean == pytest.approx(70.0 / 3.0)
    assert s.dur_median == median([10.0, 20.0, 40.0])
    assert s.longest_job_name == "long_job"
    assert s.shortest_job_name == "short_job"
    # parallelism = total_compute_time / wall_time
    assert s.parallelism == pytest.approx(70.0 / 100.0)


def test_wall_time_from_poll_time_when_running(make_job, make_snapshot):
    # No wf_end -> elapsed uses poll_time.
    jobs = [_compute_job(make_job, job_id=1, name="c1", start=100.0, end=110.0)]
    snap = make_snapshot(jobs=jobs, wf_start=10.0, wf_end=None, poll_time=210.0)
    s = compute_workflow_stats(snap)
    assert s.wall_time == 200.0  # 210 - 10
    assert s.parallelism == pytest.approx(10.0 / 200.0)


def test_display_name_prefers_transformation_for_compute(make_job, make_snapshot):
    jobs = [
        _compute_job(
            make_job,
            job_id=1,
            name="analyze_ID0000004",
            start=100.0,
            end=150.0,
            transformation="pegasus::analyze",
        ),
        _compute_job(
            make_job,
            job_id=2,
            name="preprocess_ID0000001",
            start=100.0,
            end=110.0,
            transformation="pegasus::preprocess",
        ),
    ]
    snap = make_snapshot(jobs=jobs)
    s = compute_workflow_stats(snap)
    # longest dur = 50 (analyze), shortest = 10 (preprocess); names use transformation
    assert s.longest_job_name == "pegasus::analyze"
    assert s.shortest_job_name == "pegasus::preprocess"


def test_zero_and_none_durations_excluded(make_job, make_snapshot):
    jobs = [
        # dur == 0 (start == end) -> excluded
        _compute_job(make_job, job_id=1, name="zero", start=100.0, end=100.0),
        # dur None: no start_time -> duration property returns None
        make_job(
            job_id=2,
            exec_job_id="nostart",
            type_desc="compute",
            raw_state="JOB_SUCCESS",
            start_time=None,
            end_time=None,
        ),
        # one positive duration -> distribution defined
        _compute_job(make_job, job_id=3, name="real", start=100.0, end=130.0),
    ]
    snap = make_snapshot(jobs=jobs, wf_start=0.0, wf_end=200.0)
    s = compute_workflow_stats(snap)
    assert s.total_compute_time == 30.0
    assert s.dur_min == 30.0
    assert s.dur_max == 30.0
    assert s.longest_job_name == "real"
    assert s.shortest_job_name == "real"


def test_no_positive_duration_compute_jobs_leaves_dur_fields_none(
    make_job, make_snapshot
):
    # All compute jobs lack a usable duration.
    jobs = [
        make_job(
            job_id=1,
            exec_job_id="q1",
            type_desc="compute",
            raw_state="SUBMIT",
            start_time=None,
            end_time=None,
        ),
        _compute_job(make_job, job_id=2, name="zero", start=100.0, end=100.0),
    ]
    snap = make_snapshot(jobs=jobs, wf_start=0.0, wf_end=50.0)
    s = compute_workflow_stats(snap)
    assert s.wall_time == 50.0  # wall_time still set from elapsed
    assert s.total_compute_time is None
    assert s.dur_min is None
    assert s.dur_max is None
    assert s.dur_mean is None
    assert s.dur_median is None
    assert s.longest_job_name is None
    assert s.shortest_job_name is None
    assert s.parallelism is None


def test_parallelism_none_when_wall_time_zero(make_job, make_snapshot):
    jobs = [_compute_job(make_job, job_id=1, name="c1", start=100.0, end=130.0)]
    # wf_start == wf_end -> elapsed == 0 -> parallelism stays None
    snap = make_snapshot(jobs=jobs, wf_start=5.0, wf_end=5.0)
    s = compute_workflow_stats(snap)
    assert s.wall_time == 0.0
    assert s.total_compute_time == 30.0
    assert s.parallelism is None


def test_wall_time_none_when_no_wf_start(make_job, make_snapshot):
    jobs = [_compute_job(make_job, job_id=1, name="c1", start=100.0, end=130.0)]
    snap = make_snapshot(jobs=jobs, wf_start=None, wf_end=None)
    s = compute_workflow_stats(snap)
    assert s.wall_time is None
    assert s.parallelism is None
    # distribution still computed from the positive duration
    assert s.total_compute_time == 30.0


# ─────────────────────────────────────────────────────────────────────────────
# Memory (from stampede)
# ─────────────────────────────────────────────────────────────────────────────


def test_memory_aggregation_over_compute_jobs(make_job, make_snapshot):
    jobs = [
        _compute_job(
            make_job, job_id=1, name="m1", start=100.0, end=110.0, maxrss=51200
        ),
        _compute_job(
            make_job, job_id=2, name="m2", start=100.0, end=120.0, maxrss=60000
        ),
        # maxrss 0 -> excluded
        _compute_job(make_job, job_id=3, name="m3", start=100.0, end=130.0, maxrss=0),
        # maxrss None -> excluded
        _compute_job(
            make_job, job_id=4, name="m4", start=100.0, end=140.0, maxrss=None
        ),
        # infra job with big maxrss must NOT be considered
        _infra_job(
            make_job,
            job_id=5,
            name="stage",
            type_desc="stage-in-tx",
            maxrss=999999,
        ),
    ]
    snap = make_snapshot(jobs=jobs)
    s = compute_workflow_stats(snap)
    assert s.peak_maxrss_kb == 60000
    assert s.peak_maxrss_job == "m2"
    assert s.mean_maxrss_kb == pytest.approx((51200 + 60000) / 2)


def test_memory_fields_none_when_no_maxrss(make_job, make_snapshot):
    jobs = [
        _compute_job(make_job, job_id=1, name="m1", start=100.0, end=110.0, maxrss=None)
    ]
    s = compute_workflow_stats(make_snapshot(jobs=jobs))
    assert s.peak_maxrss_kb is None
    assert s.peak_maxrss_job is None
    assert s.mean_maxrss_kb is None


# ─────────────────────────────────────────────────────────────────────────────
# condor_history aggregation
# ─────────────────────────────────────────────────────────────────────────────


def test_condor_history_none_leaves_fields_none(make_snapshot):
    s = compute_workflow_stats(make_snapshot(states=["SUCCESS"]), condor_history=None)
    for fld in (
        "cpu_eff_min",
        "cpu_eff_max",
        "cpu_eff_mean",
        "mem_eff_min",
        "mem_eff_max",
        "mem_eff_mean",
        "wait_min",
        "wait_max",
        "wait_mean",
        "cpu_seconds",
        "transfer_bytes",
        "retry_count",
        "hosts",
    ):
        assert getattr(s, fld) is None, fld


def test_condor_history_empty_list_leaves_fields_none(make_snapshot):
    s = compute_workflow_stats(make_snapshot(states=["SUCCESS"]), condor_history=[])
    assert s.cpu_eff_mean is None
    assert s.hosts is None
    assert s.transfer_bytes is None


def test_condor_history_aggregation(make_snapshot, condor_history_ads):
    snap = make_snapshot(states=["SUCCESS"])
    s = compute_workflow_stats(snap, condor_history=condor_history_ads)

    # cpu_eff: rec1 = 38/(40*1) = 0.95, rec2 = 15/(30*1) = 0.5
    assert s.cpu_eff_min == pytest.approx(0.5)
    assert s.cpu_eff_max == pytest.approx(0.95)
    assert s.cpu_eff_mean == pytest.approx((0.95 + 0.5) / 2)

    # mem_eff: rec1 = 1048576/(2048*1024) = 0.5, rec2 = 2097152/(2048*1024) = 1.0
    assert s.mem_eff_min == pytest.approx(0.5)
    assert s.mem_eff_max == pytest.approx(1.0)
    assert s.mem_eff_mean == pytest.approx(0.75)

    # wait: rec1 = 110-100 = 10, rec2 = 150-100 = 50
    assert s.wait_min == pytest.approx(10.0)
    assert s.wait_max == pytest.approx(50.0)
    assert s.wait_mean == pytest.approx(30.0)

    # cpu_seconds = sum RemoteUserCpu = 38 + 15
    assert s.cpu_seconds == pytest.approx(53.0)

    # transfer_bytes = (1024+2048) + (512+1024) = 3072 + 1536
    assert s.transfer_bytes == 4608

    # retry_count = sum(NumJobStarts-1 where >1): rec1 starts=1 (0), rec2 starts=2 (1)
    assert s.retry_count == 1

    # hosts: each shortened host appears once -> sorted by count desc (ties keep order)
    assert s.hosts == ["worker1 (1)", "worker2 (1)"]


def test_condor_history_hosts_sorted_by_count_desc(make_snapshot):
    history = [
        {"LastRemoteHost": "slot1@busy.domain.org"},
        {"LastRemoteHost": "slot2@busy.domain.org"},
        {"LastRemoteHost": "slot1@busy.domain.org"},
        {"LastRemoteHost": "slot1@quiet.domain.org"},
    ]
    s = compute_workflow_stats(
        make_snapshot(states=["SUCCESS"]), condor_history=history
    )
    # busy=3, quiet=1
    assert s.hosts == ["busy (3)", "quiet (1)"]


def test_condor_history_transfer_bytes_unset_when_zero(make_snapshot):
    history = [{"BytesSent": 0, "BytesRecvd": 0, "LastRemoteHost": "slot1@h.x.org"}]
    s = compute_workflow_stats(
        make_snapshot(states=["SUCCESS"]), condor_history=history
    )
    assert s.transfer_bytes is None  # only set when >0
    assert s.hosts == ["h (1)"]


def test_condor_history_retry_count_unset_when_no_retries(make_snapshot):
    history = [{"NumJobStarts": 1}, {"NumJobStarts": 1}]
    s = compute_workflow_stats(
        make_snapshot(states=["SUCCESS"]), condor_history=history
    )
    assert s.retry_count is None


def test_condor_history_host_without_at_sign(make_snapshot):
    # LastRemoteHost without '@' -> split on '.'
    history = [{"LastRemoteHost": "barehost.domain.org"}]
    s = compute_workflow_stats(
        make_snapshot(states=["SUCCESS"]), condor_history=history
    )
    assert s.hosts == ["barehost (1)"]


# ─────────────────────────────────────────────────────────────────────────────
# pool_status
# ─────────────────────────────────────────────────────────────────────────────


def test_pool_status_basic(make_snapshot):
    pool = PoolSummary(machines=3, total_cpus=12, total_gpus=0)
    s = compute_workflow_stats(make_snapshot(states=["SUCCESS"]), pool_status=pool)
    assert s.pool_machines == 3
    assert s.pool_total_cpus == 12
    assert s.pool_total_gpus is None  # only set when total_gpus > 0


def test_pool_status_with_gpus(make_snapshot):
    pool = PoolSummary(machines=2, total_cpus=8, total_gpus=4)
    s = compute_workflow_stats(make_snapshot(states=["SUCCESS"]), pool_status=pool)
    assert s.pool_machines == 2
    assert s.pool_total_cpus == 8
    assert s.pool_total_gpus == 4


def test_pool_status_none_leaves_fields_none(make_snapshot):
    s = compute_workflow_stats(make_snapshot(states=["SUCCESS"]), pool_status=None)
    assert s.pool_machines is None
    assert s.pool_total_cpus is None
    assert s.pool_total_gpus is None


# ─────────────────────────────────────────────────────────────────────────────
# to_dict / from_dict
# ─────────────────────────────────────────────────────────────────────────────


def test_to_dict_omits_none_values():
    s = WorkflowStats(total_jobs=5, compute_jobs=3, wall_time=100.0)
    d = s.to_dict()
    assert d["total_jobs"] == 5
    assert d["compute_jobs"] == 3
    assert d["wall_time"] == 100.0
    # None-valued fields are omitted
    assert "parallelism" not in d
    assert "hosts" not in d
    assert "pool_total_gpus" not in d
    # zero-but-not-None counts are kept (succeeded defaults to 0, not None)
    assert d["succeeded"] == 0


def test_from_dict_ignores_unknown_keys():
    d = {"total_jobs": 7, "compute_jobs": 4, "bogus_field": 123, "another": "x"}
    s = WorkflowStats.from_dict(d)
    assert s.total_jobs == 7
    assert s.compute_jobs == 4
    assert not hasattr(s, "bogus_field")


def test_to_dict_from_dict_roundtrip(make_job, make_snapshot, condor_history_ads):
    # Build a richly populated instance through the real compute path.
    jobs = [
        _compute_job(
            make_job,
            job_id=1,
            name="a",
            start=100.0,
            end=140.0,
            transformation="pegasus::analyze",
            maxrss=60000,
        ),
        _compute_job(
            make_job,
            job_id=2,
            name="b",
            start=100.0,
            end=110.0,
            transformation="pegasus::preprocess",
            maxrss=51200,
        ),
        _infra_job(make_job, job_id=3, name="dir", type_desc="create-dir"),
    ]
    snap = make_snapshot(jobs=jobs, wf_start=0.0, wf_end=200.0)
    pool = PoolSummary(machines=2, total_cpus=8, total_gpus=2)
    original = compute_workflow_stats(
        snap,
        condor_history=condor_history_ads,
        pool_status=pool,
    )

    # Sanity: a representative subset of fields is populated (not all None).
    assert original.hosts is not None
    assert original.pool_total_gpus == 2

    restored = WorkflowStats.from_dict(original.to_dict())
    assert restored == original


def test_from_dict_empty_dict_gives_defaults():
    s = WorkflowStats.from_dict({})
    assert s == WorkflowStats()
