"""Unit tests for :mod:`workflow_monitor.why_idle`.

The bulk of the value is in the pure :func:`_analyze` function, which is
exercised branch-by-branch below. The subprocess query helpers
(``_query_userprio``, ``_query_negotiator``, ``_get_idle_job_details``) are
tested by monkeypatching ``workflow_monitor.why_idle.subprocess.run`` to return
fake completed-process objects or to raise the documented exceptions. No real
HTCondor is ever invoked.
"""

from __future__ import annotations

import json
import subprocess
import types
from typing import Any, Dict, List


from workflow_monitor import why_idle
from workflow_monitor.htcondor_poll import PoolSummary


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


class FakeCompleted:
    """Stand-in for subprocess.CompletedProcess."""

    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = ""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _fake_run(*, returncode: int = 0, stdout: str = "", stderr: str = ""):
    """Build a ``subprocess.run`` replacement that records its args."""
    calls: List[List[str]] = []

    def runner(cmd, *args, **kwargs):
        calls.append(list(cmd))
        return FakeCompleted(returncode=returncode, stdout=stdout, stderr=stderr)

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


def _raising_run(exc: BaseException):
    def runner(cmd, *args, **kwargs):
        raise exc

    return runner


def _make_pool(**kwargs) -> PoolSummary:
    """PoolSummary with explicit defaults the analyzer reads."""
    defaults: Dict[str, Any] = dict(
        total_slots=4,
        idle_slots=2,
        total_cpus=8,
        idle_cpus=4,
        total_memory_mb=16384,
        idle_memory_mb=8192,
        total_gpus=0,
        idle_gpus=0,
        machines=2,
    )
    defaults.update(kwargs)
    return PoolSummary(**defaults)


def _prio_entry(name: str, eff: float, used: float = 0.0, **extra) -> Dict[str, Any]:
    entry = {"Name": name, "EffectivePriority": eff, "ResourcesUsed": used}
    entry.update(extra)
    return entry


# ─────────────────────────────────────────────────────────────────────────────
# _analyze — early-return branches
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_no_idle_jobs_returns_early(make_snapshot):
    snap = make_snapshot(states=["SUCCESS", "RUNNING", "SUCCESS"])
    diag = why_idle._analyze(
        snap, [], _make_pool(), [_prio_entry("alice@d", 5.0)], None
    )

    assert diag.idle_jobs == []
    assert any("No idle or queued jobs found" in f for f in diag.findings)
    # Early return: no pool/priority analysis happened.
    assert diag.pool_available is False
    assert diag.user_priority is None
    assert diag.suggestions == []


def test_analyze_only_unsubmitted_returns_early(make_snapshot):
    snap = make_snapshot(states=["UNSUBMITTED", "UNSUBMITTED", "SUCCESS"])
    diag = why_idle._analyze(
        snap, [], _make_pool(), [_prio_entry("alice@d", 5.0)], None
    )

    # idle_jobs captured the two UNSUBMITTED jobs
    assert len(diag.idle_jobs) == 2
    assert all(j["state"] == "UNSUBMITTED" for j in diag.idle_jobs)

    assert any("2 job(s) are UNSUBMITTED" in f for f in diag.findings)
    assert any("All waiting jobs are UNSUBMITTED" in f for f in diag.findings)
    assert any("DAGMan" in s for s in diag.suggestions)

    # Early return before pool/priority analysis.
    assert diag.pool_available is False
    assert diag.user_priority is None
    assert diag.negotiation_cycle_seconds is None


def test_analyze_idle_jobs_capture_fields(make_snapshot, make_job):
    jobs = [
        make_job(
            job_id=1,
            exec_job_id="findrange_ID0000002",
            type_desc="compute",
            raw_state="SUBMIT",
        ),
        make_job(
            job_id=2,
            exec_job_id="analyze_ID0000004",
            type_desc="compute",
            raw_state=None,
        ),  # UNSUBMITTED
        make_job(
            job_id=3,
            exec_job_id="done_ID0000009",
            type_desc="compute",
            raw_state="JOB_SUCCESS",
        ),  # excluded
    ]
    snap = make_snapshot(jobs=jobs)
    diag = why_idle._analyze(snap, [], None, [], None)

    assert len(diag.idle_jobs) == 2
    queued = [j for j in diag.idle_jobs if j["state"] == "QUEUED"][0]
    assert queued["name"] == "findrange_ID0000002"
    assert queued["exec_id"] == "findrange_ID0000002"
    assert queued["type"] == "compute"
    unsub = [j for j in diag.idle_jobs if j["state"] == "UNSUBMITTED"][0]
    assert unsub["exec_id"] == "analyze_ID0000004"


# ─────────────────────────────────────────────────────────────────────────────
# _analyze — queued + mixed unsubmitted findings
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_queued_proceeds_past_early_return(make_snapshot):
    snap = make_snapshot(states=["QUEUED", "QUEUED"])
    diag = why_idle._analyze(snap, [], None, [], None)

    assert any("2 job(s) are QUEUED in HTCondor" in f for f in diag.findings)
    # No "all unsubmitted" early-return finding.
    assert not any("All waiting jobs are UNSUBMITTED" in f for f in diag.findings)


def test_analyze_mixed_queued_and_unsubmitted(make_snapshot):
    snap = make_snapshot(states=["QUEUED", "UNSUBMITTED", "UNSUBMITTED"])
    diag = why_idle._analyze(snap, [], None, [], None)

    assert any("2 job(s) are UNSUBMITTED" in f for f in diag.findings)
    assert any("1 job(s) are QUEUED in HTCondor" in f for f in diag.findings)


# ─────────────────────────────────────────────────────────────────────────────
# _analyze — pool capacity branch
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_pool_fields_populated(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(
        total_cpus=8,
        idle_cpus=4,
        total_memory_mb=16384,
        idle_memory_mb=8192,
        total_gpus=2,
        idle_gpus=1,
        machines=3,
        total_slots=6,
        idle_slots=2,
    )
    diag = why_idle._analyze(snap, [], pool, [], None)

    assert diag.pool_available is True
    assert diag.pool_total_cpus == 8
    assert diag.pool_idle_cpus == 4
    assert diag.pool_total_memory_mb == 16384
    assert diag.pool_idle_memory_mb == 8192
    assert diag.pool_total_gpus == 2
    assert diag.pool_idle_gpus == 1
    assert diag.pool_machines == 3
    assert diag.pool_total_slots == 6
    assert diag.pool_idle_slots == 2


def test_analyze_pool_fully_occupied(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(total_cpus=8, idle_cpus=0, machines=2)
    diag = why_idle._analyze(snap, [], pool, [], None)

    assert any("Pool is fully occupied" in f for f in diag.findings)
    assert any("add more resources" in s for s in diag.suggestions)
    # The "has idle capacity" finding must NOT appear.
    assert not any("has idle capacity" in f for f in diag.findings)


def test_analyze_pool_has_idle_capacity(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(total_cpus=8, idle_cpus=4, idle_memory_mb=8192)
    diag = why_idle._analyze(snap, [], pool, [], None)

    assert any("Pool has idle capacity" in f for f in diag.findings)
    assert not any("fully occupied" in f for f in diag.findings)


def test_analyze_pool_zero_cpus_no_capacity_findings(make_snapshot):
    """idle_cpus == 0 but total_cpus == 0 -> neither pool capacity finding."""
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(total_cpus=0, idle_cpus=0, machines=0)
    diag = why_idle._analyze(snap, [], pool, [], None)

    assert diag.pool_available is True
    assert not any("fully occupied" in f for f in diag.findings)
    assert not any("has idle capacity" in f for f in diag.findings)


def test_analyze_pool_none(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    diag = why_idle._analyze(snap, [], None, [], None)

    assert diag.pool_available is False
    assert any("Could not query pool status" in f for f in diag.findings)


# ─────────────────────────────────────────────────────────────────────────────
# _analyze — requirement mismatch branch
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_cpu_requirement_mismatch(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(total_cpus=8, idle_cpus=1, idle_memory_mb=99999, idle_gpus=0)
    condor_jobs = [
        {
            "DAGNodeName": "bignode",
            "RequestCpus": 4,
            "RequestMemory": 1024,
            "RequestGpus": 0,
        }
    ]
    diag = why_idle._analyze(snap, condor_jobs, pool, [], None)

    assert len(diag.requirement_mismatches) == 1
    assert "bignode: needs 4 CPUs but only 1 idle" in diag.requirement_mismatches[0]
    assert any("exceed available pool capacity" in f for f in diag.findings)
    assert any("Reduce resource requests" in s for s in diag.suggestions)


def test_analyze_memory_and_gpu_mismatch(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(total_cpus=8, idle_cpus=8, idle_memory_mb=512, idle_gpus=0)
    condor_jobs = [
        {
            "DAGNodeName": "gpunode",
            "RequestCpus": 1,
            "RequestMemory": 4096,
            "RequestGpus": 2,
        }
    ]
    diag = why_idle._analyze(snap, condor_jobs, pool, [], None)

    assert len(diag.requirement_mismatches) == 1
    m = diag.requirement_mismatches[0]
    assert "gpunode:" in m
    assert "needs 4096MB RAM but only 512MB idle" in m
    assert "needs 2 GPU(s) but only 0 idle" in m


def test_analyze_mismatch_uses_cluster_fallback_name(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(idle_cpus=1, idle_memory_mb=99999)
    condor_jobs = [{"ClusterId": 42, "RequestCpus": 4}]  # no DAGNodeName
    diag = why_idle._analyze(snap, condor_jobs, pool, [], None)

    assert diag.requirement_mismatches
    assert diag.requirement_mismatches[0].startswith("cluster 42:")


def test_analyze_no_mismatch_when_within_capacity(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(idle_cpus=8, idle_memory_mb=8192, idle_gpus=4)
    condor_jobs = [
        {"DAGNodeName": "ok", "RequestCpus": 2, "RequestMemory": 1024, "RequestGpus": 1}
    ]
    diag = why_idle._analyze(snap, condor_jobs, pool, [], None)

    assert diag.requirement_mismatches == []
    assert not any("exceed available pool capacity" in f for f in diag.findings)


def test_analyze_request_defaults_used(make_snapshot):
    """Missing RequestCpus defaults to 1; RequestMemory/Gpus default to 0."""
    snap = make_snapshot(states=["QUEUED"])
    # idle_cpus=0: default req of 1 CPU exceeds 0 idle -> mismatch.
    pool = _make_pool(total_cpus=4, idle_cpus=0, idle_memory_mb=8192)
    condor_jobs = [{"DAGNodeName": "defaults"}]
    diag = why_idle._analyze(snap, condor_jobs, pool, [], None)

    assert diag.requirement_mismatches
    assert "needs 1 CPUs but only 0 idle" in diag.requirement_mismatches[0]


def test_analyze_mismatch_skipped_when_pool_none(make_snapshot):
    """No pool -> requirement-mismatch loop never runs."""
    snap = make_snapshot(states=["QUEUED"])
    condor_jobs = [{"DAGNodeName": "x", "RequestCpus": 999}]
    diag = why_idle._analyze(snap, condor_jobs, None, [], None)

    assert diag.requirement_mismatches == []


# ─────────────────────────────────────────────────────────────────────────────
# _analyze — user priority branch
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_user_priority_best(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    user_prio = [
        _prio_entry("alice@dom", 2.0, used=1.5),
        _prio_entry("bob@dom", 9.0, used=3.0),
    ]
    diag = why_idle._analyze(snap, [], None, user_prio, None, current_user="alice")

    assert diag.user_priority is not None
    assert diag.user_priority["name"] == "alice@dom"
    assert diag.user_priority["effective_priority"] == 2.0
    assert diag.user_priority["resources_used"] == 1.5
    # bob collected as another user
    assert len(diag.other_users) == 1
    assert diag.other_users[0]["name"] == "bob@dom"
    assert any("best (lowest) priority" in f for f in diag.findings)
    assert not any("better (lower) priority" in f for f in diag.findings)


def test_analyze_user_priority_others_better(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    user_prio = [
        _prio_entry("alice@dom", 9.0),
        _prio_entry("bob@dom", 2.0),
        _prio_entry("carol@dom", 3.0),
    ]
    diag = why_idle._analyze(snap, [], None, user_prio, None, current_user="alice")

    assert diag.user_priority["effective_priority"] == 9.0
    assert len(diag.other_users) == 2
    assert any("2 user(s) have better (lower) priority" in f for f in diag.findings)
    assert any("Fair-share priority improves" in s for s in diag.suggestions)


def test_analyze_user_priority_no_entry_for_user(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    user_prio = [_prio_entry("bob@dom", 2.0)]
    diag = why_idle._analyze(snap, [], None, user_prio, None, current_user="alice")

    assert diag.user_priority is None
    assert any("No priority entry found for user 'alice'" in f for f in diag.findings)


def test_analyze_user_priority_empty_list(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    diag = why_idle._analyze(snap, [], None, [], None, current_user="alice")

    assert any("Could not query user priorities" in f for f in diag.findings)
    assert diag.user_priority is None


def test_analyze_user_priority_current_user_from_env(make_snapshot, monkeypatch):
    """current_user=None -> falls back to $USER."""
    monkeypatch.setenv("USER", "alice")
    snap = make_snapshot(states=["QUEUED"])
    user_prio = [_prio_entry("alice@dom", 4.0)]
    diag = why_idle._analyze(snap, [], None, user_prio, None, current_user=None)

    assert diag.user_priority is not None
    assert diag.user_priority["name"] == "alice@dom"


def test_analyze_user_priority_prefers_fallback_keys(make_snapshot):
    """EffectivePriority absent -> Priority; ResourcesUsed absent -> Weighted."""
    snap = make_snapshot(states=["QUEUED"])
    user_prio = [
        {
            "Name": "alice@dom",
            "Priority": 6.0,
            "PriorityFactor": 1.0,
            "WeightedResourcesUsed": 7.0,
        },
    ]
    diag = why_idle._analyze(snap, [], None, user_prio, None, current_user="alice")

    assert diag.user_priority["effective_priority"] == 6.0
    assert diag.user_priority["real_priority"] == 1.0
    assert diag.user_priority["resources_used"] == 7.0


def test_analyze_other_user_missing_eff_prio_excluded(make_snapshot):
    """An other-user with no EffectivePriority/Priority is skipped."""
    snap = make_snapshot(states=["QUEUED"])
    user_prio = [
        _prio_entry("alice@dom", 5.0),
        {"Name": "ghost@dom"},  # no priority at all -> other_eff is None -> skipped
    ]
    diag = why_idle._analyze(snap, [], None, user_prio, None, current_user="alice")

    assert diag.other_users == []
    # Comparison findings only fire when other_users is non-empty (code line
    # `if eff_prio is not None and diag.other_users`). With no comparable
    # others, neither the "best (lowest)" nor "better (lower)" finding appears,
    # though user_priority is still populated.
    assert diag.user_priority is not None
    assert not any("best (lowest) priority" in f for f in diag.findings)
    assert not any("better (lower) priority" in f for f in diag.findings)


# ─────────────────────────────────────────────────────────────────────────────
# _analyze — negotiator branch
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_negotiator_zero_matches(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    neg = {"LastNegotiationCycleDuration0": 12.0, "LastNegotiationCycleMatches0": 0}
    diag = why_idle._analyze(snap, [], None, [], neg)

    assert diag.negotiation_cycle_seconds == 12.0
    assert diag.negotiation_matches == 0
    assert any("Last negotiation cycle took 12.0s" in f for f in diag.findings)
    assert any("produced 0 matches" in f for f in diag.findings)
    assert any("Zero matches with idle jobs" in s for s in diag.suggestions)


def test_analyze_negotiator_with_matches(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    neg = {"LastNegotiationCycleDuration0": 5.0, "LastNegotiationCycleMatches0": 7}
    diag = why_idle._analyze(snap, [], None, [], neg)

    assert diag.negotiation_matches == 7
    assert any("matched 7 job(s) to slots" in f for f in diag.findings)
    assert not any("produced 0 matches" in f for f in diag.findings)


def test_analyze_negotiator_overloaded_cycle(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    neg = {"LastNegotiationCycleDuration0": 120.5, "LastNegotiationCycleMatches0": 3}
    diag = why_idle._analyze(snap, [], None, [], neg)

    assert diag.negotiation_cycle_seconds == 120.5
    assert any("Negotiation cycles over 60s" in s for s in diag.suggestions)


def test_analyze_negotiator_duration_exactly_60_not_overloaded(make_snapshot):
    """Boundary: duration == 60 is NOT > 60 -> no overload suggestion."""
    snap = make_snapshot(states=["QUEUED"])
    neg = {"LastNegotiationCycleDuration0": 60.0, "LastNegotiationCycleMatches0": 5}
    diag = why_idle._analyze(snap, [], None, [], neg)

    assert not any("over 60s" in s for s in diag.suggestions)


def test_analyze_negotiator_missing_fields(make_snapshot):
    """negotiator present but lacking the cycle-0 attrs -> no negotiator findings."""
    snap = make_snapshot(states=["QUEUED"])
    neg = {"SomethingElse": 1}
    diag = why_idle._analyze(snap, [], None, [], neg)

    assert diag.negotiation_cycle_seconds is None
    assert diag.negotiation_matches is None
    assert not any("negotiation cycle" in f.lower() for f in diag.findings)


# ─────────────────────────────────────────────────────────────────────────────
# _analyze — fallback suggestion
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_fallback_suggestion_when_none(make_snapshot):
    """Queued job, no pool, no prio, no negotiator -> generic fallback suggestion."""
    snap = make_snapshot(states=["QUEUED"])
    diag = why_idle._analyze(snap, [], None, [], None)

    assert len(diag.suggestions) == 1
    assert "waiting for the next negotiation cycle" in diag.suggestions[0]


def test_analyze_no_fallback_when_other_suggestions_exist(make_snapshot):
    snap = make_snapshot(states=["QUEUED"])
    pool = _make_pool(total_cpus=8, idle_cpus=0)  # produces a suggestion
    diag = why_idle._analyze(snap, [], pool, [], None)

    assert not any(
        "waiting for the next negotiation cycle" in s for s in diag.suggestions
    )


def test_analyze_full_integration(make_snapshot):
    """All branches together: queued+unsubmitted, full pool, mismatch, prio, neg."""
    snap = make_snapshot(states=["QUEUED", "UNSUBMITTED"])
    pool = _make_pool(total_cpus=8, idle_cpus=0, idle_memory_mb=0, machines=2)
    condor_jobs = [{"DAGNodeName": "n1", "RequestCpus": 4, "RequestMemory": 2048}]
    user_prio = [_prio_entry("alice@dom", 9.0), _prio_entry("bob@dom", 1.0)]
    neg = {"LastNegotiationCycleDuration0": 90.0, "LastNegotiationCycleMatches0": 0}
    diag = why_idle._analyze(
        snap, condor_jobs, pool, user_prio, neg, current_user="alice"
    )

    assert any("QUEUED" in f for f in diag.findings)
    assert any("UNSUBMITTED" in f for f in diag.findings)
    assert any("fully occupied" in f for f in diag.findings)
    assert diag.requirement_mismatches
    assert any("better (lower) priority" in f for f in diag.findings)
    assert diag.negotiation_matches == 0
    # Multiple genuine suggestions -> fallback not appended.
    assert not any(
        "waiting for the next negotiation cycle" in s for s in diag.suggestions
    )


# ─────────────────────────────────────────────────────────────────────────────
# _query_userprio
# ─────────────────────────────────────────────────────────────────────────────


_USERPRIO_LONG = """\
Name = "alice@example.com"
Priority = 10.5
EffectivePriority = 5.25
ResourcesUsed = 3

Name = "bob@example.com"
Priority = 20.0
EffectivePriority = 18.75
ResourcesUsed = 7
"""


def test_query_userprio_parses_blocks(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout=_USERPRIO_LONG)
    )
    users = why_idle._query_userprio()

    assert len(users) == 2
    alice, bob = users
    assert alice["Name"] == "alice@example.com"  # quotes stripped, stays str
    assert alice["Priority"] == 10.5
    assert isinstance(alice["Priority"], float)
    assert alice["EffectivePriority"] == 5.25
    assert alice["ResourcesUsed"] == 3
    assert isinstance(alice["ResourcesUsed"], int)
    assert bob["Name"] == "bob@example.com"
    assert bob["EffectivePriority"] == 18.75


def test_query_userprio_trailing_block_without_blank_line(monkeypatch):
    """A final block not terminated by a blank line is still captured."""
    stdout = 'Name = "solo@d"\nEffectivePriority = 1.0'
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout=stdout)
    )
    users = why_idle._query_userprio()

    assert len(users) == 1
    assert users[0]["Name"] == "solo@d"
    assert users[0]["EffectivePriority"] == 1.0


def test_query_userprio_nonzero_returncode(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=1, stdout="ignored")
    )
    assert why_idle._query_userprio() == []


def test_query_userprio_empty_stdout(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout="   \n  ")
    )
    assert why_idle._query_userprio() == []


def test_query_userprio_file_not_found(monkeypatch):
    monkeypatch.setattr(why_idle.subprocess, "run", _raising_run(FileNotFoundError()))
    assert why_idle._query_userprio() == []


def test_query_userprio_timeout(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess,
        "run",
        _raising_run(subprocess.TimeoutExpired("condor_userprio", 10)),
    )
    assert why_idle._query_userprio() == []


def test_query_userprio_collector_host_in_cmd(monkeypatch):
    runner = _fake_run(returncode=0, stdout=_USERPRIO_LONG)
    monkeypatch.setattr(why_idle.subprocess, "run", runner)
    why_idle._query_userprio(collector_host="collector.example.org")

    cmd = runner.calls[0]  # type: ignore[attr-defined]
    assert "-pool" in cmd
    assert "collector.example.org" in cmd
    assert cmd[0] == "condor_userprio"


# ─────────────────────────────────────────────────────────────────────────────
# _query_negotiator
# ─────────────────────────────────────────────────────────────────────────────


def test_query_negotiator_returns_first_element(monkeypatch):
    payload = [
        {"LastNegotiationCycleDuration0": 5.0, "LastNegotiationCycleMatches0": 3},
        {"LastNegotiationCycleDuration0": 9.0},
    ]
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout=json.dumps(payload))
    )
    out = why_idle._query_negotiator()

    assert out == payload[0]


def test_query_negotiator_empty_list(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout="[]")
    )
    assert why_idle._query_negotiator() is None


def test_query_negotiator_non_list_json(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout='{"a": 1}')
    )
    assert why_idle._query_negotiator() is None


def test_query_negotiator_invalid_json(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout="not json{")
    )
    assert why_idle._query_negotiator() is None


def test_query_negotiator_nonzero_returncode(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=2, stdout="[]")
    )
    assert why_idle._query_negotiator() is None


def test_query_negotiator_empty_stdout(monkeypatch):
    monkeypatch.setattr(why_idle.subprocess, "run", _fake_run(returncode=0, stdout=""))
    assert why_idle._query_negotiator() is None


def test_query_negotiator_file_not_found(monkeypatch):
    monkeypatch.setattr(why_idle.subprocess, "run", _raising_run(FileNotFoundError()))
    assert why_idle._query_negotiator() is None


def test_query_negotiator_timeout(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess,
        "run",
        _raising_run(subprocess.TimeoutExpired("condor_status", 10)),
    )
    assert why_idle._query_negotiator() is None


def test_query_negotiator_collector_host_in_cmd(monkeypatch):
    runner = _fake_run(returncode=0, stdout="[]")
    monkeypatch.setattr(why_idle.subprocess, "run", runner)
    why_idle._query_negotiator(collector_host="c.example.org")

    cmd = runner.calls[0]  # type: ignore[attr-defined]
    assert "-negotiator" in cmd
    assert "-pool" in cmd
    assert "c.example.org" in cmd


# ─────────────────────────────────────────────────────────────────────────────
# _get_idle_job_details
# ─────────────────────────────────────────────────────────────────────────────


def test_get_idle_job_details_basic_constraint(monkeypatch):
    payload = [{"ClusterId": 1, "JobStatus": 1, "DAGNodeName": "n"}]
    runner = _fake_run(returncode=0, stdout=json.dumps(payload))
    monkeypatch.setattr(why_idle.subprocess, "run", runner)
    out = why_idle._get_idle_job_details()

    assert out == payload
    cmd = runner.calls[0]  # type: ignore[attr-defined]
    assert cmd[0] == "condor_q"
    # The constraint is the token following "-constraint".
    idx = cmd.index("-constraint")
    assert cmd[idx + 1] == "JobStatus == 1"


def test_get_idle_job_details_ands_user_constraint(monkeypatch):
    runner = _fake_run(returncode=0, stdout="[]")
    monkeypatch.setattr(why_idle.subprocess, "run", runner)
    why_idle._get_idle_job_details(constraint='Owner == "alice"')

    cmd = runner.calls[0]  # type: ignore[attr-defined]
    idx = cmd.index("-constraint")
    assert cmd[idx + 1] == '(Owner == "alice") && JobStatus == 1'


def test_get_idle_job_details_schedd_name_in_cmd(monkeypatch):
    runner = _fake_run(returncode=0, stdout="[]")
    monkeypatch.setattr(why_idle.subprocess, "run", runner)
    why_idle._get_idle_job_details(schedd_name="schedd.example.org")

    cmd = runner.calls[0]  # type: ignore[attr-defined]
    assert "-name" in cmd
    assert "schedd.example.org" in cmd


def test_get_idle_job_details_non_list_json(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout='{"a": 1}')
    )
    assert why_idle._get_idle_job_details() == []


def test_get_idle_job_details_invalid_json(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=0, stdout="garbage{")
    )
    assert why_idle._get_idle_job_details() == []


def test_get_idle_job_details_nonzero_returncode(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess, "run", _fake_run(returncode=1, stdout="[]")
    )
    assert why_idle._get_idle_job_details() == []


def test_get_idle_job_details_empty_stdout(monkeypatch):
    monkeypatch.setattr(why_idle.subprocess, "run", _fake_run(returncode=0, stdout=""))
    assert why_idle._get_idle_job_details() == []


def test_get_idle_job_details_file_not_found(monkeypatch):
    monkeypatch.setattr(why_idle.subprocess, "run", _raising_run(FileNotFoundError()))
    assert why_idle._get_idle_job_details() == []


def test_get_idle_job_details_timeout(monkeypatch):
    monkeypatch.setattr(
        why_idle.subprocess,
        "run",
        _raising_run(subprocess.TimeoutExpired("condor_q", 10)),
    )
    assert why_idle._get_idle_job_details() == []


# ─────────────────────────────────────────────────────────────────────────────
# run_why_idle — public entry point (light coverage)
# ─────────────────────────────────────────────────────────────────────────────


def test_run_why_idle_returns_zero(monkeypatch, make_snapshot):
    snap = make_snapshot(states=["QUEUED", "UNSUBMITTED"])

    # Stub all external queries. query_slots is imported inside run_why_idle
    # from .htcondor_poll, so patch it there.
    import workflow_monitor.htcondor_poll as hp

    monkeypatch.setattr(hp, "query_slots", lambda **kw: _make_pool())
    monkeypatch.setattr(
        why_idle,
        "_get_idle_job_details",
        lambda **kw: [{"DAGNodeName": "n", "RequestCpus": 1}],
    )
    monkeypatch.setattr(
        why_idle, "_query_userprio", lambda **kw: [_prio_entry("tester@d", 5.0)]
    )
    monkeypatch.setattr(
        why_idle,
        "_query_negotiator",
        lambda **kw: {
            "LastNegotiationCycleDuration0": 5.0,
            "LastNegotiationCycleMatches0": 2,
        },
    )

    info = types.SimpleNamespace(user="tester", dax_label="diamond")
    rc = why_idle.run_why_idle(info, snap)
    assert rc == 0


def test_run_why_idle_with_braindump_info(
    monkeypatch, make_snapshot, write_braindump, tmp_path
):
    """Exercise run_why_idle with a real WorkflowInfo from a braindump."""
    from workflow_monitor.braindump import load_braindump

    bd = write_braindump(tmp_path / "run0001", user="tester", dax_label="diamond")
    info = load_braindump(bd)

    snap = make_snapshot(states=["SUCCESS", "RUNNING"])  # no idle -> early return

    import workflow_monitor.htcondor_poll as hp

    monkeypatch.setattr(hp, "query_slots", lambda **kw: None)
    monkeypatch.setattr(why_idle, "_get_idle_job_details", lambda **kw: [])
    monkeypatch.setattr(why_idle, "_query_userprio", lambda **kw: [])
    monkeypatch.setattr(why_idle, "_query_negotiator", lambda **kw: None)

    rc = why_idle.run_why_idle(info, snap)
    assert rc == 0
