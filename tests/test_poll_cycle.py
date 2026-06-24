"""Tests for the per-cycle HTCondor polling pipeline.

Covers two newly extracted pieces of the monitoring loop:

* :class:`workflow_monitor.htcondor_poll.CondorPoller` — the back-off-gated,
  throttled per-cycle poller.  The three underlying query functions
  (``query_queue``/``query_history``/``query_slots``) are monkeypatched *on the
  ``htcondor_poll`` module* (where ``CondorPoller`` looks them up), and every
  poll is driven with an explicit ``now`` so the cadence is deterministic and no
  real condor/network is touched.
* :func:`workflow_monitor.server._run_poll_cycle` — one server iteration that
  snapshots the DB, polls condor, records the event, and ticks diagnostics.

The :class:`~workflow_monitor.htcondor_poll.CondorBackoff` adds nondeterministic
jitter to its retry interval, so timestamps that must straddle a retry boundary
are derived from ``poller.backoff.next_retry_in(now)`` / ``due(now)`` rather than
being hard-coded.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


from workflow_monitor import htcondor_poll
from workflow_monitor.htcondor_poll import CondorBackoff, CondorPoller, PoolSummary


# ─────────────────────────────────────────────────────────────────────────────
# Helpers / local fakes
# ─────────────────────────────────────────────────────────────────────────────


class CallRecorder:
    """A callable that records its positional/keyword args per invocation."""

    def __init__(self, result=None, raises: Optional[Exception] = None):
        self.calls: List[tuple] = []
        self._result = result
        self._raises = raises

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self._raises is not None:
            raise self._raises
        return self._result

    @property
    def count(self) -> int:
        return len(self.calls)


def _patch_queue(monkeypatch, fn):
    monkeypatch.setattr(htcondor_poll, "query_queue", fn)


def _patch_history(monkeypatch, fn):
    monkeypatch.setattr(htcondor_poll, "query_history", fn)


def _patch_slots(monkeypatch, fn):
    monkeypatch.setattr(htcondor_poll, "query_slots", fn)


def _open_gate_time(poller: CondorPoller, now: float) -> float:
    """Return a ``now`` just past the back-off retry boundary (gate open)."""
    return now + poller.backoff.next_retry_in(now) + 0.001


# ─────────────────────────────────────────────────────────────────────────────
# Subject 1: CondorPoller.poll_condor — success
# ─────────────────────────────────────────────────────────────────────────────


def test_poll_condor_success_first_call_queries_and_caches(monkeypatch):
    first = [{"ClusterId": 1}]
    qq = CallRecorder(result=first)
    _patch_queue(monkeypatch, qq)

    poller = CondorPoller(poll_interval=5.0)
    # fail_streak == 0 -> due is True even at now=0
    assert poller.backoff.due(0.0) is True

    out = poller.poll_condor(now=0.0)

    assert out == first
    assert qq.count == 1
    # constraint defaults to None, raise_on_error forced True
    _, kwargs = qq.calls[0]
    assert kwargs["constraint"] is None
    assert kwargs["raise_on_error"] is True


def test_poll_condor_second_success_returns_new_result(monkeypatch):
    results = [[{"ClusterId": 1}], [{"ClusterId": 2}]]
    seq = iter(results)
    calls: List[tuple] = []

    def query(*args, **kwargs):
        calls.append((args, kwargs))
        return next(seq)

    _patch_queue(monkeypatch, query)

    poller = CondorPoller(poll_interval=5.0)
    out1 = poller.poll_condor(now=0.0)
    out2 = poller.poll_condor(now=1.0)

    assert out1 == [{"ClusterId": 1}]
    assert out2 == [{"ClusterId": 2}]
    assert len(calls) == 2


def test_poll_condor_forwards_constraint_and_condor_kwargs(monkeypatch):
    qq = CallRecorder(result=[])
    _patch_queue(monkeypatch, qq)

    ck = {"schedd_name": "sched1", "collector_host": "c:9618"}
    poller = CondorPoller(
        poll_interval=5.0, condor_constraint='Owner=="me"', condor_kwargs=ck
    )
    poller.poll_condor(now=0.0)

    _, kwargs = qq.calls[0]
    assert kwargs["constraint"] == 'Owner=="me"'
    assert kwargs["raise_on_error"] is True
    assert kwargs["schedd_name"] == "sched1"
    assert kwargs["collector_host"] == "c:9618"


# ─────────────────────────────────────────────────────────────────────────────
# Subject 1: CondorPoller.poll_condor — failure gating
# ─────────────────────────────────────────────────────────────────────────────


def test_poll_condor_failure_records_and_returns_cache_and_fires_on_down(monkeypatch):
    cached = [{"ClusterId": 1}]
    good = CallRecorder(result=cached)
    _patch_queue(monkeypatch, good)

    on_down = CallRecorder()
    on_up = CallRecorder()
    poller = CondorPoller(poll_interval=5.0, on_condor_down=on_down, on_condor_up=on_up)

    # Seed a cached good result first.
    assert poller.poll_condor(now=0.0) == cached
    assert poller.condor_offline is False

    # Now make the query raise.
    boom = htcondor_poll.CondorQueryError("schedd down")
    failing = CallRecorder(raises=boom)
    _patch_queue(monkeypatch, failing)

    out = poller.poll_condor(now=10.0)

    # Does NOT raise; returns the cached last-good jobs.
    assert out == cached
    assert failing.count == 1
    # fail_streak advanced to 1.
    assert poller.backoff.fail_streak == 1
    assert poller.condor_offline is True
    # on_condor_down fired exactly once, with the raising exception.
    assert on_down.count == 1
    assert on_down.calls[0][0][0] is boom
    assert on_up.count == 0


def test_poll_condor_gated_skips_query_within_backoff_window(monkeypatch):
    cached = [{"ClusterId": 1}]
    good = CallRecorder(result=cached)
    _patch_queue(monkeypatch, good)
    poller = CondorPoller(poll_interval=5.0)
    poller.poll_condor(now=0.0)

    # Drive a failure to open a back-off window.
    failing = CallRecorder(raises=htcondor_poll.CondorQueryError("down"))
    _patch_queue(monkeypatch, failing)
    poller.poll_condor(now=10.0)
    assert failing.count == 1

    # A retry while still inside the back-off window must be gated.
    retry_in = poller.backoff.next_retry_in(10.0)
    assert retry_in > 0
    gated_now = 10.0 + retry_in / 2.0
    assert poller.backoff.due(gated_now) is False

    out = poller.poll_condor(now=gated_now)
    assert out == cached
    # query_queue NOT called again while gated.
    assert failing.count == 1


def test_poll_condor_recovery_fires_on_up_once_and_resets_streak(monkeypatch):
    cached = [{"ClusterId": 1}]
    good = CallRecorder(result=cached)
    _patch_queue(monkeypatch, good)
    on_down = CallRecorder()
    on_up = CallRecorder()
    poller = CondorPoller(poll_interval=5.0, on_condor_down=on_down, on_condor_up=on_up)
    poller.poll_condor(now=0.0)

    # Fail once.
    failing = CallRecorder(raises=htcondor_poll.CondorQueryError("down"))
    _patch_queue(monkeypatch, failing)
    poller.poll_condor(now=10.0)
    assert poller.backoff.fail_streak == 1
    assert on_down.count == 1

    # Advance past retry_at and succeed.
    recovered = [{"ClusterId": 9}]
    good2 = CallRecorder(result=recovered)
    _patch_queue(monkeypatch, good2)
    open_now = _open_gate_time(poller, 10.0)
    assert poller.backoff.due(open_now) is True

    out = poller.poll_condor(now=open_now)
    assert out == recovered
    assert good2.count == 1
    # Streak reset, back online.
    assert poller.backoff.fail_streak == 0
    assert poller.condor_offline is False
    # Recovery callback fired exactly once; down still only once.
    assert on_up.count == 1
    assert on_down.count == 1


def test_on_condor_down_fires_only_on_first_failure_of_streak(monkeypatch):
    good = CallRecorder(result=[{"ClusterId": 1}])
    _patch_queue(monkeypatch, good)
    on_down = CallRecorder()
    poller = CondorPoller(poll_interval=5.0, on_condor_down=on_down)
    poller.poll_condor(now=0.0)

    failing = CallRecorder(raises=htcondor_poll.CondorQueryError("down"))
    _patch_queue(monkeypatch, failing)

    # First failure.
    poller.poll_condor(now=10.0)
    assert poller.backoff.fail_streak == 1
    # Advance past the first retry window for a SECOND failure.
    second_now = _open_gate_time(poller, 10.0)
    poller.poll_condor(now=second_now)
    assert poller.backoff.fail_streak == 2
    assert failing.count == 2

    # on_condor_down only fired on the first failure of the streak.
    assert on_down.count == 1


def test_condor_offline_property_tracks_fail_streak(monkeypatch):
    good = CallRecorder(result=[])
    _patch_queue(monkeypatch, good)
    poller = CondorPoller(poll_interval=5.0)
    assert poller.condor_offline is False
    poller.poll_condor(now=0.0)
    assert poller.condor_offline is False

    failing = CallRecorder(raises=htcondor_poll.CondorQueryError("x"))
    _patch_queue(monkeypatch, failing)
    poller.poll_condor(now=10.0)
    assert poller.condor_offline is True

    good2 = CallRecorder(result=[])
    _patch_queue(monkeypatch, good2)
    poller.poll_condor(now=_open_gate_time(poller, 10.0))
    assert poller.condor_offline is False


# ─────────────────────────────────────────────────────────────────────────────
# Subject 1: CondorPoller.poll_history — throttle, merge, skip-while-failing
# ─────────────────────────────────────────────────────────────────────────────


def test_poll_history_throttle_and_merge_dedup(monkeypatch):
    # Two polls with overlapping ClusterIds; cache must hold each cluster once.
    first = [{"ClusterId": 101, "ExitCode": 0}, {"ClusterId": 102, "ExitCode": 0}]
    second = [{"ClusterId": 102, "ExitCode": 0}, {"ClusterId": 103, "ExitCode": 0}]
    seq = iter([first, second])
    calls: List[tuple] = []

    def query_history(*args, **kwargs):
        calls.append((args, kwargs))
        return next(seq)

    _patch_history(monkeypatch, query_history)

    poller = CondorPoller(poll_interval=5.0, history_interval=10.0)

    # First call: last_poll is None -> queries.
    out1 = poller.poll_history(now=0.0)
    assert len(calls) == 1
    assert {h["ClusterId"] for h in out1} == {101, 102}

    # Immediate second call inside the interval -> cache, no re-query.
    out2 = poller.poll_history(now=5.0)
    assert len(calls) == 1
    assert out2 is poller.history_cache
    assert {h["ClusterId"] for h in out2} == {101, 102}

    # Past the interval -> re-queries and merges (102 dedup'd, 103 added).
    out3 = poller.poll_history(now=11.0)
    assert len(calls) == 2
    cids = [h["ClusterId"] for h in out3]
    assert cids == [101, 102, 103]  # order preserved, 102 only once
    assert len(cids) == len(set(cids))


def test_poll_history_skipped_while_condor_failing(monkeypatch):
    # Drive a real condor failure so fail_streak > 0.
    failing = CallRecorder(raises=htcondor_poll.CondorQueryError("down"))
    _patch_queue(monkeypatch, failing)
    qh = CallRecorder(result=[{"ClusterId": 1}])
    _patch_history(monkeypatch, qh)

    poller = CondorPoller(poll_interval=5.0, history_interval=10.0)
    poller.poll_condor(now=0.0)
    assert poller.backoff.fail_streak > 0

    out = poller.poll_history(now=0.0)
    # Returns the (empty) cache and never queries history while failing.
    assert out is poller.history_cache
    assert out == []
    assert qh.count == 0


def test_poll_history_empty_result_does_not_corrupt_cache(monkeypatch):
    qh = CallRecorder(result=[])
    _patch_history(monkeypatch, qh)
    poller = CondorPoller(poll_interval=5.0, history_interval=10.0)
    out = poller.poll_history(now=0.0)
    assert out == []
    assert qh.count == 1
    # last_poll advanced; second call within interval still cached.
    assert poller.poll_history(now=1.0) == []
    assert qh.count == 1


# ─────────────────────────────────────────────────────────────────────────────
# Subject 1: CondorPoller.poll_pool — throttle, kwargs, skip-while-failing
# ─────────────────────────────────────────────────────────────────────────────


def test_poll_pool_throttle(monkeypatch):
    s1 = PoolSummary(total_slots=2)
    s2 = PoolSummary(total_slots=3)
    seq = iter([s1, s2])
    calls: List[tuple] = []

    def query_slots(*args, **kwargs):
        calls.append((args, kwargs))
        return next(seq)

    _patch_slots(monkeypatch, query_slots)

    poller = CondorPoller(poll_interval=5.0, pool_interval=15.0)
    out1 = poller.poll_pool(now=0.0)
    assert out1 is s1
    assert len(calls) == 1

    # Within interval -> cached.
    out2 = poller.poll_pool(now=10.0)
    assert out2 is s1
    assert len(calls) == 1
    assert poller.pool_cache is s1

    # Past interval -> re-query.
    out3 = poller.poll_pool(now=16.0)
    assert out3 is s2
    assert len(calls) == 2


def test_poll_pool_forwards_only_present_credential_keys(monkeypatch):
    qs = CallRecorder(result=PoolSummary())
    _patch_slots(monkeypatch, qs)

    ck = {
        "collector_host": "c:9618",
        "token_path": "/t",
        # schedd_name/constraint are NOT pool kwargs and must be dropped.
        "schedd_name": "sched1",
    }
    poller = CondorPoller(poll_interval=5.0, pool_interval=15.0, condor_kwargs=ck)
    poller.poll_pool(now=0.0)

    _, kwargs = qs.calls[0]
    assert kwargs == {"collector_host": "c:9618", "token_path": "/t"}


def test_poll_pool_skipped_while_condor_failing(monkeypatch):
    failing = CallRecorder(raises=htcondor_poll.CondorQueryError("down"))
    _patch_queue(monkeypatch, failing)
    qs = CallRecorder(result=PoolSummary(total_slots=5))
    _patch_slots(monkeypatch, qs)

    poller = CondorPoller(poll_interval=5.0, pool_interval=15.0)
    poller.poll_condor(now=0.0)
    assert poller.backoff.fail_streak > 0

    out = poller.poll_pool(now=0.0)
    assert out is poller.pool_cache  # None, the unset cache
    assert out is None
    assert qs.count == 0


# ─────────────────────────────────────────────────────────────────────────────
# Subject 1: CondorPoller.poll — ordering and skip-in-same-cycle
# ─────────────────────────────────────────────────────────────────────────────


def test_poll_all_success_returns_triple(monkeypatch):
    jobs = [{"ClusterId": 1}]
    hist = [{"ClusterId": 101}]
    pool = PoolSummary(total_slots=4)
    _patch_queue(monkeypatch, CallRecorder(result=jobs))
    _patch_history(monkeypatch, CallRecorder(result=hist))
    _patch_slots(monkeypatch, CallRecorder(result=pool))

    poller = CondorPoller(poll_interval=5.0, history_interval=10.0, pool_interval=15.0)
    out = poller.poll(now=0.0)

    assert isinstance(out, tuple) and len(out) == 3
    cj, h, p = out
    assert cj == jobs
    assert h == hist
    assert h == poller.history_cache
    assert p is pool


def test_poll_condor_failure_skips_history_and_pool_same_cycle(monkeypatch):
    failing = CallRecorder(raises=htcondor_poll.CondorQueryError("down"))
    _patch_queue(monkeypatch, failing)
    qh = CallRecorder(result=[{"ClusterId": 101}])
    qs = CallRecorder(result=PoolSummary(total_slots=4))
    _patch_history(monkeypatch, qh)
    _patch_slots(monkeypatch, qs)

    poller = CondorPoller(poll_interval=5.0, history_interval=10.0, pool_interval=15.0)
    cj, h, p = poller.poll(now=0.0)

    # Condor failed this cycle -> fail_streak>0 -> history & pool skipped.
    assert failing.count == 1
    assert qh.count == 0
    assert qs.count == 0
    assert cj == []  # cached empty
    assert h == []
    assert p is None


# ─────────────────────────────────────────────────────────────────────────────
# CondorBackoff direct behavior (anchors the gating semantics above)
# ─────────────────────────────────────────────────────────────────────────────


def test_backoff_due_and_recovery_transitions():
    b = CondorBackoff(base_interval=5.0)
    assert b.due(0.0) is True  # streak 0 always due
    assert b.next_retry_in(0.0) == 0.0

    assert b.record(False, now=0.0) is False  # first failure, not a recovery
    assert b.fail_streak == 1
    assert b.next_retry_in(0.0) > 0.0
    # Not due before retry_at.
    assert b.due(0.0) is False

    # Recovery returns True exactly once.
    open_now = 0.0 + b.next_retry_in(0.0) + 0.1
    assert b.due(open_now) is True
    assert b.record(True, now=open_now) is True
    assert b.fail_streak == 0
    # A subsequent success is not a recovery transition.
    assert b.record(True, now=open_now + 1) is False


# ─────────────────────────────────────────────────────────────────────────────
# Subject 2: server._run_poll_cycle
# ─────────────────────────────────────────────────────────────────────────────


class FakeDB:
    def __init__(self, snap):
        self._snap = snap
        self.snapshot_calls = 0

    def snapshot(self):
        self.snapshot_calls += 1
        return self._snap


class FakeLogger:
    def __init__(self, high_water_ts: float = 0.0):
        self.high_water_ts = high_water_ts
        self.record_calls: List[tuple] = []

    def record(self, snap, condor_jobs, history, pool):
        self.record_calls.append((snap, condor_jobs, history, pool))


class FakePoller:
    def __init__(self, result):
        self._result = result
        self.poll_calls = 0

    def poll(self, now=None):
        self.poll_calls += 1
        return self._result


class FakeDiagEngine:
    def __init__(self, raises: Optional[Exception] = None):
        self.tick_calls: List[tuple] = []
        self._raises = raises

    def tick(self, snap, condor_jobs, pool, high_water_ts):
        self.tick_calls.append((snap, condor_jobs, pool, high_water_ts))
        if self._raises is not None:
            raise self._raises


def test_run_poll_cycle_returns_tuple_and_records(make_snapshot):
    from workflow_monitor.server import _run_poll_cycle

    snap = make_snapshot(states=["SUCCESS", "RUNNING"])
    jobs = [{"ClusterId": 1}]
    hist = [{"ClusterId": 101}]
    pool = PoolSummary(total_slots=2)

    db = FakeDB(snap)
    logger = FakeLogger()
    poller = FakePoller((jobs, hist, pool))

    out = _run_poll_cycle(db, logger, poller, diag_engine=None)

    assert out == (snap, jobs, hist, pool)
    assert db.snapshot_calls == 1
    assert poller.poll_calls == 1
    # logger.record called once with exactly those values.
    assert len(logger.record_calls) == 1
    assert logger.record_calls[0] == (snap, jobs, hist, pool)


def test_run_poll_cycle_with_real_poller(monkeypatch, make_snapshot):
    from workflow_monitor.server import _run_poll_cycle

    snap = make_snapshot(states=["RUNNING"])
    jobs = [{"ClusterId": 7}]
    hist = [{"ClusterId": 700}]
    pool = PoolSummary(total_slots=1)
    _patch_queue(monkeypatch, CallRecorder(result=jobs))
    _patch_history(monkeypatch, CallRecorder(result=hist))
    _patch_slots(monkeypatch, CallRecorder(result=pool))

    poller = CondorPoller(poll_interval=5.0)
    db = FakeDB(snap)
    logger = FakeLogger()

    s, cj, h, p = _run_poll_cycle(db, logger, poller, diag_engine=None)
    assert s is snap
    assert cj == jobs
    assert h == hist
    assert p is pool
    assert logger.record_calls == [(snap, jobs, hist, pool)]


def test_run_poll_cycle_ticks_diag_engine_with_high_water_ts(make_snapshot):
    from workflow_monitor.server import _run_poll_cycle

    snap = make_snapshot(states=["SUCCESS"])
    jobs = [{"ClusterId": 1}]
    hist = [{"ClusterId": 101}]
    pool = PoolSummary(total_slots=3)

    db = FakeDB(snap)
    logger = FakeLogger(high_water_ts=123.5)
    poller = FakePoller((jobs, hist, pool))
    diag = FakeDiagEngine()

    _run_poll_cycle(db, logger, poller, diag_engine=diag)

    # tick gets snapshot, condor jobs, pool (NOT history), and high_water_ts.
    assert len(diag.tick_calls) == 1
    assert diag.tick_calls[0] == (snap, jobs, pool, 123.5)


def test_run_poll_cycle_swallows_diag_engine_error(make_snapshot, capsys):
    from workflow_monitor.server import _run_poll_cycle

    snap = make_snapshot(states=["FAILED"])
    jobs: List[Dict[str, Any]] = []
    hist: List[Dict[str, Any]] = []
    pool = None

    db = FakeDB(snap)
    logger = FakeLogger()
    poller = FakePoller((jobs, hist, pool))
    diag = FakeDiagEngine(raises=RuntimeError("diag boom"))

    # Must NOT propagate; still returns the full tuple.
    out = _run_poll_cycle(db, logger, poller, diag_engine=diag)
    assert out == (snap, jobs, hist, pool)
    assert len(diag.tick_calls) == 1
    # record still happened before the diag tick.
    assert len(logger.record_calls) == 1
