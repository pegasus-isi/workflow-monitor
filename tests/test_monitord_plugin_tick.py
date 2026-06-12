"""
Tests for the wfmonitor monitord plugin's condor polling (driven by the
pegasus-monitord plugin host's tick()) and the --no-condor-poll plumbing.

The plugin is driven directly -- no Pegasus install needed (the duck-typed
base-class stub kicks in) and no live condor: query_queue / query_history /
query_slots are monkeypatched at the module level where the plugin looks
them up.
"""

import json


import workflow_monitor.monitord_plugin as mp
from workflow_monitor.cli import build_parser
from workflow_monitor.monitord_plugin import WorkflowMonitorPlugin

SUBMIT_DIR = "/opt/workflows/submit/run-x"
EXPECTED_CONSTRAINT = (
    f'Cmd =!= UNDEFINED && substr(Cmd, 0, {len(SUBMIT_DIR)}) == "{SUBMIT_DIR}"'
)


class _Props:
    """Stub of Pegasus.tools.properties.Properties: propertyset() returns the
    plugin's config keys with the prefix already stripped."""

    def __init__(self, cfg):
        self._cfg = dict(cfg)

    def propertyset(self, prefix, remove=True):
        return dict(self._cfg)


class _FakeTime:
    """Replaces the plugin module's `time` so cadence is deterministic."""

    def __init__(self, monotonic=1000.0, wall=1.8e9):
        self.mono = monotonic
        self.wall = wall

    def monotonic(self):
        return self.mono

    def time(self):
        return self.wall


class _FakePool:
    def __init__(self, claimed=1):
        self._claimed = claimed

    def to_dict(self):
        return {
            "total_slots": 2,
            "claimed_slots": self._claimed,
            "idle_slots": 2 - self._claimed,
            "total_cpus": 16,
            "idle_cpus": 8,
        }


def _job(cluster=1, status=2, host="work1"):
    return {
        "ClusterId": cluster,
        "ProcId": 0,
        "JobStatus": status,
        "RemoteHost": host,
        "BytesSent": 0,
        "BytesRecvd": 0,
    }


def _start(tmp_path, monkeypatch, condor_poll=True, tick_interval="5"):
    cfg = {"events_path": str(tmp_path / "monitord-events.jsonl")}
    if condor_poll:
        cfg["condor_poll"] = "true"
        cfg["tick_interval"] = tick_interval
    plugin = WorkflowMonitorPlugin()
    plugin.start(_Props(cfg))
    return plugin


def _send_wf_plan(plugin, submit_dir=SUBMIT_DIR):
    plugin.handle_event(
        "stampede.wf.plan",
        {
            "xwf__id": "uuid-1",
            "ts": 1.78e9,
            "submit_dir": submit_dir,
            "dax__label": "diamond",
        },
    )


def _records(tmp_path, event_type=None):
    path = tmp_path / "monitord-events.jsonl"
    if not path.exists():
        return []
    recs = [json.loads(line) for line in path.read_text().splitlines()]
    if event_type:
        recs = [r for r in recs if r.get("event_type") == event_type]
    return recs


# --------------------------------------------------------------------------- #
# regression: the planner's 1970-era roster timestamps must not poison records
# --------------------------------------------------------------------------- #


def test_jobs_init_timestamp_falls_back_to_wall_clock(tmp_path, monkeypatch):
    """Roster events carry monotonic-as-epoch 1970 stamps; with no wall-clock
    ts seen, jobs_init must fall back to time.time() -- this is also the
    import-time regression (a NameError if `import time` goes missing)."""
    plugin = _start(tmp_path, monkeypatch, condor_poll=False)
    plugin.handle_event(
        "stampede.job.info", {"xwf__id": "u", "job__id": "j1", "ts": 692252}
    )
    plugin.handle_event("stampede.static.end", {"xwf__id": "u", "ts": 692253})
    (rec,) = _records(tmp_path, "jobs_init")
    assert rec["timestamp"] > 1e9
    plugin.stop()


# --------------------------------------------------------------------------- #
# tick gating
# --------------------------------------------------------------------------- #


def test_tick_noop_without_condor_poll(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(mp, "query_queue", lambda **kw: calls.append(kw) or [])
    plugin = _start(tmp_path, monkeypatch, condor_poll=False)
    _send_wf_plan(plugin)
    plugin.tick()
    assert calls == []
    assert _records(tmp_path, "htcondor_poll") == []
    plugin.stop()


def test_tick_skips_until_wf_plan_then_uses_constraint(tmp_path, monkeypatch):
    calls = []

    def fake_queue(**kw):
        calls.append(kw)
        return [_job()]

    monkeypatch.setattr(mp, "query_queue", fake_queue)
    monkeypatch.setattr(mp, "query_history", lambda **kw: [])
    monkeypatch.setattr(mp, "query_slots", lambda **kw: None)
    plugin = _start(tmp_path, monkeypatch)

    plugin.tick()  # before wf.plan: no submit_dir -> never poll unconstrained
    assert calls == []

    _send_wf_plan(plugin)
    plugin.tick()
    assert len(calls) == 1
    assert calls[0]["constraint"] == EXPECTED_CONSTRAINT
    assert calls[0]["raise_on_error"] is True
    (rec,) = _records(tmp_path, "htcondor_poll")
    assert rec["wf_uuid"] == "uuid-1"
    assert rec["timestamp"] > 1e9
    plugin.stop()


# --------------------------------------------------------------------------- #
# fingerprint dedup
# --------------------------------------------------------------------------- #


def test_classad_exprtree_values_serialize(tmp_path, monkeypatch):
    """The bindings path returns dict(ad) with classad.ExprTree values; _write
    must stringify them (default=str, EventLogger parity), not raise."""

    class _ExprTreeLike:
        def __str__(self):
            return "RemoteHost =?= undefined"

    job = _job()
    job["MemoryUsage"] = _ExprTreeLike()
    monkeypatch.setattr(mp, "query_queue", lambda **kw: [job])
    monkeypatch.setattr(mp, "query_history", lambda **kw: [])
    monkeypatch.setattr(mp, "query_slots", lambda **kw: None)
    plugin = _start(tmp_path, monkeypatch)
    _send_wf_plan(plugin)
    plugin.tick()
    (rec,) = _records(tmp_path, "htcondor_poll")
    assert rec["jobs"][0]["MemoryUsage"] == "RemoteHost =?= undefined"
    plugin.stop()


def test_queue_fingerprint_dedup(tmp_path, monkeypatch):
    jobs = [[_job(status=1)], [_job(status=1)], [_job(status=2)]]
    it = iter(jobs)
    monkeypatch.setattr(mp, "query_queue", lambda **kw: next(it))
    monkeypatch.setattr(mp, "query_history", lambda **kw: [])
    monkeypatch.setattr(mp, "query_slots", lambda **kw: None)
    ft = _FakeTime()
    monkeypatch.setattr(mp, "time", ft)
    plugin = _start(tmp_path, monkeypatch)
    _send_wf_plan(plugin)

    for _ in jobs:
        plugin.tick()
        ft.mono += 6  # past the backoff base each time

    recs = _records(tmp_path, "htcondor_poll")
    # identical poll deduped; the JobStatus change re-emits
    assert len(recs) == 2
    assert recs[0]["jobs"][0]["JobStatus"] == 1
    assert recs[1]["jobs"][0]["JobStatus"] == 2
    plugin.stop()


# --------------------------------------------------------------------------- #
# history / pool cadence and failure gating
# --------------------------------------------------------------------------- #


def test_history_and_pool_cadence(tmp_path, monkeypatch):
    hist_calls, pool_calls = [], []
    monkeypatch.setattr(mp, "query_queue", lambda **kw: [_job()])
    monkeypatch.setattr(
        mp, "query_history", lambda **kw: hist_calls.append(kw) or [_job(9)]
    )
    monkeypatch.setattr(
        mp, "query_slots", lambda **kw: pool_calls.append(kw) or _FakePool()
    )
    ft = _FakeTime()
    monkeypatch.setattr(mp, "time", ft)
    plugin = _start(tmp_path, monkeypatch, tick_interval="5")
    _send_wf_plan(plugin)
    # base 5 -> history >= 15s, pool >= 25s

    plugin.tick()  # t=1000: first tick polls everything (last marks at 0)
    assert (len(hist_calls), len(pool_calls)) == (1, 1)

    ft.mono += 10
    plugin.tick()  # t=+10: below both intervals
    assert (len(hist_calls), len(pool_calls)) == (1, 1)

    ft.mono += 6
    plugin.tick()  # t=+16: history due, pool not
    assert (len(hist_calls), len(pool_calls)) == (2, 1)

    ft.mono += 10
    plugin.tick()  # t=+26: pool due
    assert (len(hist_calls), len(pool_calls)) == (2, 2)

    # pool kwargs never include schedd_name
    assert all("schedd_name" not in kw for kw in pool_calls)
    assert len(_records(tmp_path, "htcondor_history")) == 1  # ClusterId set unchanged
    assert len(_records(tmp_path, "pool_status")) == 1
    plugin.stop()


def test_backoff_on_failure_skips_history_and_pool(tmp_path, monkeypatch):
    hist_calls, pool_calls = [], []

    def failing_queue(**kw):
        raise RuntimeError("schedd down")

    monkeypatch.setattr(mp, "query_queue", failing_queue)
    monkeypatch.setattr(mp, "query_history", lambda **kw: hist_calls.append(kw) or [])
    monkeypatch.setattr(
        mp, "query_slots", lambda **kw: pool_calls.append(kw) or _FakePool()
    )
    ft = _FakeTime()
    monkeypatch.setattr(mp, "time", ft)
    plugin = _start(tmp_path, monkeypatch)
    _send_wf_plan(plugin)

    plugin.tick()
    assert plugin._backoff.fail_streak == 1
    # while failing, history/pool are skipped entirely
    assert hist_calls == [] and pool_calls == []
    # next tick inside the backoff window: queue poll gated too
    ft.mono += 1
    plugin.tick()
    assert plugin._backoff.fail_streak == 1
    assert _records(tmp_path, "htcondor_poll") == []

    # recovery: queue succeeds again, emission resumes
    monkeypatch.setattr(mp, "query_queue", lambda **kw: [_job()])
    ft.mono += 120  # past max backoff
    plugin.tick()
    assert plugin._backoff.fail_streak == 0
    assert len(_records(tmp_path, "htcondor_poll")) == 1
    plugin.stop()


# --------------------------------------------------------------------------- #
# final flush on stop
# --------------------------------------------------------------------------- #


def test_stop_final_flush(tmp_path, monkeypatch):
    monkeypatch.setattr(mp, "query_queue", lambda **kw: [_job(status=4)])
    monkeypatch.setattr(mp, "query_history", lambda **kw: [_job(7, status=4)])
    monkeypatch.setattr(mp, "query_slots", lambda **kw: _FakePool(claimed=0))
    plugin = _start(tmp_path, monkeypatch)
    _send_wf_plan(plugin)

    plugin.stop()  # forces one last queue + history + pool poll
    assert len(_records(tmp_path, "htcondor_poll")) == 1
    assert len(_records(tmp_path, "htcondor_history")) == 1
    assert len(_records(tmp_path, "pool_status")) == 1
    assert plugin._output is None  # file closed after the flush


# --------------------------------------------------------------------------- #
# --serve plumbing
# --------------------------------------------------------------------------- #


def test_no_condor_poll_flag():
    p = build_parser()
    assert p.parse_args(["--serve"]).condor_poll is True
    assert p.parse_args(["--serve", "--no-condor-poll"]).condor_poll is False


# --------------------------------------------------------------------------- #
# synthesized workflow_end terminal marker
# --------------------------------------------------------------------------- #


def test_workflow_end_emitted_on_stop(tmp_path, monkeypatch):
    plugin = _start(tmp_path, monkeypatch, condor_poll=False)
    _send_wf_plan(plugin)
    plugin.handle_event(
        "stampede.job.info", {"xwf__id": "uuid-1", "job__id": "j1", "ts": 692252}
    )
    plugin.handle_event(
        "stampede.job.info", {"xwf__id": "uuid-1", "job__id": "j2", "ts": 692253}
    )
    plugin.handle_event("stampede.static.end", {"xwf__id": "uuid-1", "ts": 692254})
    plugin.handle_event("stampede.xwf.start", {"xwf__id": "uuid-1", "ts": 1.78e9 + 10})
    plugin.handle_event(
        "stampede.job_inst.main.end",
        {"xwf__id": "uuid-1", "ts": 1.78e9 + 20, "job__id": "j1", "status": 0},
    )
    plugin.handle_event(
        "stampede.job_inst.main.end",
        {"xwf__id": "uuid-1", "ts": 1.78e9 + 30, "job__id": "j2", "status": -1},
    )
    plugin.handle_event(  # retry succeeds
        "stampede.job_inst.main.end",
        {"xwf__id": "uuid-1", "ts": 1.78e9 + 40, "job__id": "j2", "status": 0},
    )
    plugin.handle_event(
        "stampede.xwf.end", {"xwf__id": "uuid-1", "ts": 1.78e9 + 100, "status": 0}
    )
    plugin.stop()

    recs = _records(tmp_path)
    end = recs[-1]
    assert end["event_type"] == "workflow_end"  # the LAST record
    assert end["wf_state"] == "WORKFLOW_TERMINATED"
    assert end["wf_status"] == 0
    assert end["wf_end"] == 1.78e9 + 100
    assert end["total_jobs"] == 2
    assert end["done"] == 2  # retried j2 counts done (last terminal state wins)
    assert end["failed"] == 0
    assert end["elapsed"] == 90


def test_no_workflow_end_without_termination(tmp_path, monkeypatch):
    plugin = _start(tmp_path, monkeypatch, condor_poll=False)
    _send_wf_plan(plugin)
    plugin.handle_event("stampede.xwf.start", {"xwf__id": "uuid-1", "ts": 1.78e9 + 10})
    plugin.stop()  # monitord killed mid-run: no xwf.end seen

    assert _records(tmp_path, "workflow_end") == []


def test_workflow_end_is_last_after_final_flush(tmp_path, monkeypatch):
    """The terminal marker must follow the final condor flush — a poll event
    after workflow_end reads as a server resume to the --remote consumer."""
    monkeypatch.setattr(mp, "query_queue", lambda **kw: [_job(status=4)])
    monkeypatch.setattr(mp, "query_history", lambda **kw: [_job(7, status=4)])
    monkeypatch.setattr(mp, "query_slots", lambda **kw: _FakePool(claimed=0))
    plugin = _start(tmp_path, monkeypatch)
    _send_wf_plan(plugin)
    plugin.handle_event(
        "stampede.xwf.end", {"xwf__id": "uuid-1", "ts": 1.78e9 + 50, "status": 0}
    )

    plugin.stop()
    recs = _records(tmp_path)
    assert recs[-1]["event_type"] == "workflow_end"
    assert {"htcondor_poll", "htcondor_history", "pool_status"} <= {
        r["event_type"] for r in recs[:-1]
    }
