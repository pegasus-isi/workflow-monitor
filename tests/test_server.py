"""Unit tests for :mod:`workflow_monitor.server`.

Covers the testable surface WITHOUT real daemonization, forking, or signals to
real PIDs:

* ``stop_server`` — PID-file read + SIGTERM (``os.kill`` monkeypatched).
* ``_cleanup_pid`` — tolerant PID-file removal.
* ``run_server`` — driven in ``foreground=True`` mode (NO ``_daemonize`` /
  ``os.fork``) with all condor queries and ``time.sleep`` mocked and a snapshot
  that is already complete, so the poll loop exits after one finalising cycle.
  ``signal.signal`` is monkeypatched so the test process's handlers are never
  touched.

See the test report for the note on ``_daemonize`` (forks; not unit-tested) and
the suggested refactor to extract a single-iteration poll function.
"""

from __future__ import annotations

import json
import signal
from pathlib import Path
from typing import List

import pytest

from workflow_monitor.braindump import WorkflowInfo
import workflow_monitor.server as server
import workflow_monitor.htcondor_poll as htcondor_poll
from workflow_monitor.server import _cleanup_pid, run_server, stop_server


# ─────────────────────────────────────────────────────────────────────────────
# stop_server
# ─────────────────────────────────────────────────────────────────────────────


def test_stop_server_missing_pid_file_returns_false(tmp_path):
    pid_file = tmp_path / "nope.pid"
    assert not pid_file.exists()
    assert stop_server(pid_file) is False


def test_stop_server_running_pid_signals_and_returns_true(tmp_path, monkeypatch):
    pid_file = tmp_path / "wf.pid"
    pid_file.write_text("4321")

    sent: List[tuple] = []
    monkeypatch.setattr(server.os, "kill", lambda pid, sig: sent.append((pid, sig)))

    assert stop_server(pid_file) is True
    assert sent == [(4321, signal.SIGTERM)]


def test_stop_server_pid_file_with_whitespace(tmp_path, monkeypatch):
    pid_file = tmp_path / "wf.pid"
    pid_file.write_text("  9876\n")

    sent: List[tuple] = []
    monkeypatch.setattr(server.os, "kill", lambda pid, sig: sent.append((pid, sig)))

    assert stop_server(pid_file) is True
    assert sent == [(9876, signal.SIGTERM)]


def test_stop_server_stale_pid_returns_false(tmp_path, monkeypatch):
    """A nonexistent PID -> os.kill raises ProcessLookupError (an OSError),
    handled gracefully by returning False."""
    pid_file = tmp_path / "wf.pid"
    pid_file.write_text("12345")

    def boom(pid, sig):
        raise ProcessLookupError("no such process")

    monkeypatch.setattr(server.os, "kill", boom)
    assert stop_server(pid_file) is False


def test_stop_server_permission_error_returns_false(tmp_path, monkeypatch):
    pid_file = tmp_path / "wf.pid"
    pid_file.write_text("1")  # e.g. init — not ours to signal

    def boom(pid, sig):
        raise PermissionError("operation not permitted")

    monkeypatch.setattr(server.os, "kill", boom)
    assert stop_server(pid_file) is False


def test_stop_server_non_integer_pid_returns_false(tmp_path, monkeypatch):
    pid_file = tmp_path / "wf.pid"
    pid_file.write_text("not-a-number")

    # os.kill must never be reached (int() raises ValueError first).
    monkeypatch.setattr(
        server.os,
        "kill",
        lambda pid, sig: pytest.fail("os.kill should not be called for bad PID"),
    )
    assert stop_server(pid_file) is False


# ─────────────────────────────────────────────────────────────────────────────
# _cleanup_pid
# ─────────────────────────────────────────────────────────────────────────────


def test_cleanup_pid_removes_existing(tmp_path):
    pid_file = tmp_path / "wf.pid"
    pid_file.write_text("999")
    _cleanup_pid(pid_file)
    assert not pid_file.exists()


def test_cleanup_pid_tolerates_missing(tmp_path):
    pid_file = tmp_path / "absent.pid"
    assert not pid_file.exists()
    # Must not raise.
    _cleanup_pid(pid_file)
    assert not pid_file.exists()


def test_cleanup_pid_tolerates_oserror(tmp_path, monkeypatch):
    pid_file = tmp_path / "wf.pid"
    pid_file.write_text("999")

    def boom(*a, **k):
        raise OSError("device busy")

    monkeypatch.setattr(Path, "unlink", boom)
    # Swallowed — no exception escapes.
    _cleanup_pid(pid_file)


# ─────────────────────────────────────────────────────────────────────────────
# run_server (foreground; loop exits after one finalising cycle)
# ─────────────────────────────────────────────────────────────────────────────


class _FakeDB:
    """StampedeDB stand-in that returns a fixed completed snapshot.

    Implements the three methods EventLogger / run_server invoke on a DB:
    ``snapshot`` (the loop), and ``get_workflow_times`` / ``get_events_since``
    (used by EventLogger's header + job-event recording).  This keeps the test
    fully offline while still exercising the real EventLogger.
    """

    def __init__(self, snapshot):
        self._snap = snapshot
        self.snapshot_calls = 0

    def snapshot(self):
        self.snapshot_calls += 1
        return self._snap

    def get_workflow_times(self):
        return {"start": self._snap.wf_start, "end": self._snap.wf_end}

    def get_events_since(self, after_ts):
        return []


def _make_info(submit_dir: Path) -> WorkflowInfo:
    return WorkflowInfo(
        wf_uuid="uuid-1",
        root_wf_uuid="uuid-1",
        dax_label="diamond",
        submit_dir=submit_dir,
        user="tester",
        planner_version="5.1.2",
        dag_file="diamond-0.dag",
        condor_log="diamond-0.log",
        timestamp="20260101T000000",
        basedir=submit_dir.parent,
    )


@pytest.fixture
def patch_condor(monkeypatch):
    """Stub out all condor queries used by run_server so no condor runs.

    run_server polls through htcondor_poll.CondorPoller, which calls the query
    functions in the htcondor_poll module — so the stubs are installed there.
    """
    monkeypatch.setattr(htcondor_poll, "query_queue", lambda **kw: [])
    monkeypatch.setattr(htcondor_poll, "query_history", lambda **kw: [])
    monkeypatch.setattr(htcondor_poll, "query_slots", lambda **kw: None)


@pytest.fixture
def no_sleep(monkeypatch):
    """Make time.sleep a no-op inside server (keeps the test instant)."""
    monkeypatch.setattr(server.time, "sleep", lambda *_a, **_k: None)


@pytest.fixture
def no_signal(monkeypatch):
    """Don't install real signal handlers in the test process."""
    monkeypatch.setattr(server.signal, "signal", lambda *a, **k: None)


def _read_events(path: Path) -> List[dict]:
    return [json.loads(ln) for ln in path.read_text().splitlines() if ln.strip()]


def test_run_server_foreground_completes_and_writes_log(
    make_snapshot,
    tmp_path,
    monkeypatch,
    patch_condor,
    no_sleep,
    no_signal,
):
    """A complete + not-running snapshot drives the loop through the
    is_complete branch once, then breaks; the log gets header + end events and
    the PID file is created then cleaned up."""
    submit = tmp_path / "run0001"
    submit.mkdir()
    info = _make_info(submit)

    snap = make_snapshot(
        states=["SUCCESS", "SUCCESS"],
        wf_state="WORKFLOW_TERMINATED",
        wf_status=0,
        wf_start=1000.0,
        wf_end=1060.0,
    )
    db = _FakeDB(snap)
    log_path = submit / "workflow-events.jsonl"

    # Guard against accidental daemonization.
    monkeypatch.setattr(
        server,
        "_daemonize",
        lambda *_a: pytest.fail("run_server must not daemonize in foreground"),
    )

    run_server(info, db, poll_interval=0.01, log_path=log_path, foreground=True)

    assert log_path.exists()
    events = _read_events(log_path)
    types = [e["event_type"] for e in events]
    assert "workflow_start" in types
    assert "workflow_end" in types
    assert types[-1] == "workflow_end"

    end = events[-1]
    assert end["wf_state"] == "WORKFLOW_TERMINATED"
    assert end["wf_status"] == 0

    # The complete-branch polls the DB twice (initial + final flush).
    assert db.snapshot_calls == 2

    # PID file is created on entry and removed in the finally block.
    pid_file = log_path.with_suffix(".pid")
    assert not pid_file.exists()


def test_run_server_default_log_path_under_submit_dir(
    make_snapshot, tmp_path, monkeypatch, patch_condor, no_sleep, no_signal
):
    submit = tmp_path / "run0002"
    submit.mkdir()
    info = _make_info(submit)
    snap = make_snapshot(
        states=["SUCCESS"],
        wf_state="WORKFLOW_TERMINATED",
        wf_status=0,
    )
    db = _FakeDB(snap)

    monkeypatch.setattr(server, "_daemonize", lambda *_a: pytest.fail("no daemonize"))

    run_server(info, db, poll_interval=0.01, log_path=None, foreground=True)

    default_log = submit / "workflow-events.jsonl"
    assert default_log.exists()
    assert not default_log.with_suffix(".pid").exists()


def test_run_server_installs_sigterm_and_sigint_handlers(
    make_snapshot, tmp_path, monkeypatch, patch_condor, no_sleep
):
    submit = tmp_path / "run0003"
    submit.mkdir()
    info = _make_info(submit)
    snap = make_snapshot(
        states=["SUCCESS"], wf_state="WORKFLOW_TERMINATED", wf_status=0
    )
    db = _FakeDB(snap)

    installed = []
    monkeypatch.setattr(
        server.signal, "signal", lambda sig, handler: installed.append(sig)
    )
    monkeypatch.setattr(server, "_daemonize", lambda *_a: pytest.fail("no daemonize"))

    run_server(
        info,
        db,
        poll_interval=0.01,
        log_path=submit / "ev.jsonl",
        foreground=True,
    )

    assert signal.SIGTERM in installed
    assert signal.SIGINT in installed


def test_run_server_polls_condor_queue(
    make_snapshot, tmp_path, monkeypatch, no_sleep, no_signal
):
    """The live condor queue is polled and forwarded into the htcondor_poll
    event in the log."""
    submit = tmp_path / "run0004"
    submit.mkdir()
    info = _make_info(submit)
    snap = make_snapshot(
        states=["SUCCESS"], wf_state="WORKFLOW_TERMINATED", wf_status=0
    )
    db = _FakeDB(snap)

    fake_jobs = [{"ClusterId": 101, "ProcId": 0, "JobStatus": 4}]
    queue_calls = {"n": 0}

    def fake_queue(**kw):
        queue_calls["n"] += 1
        return list(fake_jobs)

    monkeypatch.setattr(htcondor_poll, "query_queue", fake_queue)
    monkeypatch.setattr(htcondor_poll, "query_history", lambda **kw: [])
    monkeypatch.setattr(htcondor_poll, "query_slots", lambda **kw: None)
    monkeypatch.setattr(server, "_daemonize", lambda *_a: pytest.fail("no daemonize"))

    log_path = submit / "ev.jsonl"
    run_server(info, db, poll_interval=0.01, log_path=log_path, foreground=True)

    assert queue_calls["n"] >= 1
    events = _read_events(log_path)
    polls = [e for e in events if e["event_type"] == "htcondor_poll"]
    assert polls, "expected at least one htcondor_poll event"
    assert polls[0]["jobs"] == fake_jobs


def test_run_server_survives_condor_query_failure(
    make_snapshot, tmp_path, monkeypatch, no_sleep, no_signal, capsys
):
    """A failing schedd must not crash the server — the CondorBackoff gate
    catches the exception, logs once, and the loop still completes."""
    submit = tmp_path / "run0005"
    submit.mkdir()
    info = _make_info(submit)
    snap = make_snapshot(
        states=["SUCCESS"], wf_state="WORKFLOW_TERMINATED", wf_status=0
    )
    db = _FakeDB(snap)

    def boom(**kw):
        raise RuntimeError("schedd unreachable")

    monkeypatch.setattr(htcondor_poll, "query_queue", boom)
    monkeypatch.setattr(htcondor_poll, "query_history", lambda **kw: [])
    monkeypatch.setattr(htcondor_poll, "query_slots", lambda **kw: None)
    monkeypatch.setattr(server, "_daemonize", lambda *_a: pytest.fail("no daemonize"))

    log_path = submit / "ev.jsonl"
    # Should NOT raise.
    run_server(info, db, poll_interval=0.01, log_path=log_path, foreground=True)

    assert log_path.exists()
    events = _read_events(log_path)
    assert events[-1]["event_type"] == "workflow_end"
    # Backoff prints a one-time notice to stderr.
    assert "backoff" in capsys.readouterr().err.lower()


def test_run_server_cleans_pid_on_exception(
    tmp_path, monkeypatch, patch_condor, no_sleep, no_signal
):
    """If the loop body raises, the finally block still removes the PID file
    and closes the logger."""
    submit = tmp_path / "run0006"
    submit.mkdir()
    info = _make_info(submit)

    class _ExplodingDB:
        # EventLogger.__init__ succeeds (header written) ...
        def get_workflow_times(self):
            return {"start": None, "end": None}

        def get_events_since(self, after_ts):
            return []

        # ... but the first poll-loop snapshot() blows up.
        def snapshot(self):
            raise RuntimeError("db gone")

    monkeypatch.setattr(server, "_daemonize", lambda *_a: pytest.fail("no daemonize"))

    log_path = submit / "ev.jsonl"
    # run_server catches the loop exception internally (prints traceback);
    # it should return without propagating.
    run_server(
        info, _ExplodingDB(), poll_interval=0.01, log_path=log_path, foreground=True
    )

    pid_file = log_path.with_suffix(".pid")
    assert not pid_file.exists()
    # Logger.close() with no snapshot still writes a workflow_end marker.
    assert log_path.exists()
