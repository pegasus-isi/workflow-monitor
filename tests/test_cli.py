"""Unit tests for workflow_monitor.cli.

Two halves:

1. ``build_parser`` — parse representative argv lists and assert the resulting
   ``argparse.Namespace`` (defaults + flags flipping them, types, choices).
2. ``main`` — dispatch logic. Every heavy entry point that ``main`` reaches
   (``run_monitor``, ``run_server``, ``stop_server``, ``ReplayEngine``,
   ``RemoteEngine``, ``run_why_idle``, ``load_braindump``, ``StampedeDB``) is
   monkeypatched so nothing real runs; we assert the right one fires with the
   args derived from the parsed namespace and that ``main`` returns the right
   int.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import workflow_monitor.cli as cli
from workflow_monitor.cli import build_parser, main


# ─────────────────────────────────────────────────────────────────────────────
# build_parser — defaults and flag parsing
# ─────────────────────────────────────────────────────────────────────────────


def test_parser_defaults_with_no_args():
    args = build_parser().parse_args([])
    # Positional default
    assert args.target == "."
    # Refresh/display defaults
    assert args.interval == 2.0
    assert isinstance(args.interval, float)
    assert args.all_jobs is False
    assert args.sort_by_activity is True
    assert args.events == 15
    assert isinstance(args.events, int)
    # Mode flags default off / None
    assert args.once is False
    assert args.why_idle is False
    assert args.diagnose is False
    assert args.remap_submit_dir == "auto"
    assert args.log is None
    assert args.replay is None
    assert args.speed == 1.0
    # Logging safeguards
    assert args.min_free_mb == 200.0
    assert args.max_log_mb is None
    # Client/server
    assert args.serve is False
    assert args.serve_foreground is False
    assert args.stop_server is None
    assert args.remote is None
    assert args.sync_interval == 5.0
    assert args.ssh_config is None
    assert args.ssh_identity is None
    # Condor + credentials default to None
    assert args.schedd is None
    assert args.collector is None
    assert args.token is None
    assert args.cert is None
    assert args.key is None
    assert args.password_file is None


def test_parser_positional_target():
    args = build_parser().parse_args(["/some/run0001"])
    assert args.target == "/some/run0001"


def test_parser_once_and_all_jobs_flags():
    args = build_parser().parse_args(["--once", "--all-jobs", "TARGET"])
    assert args.once is True
    assert args.all_jobs is True
    assert args.target == "TARGET"


def test_parser_short_flags():
    # -a == --all-jobs, -e N == --events, -i N == --interval
    args = build_parser().parse_args(["-a", "-e", "30", "-i", "5"])
    assert args.all_jobs is True
    assert args.events == 30
    assert args.interval == 5.0


def test_parser_interval_is_float_type():
    args = build_parser().parse_args(["--interval", "2.5"])
    assert args.interval == 2.5
    assert isinstance(args.interval, float)


def test_parser_events_is_int_type():
    args = build_parser().parse_args(["--events", "42"])
    assert args.events == 42
    assert isinstance(args.events, int)


def test_parser_events_rejects_non_int():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--events", "notanumber"])


def test_parser_why_idle_flag():
    args = build_parser().parse_args(["--why-idle", "TARGET"])
    assert args.why_idle is True


def test_parser_diagnose_flag():
    args = build_parser().parse_args(["--diagnose"])
    assert args.diagnose is True


def test_parser_sort_by_activity_boolean_optional_action():
    # default True; --no-sort-by-activity flips it off; --sort-by-activity on
    assert build_parser().parse_args([]).sort_by_activity is True
    assert (
        build_parser().parse_args(["--no-sort-by-activity"]).sort_by_activity is False
    )
    assert build_parser().parse_args(["--sort-by-activity"]).sort_by_activity is True


def test_parser_log_nargs_optional_const_auto():
    # Bare --log -> "auto"; --log PATH -> the path; absent -> None
    assert build_parser().parse_args(["--log"]).log == "auto"
    assert build_parser().parse_args(["--log", "/tmp/x.jsonl"]).log == "/tmp/x.jsonl"
    assert build_parser().parse_args([]).log is None


def test_parser_replay_and_speed():
    args = build_parser().parse_args(["--replay", "/logs/events.jsonl", "--speed", "4"])
    assert args.replay == "/logs/events.jsonl"
    assert args.speed == 4.0


def test_parser_serve_flags():
    args = build_parser().parse_args(["--serve", "TARGET"])
    assert args.serve is True
    assert args.serve_foreground is False

    args2 = build_parser().parse_args(["--serve-foreground", "TARGET"])
    assert args2.serve_foreground is True
    assert args2.serve is False


def test_parser_stop_server_nargs_optional():
    assert build_parser().parse_args(["--stop-server"]).stop_server == "auto"
    assert (
        build_parser().parse_args(["--stop-server", "/run/x.pid"]).stop_server
        == "/run/x.pid"
    )
    assert build_parser().parse_args([]).stop_server is None


def test_parser_remote_spec():
    spec = "user@host:/path/to/workflow-events.jsonl"
    args = build_parser().parse_args(["--remote", spec])
    assert args.remote == spec


def test_parser_remap_submit_dir_choices():
    for choice in ("auto", "always", "never"):
        assert (
            build_parser().parse_args(["--remap-submit-dir", choice]).remap_submit_dir
            == choice
        )
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--remap-submit-dir", "bogus"])


def test_parser_credential_and_condor_flags():
    args = build_parser().parse_args(
        [
            "--token",
            "/tok",
            "--cert",
            "/cert",
            "--key",
            "/key",
            "--password-file",
            "/pw",
            "--schedd",
            "myschedd",
            "--collector",
            "collector.example.org:9618",
        ]
    )
    assert args.token == "/tok"
    assert args.cert == "/cert"
    assert args.key == "/key"
    assert args.password_file == "/pw"  # dash -> underscore in dest
    assert args.schedd == "myschedd"
    assert args.collector == "collector.example.org:9618"


def test_parser_log_safeguard_flags():
    args = build_parser().parse_args(["--min-free-mb", "50", "--max-log-mb", "10"])
    assert args.min_free_mb == 50.0
    assert args.max_log_mb == 10.0


def test_parser_version_exits():
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["--version"])
    assert exc.value.code == 0


def test_parser_unknown_flag_exits():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--no-such-flag"])


# ─────────────────────────────────────────────────────────────────────────────
# Helpers / fixtures for main() dispatch tests
# ─────────────────────────────────────────────────────────────────────────────


class _StubInfo:
    """A minimal stand-in for WorkflowInfo, enough for main()'s use."""

    def __init__(self, submit_dir, stampede_db, wf_uuid="uuid-1234abcd"):
        self.submit_dir = Path(submit_dir)
        self.recorded_submit_dir = Path(submit_dir)
        self.stampede_db = stampede_db
        self.wf_uuid = wf_uuid


class _StubDB:
    """Context-manager stub for StampedeDB; records construction kwargs."""

    last = None

    def __init__(self, db_path, wf_uuid=None):
        self.db_path = db_path
        self.wf_uuid = wf_uuid
        _StubDB.last = self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


@pytest.fixture
def patched_db(monkeypatch):
    """Patch StampedeDB in cli with the recording stub. Returns the class."""
    _StubDB.last = None
    monkeypatch.setattr(cli, "StampedeDB", _StubDB)
    return _StubDB


def _patch_braindump(monkeypatch, info):
    """Make cli.load_braindump return *info* and record its call args."""
    calls = {}

    def _fake(path, remap="auto"):
        calls["path"] = path
        calls["remap"] = remap
        return info

    monkeypatch.setattr(cli, "load_braindump", _fake)
    return calls


# ─────────────────────────────────────────────────────────────────────────────
# main() — live (default) mode
# ─────────────────────────────────────────────────────────────────────────────


def test_main_live_mode_invokes_run_monitor(monkeypatch, tmp_path, patched_db):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    bd_calls = _patch_braindump(monkeypatch, info)

    captured = {}

    def fake_run_monitor(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(cli, "run_monitor", fake_run_monitor)

    rc = main(["--interval", "3", "--events", "7", "--all-jobs", str(tmp_path)])

    assert rc == 0
    # load_braindump was called with the parsed target + remap
    assert bd_calls["path"] == Path(str(tmp_path))
    assert bd_calls["remap"] == "auto"
    # StampedeDB constructed with the resolved db_path and uuid
    assert patched_db.last is not None
    assert patched_db.last.db_path == info.stampede_db
    assert patched_db.last.wf_uuid == info.wf_uuid
    # run_monitor received namespace-derived args
    assert captured["info"] is info
    assert captured["poll_interval"] == 3.0
    assert captured["events_n"] == 7
    assert captured["show_all"] is True
    assert captured["once"] is False
    assert captured["log_path"] is None
    assert captured["diagnose"] is False
    assert captured["sort_by_activity"] is True
    # condor constraint scoped to the recorded submit dir
    assert "Cmd" in captured["condor_constraint"]


def test_main_live_mode_once_and_credentials(monkeypatch, tmp_path, patched_db):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    _patch_braindump(monkeypatch, info)

    captured = {}
    monkeypatch.setattr(cli, "run_monitor", lambda **kw: captured.update(kw))

    rc = main(
        [
            "--once",
            "--token",
            "/tok",
            "--schedd",
            "sched1",
            "--collector",
            "c:9618",
            str(tmp_path),
        ]
    )
    assert rc == 0
    assert captured["once"] is True
    ck = captured["condor_kwargs"]
    assert ck["token_path"] == "/tok"
    assert ck["schedd_name"] == "sched1"
    assert ck["collector_host"] == "c:9618"


def test_main_live_mode_log_auto_resolves_under_submit_dir(
    monkeypatch, tmp_path, patched_db
):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    _patch_braindump(monkeypatch, info)

    captured = {}
    monkeypatch.setattr(cli, "run_monitor", lambda **kw: captured.update(kw))

    # Bare --log (no value) -> const "auto" -> resolved under submit_dir.
    # Target precedes --log so argparse doesn't swallow it as --log's value.
    rc = main([str(tmp_path), "--log"])
    assert rc == 0
    assert captured["log_path"] == tmp_path / "workflow-events.jsonl"


def test_main_live_mode_log_explicit_path(monkeypatch, tmp_path, patched_db):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    _patch_braindump(monkeypatch, info)

    captured = {}
    monkeypatch.setattr(cli, "run_monitor", lambda **kw: captured.update(kw))

    explicit = str(tmp_path / "custom.jsonl")
    rc = main(["--log", explicit, str(tmp_path)])
    assert rc == 0
    assert captured["log_path"] == Path(explicit)


def test_main_braindump_not_found_returns_1(monkeypatch, tmp_path, capsys):
    def _raise(path, remap="auto"):
        raise FileNotFoundError("Cannot find braindump.yml")

    monkeypatch.setattr(cli, "load_braindump", _raise)
    # run_monitor should never be reached
    monkeypatch.setattr(
        cli, "run_monitor", lambda **kw: pytest.fail("run_monitor reached")
    )

    rc = main([str(tmp_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error" in err.lower()


def test_main_missing_stampede_db_returns_1(monkeypatch, tmp_path, capsys):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=None)  # no DB found
    _patch_braindump(monkeypatch, info)
    monkeypatch.setattr(
        cli, "run_monitor", lambda **kw: pytest.fail("run_monitor reached")
    )

    rc = main([str(tmp_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "Stampede database not found" in err


# ─────────────────────────────────────────────────────────────────────────────
# main() — replay mode
# ─────────────────────────────────────────────────────────────────────────────


def test_main_replay_mode_runs_engine(monkeypatch, tmp_path):
    log = tmp_path / "events.jsonl"
    log.write_text("{}\n")

    captured = {}

    class FakeReplayEngine:
        def __init__(self, path, speed=1.0, events_n=15):
            captured["init"] = (path, speed, events_n)

        def run(self, show_all=False, sort_by_activity=True):
            captured["run"] = (show_all, sort_by_activity)

    # ReplayEngine is imported lazily inside main(); patch the module attr.
    import workflow_monitor.replay as replay_mod

    monkeypatch.setattr(replay_mod, "ReplayEngine", FakeReplayEngine)

    rc = main(["--replay", str(log), "--speed", "4", "--events", "8", "--all-jobs"])
    assert rc == 0
    assert captured["init"] == (log, 4.0, 8)
    assert captured["run"] == (True, True)


def test_main_replay_file_not_found_returns_1(monkeypatch, tmp_path, capsys):
    missing = tmp_path / "nope.jsonl"
    rc = main(["--replay", str(missing)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "Replay file not found" in err


def test_main_replay_value_error_returns_1(monkeypatch, tmp_path, capsys):
    log = tmp_path / "bad.jsonl"
    log.write_text("garbage")

    class FakeReplayEngine:
        def __init__(self, *a, **k):
            raise ValueError("corrupt log")

        def run(self, **k):  # pragma: no cover - never reached
            pass

    import workflow_monitor.replay as replay_mod

    monkeypatch.setattr(replay_mod, "ReplayEngine", FakeReplayEngine)

    rc = main(["--replay", str(log)])
    assert rc == 1
    assert "corrupt log" in capsys.readouterr().err


# ─────────────────────────────────────────────────────────────────────────────
# main() — remote mode
# ─────────────────────────────────────────────────────────────────────────────


def test_main_remote_mode_runs_engine(monkeypatch):
    captured = {}

    class FakeRemoteEngine:
        def __init__(
            self,
            spec,
            sync_interval=5.0,
            events_n=15,
            ssh_config=None,
            ssh_identity=None,
        ):
            captured["init"] = dict(
                spec=spec,
                sync_interval=sync_interval,
                events_n=events_n,
                ssh_config=ssh_config,
                ssh_identity=ssh_identity,
            )

        def run(self, show_all=False, once=False, sort_by_activity=True):
            captured["run"] = dict(
                show_all=show_all, once=once, sort_by_activity=sort_by_activity
            )

    import workflow_monitor.remote as remote_mod

    monkeypatch.setattr(remote_mod, "RemoteEngine", FakeRemoteEngine)

    spec = "user@host:/path/events.jsonl"
    rc = main(
        [
            "--remote",
            spec,
            "--sync-interval",
            "9",
            "--events",
            "5",
            "--ssh-config",
            "/cfg",
            "--ssh-identity",
            "/id",
            "--once",
            "--all-jobs",
        ]
    )
    assert rc == 0
    assert captured["init"]["spec"] == spec
    assert captured["init"]["sync_interval"] == 9.0
    assert captured["init"]["events_n"] == 5
    assert captured["init"]["ssh_config"] == "/cfg"
    assert captured["init"]["ssh_identity"] == "/id"
    assert captured["run"] == dict(show_all=True, once=True, sort_by_activity=True)


def test_main_remote_value_error_returns_1(monkeypatch, capsys):
    class FakeRemoteEngine:
        def __init__(self, *a, **k):
            raise ValueError("bad spec")

        def run(self, **k):  # pragma: no cover
            pass

    import workflow_monitor.remote as remote_mod

    monkeypatch.setattr(remote_mod, "RemoteEngine", FakeRemoteEngine)

    rc = main(["--remote", "garbage"])
    assert rc == 1
    assert "bad spec" in capsys.readouterr().err


# ─────────────────────────────────────────────────────────────────────────────
# main() — why-idle mode
# ─────────────────────────────────────────────────────────────────────────────


def test_main_why_idle_invokes_run_why_idle(monkeypatch, tmp_path, patched_db):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    _patch_braindump(monkeypatch, info)

    sentinel_snap = object()
    # StampedeDB stub must expose snapshot(); extend the recording stub.
    monkeypatch.setattr(_StubDB, "snapshot", lambda self: sentinel_snap, raising=False)

    captured = {}

    def fake_run_why_idle(*, info, snapshot, condor_constraint, condor_kwargs):
        captured.update(
            info=info,
            snapshot=snapshot,
            condor_constraint=condor_constraint,
            condor_kwargs=condor_kwargs,
        )
        return 0

    import workflow_monitor.why_idle as why_mod

    monkeypatch.setattr(why_mod, "run_why_idle", fake_run_why_idle)
    # run_monitor must NOT be reached in why-idle mode
    monkeypatch.setattr(
        cli, "run_monitor", lambda **kw: pytest.fail("run_monitor reached")
    )

    rc = main(["--why-idle", "--schedd", "s1", str(tmp_path)])
    assert rc == 0
    assert captured["info"] is info
    assert captured["snapshot"] is sentinel_snap
    assert "Cmd" in captured["condor_constraint"]
    assert captured["condor_kwargs"]["schedd_name"] == "s1"


# ─────────────────────────────────────────────────────────────────────────────
# main() — serve mode
# ─────────────────────────────────────────────────────────────────────────────


def test_main_serve_mode_invokes_run_server(monkeypatch, tmp_path, patched_db):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    _patch_braindump(monkeypatch, info)

    captured = {}

    def fake_run_server(**kwargs):
        captured.update(kwargs)

    import workflow_monitor.server as server_mod

    monkeypatch.setattr(server_mod, "run_server", fake_run_server)
    monkeypatch.setattr(
        cli, "run_monitor", lambda **kw: pytest.fail("run_monitor reached")
    )

    rc = main(["--serve", "--interval", "4", "--diagnose", str(tmp_path)])
    assert rc == 0
    assert captured["info"] is info
    assert captured["poll_interval"] == 4.0
    assert captured["foreground"] is False
    assert captured["diagnose"] is True
    assert captured["min_free_mb"] == 200.0
    assert captured["max_log_mb"] is None
    assert "Cmd" in captured["condor_constraint"]
    # db kwarg is the StampedeDB stub instance
    assert captured["db"] is patched_db.last


def test_main_serve_foreground_sets_foreground_true(monkeypatch, tmp_path, patched_db):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    _patch_braindump(monkeypatch, info)

    captured = {}
    import workflow_monitor.server as server_mod

    monkeypatch.setattr(server_mod, "run_server", lambda **kw: captured.update(kw))

    rc = main(["--serve-foreground", str(tmp_path)])
    assert rc == 0
    assert captured["foreground"] is True


# ─────────────────────────────────────────────────────────────────────────────
# main() — stop-server mode
# ─────────────────────────────────────────────────────────────────────────────


def test_main_stop_server_auto_uses_braindump_pid_path(monkeypatch, tmp_path, capsys):
    info = _StubInfo(submit_dir=tmp_path, stampede_db=tmp_path / "x.stampede.db")
    _patch_braindump(monkeypatch, info)

    captured = {}

    def fake_stop_server(pid_file):
        captured["pid_file"] = pid_file
        return True

    import workflow_monitor.server as server_mod

    monkeypatch.setattr(server_mod, "stop_server", fake_stop_server)

    # Bare --stop-server (const "auto") + a positional target -> the pid file
    # is derived from the braindump's submit_dir.
    rc = main([str(tmp_path), "--stop-server"])
    assert rc == 0
    assert captured["pid_file"] == tmp_path / "workflow-events.pid"
    assert "Server stopped" in capsys.readouterr().out


def test_main_stop_server_explicit_pid_file(monkeypatch, tmp_path, capsys):
    pid = tmp_path / "my.pid"

    captured = {}

    def fake_stop_server(pid_file):
        captured["pid_file"] = pid_file
        return True

    import workflow_monitor.server as server_mod

    monkeypatch.setattr(server_mod, "stop_server", fake_stop_server)

    rc = main(["--stop-server", str(pid)])
    assert rc == 0
    assert captured["pid_file"] == pid


def test_main_stop_server_failure_returns_1(monkeypatch, tmp_path, capsys):
    pid = tmp_path / "my.pid"

    import workflow_monitor.server as server_mod

    monkeypatch.setattr(server_mod, "stop_server", lambda pid_file: False)

    rc = main(["--stop-server", str(pid)])
    assert rc == 1
    assert "Could not stop server" in capsys.readouterr().err


def test_main_stop_server_auto_braindump_missing_returns_1(
    monkeypatch, tmp_path, capsys
):
    def _raise(path, remap="auto"):
        raise FileNotFoundError("no braindump")

    monkeypatch.setattr(cli, "load_braindump", _raise)
    import workflow_monitor.server as server_mod

    monkeypatch.setattr(
        server_mod, "stop_server", lambda pid_file: pytest.fail("stop_server reached")
    )

    rc = main([str(tmp_path), "--stop-server"])
    assert rc == 1
    assert "error" in capsys.readouterr().err.lower()
