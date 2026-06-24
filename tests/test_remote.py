"""Unit tests for :mod:`workflow_monitor.remote`.

These tests exercise the pure parsing helpers and the event-application /
snapshot-reconstruction path of :class:`RemoteEngine` WITHOUT any real SSH or
network access.  ``_fetch_file`` / ``subprocess`` are never invoked; we drive
the engine by writing JSONL directly into its local cache file and calling the
loader/apply methods, mirroring the replay reconstruction semantics.

``RemoteEngine.run()`` is intentionally NOT exercised here — it enters a Rich
``Live`` TUI loop with real ``time.sleep`` and (in non-``once`` mode) blocks
forever.  See the module docstring of the test report for the gap note.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from workflow_monitor.braindump import WorkflowInfo
from workflow_monitor.db import WorkflowSnapshot
from workflow_monitor.htcondor_poll import PoolSummary
from workflow_monitor.stats import WorkflowStats
import workflow_monitor.remote as remote
from workflow_monitor.remote import (
    RemoteEngine,
    _build_ssh_base,
    _parse_remote_spec,
    _strip_brackets,
)


# ─────────────────────────────────────────────────────────────────────────────
# _parse_remote_spec
# ─────────────────────────────────────────────────────────────────────────────


def test_parse_remote_spec_user_host_path():
    host, path = _parse_remote_spec("alice@host.example.org:/var/log/wf.jsonl")
    assert host == "alice@host.example.org"
    assert path == "/var/log/wf.jsonl"


def test_parse_remote_spec_host_without_user():
    host, path = _parse_remote_spec("host:/path/to/file.jsonl")
    assert host == "host"
    assert path == "/path/to/file.jsonl"


def test_parse_remote_spec_ipv6_in_brackets():
    host, path = _parse_remote_spec("user@[2001:db8::1]:/path/to/file.jsonl")
    # The host part keeps the brackets; the colon AFTER ']' splits host/path.
    assert host == "user@[2001:db8::1]"
    assert path == "/path/to/file.jsonl"


def test_parse_remote_spec_ipv6_no_user():
    host, path = _parse_remote_spec("[::1]:/path/file.jsonl")
    assert host == "[::1]"
    assert path == "/path/file.jsonl"


def test_parse_remote_spec_path_with_colon_only_first_split():
    # Only the FIRST colon (after host) splits; later colons stay in the path.
    host, path = _parse_remote_spec("host:/weird:path:name.jsonl")
    assert host == "host"
    assert path == "/weird:path:name.jsonl"


def test_parse_remote_spec_no_colon_raises():
    with pytest.raises(ValueError):
        _parse_remote_spec("just-a-host-no-path")


def test_parse_remote_spec_empty_host_raises():
    # ":/path" -> host_part is empty
    with pytest.raises(ValueError):
        _parse_remote_spec(":/path/to/file.jsonl")


def test_parse_remote_spec_empty_path_raises():
    with pytest.raises(ValueError):
        _parse_remote_spec("host:")


def test_parse_remote_spec_unclosed_bracket_raises():
    with pytest.raises(ValueError):
        _parse_remote_spec("user@[2001:db8::1:/path/file.jsonl")


def test_parse_remote_spec_bracket_no_path_colon_raises():
    # Closing bracket present but no ':' after it -> malformed.
    with pytest.raises(ValueError):
        _parse_remote_spec("user@[2001:db8::1]")


# ─────────────────────────────────────────────────────────────────────────────
# _strip_brackets
# ─────────────────────────────────────────────────────────────────────────────


def test_strip_brackets_with_user():
    assert _strip_brackets("user@[2001:db8::1]") == "user@2001:db8::1"


def test_strip_brackets_no_user():
    assert _strip_brackets("[::1]") == "::1"


def test_strip_brackets_plain_host_unchanged():
    assert _strip_brackets("host.example.org") == "host.example.org"


def test_strip_brackets_user_plain_host_unchanged():
    assert _strip_brackets("alice@host.example.org") == "alice@host.example.org"


# ─────────────────────────────────────────────────────────────────────────────
# _build_ssh_base
# ─────────────────────────────────────────────────────────────────────────────


def test_build_ssh_base_no_options():
    assert _build_ssh_base() == ["ssh"]


def test_build_ssh_base_with_config():
    assert _build_ssh_base(ssh_config="/home/me/.ssh/config") == [
        "ssh",
        "-F",
        "/home/me/.ssh/config",
    ]


def test_build_ssh_base_with_identity():
    assert _build_ssh_base(ssh_identity="/home/me/.ssh/id_ed25519") == [
        "ssh",
        "-i",
        "/home/me/.ssh/id_ed25519",
    ]


def test_build_ssh_base_with_config_and_identity_order():
    # Config flag is emitted before identity flag.
    assert _build_ssh_base(ssh_config="/cfg", ssh_identity="/key") == [
        "ssh",
        "-F",
        "/cfg",
        "-i",
        "/key",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# RemoteEngine construction
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def engine(tmp_path, monkeypatch):
    """A RemoteEngine whose temp dir lives under tmp_path (no real SSH).

    We redirect tempfile.mkdtemp so the engine's local cache files land under
    the test's tmp_path and get cleaned up automatically.
    """
    monkeypatch.setattr(
        remote.tempfile,
        "mkdtemp",
        lambda prefix="": str(tmp_path / "remote-cache"),
    )
    (tmp_path / "remote-cache").mkdir()
    eng = RemoteEngine("alice@host.example.org:/var/log/workflow-events.jsonl")
    return eng


def test_engine_init_parses_spec_and_paths(engine, tmp_path):
    assert engine._host == "alice@host.example.org"
    assert engine._remote_path == "/var/log/workflow-events.jsonl"
    assert engine._ssh_base == ["ssh"]
    assert engine._local_path.name == "workflow-events.jsonl"
    # Diagnostics sidecar path is derived from the remote path's parent.
    assert engine._diag_remote_path == "/var/log/diagnostics-events.jsonl"
    assert engine._diag_local_path.name == "diagnostics-events.jsonl"


def test_engine_init_default_state(engine):
    assert engine._info is None
    assert engine._wf_state == "UNKNOWN"
    assert engine._job_state == {}
    assert engine._processed_lines == 0
    assert engine._workflow_complete is False
    assert engine._pending_workflow_end is None


# ─────────────────────────────────────────────────────────────────────────────
# _process_header
# ─────────────────────────────────────────────────────────────────────────────


def test_process_header_populates_info(engine):
    engine._process_header(
        {
            "event_type": "workflow_start",
            "wf_uuid": "uuid-123",
            "dax_label": "diamond",
            "submit_dir": "/submit/run0001",
            "user": "alice",
            "planner_version": "5.1.2",
            "timestamp": "20260101T000000",
            "wf_start": 1000.0,
        }
    )
    info = engine._info
    assert isinstance(info, WorkflowInfo)
    assert info.wf_uuid == "uuid-123"
    assert info.dax_label == "diamond"
    assert info.submit_dir == Path("/submit/run0001")
    assert info.user == "alice"
    assert info.planner_version == "5.1.2"
    assert engine._header_wf_start == 1000.0


def test_process_header_defaults_for_missing_fields(engine):
    engine._process_header({"event_type": "workflow_start"})
    info = engine._info
    assert info.wf_uuid == "unknown"
    assert info.dax_label == "unknown"
    assert info.submit_dir == Path(".")
    assert engine._header_wf_start is None


# ─────────────────────────────────────────────────────────────────────────────
# _apply_event: workflow_start
# ─────────────────────────────────────────────────────────────────────────────


def test_apply_workflow_start_sets_info_once(engine):
    engine._apply_event(
        {"event_type": "workflow_start", "wf_uuid": "u1", "dax_label": "d1"}
    )
    assert engine._info is not None
    assert engine._info.wf_uuid == "u1"
    # A second workflow_start must NOT overwrite the existing header.
    engine._apply_event(
        {"event_type": "workflow_start", "wf_uuid": "u2", "dax_label": "d2"}
    )
    assert engine._info.wf_uuid == "u1"


# ─────────────────────────────────────────────────────────────────────────────
# _apply_event: workflow_state
# ─────────────────────────────────────────────────────────────────────────────


def test_apply_workflow_state_started(engine):
    engine._apply_event(
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_STARTED",
            "status": None,
            "wf_start": 500.0,
            "timestamp": 999.0,
        }
    )
    assert engine._wf_state == "WORKFLOW_STARTED"
    # Prefers wf_start (DB time) over the logger wall-clock timestamp.
    assert engine._wf_start == 500.0


def test_apply_workflow_state_started_falls_back_to_timestamp(engine):
    engine._apply_event(
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_STARTED",
            "timestamp": 777.0,
        }
    )
    assert engine._wf_start == 777.0


def test_apply_workflow_state_terminated_prefers_wf_end(engine):
    engine._wf_state = "WORKFLOW_STARTED"
    engine._apply_event(
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_TERMINATED",
            "status": 0,
            "wf_end": 2000.0,
            "timestamp": 2100.0,
        }
    )
    assert engine._wf_state == "WORKFLOW_TERMINATED"
    assert engine._wf_status == 0
    assert engine._wf_end == 2000.0


# ─────────────────────────────────────────────────────────────────────────────
# _apply_event: jobs_init
# ─────────────────────────────────────────────────────────────────────────────


def test_apply_jobs_init_seeds_job_state(engine):
    engine._apply_event(
        {
            "event_type": "jobs_init",
            "jobs": [
                {
                    "job_id": 1,
                    "exec_job_id": "preprocess_ID0000001",
                    "type_desc": "compute",
                    "transformation": "pegasus::preprocess",
                    "task_argv": "-a top",
                },
                {
                    "job_id": 2,
                    "exec_job_id": "create_dir_local",
                    "type_desc": "create-dir",
                },
            ],
        }
    )
    assert set(engine._job_state) == {1, 2}
    js1 = engine._job_state[1]
    assert js1["exec_job_id"] == "preprocess_ID0000001"
    assert js1["type_desc"] == "compute"
    assert js1["transformation"] == "pegasus::preprocess"
    assert js1["task_argv"] == "-a top"
    assert js1["raw_state"] is None
    assert engine._job_state[2]["type_desc"] == "create-dir"


def test_apply_jobs_init_does_not_clobber_existing(engine):
    engine._job_state[1] = {"exec_job_id": "EXISTING", "type_desc": "compute"}
    engine._apply_event(
        {
            "event_type": "jobs_init",
            "jobs": [{"job_id": 1, "exec_job_id": "NEW", "type_desc": "compute"}],
        }
    )
    # Existing entry preserved (jid already present).
    assert engine._job_state[1]["exec_job_id"] == "EXISTING"


def test_apply_jobs_init_skips_none_job_id(engine):
    engine._apply_event(
        {
            "event_type": "jobs_init",
            "jobs": [{"exec_job_id": "x", "type_desc": "compute"}],
        }
    )
    assert engine._job_state == {}


# ─────────────────────────────────────────────────────────────────────────────
# _apply_event: job_state
# ─────────────────────────────────────────────────────────────────────────────


def test_apply_job_state_creates_and_tracks_transitions(engine):
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 7,
            "exec_job_id": "analyze_ID0000007",
            "type_desc": "compute",
            "state": "SUBMIT",
            "timestamp": 100.0,
        }
    )
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 7,
            "state": "EXECUTE",
            "timestamp": 110.0,
        }
    )
    js = engine._job_state[7]
    assert js["exec_job_id"] == "analyze_ID0000007"
    assert js["raw_state"] == "EXECUTE"
    assert js["submit_time"] == 100.0
    assert js["start_time"] == 110.0
    assert js["end_time"] is None


def test_apply_job_state_terminal_sets_end_time(engine):
    engine._apply_event(
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 1.0}
    )
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_TERMINATED",
            "timestamp": 50.0,
        }
    )
    assert engine._job_state[1]["end_time"] == 50.0


def test_apply_job_state_exitcode_decoded(engine):
    # raw wait-status 256 -> real exit code 1 (256 >> 8).
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_FAILURE",
            "timestamp": 5.0,
            "exitcode": 256,
        }
    )
    assert engine._job_state[1]["exitcode"] == 1


def test_apply_job_state_captures_runtime_metadata(engine):
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "EXECUTE",
            "timestamp": 10.0,
            "stdout_file": "00/00/foo.out",
            "stderr_file": "00/00/foo.err",
            "maxrss": 51200,
        }
    )
    js = engine._job_state[1]
    assert js["stdout_file"] == "00/00/foo.out"
    assert js["stderr_file"] == "00/00/foo.err"
    assert js["maxrss"] == 51200


def test_apply_job_state_appends_recent_event(engine):
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 1,
            "exec_job_id": "preprocess_ID0000001",
            "type_desc": "compute",
            "state": "EXECUTE",
            "timestamp": 10.0,
        }
    )
    assert len(engine._recent_events) == 1
    ev = engine._recent_events[0]
    assert ev["exec_job_id"] == "preprocess_ID0000001"
    assert ev["state"] == "EXECUTE"
    assert ev["timestamp"] == 10.0


def test_recent_events_trimmed_to_events_n(tmp_path, monkeypatch):
    monkeypatch.setattr(
        remote.tempfile, "mkdtemp", lambda prefix="": str(tmp_path / "rc")
    )
    (tmp_path / "rc").mkdir()
    eng = RemoteEngine("h:/p/wf.jsonl", events_n=3)
    for i in range(10):
        eng._apply_event(
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": "EXECUTE",
                "timestamp": float(i),
            }
        )
    assert len(eng._recent_events) == 3
    # Kept the most recent three (timestamps 7, 8, 9).
    assert [e["timestamp"] for e in eng._recent_events] == [7.0, 8.0, 9.0]


# ─────────────────────────────────────────────────────────────────────────────
# _apply_event: htcondor_poll / htcondor_history / pool_status / workflow_stats
# ─────────────────────────────────────────────────────────────────────────────


def test_apply_htcondor_poll(engine, condor_queue_ads):
    engine._apply_event({"event_type": "htcondor_poll", "jobs": condor_queue_ads})
    assert engine._condor_jobs == condor_queue_ads


def test_apply_htcondor_history(engine, condor_history_ads):
    engine._apply_event({"event_type": "htcondor_history", "jobs": condor_history_ads})
    assert engine._condor_history == condor_history_ads


def test_apply_pool_status(engine):
    pool = {
        "total_slots": 8,
        "idle_slots": 3,
        "claimed_slots": 5,
        "machines": 2,
        "load_avg": 1.5,
        "os_arch": "LINUX/X86_64",
    }
    engine._apply_event({"event_type": "pool_status", "pool": pool})
    assert isinstance(engine._pool_status, PoolSummary)
    assert engine._pool_status.total_slots == 8
    assert engine._pool_status.idle_slots == 3
    assert engine._pool_status.os_arch == "LINUX/X86_64"


def test_apply_workflow_stats(engine):
    engine._apply_event(
        {
            "event_type": "workflow_stats",
            "stats": {"total_jobs": 5, "compute_jobs": 4, "succeeded": 4},
        }
    )
    assert isinstance(engine._workflow_stats, WorkflowStats)
    assert engine._workflow_stats.total_jobs == 5
    assert engine._workflow_stats.compute_jobs == 4
    assert engine._workflow_stats.succeeded == 4


# ─────────────────────────────────────────────────────────────────────────────
# _apply_event: workflow_end deferral + _finalize_batch
# ─────────────────────────────────────────────────────────────────────────────


def test_workflow_end_deferred_until_finalize(engine):
    end_ev = {
        "event_type": "workflow_end",
        "wf_state": "WORKFLOW_TERMINATED",
        "wf_status": 0,
        "wf_end": 3000.0,
        "timestamp": 3100.0,
    }
    engine._apply_event(end_ev)
    # Deferred — not yet marked complete.
    assert engine._pending_workflow_end is end_ev
    assert engine._workflow_complete is False

    engine._finalize_batch()
    assert engine._workflow_complete is True
    assert engine._wf_state == "WORKFLOW_TERMINATED"
    assert engine._wf_status == 0
    assert engine._wf_end == 3000.0
    assert engine._pending_workflow_end is None


def test_workflow_end_cleared_by_resume_event(engine):
    engine._apply_event(
        {
            "event_type": "workflow_end",
            "wf_state": "WORKFLOW_TERMINATED",
            "wf_status": 0,
            "wf_end": 3000.0,
        }
    )
    assert engine._pending_workflow_end is not None
    # A newer job_state event means the server resumed: cancel the pending end.
    engine._apply_event(
        {"event_type": "job_state", "job_id": 9, "state": "EXECUTE", "timestamp": 5.0}
    )
    assert engine._pending_workflow_end is None
    assert engine._workflow_complete is False

    # Finalize is now a no-op (nothing pending).
    engine._finalize_batch()
    assert engine._workflow_complete is False


def test_finalize_batch_noop_when_nothing_pending(engine):
    engine._finalize_batch()
    assert engine._workflow_complete is False
    assert engine._pending_workflow_end is None


def test_header_wf_start_used_when_no_state_start(engine):
    engine._header_wf_start = 42.0
    # A non-end, non-start event triggers the header-fallback assignment.
    engine._apply_event({"event_type": "htcondor_poll", "jobs": []})
    assert engine._wf_start == 42.0


# ─────────────────────────────────────────────────────────────────────────────
# _build_snapshot
# ─────────────────────────────────────────────────────────────────────────────


def test_build_snapshot_reconstructs_jobs_and_state(engine):
    engine._apply_event(
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_STARTED",
            "wf_start": 100.0,
        }
    )
    engine._apply_event(
        {
            "event_type": "jobs_init",
            "jobs": [
                {"job_id": 2, "exec_job_id": "b", "type_desc": "compute"},
                {"job_id": 1, "exec_job_id": "a", "type_desc": "compute"},
            ],
        }
    )
    engine._apply_event(
        {"event_type": "job_state", "job_id": 1, "state": "SUBMIT", "timestamp": 100.0}
    )
    engine._apply_event(
        {"event_type": "job_state", "job_id": 1, "state": "EXECUTE", "timestamp": 110.0}
    )
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_SUCCESS",
            "timestamp": 150.0,
        }
    )
    snap = engine._build_snapshot()
    assert isinstance(snap, WorkflowSnapshot)
    assert snap.wf_state == "WORKFLOW_STARTED"
    assert snap.wf_start == 100.0
    # Jobs are sorted by job_id.
    assert [j.job_id for j in snap.jobs] == [1, 2]
    j1 = snap.jobs[0]
    assert j1.exec_job_id == "a"
    assert j1.raw_state == "JOB_SUCCESS"
    assert j1.disp_state == "SUCCESS"
    assert j1.submit_time == 100.0
    assert j1.start_time == 110.0
    assert j1.end_time == 150.0


def test_build_snapshot_recent_events_reversed(engine):
    for i, state in enumerate(("SUBMIT", "EXECUTE", "JOB_SUCCESS")):
        engine._apply_event(
            {
                "event_type": "job_state",
                "job_id": 1,
                "state": state,
                "timestamp": float(i),
            }
        )
    snap = engine._build_snapshot()
    # recent_events is reversed so the newest transition is first.
    assert [e["state"] for e in snap.recent_events] == [
        "JOB_SUCCESS",
        "EXECUTE",
        "SUBMIT",
    ]


def test_build_snapshot_counts(engine, state_seqs):
    engine._apply_event(
        {
            "event_type": "jobs_init",
            "jobs": [
                {"job_id": 1, "exec_job_id": "a", "type_desc": "compute"},
                {"job_id": 2, "exec_job_id": "b", "type_desc": "compute"},
                {"job_id": 3, "exec_job_id": "c", "type_desc": "compute"},
            ],
        }
    )
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_SUCCESS",
            "timestamp": 1.0,
        }
    )
    engine._apply_event(
        {"event_type": "job_state", "job_id": 2, "state": "EXECUTE", "timestamp": 1.0}
    )
    engine._apply_event(
        {
            "event_type": "job_state",
            "job_id": 3,
            "state": "JOB_FAILURE",
            "timestamp": 1.0,
        }
    )
    snap = engine._build_snapshot()
    assert snap.total_jobs() == 3
    assert snap.done_count() == 1
    assert snap.running_count() == 1
    assert snap.failed_count() == 1


# ─────────────────────────────────────────────────────────────────────────────
# _load_new_events — incremental JSONL parsing from the local cache file
# ─────────────────────────────────────────────────────────────────────────────


def _write_jsonl(path: Path, records, append: bool = False) -> None:
    mode = "a" if append else "w"
    with open(path, mode) as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def test_load_new_events_empty_when_file_missing(engine):
    assert not engine._local_path.exists()
    assert engine._load_new_events() == []


def test_load_new_events_reads_then_incremental(engine):
    _write_jsonl(
        engine._local_path,
        [
            {"event_type": "workflow_start", "wf_uuid": "u1"},
            {"event_type": "job_state", "job_id": 1, "state": "SUBMIT"},
        ],
    )
    first = engine._load_new_events()
    assert len(first) == 2
    assert first[0]["event_type"] == "workflow_start"
    assert engine._processed_lines == 2

    # No new lines -> empty.
    assert engine._load_new_events() == []

    # Append one more line; only that new line is returned.
    _write_jsonl(
        engine._local_path,
        [{"event_type": "job_state", "job_id": 1, "state": "EXECUTE"}],
        append=True,
    )
    second = engine._load_new_events()
    assert len(second) == 1
    assert second[0]["state"] == "EXECUTE"
    assert engine._processed_lines == 3


def test_load_new_events_skips_blank_lines(engine):
    with open(engine._local_path, "w") as fh:
        fh.write('{"event_type": "a"}\n')
        fh.write("\n")
        fh.write("   \n")
        fh.write('{"event_type": "b"}\n')
    events = engine._load_new_events()
    assert [e["event_type"] for e in events] == ["a", "b"]
    # processed_lines counts ALL lines enumerated (including blanks).
    assert engine._processed_lines == 4


def test_load_new_events_partial_line_handled_gracefully(engine):
    # A trailing partial/garbage line raises JSONDecodeError, which is caught;
    # already-collected events before the bad line are returned.
    with open(engine._local_path, "w") as fh:
        fh.write('{"event_type": "a"}\n')
        fh.write("{not valid json\n")
    events = engine._load_new_events()
    assert [e["event_type"] for e in events] == ["a"]


# ─────────────────────────────────────────────────────────────────────────────
# _load_new_diag_events — diagnostics sidecar parsing
# ─────────────────────────────────────────────────────────────────────────────


def test_load_new_diag_events_missing_file_noop(engine):
    assert not engine._diag_local_path.exists()
    engine._load_new_diag_events()
    assert engine._diag_active_alerts == []


def test_load_new_diag_events_accumulates_alerts(engine):
    _write_jsonl(
        engine._diag_local_path,
        [
            {"event_type": "diag_start"},
            {"event_type": "stall_detected", "detail": "no progress"},
            {"event_type": "idle_diagnosis", "detail": "pool full"},
        ],
    )
    engine._load_new_diag_events()
    assert len(engine._diag_active_alerts) == 2
    assert engine._diag_active_alerts[0]["event_type"] == "stall_detected"
    assert engine._diag_active_alerts[1]["event_type"] == "idle_diagnosis"
    assert engine._diag_processed_lines == 3


def test_load_new_diag_events_stall_resolved_clears(engine):
    _write_jsonl(
        engine._diag_local_path,
        [
            {"event_type": "stall_detected"},
            {"event_type": "hold_diagnosis"},
        ],
    )
    engine._load_new_diag_events()
    assert len(engine._diag_active_alerts) == 2

    _write_jsonl(
        engine._diag_local_path,
        [{"event_type": "stall_resolved"}],
        append=True,
    )
    engine._load_new_diag_events()
    assert engine._diag_active_alerts == []


def test_load_new_diag_events_diag_start_resets(engine):
    _write_jsonl(
        engine._diag_local_path,
        [{"event_type": "failure_diagnosis"}],
    )
    engine._load_new_diag_events()
    assert len(engine._diag_active_alerts) == 1

    # A new daemon session (diag_start) resets accumulated alerts.
    _write_jsonl(
        engine._diag_local_path,
        [{"event_type": "diag_start"}],
        append=True,
    )
    engine._load_new_diag_events()
    assert engine._diag_active_alerts == []


def test_load_new_diag_events_ignores_bad_json(engine):
    with open(engine._diag_local_path, "w") as fh:
        fh.write('{"event_type": "stall_detected"}\n')
        fh.write("garbage{{{\n")
        fh.write('{"event_type": "hold_diagnosis"}\n')
    engine._load_new_diag_events()
    # Bad line skipped (continue), both valid alerts kept.
    assert [a["event_type"] for a in engine._diag_active_alerts] == [
        "stall_detected",
        "hold_diagnosis",
    ]
    assert engine._diag_processed_lines == 3


# ─────────────────────────────────────────────────────────────────────────────
# Integration: full event stream applied then reconstructed (no SSH)
# ─────────────────────────────────────────────────────────────────────────────


def test_full_stream_apply_and_finalize(engine):
    stream = [
        {
            "event_type": "workflow_start",
            "wf_uuid": "uuid-xyz",
            "dax_label": "diamond",
            "submit_dir": "/submit/run0001",
        },
        {
            "event_type": "workflow_state",
            "state": "WORKFLOW_STARTED",
            "wf_start": 1000.0,
        },
        {
            "event_type": "jobs_init",
            "jobs": [
                {"job_id": 1, "exec_job_id": "preprocess", "type_desc": "compute"}
            ],
        },
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "SUBMIT",
            "timestamp": 1000.0,
        },
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "EXECUTE",
            "timestamp": 1010.0,
        },
        {
            "event_type": "job_state",
            "job_id": 1,
            "state": "JOB_SUCCESS",
            "timestamp": 1050.0,
        },
        {
            "event_type": "workflow_stats",
            "stats": {"total_jobs": 1, "compute_jobs": 1, "succeeded": 1},
        },
        {
            "event_type": "workflow_end",
            "wf_state": "WORKFLOW_TERMINATED",
            "wf_status": 0,
            "wf_end": 1060.0,
        },
    ]
    for ev in stream:
        engine._apply_event(ev)
    engine._finalize_batch()

    assert engine._info.wf_uuid == "uuid-xyz"
    assert engine._workflow_complete is True

    snap = engine._build_snapshot()
    assert snap.is_complete is True
    assert snap.succeeded is True
    assert snap.wf_start == 1000.0
    assert snap.wf_end == 1060.0
    assert snap.done_count() == 1
    assert engine._workflow_stats.total_jobs == 1


# ─────────────────────────────────────────────────────────────────────────────
# _do_sync — drive via a mocked _fetch_file (no network), incremental + shrink
# ─────────────────────────────────────────────────────────────────────────────


def test_do_sync_advances_offset(engine, monkeypatch):
    calls = []

    def fake_fetch(host, remote_path, local_path, ssh_base, byte_offset=0):
        calls.append(byte_offset)
        return True, "", 128

    monkeypatch.setattr(remote, "_fetch_file", fake_fetch)
    ok, err = engine._do_sync()
    assert ok is True
    assert err == ""
    assert engine._remote_offset == 128
    assert calls == [0]


def test_do_sync_failure_keeps_offset(engine, monkeypatch):
    engine._remote_offset = 64

    def fake_fetch(host, remote_path, local_path, ssh_base, byte_offset=0):
        return False, "ssh boom", byte_offset

    monkeypatch.setattr(remote, "_fetch_file", fake_fetch)
    ok, err = engine._do_sync()
    assert ok is False
    assert err == "ssh boom"
    # Offset unchanged on failure.
    assert engine._remote_offset == 64


def test_do_sync_shrink_triggers_full_refetch(engine, monkeypatch):
    engine._remote_offset = 500
    engine._processed_lines = 9
    seq = iter([(True, "", 50), (True, "", 200)])

    def fake_fetch(host, remote_path, local_path, ssh_base, byte_offset=0):
        return next(seq)

    monkeypatch.setattr(remote, "_fetch_file", fake_fetch)
    ok, err = engine._do_sync()
    assert ok is True
    # File shrank (50 < 500) -> offset/processed reset and re-fetched from 0.
    assert engine._remote_offset == 200
    # After reset, processed_lines was zeroed before the recursive call.
    assert engine._processed_lines == 0


# ─────────────────────────────────────────────────────────────────────────────
# _cleanup — removes temp files, tolerant of missing
# ─────────────────────────────────────────────────────────────────────────────


def test_cleanup_removes_temp_files(engine):
    engine._local_path.write_text("data\n")
    engine._diag_local_path.write_text("data\n")
    engine._cleanup()
    assert not engine._local_path.exists()
    assert not engine._diag_local_path.exists()
    assert not Path(engine._tmpdir).exists()


def test_cleanup_tolerates_missing_files(engine):
    # Nothing written; cleanup must not raise.
    engine._cleanup()
    assert not Path(engine._tmpdir).exists()
