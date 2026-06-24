"""Unit tests for :mod:`workflow_monitor.diag_log`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from workflow_monitor.diag_log import DIAG_SCHEMA_VERSION, DiagnosticLogger


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────


def _read_lines(path: Path) -> list[dict]:
    """Parse a JSONL file into a list of dicts (one per non-empty line)."""
    text = path.read_text()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


@pytest.fixture
def frozen_time(monkeypatch):
    """Freeze time.time() inside diag_log at a known value."""
    import workflow_monitor.diag_log as mod

    clock = {"t": 1000.0}
    monkeypatch.setattr(mod.time, "time", lambda: clock["t"])
    return clock


# ─────────────────────────────────────────────────────────────────────────────
# construction -> diag_start
# ─────────────────────────────────────────────────────────────────────────────


def test_construction_writes_diag_start(tmp_path, frozen_time):
    log = tmp_path / "diagnostics-events.jsonl"
    DiagnosticLogger("wf-1", log)

    lines = _read_lines(log)
    assert len(lines) == 1
    start = lines[0]
    assert start["event_type"] == "diag_start"
    assert start["wf_uuid"] == "wf-1"
    assert start["schema_version"] == DIAG_SCHEMA_VERSION
    assert start["timestamp"] == 1000.0


def test_construction_engine_config_none_becomes_empty_dict(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    DiagnosticLogger("wf-1", log, engine_config=None)
    start = _read_lines(log)[0]
    assert start["engine_config"] == {}


def test_construction_echoes_engine_config(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    cfg = {"interval": 5, "heuristics": ["stall", "hold"]}
    DiagnosticLogger("wf-1", log, engine_config=cfg)
    start = _read_lines(log)[0]
    assert start["engine_config"] == cfg


def test_construction_appends_to_existing_file(tmp_path, frozen_time):
    """The file is opened in append mode; prior content is preserved."""
    log = tmp_path / "d.jsonl"
    log.write_text('{"event_type": "preexisting"}\n')
    DiagnosticLogger("wf-1", log)
    lines = _read_lines(log)
    assert len(lines) == 2
    assert lines[0]["event_type"] == "preexisting"
    assert lines[1]["event_type"] == "diag_start"


# ─────────────────────────────────────────────────────────────────────────────
# emit
# ─────────────────────────────────────────────────────────────────────────────


def test_emit_appends_and_flushes_without_close(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    logger.emit({"event_type": "stall", "node": "analyze"})

    # No close() called -> content must already be visible thanks to flush().
    lines = _read_lines(log)
    assert len(lines) == 2
    ev = lines[1]
    assert ev["event_type"] == "stall"
    assert ev["node"] == "analyze"


def test_emit_sets_default_wf_uuid_and_timestamp(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-default", log)
    logger.emit({"event_type": "stall"})
    ev = _read_lines(log)[1]
    assert ev["wf_uuid"] == "wf-default"
    assert ev["timestamp"] == 1000.0


def test_emit_does_not_override_provided_wf_uuid_and_timestamp(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-default", log)
    logger.emit({"event_type": "stall", "wf_uuid": "other-wf", "timestamp": 42.0})
    ev = _read_lines(log)[1]
    assert ev["wf_uuid"] == "other-wf"
    assert ev["timestamp"] == 42.0


def test_emit_multiple_preserves_order(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    for i in range(3):
        logger.emit({"event_type": "tick", "n": i})
    lines = _read_lines(log)
    assert [l["event_type"] for l in lines] == ["diag_start", "tick", "tick", "tick"]
    assert [l["n"] for l in lines[1:]] == [0, 1, 2]


def test_emit_mutates_caller_dict_in_place(tmp_path, frozen_time):
    """emit uses setdefault on the passed dict, so it mutates the caller's dict.

    This documents the actual (side-effecting) behavior of emit().
    """
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    event = {"event_type": "stall"}
    logger.emit(event)
    assert event["wf_uuid"] == "wf-1"
    assert event["timestamp"] == 1000.0


# ─────────────────────────────────────────────────────────────────────────────
# _emit_raw json serialization
# ─────────────────────────────────────────────────────────────────────────────


def test_emit_serializes_path_via_default_str(tmp_path, frozen_time):
    """Non-JSON-native values (e.g. Path) are stringified, not raised."""
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    some_path = Path("/work/run0001/analyze.err")
    logger.emit({"event_type": "stall", "stderr": some_path})
    ev = _read_lines(log)[1]
    assert ev["stderr"] == str(some_path)
    assert isinstance(ev["stderr"], str)


def test_emit_serializes_set_via_default_str(tmp_path, frozen_time):
    """A set is also non-native; default=str stringifies it instead of raising."""
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    logger.emit({"event_type": "stall", "codes": {12}})
    ev = _read_lines(log)[1]
    # repr of the set, as a string.
    assert ev["codes"] == str({12})


# ─────────────────────────────────────────────────────────────────────────────
# close -> diag_end
# ─────────────────────────────────────────────────────────────────────────────


def test_close_writes_diag_end_as_last_line(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-9", log)
    logger.emit({"event_type": "stall"})
    logger.close()

    lines = _read_lines(log)
    assert [l["event_type"] for l in lines] == ["diag_start", "stall", "diag_end"]
    end = lines[-1]
    assert end["wf_uuid"] == "wf-9"
    assert end["timestamp"] == 1000.0


def test_close_with_no_events(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    logger.close()
    lines = _read_lines(log)
    assert [l["event_type"] for l in lines] == ["diag_start", "diag_end"]


def test_close_closes_file_handle(tmp_path, frozen_time):
    log = tmp_path / "d.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    logger.close()
    # The underlying handle is closed; we don't write post-close, just verify state.
    assert logger._fh.closed is True


# ─────────────────────────────────────────────────────────────────────────────
# path property & path_from_event_log
# ─────────────────────────────────────────────────────────────────────────────


def test_path_property_returns_log_path(tmp_path, frozen_time):
    log = tmp_path / "diagnostics-events.jsonl"
    logger = DiagnosticLogger("wf-1", log)
    assert logger.path == log


@pytest.mark.parametrize(
    "event_log",
    [
        "/var/run/wf/workflow-events.jsonl",
        "/home/user/runs/run0001/events.jsonl",
        "relative/events.jsonl",
    ],
)
def test_path_from_event_log_derives_sibling(event_log):
    p = Path(event_log)
    derived = DiagnosticLogger.path_from_event_log(p)
    assert derived == p.parent / "diagnostics-events.jsonl"
    assert derived.name == "diagnostics-events.jsonl"
    assert derived.parent == p.parent


def test_path_from_event_log_is_static(tmp_path):
    """Callable without an instance."""
    p = tmp_path / "sub" / "workflow-events.jsonl"
    derived = DiagnosticLogger.path_from_event_log(p)
    assert derived == tmp_path / "sub" / "diagnostics-events.jsonl"
