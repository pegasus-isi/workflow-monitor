"""Unit tests for :mod:`workflow_monitor.diagnostics`.

Covers hold-reason pattern matching (``diagnose_held_job`` /
``_HOLD_PATTERNS``), the kickstart-stderr analyzer (``_analyze_stderr`` /
``StderrAnalysis``), the stderr-driven diagnosis priority ladder
(``_diagnose_from_stderr``), exit-code-based failure diagnosis
(``diagnose_failed_job`` / ``_FAILURE_SUGGESTIONS``), kickstart YAML parsing
(``parse_kickstart_output``), and the aggregation entry point
(``collect_diagnostics``).

Tests avoid all network/subprocess access; kickstart files are written under
``tmp_path``.
"""

from __future__ import annotations

import textwrap

import pytest
import yaml

from workflow_monitor.diagnostics import (
    Diagnostic,
    KickstartInfo,
    StderrAnalysis,
    _analyze_stderr,
    _diagnose_from_stderr,
    _real_exitcode,
    collect_diagnostics,
    diagnose_failed_job,
    diagnose_held_job,
    parse_kickstart_output,
    _FAILURE_SUGGESTIONS,
    _HOLD_PATTERNS,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers to look up the canonical suggestion list for a given hold pattern.
# ─────────────────────────────────────────────────────────────────────────────


def _hold_entry_for(reason: str):
    """Return the first (summary, suggestions) in _HOLD_PATTERNS matching reason."""
    for pattern, summary, suggestions in _HOLD_PATTERNS:
        if pattern.search(reason):
            return summary, suggestions
    return None


# ─────────────────────────────────────────────────────────────────────────────
# diagnose_held_job — no hold reason
# ─────────────────────────────────────────────────────────────────────────────


def test_held_no_reason_generic(make_job):
    job = make_job(exec_job_id="analyze_ID0000004", raw_state="JOB_HELD")
    d = diagnose_held_job(job, hold_reason=None)

    assert isinstance(d, Diagnostic)
    assert d.severity == "held"
    assert d.job_name == "analyze_ID0000004"
    assert d.summary == "Job held by HTCondor"
    assert d.reason == "No hold reason available"
    # Default suggestion set (condor_q / condor_release / pegasus-analyzer).
    assert d.suggestions == [
        "Use `condor_q -hold <job_id>` to see the full hold reason",
        "Try `condor_release <job_id>` to release the job if the issue is resolved",
        "Run `pegasus-analyzer <submit_dir>` for Pegasus-level diagnostics",
    ]


def test_held_empty_string_reason_is_generic(make_job):
    """An empty hold_reason is falsy -> generic path, reason placeholder used."""
    job = make_job(raw_state="JOB_HELD")
    d = diagnose_held_job(job, hold_reason="")
    assert d.summary == "Job held by HTCondor"
    assert d.reason == "No hold reason available"


def test_held_unmatched_reason_keeps_generic_summary_but_real_reason(make_job):
    """A non-empty reason that matches no pattern keeps the generic summary."""
    job = make_job(raw_state="JOB_HELD")
    reason = "Something totally unrecognized happened on the worker"
    # Sanity: this really matches nothing.
    assert _hold_entry_for(reason) is None
    d = diagnose_held_job(job, hold_reason=reason)
    assert d.summary == "Job held by HTCondor"
    assert d.reason == reason
    # Generic suggestion list retained.
    assert d.suggestions[0].startswith("Use `condor_q -hold")


# ─────────────────────────────────────────────────────────────────────────────
# diagnose_held_job — every _HOLD_PATTERNS entry
# ─────────────────────────────────────────────────────────────────────────────


# (reason string, expected summary). Suggestions are asserted against the
# pattern's own list via _hold_entry_for to avoid duplicating them here.
_HOLD_CASES = [
    (
        "Error from slot1@w2: Transfer output files failure: writing to file "
        "/scratch/out.txt: No such file or directory",
        "Output file transfer failed — expected file not created",
    ),
    (
        "Error from slot1@w2: Transfer output files failure while uploading",
        "Output file transfer failed",
    ),
    (
        "Error from slot1@w2: Transfer input files failure: could not fetch f.in",
        "Input file transfer failed",
    ),
    (
        "Job has gone over memory limit of 2048 megabytes",
        "Job exceeded memory limit",
    ),
    (
        "MEMORY_EXCEEDED: cgroup OOM",
        "Job exceeded memory limit",
    ),
    (
        "Job has gone over disk usage limit",
        "Job exceeded disk limit",
    ),
    (
        "Job exceeded walltime allowance",
        "Job exceeded wall time limit",
    ),
    (
        "Cannot access myproxy server to refresh credential",
        "Credential or proxy issue",
    ),
    (
        "Error from starter: SHADOW_EXCEPTION encountered on submit host",
        "Condor shadow exception",
    ),
    (
        "UNRESOLVABLE: cannot resolve hostname worker3",
        "DNS or hostname resolution failure",
    ),
    (
        "docker: failed to pull image library/foo:latest",
        "Container runtime error",
    ),
    (
        "Job put on hold by periodic_hold expression in submit file",
        "Held by periodic hold policy",
    ),
]


@pytest.mark.parametrize("reason, expected_summary", _HOLD_CASES)
def test_held_pattern_summaries_and_suggestions(make_job, reason, expected_summary):
    job = make_job(exec_job_id="some_ID0000009", raw_state="JOB_HELD")
    d = diagnose_held_job(job, hold_reason=reason)

    assert d.severity == "held"
    assert d.job_name == "some_ID0000009"
    assert d.summary == expected_summary
    assert d.reason == reason

    entry = _hold_entry_for(reason)
    assert entry is not None, f"reason did not match any pattern: {reason!r}"
    matched_summary, matched_suggestions = entry
    assert matched_summary == expected_summary
    # The returned suggestions must be exactly that pattern's list.
    assert d.suggestions == matched_suggestions


def test_held_output_no_such_file_wins_over_generic_transfer_output(make_job):
    """Order matters: the more specific 'No such file or directory' variant
    must win over the generic 'Transfer output files failure' pattern."""
    reason = (
        "Error: Transfer output files failure: opening "
        "/scratch/missing.dat: No such file or directory"
    )
    job = make_job(raw_state="JOB_HELD")
    d = diagnose_held_job(job, hold_reason=reason)

    assert d.summary == "Output file transfer failed — expected file not created"
    # And NOT the generic summary.
    assert d.summary != "Output file transfer failed"

    # The specific pattern is listed before the generic one in _HOLD_PATTERNS.
    specific_idx = next(
        i
        for i, (p, *_rest) in enumerate(_HOLD_PATTERNS)
        if p.search("Transfer output files failure: No such file or directory")
    )
    generic_idx = next(
        i
        for i, (p, *_rest) in enumerate(_HOLD_PATTERNS)
        if p.pattern == "Transfer output files failure"
    )
    assert specific_idx < generic_idx


def test_held_case_insensitive(make_job):
    """_HOLD_PATTERNS are compiled with re.I."""
    job = make_job(raw_state="JOB_HELD")
    d = diagnose_held_job(job, hold_reason="TRANSFER INPUT FILES FAILURE: nope")
    assert d.summary == "Input file transfer failed"


# ─────────────────────────────────────────────────────────────────────────────
# _analyze_stderr / StderrAnalysis
# ─────────────────────────────────────────────────────────────────────────────


def test_analyze_stderr_empty_has_no_findings():
    a = _analyze_stderr("")
    assert a.missing_files == []
    assert a.transfer_attempts == 1  # default when no attempt lines
    assert a.transfers_failed is False
    assert a.integrity_error is False
    assert a.permission_denied == []
    assert a.no_space is False
    assert a.connection_error is False
    assert a.has_findings is False


def test_analyze_stderr_missing_files_deduped_order_preserved():
    stderr = textwrap.dedent(
        """\
        2026-06-24 PFN check
        Expected local file does not exist: /work/data/a.txt
        ... retrying ...
        Expected local file does not exist: /work/data/a.txt
        Expected local file does not exist: /work/data/b.txt
        """
    )
    a = _analyze_stderr(stderr)
    # Deduped, but order preserved (a before b, a not repeated).
    assert a.missing_files == ["/work/data/a.txt", "/work/data/b.txt"]
    assert a.has_findings is True


def test_analyze_stderr_transfer_attempts_max():
    stderr = (
        "Starting transfers - attempt 1\n"
        "Starting transfers - attempt 2\n"
        "Starting transfers - attempt 3\n"
    )
    a = _analyze_stderr(stderr)
    assert a.transfer_attempts == 3


def test_analyze_stderr_transfer_attempts_default_one_when_absent():
    a = _analyze_stderr("nothing interesting here")
    assert a.transfer_attempts == 1
    assert a.has_findings is False


def test_analyze_stderr_attempts_gt_one_alone_counts_as_findings():
    """transfer_attempts > 1 by itself flips has_findings to True."""
    a = _analyze_stderr("Starting transfers - attempt 2")
    assert a.transfer_attempts == 2
    assert a.missing_files == []
    assert a.transfers_failed is False
    assert a.has_findings is True


def test_analyze_stderr_transfers_failed_flag():
    a = _analyze_stderr("Some transfers failed; see log")
    assert a.transfers_failed is True
    assert a.has_findings is True


def test_analyze_stderr_integrity_error_flag():
    a = _analyze_stderr("pegasus-transfer: integrity verification failed for x")
    assert a.integrity_error is True
    assert a.has_findings is True


def test_analyze_stderr_no_space_flag():
    a = _analyze_stderr("write error: No space left on device")
    assert a.no_space is True
    assert a.has_findings is True


@pytest.mark.parametrize(
    "text",
    [
        "curl: Connection refused",
        "ssh: Connection timed out",
        "scp: Network is unreachable",
    ],
)
def test_analyze_stderr_connection_error_flag(text):
    a = _analyze_stderr(text)
    assert a.connection_error is True
    assert a.has_findings is True


def test_analyze_stderr_permission_denied_list():
    stderr = (
        "cp: Permission denied while copying: /restricted/out.dat\n"
        "rsync error: Permission denied opening: /locked/in.dat\n"
    )
    a = _analyze_stderr(stderr)
    assert a.permission_denied == ["/restricted/out.dat", "/locked/in.dat"]
    assert a.has_findings is True


# ─────────────────────────────────────────────────────────────────────────────
# _diagnose_from_stderr — priority ladder
# ─────────────────────────────────────────────────────────────────────────────


def _analysis(**overrides) -> StderrAnalysis:
    base = dict(
        missing_files=[],
        transfer_attempts=1,
        transfers_failed=False,
        integrity_error=False,
        permission_denied=[],
        no_space=False,
        connection_error=False,
    )
    base.update(overrides)
    return StderrAnalysis(**base)


def test_diagnose_from_stderr_none_returns_none_empty():
    assert _diagnose_from_stderr(None, 1) == (None, [])


def test_diagnose_from_stderr_no_findings_returns_none_empty():
    assert _diagnose_from_stderr(_analysis(), 1) == (None, [])


def test_diagnose_from_stderr_missing_files_top_priority():
    # All findings set at once; missing_files must win.
    a = _analysis(
        missing_files=["/work/a.txt"],
        transfers_failed=True,
        integrity_error=True,
        permission_denied=["/p"],
        no_space=True,
        connection_error=True,
    )
    summary, suggestions = _diagnose_from_stderr(a, 1)
    assert summary == "Input file missing"
    assert suggestions[0] == "Missing: /work/a.txt"
    # Tail guidance present.
    assert "Verify the file exists at the expected path" in suggestions
    assert any("pegasus-analyzer" in s for s in suggestions)


def test_diagnose_from_stderr_missing_files_with_retry_note():
    a = _analysis(missing_files=["/work/a.txt", "/work/b.txt"], transfer_attempts=3)
    summary, suggestions = _diagnose_from_stderr(a, 1)
    assert summary == "Input file missing (3 transfer attempts failed)"
    assert suggestions[0] == "Missing: /work/a.txt"
    assert suggestions[1] == "Missing: /work/b.txt"


def test_diagnose_from_stderr_transfers_failed_second_priority():
    a = _analysis(
        transfers_failed=True,
        integrity_error=True,
        permission_denied=["/p"],
        no_space=True,
        connection_error=True,
    )
    summary, suggestions = _diagnose_from_stderr(a, 1)
    assert summary == "File transfers failed"
    assert any("kickstart .out.000 stderr" in s for s in suggestions)


def test_diagnose_from_stderr_transfers_failed_retry_note():
    a = _analysis(transfers_failed=True, transfer_attempts=4)
    summary, _ = _diagnose_from_stderr(a, 1)
    assert summary == "File transfers failed after 4 attempts"


def test_diagnose_from_stderr_integrity_third_priority():
    a = _analysis(
        integrity_error=True,
        permission_denied=["/p"],
        no_space=True,
        connection_error=True,
    )
    summary, suggestions = _diagnose_from_stderr(a, 1)
    assert summary == "File integrity verification failed"
    assert any("checksum" in s for s in suggestions)


def test_diagnose_from_stderr_permission_fourth_priority():
    a = _analysis(
        permission_denied=["/restricted/a", "/restricted/b"],
        no_space=True,
        connection_error=True,
    )
    summary, suggestions = _diagnose_from_stderr(a, 1)
    assert summary == "Permission denied during file transfer"
    assert suggestions[0] == "Permission denied: /restricted/a"
    assert suggestions[1] == "Permission denied: /restricted/b"


def test_diagnose_from_stderr_no_space_fifth_priority():
    a = _analysis(no_space=True, connection_error=True)
    summary, suggestions = _diagnose_from_stderr(a, 1)
    assert summary == "No space left on device"
    assert any("disk space" in s for s in suggestions)


def test_diagnose_from_stderr_connection_last():
    a = _analysis(connection_error=True)
    summary, suggestions = _diagnose_from_stderr(a, 1)
    assert summary == "Network error during file transfer"
    assert any("network connectivity" in s for s in suggestions)


def test_diagnose_from_stderr_connection_retry_note():
    a = _analysis(connection_error=True, transfer_attempts=2)
    summary, _ = _diagnose_from_stderr(a, 1)
    assert summary == "Network error during file transfer after 2 attempts"


# ─────────────────────────────────────────────────────────────────────────────
# _real_exitcode
# ─────────────────────────────────────────────────────────────────────────────


def test_real_exitcode_kickstart_overrides_job():
    ks = KickstartInfo(exitcode=42)
    assert _real_exitcode(0, ks) == 42


def test_real_exitcode_falls_back_to_raw_conversion():
    # No kickstart: the raw wait status is decoded via db.real_exitcode.
    assert _real_exitcode(256, None) == 1  # exit(1) -> high byte
    assert _real_exitcode(139, None) == 139  # SIGSEGV+core -> 128 + signal
    assert _real_exitcode(9, None) == 137  # SIGKILL -> 128 + signal
    assert _real_exitcode(None, None) is None


def test_real_exitcode_kickstart_none_exitcode_uses_raw():
    # kickstart present but exitcode None -> fall back to decoding the raw
    # wait status (512 == exit(2)).
    ks = KickstartInfo(exitcode=None)
    assert _real_exitcode(512, ks) == 2


# ─────────────────────────────────────────────────────────────────────────────
# diagnose_failed_job — exit-code path (no kickstart)
# ─────────────────────────────────────────────────────────────────────────────


# Each known suggestion code, paired with a raw stampede wait-status that
# decodes to it. Normal exits live in the high byte (N << 8); signal kills are
# 128 + signal (here shown with the core-dump bit set, e.g. 137 == SIGKILL|core).
_CODE_TO_RAW = {
    1: 256,  # exit(1)
    2: 512,  # exit(2)
    126: 126 << 8,  # exit(126)
    127: 127 << 8,  # exit(127)
    137: 137,  # SIGKILL (+core)
    139: 139,  # SIGSEGV (+core)
}


@pytest.mark.parametrize("code", sorted(_CODE_TO_RAW))
def test_failed_known_exit_codes_use_their_suggestions(make_job, code):
    """Every known suggestion code is reachable directly from the raw
    stampede wait-status — including 137/139, which the old ``raw >> 8``
    decode silently collapsed to 0 (and thus to the generic suggestions).
    """
    job = make_job(exitcode=_CODE_TO_RAW[code], raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job)

    assert d.severity == "failed"
    assert d.summary == f"Job failed with exit code {code}"
    assert d.suggestions == _FAILURE_SUGGESTIONS[code]
    assert f"Exit code: {code}" in d.reason


@pytest.mark.parametrize("raw,code", [(9, 137), (11, 139)])
def test_failed_signal_without_core_maps_to_shell_code(make_job, raw, code):
    """A signal kill without a core dump (low byte 9/11) maps to 137/139 too."""
    job = make_job(exitcode=raw, raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job)
    assert d.summary == f"Job failed with exit code {code}"
    assert d.suggestions == _FAILURE_SUGGESTIONS[code]


def test_failed_unknown_exit_code_generic(make_job):
    job = make_job(exitcode=7 << 8, raw_state="JOB_FAILURE")  # exit(7), unmapped
    d = diagnose_failed_job(job)
    assert d.summary == "Job failed with exit code 7"
    # Unknown code -> the None-keyed generic suggestions.
    assert d.suggestions == _FAILURE_SUGGESTIONS[None]
    assert "Exit code: 7" in d.reason


def test_failed_none_exit_code(make_job):
    job = make_job(exitcode=None, raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job)
    assert d.summary == "Job failed (no exit code recorded)"
    assert d.suggestions == _FAILURE_SUGGESTIONS[None]
    assert "Exit code: None" in d.reason


def test_failed_job_name_is_exec_job_id(make_job):
    job = make_job(
        exec_job_id="findrange_ID0000002", exitcode=1, raw_state="JOB_FAILURE"
    )
    d = diagnose_failed_job(job)
    assert d.job_name == "findrange_ID0000002"


def test_failed_raw_waitstatus_converted(make_job):
    """A raw wait-status > 128 is converted before lookup (no kickstart)."""
    # raw 256 -> real 1 -> exit-code-1 suggestions.
    job = make_job(exitcode=256, raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job)
    assert d.summary == "Job failed with exit code 1"
    assert d.suggestions == _FAILURE_SUGGESTIONS[1]


# ─────────────────────────────────────────────────────────────────────────────
# diagnose_failed_job — kickstart / stderr-driven path
# ─────────────────────────────────────────────────────────────────────────────


def test_failed_stderr_driven_summary_wins(make_job):
    """When kickstart stderr has findings, the stderr summary beats exit-code."""
    analysis = _analysis(missing_files=["/work/missing.in"])
    ks = KickstartInfo(
        exitcode=1,
        stderr="Expected local file does not exist: /work/missing.in",
        stderr_analysis=analysis,
    )
    job = make_job(exitcode=1, raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job, kickstart=ks)
    assert d.summary == "Input file missing"
    assert d.suggestions[0] == "Missing: /work/missing.in"


def test_failed_kickstart_exitcode_overrides_job(make_job):
    """kickstart.exitcode overrides job.exitcode for the summary line."""
    ks = KickstartInfo(exitcode=127)  # no stderr findings
    job = make_job(exitcode=1, raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job, kickstart=ks)
    assert d.summary == "Job failed with exit code 127"
    assert d.suggestions == _FAILURE_SUGGESTIONS[127]
    assert "Exit code: 127" in d.reason


def test_failed_reason_includes_stdout_stderr_executable(make_job):
    ks = KickstartInfo(
        exitcode=1,
        stdout="hello world",
        stderr="boom happened",
        executable="/usr/bin/analyze",
    )
    job = make_job(exitcode=1, raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job, kickstart=ks)
    assert "Exit code: 1" in d.reason
    assert "stdout: hello world" in d.reason
    assert "stderr: boom happened" in d.reason
    assert "executable: /usr/bin/analyze" in d.reason
    assert " | " in d.reason


def test_failed_kickstart_no_findings_falls_back_to_exitcode(make_job):
    """Kickstart present but stderr_analysis has no findings -> exit-code path."""
    ks = KickstartInfo(exitcode=2, stderr_analysis=_analysis())
    job = make_job(exitcode=0, raw_state="JOB_FAILURE")
    d = diagnose_failed_job(job, kickstart=ks)
    assert d.summary == "Job failed with exit code 2"
    assert d.suggestions == _FAILURE_SUGGESTIONS[2]


# ─────────────────────────────────────────────────────────────────────────────
# parse_kickstart_output
# ─────────────────────────────────────────────────────────────────────────────


def _write_kickstart(path, doc):
    """Serialize a kickstart doc (list-of-one-dict) to a .out file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(doc, sort_keys=False))


def _kickstart_doc(
    *,
    transformation="pegasus::analyze",
    exitcode=0,
    executable="/usr/bin/analyze",
    argv=None,
    duration=12.5,
    stdout="run ok",
    stderr="",
):
    return [
        {
            "transformation": transformation,
            "mainjob": {
                "status": {"regular_exitcode": exitcode},
                "executable": {"file_name": executable},
                "argument_vector": argv if argv is not None else ["-i", "in.dat"],
                "duration": duration,
            },
            "files": {
                "stdout": {"data": stdout},
                "stderr": {"data": stderr},
            },
        }
    ]


def test_parse_kickstart_missing_file_returns_none(tmp_path):
    assert parse_kickstart_output(tmp_path, "nope_ID0000001") is None


def test_parse_kickstart_default_path(tmp_path):
    exec_id = "analyze_ID0000004"
    out = tmp_path / "00" / "00" / f"{exec_id}.out.000"
    _write_kickstart(out, _kickstart_doc(exitcode=0, duration=12.5))

    info = parse_kickstart_output(tmp_path, exec_id)
    assert info is not None
    assert isinstance(info, KickstartInfo)
    assert info.exitcode == 0
    assert info.transformation == "pegasus::analyze"
    assert info.executable == "/usr/bin/analyze"
    assert info.argv == ["-i", "in.dat"]
    assert info.duration == 12.5
    assert info.stdout == "run ok"
    assert info.stderr == ""
    # No stderr -> no analysis object.
    assert info.stderr_analysis is None


def test_parse_kickstart_explicit_stdout_file(tmp_path):
    rel = "custom/loc/job.out.000"
    out = tmp_path / rel
    _write_kickstart(out, _kickstart_doc(exitcode=2, executable="/bin/foo"))

    info = parse_kickstart_output(tmp_path, "anything", stdout_file=rel)
    assert info is not None
    assert info.exitcode == 2
    assert info.executable == "/bin/foo"


def test_parse_kickstart_populates_stderr_analysis(tmp_path):
    stderr = (
        "Starting transfers - attempt 1\n"
        "Starting transfers - attempt 2\n"
        "Expected local file does not exist: /work/in.dat\n"
        "Some transfers failed\n"
    )
    out = tmp_path / "00" / "00" / "stage_in.out.000"
    _write_kickstart(out, _kickstart_doc(exitcode=1, stderr=stderr))

    info = parse_kickstart_output(tmp_path, "stage_in")
    assert info is not None
    assert info.stderr_analysis is not None
    assert info.stderr_analysis.missing_files == ["/work/in.dat"]
    assert info.stderr_analysis.transfer_attempts == 2
    assert info.stderr_analysis.transfers_failed is True
    assert info.stderr_analysis.has_findings is True


def test_parse_kickstart_strips_whitespace(tmp_path):
    out = tmp_path / "00" / "00" / "j.out.000"
    _write_kickstart(out, _kickstart_doc(stdout="  padded  \n", stderr="\n  err \n"))
    info = parse_kickstart_output(tmp_path, "j")
    assert info.stdout == "padded"
    assert info.stderr == "err"


def test_parse_kickstart_malformed_yaml_returns_none(tmp_path):
    out = tmp_path / "00" / "00" / "bad.out.000"
    out.parent.mkdir(parents=True, exist_ok=True)
    # Unbalanced bracket -> yaml.safe_load raises -> None.
    out.write_text("{ this: [is, : not valid yaml }\n:::")
    assert parse_kickstart_output(tmp_path, "bad") is None


def test_parse_kickstart_non_dict_scalar_returns_none(tmp_path):
    out = tmp_path / "00" / "00" / "scalar.out.000"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("just a plain string\n")
    assert parse_kickstart_output(tmp_path, "scalar") is None


def test_parse_kickstart_empty_list_returns_none(tmp_path):
    out = tmp_path / "00" / "00" / "emptylist.out.000"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("[]\n")
    # Empty list -> data becomes {} (falsy dict) -> still a dict, parsed with
    # defaults. exitcode missing -> None.
    info = parse_kickstart_output(tmp_path, "emptylist")
    assert info is not None
    assert info.exitcode is None
    assert info.transformation == ""
    assert info.argv == []


def test_parse_kickstart_list_of_non_dict_returns_none(tmp_path):
    out = tmp_path / "00" / "00" / "listscalar.out.000"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("- just a string\n")
    assert parse_kickstart_output(tmp_path, "listscalar") is None


# ─────────────────────────────────────────────────────────────────────────────
# collect_diagnostics
# ─────────────────────────────────────────────────────────────────────────────


def test_collect_empty_returns_empty():
    assert collect_diagnostics([], []) == []


def test_collect_held_then_failed_order(make_job):
    held = [
        make_job(exec_job_id="h1", raw_state="JOB_HELD"),
        make_job(exec_job_id="h2", raw_state="JOB_HELD"),
    ]
    failed = [make_job(exec_job_id="f1", exitcode=256, raw_state="JOB_FAILURE")]

    out = collect_diagnostics(held, failed)
    assert len(out) == len(held) + len(failed)
    assert [d.job_name for d in out] == ["h1", "h2", "f1"]
    assert [d.severity for d in out] == ["held", "held", "failed"]


def test_collect_maps_hold_reason_by_dagnodename(make_job):
    held = [make_job(exec_job_id="analyze_ID0000004", raw_state="JOB_HELD")]
    condor_jobs = [
        {
            "DAGNodeName": "analyze_ID0000004",
            "HoldReason": "Transfer input files failure: missing f.in",
        },
        # Unrelated node — should be ignored.
        {"DAGNodeName": "other", "HoldReason": "Job exceeded memory limit"},
    ]
    out = collect_diagnostics(held, [], condor_jobs=condor_jobs)
    assert len(out) == 1
    assert out[0].summary == "Input file transfer failed"
    assert out[0].reason == "Transfer input files failure: missing f.in"


def test_collect_held_no_matching_condor_job_is_generic(make_job):
    held = [make_job(exec_job_id="unmatched_ID0000009", raw_state="JOB_HELD")]
    condor_jobs = [{"DAGNodeName": "different", "HoldReason": "memory limit"}]
    out = collect_diagnostics(held, [], condor_jobs=condor_jobs)
    assert out[0].summary == "Job held by HTCondor"
    assert out[0].reason == "No hold reason available"


def test_collect_condor_jobs_missing_fields_skipped(make_job):
    """condor entries without both DAGNodeName and HoldReason are skipped."""
    held = [make_job(exec_job_id="j1", raw_state="JOB_HELD")]
    condor_jobs = [
        {"DAGNodeName": "j1"},  # no HoldReason
        {"HoldReason": "memory limit"},  # no DAGNodeName
        {"DAGNodeName": "", "HoldReason": "memory limit"},  # empty node
    ]
    out = collect_diagnostics(held, [], condor_jobs=condor_jobs)
    assert out[0].summary == "Job held by HTCondor"


def test_collect_failed_without_submit_dir_no_kickstart(make_job):
    failed = [make_job(exec_job_id="f1", exitcode=127 << 8, raw_state="JOB_FAILURE")]
    out = collect_diagnostics([], failed)  # submit_dir=None -> no kickstart
    assert len(out) == 1
    assert out[0].summary == "Job failed with exit code 127"
    assert out[0].suggestions == _FAILURE_SUGGESTIONS[127]


def test_collect_failed_parses_kickstart_when_submit_dir_given(tmp_path, make_job):
    exec_id = "stage_in_ID0000007"
    stderr = "Expected local file does not exist: /work/in.dat\n"
    out_file = tmp_path / "00" / "00" / f"{exec_id}.out.000"
    _write_kickstart(out_file, _kickstart_doc(exitcode=1, stderr=stderr))

    failed = [make_job(exec_job_id=exec_id, exitcode=1, raw_state="JOB_FAILURE")]
    out = collect_diagnostics([], failed, submit_dir=tmp_path)
    assert len(out) == 1
    assert out[0].severity == "failed"
    assert out[0].summary == "Input file missing"
    assert out[0].suggestions[0] == "Missing: /work/in.dat"


def test_collect_failed_uses_job_stdout_file(tmp_path, make_job):
    """When submit_dir is set, the job's stdout_file is passed through to the
    kickstart parser (explicit relative path)."""
    rel = "alt/place/out.000"
    out_file = tmp_path / rel
    _write_kickstart(out_file, _kickstart_doc(exitcode=2))

    failed = [
        make_job(
            exec_job_id="whatever",
            exitcode=99,  # would map to generic, but kickstart overrides
            raw_state="JOB_FAILURE",
            stdout_file=rel,
        )
    ]
    out = collect_diagnostics([], failed, submit_dir=tmp_path)
    assert out[0].summary == "Job failed with exit code 2"


def test_collect_mixed_held_and_failed_with_submit_dir(tmp_path, make_job):
    held = [make_job(exec_job_id="analyze_ID0000004", raw_state="JOB_HELD")]
    failed = [make_job(exec_job_id="f1", exitcode=256, raw_state="JOB_FAILURE")]
    condor_jobs = [
        {"DAGNodeName": "analyze_ID0000004", "HoldReason": "Job exceeded memory limit"}
    ]
    out = collect_diagnostics(
        held, failed, condor_jobs=condor_jobs, submit_dir=tmp_path
    )
    assert len(out) == 2
    assert out[0].severity == "held"
    assert out[0].summary == "Job exceeded memory limit"
    assert out[1].severity == "failed"
    # f1 has no kickstart file -> exit-code path.
    assert out[1].summary == "Job failed with exit code 1"
