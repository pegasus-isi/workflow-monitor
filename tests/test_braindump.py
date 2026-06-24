"""Unit tests for :mod:`workflow_monitor.braindump`."""

from __future__ import annotations

from pathlib import Path

import pytest

from workflow_monitor.braindump import (
    WorkflowInfo,
    find_braindump,
    load_braindump,
)


# ─────────────────────────────────────────────────────────────────────────────
# WorkflowInfo dataclass
# ─────────────────────────────────────────────────────────────────────────────


def _info(submit_dir: Path, **overrides) -> WorkflowInfo:
    """Build a WorkflowInfo with sensible defaults rooted at *submit_dir*."""
    kwargs = dict(
        wf_uuid="u",
        root_wf_uuid="u",
        dax_label="diamond",
        submit_dir=Path(submit_dir),
        user="tester",
        planner_version="5.1.2",
        dag_file="diamond-0.dag",
        condor_log="diamond-0.log",
        timestamp="20260101T000000+0000",
        basedir=Path(submit_dir).parent,
    )
    kwargs.update(overrides)
    return WorkflowInfo(**kwargs)


def test_post_init_recorded_submit_dir_defaults_to_submit_dir(tmp_path):
    info = _info(tmp_path)
    assert info.recorded_submit_dir == info.submit_dir == tmp_path


def test_post_init_recorded_submit_dir_preserved_when_provided(tmp_path):
    recorded = Path("/in/container/run0001")
    info = _info(tmp_path, recorded_submit_dir=recorded)
    assert info.recorded_submit_dir == recorded
    assert info.submit_dir == tmp_path
    assert info.recorded_submit_dir != info.submit_dir


def test_stampede_db_returns_path_only_when_file_exists(tmp_path):
    info = _info(tmp_path)
    # dag_file "diamond-0.dag" -> stem "diamond-0" -> "diamond-0.stampede.db"
    assert info.stampede_db is None

    db = tmp_path / "diamond-0.stampede.db"
    db.write_text("")
    assert info.stampede_db == db


def test_stampede_db_stem_strips_only_dag_suffix(tmp_path):
    # ".dag" is replaced everywhere via str.replace; verify the common case.
    info = _info(tmp_path, dag_file="my-workflow.dag")
    expected = tmp_path / "my-workflow.stampede.db"
    assert info.stampede_db is None
    expected.write_text("")
    assert info.stampede_db == expected


def test_jobstate_log_existence_gated(tmp_path):
    info = _info(tmp_path)
    assert info.jobstate_log is None
    log = tmp_path / "jobstate.log"
    log.write_text("")
    assert info.jobstate_log == log


def test_condor_log_path_existence_gated(tmp_path):
    info = _info(tmp_path)
    assert info.condor_log_path is None
    clog = tmp_path / "diamond-0.log"
    clog.write_text("")
    assert info.condor_log_path == clog


def test_dag_path_is_unconditional(tmp_path):
    """dag_path is NOT existence-gated; it always returns the joined path."""
    info = _info(tmp_path)
    assert info.dag_path == tmp_path / "diamond-0.dag"
    # Still returns the path even though the file does not exist.
    assert not info.dag_path.exists()


def test_dagman_out_path_existence_gated(tmp_path):
    info = _info(tmp_path)
    assert info.dagman_out_path is None
    out = tmp_path / "diamond-0.dag.dagman.out"
    out.write_text("")
    assert info.dagman_out_path == out


# ─────────────────────────────────────────────────────────────────────────────
# find_braindump
# ─────────────────────────────────────────────────────────────────────────────


def test_find_braindump_given_file_directly(tmp_path, write_braindump):
    bd = write_braindump(tmp_path)
    assert find_braindump(bd) == bd.resolve()


def test_find_braindump_given_containing_dir(tmp_path, write_braindump):
    bd = write_braindump(tmp_path)
    found = find_braindump(tmp_path)
    assert found == bd
    assert found.name == "braindump.yml"


def test_find_braindump_base_dir_picks_last_run(tmp_path, write_braindump):
    base = tmp_path / "submit"
    run1 = base / "run0001"
    run2 = base / "run0002"
    bd1 = write_braindump(run1, submit_dir=str(run1))
    bd2 = write_braindump(run2, submit_dir=str(run2))

    found = find_braindump(base)
    # sorted(rglob(...))[-1] -> run0002 sorts after run0001.
    assert found == bd2
    assert found != bd1
    assert "run0002" in str(found)


def test_find_braindump_returns_none_when_absent(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    assert find_braindump(empty) is None


def test_find_braindump_non_braindump_file_searches_parent(tmp_path, write_braindump):
    """A file that isn't braindump.yml falls through to the rglob branch."""
    bd = write_braindump(tmp_path)
    other = tmp_path / "notes.txt"
    other.write_text("hi")
    # path.is_file() is True but name != braindump.yml, and `path / "braindump.yml"`
    # is `notes.txt/braindump.yml` (does not exist), so rglob over the file path
    # finds nothing -> None. This documents the actual behavior.
    assert find_braindump(other) is None
    assert bd.exists()  # sanity: a braindump did exist in the parent dir


# ─────────────────────────────────────────────────────────────────────────────
# load_braindump — validation & errors
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("bad", ["sometimes", "REMAP", "", "auto ", "Always"])
def test_load_braindump_invalid_remap_raises(tmp_path, write_braindump, bad):
    write_braindump(tmp_path)
    with pytest.raises(ValueError, match="remap must be"):
        load_braindump(tmp_path, remap=bad)


def test_load_braindump_missing_raises_filenotfound(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError, match="Cannot find braindump.yml"):
        load_braindump(empty)


@pytest.mark.parametrize("remap", ["auto", "always", "never"])
def test_load_braindump_valid_remap_values_accepted(tmp_path, write_braindump, remap):
    write_braindump(tmp_path)
    info = load_braindump(tmp_path, remap=remap)
    assert isinstance(info, WorkflowInfo)


# ─────────────────────────────────────────────────────────────────────────────
# load_braindump — remap="never"
# ─────────────────────────────────────────────────────────────────────────────


def test_load_braindump_never_trusts_recorded_verbatim(tmp_path, write_braindump):
    recorded = "/nonexist/base/run0001"
    write_braindump(tmp_path, submit_dir=recorded, basedir="/nonexist/base")
    info = load_braindump(tmp_path, remap="never")
    assert info.submit_dir == Path(recorded)
    assert info.basedir == Path("/nonexist/base")
    assert info.recorded_submit_dir == Path(recorded)


def test_load_braindump_never_basedir_defaults_to_recorded_parent(
    tmp_path, write_braindump
):
    """With no basedir key, recorded_basedir falls back to submit_dir.parent."""
    recorded = "/nonexist/base/run0001"
    bd = write_braindump(tmp_path, submit_dir=recorded)
    # Strip the basedir key that the fixture adds by default.
    import yaml

    data = yaml.safe_load(bd.read_text())
    del data["basedir"]
    bd.write_text(yaml.safe_dump(data))

    info = load_braindump(tmp_path, remap="never")
    assert info.submit_dir == Path(recorded)
    assert info.basedir == Path("/nonexist/base")


# ─────────────────────────────────────────────────────────────────────────────
# load_braindump — remap="always"
# ─────────────────────────────────────────────────────────────────────────────


def test_load_braindump_always_rebases_to_braindump_parent(tmp_path, write_braindump):
    somedir = tmp_path / "somedir"
    write_braindump(
        somedir,
        submit_dir="/nonexist/base/run0001",
        basedir="/nonexist/base",
    )
    info = load_braindump(somedir, remap="always")
    assert info.submit_dir == somedir.resolve()
    # offset run0001 has depth 1 -> basedir is actual_submit.parents[0] == parent.
    assert info.basedir == somedir.resolve().parent
    # recorded path stays verbatim.
    assert info.recorded_submit_dir == Path("/nonexist/base/run0001")


def test_load_braindump_always_preserves_multi_level_offset(tmp_path, write_braindump):
    """A deeper recorded offset rebases basedir to the equivalent ancestor."""
    deep = tmp_path / "a" / "b" / "c" / "d"
    write_braindump(
        deep,
        # recorded offset basedir->submit has depth 3 (x/y/z)
        submit_dir="/container/work/x/y/z",
        basedir="/container/work",
    )
    info = load_braindump(deep, remap="always")
    actual = deep.resolve()
    assert info.submit_dir == actual
    # depth 3 -> actual_submit.parents[2] == .../a/b
    assert info.basedir == actual.parents[2]


def test_load_braindump_always_offset_too_deep_falls_back_to_parent(
    tmp_path, write_braindump
):
    """When recorded depth exceeds available parents, basedir = actual.parent."""
    shallow = tmp_path / "only"
    # A huge recorded offset depth that can't fit shallow's parents.
    write_braindump(
        shallow,
        submit_dir="/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z",
        basedir="/a",
    )
    info = load_braindump(shallow, remap="always")
    actual = shallow.resolve()
    assert info.submit_dir == actual
    assert info.basedir == actual.parent


def test_load_braindump_always_unrelated_basedir_falls_back_to_parent(
    tmp_path, write_braindump
):
    """submit_dir not under basedir -> relative_to raises -> basedir = parent."""
    somedir = tmp_path / "somedir"
    write_braindump(
        somedir,
        submit_dir="/one/place/run0001",
        basedir="/totally/different/tree",
    )
    info = load_braindump(somedir, remap="always")
    actual = somedir.resolve()
    assert info.submit_dir == actual
    assert info.basedir == actual.parent


# ─────────────────────────────────────────────────────────────────────────────
# load_braindump — remap="auto" (default)
# ─────────────────────────────────────────────────────────────────────────────


def test_load_braindump_auto_is_default(tmp_path, write_braindump):
    """auto rebases a non-existent recorded path (default behavior)."""
    somedir = tmp_path / "somedir"
    write_braindump(
        somedir,
        submit_dir="/nonexist/base/run0001",
        basedir="/nonexist/base",
    )
    info = load_braindump(somedir)  # no remap arg -> "auto"
    assert info.submit_dir == somedir.resolve()
    assert info.basedir == somedir.resolve().parent


def test_load_braindump_auto_trusts_existing_recorded_path(tmp_path, write_braindump):
    """When recorded submit_dir EXISTS locally, auto trusts it verbatim."""
    real_submit = tmp_path / "real_submit"
    real_submit.mkdir()
    real_base = tmp_path / "real_base"
    real_base.mkdir()
    # The braindump lives somewhere else entirely.
    somedir = tmp_path / "elsewhere"
    write_braindump(
        somedir,
        submit_dir=str(real_submit),
        basedir=str(real_base),
    )
    info = load_braindump(somedir, remap="auto")
    # recorded path exists -> NOT remapped.
    assert info.submit_dir == real_submit
    assert info.basedir == real_base
    assert info.submit_dir != somedir.resolve()


def test_load_braindump_auto_rebases_missing_recorded_path(tmp_path, write_braindump):
    """When recorded submit_dir is MISSING, auto rebases onto braindump parent."""
    somedir = tmp_path / "somedir"
    write_braindump(
        somedir,
        submit_dir="/does/not/exist/run0007",
        basedir="/does/not/exist",
    )
    info = load_braindump(somedir, remap="auto")
    assert info.submit_dir == somedir.resolve()
    assert info.basedir == somedir.resolve().parent
    assert info.recorded_submit_dir == Path("/does/not/exist/run0007")


# ─────────────────────────────────────────────────────────────────────────────
# load_braindump — field mapping & defaults
# ─────────────────────────────────────────────────────────────────────────────


def test_load_braindump_maps_all_fields(tmp_path, write_braindump):
    write_braindump(
        tmp_path,
        wf_uuid="abc-123",
        root_wf_uuid="root-999",
        dax_label="mydax",
        user="alice",
        planner_version="5.1.3",
        dag="mydax-0.dag",
        condor_log="mydax-0.log",
        timestamp="20260624T120000+0000",
    )
    info = load_braindump(tmp_path, remap="never")
    assert info.wf_uuid == "abc-123"
    assert info.root_wf_uuid == "root-999"
    assert info.dax_label == "mydax"
    assert info.user == "alice"
    assert info.planner_version == "5.1.3"
    assert info.dag_file == "mydax-0.dag"
    assert info.condor_log == "mydax-0.log"
    assert info.timestamp == "20260624T120000+0000"


def test_load_braindump_missing_optional_keys_default_to_empty_string(tmp_path):
    """Only submit_dir is required; the rest default to ''."""
    import yaml

    d = tmp_path / "minimal"
    d.mkdir()
    bd = d / "braindump.yml"
    bd.write_text(yaml.safe_dump({"submit_dir": str(d)}))

    info = load_braindump(d, remap="never")
    assert info.wf_uuid == ""
    assert info.root_wf_uuid == ""
    assert info.dax_label == ""
    assert info.user == ""
    assert info.planner_version == ""
    assert info.dag_file == ""
    assert info.condor_log == ""
    assert info.timestamp == ""
    assert info.submit_dir == d


def test_load_braindump_recorded_submit_dir_always_verbatim(tmp_path, write_braindump):
    """recorded_submit_dir reflects the raw recorded path under every remap mode."""
    recorded = "/in/container/work/run0001"
    somedir = tmp_path / "somedir"
    write_braindump(somedir, submit_dir=recorded, basedir="/in/container/work")
    for mode in ("auto", "always", "never"):
        info = load_braindump(somedir, remap=mode)
        assert info.recorded_submit_dir == Path(recorded), mode
