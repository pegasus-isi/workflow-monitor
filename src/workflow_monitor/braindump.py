"""Parse Pegasus braindump.yml to locate workflow artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class WorkflowInfo:
    wf_uuid: str
    root_wf_uuid: str
    dax_label: str
    submit_dir: Path
    user: str
    planner_version: str
    dag_file: str
    condor_log: str
    timestamp: str
    basedir: Path
    # Verbatim submit_dir from braindump.yml. When the workflow was planned
    # inside a container, this is the in-container path (e.g. /work/work/...)
    # while `submit_dir` may be remapped to the host path. Use this for
    # matching against values reported by the schedd (e.g. condor_q's `Cmd`),
    # which always reflect the planner's view.
    recorded_submit_dir: Path = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.recorded_submit_dir is None:
            self.recorded_submit_dir = self.submit_dir

    @property
    def stampede_db(self) -> Optional[Path]:
        """Path to the stampede SQLite database (written by pegasus-monitord)."""
        stem = self.dag_file.replace(".dag", "")
        p = self.submit_dir / f"{stem}.stampede.db"
        return p if p.exists() else None

    @property
    def monitord_events_path(self) -> Path:
        """Default path for pegasus-monitord's live native-event JSONL.

        Written by Pegasus' ``WorkflowMonitorEventSink`` when monitord is run
        with ``pegasus.monitord.wfmonitor.url``. May not exist (the feature is
        opt-in); use :attr:`monitord_events` for an existence-checked variant.
        """
        return self.submit_dir / "monitord-events.jsonl"

    @property
    def monitord_events(self) -> Optional[Path]:
        """Live monitord event JSONL path, or None if it doesn't exist yet."""
        p = self.monitord_events_path
        return p if p.exists() else None

    @property
    def jobstate_log(self) -> Optional[Path]:
        """Path to the jobstate.log file."""
        p = self.submit_dir / "jobstate.log"
        return p if p.exists() else None

    @property
    def condor_log_path(self) -> Optional[Path]:
        """Path to the HTCondor event log."""
        p = self.submit_dir / self.condor_log
        return p if p.exists() else None

    @property
    def dag_path(self) -> Path:
        return self.submit_dir / self.dag_file

    @property
    def dagman_out_path(self) -> Optional[Path]:
        p = self.submit_dir / f"{self.dag_file}.dagman.out"
        return p if p.exists() else None


def find_braindump(path: Path) -> Optional[Path]:
    """Locate braindump.yml from various starting points.

    Accepts:
    - A braindump.yml file directly
    - A run directory containing braindump.yml
    - A workflow base directory (returns the highest-numbered run)
    """
    path = path.resolve()

    if path.is_file() and path.name == "braindump.yml":
        return path

    direct = path / "braindump.yml"
    if direct.exists():
        return direct

    # Search recursively for braindump.yml files and pick the last (latest run)
    candidates = sorted(path.rglob("braindump.yml"))
    if candidates:
        return candidates[-1]

    return None


def load_braindump(path: Path, remap: str = "auto") -> WorkflowInfo:
    """Load a braindump.yml and return a WorkflowInfo.

    `remap` controls how the recorded `submit_dir`/`basedir` paths are
    interpreted on this host:
      - "auto" (default): if the recorded submit_dir does not exist locally,
        rebase it onto the directory containing the braindump.yml. This
        handles workflows planned inside a container where the recorded
        path differs from the host path.
      - "always": always rebase to the directory containing braindump.yml.
      - "never": trust the recorded paths verbatim.
    """
    if remap not in {"auto", "always", "never"}:
        raise ValueError(f"remap must be 'auto', 'always', or 'never'; got {remap!r}")

    bd_path = find_braindump(path)
    if bd_path is None:
        raise FileNotFoundError(
            f"Cannot find braindump.yml at or under: {path}\n"
            "Ensure the workflow has been planned with pegasus-plan."
        )

    with open(bd_path) as f:
        data = yaml.safe_load(f)

    recorded_submit = Path(data["submit_dir"])
    recorded_basedir = Path(data.get("basedir", recorded_submit.parent))
    actual_submit = bd_path.parent.resolve()

    do_remap = remap == "always" or (remap == "auto" and not recorded_submit.exists())

    if do_remap:
        submit_dir = actual_submit
        # Preserve the recorded basedir→submit_dir offset when possible so a
        # remapped basedir lands on the equivalent host directory.
        try:
            offset = recorded_submit.relative_to(recorded_basedir)
            depth = len(offset.parts)
            if depth and depth <= len(actual_submit.parents):
                basedir = actual_submit.parents[depth - 1]
            else:
                basedir = actual_submit.parent
        except ValueError:
            basedir = actual_submit.parent
    else:
        submit_dir = recorded_submit
        basedir = recorded_basedir

    return WorkflowInfo(
        wf_uuid=data.get("wf_uuid", ""),
        root_wf_uuid=data.get("root_wf_uuid", ""),
        dax_label=data.get("dax_label", ""),
        submit_dir=submit_dir,
        user=data.get("user", ""),
        planner_version=data.get("planner_version", ""),
        dag_file=data.get("dag", ""),
        condor_log=data.get("condor_log", ""),
        timestamp=data.get("timestamp", ""),
        basedir=basedir,
        recorded_submit_dir=recorded_submit,
    )
