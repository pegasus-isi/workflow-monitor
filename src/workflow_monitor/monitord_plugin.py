"""
pegasus-monitord event plugin that feeds workflow-monitor live.

Registered under the ``pegasus.monitord.plugins`` entry-point group, this
plugin runs inside ``pegasus-monitord`` (on its own background thread) and
translates the stampede event stream into workflow-monitor's native JSONL
records as monitord parses them -- so workflow-monitor sees progress live,
without waiting for events to land in and be polled back out of the stampede
database.

It writes the same ``monitord-events.jsonl`` that workflow-monitor's
``StampedeStreamReader`` already tails, so the consuming side needs no changes.

Enable it (in the workflow's ``pegasus.properties``)::

    pegasus.monitord.plugins.wfmonitor.enabled = true
    pegasus.monitord.plugins.wfmonitor.events_path = /path/to/run/monitord-events.jsonl

The translation here mirrors ``WorkflowMonitorEventSink`` so the output is
identical whether produced by the sink or by this plugin.
"""

import json
import logging
import os
import traceback

log = logging.getLogger(__name__)

# Stampede event namespace. monitord delivers fully-qualified names to plugins
# (e.g. "stampede.job_inst.main.end"); the handlers below key off both the
# qualified and unqualified forms.
STAMPEDE_NS = "stampede."

# Import the plugin base class from Pegasus when available (it is, inside the
# monitord process that loads this entry point). Fall back to a duck-typed stub
# so the module still imports for standalone `pip install workflow-monitor`
# users who do not have Pegasus installed -- monitord only calls start/
# handle_event/stop, it never isinstance-checks.
try:
    from Pegasus.monitoring.plugin import MonitordEventPlugin
except Exception:  # pragma: no cover - exercised only without Pegasus installed

    class MonitordEventPlugin:
        def start(self, props=None):
            pass

        def handle_event(self, event, kw):
            pass

        def stop(self):
            pass


class WorkflowMonitorPlugin(MonitordEventPlugin):
    """Translate stampede events into workflow-monitor native JSONL records."""

    NAME = "wfmonitor"

    # event name (with STAMPEDE_NS prefix) -> [status==-1 state, status==0 state].
    # Copied verbatim from Pegasus.db.workflow_loader.WorkflowLoader.jobstate so
    # the emitted "state" values are identical to the stampede jobstate column
    # (which is what workflow-monitor reads). The index is int(status)+1;
    # callers only ever pass status -1, 0, or none (-> success/normal variant).
    _JOB_STATES = {
        "stampede.job_inst.pre.start": ["PRE_SCRIPT_STARTED", "PRE_SCRIPT_STARTED"],
        "stampede.job_inst.pre.term": [
            "PRE_SCRIPT_TERMINATED",
            "PRE_SCRIPT_TERMINATED",
        ],
        "stampede.job_inst.pre.end": ["PRE_SCRIPT_FAILED", "PRE_SCRIPT_SUCCESS"],
        "stampede.job_inst.submit.end": ["SUBMIT_FAILED", "SUBMIT"],
        "stampede.job_inst.main.start": ["EXECUTE", "EXECUTE"],
        "stampede.job_inst.main.term": ["JOB_EVICTED", "JOB_TERMINATED"],
        "stampede.job_inst.main.end": ["JOB_FAILURE", "JOB_SUCCESS"],
        "stampede.job_inst.post.start": ["POST_SCRIPT_STARTED", "POST_SCRIPT_STARTED"],
        "stampede.job_inst.post.term": [
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_TERMINATED",
        ],
        "stampede.job_inst.post.end": ["POST_SCRIPT_FAILED", "POST_SCRIPT_SUCCESS"],
        "stampede.job_inst.held.start": ["JOB_HELD", "JOB_HELD"],
        "stampede.job_inst.held.end": ["JOB_RELEASED", "JOB_RELEASED"],
        "stampede.job_inst.image.info": ["IMAGE_SIZE", "IMAGE_SIZE"],
        "stampede.job_inst.abort.info": ["JOB_ABORTED", "JOB_ABORTED"],
        "stampede.job_inst.grid.submit.end": ["GRID_SUBMIT_FAILED", "GRID_SUBMIT"],
        "stampede.job_inst.globus.submit.end": [
            "GLOBUS_SUBMIT_FAILED",
            "GLOBUS_SUBMIT",
        ],
    }

    _WF_STATES = {
        "stampede.xwf.start": "WORKFLOW_STARTED",
        "stampede.xwf.end": "WORKFLOW_TERMINATED",
    }

    def __init__(self):
        self._output = None
        self._wf_uuid = None
        self._last_ts = None
        self._header_emitted = False
        self._jobs_init_emitted = False
        # Correlation state (replaces the stampede DB joins)
        self._job_info = {}  # exec_job_id -> {"type_desc": str, "seq": int}
        self._task_info = {}  # task_id -> {"transformation": str, "argv": str}
        self._job_task = {}  # exec_job_id -> task_id
        self._job_extra = {}  # exec_job_id -> {exitcode, stdout_file, stderr_file, maxrss, site}
        self._job_seq = 0

    # ── lifecycle ──────────────────────────────────────────────────────────────

    def start(self, props=None):
        cfg = {}
        if props is not None:
            cfg = props.propertyset(
                f"pegasus.monitord.plugins.{self.NAME}.", remove=True
            )
        path = cfg.get("events_path") or os.path.join(
            os.getcwd(), "monitord-events.jsonl"
        )
        restart = str(cfg.get("restart", "")).lower() in ("true", "1", "yes", "on")
        # line-buffered so workflow-monitor's tailer sees records promptly
        self._output = open(path, "w" if restart else "a", 1)
        log.info("wfmonitor plugin writing events to %s", path)

    def stop(self):
        if self._output is not None:
            try:
                self._output.close()
            except Exception:
                pass
            self._output = None

    # ── helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _norm(kw):
        """Canonicalize event keys to single-underscore form (xwf_id, job_id,
        type_desc, stdout_file, ...)."""
        return {k.replace(".", "_").replace("__", "_"): v for k, v in kw.items()}

    def _write(self, record):
        record.setdefault("wf_uuid", self._wf_uuid)
        self._output.write(json.dumps(record) + "\n")

    @staticmethod
    def _as_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # ── dispatch ───────────────────────────────────────────────────────────────

    def handle_event(self, event, kw):
        if self._output is None:
            return
        # monitord delivers fully-qualified names; the original sink dispatched
        # on the unqualified form, so derive both.
        if event.startswith(STAMPEDE_NS):
            full = event
            short = event[len(STAMPEDE_NS) :]
        else:
            full = STAMPEDE_NS + event
            short = event

        try:
            d = self._norm(kw)
            if self._wf_uuid is None and d.get("xwf_id"):
                self._wf_uuid = d["xwf_id"]
            if d.get("ts") is not None:
                self._last_ts = d["ts"]

            if short == "wf.plan":
                self._on_wf_plan(d)
            elif short == "job.info":
                self._on_job_info(d)
            elif short == "task.info":
                self._on_task_info(d)
            elif short == "wf.map.task_job":
                self._on_task_job_map(d)
            elif short in ("static.end", "xwf.start"):
                self._emit_jobs_init()
                if short == "xwf.start":
                    self._on_wf_state(full, d)
            elif short == "xwf.end":
                self._on_wf_state(full, d)
            elif short == "inv.end":
                self._on_inv_end(d)
            elif full in self._JOB_STATES:
                self._on_job_state(full, d)
        except Exception:
            log.error(
                "wfmonitor plugin error on event %s: %s",
                event,
                traceback.format_exc(),
            )

    # ── handlers ───────────────────────────────────────────────────────────────

    def _on_wf_plan(self, d):
        if self._header_emitted:
            return
        self._header_emitted = True
        self._write(
            {
                "event_type": "workflow_start",
                "timestamp": d.get("ts"),
                "dax_label": d.get("dax_label"),
                "user": d.get("user"),
                "planner_version": d.get("planner_version"),
                "submit_dir": d.get("submit_dir"),
                "wf_start": None,  # authoritative start arrives with xwf.start
            }
        )

    def _on_job_info(self, d):
        name = d.get("job_id")
        if not name:
            return
        if name not in self._job_info:
            self._job_seq += 1
            self._job_info[name] = {
                "type_desc": d.get("type_desc"),
                "seq": self._job_seq,
            }

    def _on_task_info(self, d):
        tid = d.get("task_id")
        if not tid:
            return
        self._task_info[tid] = {
            "transformation": d.get("transformation"),
            "argv": d.get("argv"),
        }

    def _on_task_job_map(self, d):
        name = d.get("job_id")
        tid = d.get("task_id")
        if name and tid:
            self._job_task[name] = tid

    def _emit_jobs_init(self):
        if self._jobs_init_emitted:
            return
        self._jobs_init_emitted = True
        jobs = []
        for name, info in self._job_info.items():
            entry = {
                "job_id": info["seq"],
                "exec_job_id": name,
                "type_desc": info["type_desc"],
            }
            task = self._task_info.get(self._job_task.get(name))
            if task:
                if task.get("transformation"):
                    entry["transformation"] = task["transformation"]
                if task.get("argv"):
                    entry["task_argv"] = task["argv"]
            jobs.append(entry)
        self._write(
            {
                "event_type": "jobs_init",
                "timestamp": self._last_ts,
                "total_jobs": len(jobs),
                "jobs": jobs,
            }
        )

    def _on_wf_state(self, full, d):
        state = self._WF_STATES.get(full)
        if state is None:
            return
        rec = {
            "event_type": "workflow_state",
            "timestamp": d.get("ts"),
            "state": state,
            "status": self._as_int(d.get("status")),
        }
        if state == "WORKFLOW_STARTED":
            rec["wf_start"] = d.get("ts")
        else:
            rec["wf_end"] = d.get("ts")
        self._write(rec)

    def _on_inv_end(self, d):
        name = d.get("job_id")
        if not name or d.get("maxrss") is None:
            return
        maxrss = self._as_int(d.get("maxrss"))
        if maxrss is not None:
            self._job_extra.setdefault(name, {})["maxrss"] = maxrss

    def _on_job_state(self, full, d):
        name = d.get("job_id")
        if not name:
            return
        # jobs_init must precede the first job_state (e.g. if static.end was absent)
        if not self._jobs_init_emitted:
            self._emit_jobs_init()

        idx = self._as_int(d.get("status"))
        idx = 1 if idx is None else max(0, min(1, idx + 1))
        state = self._JOB_STATES[full][idx]

        # Carry forward per-job enrichment as it becomes known.
        extra = self._job_extra.setdefault(name, {})
        exitcode = self._as_int(d.get("exitcode"))
        if exitcode is not None:
            extra["exitcode"] = exitcode
        if d.get("stdout_file"):
            extra["stdout_file"] = d["stdout_file"]
        if d.get("stderr_file"):
            extra["stderr_file"] = d["stderr_file"]
        if d.get("site"):
            extra["site"] = d["site"]

        info = self._job_info.get(name, {})
        rec = {
            "event_type": "job_state",
            "timestamp": d.get("ts"),
            "exec_job_id": name,
            "type_desc": info.get("type_desc"),
            "state": state,
            "job_id": info.get("seq"),
        }
        if "exitcode" in extra:
            rec["exitcode"] = extra["exitcode"]
        if extra.get("stdout_file"):
            rec["stdout_file"] = extra["stdout_file"]
        if extra.get("stderr_file"):
            rec["stderr_file"] = extra["stderr_file"]
        if "maxrss" in extra:
            rec["maxrss"] = extra["maxrss"]
        self._write(rec)
