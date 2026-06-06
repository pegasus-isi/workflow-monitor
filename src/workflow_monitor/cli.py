"""Command-line entry point for workflow-monitor."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import __version__
from .braindump import load_braindump
from .db import StampedeDB
from .display import run_monitor
from .stampede_stream import StampedeStreamReader


DESCRIPTION = """\
Real-time monitor for a running Pegasus WMS workflow.

TARGET may be:
  • A submit directory  (contains braindump.yml)
  • A workflow base dir (latest run is discovered automatically)
  • A braindump.yml file directly

The monitor reads the stampede SQLite database written by pegasus-monitord
and optionally queries the live HTCondor queue for additional job detail.
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="workflow-monitor",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument(
        "target",
        nargs="?",
        default=".",
        metavar="TARGET",
        help="Workflow submit dir, base dir, or braindump.yml (default: cwd)",
    )
    p.add_argument(
        "--version",
        "-V",
        action="version",
        version=f"workflow-monitor {__version__}",
    )
    p.add_argument(
        "--interval",
        "-i",
        type=float,
        default=2.0,
        metavar="SECONDS",
        help="Refresh interval in seconds (default: 2.0)",
    )
    p.add_argument(
        "--all-jobs",
        "-a",
        action="store_true",
        default=False,
        help="Show all job types, not just compute jobs",
    )
    p.add_argument(
        "--sort-by-activity",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Order the job table by most recent activity, with RUNNING jobs "
        "always at the top (default: enabled). Use --no-sort-by-activity "
        "to keep the original DAG/submit order.",
    )
    p.add_argument(
        "--events",
        "-e",
        type=int,
        default=15,
        metavar="N",
        help="Number of recent events to display (default: 15)",
    )
    p.add_argument(
        "--once",
        action="store_true",
        default=False,
        help="Print current status once and exit (non-interactive)",
    )
    p.add_argument(
        "--why-idle",
        action="store_true",
        default=False,
        help="One-shot diagnostic: explain why workflow jobs are idle, then exit",
    )
    p.add_argument(
        "--source",
        choices=("auto", "live", "db"),
        default="auto",
        help="Where to read Pegasus workflow events from: "
        "'live' tails pegasus-monitord's monitord-events.jsonl (events as "
        "monitord parses them, before they reach the stampede DB); "
        "'db' polls the stampede SQLite database; "
        "'auto' (default) uses the live stream when monitord-events.jsonl "
        "exists in the submit dir, else falls back to the database.",
    )
    p.add_argument(
        "--remap-submit-dir",
        choices=("auto", "always", "never"),
        default="auto",
        help="How to interpret submit_dir from braindump.yml: "
        "'auto' (default) rebases onto the braindump.yml's parent "
        "directory only when the recorded path doesn't exist locally; "
        "'always' forces rebasing (use for container-planned workflows "
        "viewed from the host); 'never' trusts the recorded path verbatim.",
    )
    p.add_argument(
        "--diagnose",
        action="store_true",
        default=False,
        help="Enable the diagnostics layer (stall detection + auto-diagnosis, "
        "writes a diagnostics-events.jsonl sidecar file)",
    )
    p.add_argument(
        "--log",
        nargs="?",
        const="auto",
        default=None,
        metavar="PATH",
        help="Log events to a JSONL file for replay (default: {submit_dir}/workflow-events.jsonl)",
    )
    p.add_argument(
        "--replay",
        metavar="PATH",
        default=None,
        help="Replay a JSONL event log file in the TUI dashboard",
    )
    p.add_argument(
        "--speed",
        type=float,
        default=1.0,
        metavar="MULTIPLIER",
        help="Replay speed multiplier (default: 1.0, e.g. 4 = 4x speed)",
    )

    # ── Logging safeguards ───────────────────────────────────────────────────
    guard = p.add_argument_group(
        "Logging safeguards",
        "Bound the JSONL event log's footprint so the monitor never fills the "
        "filesystem the workflow runs on. Applies whenever an event log is "
        "written (--log or --serve).",
    )
    guard.add_argument(
        "--min-free-mb",
        type=float,
        default=200.0,
        metavar="MB",
        help="Pause event logging when free space on the log's filesystem "
        "drops below this many MB; resume when it recovers "
        "(default: 200; 0 to disable).",
    )
    guard.add_argument(
        "--max-log-mb",
        type=float,
        default=None,
        metavar="MB",
        help="Pause event logging when the JSONL file grows past this many MB "
        "(default: unlimited).",
    )

    # ── Client/Server mode ───────────────────────────────────────────────────
    cs = p.add_argument_group(
        "Client/Server mode",
        "Run a headless server that logs events, or a remote client that "
        "syncs and displays via SSH.",
    )
    cs.add_argument(
        "--serve",
        action="store_true",
        default=False,
        help="Run as a headless server daemon (daemonizes, survives terminal exit)",
    )
    cs.add_argument(
        "--serve-foreground",
        action="store_true",
        default=False,
        help="Run the server in the foreground (for debugging; Ctrl+C to stop)",
    )
    cs.add_argument(
        "--stop-server",
        nargs="?",
        const="auto",
        default=None,
        metavar="PID_FILE",
        help="Stop a running server daemon (reads PID from .pid file next to the log)",
    )
    cs.add_argument(
        "--remote",
        metavar="USER@HOST:/PATH",
        default=None,
        help="Monitor a remote workflow via SSH (e.g. user@host:/path/to/workflow-events.jsonl)",
    )
    cs.add_argument(
        "--sync-interval",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="How often the remote client fetches the log file (default: 5.0)",
    )
    cs.add_argument(
        "--ssh-config",
        metavar="PATH",
        default=None,
        help="SSH config file for --remote (passed as ssh -F <PATH>)",
    )
    cs.add_argument(
        "--ssh-identity",
        metavar="PATH",
        default=None,
        help="SSH identity/key file for --remote (passed as ssh -i <PATH>)",
    )

    # ── HTCondor options ─────────────────────────────────────────────────────
    condor = p.add_argument_group("HTCondor options")
    condor.add_argument(
        "--schedd",
        metavar="NAME",
        help="Query a specific condor_schedd by name",
    )
    condor.add_argument(
        "--collector",
        metavar="HOST[:PORT]",
        help="Collector host for remote pool queries",
    )

    # ── Credential options ───────────────────────────────────────────────────
    creds = p.add_argument_group(
        "Credential options",
        "Credentials are never required for local pools with default security settings.",
    )
    creds.add_argument(
        "--token",
        metavar="PATH",
        help="Path to an HTCondor IDTOKEN file or directory",
    )
    creds.add_argument(
        "--cert",
        metavar="PATH",
        help="Path to an X.509 / GSI certificate file",
    )
    creds.add_argument(
        "--key",
        metavar="PATH",
        help="Path to an X.509 / GSI private key file",
    )
    creds.add_argument(
        "--password-file",
        metavar="PATH",
        help="Path to an HTCondor password file",
    )

    return p


def main(argv: list | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # ── Replay mode ───────────────────────────────────────────────────────────
    if args.replay is not None:
        from .replay import ReplayEngine

        replay_path = Path(args.replay)
        if not replay_path.exists():
            print(f"[error] Replay file not found: {replay_path}", file=sys.stderr)
            return 1
        try:
            engine = ReplayEngine(
                replay_path,
                speed=args.speed,
                events_n=args.events,
            )
            engine.run(show_all=args.all_jobs, sort_by_activity=args.sort_by_activity)
        except (ValueError, json.JSONDecodeError) as exc:
            print(f"[error] {exc}", file=sys.stderr)
            return 1
        return 0

    # ── Remote client mode ────────────────────────────────────────────────────
    if args.remote is not None:
        from .remote import RemoteEngine

        try:
            engine = RemoteEngine(
                args.remote,
                sync_interval=args.sync_interval,
                events_n=args.events,
                ssh_config=args.ssh_config,
                ssh_identity=args.ssh_identity,
            )
            engine.run(
                show_all=args.all_jobs,
                once=args.once,
                sort_by_activity=args.sort_by_activity,
            )
        except (ValueError, json.JSONDecodeError) as exc:
            print(f"[error] {exc}", file=sys.stderr)
            return 1
        return 0

    # ── Stop server ───────────────────────────────────────────────────────────
    if args.stop_server is not None:
        from .server import stop_server

        if args.stop_server == "auto":
            # Locate workflow to find default log path
            target = Path(args.target)
            try:
                info = load_braindump(target, remap=args.remap_submit_dir)
            except FileNotFoundError as exc:
                print(f"[error] {exc}", file=sys.stderr)
                return 1
            pid_file = info.submit_dir / "workflow-events.pid"
        else:
            pid_file = Path(args.stop_server)

        if stop_server(pid_file):
            print(f"Server stopped (PID file: {pid_file})")
            return 0
        else:
            print(
                f"[error] Could not stop server (PID file: {pid_file})", file=sys.stderr
            )
            return 1

    target = Path(args.target)

    # ── Locate workflow ───────────────────────────────────────────────────────
    try:
        info = load_braindump(target, remap=args.remap_submit_dir)
    except FileNotFoundError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    db_path = info.stampede_db
    events_path = info.monitord_events_path

    # ── Select the Pegasus event source ───────────────────────────────────────
    # 'live' tails monitord-events.jsonl (events as monitord parses them);
    # 'db' polls the stampede SQLite DB; 'auto' prefers the live stream when
    # present, else the DB.  The live stream needs no DB; the DB path needs no
    # live stream.
    use_live = args.source == "live" or (args.source == "auto" and events_path.exists())

    if use_live:
        if args.source == "live" and not events_path.exists():
            print(
                f"[warn] Live source requested but {events_path} does not exist "
                "yet; waiting for pegasus-monitord to create it "
                "(enable it with pegasus.monitord.wfmonitor.url).",
                file=sys.stderr,
            )
    elif db_path is None:
        print(
            f"[error] Stampede database not found in: {info.submit_dir}\n"
            "  Make sure pegasus-monitord is running (it is started automatically\n"
            "  by pegasus-run / pegasus-plan --submit).\n"
            "  For live, pre-DB events, run monitord with "
            "pegasus.monitord.wfmonitor.url and use --source live.",
            file=sys.stderr,
        )
        return 1

    def make_source():
        """Open the chosen event source (StampedeDB or live stream reader)."""
        if use_live:
            return StampedeStreamReader(events_path, wf_uuid=info.wf_uuid)
        return StampedeDB(db_path, wf_uuid=info.wf_uuid)

    # ── Build condor kwargs ───────────────────────────────────────────────────
    condor_kwargs: dict = {}
    if args.schedd:
        condor_kwargs["schedd_name"] = args.schedd
    if args.collector:
        condor_kwargs["collector_host"] = args.collector
    if args.token:
        condor_kwargs["token_path"] = args.token
    if args.cert:
        condor_kwargs["cert_path"] = args.cert
    if args.key:
        condor_kwargs["key_path"] = args.key
    if getattr(args, "password_file", None):
        condor_kwargs["password_file"] = args.password_file

    # ── Why-idle diagnostic (one-shot, exits immediately) ───────────────────
    if args.why_idle:
        from .why_idle import run_why_idle

        condor_kwargs_idle: dict = {}
        if args.schedd:
            condor_kwargs_idle["schedd_name"] = args.schedd
        if args.collector:
            condor_kwargs_idle["collector_host"] = args.collector
        if args.token:
            condor_kwargs_idle["token_path"] = args.token
        if args.cert:
            condor_kwargs_idle["cert_path"] = args.cert
        if args.key:
            condor_kwargs_idle["key_path"] = args.key
        if getattr(args, "password_file", None):
            condor_kwargs_idle["password_file"] = args.password_file

        # Match against the path the planner recorded — that's what the schedd
        # sees in Cmd, even when this host views the submit_dir under a
        # different mount (e.g. a container bind mount).
        recorded_submit_str = str(info.recorded_submit_dir)
        submit_dir_esc = recorded_submit_str.replace("\\", "\\\\").replace('"', '\\"')
        condor_constraint_idle = (
            f"Cmd =!= UNDEFINED"
            f' && substr(Cmd, 0, {len(recorded_submit_str)}) == "{submit_dir_esc}"'
        )

        with make_source() as db:
            snap = db.snapshot()

        return run_why_idle(
            info=info,
            snapshot=snap,
            condor_constraint=condor_constraint_idle,
            condor_kwargs=condor_kwargs_idle,
        )

    # ── Resolve log path ─────────────────────────────────────────────────────
    log_path = None
    if args.log is not None:
        if args.log == "auto":
            log_path = info.submit_dir / "workflow-events.jsonl"
        else:
            log_path = Path(args.log)

    # ── Build condor constraint scoped to this workflow ───────────────────────
    # On shared schedds, condor_q returns all users' jobs.  Restrict to jobs
    # whose Cmd lives under this workflow's submit directory so the JSONL log
    # (and live display) only contain relevant entries. Use the path the
    # planner recorded — when planning happened inside a container the host's
    # view of submit_dir differs from what the schedd reports in Cmd.
    recorded_submit_str = str(info.recorded_submit_dir)
    submit_dir_esc = recorded_submit_str.replace("\\", "\\\\").replace('"', '\\"')
    condor_constraint = (
        f"Cmd =!= UNDEFINED"
        f' && substr(Cmd, 0, {len(recorded_submit_str)}) == "{submit_dir_esc}"'
    )

    # ── Serve mode (headless daemon) ─────────────────────────────────────────
    if args.serve or args.serve_foreground:
        from .server import run_server

        with make_source() as db:
            run_server(
                info=info,
                db=db,
                poll_interval=args.interval,
                log_path=log_path,
                condor_kwargs=condor_kwargs,
                condor_constraint=condor_constraint,
                foreground=args.serve_foreground,
                diagnose=args.diagnose,
                min_free_mb=args.min_free_mb,
                max_log_mb=args.max_log_mb,
            )
        return 0

    # ── Run monitor ───────────────────────────────────────────────────────────
    with make_source() as db:
        run_monitor(
            info=info,
            db=db,
            poll_interval=args.interval,
            show_all=args.all_jobs,
            events_n=args.events,
            condor_kwargs=condor_kwargs,
            condor_constraint=condor_constraint,
            once=args.once,
            log_path=log_path,
            diagnose=args.diagnose,
            sort_by_activity=args.sort_by_activity,
            min_free_mb=args.min_free_mb,
            max_log_mb=args.max_log_mb,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
