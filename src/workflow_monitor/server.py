"""Headless server daemon for workflow-monitor.

Runs the monitoring loop without a terminal UI, writing JSONL event logs
continuously.  Designed to survive terminal disconnection via daemonization
(double-fork on Unix).

Usage (from cli.py):
    workflow-monitor --serve [--log PATH] TARGET
"""

from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from .braindump import WorkflowInfo
from .db import StampedeDB, WorkflowSnapshot
from .event_log import EventLogger
from .htcondor_poll import CondorPoller


def _run_poll_cycle(
    db: StampedeDB,
    logger: EventLogger,
    poller: CondorPoller,
    diag_engine,
) -> tuple:
    """Run one server monitoring iteration and record it.

    Reads a fresh stampede.db snapshot, polls condor/history/pool through the
    back-off-aware :class:`~.htcondor_poll.CondorPoller`, writes the event-log
    record, and (when enabled) runs the diagnostics engine. Returns
    ``(snapshot, condor_jobs, history, pool)``. Extracted from the loop so a
    single cycle is unit-testable without daemonizing, signals, or sleeping.
    """
    snap = db.snapshot()
    condor_jobs, history, pool = poller.poll()
    logger.record(snap, condor_jobs, history, pool)
    if diag_engine is not None:
        try:
            diag_engine.tick(snap, condor_jobs, pool, logger.high_water_ts)
        except Exception:
            import traceback

            traceback.print_exc()
    return snap, condor_jobs, history, pool


def _daemonize(pid_file: Path) -> None:
    """Classic double-fork to fully detach from the controlling terminal."""
    # First fork
    pid = os.fork()
    if pid > 0:
        # Parent: print info and exit
        print(f"Server started (PID {pid}), logging in background.")
        print(f"PID file: {pid_file}")
        sys.exit(0)

    # First child — become session leader
    os.setsid()

    # Second fork — prevent re-acquiring a terminal
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # Grandchild — the actual daemon
    # Redirect stdin to /dev/null; redirect stdout/stderr to a log file
    # next to the PID file so daemon errors are diagnosable.
    sys.stdout.flush()
    sys.stderr.flush()
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)  # stdin
    os.close(devnull)

    daemon_log = str(pid_file) + ".log"
    log_fd = os.open(daemon_log, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.dup2(log_fd, 1)  # stdout
    os.dup2(log_fd, 2)  # stderr
    os.close(log_fd)

    # Write PID file
    pid_file.write_text(str(os.getpid()))


def _cleanup_pid(pid_file: Path) -> None:
    try:
        pid_file.unlink(missing_ok=True)
    except OSError:
        pass


def run_server(
    info: WorkflowInfo,
    db: StampedeDB,
    poll_interval: float = 2.0,
    log_path: Optional[Path] = None,
    condor_kwargs: Optional[dict] = None,
    condor_constraint: Optional[str] = None,
    foreground: bool = False,
    diagnose: bool = False,
    min_free_mb: float = 200.0,
    max_log_mb: Optional[float] = None,
    enable_condor_polling: bool = True,
) -> None:
    """Run the headless monitoring server.

    The loop reads the local stampede.db every cycle.  The live HTCondor queue
    is polled through a :class:`~.htcondor_poll.CondorBackoff` gate: when the
    scheduler becomes unreachable or overloaded the condor poll cadence backs
    off exponentially (the local DB/loop cadence is unaffected) so the monitor
    never piles load onto a struggling schedd, snapping back to the base
    interval as soon as a query succeeds.

    Parameters
    ----------
    info:              WorkflowInfo from braindump.yml.
    db:                Connected StampedeDB instance.
    poll_interval:     Seconds between stampede.db refreshes.
    log_path:          Path to write JSONL event log.
    condor_kwargs:     Extra kwargs forwarded to htcondor_poll.query_queue().
    condor_constraint: Optional HTCondor ClassAd constraint for live queue.
    foreground:        If True, run in foreground (don't daemonize).
    min_free_mb:       Pause event logging below this much free space (MB).
    max_log_mb:        Optional hard cap on the JSONL size (MB); None = unbounded.
    enable_condor_polling: If False, skip all condor_q/history/status queries
                       (the wfmonitor monitord plugin is polling instead); the
                       stampede.db loop and diagnostics are unaffected.
    """
    ck = condor_kwargs or {}

    if log_path is None:
        log_path = info.submit_dir / "workflow-events.jsonl"

    pid_file = log_path.with_suffix(".pid")

    if not foreground:
        # Print info before daemonizing (stdout still connected)
        print(f"Logging events to: {log_path}")
        _daemonize(pid_file)
    else:
        pid_file.write_text(str(os.getpid()))
        print(f"Server running in foreground (PID {os.getpid()})")
        print(f"Logging events to: {log_path}")
        print(f"PID file: {pid_file}")
        print("Press Ctrl+C to stop.")

    # Set up signal handling for graceful shutdown
    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger = EventLogger(
        info, db, log_path=log_path, min_free_mb=min_free_mb, max_log_mb=max_log_mb
    )

    diag_engine = None
    if diagnose:
        from .diag_log import DiagnosticLogger
        from .diagnostics_engine import DiagnosticsEngine

        diag_path = DiagnosticLogger.path_from_event_log(logger.path)
        diag_engine = DiagnosticsEngine(
            info=info,
            diag_log_path=diag_path,
            poll_interval=poll_interval,
            condor_constraint=condor_constraint,
            condor_kwargs=ck,
        )
        print(f"Diagnostics enabled — sidecar: {diag_path}")

    class _DisabledCondorPoller:
        """No-op poller used when pegasus-monitord's plugin owns condor polling."""

        def __init__(self) -> None:
            self.history_cache: list = []
            self.pool_cache = None

        def poll(self) -> tuple:
            return [], self.history_cache, self.pool_cache

    # Adaptive gate: keep the loop (stampede.db) at base cadence, but poll the
    # condor queue less often when the scheduler is failing — the poller caches
    # the last good result so a gated/failed cycle reuses it (and the fingerprint
    # dedup emits nothing new) rather than flapping to an empty queue.
    if enable_condor_polling:
        poller = CondorPoller(
            poll_interval,
            condor_constraint=condor_constraint,
            condor_kwargs=ck,
            on_condor_down=lambda exc: print(
                f"[backoff] condor query failed ({exc}); polling less frequently "
                f"until the scheduler recovers",
                file=sys.stderr,
            ),
            on_condor_up=lambda: print(
                "[backoff] condor reachable again; resuming normal poll cadence",
                file=sys.stderr,
            ),
        )
    else:
        poller = _DisabledCondorPoller()

    snap: Optional[WorkflowSnapshot] = None

    try:
        while not shutdown:
            snap, _, _, _ = _run_poll_cycle(db, logger, poller, diag_engine)

            if snap.is_complete and not snap.is_running:
                # One more poll for final DB flush
                time.sleep(poll_interval)
                snap, _, _, _ = _run_poll_cycle(db, logger, poller, diag_engine)
                break

            # Base cadence for the local DB/loop; the condor poll itself backs
            # off internally via the poller's CondorBackoff gate.
            time.sleep(poll_interval)
    except Exception as exc:
        import traceback

        traceback.print_exc()
        print(f"Server error: {exc}", file=sys.stderr)
    finally:
        if snap is not None:
            logger.close(
                snap,
                condor_history=poller.history_cache,
                pool_status=poller.pool_cache,
            )
        else:
            logger.close()
        if diag_engine is not None:
            try:
                diag_engine.close()
            except Exception:
                pass
        _cleanup_pid(pid_file)


def stop_server(pid_file: Path) -> bool:
    """Stop a running server by sending SIGTERM to the PID in the file."""
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal.SIGTERM)
        return True
    except (ValueError, OSError):
        return False
