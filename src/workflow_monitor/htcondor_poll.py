"""Live HTCondor queue, history, and pool status polling.

Tries the Python bindings first (htcondor / htcondor2).  Falls back to
``condor_q -json`` / ``condor_history -json`` / ``condor_status -json``
subprocesses when the bindings are unavailable or fail (e.g. architecture
mismatch on macOS).

Credentials can be supplied via the environment or passed explicitly.
Supported mechanisms (in priority order):
  1. IDTOKENS — set CONDOR_CONFIG or COLLECTOR_HOST as needed; token path
     may be supplied via the ``token_path`` parameter.
  2. GSI certificates — set X509_USER_CERT / X509_USER_KEY environment
     variables, or pass ``cert_path`` / ``key_path``.
  3. Username/password — not universally supported; set CONDOR_PASSWORD_FILE
     or pass ``password_file``.

If no credentials are needed (local pool with ALLOW_READ = *), nothing
special is required.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# HTCondor JobStatus codes
JOB_STATUS: Dict[int, str] = {
    1: "Idle",
    2: "Running",
    3: "Removed",
    4: "Completed",
    5: "Held",
    6: "TransferOutput",
    7: "Suspended",
}


class CondorQueryError(RuntimeError):
    """A HTCondor query could not reach or parse the scheduler.

    Only raised when a caller passes ``raise_on_error=True`` (e.g. the server's
    poll loop, which uses it to drive adaptive back-off).  The default contract
    of the public query functions is unchanged: errors are swallowed and an
    empty result is returned.  Note that an *empty but valid* result (no jobs
    matched) is **not** an error and never raises.
    """


class CondorBackoff:
    """Adaptive gate for HTCondor polling.

    Lets a monitoring loop keep refreshing *local* state (stampede.db, the TUI)
    at its base cadence while querying a *failing* scheduler less and less
    often, so a down or overloaded ``schedd`` is never hammered — and so the
    user's view of workflow progress never freezes just because condor is
    unreachable.

    Each cycle, call :meth:`due` to decide whether to run a condor query now;
    after the attempt, call :meth:`record`::

        if backoff.due(now):
            try:
                jobs = query_queue(..., raise_on_error=True)
                backoff.record(True, now)
            except CondorQueryError:
                backoff.record(False, now)

    On success the gate opens every cycle (base cadence).  After consecutive
    failures it opens only on an exponentially growing interval — ``base`` → 2×
    → 4× … capped at ``max_backoff`` — with jitter so multiple monitors sharing
    one ``schedd`` do not retry in lock-step.  An *empty but valid* result is a
    success, not a failure, so an idle workflow never triggers back-off.
    """

    def __init__(self, base_interval: float, max_backoff: float = 60.0) -> None:
        self._base = max(base_interval, 0.1)
        self._max = max_backoff
        self.fail_streak = 0
        self._retry_at = 0.0

    def due(self, now: float) -> bool:
        """Whether a condor query is allowed this cycle."""
        return self.fail_streak == 0 or now >= self._retry_at

    def record(self, ok: bool, now: float) -> bool:
        """Update state after a query attempt.

        Returns ``True`` only on a failure→recovery transition, so a caller can
        log "reachable again" exactly once.
        """
        if ok:
            recovered = self.fail_streak > 0
            self.fail_streak = 0
            self._retry_at = 0.0
            return recovered
        self.fail_streak += 1
        delay = min(self._base * (2 ** min(self.fail_streak, 10)), self._max)
        delay += random.uniform(0, min(delay * 0.1, 5.0))
        self._retry_at = now + delay
        return False

    def next_retry_in(self, now: float) -> float:
        """Seconds until the next allowed condor poll (0 when due now)."""
        if self.fail_streak == 0:
            return 0.0
        return max(0.0, self._retry_at - now)


class CondorPoller:
    """Per-cycle HTCondor polling with adaptive back-off and throttling.

    Encapsulates the polling logic shared by the headless server and the live
    TUI so a single monitoring cycle is testable without a loop, a TTY, or a
    real scheduler:

      * the live queue (:func:`query_queue`) is gated by a :class:`CondorBackoff`
        so a failing/overloaded schedd is polled less and less often while the
        caller keeps refreshing local state every cycle;
      * history and pool are throttled to coarser intervals and skipped entirely
        while the schedd is failing (``fail_streak > 0``);
      * the last good result of each is cached and returned on a gated, failed,
        or throttled cycle so the caller never flaps to an empty view.

    Every poll takes an explicit ``now`` (defaulting to :func:`time.time`) so
    tests can drive the cadence deterministically. ``poll_condor`` must run
    before ``poll_history``/``poll_pool`` within a cycle — :meth:`poll` enforces
    this — because the latter two consult the back-off streak the former updates.

    ``on_condor_down`` (called once when the schedd first becomes unreachable,
    with the raising exception) and ``on_condor_up`` (called once on recovery)
    let the server log to stderr while the TUI stays silent and uses
    :attr:`condor_offline` to drive a header badge instead.
    """

    def __init__(
        self,
        poll_interval: float,
        condor_constraint: Optional[str] = None,
        condor_kwargs: Optional[Dict[str, Any]] = None,
        history_interval: Optional[float] = None,
        pool_interval: Optional[float] = None,
        on_condor_down: Optional[Callable[[Exception], None]] = None,
        on_condor_up: Optional[Callable[[], None]] = None,
    ) -> None:
        self._constraint = condor_constraint
        self._ck = condor_kwargs or {}
        self._backoff = CondorBackoff(poll_interval)
        self._on_down = on_condor_down
        self._on_up = on_condor_up

        self._last_condor_jobs: List[Dict] = []
        self._history_cache: List[Dict] = []
        self._history_last_poll: Optional[float] = None
        self._history_interval = (
            history_interval
            if history_interval is not None
            else max(poll_interval * 3, 10.0)
        )
        self._pool_cache: Optional[PoolSummary] = None
        self._pool_last_poll: Optional[float] = None
        self._pool_interval = (
            pool_interval if pool_interval is not None else max(poll_interval * 5, 15.0)
        )

    # ── State accessors ───────────────────────────────────────────────────────
    @property
    def backoff(self) -> CondorBackoff:
        return self._backoff

    @property
    def condor_offline(self) -> bool:
        """True while the schedd is in a failed back-off streak."""
        return self._backoff.fail_streak > 0

    @property
    def history_cache(self) -> List[Dict]:
        return self._history_cache

    @property
    def pool_cache(self) -> Optional[PoolSummary]:
        return self._pool_cache

    # ── Individual polls ────────────────────────────────────────────────────
    def poll_condor(self, now: Optional[float] = None) -> List[Dict]:
        """Poll the live queue through the back-off gate; reuse cache when gated."""
        if now is None:
            now = time.time()
        if not self._backoff.due(now):
            return self._last_condor_jobs  # gated — don't hit a struggling schedd
        try:
            jobs = query_queue(
                constraint=self._constraint, raise_on_error=True, **self._ck
            )
        except Exception as exc:
            first_failure = self._backoff.fail_streak == 0
            self._backoff.record(False, now)
            if first_failure and self._on_down is not None:
                self._on_down(exc)
            return self._last_condor_jobs
        if self._backoff.record(True, now) and self._on_up is not None:
            self._on_up()
        self._last_condor_jobs = jobs
        return jobs

    def poll_history(self, now: Optional[float] = None) -> List[Dict]:
        """Poll completed-job history on a throttled cadence; merge by ClusterId."""
        if now is None:
            now = time.time()
        if self._backoff.fail_streak > 0:
            return self._history_cache  # schedd failing — don't pile on more load
        if (
            self._history_last_poll is not None
            and now - self._history_last_poll < self._history_interval
        ):
            return self._history_cache
        try:
            result = query_history(constraint=self._constraint, **self._ck)
            if result:
                seen = {h.get("ClusterId") for h in self._history_cache}
                for h in result:
                    cid = h.get("ClusterId")
                    if cid not in seen:
                        self._history_cache.append(h)
                        seen.add(cid)
        except Exception:
            pass
        self._history_last_poll = now
        return self._history_cache

    def poll_pool(self, now: Optional[float] = None) -> Optional[PoolSummary]:
        """Poll pool slot status on a (coarser) throttled cadence."""
        if now is None:
            now = time.time()
        if self._backoff.fail_streak > 0:
            return self._pool_cache  # schedd failing — skip pool queries too
        if (
            self._pool_last_poll is not None
            and now - self._pool_last_poll < self._pool_interval
        ):
            return self._pool_cache
        try:
            pool_kwargs: Dict[str, Any] = {}
            for key in (
                "collector_host",
                "token_path",
                "cert_path",
                "key_path",
                "password_file",
            ):
                if self._ck.get(key):
                    pool_kwargs[key] = self._ck[key]
            self._pool_cache = query_slots(**pool_kwargs)
        except Exception:
            pass
        self._pool_last_poll = now
        return self._pool_cache

    def poll(self, now: Optional[float] = None) -> tuple:
        """Run a full cycle: condor first (updates back-off), then history, pool."""
        if now is None:
            now = time.time()
        condor_jobs = self.poll_condor(now)
        history = self.poll_history(now)
        pool = self.poll_pool(now)
        return condor_jobs, history, pool


# ─── Python bindings (preferred) ─────────────────────────────────────────────


_EVAL_MISS = object()


def _clean_classad_value(val: Any) -> Any:
    """Normalize a ClassAd-evaluated value for JSON serialization.

    Native JSON scalars pass through; ClassAd ``Undefined``/``Error`` sentinels
    become None and any residual ``ExprTree`` becomes its string form, so a
    stray expression never reaches a numeric consumer as a live object.
    """
    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    if type(val).__name__ == "Value":  # classad.Value.Undefined / .Error
        return None
    return str(val)


def _ad_to_dict(ad: Any) -> Dict[str, Any]:
    """Convert a ClassAd to a plain dict with expression attributes evaluated.

    ``dict(ad)`` leaves expression-valued attributes as ``classad.ExprTree``
    objects -- notably RequestMemory, whose HTCondor default is
    ``ifthenelse(MemoryUsage isnt undefined, MemoryUsage, (ImageSize + 1023) / 1024)``.
    Used directly (or serialized with ``json.dumps(default=str)``) those are the
    raw expression, which int()/float() consumers cannot parse. Evaluating each
    attribute in the ad's own context (``ClassAd.eval``) resolves RequestMemory
    and friends to numbers; attributes that cannot be evaluated fall back to
    their raw value. Non-ClassAd mappings round-trip unchanged.
    """
    keys_fn = getattr(ad, "keys", None)
    if keys_fn is None:
        return dict(ad)
    eval_fn = getattr(ad, "eval", None)
    out: Dict[str, Any] = {}
    for key in list(keys_fn()):
        val = _EVAL_MISS
        if eval_fn is not None:
            try:
                val = eval_fn(key)
            except Exception:
                val = _EVAL_MISS
        if val is _EVAL_MISS:
            try:
                val = ad[key]
            except Exception:
                continue
        out[key] = _clean_classad_value(val)
    return out


def _try_python_bindings(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
) -> Optional[List[Dict]]:
    """Attempt to query via htcondor or htcondor2 Python bindings."""
    # Configure token path before importing
    if token_path:
        os.environ.setdefault("_CONDOR_SEC_TOKEN_DIRECTORY", str(token_path))

    # Try legacy htcondor first, then htcondor2
    for mod_name in ("htcondor", "htcondor2"):
        try:
            import importlib

            ht = importlib.import_module(mod_name)
        except ImportError:
            continue

        try:
            if collector_host:
                ht.param["COLLECTOR_HOST"] = collector_host

            if schedd_name:
                try:
                    coll = ht.Collector(collector_host or "")
                    schedd_ad = coll.locate(ht.DaemonTypes.Schedd, schedd_name)
                    schedd = ht.Schedd(schedd_ad)
                except Exception:
                    schedd = ht.Schedd()
            else:
                schedd = ht.Schedd()

            projection = [
                # Identity & status
                "ClusterId",
                "ProcId",
                "JobStatus",
                "Cmd",
                "RemoteHost",
                "QDate",
                "JobStartDate",
                "DAGNodeName",
                "Owner",
                "HoldReason",
                "HoldReasonCode",
                # Resource requests (Tier 1)
                "RequestCpus",
                "RequestMemory",
                "RequestDisk",
                "RequestGpus",
                "ImageSize",
                "NumJobStarts",
                "AccountingGroup",
                # File transfer & I/O (Tier 2)
                "TransferInputSizeMB",
                "BytesSent",
                "BytesRecvd",
            ]
            q_args: Dict[str, Any] = {"projection": projection}
            if constraint:
                q_args["constraint"] = constraint

            jobs = [_ad_to_dict(ad) for ad in schedd.query(**q_args)]
            return jobs
        except Exception:
            continue

    return None


# ─── subprocess fallback ──────────────────────────────────────────────────────

_SUBPROCESS_ATTRS = [
    "ClusterId",
    "ProcId",
    "JobStatus",
    "Cmd",
    "RemoteHost",
    "QDate",
    "JobStartDate",
    "DAGNodeName",
    "Owner",
    "HoldReason",
    "HoldReasonCode",
    "RequestCpus",
    "RequestMemory",
    "RequestDisk",
    "RequestGpus",
    "ImageSize",
    "NumJobStarts",
    "AccountingGroup",
    "TransferInputSizeMB",
    "BytesSent",
    "BytesRecvd",
]


def _query_via_subprocess(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    raise_on_error: bool = False,
) -> List[Dict]:
    """Query HTCondor queue via ``condor_q -json``.

    With ``raise_on_error=True``, a *hard* failure (timeout, missing binary,
    non-zero exit, or unparseable output — i.e. the scheduler could not be
    reached/understood) raises :class:`CondorQueryError`.  An empty queue
    (exit 0, no jobs matched) is a valid result and returns ``[]`` regardless.
    """
    cmd = ["condor_q", "-json", "-attributes", ",".join(_SUBPROCESS_ATTRS)]
    if schedd_name:
        cmd += ["-name", schedd_name]
    if constraint:
        cmd += ["-constraint", constraint]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        if raise_on_error:
            raise CondorQueryError(f"condor_q failed: {exc}") from exc
        return []

    if result.returncode != 0:
        if raise_on_error:
            raise CondorQueryError(
                f"condor_q exited {result.returncode}: {result.stderr.strip()[:200]}"
            )
        return []
    if not result.stdout.strip():
        return []  # exit 0, no jobs matched — a valid empty result
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        if raise_on_error:
            raise CondorQueryError(
                f"condor_q returned unparseable JSON: {exc}"
            ) from exc
        return []
    return data if isinstance(data, list) else []


# ─── Public interface ─────────────────────────────────────────────────────────


def query_queue(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    password_file: Optional[str] = None,
    raise_on_error: bool = False,
) -> List[Dict]:
    """Query the HTCondor job queue.

    Returns a list of job ClassAd dicts (may be empty).

    By default never raises; errors are swallowed and an empty list is
    returned.  When ``raise_on_error=True``, a hard failure to reach/parse the
    scheduler raises :class:`CondorQueryError` (an empty-but-valid result still
    returns ``[]``).  The server poll loop uses this to drive adaptive back-off
    when the schedd is down or overloaded.

    Parameters
    ----------
    constraint:      HTCondor ClassAd expression to filter jobs.
    schedd_name:     Name of a specific schedd to query.
    collector_host:  Host[:port] of the pool collector.
    token_path:      Path to an IDTOKEN file or directory.
    cert_path:       Path to GSI certificate.
    key_path:        Path to GSI private key.
    password_file:   Path to a password file.
    raise_on_error:  Raise CondorQueryError on hard failure instead of
                     returning an empty list.
    """
    # Apply credential environment variables
    if cert_path:
        os.environ["X509_USER_CERT"] = str(cert_path)
    if key_path:
        os.environ["X509_USER_KEY"] = str(key_path)
    if password_file:
        os.environ["_CONDOR_PASSWORD_FILE"] = str(password_file)
    if collector_host:
        os.environ.setdefault("_CONDOR_COLLECTOR_HOST", str(collector_host))

    # Try Python bindings first
    result = _try_python_bindings(
        constraint=constraint,
        schedd_name=schedd_name,
        collector_host=collector_host,
        token_path=token_path,
    )
    if result is not None:
        return result

    # Fall back to subprocess. The bindings returning None means "unavailable
    # or failed to reach the schedd" — so the subprocess attempt is also our
    # reachability probe; let it surface a hard failure when asked.
    return _query_via_subprocess(
        constraint=constraint,
        schedd_name=schedd_name,
        raise_on_error=raise_on_error,
    )


# ─── History attributes (Tier 3) ────────────────────────────────────────────

_HISTORY_ATTRS = [
    "ClusterId",
    "ProcId",
    "DAGNodeName",
    "Owner",
    "Cmd",
    "JobStatus",
    "ExitCode",
    "RemoteWallClockTime",
    "RemoteUserCpu",
    "RemoteSysCpu",
    "CumulativeRemoteUserCpu",
    "RequestCpus",
    "RequestMemory",
    "RequestDisk",
    "RequestGpus",
    "ImageSize",
    "DiskUsage",
    "LastRemoteHost",
    "BytesSent",
    "BytesRecvd",
    "NumJobStarts",
]


def _try_history_bindings(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
    match: int = 200,
) -> Optional[List[Dict]]:
    """Attempt to query job history via Python bindings."""
    if token_path:
        os.environ.setdefault("_CONDOR_SEC_TOKEN_DIRECTORY", str(token_path))

    for mod_name in ("htcondor", "htcondor2"):
        try:
            import importlib

            ht = importlib.import_module(mod_name)
        except ImportError:
            continue

        try:
            if collector_host:
                ht.param["COLLECTOR_HOST"] = collector_host

            if schedd_name:
                try:
                    coll = ht.Collector(collector_host or "")
                    schedd_ad = coll.locate(ht.DaemonTypes.Schedd, schedd_name)
                    schedd = ht.Schedd(schedd_ad)
                except Exception:
                    schedd = ht.Schedd()
            else:
                schedd = ht.Schedd()

            h_args: Dict[str, Any] = {
                "projection": _HISTORY_ATTRS,
                "match": match,
            }
            if constraint:
                h_args["constraint"] = constraint

            jobs = [_ad_to_dict(ad) for ad in schedd.history(**h_args)]
            return jobs
        except Exception:
            continue

    return None


def _history_via_subprocess(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    match: int = 200,
) -> List[Dict]:
    """Query HTCondor history via ``condor_history -json``."""
    cmd = [
        "condor_history",
        "-json",
        "-attributes",
        ",".join(_HISTORY_ATTRS),
        "-match",
        str(match),
    ]
    if schedd_name:
        cmd += ["-name", schedd_name]
    if constraint:
        cmd += ["-constraint", constraint]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else []
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        return []


def query_history(
    constraint: Optional[str] = None,
    schedd_name: Optional[str] = None,
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    password_file: Optional[str] = None,
    match: int = 200,
) -> List[Dict]:
    """Query the HTCondor job history for completed jobs.

    Returns a list of job ClassAd dicts (may be empty).
    Never raises; errors are swallowed silently so the monitor
    continues even when condor_history is unavailable.

    Parameters
    ----------
    constraint:      HTCondor ClassAd expression to filter jobs.
    schedd_name:     Name of a specific schedd to query.
    collector_host:  Host[:port] of the pool collector.
    token_path:      Path to an IDTOKEN file or directory.
    cert_path:       Path to GSI certificate.
    key_path:        Path to GSI private key.
    password_file:   Path to a password file.
    match:           Maximum number of history records to return.
    """
    # Apply credential environment variables (same as query_queue)
    if cert_path:
        os.environ["X509_USER_CERT"] = str(cert_path)
    if key_path:
        os.environ["X509_USER_KEY"] = str(key_path)
    if password_file:
        os.environ["_CONDOR_PASSWORD_FILE"] = str(password_file)
    if collector_host:
        os.environ.setdefault("_CONDOR_COLLECTOR_HOST", str(collector_host))

    result = _try_history_bindings(
        constraint=constraint,
        schedd_name=schedd_name,
        collector_host=collector_host,
        token_path=token_path,
        match=match,
    )
    if result is not None:
        return result

    return _history_via_subprocess(
        constraint=constraint,
        schedd_name=schedd_name,
        match=match,
    )


def format_job_status(status_code: Any) -> str:
    try:
        return JOB_STATUS.get(int(status_code), str(status_code))
    except (TypeError, ValueError):
        return str(status_code)


def _coerce_int(value: Any) -> Optional[int]:
    """Best-effort int from a ClassAd value, or None if not a literal number.

    HTCondor request attributes are frequently unevaluated expressions rather
    than literals -- RequestMemory defaults to
    ``ifthenelse(MemoryUsage isnt undefined, MemoryUsage, (ImageSize + 1023) / 1024)``.
    Over the plugin/--remote JSON path these arrive as expression strings (the
    ``classad.ExprTree`` was stringified at serialization); in live mode they
    arrive as ExprTree objects. Neither is int()-able, so return None and let
    the caller omit the field rather than crash the display.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def format_resources(ad: Dict) -> str:
    """Format resource requests as a compact string like '2c/4G' or '1c/2G/1gpu'.

    Any request attribute that is an unevaluated ClassAd expression rather than
    a literal number is skipped (see _coerce_int)."""
    parts = []
    cpus = _coerce_int(ad.get("RequestCpus"))
    if cpus is not None:
        parts.append(f"{cpus}c")
    mem_mb = _coerce_int(ad.get("RequestMemory"))
    if mem_mb is not None:
        if mem_mb >= 1024:
            parts.append(f"{mem_mb / 1024:.1f}G")
        else:
            parts.append(f"{mem_mb}M")
    gpus = _coerce_int(ad.get("RequestGpus"))
    if gpus is not None and gpus > 0:
        parts.append(f"{gpus}gpu")
    return "/".join(parts) if parts else ""


def format_transfer(ad: Dict) -> str:
    """Format transfer bytes as compact string."""
    sent = ad.get("BytesSent", 0) or 0
    recvd = ad.get("BytesRecvd", 0) or 0
    total = sent + recvd
    if total == 0:
        return ""
    if total < 1024:
        return f"{total}B"
    if total < 1024 * 1024:
        return f"{total / 1024:.1f}K"
    if total < 1024 * 1024 * 1024:
        return f"{total / (1024 * 1024):.1f}M"
    return f"{total / (1024 * 1024 * 1024):.2f}G"


def queue_wait_seconds(ad: Dict) -> Optional[float]:
    """Compute how long the job waited in queue before starting."""
    qdate = ad.get("QDate")
    start = ad.get("JobStartDate")
    if qdate is not None and start is not None:
        wait = float(start) - float(qdate)
        return max(0.0, wait)
    return None


def cpu_efficiency(ad: Dict) -> Optional[float]:
    """Compute CPU efficiency as a fraction (0.0–1.0+).

    Formula: RemoteUserCpu / (RemoteWallClockTime * RequestCpus)
    Returns None if insufficient data is available.
    """
    wall = ad.get("RemoteWallClockTime")
    user_cpu = ad.get("RemoteUserCpu")
    cpus = ad.get("RequestCpus", 1)
    if wall is None or user_cpu is None:
        return None
    try:
        wall_f = float(wall)
        cpu_f = float(user_cpu)
        # Default only when RequestCpus is absent/None; a present 0 or negative
        # must fall through to the guard below, not be silently rewritten to 1.
        cpus_f = float(cpus) if cpus is not None else 1.0
        if wall_f <= 0 or cpus_f <= 0:
            return None
        return cpu_f / (wall_f * cpus_f)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def memory_efficiency(ad: Dict) -> Optional[float]:
    """Compute memory efficiency as a fraction (0.0–1.0+).

    Formula: ImageSize (KB) / (RequestMemory (MB) * 1024)
    Returns None if insufficient data is available.
    """
    image_kb = ad.get("ImageSize")
    req_mb = ad.get("RequestMemory")
    if image_kb is None or req_mb is None:
        return None
    try:
        image_f = float(image_kb)
        req_f = float(req_mb) * 1024.0  # convert MB to KB
        if req_f <= 0:
            return None
        return image_f / req_f
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def format_efficiency(eff: Optional[float]) -> str:
    """Format an efficiency fraction as a percentage string."""
    if eff is None:
        return "-"
    return f"{eff * 100:.0f}%"


def format_disk_usage(kb: Optional[Any]) -> str:
    """Format DiskUsage (KB) as a compact string."""
    if kb is None:
        return "-"
    try:
        kb_f = float(kb)
    except (TypeError, ValueError):
        return "-"
    if kb_f < 1024:
        return f"{kb_f:.0f}K"
    mb = kb_f / 1024
    if mb < 1024:
        return f"{mb:.1f}M"
    return f"{mb / 1024:.2f}G"


# ─── Slot / pool status (Tier 4) ───────────────────────────────────────────

# HTCondor slot Activity values
SLOT_ACTIVITY: Dict[str, str] = {
    "Idle": "Idle",
    "Busy": "Busy",
    "Suspended": "Susp",
    "Vacating": "Vac",
    "Benchmarking": "Bench",
    "Retiring": "Ret",
}

_SLOT_ATTRS = [
    "Name",
    "Machine",
    "SlotType",
    "Cpus",
    "Memory",
    "Disk",
    "TotalSlotCpus",
    "TotalSlotMemory",
    "TotalLoadAvg",
    "Activity",
    "State",
    "OpSys",
    "Arch",
    "GPUs",
]


@dataclass
class PoolSummary:
    """Aggregated pool resource summary from condor_status."""

    total_slots: int = 0
    idle_slots: int = 0
    claimed_slots: int = 0
    other_slots: int = 0  # Preempting, Vacating, etc.
    total_cpus: int = 0
    idle_cpus: int = 0
    total_memory_mb: int = 0
    idle_memory_mb: int = 0
    total_disk_kb: int = 0
    total_gpus: int = 0
    idle_gpus: int = 0
    machines: int = 0
    load_avg: float = 0.0
    os_arch: str = ""  # e.g. "LINUX/X86_64"
    # Raw slot ads for detailed inspection (not serialized to JSONL)
    slots: List[Dict] = field(default_factory=list, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-safe dict (excludes raw slot ads)."""
        return {
            "total_slots": self.total_slots,
            "idle_slots": self.idle_slots,
            "claimed_slots": self.claimed_slots,
            "other_slots": self.other_slots,
            "total_cpus": self.total_cpus,
            "idle_cpus": self.idle_cpus,
            "total_memory_mb": self.total_memory_mb,
            "idle_memory_mb": self.idle_memory_mb,
            "total_disk_kb": self.total_disk_kb,
            "total_gpus": self.total_gpus,
            "idle_gpus": self.idle_gpus,
            "machines": self.machines,
            "load_avg": self.load_avg,
            "os_arch": self.os_arch,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PoolSummary":
        """Reconstruct from a serialized dict."""
        return cls(
            total_slots=d.get("total_slots", 0),
            idle_slots=d.get("idle_slots", 0),
            claimed_slots=d.get("claimed_slots", 0),
            other_slots=d.get("other_slots", 0),
            total_cpus=d.get("total_cpus", 0),
            idle_cpus=d.get("idle_cpus", 0),
            total_memory_mb=d.get("total_memory_mb", 0),
            idle_memory_mb=d.get("idle_memory_mb", 0),
            total_disk_kb=d.get("total_disk_kb", 0),
            total_gpus=d.get("total_gpus", 0),
            idle_gpus=d.get("idle_gpus", 0),
            machines=d.get("machines", 0),
            load_avg=d.get("load_avg", 0.0),
            os_arch=d.get("os_arch", ""),
        )


def _summarize_slots(slots: List[Dict]) -> PoolSummary:
    """Aggregate raw slot ClassAds into a PoolSummary."""
    summary = PoolSummary(slots=slots)
    machines: set = set()
    os_arch_set: set = set()

    for s in slots:
        slot_type = s.get("SlotType", "")
        # Skip partitionable parent slots — their child dynamic slots
        # represent the actual claimed resources.  Counting the parent
        # would double-count idle resources.
        if slot_type == "Partitionable":
            # Still count machine and load from partitionable slots
            machine = s.get("Machine", "")
            if machine:
                machines.add(machine)
            load = s.get("TotalLoadAvg")
            if load is not None:
                try:
                    summary.load_avg += float(load)
                except (TypeError, ValueError):
                    pass
            opsys = s.get("OpSys", "")
            arch = s.get("Arch", "")
            if opsys or arch:
                os_arch_set.add(f"{opsys}/{arch}")

            # For partitionable slots, count their available resources
            # as idle pool capacity
            cpus = s.get("Cpus", 0)
            mem = s.get("Memory", 0)
            disk = s.get("Disk", 0)
            gpus = s.get("GPUs", 0)
            try:
                total_cpus = int(s.get("TotalSlotCpus", cpus) or 0)
                total_mem = int(s.get("TotalSlotMemory", mem) or 0)
                summary.total_cpus += total_cpus
                summary.total_memory_mb += total_mem
                summary.idle_cpus += int(cpus or 0)
                summary.idle_memory_mb += int(mem or 0)
            except (TypeError, ValueError):
                pass
            try:
                summary.total_disk_kb += int(disk or 0)
            except (TypeError, ValueError):
                pass
            try:
                g = int(gpus or 0)
                summary.total_gpus += g
                summary.idle_gpus += g
            except (TypeError, ValueError):
                pass
            continue

        # Dynamic or static slots
        summary.total_slots += 1
        activity = s.get("Activity", "")
        state = s.get("State", "")

        if state == "Unclaimed" or activity == "Idle":
            summary.idle_slots += 1
        elif state == "Claimed":
            summary.claimed_slots += 1
        else:
            summary.other_slots += 1

        machine = s.get("Machine", "")
        if machine:
            machines.add(machine)

        cpus = s.get("Cpus", 0)
        mem = s.get("Memory", 0)
        disk = s.get("Disk", 0)
        gpus = s.get("GPUs", 0)

        try:
            c = int(cpus or 0)
            summary.total_cpus += c
            if state == "Unclaimed" or activity == "Idle":
                summary.idle_cpus += c
        except (TypeError, ValueError):
            pass
        try:
            m = int(mem or 0)
            summary.total_memory_mb += m
            if state == "Unclaimed" or activity == "Idle":
                summary.idle_memory_mb += m
        except (TypeError, ValueError):
            pass
        try:
            summary.total_disk_kb += int(disk or 0)
        except (TypeError, ValueError):
            pass
        try:
            g = int(gpus or 0)
            summary.total_gpus += g
            if state == "Unclaimed" or activity == "Idle":
                summary.idle_gpus += g
        except (TypeError, ValueError):
            pass

        opsys = s.get("OpSys", "")
        arch = s.get("Arch", "")
        if opsys or arch:
            os_arch_set.add(f"{opsys}/{arch}")

    summary.machines = len(machines)
    if os_arch_set:
        summary.os_arch = ", ".join(sorted(os_arch_set))

    return summary


def _try_slots_bindings(
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
) -> Optional[List[Dict]]:
    """Attempt to query startd ads via Python bindings."""
    if token_path:
        os.environ.setdefault("_CONDOR_SEC_TOKEN_DIRECTORY", str(token_path))

    for mod_name in ("htcondor", "htcondor2"):
        try:
            import importlib

            ht = importlib.import_module(mod_name)
        except ImportError:
            continue

        try:
            if collector_host:
                ht.param["COLLECTOR_HOST"] = collector_host

            coll = ht.Collector(collector_host or "")
            ads = coll.query(
                ht.AdTypes.Startd,
                projection=_SLOT_ATTRS,
            )
            return [_ad_to_dict(ad) for ad in ads]
        except Exception:
            continue

    return None


def _slots_via_subprocess(
    collector_host: Optional[str] = None,
) -> List[Dict]:
    """Query slot status via ``condor_status -json``."""
    cmd = [
        "condor_status",
        "-json",
        "-attributes",
        ",".join(_SLOT_ATTRS),
    ]
    if collector_host:
        cmd += ["-pool", collector_host]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else []
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        return []


def query_slots(
    collector_host: Optional[str] = None,
    token_path: Optional[str] = None,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    password_file: Optional[str] = None,
) -> Optional[PoolSummary]:
    """Query HTCondor pool slot status and return a PoolSummary.

    Returns None if no data is available (condor_status unavailable,
    no startd daemons, etc.).  Never raises.

    Parameters
    ----------
    collector_host:  Host[:port] of the pool collector.
    token_path:      Path to an IDTOKEN file or directory.
    cert_path:       Path to GSI certificate.
    key_path:        Path to GSI private key.
    password_file:   Path to a password file.
    """
    if cert_path:
        os.environ["X509_USER_CERT"] = str(cert_path)
    if key_path:
        os.environ["X509_USER_KEY"] = str(key_path)
    if password_file:
        os.environ["_CONDOR_PASSWORD_FILE"] = str(password_file)
    if collector_host:
        os.environ.setdefault("_CONDOR_COLLECTOR_HOST", str(collector_host))

    slots = _try_slots_bindings(
        collector_host=collector_host,
        token_path=token_path,
    )
    if slots is None:
        slots = _slots_via_subprocess(collector_host=collector_host)

    if not slots:
        return None

    return _summarize_slots(slots)
