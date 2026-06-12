# workflow-monitor

A real-time terminal dashboard for monitoring running [Pegasus WMS](https://pegasus.isi.edu) workflows. Reads directly from the data sources that Pegasus and HTCondor produce — no source code modifications required, no additional daemons to run.

```
╭───────────────────────────────────────────────────────────────────────────────────────────╮
│  Pegasus Workflow Monitor   LIVE                                    Refreshed 14:23:47    │
│  workflow: earthquake  uuid: f886d43d…  user: ubuntu                    pegasus 5.1.2     │
╰───────────────────────────────────────────────────────────────────────────────────────────╯
╭──────────────────────────────── Workflow Status ──────────────────────────────────────────╮
│  ● RUNNING   Elapsed: 2m48s   54.2%   13/24 done   Done:13  Run:3  Queued:1  Wait:7       │
│  [██████████████████████████░░░░░░░░░░░░░░░░░░░░░░]                                       │
╰───────────────────────────────────────────────────────────────────────────────────────────╯
╭──────────────────────────────── Compute Jobs ─────────────────────────────────────────────╮
│  Job                        Type    State    Exit  Duration  Args              Mem    Req │
│  visualize_seismic_gaps     compute RUNNING   -        5s   --input cali…       -  1c/2G  │
│  cluster_seismic_zones      compute RUNNING   -       18s   --input cali…       -  1c/2G  │
│  assess_seismic_hazard      compute RUNNING   -       42s   --input cali…       -  1c/2G  │
│  visualize_earthquakes      compute SUCCESS   0       11s   --input cali…  324.0M  1c/2G  │
│  predict_aftershocks        compute SUCCESS   0       10s   --input cali…  101.1M  1c/4G  │
│  analyze_seismic_patterns   compute SUCCESS   0        9s   --input cali…  101.4M  1c/2G  │
│  analyze_seismic_gaps       compute SUCCESS   0       10s   --input cali…  101.6M  1c/2G  │
│  detect_seismic_anomalies   compute SUCCESS   0       11s   --input cali…  101.7M  1c/2G  │
│  fetch_earthquake_data      compute SUCCESS   0        8s   --region cali…  69.1M  1c/2G  │
│  visualize_aftershock_…     compute QUEUED    -        -    --input cali…       -  1c/2G  │
│  visualize_seismic_hazard   compute   -       -        -    --input cali…       -  1c/2G  │
╰───────────────────────────────────────────────────────────────────────────────────────────╯
╭──────────────────────────────── Pool Resources ───────────────────────────────────────────╮
│  Machines: 3   Slots: 3/20 claimed (17 idle)   CPUs: 3/20 (17 idle)                       │
│  Memory: 6.0G/39.0G (33.0G free)   Platform: LINUX/X86_64                                 │
╰───────────────────────────────────────────────────────────────────────────────────────────╯
╭──────────────────────────────── Recent Events ────────────────────────────────────────────╮
│  Job                        State    Start      End        Duration    Mem                │
│  visualize_seismic_gaps     RUNNING  14:23:42   -               5s      -                 │
│  cluster_seismic_zones      RUNNING  14:21:29   -              18s      -                 │
│  assess_seismic_hazard      RUNNING  14:21:05   -              42s      -                 │
│  visualize_earthquakes      SUCCESS  14:21:29   14:21:40       11s  324.0M                │
│  predict_aftershocks        SUCCESS  14:21:29   14:21:39       10s  101.1M                │
│  analyze_seismic_patterns   SUCCESS  14:20:37   14:20:46        9s  101.4M                │
╰───────────────────────────────────────────────────────────────────────────────────────────╯
```

## Features

- **Near real-time** — polls the Pegasus stampede database every 2 seconds (configurable)
- **Live HTCondor queue** — overlays live `condor_q` status on running jobs with resource requests, queue wait time, retry count, and transfer I/O
- **Post-completion metrics** — queries `condor_history` for CPU/memory efficiency, disk usage, and remote host after jobs finish
- **Pool resources** — queries `condor_status` for pool-wide slot, CPU, memory, and GPU availability; helps diagnose why jobs are idle
- **Diagnostics** — pattern-matches HTCondor hold reasons and kickstart stderr to surface actionable suggestions for held and failed jobs
- **Why-idle diagnostic** — one-shot `--why-idle` command explains why workflow jobs are stuck idle by checking pool capacity vs. job requirements, user fair-share priority, and negotiation cycle timing
- **Diagnostics layer (`--diagnose`)** — opt-in stall detector + auto-diagnosis that runs alongside any monitoring mode and writes a `diagnostics-events.jsonl` sidecar file. Detects four kinds of stalls (`all_held`, `no_transitions`, `progress_plateau`, `idle_too_long`), runs the why-idle and held/failed analyzers automatically when a stall is confirmed, and surfaces a red `STALL` alert panel in the TUI. Remote SSH clients fetch the sidecar best-effort and display the same alerts.
- **Activity-sorted job table** — by default the Compute Jobs table places `RUNNING` jobs at the top (most recently started first), then everything else by most-recent activity, so currently-active work is always visible without scrolling. Disable with `--no-sort-by-activity` to keep the original DAG/submit order.
- **Zero workflow modification** — reads only from files Pegasus and HTCondor already produce
- **Credential-aware** — supports IDTOKEN, X.509/GSI certificates, and password file auth for remote pools; local pools need nothing
- **Flexible target** — point at a workflow base directory, a specific run directory, or a `braindump.yml` file directly
- **Workflow statistics** — prints an analytics summary on completion covering job durations, CPU/memory efficiency, queue wait, transfer I/O, parallelism, and host distribution; also emitted as a `workflow_stats` JSONL event for post-hoc analysis
- **Non-interactive mode** — `--once` for scripting, CI, or quick status checks
- **Event logging** — `--log` captures every workflow and job state transition to a JSONL file for replay or post-hoc analysis
- **Replay mode** — `--replay` reads a JSONL event log and visually replays the workflow in the TUI at configurable speed (`--speed`)
- **Client/Server mode** — `--serve` runs a headless daemon on the workflow machine that logs events continuously; `--remote` connects from any machine via SSH to display the workflow in the TUI
- **pegasus-monitord plugin** — on Pegasus builds with the monitord plugin system, the bundled `wfmonitor` plugin runs *inside* `pegasus-monitord` and writes the same JSONL schema event-driven (no polling, no second process), optionally including the HTCondor queue/history/pool polling on the plugin's own thread — see [`MONITORD-PLUGIN.md`](MONITORD-PLUGIN.md)

## Requirements

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) package manager
- A planned and running (or completed) Pegasus workflow (not needed for `--replay` or `--remote`)
- HTCondor with `condor_q` on `PATH` (for live queue data; not needed for `--replay` or `--remote`)
- SSH access to the remote host (for `--remote` client mode only)

## Installation

Clone the repository and sync dependencies with uv:

```bash
git clone <repo-url> workflow-monitor
cd workflow-monitor
uv sync
```

The `workflow-monitor` command is installed into the project's virtual environment and available via `uv run`.

### Optional: HTCondor Python bindings

If the `htcondor` Python package is available and architecture-compatible, the monitor will prefer it over the `condor_q` subprocess for live queue data:

```bash
uv sync --extra htcondor
```

On systems where the bindings are unavailable (e.g., architecture mismatch on macOS), the monitor falls back to `condor_q -json` automatically with no configuration needed.

## Quick Start

```bash
# Monitor the latest run under a workflow directory (live TUI, Ctrl+C to exit)
uv run workflow-monitor /path/to/diamond-workflow

# Monitor a specific run directory
uv run workflow-monitor /path/to/submit/stealey/pegasus/diamond/run0003

# Print current status once and exit
uv run workflow-monitor --once /path/to/diamond-workflow

# Monitor with JSONL event logging (auto-path in submit dir)
uv run workflow-monitor --log /path/to/diamond-workflow

# Log events to a specific file
uv run workflow-monitor --log /tmp/events.jsonl /path/to/diamond-workflow

# Replay a recorded event log (no live workflow or HTCondor needed)
uv run workflow-monitor --replay example-logs/workflow-events.jsonl

# Replay at 4x speed
uv run workflow-monitor --replay example-logs/workflow-events.jsonl --speed 4

# Start a headless server on the workflow machine (daemonizes, survives terminal exit)
uv run workflow-monitor --serve /path/to/diamond-workflow

# Monitor a remote workflow via SSH from another machine
uv run workflow-monitor --remote user@host:/path/to/submit_dir/workflow-events.jsonl

# Remote monitoring with SSH config and identity file (e.g. FABRIC testbed)
uv run workflow-monitor \
  --remote 'ubuntu@[2001:db8::1]:/home/ubuntu/pegasus/earthquake/run0001/workflow-events.jsonl' \
  --ssh-config ~/.ssh/fabric-ssh-config \
  --ssh-identity ~/.ssh/my-sliver-key

# Diagnose why jobs are stuck idle (one-shot, prints and exits)
uv run workflow-monitor --why-idle /path/to/diamond-workflow

# Run with the diagnostics layer enabled (stall detection + auto-diagnosis sidecar)
uv run workflow-monitor --diagnose /path/to/diamond-workflow
uv run workflow-monitor --diagnose --serve /path/to/diamond-workflow

# Stop a running server daemon
uv run workflow-monitor --stop-server /path/to/diamond-workflow
```

## Usage

```
usage: workflow-monitor [-h] [--version] [--interval SECONDS] [--all-jobs]
                        [--sort-by-activity | --no-sort-by-activity]
                        [--events N] [--once] [--why-idle] [--diagnose]
                        [--log [PATH]]
                        [--replay PATH] [--speed MULTIPLIER] [--serve]
                        [--serve-foreground] [--stop-server [PID_FILE]]
                        [--remote USER@HOST:/PATH] [--sync-interval SECONDS]
                        [--ssh-config PATH] [--ssh-identity PATH]
                        [--schedd NAME] [--collector HOST[:PORT]]
                        [--token PATH] [--cert PATH] [--key PATH]
                        [--password-file PATH]
                        [TARGET]
```

### Positional argument

| Argument | Description |
|----------|-------------|
| `TARGET` | Workflow submit directory, base directory, or `braindump.yml` file. Defaults to the current working directory. |

The `TARGET` is resolved in this order:
1. If it is a `braindump.yml` file, use it directly.
2. If it is a directory containing `braindump.yml`, use that run.
3. Otherwise, search recursively for `braindump.yml` files and use the **latest run** found (highest-numbered `runNNNN` directory).

### General options

| Flag | Default | Description |
|------|---------|-------------|
| `--interval SECONDS`, `-i` | `2.0` | Stampede database poll interval in seconds. |
| `--all-jobs`, `-a` | off | Show all job types (stage-in/out, cleanup, etc.) in the job table, not just compute jobs. |
| `--sort-by-activity` / `--no-sort-by-activity` | on | Order the Compute Jobs table by most-recent activity (`RUNNING` first, then by most-recent timestamp; `UNSUBMITTED` at the bottom). Use `--no-sort-by-activity` to keep the underlying DAG/submit order. See [Compute Jobs](#compute-jobs-or-all-jobs-with---all-jobs). |
| `--events N`, `-e` | `15` | Number of recent job-state events to show in the events panel. |
| `--once` | off | Print the current status once and exit. Useful for scripting. Works with all modes including `--remote`. |
| `--why-idle` | off | One-shot diagnostic: explain why workflow jobs are idle, then exit. Checks pool capacity, user priority, and negotiation cycles. |
| `--diagnose` | off | Enable the diagnostics layer alongside any monitoring mode. Runs stall detection on each poll cycle, auto-diagnoses confirmed stalls and held/failed jobs, and writes a `diagnostics-events.jsonl` sidecar next to the event log. Surfaces a red `STALL` alert panel in the TUI. |
| `--log [PATH]` | off | Log all events to a JSONL file. If `PATH` is omitted, writes to `{submit_dir}/workflow-events.jsonl`. |
| `--replay PATH` | — | Replay a JSONL event log in the TUI dashboard (no live workflow needed). |
| `--speed MULTIPLIER` | `1.0` | Replay speed multiplier (e.g. `4` = 4x speed, `0.5` = half speed). Only used with `--replay`. |
| `--remap-submit-dir {auto,always,never}` | `auto` | How to interpret `submit_dir` from `braindump.yml`. `auto` (default) rebases onto the directory containing the discovered `braindump.yml` only when the recorded path doesn't exist locally — handles workflows planned inside a container and viewed from the host via a bind mount. `always` forces rebasing. `never` trusts the recorded path verbatim. See [Container-planned workflows](#container-planned-workflows). |
| `--version`, `-V` | — | Print the version and exit. |

### HTCondor options

| Flag | Description |
|------|-------------|
| `--schedd NAME` | Query a specific named `condor_schedd`. Useful when multiple schedds are present or for remote pools. |
| `--collector HOST[:PORT]` | Address of the HTCondor collector for the target pool (e.g. `cm.example.org:9618`). |

### Credential options

Credentials are **not required** for local pools configured with open read access (`ALLOW_READ = *`). For secured or remote pools, pass credentials via flags or environment variables.

| Flag | Environment variable | Description |
|------|---------------------|-------------|
| `--token PATH` | `_CONDOR_SEC_TOKEN_DIRECTORY` | Path to an HTCondor IDTOKEN file or a directory containing tokens. |
| `--cert PATH` | `X509_USER_CERT` | Path to a GSI / X.509 certificate file. |
| `--key PATH` | `X509_USER_KEY` | Path to the private key corresponding to `--cert`. |
| `--password-file PATH` | `_CONDOR_PASSWORD_FILE` | Path to an HTCondor password file. |

Credentials set via environment variables before invocation are also respected; the flags take precedence.

## Operating Modes

The monitor has four operating modes, each indicated by a badge in the dashboard header:

| Mode | Badge | Data source | Requires |
|------|-------|-------------|----------|
| **Live** | `LIVE` (green) | stampede.db + condor_q | Local Pegasus workflow + HTCondor |
| **Server** | *(headless, no TUI)* | stampede.db + condor_q → JSONL | Local Pegasus workflow + HTCondor |
| **Remote/SSH** | `SSH` (blue) | JSONL via SSH | SSH access to server host |
| **Replay** | `REPLAY` (orange) | JSONL file | Nothing (fully offline) |

There are also three diagnostic-oriented options that compose with any of the modes above:

| Option | What it does | When to use |
|--------|--------------|-------------|
| `--once` | Polls once, prints a static dashboard, exits | Cron jobs, CI status checks, scripting, quick "is it still running?" probes |
| `--why-idle` | One-shot diagnostic for stuck-idle workflows | When jobs sit `QUEUED` and you don't know why (capacity? priority? requirements mismatch?) |
| `--diagnose` | Continuous stall detection + auto-diagnosis sidecar | Long unattended runs — surfaces alerts when something goes wrong without you having to watch |

### Choosing the right mode

| Situation | Recommended invocation |
|-----------|------------------------|
| You're SSH'd into the submit node and want to watch a workflow | `uv run workflow-monitor /path/to/run` (live mode) |
| You want to see status from your laptop while the workflow runs on a remote machine | Start `--serve` on the submit node, then `--remote` from your laptop |
| The submit node is reachable only via a bastion / ProxyJump | `--remote` with `--ssh-config` and `--ssh-identity` (see [FABRIC example](#example-fabric-testbed)) |
| You're debugging a completed workflow and have its event log | `--replay path/to/workflow-events.jsonl` |
| You want to keep a historical record of how a run unfolded | `--log` (live mode + log) or `--serve` (headless logger) |
| A long-running workflow has stalled and you want to know why | `--why-idle /path/to/run` for an immediate one-shot diagnosis |
| You're starting a long unattended run and want stall alerts surfaced automatically | Add `--diagnose` to whichever mode you're using |
| You need to script a status check or feed status into another tool | `--once` (works in live, replay, and remote modes) |
| The workflow was planned inside a container, viewed from the host | Live mode works as-is — `--remap-submit-dir auto` (default) detects and remaps. See [Container-planned workflows](#container-planned-workflows). |

A few rules of thumb:

- **Live is the richest mode.** Kickstart stderr is parsed directly from disk, every diagnostic source is available, and there's no SSH latency. Prefer it whenever you're on the submit node.
- **Server + Remote is for operators away from the submit node.** The server captures HTCondor ClassAds (including hold reasons) into the JSONL so the remote client gets the same diagnostics — but only what was captured at write time.
- **Replay is fully offline.** It does not require Pegasus, HTCondor, or even the original submit directory. The JSONL is self-contained.
- **`--once`, `--why-idle`, and `--diagnose` are orthogonal to the modes.** `--once` works in live, replay, and remote; `--why-idle` and `--diagnose` work locally; `--diagnose` events propagate to remote clients via a sidecar JSONL.

### Live mode (default)

Polls the stampede database and HTCondor queue directly on the same machine where the workflow is running. This is the richest mode — all diagnostics, including kickstart stderr analysis, are available.

### Server mode (`--serve`)

Runs the same polling loop as live mode but without a terminal UI. Writes a JSONL event log continuously. Designed to run on the workflow machine (submit node) so that remote clients can consume the log.

The server captures HTCondor ClassAd data (including `HoldReason`, `JobStatus`) in `htcondor_poll` events, making diagnostics available to SSH clients. The event stream uses fingerprint-based deduplication — changes to job attributes (not just the set of job IDs) trigger new events.

```bash
# Start as daemon (double-fork, survives terminal exit)
uv run workflow-monitor --serve /path/to/workflow

# Start in foreground (for debugging; Ctrl+C to stop)
uv run workflow-monitor --serve-foreground /path/to/workflow

# Start with custom log path and poll interval
uv run workflow-monitor --serve --log /tmp/my-events.jsonl --interval 5 /path/to/workflow

# Stop a running daemon
uv run workflow-monitor --stop-server /path/to/workflow
```

When `--serve` is used, the monitor:
1. Locates the workflow (same as normal mode)
2. Prints the log file path and PID file location
3. Double-forks to detach from the terminal (daemonize)
4. Polls the stampede database at `--interval` and writes events to the JSONL log
5. Exits automatically when the workflow completes, writing a final `workflow_end` event
6. Cleans up the PID file on exit

The PID file is written alongside the log file with a `.pid` extension (e.g., `workflow-events.pid`). Daemon stdout/stderr is redirected to a `.pid.log` file for diagnostics.

**Tip:** On some systems (e.g., FABRIC), `screen -dmS monitor uv run workflow-monitor --serve-foreground ...` is more reliable than the double-fork daemon.

### Remote/SSH mode (`--remote`)

Fetches the JSONL log from a remote machine via SSH and displays it in the TUI. The header shows an `SSH` badge with the remote host. After the initial full download, only new bytes are transferred each sync cycle using incremental byte-offset fetching (`tail -c +N`), minimizing bandwidth and memory usage.

```bash
# Basic usage
uv run workflow-monitor --remote user@host:/path/to/workflow-events.jsonl

# With custom sync interval (default: 5 seconds)
uv run workflow-monitor --remote user@host:/path/to/events.jsonl --sync-interval 10

# Print once and exit (non-interactive)
uv run workflow-monitor --remote user@host:/path/to/events.jsonl --once

# Show all job types
uv run workflow-monitor --remote user@host:/path/to/events.jsonl --all-jobs
```

The client can connect and disconnect at any time without affecting the workflow or the server. When the client reconnects, it catches up to the current state automatically. The server's resume-aware event logger ensures no duplicate headers or events across restarts.

#### SSH configuration

For hosts that require specific SSH settings (config files, identity keys, bastion/jump hosts), use `--ssh-config` and `--ssh-identity`:

```bash
uv run workflow-monitor \
  --remote 'ubuntu@[2001:db8::1]:/home/ubuntu/pegasus/earthquake/run0001/workflow-events.jsonl' \
  --ssh-config ~/.ssh/my-ssh-config \
  --ssh-identity ~/.ssh/my-private-key
```

These flags are passed directly to `ssh -F` and `ssh -i` respectively, so any SSH config features are supported — including `ProxyJump` for bastion hosts, `StrictHostKeyChecking`, and `ServerAliveInterval`.

**IPv6 addresses** must be enclosed in square brackets in the remote spec (e.g., `user@[2001:db8::1]:/path`). The brackets are stripped automatically before passing to SSH.

#### Example: FABRIC testbed

[FABRIC](https://fabric-testbed.net/) uses a bastion host with ProxyJump. A typical SSH config (`~/.ssh/fabric-ssh-config`):

```
UserKnownHostsFile /dev/null
StrictHostKeyChecking no
ServerAliveInterval 120

Host bastion.fabric-testbed.net
     User <FABRIC_BASTION_USERNAME>
     ForwardAgent yes
     Hostname %h
     IdentityFile <FABRIC_BASTION_PRIVATE_KEY_LOCATION>
     IdentitiesOnly yes

Host * !bastion.fabric-testbed.net
     ProxyJump <FABRIC_BASTION_USERNAME>@bastion.fabric-testbed.net:22
```

Monitor command:

```bash
uv run workflow-monitor \
  --remote 'ubuntu@[2001:400:a100:3030:f816:3eff:fe0b:f146]:/home/ubuntu/earthquake-workflow/ubuntu/pegasus/earthquake/run0002/workflow-events.jsonl' \
  --ssh-config ~/.ssh/fabric-ssh-config \
  --ssh-identity ~/.ssh/pegasusai-sliver
```

### Replay mode (`--replay`)

Reads a JSONL event log (produced by `--log` or `--serve`) and visually replays the workflow in the same Rich TUI dashboard. No live Pegasus workflow or HTCondor environment is required — useful for demos, post-hoc review, and debugging.

```bash
# Replay at normal speed
uv run workflow-monitor --replay example-logs/workflow-events.jsonl

# Replay at 4x speed
uv run workflow-monitor --replay example-logs/workflow-events.jsonl --speed 4

# Slow-motion replay
uv run workflow-monitor --replay example-logs/workflow-events.jsonl --speed 0.5

# Show all job types (including infrastructure)
uv run workflow-monitor --replay example-logs/workflow-events.jsonl --all-jobs
```

The header displays a `REPLAY` badge and the current speed multiplier. Events are grouped by timestamp and paced according to the original timing, scaled by `--speed`. Press `Ctrl+C` to exit early.

### Why-idle diagnostic (`--why-idle`)

A focused, one-shot command that answers "why are my jobs stuck idle?" without adding to the continuous monitoring loop. It gathers data from multiple sources, analyzes it, and prints a human-readable diagnosis.

```bash
# Basic usage — prints diagnosis and exits
uv run workflow-monitor --why-idle /path/to/workflow

# With remote pool credentials
uv run workflow-monitor --why-idle --collector cm.example.org --token /path/to/token /path/to/workflow
```

The diagnostic checks:

1. **Workflow state** — identifies which jobs are QUEUED (in HTCondor, waiting for a slot) vs. UNSUBMITTED (waiting on DAG dependencies)
2. **Pool capacity** — queries `condor_status` to compare available CPUs, memory, and GPUs against job resource requests
3. **Resource mismatches** — flags specific jobs whose requirements exceed available pool capacity (e.g., "needs 8GB RAM but only 4GB idle")
4. **User priority** — runs `condor_userprio` to show your fair-share priority relative to other users on shared pools
5. **Negotiation cycles** — queries the negotiator daemon for cycle duration and match count to detect matchmaking bottlenecks

Output includes tables for idle jobs, pool resources, and user priority, followed by numbered findings and actionable suggestions. Data sources that are unavailable (e.g., `condor_userprio` on a single-user pool) are silently skipped — the diagnostic works with whatever is available.

### Diagnostics layer (`--diagnose`)

An opt-in additive layer that runs alongside the normal monitoring loop. When enabled, the monitor instantiates a `DiagnosticsEngine` that owns a stall-detection state machine, a per-job dedup set for hold/failure analysis, and a sidecar JSONL writer. Diagnostic events are written to a **separate** file (`diagnostics-events.jsonl`) next to the main event log, so the canonical replay stream is never modified.

```bash
# Local live mode with the diagnostics overlay
uv run workflow-monitor --diagnose /path/to/workflow

# Headless server with sidecar (writes diagnostics-events.jsonl alongside workflow-events.jsonl)
uv run workflow-monitor --diagnose --serve /path/to/workflow

# --diagnose works with --once, --serve-foreground, and combined with --log
uv run workflow-monitor --diagnose --log /path/to/workflow
```

**Stall detection heuristics** (evaluated in priority order):

| # | Name | Trigger | Default threshold |
|---|------|---------|-------------------|
| 1 | `all_held` | Every active (non-done, non-failed) job is HELD | Immediate |
| 2 | `no_transitions` | No job state transitions while jobs are queued or running | 60s |
| 3 | `progress_plateau` | Done count flat while jobs are running | 120s |
| 4 | `idle_too_long` | Jobs queued with 0 running | 120s |

False-positive guards: a 60-second startup grace window, skip-when-complete, skip-when-no-jobs-submitted, and a **two-strike** confirmation rule (a heuristic must fire on two consecutive cycles before being confirmed). After a confirmed stall, a 120s cooldown suppresses repeated alerts of the same type. Stalls are automatically resolved when the done count advances or new state transitions arrive, emitting a `stall_resolved` event.

**Auto-diagnosis on confirmed stall:**
- If queued jobs exist, the engine reuses `why_idle._analyze()` to gather pool capacity, user priority, and negotiator data, emitting an `idle_diagnosis` event with structured findings and suggestions.
- Independently of stall state, every poll cycle runs `diagnostics.collect_diagnostics()` over held/failed jobs, emitting `hold_diagnosis` / `failure_diagnosis` events for each newly problematic job (deduped by `severity:job_name`).

**Diagnostic event types** (all written to `diagnostics-events.jsonl`):

| Event | Emitted when |
|-------|--------------|
| `diag_start` | Engine starts. Includes `schema_version` and the engine's threshold config so a run is auditable. |
| `stall_detected` | A heuristic fires on two consecutive cycles. Includes `stall_type`, `details`, and current job counts. |
| `idle_diagnosis` | Immediately after `stall_detected` if queued jobs exist. Contains `findings`, `suggestions`, `requirement_mismatches`, and pool capacity. |
| `hold_diagnosis` | A held job is matched against `_HOLD_PATTERNS`. Contains `summary`, `reason`, `suggestions`. |
| `failure_diagnosis` | A failed job is analyzed (kickstart stderr + exit code). |
| `stall_resolved` | Progress resumes after a confirmed stall. Includes `stall_duration_seconds`. |
| `diag_end` | Engine shuts down cleanly. |
| `diag_error` | An exception occurred inside `idle_diagnosis` or `held_failed` analysis. The monitor loop is never broken by engine errors. |

**TUI alert panel:** When alerts are active, a red `STALL DETECTED` panel appears between the status bar and the main job table, summarizing the stall type, top findings/suggestions from the most recent idle diagnosis, and the latest held/failed job summaries. A `STALL` badge is also added to the header. The panel disappears when `stall_resolved` clears the active alerts.

**Remote (`--remote`) integration:** The SSH client fetches `diagnostics-events.jsonl` best-effort on each sync cycle (silently skipped if the server isn't running with `--diagnose`, or if the file doesn't exist yet). Stall alerts surface in the remote TUI exactly as they do locally.

### Container-planned workflows

When a workflow is planned and submitted inside a container that bind-mounts data to the host, two things differ from a normal submit:

1. The recorded `submit_dir` in `braindump.yml` is the **container's** path (e.g. `/work/work/pegasus/.../run0001`), not the host path you see at the bind-mount destination (e.g. `/Users/me/project/work/pegasus/.../run0001`).
2. HTCondor inside the container reports job `Cmd` values rooted at the container path, while the host's terminal can only access the host path.

The monitor handles both:

**Path remapping (`--remap-submit-dir`)**

`--remap-submit-dir` is `auto` by default. The monitor checks whether the recorded `submit_dir` exists on this host:

- **Recorded path exists** (normal submit-host case) → trust it verbatim. Behavior unchanged.
- **Recorded path does not exist** (container-planned, host-viewed case) → rebase `submit_dir` onto the directory containing the discovered `braindump.yml`, and rebase `basedir` using the same depth offset.

To force rebasing regardless: `--remap-submit-dir always`. To disable auto-detection (e.g. on a network filesystem where the recorded path coincidentally exists but is wrong): `--remap-submit-dir never`.

**`Cmd` constraint preservation**

When restricting `condor_q` results to "this workflow only", the monitor matches against the **recorded** (un-remapped) `submit_dir` — because that's what the schedd reports in `Cmd`. This means the filter works correctly whether you're querying the host's pool or the container's pool.

**Querying the container's HTCondor pool from the host**

Path remapping fixes the file-side mismatch but does **not** redirect HTCondor calls. By default the monitor talks to whichever pool the host's `condor` environment points at — which is usually *your host's* pool, not the container's. If `--why-idle` or the Pool Resources panel shows numbers that don't match what the workflow actually sees, you have three options:

- **Option A (cleanest):** run `workflow-monitor` *inside* the container. Same DAG state files (just a different mount root), and direct access to the container's pool. Requires installing the package into the image or bind-mounting the source and `pip install -e`.

- **Option B (env vars):** point the host's HTCondor CLI at the container's collector with `_CONDOR_COLLECTOR_HOST=...`. Works only if the collector port is reachable from the host *and* the container's auth allows host connections. Equivalent to passing `--collector` and (optionally) `--schedd` to `workflow-monitor`.

- **Option C (publish ports):** publish the container's condor port to the host (e.g. `-p 127.0.0.1:9619:9618` in `docker run`) and use a [shared-port + TCP-forwarding](https://htcondor.readthedocs.io/) configuration inside the container. Then `--collector localhost:9619` from the host. This is the most permanent setup; see the troubleshooting section below for the specific config knobs and a port-collision warning.

In all three options, `--remap-submit-dir auto` continues to do the right thing for file-side paths.

### Client/Server options

| Flag | Default | Description |
|------|---------|-------------|
| `--serve` | off | Run as a headless server daemon. Daemonizes and survives terminal exit. |
| `--serve-foreground` | off | Run the server in the foreground (for debugging). |
| `--stop-server [PID_FILE]` | — | Stop a running server daemon. If `PID_FILE` is omitted, locates it from the workflow target. |
| `--remote USER@HOST:/PATH` | — | Monitor a remote workflow via SSH. No local Pegasus or HTCondor required. |
| `--sync-interval SECONDS` | `5.0` | How often the client fetches the remote log file. |
| `--ssh-config PATH` | — | SSH config file passed as `ssh -F <PATH>`. |
| `--ssh-identity PATH` | — | SSH identity/key file passed as `ssh -i <PATH>`. |

## Dashboard Panels

### Header
Displays the workflow label, shortened UUID, submitting user, Pegasus planner version, mode badge, and the last-refreshed time.

### Workflow Status
Shows the current workflow state (`RUNNING`, `SUCCESS`, or `FAILED`), elapsed wall-clock time, percentage complete, job counts by category, and a progress bar. Failed jobs are indicated in red on the bar.

### Diagnostics (conditional)
Appears automatically when jobs are held or have failed. For each problematic job, shows:
- A one-line summary of the issue (e.g., "Job exceeded memory limit", "Output file transfer failed")
- The underlying reason (HTCondor hold reason, exit code, or kickstart stderr excerpts)
- Actionable remediation suggestions

Diagnostics are powered by pattern matching against:
- **HTCondor hold reasons** — 11 regex patterns covering transfer failures, resource limits, credential issues, container errors, and more
- **Exit codes** — maps common codes (1, 2, 126, 127, 137, 139) to likely causes
- **Kickstart stderr** — detects missing files, transfer failures, permission denied, disk full, and connection errors

In live mode, kickstart `.out.000` files are parsed directly from the submit directory. In SSH/remote mode, diagnostics use the HTCondor ClassAd data captured in `htcondor_poll` JSONL events.

### Compute Jobs (or All Jobs with `--all-jobs`)
A table of individual jobs.

**Row ordering.** By default (`--sort-by-activity`, on), rows use a stable two-tier sort so that currently-active work is always visible without scrolling — important when the workflow has more compute jobs than fit on a screen:

1. `RUNNING` jobs at the top, most recently started first.
2. Everything else by the most recent timestamp among `end_time`, `start_time`, and `submit_time` (newest first) — so jobs that just completed, failed, or were held bubble up next.
3. `UNSUBMITTED` jobs (no timestamps yet) at the bottom, in their original DAG order.

Pass `--no-sort-by-activity` to keep the original DAG/submit order — useful when you want a deterministic top-to-bottom view that mirrors the DAG structure (or when the active sort feels too jumpy at very short refresh intervals).

Columns:

| Column | Description |
|--------|-------------|
| **Job** | Transformation name for compute jobs (e.g. `detect_seismic_anomalies`), DAG node name for infrastructure jobs |
| **Type** | Job category: `compute`, `stage-in`, `stage-out`, `dir-create`, `cleanup`, `register` |
| **State** | Simplified state — see [Job states](#job-states) |
| **Exit** | Exit code when the job has completed |
| **Duration** | Wall time from `EXECUTE` to `JOB_TERMINATED`; updates live for running jobs |
| **Args** | Task arguments from the Pegasus workflow definition (truncated to 40 chars); compute jobs only |
| **Mem** | Peak resident memory (maxrss) from the kickstart invocation record; populated after job completion |
| **Req** | HTCondor resource requests — compact format like `2c/4.0G/1gpu` (CPUs/memory/GPUs) |
| **Live** | For running jobs: HTCondor queue status, execute host, queue wait time, retry count, I/O transfer. For completed jobs: execute host, CPU efficiency, memory efficiency, disk usage, transfer I/O |

### Auxiliary Jobs
A compact summary of non-compute jobs (stage-in/out, directory creation, cleanup, registration) grouped by type and state. Hidden when no auxiliary jobs have been seen yet.

### Pool Resources (conditional)
Appears when `condor_status` data is available. Shows a compact summary of the HTCondor pool:

| Row | Description |
|-----|-------------|
| **Machines** | Number of unique machines in the pool |
| **Slots** | Claimed/total slots with idle count |
| **CPUs** | Used/total CPUs with idle count |
| **Memory** | Used/total memory (GB) with free amount |
| **GPUs** | Used/total GPUs (only shown when GPUs exist in the pool) |
| **Platform** | OS and architecture (e.g., `LINUX/X86_64`) |

This panel helps diagnose idle jobs — if all slots are claimed, jobs will wait until resources free up. If the pool has available capacity but jobs are still idle, the issue is likely a requirements mismatch or negotiator priority.

### Recent Events
A per-job activity summary showing the most recently active jobs, sorted by completion time (newest first). The number of rows is controlled by `--events`.

| Column | Description |
|--------|-------------|
| **Job** | Transformation name for compute jobs, DAG node name for infrastructure jobs |
| **State** | Simplified display state |
| **Start** | Time the job began executing (or was submitted) |
| **End** | Time the job terminated |
| **Duration** | Wall-clock time from start to end; updates live for running jobs |
| **Mem** | Peak resident memory from the kickstart invocation record |

## Event Log Format

When `--log` or `--serve` is active, the monitor writes a JSONL (JSON Lines) file where each line is a self-contained JSON object. This file can be replayed with `--replay`, streamed to an SSH client with `--remote`, or ingested into analytics tools.

### Event types

| Event type | Emitted when | Key fields |
|------------|-------------|------------|
| `workflow_start` | Logger initializes (first line) | `dax_label`, `user`, `planner_version`, `submit_dir`, `wf_start` |
| `jobs_init` | First snapshot | `total_jobs`, `jobs` (list of `{job_id, exec_job_id, type_desc, transformation, task_argv}`) |
| `workflow_state` | Workflow state changes | `state` (`WORKFLOW_STARTED`, `WORKFLOW_TERMINATED`), `status`, `wf_start`/`wf_end` |
| `job_state` | A job transitions to a new state | `exec_job_id`, `type_desc`, `state`, `job_id`, `exitcode`, `stdout_file`, `stderr_file`, `maxrss` |
| `htcondor_poll` | HTCondor queue contents or attributes change | `jobs` (list of ClassAd dicts including `HoldReason`, `JobStatus`, resource requests, transfer bytes) |
| `htcondor_history` | New completed jobs appear in condor_history | `jobs` (list of ClassAd dicts with `RemoteWallClockTime`, `RemoteUserCpu`, `DiskUsage`, `LastRemoteHost`, etc.) |
| `pool_status` | Pool slot/CPU/memory counts change | `pool` (dict with `total_slots`, `idle_slots`, `claimed_slots`, `total_cpus`, `idle_cpus`, `total_memory_mb`, `idle_memory_mb`, `total_gpus`, `idle_gpus`, `machines`, `os_arch`) |
| `workflow_stats` | Just before workflow_end | `stats` (dict with job counts, duration distribution, CPU/memory efficiency, queue wait, transfer I/O, parallelism, host distribution, pool summary) |
| `workflow_end` | Monitor exits (last line) | `wf_state`, `wf_status`, `wf_end`, `total_jobs`, `done`, `failed`, `elapsed` |

Every event includes `event_type`, `timestamp` (Unix float), and `wf_uuid`.

The logger is resume-aware — if the server restarts, it detects the existing log, recovers its state (high-water timestamp, last workflow state), truncates any trailing `workflow_end` marker, and appends new events seamlessly.

### Example

```bash
# Monitor and log
uv run workflow-monitor --log /path/to/workflow

# Inspect the log
cat /path/to/submit/dir/workflow-events.jsonl | python -m json.tool
```

## Job States

| Display state | Meaning |
|---------------|---------|
| `UNSUBMITTED` | Job exists in the DAG but has not been submitted yet (waiting on dependencies) |
| `QUEUED` | Submitted to HTCondor; waiting to be matched to a slot |
| `PRE` | Pre-script is running |
| `RUNNING` | Job is executing on a worker node |
| `POST` | Post-script is running |
| `SUCCESS` | Job completed successfully (post-script returned 0) |
| `FAILED` | Job or post-script exited non-zero |
| `HELD` | Job is held in the HTCondor queue |

## Architecture

The monitor reads from three data sources produced by a running workflow, with no modifications to Pegasus or HTCondor:

```
Pegasus workflow run
        │
        ├── braindump.yml          ← workflow metadata (UUID, submit dir, dag name)
        │
        ├── <dag>.stampede.db      ← SQLite database written by pegasus-monitord
        │       tables: workflow, workflowstate, job, job_instance, jobstate,
        │               task, invocation, rc_lfn, rc_meta
        │
        ├── condor_q / htcondor    ← live HTCondor queue (Python bindings or subprocess)
        │       resource requests, queue wait, transfer I/O, retry count
        │
        ├── condor_history         ← post-completion metrics (throttled, ~10s)
        │       CPU/memory efficiency, disk usage, remote host, transfer bytes
        │
        └── condor_status          ← pool slot/machine profiling (throttled, ~15s)
                slot capacity, CPU/memory/GPU availability, platform info
```

In client/server mode, the data flows through a JSONL log file:

```
┌─────────────────────── Server machine ───────────────────────┐
│                                                              │
│  stampede.db ──┐                                             │
│                ├──▶ server daemon ──▶ workflow-events.jsonl  │
│  condor_q ──-──┘         (--serve)                           │
│                                                              │
└──────────────────────────────┬───────────────────────────────┘
                               │ SSH (incremental tail -c / cat)
┌──────────────────────────────▼───────────────────────────────┐
│                                                              │
│  client (--remote) ──▶ Rich TUI  [SSH badge]                 │
│                                                              │
└─────────────────────── Client machine ───────────────────────┘
```

### Modules

| Module | Purpose |
|--------|---------|
| `braindump.py` | Locates and parses `braindump.yml` from any accepted `TARGET` form. Provides paths to all workflow artifacts. |
| `db.py` | Queries the stampede SQLite database in read-only mode. Handles `database is locked` gracefully during concurrent writes. |
| `htcondor_poll.py` | Queries the live HTCondor queue (`condor_q`), job history (`condor_history`), and pool status (`condor_status`). Tries Python bindings first; falls back to subprocess. Applies credential setup before queries. Provides resource request formatting, efficiency calculations, transfer I/O metrics, and pool resource aggregation. |
| `display.py` | Builds and drives the Rich terminal dashboard. Renders inside `rich.live.Live` with configurable refresh. Supports mode badges. |
| `diagnostics.py` | Pattern-matches held and failed jobs against HTCondor hold reasons, exit codes, and kickstart stderr for actionable suggestions. |
| `stats.py` | Computes end-of-workflow analytics from stampede.db, condor_history, and pool status. Produces a `WorkflowStats` dataclass for JSONL serialization and console rendering. |
| `event_log.py` | JSONL event logger with resume support. Tracks high-water timestamps to avoid duplicates. Fingerprint-based HTCondor poll dedup. Emits `workflow_stats` before `workflow_end`. |
| `replay.py` | Loads a JSONL event log and replays it through the TUI at configurable speed. Handles multi-session logs. |
| `server.py` | Headless daemon for client/server mode. Daemonizes via double-fork. Writes PID file for lifecycle management. |
| `why_idle.py` | One-shot idle job diagnostic. Queries pool capacity, user priority, and negotiator timing. Produces human-readable analysis with Rich output. The internal `_analyze()` is reused by the diagnostics engine. |
| `remote.py` | SSH client engine. Incremental byte-offset fetching for both `workflow-events.jsonl` and the optional `diagnostics-events.jsonl` sidecar. Reconstructs workflow state from events. Supports `--once`. |
| `stall_detector.py` | Stall detection state machine (normal → suspected → confirmed). Owns the four heuristics, two-strike confirmation, cooldown, startup grace, and resolution detection. |
| `diag_log.py` | Append-only JSONL writer for the diagnostics sidecar. Emits `diag_start` (with schema version + engine config) and `diag_end` framing. |
| `diagnostics_engine.py` | Single integration point for the `--diagnose` layer. Wraps `StallDetector`, runs `why_idle._analyze()` on confirmed stalls, runs `collect_diagnostics()` continuously with per-job dedup, and routes everything through `DiagnosticLogger`. Engine errors never break the monitor loop. |

## Related Projects

### [predictable-workflow-failures](https://github.com/pegasusai/predictable-workflow-failures)

Generates Pegasus workflows with predictable, labeled failure modes — 7 scenarios covering exit codes, missing input, memory exceeded, timeout, dependency cascades, and transfer failures. Use it to:

- **Test workflow-monitor** across all failure categories on any Pegasus/HTCondor environment
- **Validate diagnostics** — each scenario triggers a specific diagnostic pattern (held, failed, cascading)
- **Compare monitoring modes** — run the same scenario with live TUI vs. SSH client to verify parity

Typical testing workflow:

```bash
# On the submit node: generate and run a failure scenario
uv run workflow-generator generate memory_exceeded -o ./generated
cd generated/memory_exceeded && python3 workflow_memory_exceeded.py

# Start the server daemon
uv run workflow-monitor --serve /path/to/submit/run0001

# From your local machine: monitor via SSH
uv run workflow-monitor --remote user@host:/path/to/workflow-events.jsonl
```

## Troubleshooting

### Locating the workflow

**"Cannot find braindump.yml at or under: ..."**
The target you passed in does not contain (or recursively contain) a `braindump.yml`. Confirm the workflow has been planned with `pegasus-plan` — `braindump.yml` is written next to the DAG file inside `runNNNN/`. Pass either the run directory directly, the workflow base directory (the monitor picks the highest-numbered `runNNNN`), or the `braindump.yml` path itself.

**"Stampede database not found in: /some/path"**
Two distinct causes:

1. **`pegasus-monitord` hasn't created it yet.** The `*.stampede.db` file appears shortly after `pegasus-run` starts the workflow. If you only planned (not submitted), it won't exist. Start the workflow with `pegasus-run`, then re-run the monitor.
2. **The path is wrong because `submit_dir` in `braindump.yml` doesn't match this host.** This happens when the workflow was planned inside a container and `braindump.yml` records a container path. The monitor's `auto` remap should detect this and rebase, but if it didn't (e.g. the recorded path coincidentally exists on this host but points to a different file tree), force the remap with `--remap-submit-dir always`. See [Container-planned workflows](#container-planned-workflows).

**Auto-detection picked the wrong run directory**
When given a workflow base directory, the monitor recursively searches for `braindump.yml` and selects the lexicographically last match (typically the highest-numbered `runNNNN`). If you want a specific older run, pass that run directory directly:

```bash
uv run workflow-monitor /path/to/submit/run0003   # not /path/to/submit
```

### HTCondor data

**Live (Condor) column is always empty**
The monitor could not reach the HTCondor schedd. Ensure `condor_q` is on your `PATH` (e.g. `. ~/condor/condor.sh`) and that the schedd is running. For remote pools, supply `--collector` and any required credential flags.

**Pool Resources panel does not appear**
The monitor queries `condor_status` for pool-wide slot information. This requires access to the HTCondor collector daemon. On submit-only nodes that cannot reach the collector, or when `condor_status` is not installed, the panel is silently omitted. The rest of the dashboard continues to work normally. In SSH/remote mode, the panel appears if the server captured `pool_status` events in the JSONL log.

**Live column shows no efficiency data for completed jobs**
The monitor queries `condor_history` for post-completion metrics (CPU efficiency, memory efficiency, disk usage). If `condor_history` is not available on your submit node, these fields are silently omitted — the monitor continues to work with data from the stampede database. This is normal on systems where history is kept on a central manager rather than the local schedd.

**Workflow shows RUNNING but condor_q shows no jobs**
This is normal at the very start of a run while DAGMan is initializing, and during transitions between DAG nodes. The `UNSUBMITTED` state in the job table indicates jobs that are waiting on upstream dependencies.

**`condor_q` returns thousands of unrelated jobs on a shared schedd**
The monitor restricts `condor_q` results to jobs whose `Cmd` lives under the workflow's submit directory. If you're seeing unrelated jobs, the filter is mismatched — most likely because `--remap-submit-dir` rebased `submit_dir` to a host path while the schedd reports container paths in `Cmd`. The monitor uses the *recorded* (un-remapped) submit dir for this filter automatically; if you've explicitly forced `--remap-submit-dir never` or you're running an old version, that's where to look.

### Container-planned workflows

**Pool Resources / `--why-idle` shows my host's pool, not the container's**
This is expected: the monitor's `--remap-submit-dir` only fixes file paths, not HTCondor connections. The host's `condor_q`/`condor_status` queries whatever pool your host config points at. Choose one of the three options described in [Container-planned workflows](#container-planned-workflows) (run inside the container, set `_CONDOR_COLLECTOR_HOST`, or publish container ports).

**Publishing container ports — port collision with host condor**
Your host condor pool is probably already listening on the default `9618`. Don't try `-p 9618:9618`. Either stop host condor for the duration (`condor_off -master`) or pick a different host-side port (e.g. `-p 127.0.0.1:9619:9618` and `_CONDOR_COLLECTOR_HOST=localhost:9619`). Bind to `127.0.0.1` so the throwaway pool isn't reachable from the LAN.

**Publishing container ports — schedd reports an unreachable address**
HTCondor's collector hands out the schedd's *advertised* address, which by default is the container's internal IP. Set `TCP_FORWARDING_HOST = 127.0.0.1` (not `NETWORK_HOSTNAME`, which leaks into auth) in the container's condor config so daemons publish a host-reachable address. Use `USE_SHARED_PORT = TRUE` + `SHARED_PORT_PORT = 9618` so you only need to publish one port.

**Publishing container ports — auth fails between host and container**
The default `SEC_DEFAULT_AUTHENTICATION_METHODS = FS` compares Unix UIDs via shared filesystem and fails across the container boundary. Add `CLAIMTOBE` (for permissive throwaway pools) or set up a token. Permissive `ALLOW_READ = */*` / `ALLOW_WRITE = */*` is necessary but not sufficient — it's *authorization*, not *authentication*.

**HTCondor version skew between host and container**
If the host's `condor_q` is much older than the container's schedd, some queries may return malformed output. Run `condor_version` on both sides; the host should ideally be the same major version or newer. Workflow-monitor uses `-json` output which is stable across recent versions, but `-long` ClassAd parsing (used by `--why-idle`'s `condor_userprio`) is more sensitive.

### `--why-idle`

**"Could not query pool status" or "Could not query user priorities"**
Informational, not errors. The diagnostic works with whatever data sources are available. If `condor_status` or `condor_userprio` are not on your PATH, the corresponding checks are skipped and the remaining findings are still shown. Source the condor environment (e.g. `. ~/condor/condor.sh`) for the full diagnostic.

**"No priority entry found for user 'pegasus'" but the user clearly exists**
You're querying a different pool than the one that's running the workflow. If the workflow runs in a container, your host's `condor_userprio` doesn't know about the container's `pegasus` user. See [Container-planned workflows](#container-planned-workflows).

**`--why-idle` says "no idle jobs found" but the dashboard shows queued jobs**
The dashboard's `Queued` count comes from the stampede database (DAG state), but `--why-idle` queries `condor_q` for jobs in `JobStatus == 1` (idle). If those numbers diverge, you're likely viewing the wrong pool — same root cause as the priority issue above.

### Server / Remote / SSH

**SSH mode shows "No hold reason available" but LIVE shows the hold reason**
Make sure the server was started with HTCondor accessible (`condor_q` on PATH). The server captures hold reasons via `htcondor_poll` events in the JSONL. If the server was started without HTCondor access, or if the job was held after the server stopped, the hold reason won't be in the log.

**Server daemon dies silently**
Check the daemon log file at `<log_path>.pid.log` (e.g., `workflow-events.pid.log`). On some systems, `screen -dmS monitor uv run workflow-monitor --serve-foreground ...` is more reliable than the double-fork daemon.

**`--remote` connects but the dashboard never advances**
The client polls the remote JSONL at `--sync-interval` (default 5s). If the file is empty or the server hasn't written events since you connected, you'll see the last known state. Verify on the server side: `tail -F /path/to/workflow-events.jsonl`. If new lines aren't being added, the server has stopped or the workflow has completed (look for `workflow_end`).

**`--remote` with IPv6 address fails to parse**
Wrap IPv6 addresses in brackets: `user@[2001:db8::1]:/path/to/log`. Without brackets, the colon-laden address is ambiguous with the `host:port` form.

**`--remote` over a bastion/ProxyJump prompts for a password every cycle**
Use `--ssh-identity` to point at a key, and configure the bastion in `--ssh-config` with `IdentityFile` and `IdentitiesOnly yes`. If a passphrase is still required, run `ssh-add` first so the agent caches it.

**Diagnostics events missing in `--remote`**
The diagnostics sidecar is fetched best-effort. Confirm the server was started with `--diagnose` (the sidecar `diagnostics-events.jsonl` should exist next to the main log on the server). The remote client silently skips a missing sidecar.

### Replay

**Replay finishes instantly / no events appear**
The JSONL log is empty or contains only framing events. Verify with `wc -l workflow-events.jsonl` and inspect the first few lines. A normal log starts with `workflow_start` and `jobs_init`.

**Replay timestamps look ancient**
This is expected — replay shows the *original* timestamps from when the workflow ran. The "elapsed" counter in the status panel reflects real workflow elapsed time, not your replay time.

### Display

**Dashboard appears garbled or cut off**
The Rich layout adapts to your terminal size. Widen the terminal window for best results. A minimum width of ~100 columns is recommended.

**Colors look wrong or are absent**
Set `TERM=xterm-256color` and ensure your terminal supports truecolor. The dashboard degrades gracefully on monochrome terminals but is best on truecolor.

### Diagnostics layer (`--diagnose`)

**No `STALL` panel ever appears even though the workflow is stuck**
Several false-positive guards must clear before a stall is confirmed: a 60-second startup grace, two consecutive cycles meeting the heuristic threshold, and at least one job submitted. For idle workflows that *just* started, this is by design. Inspect `diagnostics-events.jsonl` directly to see what the engine is observing each cycle — the file logs every `diag_start` config so you can audit what thresholds are in effect.

**`STALL` panel never disappears after the workflow recovers**
A stall is resolved when the done count advances or new state transitions arrive. If neither happens (e.g. all jobs failed and the workflow exited), the panel will persist until the monitor exits — that is the correct signal.

## Development

```bash
# Install in editable mode with all dependencies
uv sync

# Run the monitor directly from source
uv run workflow-monitor --help

# Run against a completed workflow (no HTCondor required)
uv run workflow-monitor --once /path/to/workflow

# Replay the bundled example log (no Pegasus or HTCondor required)
uv run workflow-monitor --replay example-logs/workflow-events.jsonl --speed 4
```

The project uses [hatchling](https://hatch.pypa.io/) as its build backend and [uv](https://docs.astral.sh/uv/) as the package and virtual environment manager.

## Tested Platforms

| Platform | Pegasus | HTCondor | Mode | Pool |
|----------|---------|----------|------|------|
| macOS arm64 (Homebrew) | 5.1.2 | 25.6.1 | Live, Replay | Single-node `minicondor` |
| Ubuntu 24.04 x86_64 (FABRIC) | 5.1.2 | 24.12.17 | Live, Server, SSH | Multi-node (submit + 2 workers) |

## License

Apache-2.0. See [LICENSE](LICENSE) for details.
