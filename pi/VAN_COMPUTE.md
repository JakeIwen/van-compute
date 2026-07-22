# Opportunistic M4 analysis worker

`van_compute.py` lets agents on `vanpi` queue CPU- or memory-intensive **offline** analysis for
the M4 MacBook. The Mac initiates SSH connections to the Pi, so macOS Remote Login is not needed.
If the Mac is asleep or away, jobs remain queued on the Pi until the worker returns.

The worker is intentionally not a general remote shell. Both hosts share an allowlist in
`shared/python/van_compute_protocol.py`. Each task fixes its entry point and validates its options.
The `signal-correlate-analyze` task also fixes the `analyze` subcommand, making its active CAN
`capture` mode unreachable through the worker.

## Data flow and safety boundary

1. The Pi CLI records the input file's inode and submitted byte length.
2. It snapshots and hashes the task's small source-code set. This preserves uncommitted analysis
   code without copying the entire checkout.
3. A macOS LaunchAgent polls every 15 seconds and claims at most one job.
4. The Mac streams the exact source snapshot and the input's submitted byte range over SSH.
   Appending to a growing capture is safe; replacing or truncating it makes the job fail closed.
5. The Mac runs a shell-free argument vector at low priority with a wall-time limit, captures its
   output, and uploads bounded result files.
6. The Pi hashes results and atomically moves the job to `done/` or `failed/`.

The worker cannot capture CAN, open SocketCAN, change an interface, wake the bus, send UDS, or
control a service. Those operations remain on `vanpi` under the live toolkit's normal safeguards.

## Install

From the scripts checkout on the Mac:

```zsh
./macbook/scripts/install_van_compute_worker.zsh
```

The installer checks the plist, NumPy, and SSH; deploys only the queue CLI and shared task catalog
to `/home/pi/scripts`; copies the Mac worker into
`~/Library/Application Support/van-compute`; and installs the user LaunchAgent. The installed worker
does not depend on a particular Git branch remaining checked out. The installer does not enable
inbound SSH on the Mac; rerun it after updating worker or protocol code.

The LaunchAgent runs only while Jacob's GUI account is logged in. Its job label is
`com.jacobr.van-compute-worker`.

## Agent-facing commands on vanpi

List tasks and check whether a worker has checked in recently:

```bash
/home/pi/scripts/van_compute.py tasks
/home/pi/scripts/van_compute.py available
```

Submit one capture summary and wait up to ten minutes:

```bash
/home/pi/scripts/van_compute.py submit can-capture-summary \
  --input /home/pi/dev/obd-things/tmp/captures/ccan/drive.log \
  --arg=--snapshot \
  --wait 600
```

The input must be a regular, non-symlink file inside `/home/pi/dev/obd-things`. Queue metadata and
results default to `/home/pi/dev/obd-things/tmp/compute/` and therefore remain ignored/transient.

Other initial tasks:

```bash
# Compare two existing summary JSON reports.
/home/pi/scripts/van_compute.py submit can-capture-compare \
  --input /home/pi/dev/obd-things/tmp/baseline.json \
  --input /home/pi/dev/obd-things/tmp/current.json \
  --arg=--rate-factor=2.0

# Find a field across captures with paired ground-truth values.
/home/pi/scripts/van_compute.py submit can-field-finder \
  --input /home/pi/dev/obd-things/tmp/v_off.log --input-value 12.5 \
  --input /home/pi/dev/obd-things/tmp/v_run.log --input-value 14.2 \
  --arg=--top=30

# The worker forces signal_correlate.py's offline `analyze` subcommand.
/home/pi/scripts/van_compute.py submit signal-correlate-analyze \
  --input /home/pi/dev/obd-things/tmp/sweeps/radar_acc_correlate_example.json \
  --arg=--ground --arg='0845:0:4:>i4' --arg=--top --arg=50
```

Inspect jobs and retrieve a declared result without knowing the queue layout:

```bash
/home/pi/scripts/van_compute.py list
/home/pi/scripts/van_compute.py status JOB_ID
/home/pi/scripts/van_compute.py wait JOB_ID --timeout 3600
/home/pi/scripts/van_compute.py result JOB_ID summary.json > tmp/summary.json
/home/pi/scripts/van_compute.py result JOB_ID stdout.txt
/home/pi/scripts/van_compute.py result JOB_ID stderr.txt >&2
```

If a Mac worker is interrupted after claiming a job, its next invocation resumes that same job
before claiming a new one. Result uploads are atomic and safe to repeat.

## Manual operation and diagnostics

Run one poll immediately on the Mac:

```zsh
/opt/homebrew/bin/python3 ./macbook/scripts/van_compute_worker.py
```

Inspect or restart the LaunchAgent:

```zsh
launchctl print "gui/$(id -u)/com.jacobr.van-compute-worker"
launchctl kickstart -k "gui/$(id -u)/com.jacobr.van-compute-worker"
```

The worker defaults to a one-hour wall-time limit, 128 MiB per result file, at most 64 result
files, one job at a time, and process niceness 10. It uses the Homebrew Python at
`/opt/homebrew/bin/python3`; NumPy is required by `signal-correlate-analyze`.

## Resource telemetry and dashboard

Each upgraded-worker job records analysis-child user/system CPU time, wall time, average CPU
utilization, process-tree peak RSS, page faults, context switches, input/source bytes, queue delay,
and returned-result bytes. The van dashboard reads queue manifests directly through the read-only
`/api/compute` endpoint and presents:

- current worker heartbeat plus running and queued counts;
- jobs completed on the Mac rather than on vanpi;
- measured Mac CPU, wall time, peak job memory, and transferred bytes;
- recent-job CPU/memory bars and totals grouped by task; and
- 6-hour, 24-hour, 7-day, and 30-day ranges.

Mac CPU seconds are evidence of work kept off the Pi, but are not a Pi-equivalent time estimate.
Hardware and implementation performance differ; calibrating that claim would require occasionally
benchmarking the same representative input on both hosts. Jobs completed before telemetry was
added remain visible, labeled as lacking resource measurements.

## Current limitations

- There is no automatic pruning yet; completed jobs must be reviewed before adding deliberate
  retention and cleanup policy.
- Jobs use Jacob's logged-in macOS account. Task selection and arguments are strictly allowlisted,
  but the exact analysis source is intentionally supplied by the Pi checkout. Therefore a Pi agent
  that can modify an allowlisted analysis file can execute those modifications on the Mac. A
  separate standard worker account or a Mac-approved source-hash catalog is the next hardening step.
- Only files inside the live `obd-things` checkout are accepted as inputs.
- A replaced or truncated queued input fails instead of silently analyzing different data.
