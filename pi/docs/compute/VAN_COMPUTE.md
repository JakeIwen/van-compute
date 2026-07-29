# Van compute broker

[Pi documentation index](../../README.md)

`pi_compute.py` is the single agent-facing entry point for CPU- or memory-heavy
offline work. Agents enqueue a named task and never decide whether the Mac is
reachable or whether the Pi should fall back locally.

```text
agent -> pi_compute -> queue on vanpi
                         |
                         +-> fresh remote lease -> one of 10 equal Mac slots
                         |
                         +-> no remote lease -> one guarded Pi-local slot
```

The Mac initiates SSH connections to the Pi, so macOS Remote Login is not
needed. The queue and broker are intentionally not a general remote shell.

## Placement behavior

- The persistent Mac scheduler advertises one capacity heartbeat with exactly
  10 equal slots. When idle it uses one centralized queue poll instead of ten
  independent polling loops. Four SSH control connections distribute slot,
  heartbeat, and transfer channels below vanpi's per-connection session limit.
- Heartbeats are leases. The broker never pings or probes the network.
- While any non-local compute lease is fresh, queued work is left for remote
  workers. This naturally extends to additional compute nodes later.
- If every remote lease is stale, the broker waits a short grace period and may
  run one eligible task locally. Memory, disk free space, load, temperature,
  throttling, runtime, file size, and cgroup limits guard that
  fallback.
- A remotely claimed job whose exact slot heartbeat has been stale for five
  minutes is conservatively returned to the queue. Attempt tokens reject late
  uploads from the superseded worker.
- Jobs too risky for the Pi fallback, notably JADX decompilation and configured
  corpus searches, remain queued until a remote worker is available.

## Safety boundaries

Repository tasks are declared in `.van-compute.json`. A declaration chooses a
fixed executable family and a shell-free argument template. Supported profiles
cover repository tests, Python scripts/modules, saved CAN-log analysis,
read-only SQLite, ripgrep corpus search, and JADX APK analysis. Shells,
SocketCAN, ADB, SSH, service managers, and network tools are not executable
families.

The Mac installer fails closed unless it can validate the effective
`sandbox-exec` profile. Each job gets a clean environment and private
HOME/TMP/cache, no credentials passed through its environment, no network, declared read-only
datasets only, monitored resource ceilings, and a writable job directory only.
Private Mac dataset paths stay out of manifests and logical command telemetry;
recognized text results are scrubbed before upload. Tasks that use a private
dataset must not copy its physical path into binary output or files embedded in
a returned directory archive. `sandbox-exec` is deprecated by Apple, so the
installer tests the actual OS behavior before replacing a working LaunchAgent.

The Mac parent shares one process-table sample per second across all slots and
tracks each job process group's aggregate resident memory and process count,
terminating it above the default 16 GiB or 256-process emergency ceiling.
van_compute does not impose a host-wide Mac memory reservation or kill running
jobs based on estimated macOS free memory; macOS remains responsible for
pressure management across the ten slots and the user's other applications.
Disk admission reserves concurrent staging and packaging peaks and preserves
5 GiB of free space through those phases and execution. Jobs wait when that
shared disk reserve is temporarily unavailable without reducing the ten logical
slots. These are watchdogs rather than Darwin kernel hard limits; a process
that deliberately detaches into another session can evade them. The Pi fallback
has the stronger systemd cgroup ceiling in addition to its child limits.

Pi fallback jobs run inside Bubblewrap with a private PID/user/network/IPC/UTS
namespace. Only staged source and inputs, a minimal system runtime, and the
job's writable output directories are visible. The systemd service also has no
CAN devices, no network address families, no service-manager sockets, no
capabilities, no swap allowance, a 1 GiB memory ceiling, and a 128-task cgroup
ceiling shared only by the broker and its one local job. A per-process
`RLIMIT_NPROC` is intentionally not used because Linux counts it across every
process and thread owned by the shared `pi` account rather than per job. Dynamic
local work fails closed if the Bubblewrap self-test or runtime dependency check
fails.

Neither sandbox is a VM. The Mac worker runs under the logged-in account;
same-account process metadata is therefore not a strong isolation boundary,
and a deliberately detached process can evade process-group cleanup while
remaining inside the file/network sandbox. Only reviewed repository task code
should be submitted. The emergency
`VAN_COMPUTE_ALLOW_UNSANDBOXED=1` installer escape hatch should therefore be
used only after explicit review, never as the normal configuration.

Live CAN/SocketCAN access, interface configuration, bus wake or UDS traffic,
ADB/device access, routing, storage, and service control never go through this
system.

## Install

From this checkout in a freshly opened Terminal.app or iTerm window—not a
Codex-managed or otherwise sandboxed shell:

```zsh
./macbook/scripts/install_van_compute_worker.zsh
```

The repository-wide updater is also a supported entry point:

```zsh
./pi/sync_scripts.sh
```

After its normal Pi deployment succeeds, `sync_scripts.sh` invokes the compute
installer with `--if-needed`. A fingerprint covers the installer, worker,
LaunchAgent template, Pi compute scripts/configuration, worker identity,
connection target, dataset configuration, and isolation mode. Matching local
and Pi deployment markers plus healthy loaded services make the check exit
immediately. A changed or unhealthy deployment runs the ordinary drain-first
installer in the foreground, and any installer failure makes `sync_scripts.sh`
fail. A required compute upgrade may therefore take time; the updater never
reports success while that work is still running.

macOS cannot apply the worker's Seatbelt profile from inside another sandbox.
The installer checks that capability before downloading or building anything
and fails closed if profiles cannot be nested in its current environment.

The installer provisions a private Mac Python environment and required offline
tools, validates the Mac sandbox, deploys the Pi queue/frontend/broker and
read-only dashboard metrics module, provisions the Pi fallback Python
environment, registers the systemd unit, and starts the persistent LaunchAgent.
Immutable worker releases live under
`~/Library/Application Support/van-compute/releases`; the current and previous
release are retained. Rerun the installer after changing the worker, protocol,
broker, or metrics module. Dashboard application, template, static, and service
changes still deploy through `pi/sync_scripts.sh`.

The compute installer is the sole deployment path for `pi/van_compute/`. Its Pi
files have one self-contained destination:

- `/home/pi/van_compute/scripts/` contains the queue CLI, agent frontend,
  broker, upgrade gate, protocol, and metrics modules.
- `/home/pi/van_compute/configs/` contains the example task policy and the
  auditable source copy of the systemd unit.
- `/home/pi/van_compute/venv/` is the private Pi fallback runtime.
- `/home/pi/van_compute/runtime.lock` serializes fallback-runtime provisioning.

The operational unit is also installed as a root-owned regular file at
`/etc/systemd/system/van-compute-broker.service`; systemd requires that
registration outside the application tree. It is intentionally not a symlink
into pi-owned `/home`, which would let the service account replace
root-interpreted configuration. This is the sole deployed-file exception.

Queue jobs and results remain runtime data under the configured `obd-things`
compute directory; they are not deployed files. The generic staging portion of
`pi/sync_scripts.sh` does not copy compute files. Its final conditional
installer call preserves the coupled Pi/Mac protocol boundary, so it cannot
publish half of an upgrade. During the one-time layout migration, the installer
retires the legacy `/home/pi/scripts/compute/` tree only after the replacement
broker and worker have been validated.

The dashboard migration to
`/home/pi/van_compute/scripts/van_compute_metrics.py` is complete. Its retired
copies under `/home/pi/scripts/python-automation/` have been removed, and
`pi/sync_scripts.sh` does not recreate them. Normal compute deployments
therefore require no retired-file cleanup.

Upgrades are drain-first. The installer requires the running queue to be empty,
disables new launches, asks a current persistent scheduler to stop claiming,
then temporarily fences the public Pi queue CLI. It waits for any already
loaded `van_compute submit` or `pi_compute run` process and checks every queued
or running entry, including hidden submission staging directories, before
placing the queue in maintenance through protocol replacement and the first
new coordinator heartbeat. Installer ownership persists across reruns so an
interrupted post-protocol upgrade resumes forward without releasing another
machine's maintenance lease. If the boundary cannot be proven safe, the
installer either restores the exact prior CLI/worker before replacement or
leaves the queue fenced in maintenance afterward and tells the operator to
rerun it.

It deploys, but deliberately does not activate, the example policy for the
separate live `obd-things` checkout. After reviewing it, activate it only if no
policy already exists:

```bash
ssh pi@vanpi '
  set -eu
  cd /home/pi/dev/obd-things
  test ! -e .van-compute.json
  test ! -L .van-compute.json
  install -m 600 /home/pi/van_compute/configs/van-compute-obd.example.json .van-compute.json
  /home/pi/van_compute/scripts/pi_compute.py tasks
'
```

That creates a deliberate untracked file in the separate checkout. Review and
commit it there independently when its task policy is stable.

Optional Mac-only datasets use logical aliases, keeping their physical paths out
of tracked task policies, queue manifests, and logical command telemetry. Create
a private JSON file such as:

```json
{
  "datasets": {
    "oem-service-docs": "/absolute/read-only/path/on/the/mac"
  }
}
```

Then rerun the installer with:

```zsh
VAN_COMPUTE_DATASET_CONFIG=/absolute/path/to/datasets.json \
  ./macbook/scripts/install_van_compute_worker.zsh
```

This checkout's active private source is the ignored, mode-0600 Mac-only file
`macbook/secrets/van-compute-datasets.json`; keep the physical corpus path there and
pass that file through `VAN_COMPUTE_DATASET_CONFIG` on future installs.

The example `oem-corpus-search` task is listed even without this private
configuration, but it is not runnable until the `oem-service-docs` alias is
configured on the Mac.

## Agent-facing commands

List the named tasks:

```bash
/home/pi/van_compute/scripts/pi_compute.py tasks
```

Submit work and wait for a bounded time:

```bash
# Portable AlfaOBD DAT smoke tests; pass another -k expression to select a subset.
/home/pi/van_compute/scripts/pi_compute.py run repo-tests --wait 1800

# Existing fixed offline capture summary.
/home/pi/van_compute/scripts/pi_compute.py run can-capture-summary \
  --input /home/pi/dev/obd-things/tmp/captures/ccan/drive.log \
  --arg=--snapshot --wait 600

# Exactly one read-only SQL query.
/home/pi/van_compute/scripts/pi_compute.py run sqlite-query \
  --input /home/pi/dev/obd-things/tmp/example.sqlite3 \
  --arg='SELECT name FROM sqlite_master ORDER BY name' --wait 600

# Remote-only corpus search through a configured dataset alias.
/home/pi/van_compute/scripts/pi_compute.py run oem-corpus-search \
  --arg='diagnostic trouble code' --wait 600

# Remote-only decompilation; the declared directory returns as jadx.tar.gz.
/home/pi/van_compute/scripts/pi_compute.py run apk-decompile \
  --input /home/pi/dev/obd-things/tmp/android/base.apk --wait 3600

# Extract a TCM wire stream from 1-512 capture chunks. Inputs may live on
# external storage; repeat --input in chronological order.
/home/pi/van_compute/scripts/pi_compute.py run candump-diagnostic-wire-tcm \
  --input /mnt/EXFAT512/obd-things/tmp/captures/chunk_000000_full.candump.zst \
  --input /mnt/EXFAT512/obd-things/tmp/captures/chunk_000001_full.candump.zst \
  --wait 3600

# Correlate one wire stream followed by 1-512 chronological capture chunks.
/home/pi/van_compute/scripts/pi_compute.py run can-timeseries-correlate-tcm \
  --input /home/pi/dev/obd-things/tmp/tcm_wire.jsonl \
  --input /mnt/EXFAT512/obd-things/tmp/captures/chunk_000000_full.candump.zst \
  --input /mnt/EXFAT512/obd-things/tmp/captures/chunk_000001_full.candump.zst \
  --wait 3600
```

Inspect and retrieve results without knowing the queue layout:

```bash
/home/pi/van_compute/scripts/pi_compute.py list
/home/pi/van_compute/scripts/pi_compute.py status JOB_ID
/home/pi/van_compute/scripts/pi_compute.py wait JOB_ID --timeout 3600
/home/pi/van_compute/scripts/pi_compute.py result JOB_ID stdout.txt
/home/pi/van_compute/scripts/pi_compute.py result JOB_ID stderr.txt >&2
/home/pi/van_compute/scripts/pi_compute.py result JOB_ID summary.json > tmp/summary.json
/home/pi/van_compute/scripts/pi_compute.py result JOB_ID jadx.tar.gz > tmp/jadx.tar.gz
```

Inputs may be regular, non-symlink files at any path the Pi user can read,
including mounted external drives; they do not need to be inside the selected
source root. The submitted byte range is fingerprinted and immutable: appending
to a growing capture is safe, but replacement, prefix editing, or truncation
makes staging fail closed. This adds one bounded linear read on submission; an
actively changing file may require a second prefix read to distinguish an
append from an edit. Source-code snapshots remain confined to the selected
repository root. They are hashed, then transferred to the Mac as one bounded
verified bundle rather than one SSH process per file. Repository source
snapshots are capped at 10,000 files and 256 MiB; large captures, APKs, and
corpora belong in `--input` files or private dataset aliases instead.

## Dashboard and measurement

The dashboard separates completed Mac work from eligible work that actually
ran through the guarded Pi fallback. It shows scheduler capacity, queue depth,
recorded placement share, job counts, CPU time, analysis/transfer/packaging
timings, input and result bytes, and maximum RSS. The broker automatically
records measured Pi-local runs; `van_compute.py missed-offload` remains
available for explicitly recording eligible work that bypassed the broker.

The dashboard passively recognizes an exact-content benchmark when a successful
task has measured executions on both the Mac and the Pi within the selected
time range. A match requires the same task and arguments, embedded execution
policy, ordered input names, sizes, values and SHA-256 hashes, and snapshotted
source paths, sizes and hashes. Dataset-backed jobs are never matched because a
dataset alias does not fingerprint the private corpus. The scheduler does not
force a Pi run or duplicate work to manufacture a benchmark.

For each matched workload, measured Pi samples are averaged and applied only to
the matching completed Mac jobs. The dashboard reports estimated Pi analysis
time and CPU avoided, the observed Pi-to-Mac ratios, and maximum measured Pi job
RSS. It does not extrapolate those estimates to unmatched work. These are
analysis-process estimates rather than net system-load measurements: Pi input
fingerprinting, source snapshots, queue handling, and SSH streaming for the Mac
run are not measured or subtracted. `wait4` maximum RSS does not sum concurrent
child-process memory, while Mac telemetry uses the higher of that leader maximum
and a sampled process-group aggregate, so the two RSS scopes also differ.

To deliberately seed a calibration, temporarily stop the Mac worker, let one
eligible unchanged task run through the ordinary guarded Pi fallback, restart
the worker, and submit that exact task again without changing its inputs,
arguments, source snapshot, or task policy. Do this only when the extra Pi load
is acceptable. There is intentionally no forced-placement benchmark command.

## Diagnostics

```zsh
launchctl print "gui/$(id -u)/com.jacobr.van-compute-worker"
launchctl kickstart -k "gui/$(id -u)/com.jacobr.van-compute-worker"
tail -n 100 "$HOME/Library/Caches/van-compute/logs/worker.stderr.log"
```

```bash
ssh pi@vanpi '/home/pi/van_compute/scripts/van_compute.py available'
ssh pi@vanpi 'systemctl status van-compute-broker --no-pager'
ssh pi@vanpi 'journalctl -u van-compute-broker -n 100 --no-pager'
```

## Drop-in agent instructions

```markdown
Always send CPU- or memory-intensive offline commands—including repository tests, APK/decompilation, SQLite queries, large corpus searches, Python analysis, and saved CAN or AlfaOBD log analysis—to `/home/pi/van_compute/scripts/pi_compute.py` as named tasks; never run those commands directly on vanpi. The compute service decides availability, Mac-versus-Pi placement, fallback, resource limits, and scheduling. Use `/home/pi/van_compute/scripts/pi_compute.py tasks` to discover task names. If no suitable task exists, add or review a `.van-compute.json` task instead of bypassing the service. Submit independent jobs before waiting when work can run in parallel.

Never send live CAN/SocketCAN access, interface setup, bus wake or UDS transmission, ADB/device access, network changes, mounts/storage operations, or service control through `pi_compute`; those remain local under their existing safety and authorization rules.
```
