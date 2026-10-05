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
capabilities, no swap allowance, a 1 GiB memory ceiling, and a 256-task cgroup
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

The shell path remains the supported compatibility entry point, but it now execs
the Python deployer with the same arguments. From this checkout in a freshly
opened Terminal.app or iTerm window—not a Codex-managed or otherwise sandboxed
shell:

```zsh
./macbook/scripts/install_van_compute_worker.zsh
```

Review the local-only plan before a live run. Dry-run reads and hashes the
selected source and optional dataset configuration, but creates no local files,
probes no executables or launchd jobs, and makes no SSH/SCP calls:

```zsh
./macbook/scripts/install_van_compute_worker.zsh --dry-run
```

The deployment remains coupled: there is no Pi-only cutover mode. Every live
install drains the worker, fences submissions, enters maintenance, switches the
Pi package, replaces the Mac worker, and requires a fresh compatible heartbeat
before releasing the queue.

The repository-wide updater is a supported entry point only after the initial
provider-first package cutover has completed. The Step 1 dashboard imports
`van_compute.metrics` from `/home/pi/van_compute/current`, while broad sync
updates Python packages before it runs the compute installer. Therefore the
first deployment must install and validate compute on both hosts before updating
the dashboard consumer:

```zsh
cd /Users/jacobr/dev/scripts
./macbook/scripts/install_van_compute_worker.zsh --dry-run
./macbook/scripts/install_van_compute_worker.zsh
python3 pi/deploy_python.py --update --service van-dashboard.service
ssh pi@vanpi.lan 'set -eu; /usr/bin/systemctl is-active van-compute-broker.service; /usr/bin/systemctl is-active van-dashboard.service; /home/pi/van_compute/scripts/van_compute.py available; /usr/bin/test -r /home/pi/van_compute/current/van_compute/metrics.py'
```

After that supervised provider-first cutover, routine repository-wide updates
remain supported:

```zsh
./pi/sync_scripts.sh
```

After its normal Pi deployment succeeds, `sync_scripts.sh` invokes the
compatibility entry point with `--if-needed`. A fingerprint covers both installer
entry points, the LaunchAgent template, the allowlisted `van_compute/` sources,
worker identity, connection target, dataset configuration, and isolation mode.
Source inputs are immediate `.py` modules in `van_compute/` and `entrypoints/`,
plus the broker unit and example policy in `configs/`; unrelated data, caches
and private config files are not transferred. Copied bytes are verified against
the planned source digest, including when an existing release is reused.
Matching local and Pi provenance plus healthy loaded services make the check exit
immediately. A changed or unhealthy deployment runs the ordinary drain-first
installer in the foreground, and any installer failure makes `sync_scripts.sh`
fail. A required compute upgrade may therefore take time; the updater never
reports success while that work is still running.

macOS cannot apply the worker's Seatbelt profile from inside another sandbox.
The installer checks that capability before downloading or building anything
and fails closed if profiles cannot be nested in its current environment. It
then provisions a private Mac Python environment and required offline tools,
validates the full Mac sandbox and process watchdog, stages and validates the
complete Pi package, provisions the locked Pi fallback environment, and only
then drains or fences production.

Both sides retain immutable, content-addressed releases and provenance:

```text
~/Library/Application Support/van-compute/
  releases/<24-hex-deployment-digest>/
    app/van_compute/...
    app/macbook/scripts/install_van_compute_worker.{py,zsh}
    app/macbook/launchagents/com.jacobr.van-compute-worker.plist
    venv/
    sandbox.sb
    source.sha256
    deployment.sha256
    provenance.json
    manifest.json
  datasets.json
  installer.lock
  installer-owner

/home/pi/van_compute/
  releases/<24-hex-source-digest>/
    van_compute/...
    source.sha256
    provenance.json
    manifest.json
  current -> releases/<digest>
  previous -> releases/<prior-digest>
  scripts/{van_compute.py,pi_compute.py,upgrade_gate.py}
  configs/{van-compute-broker.service,van-compute-obd.example.json}
  venv/
  runtime.lock
  deployment.sha256
```

The LaunchAgent pins its interpreter and `PYTHONPATH` to one verified Mac
release. The broker and dashboard import from the atomically switched Pi
`current` link. Reusing an existing digest verifies every manifest file and its
hash; a same-version repair leaves `previous` unchanged. The current Mac release
and one prior release are retained. Each Mac release carries the exact frozen
installer, package, and plist source needed to deploy that version again.
Ambiguous, mounted or unrecognized retention candidates are kept rather than
deleted. Mac same-version repairs skip pruning so they cannot mistake a newer
abandoned stage for the earlier rollback release.

**Installer behavior decisions:** the Python port preserves coupled fencing,
maintenance ownership, staging-before-drain and heartbeat-before-release. The
package layout adds content-addressed source releases, integrity manifests and
frozen installers. Unlike the former timestamp-only Mac installer, a repeated
manual install verifies and reuses the same immutable environment instead of
refreshing unpinned pip dependencies. It still performs the normal coupled
checks; only `--if-needed` can skip the healthy deployment entirely. A fresh
release still resolves currently available dependencies, so the source digest
alone does not guarantee dependency reproducibility across machines. A future
dependency refresh needs a new release identity, ideally a dependency lock;
never mutate a retained rollback environment.

The retired protocol copy under `/home/pi/scripts/python-automation/` is frozen:
leave it byte-unchanged, whether present or absent. Unlike the old shell
installer, this deployer neither deletes that copy nor rejects a successful
package cutover merely because it exists. Retirement also distinguishes a
confirmed unmounted path from a failed mount probe; an inspection error never
authorizes deletion.

The operational unit is also installed as a root-owned regular file at
`/etc/systemd/system/van-compute-broker.service`; systemd requires that
registration outside the application tree. It is intentionally not a symlink
into pi-owned `/home`, which would let the service account replace
root-interpreted configuration. This is the sole deployed-file exception.

Queue jobs and results remain runtime data under the configured `obd-things`
compute directory; they are not deployed files. The generic staging portion of
`pi/sync_scripts.sh` does not copy compute files. Its final conditional installer
call preserves the coupled Pi/Mac protocol boundary, so it cannot publish half
of an upgrade. During the one-time layout migration, the installer retires the
legacy `/home/pi/scripts/compute/` tree only after the replacement package
broker and worker have been validated.

Upgrades are drain-first. The installer requires the running queue to be empty,
disables new launches, asks a current persistent scheduler to stop claiming,
then temporarily fences the public Pi queue CLI. It waits up to 15 seconds for
an idle worker before forcibly unloading that disabled job, and up to 120
seconds for already loaded `van_compute submit` or `pi_compute run` processes.
It checks every queued or running entry, including hidden submission staging
directories, before placing the queue in maintenance through package-link
replacement and the first new coordinator heartbeat.

Installer ownership persists across reruns so an interrupted post-protocol
upgrade resumes forward without releasing another machine's maintenance lease.
A failure before cutover restores the exact previous CLI and LaunchAgent in that
order. Once package replacement begins, an incompatible previous worker is never
restarted: the queue remains fenced in maintenance and the supported recovery is
to rerun the same installer command until it validates the broker, worker, and
fresh heartbeat.

### Rollback and recovery commands

Before a supervised deployment, record the exact pre-deploy releases:

```zsh
PREVIOUS_MAC_RELEASE="$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:0' "$HOME/Library/LaunchAgents/com.jacobr.van-compute-worker.plist" | /usr/bin/sed 's#/venv/bin/python$##')"
PREVIOUS_PI_RELEASE="$(ssh pi@vanpi.lan '/usr/bin/readlink -e /home/pi/van_compute/current 2>/dev/null || true')"
printf 'Mac: %s\nPi:  %s\n' "$PREVIOUS_MAC_RELEASE" "$PREVIOUS_PI_RELEASE"
```

If a deployment reports that protocol replacement began or maintenance remains
active, do not switch either release link or bootstrap the old worker manually.
Resume the owned, fenced upgrade forward:

```zsh
cd /Users/jacobr/dev/scripts
./macbook/scripts/install_van_compute_worker.zsh
```

After a deployment completed successfully and released maintenance, a deliberate
rollback is a new coupled deployment through the prior installer. For a retained
Python-deployer release, use its frozen source; this applies all normal drain,
fence, stage, cutover, provenance, and heartbeat checks:

```zsh
test -x "$PREVIOUS_MAC_RELEASE/app/macbook/scripts/install_van_compute_worker.py"
test -n "$PREVIOUS_PI_RELEASE"
"$PREVIOUS_MAC_RELEASE/venv/bin/python" \
  "$PREVIOUS_MAC_RELEASE/app/macbook/scripts/install_van_compute_worker.py"
```

The first package deployment may follow an older worker release without a frozen
Python deployer. Never run that old shell installer: it deletes a file in the
frozen `/home/pi/scripts/python-automation/` tree and restores a layout that does
not satisfy the dashboard's new package import. An **incomplete, fenced cutover**
still requires the forward recovery command above.

After a **completed** cutover, the tested fallback is to restore the verified
Step 1 broker and worker implementations while retaining the new safe installer,
package entrypoints, unit and metrics provider. From the reviewed, merged S1
checkout, prepare a separate rollback worktree (creation refuses an existing
path; the chained commands stop on failure):

```zsh
ROLLBACK=/Users/jacobr/dev/scripts/.claude/worktrees/s1-step2-rollback
git -C /Users/jacobr/dev/scripts worktree add --detach "$ROLLBACK" HEAD && git -C "$ROLLBACK" restore --source=c69a7a6 --worktree -- van_compute/broker.py van_compute/worker.py && /opt/homebrew/bin/python3 "$ROLLBACK/macbook/scripts/install_van_compute_worker.py" --dry-run
```

Review the intentional two-file source changes and dry-run, then roll back **both
hosts together** through the unchanged drain/fence/health gates:

```zsh
/opt/homebrew/bin/python3 "$ROLLBACK/macbook/scripts/install_van_compute_worker.py"
```

Use the original host/worker identity and intended sandbox/dataset overrides.
This reverts Step 2 runtime implementations (including backoff), not package
layout, installed dependencies, configuration or queue state. The six unchanged
protocol/queue/metrics/frontend/upgrade-gate/limit-helper dependencies match
`c69a7a6`; the extra engine/config/backoff modules are not imported by those old
entry modules. Do not run broad sync or the master installer's `--if-needed`
until the cause is resolved: they would reinstall the newer runtime.

For later package-to-package rollbacks, the recorded `PREVIOUS_PI_RELEASE` is
verification evidence, not a link to set by hand. Confirm that the rollback
command publishes its digest as `current`, that `previous` names the release
being left, that the package metrics provider remains present for the dashboard,
and that the queue is out of maintenance:

```zsh
ssh pi@vanpi.lan 'set -eu; /usr/bin/readlink -e /home/pi/van_compute/current; /usr/bin/readlink -e /home/pi/van_compute/previous; /usr/bin/test -r /home/pi/van_compute/current/van_compute/metrics.py; /home/pi/van_compute/scripts/van_compute.py maintenance status; /home/pi/van_compute/scripts/van_compute.py available; /usr/bin/systemctl is-active van-compute-broker.service; /usr/bin/systemctl is-active van-dashboard.service'
```

The dashboard code expects `/home/pi/van_compute/current/van_compute/metrics.py`.
The owner must perform and verify the live cutover; offline tests do not establish
that it has happened. Dashboard application, template, static, and service changes
still deploy through `pi/sync_scripts.sh`; normal compute deployment only refreshes
an active dashboard already configured for that canonical package path.

It deploys, but deliberately does not activate, the example policy for the
separate live `obd-things` checkout. After reviewing it, activate it only if no
policy already exists:

```bash
ssh pi@vanpi.lan 'set -eu; cd /home/pi/dev/obd-things; test ! -e .van-compute.json; test ! -L .van-compute.json; install -m 600 /home/pi/van_compute/configs/van-compute-obd.example.json .van-compute.json; /home/pi/van_compute/scripts/pi_compute.py tasks'
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

The LAN deployment uses `pi@vanpi.lan`. Older installed LaunchAgents may still
contain `pi@vanpi`; changing repository defaults does not update a loaded agent.
If stderr reports `Could not resolve hostname vanpi`, compare the installed
plist and `launchctl print` arguments. Automatic restart retries the configured
hostname, so it cannot repair an obsolete connection target. A hostname-only
repair must preserve the installed release, dataset and isolation options, save
the original plist, reload the agent from a regular Terminal, and verify a fresh
10-slot lease on the Pi. Check for running jobs before restarting the worker.

The full installer deliberately refuses queued or running work. If the worker
is offline and jobs are waiting for it, restore connectivity to the existing
worker first so those jobs can finish; do not delete jobs or bypass the upgrade
preflight to change the hostname. A fresh worker lease, rather than a loaded
LaunchAgent alone, confirms availability.

```zsh
launchctl print "gui/$(id -u)/com.jacobr.van-compute-worker"
launchctl kickstart -k "gui/$(id -u)/com.jacobr.van-compute-worker"
tail -n 100 "$HOME/Library/Caches/van-compute/logs/worker.stderr.log"
```

```bash
ssh pi@vanpi.lan '/home/pi/van_compute/scripts/van_compute.py available'
ssh pi@vanpi.lan 'systemctl status van-compute-broker --no-pager'
ssh pi@vanpi.lan 'journalctl -u van-compute-broker -n 100 --no-pager'
```

## Drop-in agent instructions

```markdown
Always send CPU- or memory-intensive offline commands—including repository tests, APK/decompilation, SQLite queries, large corpus searches, Python analysis, and saved CAN or AlfaOBD log analysis—to `/home/pi/van_compute/scripts/pi_compute.py` as named tasks; never run those commands directly on vanpi. The compute service decides availability, Mac-versus-Pi placement, fallback, resource limits, and scheduling. Use `/home/pi/van_compute/scripts/pi_compute.py tasks` to discover task names. If no suitable task exists, add or review a `.van-compute.json` task instead of bypassing the service. Submit independent jobs before waiting when work can run in parallel.

Never send live CAN/SocketCAN access, interface setup, bus wake or UDS transmission, ADB/device access, network changes, mounts/storage operations, or service control through `pi_compute`; those remain local under their existing safety and authorization rules.
```

## Host compatibility decisions

The shared `van_compute.engine` owns child launch/wait/escalation, the `Sandbox`
interface (`BubblewrapSandbox` / `MacSandbox`), numeric rusage, and result
validation/archiving. It deliberately does not unify ownership, staging,
admission, scheduling, privacy filtering, or telemetry/publication. Every
pre-existing host difference below is retained, not silently hardened or
normalized. The frozen 258 protocol fixtures plus two child-environment
fixtures must remain byte-identical.

1. **Ownership and placement:** Pi has one mandatory-token local identity,
   authoritative manifest re-read, health/self-test/grace gates, and local
   recovery. Mac has ten exact-slot identities, optional legacy tokens, and
   remote lease resumption. Neither placement policy nor queue protocol changes.
2. **Supported tasks/runtimes:** Pi excludes datasets, corpus search and APK
   analysis and checks the required runtime. Mac supports those tasks, exposes
   only referenced datasets to execution, and checks all four runtime families
   at startup.
3. **Preparation:** Pi verifies/copies local sources and makes source/input
   files 0400 and directories 0500. Mac verifies streamed tar membership,
   hashes and sizes, without imposing those Pi modes. Both paths stay outside
   the engine.
4. **Isolation:** Pi requires bwrap, `/job/...` mappings, dedicated runtime
   bindings, read-only source/input mounts and no network. Mac retains real
   paths, its installed sandbox-exec profile and JOB_ROOT write scope. Legacy
   unsandboxed jobs and the explicit dynamic escape hatch remain unchanged.
   Process-group cleanup is not a container boundary: descendants that create
   another session can escape that cleanup, but still inherit the sandbox.
5. **Environment/cwd:** Pi keeps Linux PATH, C.UTF-8, outer workspace cwd,
   inner source cwd, placement variables and pytest cache suppression. Mac
   keeps Homebrew PATH, en_US.UTF-8, source cwd and delayed
   VAN_COMPUTE_CHILD_PYTHONPATH. The standalone limit helper remains
   package-import-free; untrusted sitecustomize cannot run ahead of limits.
6. **Limits:** Pi keeps 1800-second wall/1200-second CPU defaults, 768 MiB
   memory, hard-limit clamping and failure on limit refusal. Mac keeps
   3600-second wall, 16 GiB memory, 256-process watchdog and best-effort rlimits
   (CPU timeout+30/+60). Neither adds UID-wide RLIMIT_NPROC.
7. **Admission:** Pi reserves sources+inputs+2*result_limit with strict size
   validation and a 1 GiB free reserve. Mac keeps cross-slot reservations of
   max(2*sources+inputs, sources+inputs+2*result_limit), permissive counting,
   preparation/packaging checks, a 5 GiB reserve and release before upload.
   The typed resource_admission_stop_event is a drain event, not hard stop;
   the scheduler copies config rather than mutating its caller's event field.
8. **Watchdogs:** Pi checks disk every 0.5 seconds at reserve+result_limit and
   samples again after a fast exit. Mac checks RSS, process count then disk
   every second at reserve only, without a final sample. Thresholds, sampling
   order, error strings and exit codes remain host-specific, including Mac's
   legacy empty-watchdog-error graceful termination/143 behavior.
9. **Cleanup:** Both terminate descendants before publication. Pi records a
   cleanup boolean and fails on process-group inspection PermissionError;
   Mac treats PermissionError as existence and returns no cleanup boolean.
   Cleanup timing and TERM/KILL grace periods are preserved.
10. **Interruption/failure:** Pi publishes interruption as exit 143. Mac raises
    WorkerShutdown, uploads nothing and leaves its exact-slot lease resumable.
    Pi watchdog failures drop task output and keep telemetry; Mac follows its
    existing validation/packaging path. Other local failures retain exit 70 and
    their original distinct diagnostic prefixes.
11. **Archives:** Pi preserves tar ownership/names/mtime, random mkstemp,
    flush/fsync and descriptor-based size validation. Mac zeros tar metadata,
    uses an exclusive fixed .partial file, caps enumeration at 100,000 entries,
    and also rejects collision with another declared output. Error ordering
    remains distinct. Mac gzip headers are not newly normalized.
12. **Results:** Pi restricts legacy files to metadata plus the catalog result;
    Mac permits any legacy filename within its limits. Mac retains aggregate
    raw-output checking before redaction/archiving. Both reject symlinks and
    retain their existing handling of undeclared nonregular entries.
13. **Privacy/telemetry:** Pi records concrete sandbox argv, top-level phase
    timings, sandbox/cleanup fields and idempotent eligible-local events. Mac
    records logical argv, nested timing and private-path-scrubbed results.
    Host-specific rusage notes, worker identity and execution.json-last upload
    order are unchanged.
14. **Serialization:** Intermediate execution.json writes and UTC/monotonic
    call ordering are contractual too: Pi file success writes twice and
    directory/resource-limit paths three times; Mac ordinary paths three,
    directories four and interruption once. Goldens cover these intermediate
    bytes, not just final results.

**Only approved runtime change — unreachable-host retry:** persistent Mac mode
uses one thread-safe host-wide gate across all four SSH multiplexers, claims,
heartbeats and transfers. Delays are poll_interval, 2*poll_interval, then capped
at max(60 seconds, poll_interval): the defaults are 15, 30, 60, 60… without
jitter. Concurrent failures from one generation count once; only one recovery
probe runs after a cooldown. Successful communication resets the gate, even
when the remote application rejects an operation or returns malformed JSON.
SSH status 255, transport timeout and connection-establishment failures back
off; empty/maintenance claims and task/application failures do not.

Startup establishes connections lazily, so an offline Pi no longer causes a
service restart loop. The startup JSON describes the local scheduler, not
verified broker reachability. `--run-once` stays fail-fast. Only outage waits
introduce cancellation points: hard stop wakes promptly, drain cancels waiting
claims/admission but lets active jobs heartbeat/upload. Healthy-call stop/drain
semantics remain unchanged. The gate never replays a failed upload or finish;
existing exact-slot recovery remains authoritative.
