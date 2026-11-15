# Van compute broker

[Repository README](../../../README.md)

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
  run one eligible task locally. CAN transmit activity, memory, swap paging,
  CPU use, temperature, throttling, disk free space, runtime, file size, and
  cgroup limits guard that fallback (see Pi fallback gate).
- A remotely claimed job whose exact slot heartbeat has been stale for five
  minutes is conservatively returned to the queue. Attempt tokens reject late
  uploads from the superseded worker.
- Jobs too risky for the Pi fallback, notably JADX decompilation and configured
  corpus searches, remain queued until a remote worker is available.

### Pi fallback gate

The broker starts a Pi-local job only when every check passes. Rates come from
a sampler that keeps its previous reading: the delta since the last poll, or a
fresh one-second window when there is no reading, it is over 30 s old, or any
single counter (each swap counter and each CPU field, checked separately) went
backwards. A missing field, a negative value, an unreadable source or a fresh
window that is still invalid defers the job (`health probe failed`); nothing is
ever read as healthy by default.

| Check | Default | Flag | Why |
| --- | --- | --- | --- |
| CAN transmit | no write lock on any `active-diagnostics-*.lock` now, and none for the last 60 s | `--can-lock-dir`, `--can-quiet-seconds` (0-3600) | The one hard rule: the Pi must not be loaded while it sends CAN frames. |
| MemAvailable | at least 1024 MiB | `--min-available-memory-mb` (at least 256 and at least `--max-memory-bytes`) | The job's 768 MiB ceiling plus 256 MiB for other services. |
| Swap paging | at most 1024 pages/s (`pswpin` + `pswpout`, 4 MiB/s) | `--max-swap-pages-per-second` | Current thrash, not cold pages parked in swap (the idle Pi keeps about 0.9 GiB there). |
| CPU busy | at most 0.90 of CPU time, excluding idle and iowait | `--max-cpu-busy` | Backup I/O wait (load average 5-6 at idle) must not block work; real compute pressure does. |
| Temperature | at most 75 C | `--max-temperature-c` | Unchanged. |
| Throttling | no current bits 0-3 of `vcgencmd get_throttled` | none | Unchanged. |

`HealthThresholds` in `van_compute/config.py` is the single definition of these
defaults, and the service unit passes no gate flags. Load average and swap used
are reported in the health snapshot but no longer gate. The removed flags
`--max-swap-used`, `--max-swap-used-mb` and `--max-load-per-cpu` still parse
only to exit with an error naming `--max-swap-pages-per-second` or
`--max-cpu-busy`.

The memory minimum is not guaranteed headroom for other services. The job's
768 MiB limit is per-process address space (`RLIMIT_AS`), and the unit's 1 GiB
`MemoryMax` covers the broker plus every descendant, so a job and the broker
together may use more than the 256 MiB the minimum leaves free.

CAN lock contract. The Pi CAN transmitters in obd-things that take the lock
each take a nonblocking exclusive `flock` on `active-diagnostics-<name>.lock` in
`/home/pi/dev/obd-things/tmp/locks` for as long as it may transmit; the kernel
drops it when the holder exits, so it cannot go stale. The broker reads
`/proc/locks` and matches each record's `major:minor:inode` (device in hex,
inode in decimal) against a fresh `stat` of the lock files on every probe. Any
WRITE lock, held or waited for (FLOCK, POSIX or OFDLCK), counts as active; READ
locks (passive observers such as the telemetry broker) are ignored. The broker
never opens, creates or locks those files, since a probing shared lock could
make a sender's `LOCK_EX|LOCK_NB` fail. A missing lock directory, a listing or
`stat` error, an unreadable `/proc/locks` or an unparseable record about a lock
file counts as CAN active (fail closed). The obd-things lock files must never
be replaced or removed while they have owners: a replacement is a different
inode that the old holders do not lock.

Running jobs are stopped, not just kept from starting. While a Pi-local child
runs, the broker checks CAN at most every 0.5 s (the same probe, fed to the same
quiet chain). The first stop cause latched wins:

- A CAN write lock (`pi-can-transmit`) or a probe failure (`can-probe-failed`,
  fail closed) sends SIGTERM to the job's process group, then SIGKILL after 1 s
  instead of the usual 10 s, and kills any descendant left behind at once. The
  broker discards the job's temporary outputs and local work, then, under one
  queue lock, re-reads the manifest and checks that the worker is the local
  one and the lease token is this attempt's. If so it appends an
  `attempt_history` entry (reason `pi-can-transmit` or `can-probe-failed`),
  strips the ownership fields, resets the result directory and moves the job to
  `queued/` before publishing the manifest (the order the stale-remote-lease
  recovery uses, so a crash cannot strand it). It logs
  `{"action": "requeued", "reason": ..., "job_id": ...}` and publishes no
  results, validation, packaging or completion telemetry and never calls
  `worker_finish`. A later claim gets a fresh lease token, and uploads under
  the old token are rejected. If ownership was lost, or the queue is under
  maintenance, the job is left exactly as it is.
- A service stop keeps today's behavior: the job fails with an interrupted
  result.
- CAN is also checked before staging and again, with the task cache held,
  immediately before the child launches; a lock there requeues with no child
  started.
- Any exception while waiting on a child (or while cleaning up after it) must
  end with the job's process group confirmed gone before the cache lock is
  released or the work directory removed. Cleanup never makes a blocking wait:
  it SIGKILLs the group and polls non-blockingly for about a second, then
  holds: it keeps re-killing and checking about once a second, logging a
  `cleanup-hold` JSON line when the hold starts and about every 60 s (every
  log call is best-effort and cannot end the hold). Once the hold starts, the
  running manifest carries `cleanup_unconfirmed`, `cleanup_group` and
  `cleanup_boot_id` (and a `cleanup-unconfirmed` attempt-history entry); the
  write is retried every iteration. When the group is gone the marker is
  cleared (retried until it is) and the original error takes the normal
  failure or requeue path. If the broker is told to stop first (SIGTERM or
  SIGINT; foreground `--once` installs the same handlers), the hold raises
  `TerminationUnconfirmed` and publishes and requeues nothing. A stop exit does
  not prove the group is gone: systemd only logs "Processes still around after
  SIGKILL" for uninterruptible descendants and may restart the unit, and a
  foreground `--once` run has no cgroup containment at all.
- So a marked running job is handled before any new claim, and before the
  health gate, the staging-capacity check, any staging directory and (under
  `--once`) the initial CAN quiet wait: killing the old group and discarding
  its cache entry depend on neither disk space nor CAN. On the same boot (`cleanup_boot_id` matches `/proc/sys/kernel/random/boot_id`)
  while `cleanup_group` still has members, the broker holds that group the
  same way and claims nothing else. It then discards the job's task-cache
  entry under the cache's exclusive lock (never holding the queue lock), so a
  late write by a surviving process lands in unlinked files, and fails the job
  without running it. A different boot id means a reboot ended the group; a missing
  one skips the same-boot hold too, which on the Pi (where the boot id is always
  readable) happens only after a reboot. The cache entry is still discarded,
  retried (with `cache-discard` log lines at the start and about every 60 s)
  while it is busy or its namespace cannot be inspected. The failure itself is
  published once staging space allows, waiting (logged as `failure-publication`)
  until then. There is
  no timeout-based release. Residual case: a marker that could not be written
  (the queue write kept failing until the stop) leaves nothing for the next run
  to find; the stop then logs that it could not write the marker.

The guarantee is bounded, and covers only senders that take the lock (the
`vonstar` unlock goes through `can_handoff.active_turn` and radar through
transmit permits that revalidate the exclusive lock; arbitrary senders are not
proven to). No Pi-local job starts while a CAN transmitter lock is
held or until CAN has been quiet for `--can-quiet-seconds`, rechecked just
before launch (a slow staging or cache wait that breaks the quiet chain
requeues the job with no child started); a running job's
process group is gone about 1.5 s after a lock appears (0.5 s watch, SIGTERM,
SIGKILL after 1 s). A burst shorter than the watch interval can pass unseen,
and strict zero overlap would need a handshake with the senders. Once the child
has exited on its own, its results publish without another CAN check: that is a
bounded local copy at the unit's `IOWeight=10`.

`--once` first watches CAN every 0.5 s for up to `--can-quiet-seconds` (stopping
early, and deferring without placing a job, if a lock appears or the probe
fails). `--health` prints the current snapshot
and assessment as JSON and exits 0 whether or not the gate passes (an unreadable
source is reported as unhealthy JSON; invalid arguments still exit nonzero); it takes no
queue lock, so it works beside the service under its systemd sandbox. Its quiet
time counts only from that process's start.

## Safety boundaries

Repository tasks are declared in `.van-compute.json`. A declaration chooses a
fixed executable family and a shell-free argument template. Supported profiles
cover repository tests, Python scripts/modules, saved CAN-log analysis,
read-only SQLite, ripgrep corpus search, and JADX APK analysis. Shells,
SocketCAN, ADB, SSH, service managers, and network tools are not executable
families.

The Mac installer fails closed unless it can validate the effective
`sandbox-exec` profile. Each job gets a clean environment and a private
HOME and TMP, and a private cache unless its task opts into a persistent one
(see Persistent task caches); no credentials passed through its environment, no
network except the package registries through the worker's proxy (see Mac
registry proxy), declared read-only datasets only, monitored resource
ceilings, and a writable job directory (plus its one declared cache) only.
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

## Mac registry proxy

Sandboxed Mac jobs (`--sandbox-profile`) need `pip install` and `npm ci`, and
Seatbelt cannot allow a host name: the profile can only grant TCP to one
loopback port. So the worker runs a CONNECT proxy
(`van_compute/registry_proxy.py`) in its own process, outside the sandbox, and
the profile allows `remote tcp` to `localhost:PROXY_PORT` and nothing else (no
DNS, no UDP). Pi jobs still have no network, and an unsandboxed Mac worker
(`--allow-unsandboxed-dynamic`) runs no proxy and keeps full network.

- **Allowlist.** `CONNECT <host>:443` only, for `pypi.org`,
  `files.pythonhosted.org` and `registry.npmjs.org` (`REGISTRY_HOSTS`, defined
  once in that module). IP literals, other ports, trailing dots and plain-HTTP
  or absolute-URI requests are refused. Adding a registry means editing that
  set and reviewing the risk below.
- **Addresses.** The proxy resolves the name itself and refuses it if any
  answer is not global unicast (private, loopback, link-local, CGNAT,
  multicast, or an IPv6 form embedding one), so a poisoned or rebound name
  cannot reach the LAN (router, Pi, UBNT) or loopback services. It connects to
  the address it vetted and never re-resolves.
- **SNI.** The registries share CDN addresses with other sites, so a tunnel
  bounded by address alone could reach them. Before connecting upstream the
  proxy reads the client's first TLS record and requires a ClientHello, whole
  in that record, whose SNI equals the CONNECT host and that has no encrypted
  ClientHello extension; it then forwards that record and relays opaquely.
- **Bounds.** At most 64 tunnels (excess connections get 503 and are closed
  before any thread or lookup); 8 KiB request head. Every phase has a
  timeout: head and ClientHello (10 s each), name lookup (10 s, then 504),
  upstream connect (10 s shared by all of a name's addresses), idle tunnels
  (120 s). One absolute deadline, a tunnel's whole life (1 hour), caps all of
  them, sends included: relaying is nonblocking with at most 256 KiB
  buffered per direction, so a client that stops reading loses its slot at
  the deadline. Lookups run on a fixed pool of 4 daemon threads because
  `getaddrinfo` cannot be interrupted; a tunnel stops waiting at its deadline
  or at `stop()`, its queued lookup leaves the queue, and a late answer is
  discarded. The queue holds at most 64 lookups (a full queue gets 503), so
  stuck resolvers retain at most that plus one lookup per thread. Each socket a tunnel opens
  is closed on every exit path, and `stop()` closes any a stuck handler
  still holds. Each tunnel is logged to the worker log with host, bytes each
  way, duration and how it ended; refusals are logged with their reason.
- **Lifecycle.** `main()` starts the proxy only with `--sandbox-profile`,
  around both `--run-once` and the persistent scheduler, and stops it after
  every job child is reaped, drain included. It holds the same port on
  127.0.0.1 and ::1 (Seatbelt's `localhost` covers both), retrying a fresh
  port if ::1 has it taken, so no other service can sit on the granted port.
  A proxy that cannot bind fails worker startup. The port is threaded
  explicitly through `WorkerConfig.registry_proxy_port` to the job; jobs get
  `HTTPS_PROXY`/`https_proxy` (no `HTTP_PROXY`, no `NO_PROXY`) and
  `PIP_USE_DEPRECATED=legacy-certs` only then, and
  the profile is never applied without a live port (`PROXY_PORT=0` would make
  `sandbox-exec` fail). Children orphaned by a worker crash lose their network
  when the next start picks a new port.
- **Accepted risk.** No TLS interception: inside the tunnel a job can talk to
  an allowed registry for any purpose, such as publishing with a token it
  holds, and HTTP domain fronting inside the encrypted channel is refused only
  by the CDNs' own SNI/Host matching.
- **Certificate trust.** The profile grants no Mach services, so the macOS
  trust daemon (trustd) is unreachable: granting it would let a job make
  trustd fetch AIA/OCSP URLs named in a crafted certificate, outside the
  sandbox. npm and curl ship or read their own CA bundles; pip >= 24.2 uses
  trustd by default, hence `PIP_USE_DEPRECATED=legacy-certs` (pip's bundled
  certifi). It is a compatibility default, not the boundary: a job may unset
  it, and a pip that drops the option rejects it (exit 3, invalid choice) or
  falls back to trustd and fails certificate checks (OSStatus -26276); either
  way pip fails without gaining network. Verified 2026-11-14 that a job
  granted `com.apple.trustd.agent` can make trustd fetch an AIA URL on a port
  the job itself is denied. Verified live 2026-11-14 against real
  pypi.org and registry.npmjs.org: obd-things' `pip install --only-binary=:all:`
  and `npm ci` succeed sandboxed; example.com, a direct 1.1.1.1:443, an SNI
  that differs from the CONNECT host, and loopback port 22 are all refused.
  The offline installer probe cannot catch a trust-store failure.
- **Installer probe.** Offline: the installer runs the staged release's proxy
  (staged venv Python, outside the sandbox) with every allowlisted name mapped
  to a local echo server, and keeps it up for every sandbox check. Inside the
  sandbox the probe tunnels through the proxy on 127.0.0.1 and ::1 with both
  directions relayed, sees an unlisted host and port 80 refused, and gets
  EPERM for TCP to another loopback port and to a public address and for UDP
  to the proxy port. The echo server must see exactly the two good tunnels.

## Persistent task caches

A repo task may declare `"cache": "<name>"` in `.van-compute.json` (lowercase
letters, digits and hyphens, at most 64 characters). That job's
`XDG_CACHE_HOME` is then a directory that outlives the job, instead of a
private per-job one. It exists to avoid re-downloading package-manager
downloads on every run; venvs and results stay per job.

- **Scope.** One cache per worker (Pi broker and Mac worker each keep their
  own), keyed by the job's `source_root` path and the declared name, so the
  same name in two repos is two caches. Isolation holds between jobs admitted
  through normal submission, which fixes `source_root`; a same-account
  submitter who forges `source_root` can reach another repo's cache.
- **Not trusted, not durable.** A cache is never results and never a trusted
  input: any job of that repo and name can leave anything in it, and it may
  vanish at any time (eviction, `clear`). Tasks must tolerate an empty cache
  and treat its contents as untrusted.
- **Sharing.** Concurrent jobs of one cache share the directory and must lock
  what they need themselves (obd-things `flock`s its own lock file inside the
  cache). The store holds a shared `flock` for each job's whole run, until the
  child and its process group are gone, and takes an exclusive one only to
  initialize, evict or clear, so a cache in use is never touched.
- **Eviction.** Between jobs (at startup and after each job) whole idle caches
  are evicted least-recently-used first until the total is under
  `--task-cache-max-bytes` (Pi 2 GiB, Mac 4 GiB) and the filesystem has twice
  the host's free-space reserve free. That is a target, not a quota:
  concurrent active caches can exceed it. A cache that cannot be measured
  (for example a mode-000 directory) counts as over the cap and is evicted
  when idle.
- **Network.** Pi jobs run with no network, so a Pi cache fills only from what
  the job brings in. Sandboxed Mac jobs reach only the package registries
  through the worker's proxy (see Mac registry proxy); an unsandboxed Mac
  worker (`--allow-unsandboxed-dynamic`, the current install) has full network.
- **Locations.** Pi: `<queue root>/task-caches` for whichever `--root` the broker
  runs with (override with `--task-cache-root`; it must be writable by the
  unit, and the default sits inside its `ReadWritePaths`). Mac:
  `~/Library/Caches/van-compute/task-caches`. Each must be on the same
  filesystem as the host's work root; startup fails otherwise so the
  free-space watchdog and admission cover it.
- **Mac sandbox.** The profile grants read/write on the `CACHE_ROOT`
  parameter, which is the selected cache directory for a cached job and the
  job's own `cache` directory otherwise. The installer probe checks the
  selected cache is usable and that a sibling cache, a private sentinel and a
  symlink inside the cache pointing at the sentinel are not writable.
- **Operate.** `python3 -m van_compute.task_cache [--root R] list` shows
  namespace, name, bytes (or unmeasurable), last use and whether in use;
  `clear --all` or `clear <namespace>/<name>` removes idle caches and reports
  busy ones. `execution.json` records `task_cache` for cached Mac jobs.
- **Deploy order.** A pre-cache worker rejects a job whose execution carries
  `cache` and fails it (it never runs silently uncached). Deploy both hosts
  before a repo declares a cache, and drop the declarations before rolling
  back to a pre-cache release while cached jobs may be queued.

## Completed-history retention

The administrative `prune` command accepts `--keep-days` (one maximum age for
both successful and failed jobs) and `--max-history-bytes` (local allocated
space under the queue's `done/` and `failed/` directories). Without these
options the existing 60-day successful / 31-day failed policy remains in force.
Preview is the default; `--apply` authorizes deletion.

Expired uncited jobs are selected first. If history remains above the space
budget, younger uncited jobs are selected oldest-first until it fits. A size
budget therefore takes precedence over retaining the full age window. Cited
jobs, transitive dependencies and active-job dependencies remain protected;
active jobs and archive targets are outside completed-history accounting.

`history_budget` reports allocated bytes before, projected remaining bytes,
and actual remaining bytes measured again after apply. The command returns
nonzero when it cannot satisfy the applicable projected/actual budget.
Protected, malformed, mounted and duplicate-ID entries are never force-deleted
to make a quota. Hardlinked blocks are counted once in measured totals and
treated conservatively in per-job reclaim projections. Citation rescanning,
queue locking, submission fencing and inode revalidation remain mandatory.

The obd-things owner policy passes `--keep-days 30 --max-history-bytes 2147483648`.
That repository owns its completion-triggered and hourly systemd maintenance;
this package owns selection, accounting and deletion. A completed job can
briefly exceed the retention target before the triggered maintenance finishes.
This feature does not place a filesystem quota on active computation.

Deploy this CLI update through the coupled installer below before updating a
consumer to require the new flags. Older installed CLIs reject those flags
before performing any deletion; do not modify immutable deployed packages.

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

### Current deployment ownership

The initial provider-first compute cutover is complete; do not repeat it. The
separate dashboard repository migration is also live (2026-11-03). The sibling
`~/dev/van-dashboard` repo now owns dashboard deployment through
`~/dev/van-dashboard/scripts/deploy.py`, which runs
`scripts/check_compute_provider.py` before deployment; its dry-run is entirely
local. Use that repo's `docs/REACT_DASHBOARD.md` deployment section for current
dashboard updates. The van-dashboard repo's `docs/cutover.md` records the
dashboard migration and its recovery context, not a requirement to redo it.
Do not deploy the dashboard with the retired scripts service selector.

Broad scripts sync still updates scripts and invokes the compute installer,
not the dashboard deployer. It no longer preflights the compute provider (that
check moved to the dashboard deployer). It uploads scripts, then
`sync_compute` runs the compute installer with `--if-needed`; the installer owns
its drain, fence and heartbeat gates and may install a missing or unhealthy
provider. Do not assume broad sync verifies provider prerequisites before it
writes.

The compute safety requirements still apply: use reviewed source, review the
local-only plan first, and retain the coupled
Pi/Mac installer and its drain/fence/heartbeat gates. Installed compute releases
carry their own frozen installer CLI and sibling helper package for recovery,
independent of the deployment worktree. Broad sync reads the primary `scripts`
checkout and the configured `van-compute` checkout: reconcile both reviewed
checkouts without discarding unrelated work before using it.

### Historical first compute cutover (completed; DO NOT RUN)

The archived recipe below predates the dashboard repository split. It installed
and validated compute on both hosts before updating the old dashboard consumer,
which imported `van_compute.metrics` from `/home/pi/van_compute/current`. That
provider-first order was necessary because broad sync updated Python packages
before running the compute installer. The recipe used reviewed full
`COMPUTE_REV` and `SCRIPTS_REV` commit IDs and clean detached worktrees retained
until deployment completed.

**DO NOT RUN this obsolete recipe.** Its `SCRIPTS_REV`, scripts worktree and
`pi/deploy_python.py --service van-dashboard.service` belong only to the
completed historical cutover, not current installation or dashboard updates.
Use the current deployment owners above; neither migration must be redone.

<details>
<summary>Archived first-cutover shell recipe — DO NOT RUN</summary>

```text
# HISTORICAL ONLY — DO NOT RUN
( set -eu
: "${COMPUTE_REV:?set COMPUTE_REV to the reviewed van-compute commit ID}"
: "${SCRIPTS_REV:?set SCRIPTS_REV to the reviewed scripts commit ID}"
COMPUTE=/Users/jacobr/dev/van-compute
SCRIPTS=/Users/jacobr/dev/scripts
COMPUTE_DEPLOY="$COMPUTE/.claude/worktrees/compute-first-deploy"
SCRIPTS_DEPLOY="$SCRIPTS/.claude/worktrees/compute-first-deploy"
git -C "$COMPUTE" worktree add --detach "$COMPUTE_DEPLOY" "$COMPUTE_REV"
git -C "$SCRIPTS" worktree add --detach "$SCRIPTS_DEPLOY" "$SCRIPTS_REV"
test "$(git -C "$COMPUTE_DEPLOY" rev-parse HEAD)" = "$COMPUTE_REV"
test "$(git -C "$SCRIPTS_DEPLOY" rev-parse HEAD)" = "$SCRIPTS_REV"
test -z "$(git -C "$COMPUTE_DEPLOY" status --porcelain --untracked-files=all)"
test -z "$(git -C "$SCRIPTS_DEPLOY" status --porcelain --untracked-files=all)"
cd "$COMPUTE_DEPLOY"
export VAN_COMPUTE_DATASET_CONFIG=/Users/jacobr/dev/van-compute/macbook/secrets/van-compute-datasets.json
test -r "$VAN_COMPUTE_DATASET_CONFIG"
./macbook/scripts/install_van_compute_worker.zsh --dry-run
/opt/homebrew/bin/python3 "$SCRIPTS_DEPLOY/pi/deploy_python.py" --dry-run --update --service van-dashboard.service
./macbook/scripts/install_van_compute_worker.zsh
/opt/homebrew/bin/python3 "$SCRIPTS_DEPLOY/pi/deploy_python.py" --update --service van-dashboard.service
ssh pi@vanpi.lan 'set -eu; /usr/bin/systemctl is-active van-compute-broker.service; /usr/bin/systemctl is-active van-dashboard.service; /home/pi/van_compute/scripts/van_compute.py maintenance status; /home/pi/van_compute/scripts/van_compute.py available; /usr/bin/test -r /home/pi/van_compute/current/van_compute/metrics.py'
)
```

</details>

### Routine scripts and compute updates

Routine repository-wide scripts and compute updates remain supported; dashboard
updates use the separate deployer above:

```zsh
/Users/jacobr/dev/scripts/pi/sync_scripts.sh
```

After its normal Pi deployment succeeds, the monorepo's `sync_scripts.sh` invokes the
compatibility entry point with `--if-needed`. A fingerprint covers both installer
entry points, every named module in the installer helper package, the LaunchAgent
template, the allowlisted `van_compute/` sources, worker identity, connection
target, dataset configuration, and isolation mode. Source inputs are immediate
`.py` modules in `van_compute/` and `entrypoints/`, plus the broker unit and
example policy in `configs/`; unrelated data, caches and private config files are
not transferred. Copied bytes are verified against
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
  releases/<deployment24>-<build_uuid32>/  # legacy 24-hex releases also accepted
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
hash **during staging, before draining/fencing or stopping the broker**; Python
bytecode caches are deliberately ignored on reused Pi releases. Unknown files,
changed source, links and special entries still fail closed. A same-version
repair leaves `previous` unchanged. The current Mac release
and one prior release are retained. Each Mac release carries the exact frozen
installer, package, and plist source needed to deploy that version again.
Ambiguous, mounted or unrecognized retention candidates are kept rather than
deleted. Mac same-version repairs skip pruning so they cannot mistake a newer
abandoned stage for the earlier rollback release.

**Installer behavior decisions:** the Python port preserves coupled fencing,
maintenance ownership, staging-before-drain and heartbeat-before-release. The
package layout adds content-addressed source releases, integrity manifests and
frozen installers. Healthy matching Mac releases are reused rather than silently
refreshing unpinned dependencies. Every live run recreates the private
`~/Library/Caches/van-compute/{logs,jobs,ssh}` directories; `.DS_Store` files are
ignored and never transferred. The pinned interpreter and required imports are
smoke-tested even for `--if-needed`. Missing/broken interpreters or imports cause
a new immutable sibling generation to be built and validated **before drain**;
source/provenance corruption still fails closed. A new generation has a recorded
build UUID and is built at its final path so venv entrypoint shebangs remain valid.
Incomplete generations are never reused. Rebuild/repair leaves all existing
releases untouched and skips pruning, preserving earlier rollback environments.

To deliberately refresh dependencies, use the documented rebuild option:

```zsh
./macbook/scripts/install_van_compute_worker.zsh --rebuild --dry-run
./macbook/scripts/install_van_compute_worker.zsh --rebuild
```

`--rebuild` overrides `--if-needed`, but never bypasses the coupled staging,
drain, fencing or heartbeat gates. Ordinary reruns prefer the installed healthy
generation; a frozen installer prefers its own generation for dependency rollback.
Only `--if-needed` skips a fully healthy, unfenced deployment. A fresh build still
resolves currently available dependencies, so the stable desired-source digest
alone does not guarantee dependency reproducibility across machines; a dependency
lock remains future work.

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
the sibling `scripts` checkout's `pi/sync_scripts.sh` does not copy compute files.
Its final conditional installer
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
fresh heartbeat. `--if-needed` never skips active maintenance or an upgrade-owner
record, even if both releases and the coordinator otherwise look healthy.
SIGHUP and SIGTERM use the same cleanup path as Ctrl-C; installing the LaunchAgent
explicitly enables it before bootstrap, recovering a job left disabled by an
uncatchable interruption. SIGKILL cannot run cleanup: rerun the safe installer.

### Rollback and recovery commands

Before a supervised deployment, record the exact pre-deploy releases:

```zsh
PREVIOUS_MAC_RELEASE="$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:0' "$HOME/Library/LaunchAgents/com.jacobr.van-compute-worker.plist" | /usr/bin/sed 's#/venv/bin/python$##')"
PREVIOUS_PI_RELEASE="$(ssh pi@vanpi.lan '/usr/bin/readlink -e /home/pi/van_compute/current 2>/dev/null || true')"
printf 'Mac: %s\nPi:  %s\n' "$PREVIOUS_MAC_RELEASE" "$PREVIOUS_PI_RELEASE"
```

If a deployment reports that protocol replacement began or maintenance remains
active, do not switch either release link or bootstrap the old worker manually.
Resume the owned, fenced upgrade forward using the frozen installer in the Mac
release built for that deployment, not a disposable source worktree. If the
LaunchAgent already pins that intended generation, obtain its path with:

```zsh
RECOVERY_PYTHON="$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:0' "$HOME/Library/LaunchAgents/com.jacobr.van-compute-worker.plist")"
RECOVERY_RELEASE="${RECOVERY_PYTHON%/venv/bin/python}"
test "$RECOVERY_RELEASE" != "$RECOVERY_PYTHON"
```

**Before LaunchAgent replacement, its plist may still name the older release.**
In that case, set `RECOVERY_RELEASE` to the exact newly built generation for the
interrupted deployment instead; check its `provenance.json` against the intended
source. Never resume a post-cutover upgrade with that older installer. Preserve
the original host/worker identity and dataset/isolation overrides. Then run:

```zsh
( set -eu
RECOVERY_INSTALLER="$RECOVERY_RELEASE/app/macbook/scripts/install_van_compute_worker.zsh"
test -x "$RECOVERY_INSTALLER"
"$RECOVERY_INSTALLER" --dry-run
"$RECOVERY_INSTALLER"
)
```

The frozen `.zsh` entrypoint uses the system Python, so recovery also works when
the release's private venv interpreter needs repair.

After a deployment completed successfully and released maintenance, a deliberate
rollback is a new coupled deployment through the prior installer. For a retained
Python-deployer release **containing these recovery fixes**, use its frozen source;
this applies all normal drain,
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
package entrypoints, unit and metrics provider. Pin the rollback base to the
reviewed filtered-history S1 commit `02a182d`, not the caller's `HEAD`. Copy the safe deployer from
the **successfully deployed fixed Mac release** into that base; do not revive
S1's pre-fix installer. Current frozen installers include the sibling
`van_compute_installer/` package and the overlay copies it with the CLI files;
a legacy monolithic frozen installer has no helper directory and remains
self-contained. Prepare a separate rollback worktree (creation refuses
an existing path; the chained commands stop on failure):

```zsh
SAFE_INSTALLER_RELEASE="$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:0' "$HOME/Library/LaunchAgents/com.jacobr.van-compute-worker.plist" | /usr/bin/sed 's#/venv/bin/python$##')"
ROLLBACK=/Users/jacobr/dev/van-compute/.claude/worktrees/s1-step2-rollback
test -x "$SAFE_INSTALLER_RELEASE/app/macbook/scripts/install_van_compute_worker.py" && git -C /Users/jacobr/dev/van-compute worktree add --detach "$ROLLBACK" 02a182d && git -C "$ROLLBACK" restore --source=a8637e8 --worktree -- van_compute/broker.py van_compute/worker.py && cp "$SAFE_INSTALLER_RELEASE/app/macbook/scripts/install_van_compute_worker.py" "$ROLLBACK/macbook/scripts/install_van_compute_worker.py" && cp "$SAFE_INSTALLER_RELEASE/app/macbook/scripts/install_van_compute_worker.zsh" "$ROLLBACK/macbook/scripts/install_van_compute_worker.zsh" && { if test -d "$SAFE_INSTALLER_RELEASE/app/macbook/scripts/van_compute_installer"; then cp -R "$SAFE_INSTALLER_RELEASE/app/macbook/scripts/van_compute_installer" "$ROLLBACK/macbook/scripts/"; fi; } && /opt/homebrew/bin/python3 "$ROLLBACK/macbook/scripts/install_van_compute_worker.py" --dry-run
```

Review the intentional broker/worker rollback and safe-installer overlay, then
the dry-run; roll back **both hosts together** through the unchanged
drain/fence/health gates:

```zsh
/opt/homebrew/bin/python3 "$ROLLBACK/macbook/scripts/install_van_compute_worker.py"
```

Use the original host/worker identity and intended sandbox/dataset overrides.
This reverts Step 2 runtime implementations (including backoff), not package
layout, installed dependencies, configuration or queue state. The six unchanged
protocol/queue/metrics/frontend/upgrade-gate/limit-helper dependencies match
`a8637e8`; the extra engine/config/backoff modules are not imported by those old
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
that it has happened. Dashboard application, frontend, and service changes deploy
through `/Users/jacobr/dev/van-dashboard/scripts/deploy.py`, which runs
`scripts/check_compute_provider.py`. Broad scripts sync still runs the compute
installer; normal compute deployment only refreshes an active dashboard already
configured for that canonical package path.

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

## Completed-job retention

The hidden operator command on the queue CLI is dry-run by default:

```bash
/home/pi/van_compute/scripts/van_compute.py prune
/home/pi/van_compute/scripts/van_compute.py prune \
  --citations-root /home/pi/dev/obd-things \
  --now 2026-10-13T12:00:00+00:00
```

It reports counts and logical bytes for protected, young, skipped and eligible
entries, and lists every eligible job ID. `--now` is a test/audit override; omit
it for the real clock. The citation root defaults to
`/home/pi/dev/obd-things`. The scan uses Git's tracked files plus
untracked-but-not-ignored files, excluding the checkout's top-level `tmp/`.
Job IDs in each listed relative filename and in file contents count, whether
bare or embedded in a path. A cited job protects every job ID transitively
referenced by its manifest. References in every readable `queued/` and
`running/` manifest seed the same closure, so active work keeps completed input
jobs. Pruning refuses an unreadable or invalid active manifest rather than
assuming it has no dependencies. It accepts the queue's recoverable claim
transition: a `queued/` directory may already have persisted `state: running`
before its atomic move into `running/`.

Successful jobs remain for 60 days and failed jobs for 31 days, based only on
`manifest.json`'s timezone-aware `finished_at`. Missing, unreadable or invalid
metadata is skipped rather than guessed. Only direct, same-filesystem, real
job directories under `done/` and `failed/` can become eligible. Active copies,
unsafe names or file types, symlinked jobs and trees at or above mount points
are never removed.

After reviewing the dry-run, apply the same plan explicitly:

```bash
/home/pi/van_compute/scripts/van_compute.py prune --apply
```

Apply first acquires the installer's canonical public-CLI submission gate and
waits for already-running submitters to drain. It then holds `.queue.lock`, takes
one citation snapshot before inventory and a second immediately before deletion,
and protects the union plus both snapshots' transitive closure. The gate remains
installed for the entire apply phase and is restored afterward, so a submission
cannot finish fingerprinting inputs after those inputs were pruned. Restoration
also covers an acquisition that fails or is interrupted after publishing the gate;
it touches only artifacts recorded under that run's unique owner. A citation
added during a prune may reach the second scan, but only a citation written
before the prune begins is guaranteed to protect a job. Each eligible directory's
device and inode are bound to its decision and compared again on the no-follow
opened directory before any entry is removed. Manifests are parsed from the same
verified nonblocking, no-follow descriptor used to read them; retention never
reopens a validated manifest path. Apply also refuses active maintenance and
canonical upgrade-owner/gate fences, and fails without deleting if the Git
checkout, citation scan, submission drain or manifest validation cannot be
completed. `pi_compute.py` does not expose pruning; retention is an operator
action, not an agent-facing task.

The lexical scanner does not decode UTF-16/UTF-32 text or JSON `\\u` escapes.
A same-device bind mount introduced during deletion remains a residual race;
ordinary and pre-existing bind mounts are rejected by the mount snapshots and
no-cross-device walk. Result streaming's pre-existing path re-open behavior is
separate from retention and unchanged.

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
   paths, its installed sandbox-exec profile and JOB_ROOT write scope, and
   sandboxed Mac jobs alone reach the package registries through the worker's
   proxy. Legacy
   unsandboxed jobs and the explicit dynamic escape hatch remain unchanged.
   Process-group cleanup is not a container boundary: descendants that create
   another session can escape that cleanup, but still inherit the sandbox.
5. **Environment/cwd:** Pi keeps Linux PATH, C.UTF-8, outer workspace cwd,
   inner source cwd, placement variables and pytest cache suppression. Mac
   keeps Homebrew PATH, en_US.UTF-8, source cwd and delayed
   VAN_COMPUTE_CHILD_PYTHONPATH, plus HTTPS_PROXY/https_proxy and
   PIP_USE_DEPRECATED=legacy-certs while the registry proxy runs. The standalone limit helper remains
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
   every second at reserve only, without a final sample. A Mac `ps` timeout
   is an unknown reading, not a breach: callers reuse the last good process
   table (an unseen group, or a first sample, reads as empty), and after each
   timeout every slot shares that outcome without probing until a backoff of
   1, 2, 4 ... seconds, capped at 30, expires. Jobs fail with exit 125 only
   once 180 seconds pass from the first timed-out probe without a fresh table
   (idle time before the outage does not count). A probe that starts just
   before that boundary holds the lock for its 5-second timeout, so RSS and
   process limits can go unenforced for about 185 seconds plus scheduling
   delay while `ps` is stalled, and a group first seen during the outage is
   unmonitored throughout: Darwin address-space limits are best-effort and no
   process-count rlimit backs the watchdog, so a runaway job can strain the
   Mac in that window. That availability tradeoff is deliberate. A nonzero `ps`
   exit or an OSError still fails the job at once. A 15-second tolerance still
   killed healthy suites on a busy Mac (Time Machine plus several agent
   sessions). Exit 125 with `resource_monitor_error` set in `execution.json`
   marks a monitoring failure rather than a job failure; callers may retry it.
   Thresholds, sampling
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
heartbeats and transfers. Cooldowns are capped at 30 seconds, below the 45-second
remote-freshness window; the base is clamped to 1–30 seconds independently of a
larger poll interval. Defaults are 15, 30, 30… without jitter. Concurrent failures
from one generation count once. One nonmutating `available` recovery probe runs
after a cooldown with a 10-second total deadline (including ControlMaster setup,
lock acquisition and cleanup). The gate opens before the actual operation, so a
long upload or stream cannot monopolize recovery and starve job/capacity
heartbeats. Control calls retain a 90-second deadline; bulk transfers have a
separate one-hour deadline. Both include connection setup and bounded cleanup.
Successful communication resets the gate, even when the remote application
rejects an operation or returns malformed JSON.
SSH status 255, transport timeout and connection-establishment failures back
off; empty/maintenance claims and task/application failures do not.

Startup establishes connections lazily, so an offline Pi no longer causes a
service restart loop. The startup JSON describes the local scheduler, not
verified broker reachability. `--run-once` stays fail-fast. Only outage waits
introduce cancellation points: hard stop wakes promptly, drain cancels waiting
claims/admission but lets active jobs heartbeat/upload. Healthy-call stop/drain
semantics remain unchanged. The gate never replays a failed upload or finish;
existing exact-slot recovery remains authoritative.
