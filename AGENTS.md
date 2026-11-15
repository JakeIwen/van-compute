# Repository guidance

## Repository map

- `van_compute/`: protocol, queue, broker, worker, metrics, typed runtime configuration, and shared child/result engine; `retention.py` owns completed-job inspection and guarded pruning, `retention_policy.py` selects age/budget candidates, and `retention_citations.py` owns Git working-tree citation discovery. `task_cache.py` owns the opt-in persistent per-task cache store (locking, eviction, `list`/`clear` CLI) shared by the broker and the Mac worker. `registry_proxy.py` owns the Mac worker's loopback CONNECT proxy and its package-registry allowlist (`REGISTRY_HOSTS`), the only network a sandboxed Mac job has. `registry_relay.py` owns its deadline-bounded, nonblocking tunnel relay. `registry_resolver_pool.py` owns its fixed, bounded name-lookup thread pool. `pi_health.py` owns the Pi fallback's health snapshot, readers, assessment and the gate's deferral result. `pi_marked_recovery.py` owns recovering a job whose cleanup was left unconfirmed (hold the old group, discard its cache, fail it) and `pi_once.py` the `--once` command's wait-then-decide flow. `pi_rates.py` owns its swap-paging and CPU-busy rate sampler, `can_activity.py` the CAN transmit-lock probe (`/proc/locks` against the obd-things lock files) and quiet-time tracker, and `pi_gate_flags.py` the gate's command-line flags, validation and the removed-flag errors, `pi_can_stop.py` the latched stop cause that ends a running Pi job when a transmitter appears and requeues it, and `pi_health_monitor.py` the per-broker bundle that feeds them (built once by `main`, passed to `run_once`). `job_requeue.py` owns returning a running job to `queued/` with its attempt recorded (stale remote lease recovery and the Pi CAN stop share it) and the exact-lease ownership check; `pi_cleanup_hold.py` holds a Pi job's cache scope and work directory until its process group is confirmed gone (never blocking, best-effort logging, also holding a previous run's group) and gives `--once` its stop-signal flag, and `job_cleanup_marker.py` records the group and boot id in the running manifest; `task_cache.py` also has a per-entry `discard`.
- `macbook/scripts/install_van_compute_worker.zsh` and `.py`: stable installer entry points.
- `macbook/scripts/van_compute_installer/`: coupled Mac/Pi installer implementation;
  `models.py` owns deployment records and `runners.py` owns local SSH/SCP adapters
  and the background helper-process seam; `sandbox_cache.py` and `sandbox_proxy.py`
  hold the sandbox cache and registry-proxy probes.
- `macbook/launchagents/`: worker LaunchAgent template.
- `pi/tests/compute/`: self-contained compute and deployment regression suites; `van_compute_test_fixtures.py` and deployment support modules hold shared fixtures.
- `pi/docs/compute/VAN_COMPUTE.md`: architecture, deployment, rollback, and host-compatibility decisions.
- `run_tests.py` and `requirements-test.txt`: complete fail-fast suite entry point and pinned test dependency; the optional wheelhouse path installs only into a temporary target.
- `.van-compute.json`: repository compute tasks; `compute-tests` supplies an offline wheelhouse to the complete suite runner.

## Compute execution contracts

The root `van_compute/` package owns the protocol, queue, broker, Mac worker,
metrics, typed runtime config and shared child/result engine. Host adapters keep
Pi bwrap and Mac sandbox-exec boundaries distinct; staging, ownership, admission,
telemetry and publication are intentionally host-specific. Preserve all 260
compute golden files (258 protocol fixtures plus two child-environment fixtures)
and the host drift decisions in `pi/docs/compute/VAN_COMPUTE.md`. First cutover
must install compute before updating the dashboard package: its unit/imports
require `/home/pi/van_compute/current/van_compute/metrics.py`. That cutover is
complete; the dashboard now deploys from `~/dev/van-dashboard`, whose
`scripts/deploy.py` runs `scripts/check_compute_provider.py` first. Broad scripts
sync no longer preflights the provider: it uploads scripts, then the compute
installer (`--if-needed`) applies its own drain/fence/heartbeat gates and may
install a missing or unhealthy provider. Reused Pi releases are
verified before drain, ignoring only runtime bytecode. Mac repair/`--rebuild`
creates a new immutable `<deployment24>-<build_uuid32>` generation, never mutates
retained releases, and skips pruning; legacy unsuffixed releases remain valid.

The Mac `.py`/`.zsh` installer paths stay stable; implementation lives in
`macbook/scripts/van_compute_installer/`. Its allowlisted modules travel with
frozen installers, including recovery/rollback copies. `van_compute.engine`
reexports shared child supervision from `engine_process.py`; host adapters still
own staging and publication. Shared compute test fixtures live beside the suites
in `pi/tests/compute/van_compute_test_fixtures.py` and deployment support modules.
Failed Mac builds are disposable only when this invocation owns the generation
and no installed/prior LaunchAgent reference protects it. Once Pi cutover starts,
only the remote cutover script may remove its stage: local SSH interruption does
not establish that the remote script stopped. The sibling `van-dashboard` repo owns dashboard deployment; its `scripts/deploy.py`
runs `scripts/check_compute_provider.py` before deployment. Broad scripts sync
still runs the compute installer.

## Contracts

| Path | Caller | Owner |
| --- | --- | --- |
| `python3 -m van_compute.broker` | `van_compute/configs/van-compute-broker.service` | `van_compute/broker.py` |
| `macbook/scripts/install_van_compute_worker.zsh` | Owner or agent shell, and `scripts/pi/sync_scripts.sh` | `macbook/scripts/install_van_compute_worker.py`, `macbook/scripts/van_compute_installer/` |
| `macbook/scripts/install_van_compute_worker.py` | Stable wrapper copied into frozen releases | `macbook/scripts/van_compute_installer/cli.py` |
| `macbook/launchagents/com.jacobr.van-compute-worker.plist` | Coupled installer and installed LaunchAgent | `van_compute/worker.py` |

The worker's host-wide retry gate applies only to transport outages, across all
four control connections and all ten slots. Cooldowns cap at 30 seconds and the
exclusive nonmutating recovery probe at 10 seconds, including connection setup;
bulk transfers run outside that exclusive probe so heartbeats can progress.
It never blindly replays an upload or finish; exact-slot lease recovery remains
authoritative. Drain cancels new
admission/waiting claims, not active jobs. Keep `limited_child.py` standalone and
free of package imports or untrusted startup PYTHONPATH.

The Pi broker descendant-cleanup integration test runs on Linux: macOS agent
sandboxes can refuse orphan process-group inspection. The complete suite reports
that platform skip on macOS; run it under Pi Python 3.11 for the broker boundary.
The installer shell-entry test runs only on macOS because the supported wrapper
requires `/bin/zsh`. Child-environment fixtures seed the Mac compatibility
variable they pin, so their bytes remain deterministic on Linux and macOS.
