# Repository guidance

## Repository map

- `van_compute/`: protocol, queue, broker, worker, metrics, typed runtime configuration, and shared child/result engine; `retention.py` owns completed-job pruning and `retention_citations.py` owns Git working-tree citation discovery.
- `macbook/scripts/install_van_compute_worker.zsh` and `.py`: stable installer entry points.
- `macbook/scripts/van_compute_installer/`: coupled Mac/Pi installer implementation.
- `macbook/launchagents/`: worker LaunchAgent template.
- `pi/tests/compute/`: self-contained compute and deployment regression suites; `van_compute_test_fixtures.py` and deployment support modules hold shared fixtures.
- `pi/docs/compute/VAN_COMPUTE.md`: architecture, deployment, rollback, and host-compatibility decisions.

## Compute execution contracts

The root `van_compute/` package owns the protocol, queue, broker, Mac worker,
metrics, typed runtime config and shared child/result engine. Host adapters keep
Pi bwrap and Mac sandbox-exec boundaries distinct; staging, ownership, admission,
telemetry and publication are intentionally host-specific. Preserve all 260
compute golden files (258 protocol fixtures plus two child-environment fixtures)
and the host drift decisions in `pi/docs/compute/VAN_COMPUTE.md`. First cutover
must install compute before updating the dashboard package: its unit/imports
require `/home/pi/van_compute/current/van_compute/metrics.py`. Broad sync's
package-before-compute order is not a first-cutover workflow; its earliest
preflight now checks the installed canonical metrics provider and refuses
missing/unsafe providers or maintenance/upgrade-owner fences before any writes.
Keep the compute installer in the owner's Terminal. Reused Pi releases are
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
not establish that the remote script stopped. Dashboard-selected package updates
in the sibling `scripts` checkout run the existing compute-provider preflight
before transferring a package.

The worker's host-wide retry gate applies only to transport outages, across all
four control connections and all ten slots. Cooldowns cap at 30 seconds and the
exclusive nonmutating recovery probe at 10 seconds, including connection setup;
bulk transfers run outside that exclusive probe so heartbeats can progress.
It never blindly replays an upload or finish; exact-slot lease recovery remains
authoritative. Drain cancels new
admission/waiting claims, not active jobs. Keep `limited_child.py` standalone and
free of package imports or untrusted startup PYTHONPATH.
