from __future__ import annotations

import json
import plistlib
import re

from .constants import OLD_COMPUTE_ROOT, QUEUE_ROOT, RELEASE_LINK_GUARD, REMOTE_RELEASES, REMOTE_ROOT, REMOTE_SCRIPTS
from .models import DeploymentError, SourceRelease


class RemotePhasesMixin:

    def maintenance_relation(self) -> str:
        script = r'''
set -eu
stage="$1"
queue="$2"
owner="$3"
payload="$(PYTHONPATH="$stage/release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue --root "$queue" maintenance status)"
printf '%s' "$payload" | /usr/bin/python3 -c '
import json,sys
payload=json.load(sys.stdin); owner=sys.argv[1]
print("inactive" if not payload.get("active") else "ours" if payload.get("owner")==owner else "other")
' "$owner"
'''
        relation = self.remote.run(
            "maintenance-status",
            script,
            [self.remote_stage, QUEUE_ROOT, self.owner],
            capture_output=True,
        ).strip()
        if relation not in {"inactive", "ours", "other"}:
            raise DeploymentError(f"invalid maintenance status from Pi: {relation}")
        return relation

    def active_queue_jobs(self) -> int:
        script = r'''
set -eu
root="$1"
/usr/bin/python3 - "$root" <<'PY'
from pathlib import Path
import sys
roots=[Path(sys.argv[1])/"queued", Path(sys.argv[1])/"running"]
if not all(path.is_dir() and not path.is_symlink() for path in roots):
    raise SystemExit("queue state directory is missing or unsafe")
print(sum(1 for path in roots for _entry in path.iterdir()))
PY
'''
        raw = self.remote.run(
            "active-queue-jobs", script, [QUEUE_ROOT], capture_output=True
        ).strip()
        if not raw.isdigit():
            raise DeploymentError(f"invalid active queue count: {raw!r}")
        return int(raw)

    def active_submitters(self) -> int:
        script = 'set -eu\n/usr/bin/python3 "$1/release/van_compute/upgrade_gate.py" --active-submitter-count\n'
        raw = self.remote.run(
            "active-submitters", script, [self.remote_stage], capture_output=True
        ).strip()
        if not raw.isdigit():
            raise DeploymentError(f"invalid active submitter count: {raw!r}")
        return int(raw)

    def acquire_submission_gate(self) -> None:
        script = r'''
set -eu
stage="$1"
owner="$2"
script_root="$3"
resuming="$4"
set -- /usr/bin/python3 "$stage/release/van_compute/upgrade_gate.py" --acquire --owner "$owner" --gate "$stage/release/van_compute/upgrade_gate.py" --script-root "$script_root"
if test "$resuming" = 1; then set -- "$@" --allow-existing-backup; fi
exec "$@"
'''
        self.remote.run(
            "acquire-submission-gate",
            script,
            [
                self.remote_stage,
                self.owner,
                self.state.upgrade_public_root,
                "1" if self.state.cutover_started else "0",
            ],
        )
        self.state.submission_gate_active = True

    def wait_for_submitter_drain(self) -> None:
        deadline = self.monotonic() + self.options.submitter_timeout
        while True:
            submitters = self.active_submitters()
            jobs = self.active_queue_jobs()
            if jobs:
                if self.state.cutover_started:
                    raise DeploymentError(
                        "a submission reached the queue; the interrupted upgrade remains fenced"
                    )
                raise DeploymentError(
                    "a submission reached the queue while fencing; the previous CLI and worker will be restored"
                )
            if submitters == 0:
                return
            if self.monotonic() >= deadline:
                if self.state.cutover_started:
                    raise DeploymentError(
                        "a Pi submission did not drain before the timeout; the interrupted upgrade remains fenced"
                    )
                raise DeploymentError(
                    "a Pi submission did not drain before the timeout; the previous CLI and worker will be restored"
                )
            self.sleep(0.5)

    def enter_maintenance(self) -> None:
        script = r'''
set -eu
PYTHONPATH="$1/release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue --root "$2" maintenance enter --owner "$3" >/dev/null
'''
        self.remote.run(
            "enter-maintenance", script, [self.remote_stage, QUEUE_ROOT, self.owner]
        )
        self.state.maintenance_active = True
        if self.active_submitters() or self.active_queue_jobs():
            raise DeploymentError(
                "submission activity appeared across the maintenance boundary; upgrade is stopping safely"
            )

    def cutover_remote(self, source: SourceRelease) -> None:
        # From this point onward the previous protocol is not restored on error.
        self.state.cutover_started = True
        script = (
            RELEASE_LINK_GUARD
            + r'''
set -eu
stage="$1"
root="$2"
version="$3"
install_id="$4"
old_root="$5"
release="$root/releases/$version"
staged="$stage/release"
reuse_marker="$stage/reuse-existing-release"

if /usr/bin/systemctl is-active --quiet van-compute-broker.service; then
  sudo -n systemctl stop van-compute-broker.service
fi
test -x "$root/venv/bin/python3"
"$root/venv/bin/python3" -c 'import isotp,numpy,pytest'
install -d -m 700 "$root" "$root/releases" "$root/scripts" "$root/configs" "$root/venv"
if test -f "$reuse_marker"; then
  /bin/rm -rf -- "$staged"
else
  mv -T "$staged" "$release"
fi
install -m 600 "$release/van_compute/configs/van-compute-obd.example.json" "$root/configs/van-compute-obd.example.json"
install -m 600 "$release/van_compute/configs/van-compute-broker.service" "$root/configs/van-compute-broker.service"
install -m 600 "$release/source.sha256" "$root/deployment.sha256"
install -m 700 "$release/van_compute/entrypoints/pi_compute.py" "$root/scripts/pi_compute.py"
install -m 700 "$release/van_compute/upgrade_gate.py" "$root/scripts/upgrade_gate.py"
sudo -n install -m 644 "$release/van_compute/configs/van-compute-broker.service" /etc/systemd/system/van-compute-broker.service

current=""
if test -e "$root/current" || test -L "$root/current"; then
  test -L "$root/current" || { echo "Current release path is not a symlink" >&2; exit 1; }
  current="$(/usr/bin/readlink -e "$root/current")"
  check_release_target "$root" "$current" || { echo "Invalid compute release target" >&2; exit 1; }
fi
if test "$current" != "$release"; then
  if test -n "$current"; then
    previous_name="${current##*/}"
    ln -s "releases/$previous_name" "$root/.previous.$install_id"
    /bin/mv -Tf -- "$root/.previous.$install_id" "$root/previous"
  fi
  ln -s "releases/$version" "$root/.current.$install_id"
  /bin/mv -Tf -- "$root/.current.$install_id" "$root/current"
fi
# Publish the ordinary queue wrapper only after current names the complete release.
install -m 700 "$release/van_compute/entrypoints/van_compute.py" "$root/scripts/.van_compute.py.install.$install_id"
mv -f "$root/scripts/.van_compute.py.install.$install_id" "$root/scripts/van_compute.py"
/bin/rm -rf -- "$stage"
"$root/scripts/van_compute.py" tasks >/dev/null
"$root/scripts/pi_compute.py" tasks >/dev/null
sudo -n systemctl daemon-reload
sudo -n systemctl enable van-compute-broker.service
sudo -n systemctl restart van-compute-broker.service
sudo -n systemctl is-active --quiet van-compute-broker.service
sudo -n test -f /etc/systemd/system/van-compute-broker.service
sudo -n test ! -L /etc/systemd/system/van-compute-broker.service
sudo -n /bin/grep -Fq "$root/current" /etc/systemd/system/van-compute-broker.service
if sudo -n /bin/grep -Fq "$old_root" /etc/systemd/system/van-compute-broker.service; then
  echo "The installed broker unit still references the retired compute root" >&2; exit 1
fi
'''
        )
        self.remote.run(
            "cutover",
            script,
            [
                self.remote_stage,
                REMOTE_ROOT,
                source.pi_version,
                self.install_id,
                OLD_COMPUTE_ROOT,
            ],
        )
        self.state.remote_stage_created = False

    def coordinator_seen(self) -> str:
        script = r'''
set -eu
"$1/van_compute.py" available | /usr/bin/python3 -c '
import json,sys
payload=json.load(sys.stdin); base=sys.argv[1]
print(next((str(w.get("seen_at", "")) for w in payload.get("workers", []) if w.get("worker")==base), ""))
' "$2"
'''
        return self.remote.run(
            "coordinator-seen",
            script,
            [REMOTE_SCRIPTS, self.worker],
            capture_output=True,
        ).strip()

    def heartbeat(self) -> dict[str, object]:
        script = 'set -eu\nexec "$1/van_compute.py" available\n'
        raw = self.remote.run(
            "heartbeat", script, [REMOTE_SCRIPTS], capture_output=True
        )
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            raise DeploymentError(
                "worker heartbeat response was invalid JSON"
            ) from None
        if not isinstance(payload, dict):
            raise DeploymentError("worker heartbeat response was not an object")
        return payload

    def wait_for_heartbeat(self, previous_seen: str) -> dict[str, object]:
        self.say("Installed. Waiting for the 10-slot scheduler heartbeat...")
        deadline = self.monotonic() + self.options.heartbeat_timeout
        last: dict[str, object] = {}
        while True:
            try:
                last = self.heartbeat()
            except DeploymentError:
                last = {}
            workers = last.get("workers", [])
            coordinator = (
                next(
                    (
                        item
                        for item in workers
                        if isinstance(item, dict)
                        and item.get("worker") == self.worker
                        and item.get("available")
                    ),
                    None,
                )
                if isinstance(workers, list)
                else None
            )
            if (
                isinstance(coordinator, dict)
                and coordinator.get("seen_at") != previous_seen
                and coordinator.get("slots_total") == 10
                and isinstance(coordinator.get("slots_busy"), int)
                and 0 <= int(coordinator["slots_busy"]) <= 10
            ):
                return last
            if self.monotonic() >= deadline:
                raise DeploymentError(
                    f"worker did not publish a fresh 10-slot coordinator heartbeat within "
                    f"{self.options.heartbeat_timeout} seconds; inspect "
                    f"{self.paths.cache_root}/logs/worker.stderr.log"
                )
            self.sleep(1)

    def finalize_upgrade(self) -> None:
        retire = self.state.upgrade_public_root == OLD_COMPUTE_ROOT
        script = r'''
set -eu
release="$1"
owner="$2"
script_root="$3"
queue_cli="$4"
retire="$5"
set -- /usr/bin/python3 "$release/van_compute/upgrade_gate.py" --finalize --owner "$owner" --script-root "$script_root"
if test "$retire" = 1; then set -- "$@" --queue-cli "$queue_cli" --retire-target; fi
exec "$@"
'''
        # The staged release has moved into the immutable release before finalize.
        gate_script_root = (
            f"{REMOTE_RELEASES}/{self.source.pi_version}" if self.source else ""
        )
        self.remote.run(
            "finalize",
            script,
            [
                gate_script_root,
                self.owner,
                self.state.upgrade_public_root,
                f"{REMOTE_SCRIPTS}/van_compute.py",
                "1" if retire else "0",
            ],
        )
        self.state.maintenance_active = False
        self.state.submission_gate_active = False

    def retire_legacy_layout(self) -> None:
        script = r'''
set -eu
require_unmounted() {
  if /usr/bin/mountpoint -q "$1"; then
    echo "Refusing to remove a mounted compute path: $1" >&2
    return 1
  else
    mount_result=$?
    test "$mount_result" -eq 32 || {
      echo "Cannot establish compute mount state: $1" >&2
      return 1
    }
  fi
}
root="$1"
old="$2"
sudo -n systemctl is-active --quiet van-compute-broker.service
sudo -n systemctl cat van-compute-broker.service | /bin/grep -Fq "$root/current"
broker_pid="$(/usr/bin/systemctl show --property MainPID --value van-compute-broker.service)"
case "$broker_pid" in ''|0|*[!0-9]*) echo "The active broker PID could not be verified" >&2; exit 1;; esac
/usr/bin/tr '\0' ' ' < "/proc/$broker_pid/cmdline" | /bin/grep -Fq 'van_compute.broker' || {
  echo "The active broker process is not using the package deployment" >&2; exit 1;
}
if test -e "$old" || test -L "$old"; then
  test -d "$old" && test ! -L "$old" || { echo "The old compute deployment is unsafe" >&2; exit 1; }
  require_unmounted "$old" || exit 1
  unexpected="$(/usr/bin/find "$old" -mindepth 1 -maxdepth 1 ! -name __pycache__ ! -name python-automation ! -name pi_compute.py ! -name van_compute.py ! -name van_compute_broker.py ! -name van_compute_metrics.py ! -name van_compute_protocol.py ! -name van_compute_upgrade_gate.py ! -name van-compute-broker.service ! -name van-compute-obd.example.json ! -name .van-compute-upgrade.lock ! -name .van-compute-upgrade-owner ! -name .van_compute.py.pre-upgrade ! -name '.van_compute.py.install.*' -print -quit)"
  test -z "$unexpected" || { echo "Refusing to remove unexpected old compute entry: $unexpected" >&2; exit 1; }
  /bin/rm -rf --one-file-system -- "$old"
fi
for path in /home/pi/configs/van-compute-obd.example.json /home/pi/secrets/van-compute-datasets.json; do
  if test -e "$path" || test -L "$path"; then
    test -f "$path" && test ! -L "$path" || { echo "Retired compute file is unsafe: $path" >&2; exit 1; }
    require_unmounted "$path" || exit 1
    /bin/rm -f -- "$path"
  fi
done
old_runtime=/home/pi/.local/share/van-compute
if test -e "$old_runtime" || test -L "$old_runtime"; then
  test -d "$old_runtime" && test ! -L "$old_runtime" || exit 1
  require_unmounted "$old_runtime" || exit 1
  unexpected="$(/usr/bin/find "$old_runtime" -mindepth 1 -maxdepth 1 ! -name venv ! -name runtime.lock -print -quit)"
  test -z "$unexpected" || { echo "Refusing unexpected old runtime entry: $unexpected" >&2; exit 1; }
  /bin/rm -rf --one-file-system -- "$old_runtime"
fi
'''
        self.remote.run("retire-legacy", script, [REMOTE_ROOT, OLD_COMPUTE_ROOT])

    def refresh_dashboard(self) -> None:
        script = r'''
set -eu
if /usr/bin/systemctl is-active --quiet van-dashboard.service && /usr/bin/systemctl cat van-dashboard.service | /bin/grep -Fq "$1/current"; then
  sudo -n systemctl restart van-dashboard.service
  sudo -n systemctl is-active --quiet van-dashboard.service
fi
'''
        try:
            self.remote.run("refresh-dashboard", script, [REMOTE_ROOT])
        except DeploymentError:
            self.warn(
                "WARNING: compute is healthy, but van-dashboard could not be refreshed."
            )
            self.warn("Inspect van-dashboard.service after this installer exits.")

    def restore_submission_cli(self) -> None:
        script = r'''
set -eu
/usr/bin/python3 "$1/release/van_compute/upgrade_gate.py" --restore --owner "$2" --script-root "$3"
'''
        self.remote.run(
            "restore-submission-gate",
            script,
            [self.remote_stage, self.owner, self.state.upgrade_public_root],
        )

    def exit_maintenance(self) -> None:
        script = r'''
set -eu
PYTHONPATH="$1/release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue --root "$2" maintenance exit --owner "$3" >/dev/null
'''
        self.remote.run(
            "exit-maintenance", script, [self.remote_stage, QUEUE_ROOT, self.owner]
        )

    def remove_remote_stage(self) -> None:
        script = r'''
set -eu
stage="$1"
case "$stage" in /home/pi/.cache/van-compute-install.*) /bin/rm -rf -- "$stage";; *) exit 2;; esac
'''
        self.remote.run("remove-stage", script, [self.remote_stage])

    def cleanup(self) -> None:
        # Before protocol replacement, undo the fence in the only safe order:
        # maintenance first, then the public CLI, then the prior worker.
        if self.state.maintenance_active and not self.state.cutover_started:
            try:
                self.exit_maintenance()
                self.state.maintenance_active = False
            except DeploymentError:
                self.state.rollback_safe = False
                self.warn("The Pi maintenance marker could not be released safely.")
                self.warn(
                    "The submission gate and previous Mac worker remain disabled; rerun this installer."
                )
        if (
            self.state.submission_gate_active
            and not self.state.cutover_started
            and self.state.rollback_safe
        ):
            try:
                self.restore_submission_cli()
                self.state.submission_gate_active = False
            except DeploymentError:
                self.state.rollback_safe = False
                self.warn(
                    "The temporary Pi submission gate could not be rolled back safely."
                )
                self.warn(
                    "The previous Mac worker remains disabled; rerun this installer."
                )
        if self.state.remote_stage_created and not self.state.cutover_started:
            try:
                self.remove_remote_stage()
            except DeploymentError:
                pass
            self.state.remote_stage_created = False
        domain = self._launch_domain()
        if self.state.previous_agent_disabled and self.state.rollback_safe:
            self.local.run(["/bin/launchctl", "enable", domain], check=False)
            self.state.previous_agent_disabled = False
        if (
            self.state.restore_previous_agent
            and not self.state.cutover_started
            and self.state.rollback_safe
        ):
            self.local.run(
                [
                    "/bin/launchctl",
                    "bootstrap",
                    domain.rsplit("/", 1)[0],
                    str(self.paths.target_plist),
                ],
                check=False,
            )
            self.state.restore_previous_agent = False
        elif self.state.restore_previous_agent and self.state.cutover_started:
            self.warn(
                "The previous worker remains unloaded because the Pi protocol upgrade began."
            )
            self.warn(
                "Rerun this installer to finish installing the compatible persistent worker."
            )
        if self.state.maintenance_active and self.state.cutover_started:
            self.warn(
                "The compute queue remains in maintenance mode after an incomplete protocol upgrade."
            )
            self.warn(
                "Rerun this installer to validate the deployment and release queued work."
            )
        elif self.state.maintenance_active and not self.state.rollback_safe:
            self.warn(
                "The compute queue remains in maintenance mode because rollback was incomplete."
            )
