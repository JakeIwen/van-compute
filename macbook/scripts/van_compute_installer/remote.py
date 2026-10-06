from __future__ import annotations

import json
import os
from pathlib import Path
import plistlib
import shutil
import tempfile

from .constants import (
    MANIFEST_FILE, OLD_COMPUTE_ROOT, PROVENANCE_FILE, QUEUE_ROOT, RELEASE_LINK_GUARD,
    REMOTE_CONFIGS, REMOTE_RELEASES, REMOTE_ROOT, REMOTE_SCRIPTS, REMOTE_VENV,
    SOURCE_HASH_FILE,
)
from .models import DeploymentError, SourceRelease


CURRENT_DEPLOYMENT_SCRIPT_BODY = r'''
set -eu
root="$1"
source_hash="$2"
worker="$3"
queue="$4"
old="$5"
test -f "$root/deployment.sha256"
test ! -L "$root/deployment.sha256"
test "$(/bin/cat "$root/deployment.sha256")" = "$source_hash"
test -L "$root/current"
current="$(/usr/bin/readlink -e "$root/current")"
check_release_target "$root" "$current" || { echo "Invalid compute release target" >&2; exit 1; }
test -f "$current/source.sha256"
test "$(/bin/cat "$current/source.sha256")" = "$source_hash"
for script_root in "$root/scripts" "$old"; do
  owner_record="$script_root/.van-compute-upgrade-owner"
  test ! -e "$owner_record" && test ! -L "$owner_record" || exit 1
done
PYTHONPATH="$current" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue --root "$queue" maintenance status | /usr/bin/python3 -c '
import json, sys
payload = json.load(sys.stdin)
raise SystemExit(payload.get("active") is not False)
'
/usr/bin/systemctl is-active --quiet van-compute-broker.service
/usr/bin/systemctl cat van-compute-broker.service | /bin/grep -Fq "$root/current"
"$root/scripts/van_compute.py" available | /usr/bin/python3 -c '
import json, sys
payload = json.load(sys.stdin)
worker = next((item for item in payload.get("workers", []) if item.get("worker") == sys.argv[1]), None)
raise SystemExit(
    worker is None
    or worker.get("age_seconds", 999) > 45
    or worker.get("slots_total") != 10
    or not isinstance(worker.get("slots_busy"), int)
    or not 0 <= worker["slots_busy"] <= 10
)
' "$worker"
'''

REMOTE_PREFLIGHT_SCRIPT_BODY = r'''
set -eu
owner="$1"
old="$2"
new="$3"
root="$4"

for executable in /usr/bin/python3 /usr/bin/bwrap /usr/bin/sqlite3 /usr/bin/flock /usr/bin/mountpoint; do
  test -x "$executable" || { echo "Missing required executable: $executable" >&2; exit 1; }
done
sudo -n true
test -d /home/pi/dev/obd-things
test ! -L /home/pi/dev/obd-things
for directory in "$root" "$root/scripts" "$root/configs" "$root/releases" "$root/venv" "$old"; do
  if test -e "$directory" || test -L "$directory"; then
    test -d "$directory" && test ! -L "$directory" || {
      echo "A compute deployment directory is unsafe: $directory" >&2
      exit 1
    }
  fi
done
for link in "$root/current" "$root/previous"; do
  if test -e "$link" || test -L "$link"; then
    test -L "$link" || { echo "A compute release link is unsafe: $link" >&2; exit 1; }
    resolved="$(/usr/bin/readlink -e "$link")"
    check_release_target "$root" "$resolved" || { echo "Invalid compute release target" >&2; exit 1; }
    test -d "$resolved" && test ! -L "$resolved" || exit 1
  fi
done
runtime_lock="$root/runtime.lock"
if test -e "$runtime_lock" || test -L "$runtime_lock"; then
  test -f "$runtime_lock" && test ! -L "$runtime_lock" || {
    echo "The compute runtime lock is unsafe: $runtime_lock" >&2
    exit 1
  }
fi
cli_state() {
  cli="$1/van_compute.py"
  if ! test -e "$cli" && ! test -L "$cli"; then echo missing; return; fi
  test -f "$cli" && test ! -L "$cli" && test -x "$cli" || {
    echo "A compute CLI is unsafe or not executable: $cli" >&2; exit 1;
  }
  if /bin/grep -Fxq 'UPGRADE_GATE = True' "$cli"; then echo gate; else echo normal; fi
}
validate_artifacts() {
  directory="$1"
  for artifact in .van-compute-upgrade-owner .van_compute.py.pre-upgrade .van-compute-upgrade.lock; do
    path="$directory/$artifact"
    if test -e "$path" || test -L "$path"; then
      test -f "$path" && test ! -L "$path" || { echo "Unsafe upgrade artifact: $path" >&2; exit 1; }
    fi
  done
  owner_record="$directory/.van-compute-upgrade-owner"
  backup="$directory/.van_compute.py.pre-upgrade"
  if test -f "$owner_record"; then
    test "$(/bin/cat "$owner_record")" = "$owner" || {
      echo "A compute upgrade is owned by another installer: $directory" >&2; exit 1;
    }
  elif test -e "$backup" || test -L "$backup"; then
    echo "A compute rollback CLI has no owner: $directory" >&2; exit 1
  fi
}
validate_artifacts "$old"
validate_artifacts "$new"
old_state="$(cli_state "$old")"
new_state="$(cli_state "$new")"
if test "$old_state" = gate; then
  test -f "$old/.van-compute-upgrade-owner" && test -f "$old/.van_compute.py.pre-upgrade" || exit 1
  test "$new_state" != gate || { echo "Both compute layouts are gated" >&2; exit 1; }
  selected="$old"
elif test "$new_state" = gate; then
  test -f "$new/.van-compute-upgrade-owner" && test -f "$new/.van_compute.py.pre-upgrade" || exit 1
  test "$old_state" = missing || { echo "The new CLI is gated while the old CLI is live" >&2; exit 1; }
  selected="$new"
elif test "$old_state" = normal && test "$new_state" = missing; then selected="$old"
elif test "$old_state" = missing && test "$new_state" = normal; then selected="$new"
elif test "$old_state" = normal && test "$new_state" = normal; then
  echo "Both old and new compute CLIs are live" >&2; exit 1
else
  echo "No supported compute CLI deployment was found" >&2; exit 1
fi
for legacy in /home/pi/scripts/van_compute.py /home/pi/scripts/.van-compute-upgrade-owner /home/pi/scripts/.van_compute.py.pre-upgrade; do
  test ! -e "$legacy" && test ! -L "$legacy" || { echo "Unsupported flat compute artifact: $legacy" >&2; exit 1; }
done
for state in queued running; do
  directory="$5/$state"
  test -d "$directory" && test ! -L "$directory" || exit 1
  test -z "$(/usr/bin/find "$directory" -mindepth 1 -maxdepth 1 -print -quit)" || {
    echo "The Pi compute queue has pending or running work" >&2; exit 1;
  }
done
/usr/bin/python3 -m venv --help >/dev/null
printf '%s\n' "$selected"
'''

VALIDATE_REMOTE_STAGE_SCRIPT = r'''
set -eu
stage="$1"
expected="$2"
root="$3"
version="$4"
release="$stage/release"
test -d "$release" && test ! -L "$release" || exit 1
test -d "$release/van_compute" && test ! -L "$release/van_compute" || exit 1
test -z "$(/usr/bin/find "$release" -type l -print -quit)" || {
  echo "The staged Pi release contains a symlink" >&2; exit 1;
}
test "$(/bin/cat "$release/source.sha256")" = "$expected"
/usr/bin/python3 - "$release" <<'PY'
import hashlib, json, pathlib, stat, sys
root = pathlib.Path(sys.argv[1])
manifest = root / "manifest.json"
payload = json.loads(manifest.read_text())
records = payload["files"]
actual = set()
for path in root.rglob("*"):
    mode = path.lstat().st_mode
    if stat.S_ISLNK(mode):
        raise SystemExit(f"staged release contains a symlink: {path}")
    if stat.S_ISDIR(mode):
        continue
    if not stat.S_ISREG(mode):
        raise SystemExit(f"staged release contains a special entry: {path}")
    if path != manifest:
        actual.add(path.relative_to(root).as_posix())
if actual != set(records):
    raise SystemExit("staged release manifest file set mismatch")
for relative, record in records.items():
    path = root / relative
    if path.is_symlink() or not path.is_file():
        raise SystemExit(f"unsafe staged release entry: {relative}")
    if hashlib.sha256(path.read_bytes()).hexdigest() != record["sha256"]:
        raise SystemExit(f"staged release hash mismatch: {relative}")
    if stat.S_IMODE(path.stat().st_mode) != record["mode"]:
        raise SystemExit(f"staged release mode mismatch: {relative}")
PY
/usr/bin/python3 -m compileall -q -f "$release/van_compute"
/bin/rm -rf "$release/van_compute/__pycache__" "$release/van_compute/entrypoints/__pycache__"
/bin/cp "$release/van_compute/configs/van-compute-obd.example.json" "$release/.van-compute.json"
PYTHONPATH="$release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue tasks --source-root "$release" >/dev/null
/bin/rm -f "$release/.van-compute.json"
PYTHONPATH="$release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.frontend --help >/dev/null
PYTHONPATH="$release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.broker --help >/dev/null
sudo -n /usr/bin/systemd-analyze verify "$release/van_compute/configs/van-compute-broker.service" >/dev/null

existing="$root/releases/$version"
reuse_marker="$stage/reuse-existing-release"
test ! -e "$reuse_marker" && test ! -L "$reuse_marker" || exit 1
if test -e "$existing" || test -L "$existing"; then
  test -d "$existing" && test ! -L "$existing" || { echo "Existing release is unsafe: $existing" >&2; exit 1; }
  test -f "$existing/source.sha256" && test ! -L "$existing/source.sha256" || exit 1
  test "$(/bin/cat "$existing/source.sha256")" = "$expected" || { echo "Existing release provenance mismatch" >&2; exit 1; }
  /usr/bin/python3 - "$existing" "$release" <<'PY_EXISTING'
import hashlib, json, pathlib, stat, sys
root = pathlib.Path(sys.argv[1])
staged = pathlib.Path(sys.argv[2])
manifest = root / "manifest.json"
records = json.loads(manifest.read_text())["files"]
wanted = json.loads((staged / "manifest.json").read_text())["files"]

def ignored_cache(path):
    relative = path.relative_to(root)
    return "__pycache__" in relative.parts or path.suffix in {".pyc", ".pyo"}

actual = set()
for path in root.rglob("*"):
    mode = path.lstat().st_mode
    if stat.S_ISLNK(mode):
        raise SystemExit(f"existing release contains a symlink: {path}")
    if stat.S_ISDIR(mode):
        continue
    if not stat.S_ISREG(mode):
        raise SystemExit(f"existing release contains a special entry: {path}")
    if path != manifest and not ignored_cache(path):
        actual.add(path.relative_to(root).as_posix())
if actual != set(records):
    raise SystemExit("existing release manifest file set mismatch")
for relative, record in records.items():
    path = root / relative
    if (
        path.is_symlink()
        or not path.is_file()
        or hashlib.sha256(path.read_bytes()).hexdigest() != record["sha256"]
        or stat.S_IMODE(path.stat().st_mode) != record["mode"]
    ):
        raise SystemExit(f"existing release verification failed: {relative}")
if set(records) != set(wanted):
    raise SystemExit("existing release does not match the planned source")
for relative, record in records.items():
    if relative != "provenance.json" and record != wanted[relative]:
        raise SystemExit("existing release does not match the planned source")
provenance = json.loads((root / "provenance.json").read_text())
if (
    set(provenance) != {"schema_version", "kind", "source_sha256", "source_root"}
    or provenance["schema_version"] != 1
    or provenance["kind"] != "pi-broker"
    or provenance["source_sha256"] != (root / "source.sha256").read_text().strip()
    or not isinstance(provenance["source_root"], str)
):
    raise SystemExit("existing release provenance is invalid")
PY_EXISTING
  : > "$reuse_marker"
  /bin/chmod 600 "$reuse_marker"
fi
'''


class RemoteReleaseMixin:

    def deployment_current(self, source: SourceRelease) -> bool:
        try:
            self._regular_file(self.paths.target_plist, "installed LaunchAgent")
            plist = plistlib.loads(self.paths.target_plist.read_bytes())
            arguments = plist.get("ProgramArguments", [])
            environment = plist.get("EnvironmentVariables", {})
            if not isinstance(arguments, list) or not isinstance(environment, dict):
                return False
            module_index = arguments.index("van_compute.worker")
            if module_index < 2 or arguments[module_index - 1] != "-m":
                return False
            python = Path(str(arguments[0]))
            installed_release = python.parents[2]
            if (
                installed_release.parent != self.paths.release_parent
                or python != installed_release / "venv/bin/python"
                or installed_release.is_symlink()
                or not installed_release.is_dir()
                or environment.get("PYTHONPATH") != str(installed_release / "app")
            ):
                return False
            self._verify_release(installed_release, source)
            if not self._runtime_healthy(installed_release):
                return False
            if self._launch_state() is None:
                return False
            script = RELEASE_LINK_GUARD + CURRENT_DEPLOYMENT_SCRIPT_BODY
            self.remote.run(
                "current-deployment",
                script,
                [
                    REMOTE_ROOT,
                    source.source_fingerprint,
                    self.worker,
                    QUEUE_ROOT,
                    OLD_COMPUTE_ROOT,
                ],
            )
            return True
        except (
            DeploymentError,
            IndexError,
            OSError,
            ValueError,
            plistlib.InvalidFileException,
        ):
            return False

    def remote_preflight(self) -> str:
        self.say("Checking SSH access and Pi prerequisites...")
        script = RELEASE_LINK_GUARD + REMOTE_PREFLIGHT_SCRIPT_BODY
        selected = self.remote.run(
            "preflight",
            script,
            [self.owner, OLD_COMPUTE_ROOT, REMOTE_SCRIPTS, REMOTE_ROOT, QUEUE_ROOT],
            capture_output=True,
        ).strip()
        if selected not in {OLD_COMPUTE_ROOT, REMOTE_SCRIPTS}:
            raise DeploymentError(
                f"the Pi returned an invalid compute deployment root: {selected}"
            )
        self.state.upgrade_public_root = selected
        return selected

    def stage_remote_release(self, source: SourceRelease, release: Path) -> None:
        self.say("Staging the isolated Pi compute deployment...")
        self.remote.run(
            "create-stage",
            'set -eu\nstage="$1"\ncase "$stage" in /home/pi/.cache/van-compute-install.*) ;; *) exit 2;; esac\n'
            'test ! -e "$stage" && test ! -L "$stage" || exit 1\ninstall -d -m 700 "$stage" "$stage/release"\n',
            [self.remote_stage],
        )
        self.state.remote_stage_created = True
        pi_staging = Path(
            tempfile.mkdtemp(prefix=".pi-release.", dir=self.paths.support_root)
        )
        os.chmod(pi_staging, 0o700)
        try:
            package_target = pi_staging / "van_compute"
            shutil.copytree(
                release / "app" / "van_compute", package_target, symlinks=False,
                ignore=shutil.ignore_patterns(".DS_Store"),
            )
            (pi_staging / SOURCE_HASH_FILE).write_text(
                source.source_fingerprint + "\n", encoding="utf-8"
            )
            (pi_staging / PROVENANCE_FILE).write_text(
                json.dumps(
                    self.provenance(source, "pi-broker"), indent=2, sort_keys=True
                )
                + "\n",
                encoding="utf-8",
            )
            self._write_manifest(pi_staging)
            self.remote.upload(
                "upload-release",
                [
                    package_target,
                    pi_staging / SOURCE_HASH_FILE,
                    pi_staging / PROVENANCE_FILE,
                    pi_staging / MANIFEST_FILE,
                ],
                f"{self.remote_stage}/release/",
            )
        finally:
            shutil.rmtree(pi_staging)

    def validate_remote_stage(self, source: SourceRelease) -> None:
        self.say(
            "Validating the staged Pi broker before stopping the current worker..."
        )
        script = VALIDATE_REMOTE_STAGE_SCRIPT
        self.remote.run(
            "validate-stage",
            script,
            [
                self.remote_stage,
                source.source_fingerprint,
                REMOTE_ROOT,
                source.pi_version,
            ],
        )

    def provision_remote_runtime(self) -> None:
        self.say("Checking and provisioning the Pi fallback runtime...")
        script = r'''
set -eu
root="$1"
venv="$2"
install -d -m 700 "$root"
if test -e "$venv" || test -L "$venv"; then
  test -d "$venv" && test ! -L "$venv" || { echo "The Pi fallback venv is not a real directory" >&2; exit 1; }
fi
runtime_lock="$root/runtime.lock"
if test -e "$runtime_lock" || test -L "$runtime_lock"; then
  test -f "$runtime_lock" && test ! -L "$runtime_lock" || { echo "The Pi fallback runtime lock is unsafe" >&2; exit 1; }
fi
umask 077
exec 9>"$runtime_lock"
chmod 600 "$runtime_lock"
/usr/bin/flock -n 9 || { echo "Another installer is provisioning the Pi fallback runtime" >&2; exit 1; }
runtime="$venv/bin/python3"
if test -x "$runtime" && "$runtime" -c 'import isotp,numpy,pytest' >/dev/null 2>&1; then exit 0; fi
if /usr/bin/systemctl is-active --quiet van-compute-broker.service; then
  unit="$(/usr/bin/systemctl cat van-compute-broker.service)"
  if /usr/bin/printf '%s\n' "$unit" | /bin/grep -Fq "$venv/bin/python3"; then
    echo "The active Pi broker has an invalid fallback runtime" >&2; exit 1
  fi
fi
/usr/bin/python3 -m venv --system-site-packages --clear "$venv"
"$runtime" -m pip install --disable-pip-version-check --no-input can-isotp numpy pytest
"$runtime" -c 'import isotp,numpy,pytest'
'''
        self.remote.run("provision-runtime", script, [REMOTE_ROOT, REMOTE_VENV])
