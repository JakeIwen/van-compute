#!/bin/zsh
set -euo pipefail
setopt NO_BG_NICE

script_dir="${0:A:h}"
repo_root="${script_dir:h:h}"
pi_host="${VAN_COMPUTE_HOST:-pi@vanpi}"
worker_name="${VAN_COMPUTE_WORKER:-m4mac}"
label="com.jacobr.van-compute-worker"
source_plist="$repo_root/macbook/launchagents/$label.plist"
target_dir="$HOME/Library/LaunchAgents"
target_plist="$target_dir/$label.plist"
cache_root="$HOME/Library/Caches/van-compute"
support_root="$HOME/Library/Application Support/van-compute"
release_parent="$support_root/releases"
dataset_target="$support_root/datasets.json"
user_id="$(id -u)"
install_id="$(/usr/bin/uuidgen | /usr/bin/tr '[:upper:]' '[:lower:]')"
remote_stage="/home/pi/.cache/van-compute-install.$install_id"
remote_compute_root="/home/pi/scripts/compute"
remote_config_root="/home/pi/configs"
upgrade_public_root="$remote_compute_root"
allow_unsandboxed="${VAN_COMPUTE_ALLOW_UNSANDBOXED:-0}"
dataset_source="${VAN_COMPUTE_DATASET_CONFIG:-}"
installer_lock="$support_root/installer.lock"
maintenance_owner_file="$support_root/installer-owner"
installer_lock_fd=""
staging=""
release=""
release_published=0
dataset_staging=""
sentinel=""
restore_previous_agent=0
remote_upgrade_started=0
remote_stage_created=0
previous_agent_disabled=0
maintenance_active=0
maintenance_owner=""
submission_gate_active=0
rollback_safe=1

cleanup_sandbox_sentinel() {
  if [[ -n "$sentinel" && -f "$sentinel" && ! -L "$sentinel" && \
        "$sentinel" == "$cache_root"/.sandbox-sentinel.* ]]; then
    /bin/rm -f -- "$sentinel"
    sentinel=""
  fi
}

restore_submission_cli() {
  /usr/bin/ssh "$pi_host" \
    "/usr/bin/python3 '$remote_stage/van_compute_upgrade_gate.py' --restore --owner '$maintenance_owner' --script-root '$upgrade_public_root'"
}

cleanup() {
  # Keep the all-submission gate in place until the maintenance marker has
  # definitely been released. Restoring the previous CLI first could reopen
  # submissions into a half-rolled-back queue.
  if (( maintenance_active && ! remote_upgrade_started )); then
    if /usr/bin/ssh "$pi_host" "/usr/bin/env PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 '$remote_stage/van_compute.py' --root /home/pi/dev/obd-things/tmp/compute maintenance exit --owner '$maintenance_owner'" >/dev/null 2>&1; then
      maintenance_active=0
    else
      rollback_safe=0
      echo "The Pi maintenance marker could not be released safely." >&2
      echo "The submission gate and previous Mac worker will remain disabled; rerun this installer." >&2
    fi
  fi
  if (( submission_gate_active && ! remote_upgrade_started && rollback_safe )); then
    if restore_submission_cli >/dev/null 2>&1; then
      submission_gate_active=0
    else
      rollback_safe=0
      echo "The temporary Pi submission gate could not be rolled back safely." >&2
      echo "The previous Mac worker will remain disabled; rerun this installer." >&2
    fi
  fi
  if (( remote_stage_created )) && [[ "$remote_stage" == /home/pi/.cache/van-compute-install.* ]]; then
    /usr/bin/ssh "$pi_host" "/bin/rm -rf -- '$remote_stage'" >/dev/null 2>&1 || true
  fi
  if [[ -n "$staging" && -d "$staging" && "$staging" == "$release_parent"/.install.* ]]; then
    /bin/rm -rf -- "$staging"
  fi
  if (( ! release_published )) && \
     [[ -n "$release" && -d "$release" && ! -L "$release" && "$release" == "$release_parent"/20* ]]; then
    /bin/rm -rf -- "$release"
  fi
  if [[ -n "$dataset_staging" && -f "$dataset_staging" && "$dataset_staging" == "$support_root"/.datasets.json.* ]]; then
    /bin/rm -f -- "$dataset_staging"
  fi
  cleanup_sandbox_sentinel
  if (( previous_agent_disabled && rollback_safe )); then
    /bin/launchctl enable "gui/$user_id/$label" >/dev/null 2>&1 || true
  fi
  if (( restore_previous_agent && ! remote_upgrade_started && rollback_safe )); then
    /bin/launchctl bootstrap "gui/$user_id" "$target_plist" >/dev/null 2>&1 || true
  elif (( restore_previous_agent && remote_upgrade_started )); then
    echo "The previous worker remains unloaded because the Pi protocol upgrade began." >&2
    echo "Rerun this installer to finish installing the compatible persistent worker." >&2
  fi
  if (( maintenance_active && remote_upgrade_started )); then
    echo "The compute queue remains in maintenance mode after an incomplete protocol upgrade." >&2
    echo "Rerun this installer to validate the deployment and release queued work." >&2
  elif (( maintenance_active && ! rollback_safe )); then
    echo "The compute queue remains in maintenance mode because rollback was incomplete." >&2
  fi
}
trap cleanup EXIT

# Applying a second Seatbelt profile from an already sandboxed parent fails
# with status 71. Detect that before Homebrew or pip can do expensive work.
if [[ "$allow_unsandboxed" != 1 ]]; then
  echo "Checking macOS sandbox capability..."
  sandbox_capability_output=""
  if sandbox_capability_output="$(
    /usr/bin/sandbox-exec -p '(version 1)(allow default)' /usr/bin/true 2>&1
  )"; then
    :
  else
    sandbox_capability_status=$?
    [[ -z "$sandbox_capability_output" ]] || print -r -- "$sandbox_capability_output" >&2
    echo "macOS could not apply a sandbox profile (status $sandbox_capability_status)." >&2
    echo "Open a fresh Terminal.app or iTerm window outside Codex/another sandbox and rerun." >&2
    echo "Do not use VAN_COMPUTE_ALLOW_UNSANDBOXED solely to bypass this environment check." >&2
    exit 1
  fi
fi

echo "Checking local installer prerequisites..."
[[ -x /opt/homebrew/bin/brew ]] || {
  echo "Homebrew is required at /opt/homebrew/bin/brew." >&2
  exit 1
}
[[ -x /opt/homebrew/bin/python3 ]] || {
  echo "Homebrew Python is required at /opt/homebrew/bin/python3." >&2
  exit 1
}
if ! /opt/homebrew/bin/python3 -c '
import re
import sys
raise SystemExit(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,59}", sys.argv[1]))
' "$worker_name"; then
  echo "VAN_COMPUTE_WORKER must be a safe name no longer than 60 characters." >&2
  exit 1
fi
/usr/bin/plutil -lint "$source_plist"

# Keep the deployment and its maintenance ownership single-writer. zsystem
# keeps the advisory lock's file descriptor open in this shell until exit.
/bin/mkdir -p "$support_root"
/bin/chmod 700 "$support_root"
/usr/bin/touch "$installer_lock"
/bin/chmod 600 "$installer_lock"
zmodload zsh/system || {
  echo "The zsh/system module is required to lock the worker installer." >&2
  exit 1
}
if ! zsystem flock -t 0 -f installer_lock_fd "$installer_lock"; then
  echo "Another van-compute worker installer is already running." >&2
  exit 1
fi

# Keep one owner identity for this Mac so an interrupted post-protocol upgrade
# can be resumed, while a different machine cannot adopt its maintenance lease.
if [[ -L "$maintenance_owner_file" || ( -e "$maintenance_owner_file" && ! -f "$maintenance_owner_file" ) ]]; then
  echo "Installer owner record is not a regular file: $maintenance_owner_file" >&2
  exit 1
fi
if [[ -f "$maintenance_owner_file" ]]; then
  maintenance_owner="$(<"$maintenance_owner_file")"
else
  maintenance_owner="installer-$install_id"
  owner_staging="$(/usr/bin/mktemp "$support_root/.installer-owner.XXXXXX")"
  print -r -- "$maintenance_owner" > "$owner_staging"
  /bin/chmod 600 "$owner_staging"
  /bin/mv -f -- "$owner_staging" "$maintenance_owner_file"
fi
/bin/chmod 600 "$maintenance_owner_file"
if ! /opt/homebrew/bin/python3 -c '
import re
import sys
raise SystemExit(not re.fullmatch(
    r"installer-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    sys.argv[1],
))
' "$maintenance_owner"; then
  echo "Installer owner record is invalid: $maintenance_owner_file" >&2
  exit 1
fi

# Fail before any large local package or virtual-environment work when this
# Pi upgrade cannot proceed. The queue is checked again immediately before
# fencing, so this is an early cost guard rather than the upgrade lock.
echo "Checking SSH access and Pi prerequisites..."
/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 "$pi_host" '
  set -eu
  test -x /usr/bin/python3
  test -x /usr/bin/bwrap || {
    echo "Missing /usr/bin/bwrap; install the Pi bubblewrap package first." >&2
    exit 1
  }
  test -x /usr/bin/sqlite3 || {
    echo "Missing /usr/bin/sqlite3; install the Pi sqlite3 package first." >&2
    exit 1
  }
  test -x /usr/bin/flock || {
    echo "Missing /usr/bin/flock; install the Pi util-linux package first." >&2
    exit 1
  }
  sudo -n true
  test -d /home/pi/dev/obd-things
  test ! -L /home/pi/dev/obd-things
  current=/home/pi/scripts/compute
  test -d "$current" && test ! -L "$current" || {
    echo "The current compute script directory is missing or unsafe: $current" >&2
    exit 1
  }
  test -f "$current/van_compute.py" && test ! -L "$current/van_compute.py" || {
    echo "The current compute CLI is missing or unsafe: $current/van_compute.py" >&2
    exit 1
  }
  for current_artifact in \
    "$current/.van-compute-upgrade-owner" \
    "$current/.van_compute.py.pre-upgrade"; do
    if test -e "$current_artifact" || test -L "$current_artifact"; then
      test -f "$current_artifact" && test ! -L "$current_artifact" || {
        echo "A current-layout upgrade artifact is unsafe: $current_artifact" >&2
        exit 1
      }
    fi
  done
  for legacy_artifact in \
    /home/pi/scripts/van_compute.py \
    /home/pi/scripts/.van-compute-upgrade-owner \
    /home/pi/scripts/.van_compute.py.pre-upgrade; do
    if test -e "$legacy_artifact" || test -L "$legacy_artifact"; then
      echo "An unsupported flat-layout compute artifact remains: $legacy_artifact" >&2
      exit 1
    fi
  done
  for state in queued running; do
    root="/home/pi/dev/obd-things/tmp/compute/$state"
    test -d "$root"
    test ! -L "$root"
    test -z "$(/usr/bin/find "$root" -mindepth 1 -maxdepth 1 -print -quit)" || {
      echo "The Pi compute queue has pending or running work; rerun after it finishes." >&2
      exit 1
    }
  done
  /usr/bin/python3 -m venv --help >/dev/null
'

echo "Checking and provisioning local worker dependencies..."
formulae=()
[[ -x /opt/homebrew/bin/rg ]] || formulae+=(ripgrep)
[[ -x /opt/homebrew/bin/jadx ]] || formulae+=(jadx)
if (( ${#formulae} )); then
  echo "Installing Homebrew worker tools: ${formulae[*]}"
  /opt/homebrew/bin/brew install "${formulae[@]}"
fi
[[ -x /usr/bin/sqlite3 ]] || {
  echo "The macOS /usr/bin/sqlite3 executable is unavailable." >&2
  exit 1
}

/bin/mkdir -p "$support_root" "$release_parent" "$cache_root/logs" "$cache_root/jobs" "$cache_root/ssh"
/bin/chmod 700 "$cache_root" "$support_root" "$release_parent" "$cache_root/logs" "$cache_root/jobs" "$cache_root/ssh"
staging="$(/usr/bin/mktemp -d "$release_parent/.install.XXXXXX")"
/bin/chmod 700 "$staging"

echo "Building an isolated Python environment..."
/opt/homebrew/bin/python3 -m venv "$staging/venv"
"$staging/venv/bin/python" -m pip install \
  --disable-pip-version-check --no-input androguard can-isotp numpy pytest

/bin/mkdir -p "$staging/app/macbook/scripts" "$staging/app/shared/python"
/usr/bin/install -m 700 \
  "$repo_root/macbook/scripts/van_compute_worker.py" \
  "$staging/app/macbook/scripts/van_compute_worker.py"
/usr/bin/install -m 600 \
  "$repo_root/shared/python/van_compute_protocol.py" \
  "$staging/app/shared/python/van_compute_protocol.py"

# sandbox-exec is deprecated but remains the only reliably scriptable,
# no-admin macOS confinement available here. The profile intentionally grants
# read access only to system runtimes, the staged worker, the private job tree,
# and configured dataset roots. Writes are confined to the job tree; network
# operations are denied by the default rule.
sandbox_profile='(version 1)
(deny default)
(allow process*)
(allow sysctl-read)
(allow ipc-posix*)
; Metadata is needed to traverse operator-configured dataset roots. Contents
; and directory reads remain constrained by file-read* below.
(allow file-read-metadata)
(allow file-read*
    (subpath "/System/Library")
    ; macOS 26 stores the dyld shared cache used by system executables here.
    (subpath "/System/Volumes/Preboot/Cryptexes/OS")
    (subpath "/usr")
    (subpath "/bin")
    (subpath "/sbin")
    (subpath "/opt/homebrew")
    (subpath "/private/etc")
    (subpath "/private/var/db/timezone")
    (subpath "/Library/Apple")
    (subpath "/Library/Java")
    (subpath (param "WORKER_ROOT"))
    (subpath (param "JOB_ROOT"))
    (subpath (param "DATASET_0"))
    (subpath (param "DATASET_1"))
    (subpath (param "DATASET_2"))
    (subpath (param "DATASET_3"))
    (subpath (param "DATASET_4"))
    (subpath (param "DATASET_5"))
    (subpath (param "DATASET_6"))
    (subpath (param "DATASET_7"))
    (subpath (param "DATASET_8"))
    (subpath (param "DATASET_9"))
    (subpath (param "DATASET_10"))
    (subpath (param "DATASET_11"))
    (subpath (param "DATASET_12"))
    (subpath (param "DATASET_13"))
    (subpath (param "DATASET_14"))
    (subpath (param "DATASET_15"))
    (literal "/dev/null")
    (literal "/dev/random")
    (literal "/dev/urandom"))
(allow file-write*
    (subpath (param "JOB_ROOT"))
    (literal "/dev/null"))
'
print -r -- "$sandbox_profile" > "$staging/sandbox.sb"
/bin/chmod 600 "$staging/sandbox.sb"

if [[ "$allow_unsandboxed" != 1 ]]; then
  echo "Validating macOS job isolation before changing the running worker..."
  sandbox_test="$staging/sandbox-test"
  sentinel="$cache_root/.sandbox-sentinel.$$"
  /bin/mkdir -m 700 "$sandbox_test"
  print -r -- "must-not-be-readable" > "$sentinel"
  /bin/chmod 600 "$sentinel"
  sandbox_arguments=(
    -f "$staging/sandbox.sb"
    -D "WORKER_ROOT=$staging"
    -D "JOB_ROOT=$sandbox_test"
  )
  for index in {0..15}; do
    sandbox_arguments+=(-D "DATASET_${index}=/dev/null")
  done
  sandbox_run() {
    (
      cd "$sandbox_test"
      /usr/bin/sandbox-exec "${sandbox_arguments[@]}" \
        /usr/bin/env -i \
          HOME="$sandbox_test" \
          TMPDIR="$sandbox_test/" \
          XDG_CACHE_HOME="$sandbox_test" \
          XDG_CONFIG_HOME="$sandbox_test/.config" \
          LANG="en_US.UTF-8" \
          LC_ALL="en_US.UTF-8" \
          PATH="/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin" \
          PYTHONNOUSERSITE=1 \
          PYTHONDONTWRITEBYTECODE=1 \
          "$@"
    )
  }
  sandbox_check() {
    local label="$1"
    shift
    local output=""
    local exit_code=0
    if output="$(sandbox_run "$@" 2>&1)"; then
      return 0
    else
      exit_code=$?
    fi
    [[ -z "$output" ]] || print -r -- "$output" >&2
    echo "Sandbox validation stage failed: $label (status $exit_code)." >&2
    return "$exit_code"
  }
  if ! sandbox_check "profile application" /usr/bin/true; then
    cleanup_sandbox_sentinel
    echo "The generated sandbox profile could not be applied; the existing LaunchAgent was not replaced." >&2
    exit 1
  fi
  if ! sandbox_check "Python imports and isolation policy" "$staging/venv/bin/python" -c '
import errno
from pathlib import Path
import socket
import sys
import androguard
import isotp
import numpy
import pytest

scratch = Path(sys.argv[1])
probe = scratch / "allowed.txt"
probe.write_text("ok", encoding="utf-8")
if probe.read_text(encoding="utf-8") != "ok":
    raise SystemExit("sandbox scratch read/write failed")
for index, raw_sentinel in enumerate(sys.argv[2:]):
    sentinel = Path(raw_sentinel)
    try:
        sentinel.read_text(encoding="utf-8")
    except OSError as exc:
        if index and exc.errno == errno.ENOENT:
            continue
        if exc.errno not in {errno.EACCES, errno.EPERM}:
            raise
    else:
        raise SystemExit(f"sandbox read a private-home sentinel via {sentinel}")
probe_socket = None
try:
    probe_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe_socket.sendto(b"x", ("127.0.0.1", 9))
except OSError as exc:
    if exc.errno not in {errno.EACCES, errno.EPERM}:
        raise
else:
    raise SystemExit("sandbox allowed network output")
finally:
    if probe_socket is not None:
        probe_socket.close()
' "$sandbox_test" "$sentinel" "/System/Volumes/Data$sentinel"; then
    cleanup_sandbox_sentinel
    echo "The Python sandbox policy check failed; the existing LaunchAgent was not replaced." >&2
    exit 1
  fi
  if ! sandbox_check "ripgrep runtime" \
      /opt/homebrew/bin/rg --fixed-strings ok "$sandbox_test/allowed.txt"; then
    cleanup_sandbox_sentinel
    echo "The existing LaunchAgent was not replaced." >&2
    exit 1
  fi
  if ! sandbox_check "SQLite runtime" \
      /usr/bin/sqlite3 -readonly -batch :memory: 'select 1;'; then
    cleanup_sandbox_sentinel
    echo "The existing LaunchAgent was not replaced." >&2
    exit 1
  fi
  if ! sandbox_check "JADX runtime" /opt/homebrew/bin/jadx --version; then
    cleanup_sandbox_sentinel
    echo "The existing LaunchAgent was not replaced." >&2
    exit 1
  fi
  cleanup_sandbox_sentinel
else
  echo "WARNING: VAN_COMPUTE_ALLOW_UNSANDBOXED=1 disables OS-level job isolation." >&2
  echo "Jobs still get private HOME/TMP/env and limits, but can address host files/network." >&2
fi

echo "Checking Mac process-group resource watchdog..."
if ! "$staging/venv/bin/python" -c '
import os
import subprocess
result = subprocess.run(
    ["/bin/ps", "-axo", "pgid=,rss="],
    check=True,
    capture_output=True,
    text=True,
)
group = os.getpgrp()
rows = [line.split() for line in result.stdout.splitlines()]
assert any(len(row) == 2 and int(row[0]) == group and int(row[1]) >= 0 for row in rows)
'; then
  echo "The Mac process-group resource watchdog cannot inspect processes." >&2
  echo "Run this installer from a normal Terminal, outside another app sandbox." >&2
  exit 1
fi

release_id="$(/bin/date -u +%Y%m%dT%H%M%SZ)-$$"
release="$release_parent/$release_id"
/bin/mv "$staging" "$release"
staging=""

validate_dataset_config() {
  "$release/venv/bin/python" -c '
import json
from pathlib import Path
import re
import sys
p = Path(sys.argv[1])
d = json.loads(p.read_text())
assert set(d) == {"datasets"} and isinstance(d["datasets"], dict)
assert len(d["datasets"]) <= 16
for name, value in d["datasets"].items():
    assert isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name)
    assert isinstance(value, str)
    path = Path(value).expanduser()
    assert path.is_absolute() and path.exists()
' "$1"
}

if [[ -n "$dataset_source" ]]; then
  [[ -f "$dataset_source" ]] || {
    echo "VAN_COMPUTE_DATASET_CONFIG is not a file: $dataset_source" >&2
    exit 1
  }
  # Validate before touching the last known-good private configuration.
  validate_dataset_config "$dataset_source"
  /bin/mkdir -p "$support_root"
  /bin/chmod 700 "$support_root"
  dataset_staging="$(/usr/bin/mktemp "$support_root/.datasets.json.XXXXXX")"
  /usr/bin/install -m 600 "$dataset_source" "$dataset_staging"
  /bin/mv -f -- "$dataset_staging" "$dataset_target"
  dataset_staging=""
fi
if [[ -f "$dataset_target" ]]; then
  validate_dataset_config "$dataset_target"
fi

echo "Deploying the Pi queue CLI and shared protocol..."
/usr/bin/ssh "$pi_host" "install -d -m 700 '$remote_stage'"
remote_stage_created=1
/usr/bin/scp \
  "$repo_root/pi/scripts/compute/van_compute.py" \
  "$repo_root/pi/scripts/compute/pi_compute.py" \
  "$repo_root/pi/scripts/compute/van_compute_broker.py" \
  "$repo_root/pi/scripts/compute/van_compute_upgrade_gate.py" \
  "$repo_root/pi/services/van-compute-broker.service" \
  "$repo_root/pi/configs/van-compute-obd.example.json" \
  "$repo_root/shared/python/van_compute_protocol.py" \
  "$repo_root/shared/python/van_compute_metrics.py" \
  "$repo_root/pi/apps/van_dashboard/templates/van_dashboard.html" \
  "$repo_root/pi/apps/van_dashboard/static/van_dashboard.js" \
  "$repo_root/pi/apps/van_dashboard/static/van_dashboard.css" \
  "$pi_host:$remote_stage/"

echo "Validating the staged Pi broker before stopping the current worker..."
/usr/bin/ssh "$pi_host" "
  set -eu
  test -x /usr/bin/bwrap || {
    echo 'Missing /usr/bin/bwrap; install the Pi bubblewrap package first.' >&2
    exit 1
  }
  sudo -n true
  /usr/bin/python3 -m py_compile \
    '$remote_stage/van_compute.py' \
    '$remote_stage/pi_compute.py' \
    '$remote_stage/van_compute_broker.py' \
    '$remote_stage/van_compute_upgrade_gate.py' \
    '$remote_stage/van_compute_protocol.py' \
    '$remote_stage/van_compute_metrics.py'
  rm -rf '$remote_stage/__pycache__'
  install -d -m 700 '$remote_stage/python-automation'
  install -m 600 '$remote_stage/van_compute_protocol.py' '$remote_stage/python-automation/van_compute_protocol.py'
  install -m 600 '$remote_stage/van-compute-obd.example.json' '$remote_stage/.van-compute.json'
  /usr/bin/python3 '$remote_stage/van_compute.py' tasks --source-root '$remote_stage' >/dev/null
  rm -f '$remote_stage/.van-compute.json'
  /usr/bin/python3 '$remote_stage/pi_compute.py' --help >/dev/null
  /usr/bin/python3 '$remote_stage/van_compute_broker.py' --help >/dev/null
  rm -rf '$remote_stage/__pycache__' '$remote_stage/python-automation/__pycache__'
  sudo -n /usr/bin/systemd-analyze verify '$remote_stage/van-compute-broker.service' >/dev/null
"

# Provision the fallback runtime before submissions are fenced. On subsequent
# runs an active broker must already have a valid runtime; never mutate its
# environment underneath it.
echo "Checking and provisioning the Pi fallback runtime..."
/usr/bin/ssh "$pi_host" "
  set -eu
  /usr/bin/install -d -m 700 /home/pi/.local/share/van-compute
  umask 077
  exec 9>/home/pi/.local/share/van-compute/runtime.lock
  /usr/bin/chmod 600 /home/pi/.local/share/van-compute/runtime.lock
  /usr/bin/flock -n 9 || {
    echo 'Another installer is provisioning the Pi fallback runtime; rerun after it finishes.' >&2
    exit 1
  }
  runtime=/home/pi/.local/share/van-compute/venv/bin/python3
  if test -x \"\$runtime\" && \"\$runtime\" -c 'import isotp, numpy, pytest' >/dev/null 2>&1; then
    exit 0
  fi
  if /usr/bin/systemctl is-active --quiet van-compute-broker.service; then
    echo 'The active Pi broker has an invalid fallback runtime; stop and repair it before upgrading.' >&2
    exit 1
  fi
  /usr/bin/python3 -m venv --system-site-packages /home/pi/.local/share/van-compute/venv
  \"\$runtime\" -m pip install \
    --disable-pip-version-check --no-input can-isotp numpy pytest
  \"\$runtime\" -c 'import isotp, numpy, pytest'
"

active_queue_jobs() {
  /usr/bin/ssh "$pi_host" "/usr/bin/python3 -c '
from pathlib import Path
roots = [
    Path(\"/home/pi/dev/obd-things/tmp/compute/queued\"),
    Path(\"/home/pi/dev/obd-things/tmp/compute/running\"),
]
if not all(root.is_dir() and not root.is_symlink() for root in roots):
    raise SystemExit(\"queue state directory is missing or unsafe\")
print(sum(1 for root in roots for _entry in root.iterdir()))
'"
}

active_submitters() {
  /usr/bin/ssh "$pi_host" \
    "/usr/bin/python3 '$remote_stage/van_compute_upgrade_gate.py' --active-submitter-count"
}

activate_submission_gate() {
  allow_existing_backup="$1"
  gate_arguments=(
    /usr/bin/python3 "$remote_stage/van_compute_upgrade_gate.py"
    --acquire --owner "$maintenance_owner"
    --gate "$remote_stage/van_compute_upgrade_gate.py"
    --script-root "$upgrade_public_root"
  )
  if [[ "$allow_existing_backup" == 1 ]]; then
    gate_arguments+=(--allow-existing-backup)
  fi
  /usr/bin/ssh "$pi_host" "${(q)gate_arguments[@]}"
}

maintenance_payload="$(
  /usr/bin/ssh "$pi_host" "/usr/bin/env PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 '$remote_stage/van_compute.py' --root /home/pi/dev/obd-things/tmp/compute maintenance status"
)"
maintenance_relation="$(
  print -r -- "$maintenance_payload" | /opt/homebrew/bin/python3 -c '
import json
import sys
payload = json.load(sys.stdin)
owner = sys.argv[1]
if not payload.get("active"):
    print("inactive")
elif payload.get("owner") == owner:
    print("ours")
else:
    print("other")
' "$maintenance_owner"
)"
if [[ "$maintenance_relation" == other ]]; then
  echo "The compute queue is in maintenance under a different installer; no changes were made." >&2
  exit 1
elif [[ "$maintenance_relation" == ours ]]; then
  maintenance_active=1
  remote_upgrade_started=1
  echo "Resuming this Mac's interrupted protocol upgrade."
fi

active_jobs="$(active_queue_jobs)"
if [[ "$active_jobs" != 0 ]]; then
  echo "The Pi compute queue has pending or running work; no deployed Pi files were changed." >&2
  echo "Let it finish (or inspect it) and rerun the installer." >&2
  exit 1
fi

agent_has_pid() {
  /bin/launchctl print "gui/$user_id/$label" 2>/dev/null |
    /usr/bin/grep -Eq '^[[:space:]]*pid = [0-9]+'
}

loaded_agent=""
if loaded_agent="$(/bin/launchctl print "gui/$user_id/$label" 2>/dev/null)"; then
  if [[ ! -f "$target_plist" || -L "$target_plist" ]] || \
     ! print -r -- "$loaded_agent" |
       /usr/bin/grep -Eq '^[[:space:]]*--serve[[:space:]]*$' || \
     ! /usr/libexec/PlistBuddy -c "Print :ProgramArguments" "$target_plist" 2>/dev/null |
       /usr/bin/grep -Eq '^[[:space:]]*--serve[[:space:]]*$'; then
    echo "The loaded worker is not the supported persistent --serve LaunchAgent." >&2
    echo "Refusing to stop or replace an unknown worker configuration." >&2
    exit 1
  fi
  previous_agent_disabled=1
  /bin/launchctl disable "gui/$user_id/$label"
  /bin/launchctl kill SIGUSR1 "gui/$user_id/$label" 2>/dev/null || true

  agent_stopped=0
  for attempt in {1..240}; do
    active_jobs="$(active_queue_jobs)"
    if [[ "$active_jobs" != 0 ]]; then
      break
    fi
    if ! agent_has_pid; then
      agent_stopped=1
      break
    fi
    /bin/sleep 0.5
  done
  if [[ "$active_jobs" != 0 ]]; then
    echo "Queue work appeared while draining; the current release will be retained." >&2
    exit 1
  fi
  if (( ! agent_stopped )); then
    echo "The persistent worker did not finish draining within 120 seconds." >&2
    exit 1
  fi
  restore_previous_agent=1
  /bin/launchctl bootout "gui/$user_id/$label"
  /bin/launchctl enable "gui/$user_id/$label"
  previous_agent_disabled=0
fi

# Fence the public CLI before crossing the protocol boundary. Any process that
# already loaded the previous submit code must disappear, and all queue entries
# (including hidden .staging-* directories) must remain absent.
activate_submission_gate "$remote_upgrade_started"
submission_gate_active=1
active_submitter_count=1
for attempt in {1..240}; do
  active_submitter_count="$(active_submitters)"
  active_jobs="$(active_queue_jobs)"
  if [[ "$active_jobs" != 0 || "$active_submitter_count" == 0 ]]; then
    break
  fi
  /bin/sleep 0.5
done
if [[ "$active_jobs" != 0 ]]; then
  echo "A submission reached the queue while the installer was fencing it." >&2
  if (( remote_upgrade_started )); then
    echo "The interrupted upgrade remains fenced; inspect the job and rerun this installer." >&2
  else
    echo "The previous CLI and worker will be restored; let that job finish and rerun." >&2
  fi
  exit 1
fi
if [[ "$active_submitter_count" != 0 ]]; then
  echo "A Pi submission did not drain within 120 seconds." >&2
  if (( remote_upgrade_started )); then
    echo "The interrupted upgrade remains fenced; let it finish and rerun this installer." >&2
  else
    echo "The previous CLI and worker will be restored; let it finish and rerun." >&2
  fi
  exit 1
fi

# The worker and Pi broker honor this marker before claiming. It remains active
# through protocol replacement and the first new heartbeat.
/usr/bin/ssh "$pi_host" "/usr/bin/env PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 '$remote_stage/van_compute.py' --root /home/pi/dev/obd-things/tmp/compute maintenance enter --owner '$maintenance_owner'" >/dev/null
maintenance_active=1
active_submitter_count="$(active_submitters)"
active_jobs="$(active_queue_jobs)"
if [[ "$active_jobs" != 0 || "$active_submitter_count" != 0 ]]; then
  echo "Submission activity appeared across the maintenance boundary; upgrade is stopping safely." >&2
  exit 1
fi

# Everything above is compatible preflight/provisioning. From the first file
# replacement onward, do not restore an incompatible old worker on failure.
remote_upgrade_started=1
/usr/bin/ssh "$pi_host" "
  set -eu

  if /usr/bin/systemctl is-active --quiet van-compute-broker.service; then
    sudo -n systemctl stop van-compute-broker.service
  fi
  test -x /home/pi/.local/share/van-compute/venv/bin/python3
  /home/pi/.local/share/van-compute/venv/bin/python3 -c 'import isotp, numpy, pytest'

  install -d -m 700 \
    '$remote_config_root' \
    /home/pi/scripts/compute \
    /home/pi/scripts/compute/python-automation \
    /home/pi/scripts/python-automation \
    /home/pi/scripts/python-automation/templates \
    /home/pi/scripts/python-automation/static
  # The public CLI is still the all-blocking gate here. Install its protocol
  # dependency and the other consumers before atomically publishing the CLI.
  install -m 600 '$remote_stage/van_compute_protocol.py' /home/pi/scripts/compute/python-automation/van_compute_protocol.py
  install -m 600 '$remote_stage/van_compute_metrics.py' /home/pi/scripts/python-automation/van_compute_metrics.py
  install -m 700 '$remote_stage/van_compute_broker.py' /home/pi/scripts/compute/van_compute_broker.py
  install -m 700 '$remote_stage/van_compute_upgrade_gate.py' /home/pi/scripts/compute/van_compute_upgrade_gate.py
  install -m 600 '$remote_stage/van-compute-obd.example.json' '$remote_config_root/van-compute-obd.example.json'
  install -m 600 '$remote_stage/van_dashboard.html' /home/pi/scripts/python-automation/templates/van_dashboard.html
  install -m 600 '$remote_stage/van_dashboard.js' /home/pi/scripts/python-automation/static/van_dashboard.js
  install -m 600 '$remote_stage/van_dashboard.css' /home/pi/scripts/python-automation/static/van_dashboard.css
  sudo -n install -m 644 '$remote_stage/van-compute-broker.service' /etc/systemd/system/van-compute-broker.service
  install -m 700 '$remote_stage/pi_compute.py' /home/pi/scripts/compute/pi_compute.py
  install -m 700 '$remote_stage/van_compute.py' /home/pi/scripts/compute/.van_compute.py.install.$install_id
  mv -f -- /home/pi/scripts/compute/.van_compute.py.install.$install_id /home/pi/scripts/compute/van_compute.py

  rm -f \
    '$remote_stage/van_compute.py' \
    '$remote_stage/pi_compute.py' \
    '$remote_stage/van_compute_broker.py' \
    '$remote_stage/van_compute_upgrade_gate.py' \
    '$remote_stage/van-compute-broker.service' \
    '$remote_stage/van-compute-obd.example.json' \
    '$remote_stage/van_compute_protocol.py' \
    '$remote_stage/van_compute_metrics.py' \
    '$remote_stage/van_dashboard.html' \
    '$remote_stage/van_dashboard.js' \
    '$remote_stage/van_dashboard.css'
  rm -f '$remote_stage/python-automation/van_compute_protocol.py'
  rmdir '$remote_stage/python-automation'
  rmdir '$remote_stage'
  /home/pi/scripts/compute/van_compute.py tasks >/dev/null
  /home/pi/scripts/compute/pi_compute.py tasks >/dev/null
  sudo -n systemctl daemon-reload
  sudo -n systemctl enable van-compute-broker.service
  sudo -n systemctl restart van-compute-broker.service
  sudo -n systemctl is-active --quiet van-compute-broker.service
  if /usr/bin/systemctl is-active --quiet van-dashboard.service; then
    sudo -n systemctl restart van-dashboard.service
  fi
"
remote_stage_created=0

echo "Installing the persistent 10-slot LaunchAgent..."
/bin/mkdir -p "$target_dir"
plist_stage="$release/$label.plist"
/usr/bin/install -m 600 "$source_plist" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:0 $release/venv/bin/python" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:1 $release/app/macbook/scripts/van_compute_worker.py" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:4 $pi_host" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:6 $worker_name" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:8 $release/venv/bin/python" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:10 $cache_root/jobs" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:12 $cache_root/ssh/control.sock" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:14 $release/sandbox.sb" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :StandardOutPath $cache_root/logs/worker.stdout.log" "$plist_stage"
/usr/libexec/PlistBuddy -c "Set :StandardErrorPath $cache_root/logs/worker.stderr.log" "$plist_stage"
next_argument=15
if [[ "$allow_unsandboxed" == 1 ]]; then
  /usr/libexec/PlistBuddy -c "Delete :ProgramArguments:14" "$plist_stage"
  /usr/libexec/PlistBuddy -c "Delete :ProgramArguments:13" "$plist_stage"
  /usr/libexec/PlistBuddy -c "Add :ProgramArguments:13 string --allow-unsandboxed-dynamic" "$plist_stage"
  next_argument=14
fi
if [[ -f "$dataset_target" ]]; then
  /usr/libexec/PlistBuddy -c "Add :ProgramArguments:${next_argument} string --dataset-config" "$plist_stage"
  (( next_argument += 1 ))
  /usr/libexec/PlistBuddy -c "Add :ProgramArguments:${next_argument} string $dataset_target" "$plist_stage"
fi
/usr/bin/plutil -lint "$plist_stage"
/usr/bin/install -m 600 "$plist_stage" "$target_plist"
release_published=1

previous_coordinator_seen="$(
  /usr/bin/ssh "$pi_host" /home/pi/scripts/compute/van_compute.py available |
    /opt/homebrew/bin/python3 -c '
import json
import sys
payload = json.load(sys.stdin)
base = sys.argv[1]
print(next((str(w.get("seen_at", "")) for w in payload.get("workers", []) if w.get("worker") == base), ""))
' "$worker_name"
)"
/bin/launchctl bootout "gui/$user_id/$label" 2>/dev/null || true
/bin/launchctl bootstrap "gui/$user_id" "$target_plist"
/bin/launchctl kickstart -k "gui/$user_id/$label"
restore_previous_agent=0

echo "Installed. Waiting for the 10-slot scheduler heartbeat..."
heartbeat=""
for attempt in {1..45}; do
  heartbeat="$(/usr/bin/ssh "$pi_host" /home/pi/scripts/compute/van_compute.py available)"
  if print -r -- "$heartbeat" | /opt/homebrew/bin/python3 -c '
import json
import sys
payload = json.load(sys.stdin)
base = sys.argv[1]
workers = payload.get("workers", [])
coordinator = next((w for w in workers if w.get("worker") == base and w.get("available")), None)
previous_seen = sys.argv[2]
raise SystemExit(
    not coordinator
    or coordinator.get("seen_at") == previous_seen
    or coordinator.get("slots_total") != 10
    or not 0 <= coordinator.get("slots_busy", -1) <= 10
)
' "$worker_name" "$previous_coordinator_seen"; then
    # The helper holds a Pi-side cross-installer lock while it verifies this
    # owner, removes rollback artifacts, and releases queue maintenance.
    finalize_arguments=(
      /usr/bin/python3 /home/pi/scripts/compute/van_compute_upgrade_gate.py
      --finalize --owner "$maintenance_owner"
      --script-root "$upgrade_public_root"
    )
    /usr/bin/ssh "$pi_host" "${(q)finalize_arguments[@]}" >/dev/null
    maintenance_active=0
    submission_gate_active=0
    previous_kept=0
    for candidate in "$release_parent"/*(N/om); do
      [[ "$candidate" == "$release" ]] && continue
      if (( ! previous_kept )); then
        previous_kept=1
        continue
      fi
      if [[ "$candidate" == "$release_parent"/20* && -d "$candidate" && ! -L "$candidate" ]]; then
        /bin/rm -rf -- "$candidate"
      fi
    done
    print -r -- "$heartbeat"
    echo "Worker isolation: $([[ "$allow_unsandboxed" == 1 ]] && echo disabled || echo sandbox-exec validated)"
    echo "A dedicated worker account, container, or VM would provide stronger isolation but requires separate admin setup."
    echo "Installed release: $release"
    exit 0
  fi
  /bin/sleep 1
done

print -r -- "$heartbeat"
echo "Worker did not publish a fresh 10-slot coordinator heartbeat within 45 seconds." >&2
echo "Inspect: $cache_root/logs/worker.stderr.log" >&2
exit 1
