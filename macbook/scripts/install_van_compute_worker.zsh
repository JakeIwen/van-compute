#!/bin/zsh
set -euo pipefail

script_dir="${0:A:h}"
repo_root="${script_dir:h:h}"
pi_host="${VAN_COMPUTE_HOST:-pi@vanpi}"
label="com.jacobr.van-compute-worker"
source_plist="$repo_root/macbook/launchagents/$label.plist"
target_dir="$HOME/Library/LaunchAgents"
target_plist="$target_dir/$label.plist"
application_root="$HOME/Library/Application Support/van-compute"
user_id="$(id -u)"
remote_stage="/home/pi/.cache/van-compute-install"

echo "Checking local worker dependencies..."
/opt/homebrew/bin/python3 -c 'import numpy'
/usr/bin/plutil -lint "$source_plist"

echo "Checking SSH access to $pi_host..."
/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 "$pi_host" true

echo "Deploying the Pi queue CLI and shared protocol..."
/usr/bin/ssh "$pi_host" "install -d -m 700 '$remote_stage'"
/usr/bin/scp \
  "$repo_root/pi/scripts/van_compute.py" \
  "$repo_root/shared/python/van_compute_protocol.py" \
  "$pi_host:$remote_stage/"
/usr/bin/ssh "$pi_host" "
  set -eu
  install -d -m 700 /home/pi/scripts/python-automation
  install -m 700 '$remote_stage/van_compute.py' /home/pi/scripts/van_compute.py
  install -m 600 '$remote_stage/van_compute_protocol.py' /home/pi/scripts/python-automation/van_compute_protocol.py
  rm -f '$remote_stage/van_compute.py' '$remote_stage/van_compute_protocol.py'
  rmdir '$remote_stage'
  /home/pi/scripts/van_compute.py tasks >/dev/null
"

echo "Installing the local worker and per-user LaunchAgent..."
/bin/mkdir -p "$application_root/macbook/scripts" "$application_root/shared/python"
/usr/bin/install -m 700 \
  "$repo_root/macbook/scripts/van_compute_worker.py" \
  "$application_root/macbook/scripts/van_compute_worker.py"
/usr/bin/install -m 600 \
  "$repo_root/shared/python/van_compute_protocol.py" \
  "$application_root/shared/python/van_compute_protocol.py"
/bin/mkdir -p "$target_dir"
/usr/bin/install -m 600 "$source_plist" "$target_plist"
/bin/launchctl bootout "gui/$user_id/$label" 2>/dev/null || true
/bin/launchctl bootstrap "gui/$user_id" "$target_plist"
/bin/launchctl kickstart -k "gui/$user_id/$label"

echo "Installed. Worker heartbeat:"
/usr/bin/ssh "$pi_host" /home/pi/scripts/van_compute.py available
