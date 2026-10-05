#!/bin/zsh
set -euo pipefail

script_dir="${0:A:h}"
export PYTHONDONTWRITEBYTECODE=1
exec /usr/bin/python3 -B "$script_dir/install_van_compute_worker.py" "$@"
