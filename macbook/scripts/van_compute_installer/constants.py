from __future__ import annotations

import re

LABEL = "com.jacobr.van-compute-worker"
DEFAULT_HOST = "pi@vanpi.lan"
DEFAULT_WORKER = "m4mac"
REMOTE_ROOT = "/home/pi/van_compute"
REMOTE_SCRIPTS = f"{REMOTE_ROOT}/scripts"
REMOTE_CONFIGS = f"{REMOTE_ROOT}/configs"
REMOTE_RELEASES = f"{REMOTE_ROOT}/releases"
REMOTE_VENV = f"{REMOTE_ROOT}/venv"
OLD_COMPUTE_ROOT = "/home/pi/scripts/compute"
QUEUE_ROOT = "/home/pi/dev/obd-things/tmp/compute"
SOURCE_HASH_FILE = "source.sha256"
DEPLOYMENT_HASH_FILE = "deployment.sha256"
PROVENANCE_FILE = "provenance.json"
# Pi marker files get this explicit mode so the manifest, which the reuse check
# compares mode by mode, does not depend on the umask of whoever ran the installer.
# 0644 matches what umask-022 builds already recorded, keeping those releases reusable.
PI_MARKER_MODE = 0o644
MANIFEST_FILE = "manifest.json"
OWNER_RE = re.compile(
    r"installer-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
SAFE_WORKER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,59}")

SANDBOX_PROFILE = r'''(version 1)
(deny default)
(allow process*)
(allow sysctl-read)
(allow ipc-posix*)
; Metadata is needed to traverse operator-configured dataset roots. Contents
; and directory reads remain constrained by file-read* below.
(allow file-read-metadata)
; dyld reads the root directory's own contents at launch on macOS 27; without
; this every process aborts. A literal grants "/" only, not paths below it.
(allow file-read-data (literal "/"))
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
    ; /usr/bin/git, make, clang and python3 are xcrun shims: they read the
    ; xcode-select link, then load libxcrun and the real tool from the Command
    ; Line Tools. Read-only; a full Xcode selected instead is not covered.
    (subpath "/Library/Developer/CommandLineTools")
    (literal "/private/var/db/xcode_select_link")
    (subpath (param "WORKER_ROOT"))
    (subpath (param "JOB_ROOT"))
    (subpath (param "CACHE_ROOT"))
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
    (subpath (param "CACHE_ROOT"))
    (literal "/dev/null"))
; Seatbelt cannot filter by host, so the only network a job gets is TCP to the
; worker's registry proxy, which enforces the package-registry allowlist.
; "localhost" covers 127.0.0.1 and ::1 (the proxy holds the port on both);
; "remote tcp" rather than "remote ip" keeps UDP to that port denied.
(allow network-outbound
    (remote tcp (string-append "localhost:" (param "PROXY_PORT"))))
'''

SANDBOX_DENIAL_PROBE = r'''
import errno
from pathlib import Path
import socket
import sys

for index, raw_sentinel in enumerate(sys.argv[1:]):
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
'''

# Runs inside the sandbox with CACHE_ROOT bound to its first argument: the
# selected cache must be usable while sibling caches, a private sentinel and a
# symlink planted in the cache that points at the sentinel stay unwritable.
SANDBOX_CACHE_PROBE = r'''
import errno
from pathlib import Path
import sys

selected, other, sentinel, link = (Path(item) for item in sys.argv[1:5])
(selected / "probe.txt").write_text("ok", encoding="utf-8")
if (selected / "probe.txt").read_text(encoding="utf-8") != "ok":
    raise SystemExit("sandbox could not read back its selected cache")


def require_denied(label, action):
    try:
        action()
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EPERM}:
            return
        raise
    raise SystemExit(f"sandbox allowed {label}")


require_denied("a read of another cache", lambda: (other / "probe.txt").read_text())
require_denied("a write to another cache", lambda: (other / "probe.txt").write_text("x"))
require_denied("a write to a private sentinel", lambda: sentinel.write_text("x"))
require_denied("a write through a cache symlink", lambda: link.write_text("x"))
'''


RELEASE_LINK_GUARD = r'''
check_release_target() {
  release_target_name="${2#"$1/releases/"}"
  case "$release_target_name" in ''|*[!0123456789abcdef]*) return 1;; esac
  test "${#release_target_name}" -eq 24 || return 1
  test "$2" = "$1/releases/$release_target_name"
}
'''
