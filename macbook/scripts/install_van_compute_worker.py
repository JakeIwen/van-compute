#!/usr/bin/python3 -B
"""Deploy the coupled van-compute Pi broker and persistent Mac worker.

The shell entry point is retained as a compatibility shim. This module owns the
actual deployment so the safety-sensitive ordering can be exercised with fake
local and remote runners.
"""

import sys

sys.dont_write_bytecode = True

from van_compute_installer.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
