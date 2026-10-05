#!/usr/bin/python3 -P
from pathlib import Path
import sys

sys.dont_write_bytecode = True

if any(
    line == b'UPGRADE_GATE = True'
    for line in Path(__file__).with_name('van_compute.py').read_bytes().splitlines()
):
    print(
        'van-compute is being upgraded; retry this command shortly',
        file=sys.stderr,
    )
    raise SystemExit(75)

RELEASE = Path(__file__).resolve().parent.parent / 'current'
sys.path.insert(0, str(RELEASE))

from van_compute.frontend import main

raise SystemExit(main())
