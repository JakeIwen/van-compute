#!/usr/bin/python3 -P
from pathlib import Path
import sys

sys.dont_write_bytecode = True

RELEASE = Path(__file__).resolve().parent.parent / 'current'
sys.path.insert(0, str(RELEASE))

from van_compute.queue import main

raise SystemExit(main())
