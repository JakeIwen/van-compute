"""Deploy the coupled van-compute Pi broker and persistent Mac worker.

The shell entry point is retained as a compatibility shim. This module owns the
actual deployment so the safety-sensitive ordering can be exercised with fake
local and remote runners.
"""

from __future__ import annotations

import argparse
import signal
import sys
from typing import Sequence

from .models import DeploymentError, Options
from .orchestrator import Installer


def parse_arguments(argv: Sequence[str] | None = None) -> Options:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--if-needed",
        action="store_true",
        help="skip only when both installed sides are current and healthy",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the local plan without probing dependencies, launchd, or the Pi",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="build a fresh immutable Mac environment; overrides --if-needed",
    )
    arguments = parser.parse_args(argv)
    return Options(
        if_needed=arguments.if_needed,
        dry_run=arguments.dry_run,
        rebuild=arguments.rebuild,
    )

def main(argv: Sequence[str] | None = None) -> int:
    previous_handlers: dict[int, object] = {}
    interrupted = False

    def interrupt(_signum: int, _frame: object) -> None:
        nonlocal interrupted
        if interrupted:
            return
        interrupted = True
        raise KeyboardInterrupt

    try:
        for signum in (signal.SIGHUP, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, interrupt)
        return Installer(parse_arguments(argv)).execute()
    except KeyboardInterrupt:
        print("van-compute installer: interrupted", file=sys.stderr)
        return 130
    except DeploymentError as exc:
        print(f"van-compute installer: {exc}", file=sys.stderr)
        return 1
    finally:
        interrupted = True
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
