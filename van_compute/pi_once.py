"""The `--once` command: wait out CAN quiet, then make one placement decision."""

from __future__ import annotations

from typing import Callable, Mapping

from van_compute.can_activity import CanProbe
from van_compute.job_cancel import is_cancel_requested
from van_compute.pi_cleanup_hold import stop_flag_signals


def run_once_command(
    config,
    monitor,
    *,
    run_once: Callable[..., dict[str, object]],
    running_job: Callable[[], Mapping[str, object] | None],
) -> dict[str, object]:
    running = running_job()
    if running is not None and (
        running.get("cleanup_unconfirmed") or is_cancel_requested(running)
    ):
        # Recovery of a marked or cancelled job must not wait on, or defer for, CAN.
        waited = CanProbe(False, (), None)
    else:
        waited = monitor.wait_for_quiet(config.can_quiet_seconds)
    if not waited.quiet:
        # The wait ended on a lock or probe failure; observing again inside
        # run_once could see a transient quiet and run anyway.
        return {
            "action": "deferred",
            "reason": "CAN not quiet while waiting: "
            + (waited.error or ", ".join(waited.held_locks)),
        }
    # Stop signals set a flag (as under serve) so a Ctrl-C during a cleanup
    # hold ends it cleanly instead of unwinding.
    with stop_flag_signals() as stopping:
        return run_once(config, monitor=monitor, should_stop=stopping)
