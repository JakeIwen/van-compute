#!/usr/bin/env python3
"""Post-deploy live check of job cancellation on the real Mac and Pi.

Run on the Mac after a deploy, never from the test suite.  It submits its own
`repo-tests` jobs and cancels them (1) while queued, (2) while running on a Mac
slot, and (3) while running on the Pi, for which it drains and stops the Mac
worker and always restores it.  It acts only on job ids it submitted and
reports every foreign job id it saw; other sessions' jobs falling back to the
Pi while the Mac is away is ordinary placement.  Process-group proof uses the
launch lines both hosts log (Mac worker stderr log, broker journal).

Prints one JSON report; exits 0 only if every stage it ran passed.
`--dry-run` prints the plan and touches nothing.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import Callable, Iterable, Sequence

PI_HOST = "vanpi"
PI_COMPUTE = "/home/pi/van_compute/scripts/pi_compute.py"
QUEUE_CLI = "/home/pi/van_compute/scripts/van_compute.py"
QUEUE_ROOT = "/home/pi/dev/obd-things/tmp/compute"
LABEL = "com.jacobr.van-compute-worker"
PLIST = Path.home() / "Library/LaunchAgents" / f"{LABEL}.plist"
WORKER_LOG = Path.home() / "Library/Caches/van-compute/logs/worker.stderr.log"
MAC_BASE = "m4mac"
PI_WORKER = "vanpi-local.00"
TASK = "repo-tests"
OVERALL_SECONDS = 20 * 60
LEASE_AND_GRACE_SECONDS = 80
STALE_AGE_SECONDS = 300
FRESH_HEARTBEAT_SECONDS = 45

# Read-only: lists every queued/ and running/ entry with its worker.
PREFLIGHT = (
    "/usr/bin/python3 -I -c 'import json, pathlib, sys\n"
    "root = pathlib.Path(sys.argv[1])\n"
    "for state in (\"queued\", \"running\"):\n"
    "    for job in sorted((root / state).iterdir()):\n"
    "        try:\n"
    "            worker = json.loads((job / \"manifest.json\").read_text()).get(\"worker\")\n"
    "        except Exception:\n"
    "            worker = None\n"
    "        print(json.dumps({\"id\": job.name, \"state\": state, \"worker\": worker}))\n"
    f"' {QUEUE_ROOT}"
)
MAC_LAUNCH_RE = re.compile(r"launched process group (\d+) in (\S+)")


# -- pure helpers (unit tested) ---------------------------------------------------


@dataclass(frozen=True)
class Entry:
    id: str
    state: str
    worker: str | None


def parse_listing(text: str) -> list[Entry]:
    entries = []
    for line in text.splitlines():
        if line.strip():
            item = json.loads(line)
            entries.append(Entry(str(item["id"]), str(item["state"]), item.get("worker")))
    return entries


def mac_work(entries: Iterable[Entry], base: str = MAC_BASE) -> list[Entry]:
    return [
        entry for entry in entries
        if entry.worker is not None and (entry.worker == base or entry.worker.startswith(f"{base}."))
    ]


def foreign_ids(entries: Iterable[Entry], own: Iterable[str]) -> list[str]:
    mine = set(own)
    return sorted({entry.id for entry in entries if entry.id not in mine and not entry.id.startswith(".")})


def mac_launch_group(log_text: str, job_id: str) -> int | None:
    """The last pgid the Mac worker logged launching for this job."""
    found = None
    for match in MAC_LAUNCH_RE.finditer(log_text):
        if match.group(2).startswith(f"{job_id}-"):
            found = int(match.group(1))
    return found


def pi_launch(journal_text: str, job_id: str) -> tuple[int, str | None] | None:
    """(pgid, leader start) from the broker's job-launched journal line."""
    found = None
    for line in journal_text.splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("action") == "job-launched" and payload.get("job") == job_id:
            found = (int(payload["group"]), payload.get("leader_start"))
    return found


def lease_lost_logged(log_text: str, job_id: str) -> bool:
    return f"job {job_id} attempt" in log_text and "no longer leased" in log_text


def coordinator_fresh(available: dict, base: str = MAC_BASE) -> bool:
    for item in available.get("workers", []):
        if item.get("worker") == base and float(item.get("age_seconds", 1e9)) < FRESH_HEARTBEAT_SECONDS:
            return True
    return False


class Deadline:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.end = clock() + seconds

    def remaining(self) -> float:
        return max(0.0, self.end - self.clock())

    def expired(self) -> bool:
        return self.remaining() <= 0


# -- host access --------------------------------------------------------------------


def run(command: Sequence[str], timeout: float = 60) -> subprocess.CompletedProcess:
    return subprocess.run(list(command), capture_output=True, text=True, timeout=timeout, check=False)


def pi(command: str, timeout: float = 60) -> subprocess.CompletedProcess:
    return run(["ssh", PI_HOST, command], timeout)


def pi_json(command: str) -> tuple[int, dict | None]:
    result = pi(command)
    try:
        return result.returncode, json.loads(result.stdout)
    except json.JSONDecodeError:
        return result.returncode, None


def listing() -> list[Entry]:
    result = pi(PREFLIGHT)
    if result.returncode != 0:
        raise RuntimeError(f"queue listing failed: {result.stderr.strip()}")
    return parse_listing(result.stdout)


def status(job_id: str) -> dict:
    _code, payload = pi_json(f"{PI_COMPUTE} status {job_id}")
    return payload or {}


def wait_until(predicate: Callable[[], bool], seconds: float, deadline: Deadline) -> bool:
    end = time.monotonic() + min(seconds, deadline.remaining())
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(1)
    return predicate()


def submit() -> str:
    code, payload = pi_json(f"{PI_COMPUTE} run {TASK}")
    if code != 0 or not payload:
        raise RuntimeError(f"submission failed with {code}")
    return str(payload["id"])


def group_alive_locally(group: int) -> bool:
    return run(["/usr/bin/pgrep", "-g", str(group)]).returncode == 0


def group_alive_on_pi(group: int, leader_start: str | None) -> bool:
    if pi(f"/usr/bin/pgrep -g {group}").returncode != 0:
        return False
    if leader_start is None:
        return True
    stat = pi(f"cat /proc/{group}/stat").stdout
    fields = stat.rpartition(")")[2].split()
    return len(fields) > 19 and fields[19] == leader_start


# -- stages ---------------------------------------------------------------------------


@dataclass
class Report:
    own: list[str] = field(default_factory=list)
    foreign: set[str] = field(default_factory=set)
    stages: dict[str, dict] = field(default_factory=dict)

    def saw(self, entries: Iterable[Entry]) -> None:
        self.foreign.update(foreign_ids(entries, self.own))


def stage_queued(report: Report, deadline: Deadline) -> dict:
    for _attempt in range(2):
        code, payload = pi_json(f"{PI_COMPUTE} run {TASK} --wait 0 --cancel-on-timeout")
        if payload:
            report.own.append(str(payload["id"]))
        if code == 4 and payload and payload.get("cancelled_from") == "queued":
            return {"ok": True, "job": payload["id"]}
    return {"ok": False, "reason": "no attempt was cancelled while still queued", "exit": code}


def stage_mac(report: Report, deadline: Deadline) -> dict:
    job = submit()
    report.own.append(job)
    if not wait_until(lambda: str(status(job).get("worker", "")).startswith(f"{MAC_BASE}."), 120, deadline):
        pi(f"{PI_COMPUTE} cancel {job}")
        return {"ok": False, "job": job, "reason": "never ran on a Mac slot"}
    time.sleep(3)
    group = mac_launch_group(WORKER_LOG.read_text(errors="replace"), job)
    alive_before = group is not None and group_alive_locally(group)
    _code, cancelled = pi_json(f"{PI_COMPUTE} cancel {job}")
    gone = group is not None and wait_until(lambda: not group_alive_locally(group), 30, deadline)
    logged = lease_lost_logged(WORKER_LOG.read_text(errors="replace"), job)
    time.sleep(min(STALE_AGE_SECONDS + 60, deadline.remaining()))
    final = status(job)
    ok = bool(cancelled and cancelled.get("cancel_result") == "cancelled" and alive_before
              and gone and logged and final.get("state") == "cancelled")
    return {"ok": ok, "job": job, "group": group, "alive_before": alive_before, "gone": gone,
            "lease_lost_logged": logged, "final_state": final.get("state")}


def stage_pi(report: Report, deadline: Deadline) -> dict:
    job = submit()
    report.own.append(job)
    if not wait_until(lambda: status(job).get("worker") == PI_WORKER, 360, deadline):
        pi(f"{PI_COMPUTE} cancel {job}")
        return {"ok": None, "job": job, "reason": "not exercised: the Pi did not run it in time"}
    time.sleep(3)
    journal = pi("journalctl -u van-compute-broker -o cat --since -15min").stdout
    launch = pi_launch(journal, job)
    alive_before = launch is not None and group_alive_on_pi(*launch)
    _code, cancelled = pi_json(f"{PI_COMPUTE} cancel {job}")
    waited = pi(f"{PI_COMPUTE} wait {job} --timeout 60", timeout=90)
    gone = launch is not None and not group_alive_on_pi(*launch)
    ok = bool(cancelled and cancelled.get("cancel_result") == "requested" and alive_before
              and waited.returncode == 4 and gone)
    return {"ok": ok, "job": job, "launch": launch, "alive_before": alive_before,
            "wait_exit": waited.returncode, "gone": gone}


# -- the Mac worker transaction ------------------------------------------------------


class MacWorkerOutage:
    """Stop the Mac worker the way the installer drains it; always restore it."""

    def __init__(self, report: Report) -> None:
        self.report = report
        self.domain = f"gui/{os.getuid()}/{LABEL}"
        self.stopped = False

    def _loaded(self) -> bool:
        return run(["/bin/launchctl", "print", self.domain]).returncode == 0

    def _mac_busy(self) -> bool:
        entries = listing()
        self.report.saw(entries)
        return any(entry.state == "running" for entry in mac_work(entries))

    def stop(self) -> bool:
        """False when Mac work appeared: restored, and the stage is skipped."""
        if self._mac_busy():
            return False
        self.stopped = True
        run(["/bin/launchctl", "disable", self.domain])
        run(["/bin/launchctl", "kill", "SIGUSR1", self.domain])
        end = time.monotonic() + 15
        while time.monotonic() < end and self._loaded():
            if self._mac_busy():
                return False
            time.sleep(0.5)
        if self._mac_busy():
            return False
        run(["/bin/launchctl", "bootout", self.domain])
        return not self._mac_busy()

    def restore(self) -> dict:
        if not self.stopped:
            return {"restored": "not stopped"}
        self.stopped = False
        run(["/bin/launchctl", "enable", self.domain])
        if not self._loaded():
            run(["/bin/launchctl", "bootstrap", f"gui/{os.getuid()}", str(PLIST)])
        end = time.monotonic() + 120
        while time.monotonic() < end:
            _code, available = pi_json(f"{QUEUE_CLI} available")
            if available and coordinator_fresh(available):
                return {"restored": True, "fresh_heartbeat": True}
            time.sleep(5)
        return {"restored": True, "fresh_heartbeat": False}


def stage_pi_transaction(report: Report, deadline: Deadline) -> dict:
    outage = MacWorkerOutage(report)
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"signal {signum}")

    for number in previous:
        signal.signal(number, interrupted)
    try:
        if not outage.stop():
            return {"ok": None, "reason": "skipped: Mac work appeared while draining", **outage.restore()}
        time.sleep(min(LEASE_AND_GRACE_SECONDS, deadline.remaining()))
        result = stage_pi(report, deadline)
    finally:
        restored = outage.restore()
        for number, handler in previous.items():
            signal.signal(number, handler)
    result.update(restored)
    result["final_state"] = status(result["job"]).get("state") if "job" in result else None
    if not restored.get("fresh_heartbeat"):
        result["ok"] = False
    return result


PLAN = (
    "queued: run --wait 0 --cancel-on-timeout; expect exit 4, cancelled_from queued",
    "mac: submit, wait for an m4mac slot, check its pgid, cancel, expect the group gone, "
    "LeaseLost logged, still cancelled after the 300 s stale age",
    "pi: exhaustive read-only queue listing; drain (disable, SIGUSR1, poll), bootout; "
    "submit, wait for vanpi-local.00, check pgid and leader start, cancel, wait exits 4, "
    "group gone; restore the worker and require a fresh heartbeat on every exit",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-pi", action="store_true", help="do not stop the Mac worker")
    args = parser.parse_args(argv)
    if args.dry_run:
        print(json.dumps({"plan": PLAN}, indent=2))
        return 0
    report = Report()
    deadline = Deadline(OVERALL_SECONDS)
    report.saw(listing())
    stages = [("queued", stage_queued), ("mac", stage_mac)]
    if not args.skip_pi:
        stages.append(("pi", stage_pi_transaction))
    for name, stage in stages:
        if deadline.expired():
            report.stages[name] = {"ok": None, "reason": "not run: overall deadline"}
            continue
        try:
            report.stages[name] = stage(report, deadline)
        except Exception as exc:  # report and move on; the outage restores itself
            report.stages[name] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    for job in report.own:
        if status(job).get("state") not in ("cancelled", "done", "failed"):
            pi(f"{PI_COMPUTE} cancel {job}")
    report.saw(listing())
    print(json.dumps({
        "own_jobs": report.own, "foreign_jobs_seen": sorted(report.foreign),
        "stages": report.stages,
    }, indent=2, sort_keys=True))
    return 0 if all(stage.get("ok") is not False for stage in report.stages.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
