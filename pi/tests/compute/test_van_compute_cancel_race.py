"""Processes racing cancels against claims, finishes, requeues and readers.

Every process uses the real queue lock (flock).  Mutators claim, finish,
requeue (stale recovery and the CAN path) and cancel at random; readers poll
status, list and wait.  Afterwards: each job is in exactly one directory,
every landed cancel ended in cancelled/, and no reader ever called an
existing job unknown.
"""

import datetime as dt
import json
import multiprocessing
import os
from pathlib import Path
import random
import tempfile
import time
import unittest

from van_compute import broker, job_cancel, job_requeue, job_states, queue

from pi.tests.compute.van_compute_test_fixtures import BrokerHarness

ITERATIONS = 100
LANDED = set(job_states.CANCEL_LANDED)


def _log(path: Path, record: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def _mutator(root_text: str, ids: list[str], seed: int, log_text: str) -> None:
    root, log = Path(root_text), Path(log_text)
    rng = random.Random(seed)
    worker = f"m4mac.{seed:02d}"
    leases: dict[str, str] = {}
    for _ in range(ITERATIONS):
        action = rng.choice(
            ("claim", "claim", "claim", "finish", "cancel", "stale", "stale", "can", "reconcile")
        )
        try:
            if action == "claim":
                claimed = queue.worker_claim(root, worker)
                if claimed is not None:
                    leases[str(claimed["id"])] = str(claimed["lease_token"])
            elif action == "finish" and leases and rng.random() < 0.3:
                job_id = rng.choice(sorted(leases))
                queue.worker_finish(root, job_id, worker, rng.choice((0, 1)), [], leases.pop(job_id))
            elif action == "cancel":
                job_id = rng.choice(ids)
                result, _manifest = job_cancel.cancel_job(root, job_id)
                _log(log, {"cancel": job_id, "result": result})
            elif action == "stale":
                later = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
                broker.recover_stale_remote_jobs(root, stale_age=0.0, now=later)
            elif action == "can" and leases:
                job_id = rng.choice(sorted(leases))
                job_requeue.requeue_owned_job(
                    root, job_id, leases.pop(job_id), worker=worker, reason="pi-can-transmit",
                    now=dt.datetime.now(dt.timezone.utc),
                )
            elif action == "reconcile":
                job_cancel.reconcile(root)
        except queue.QueueError:
            continue  # a lost lease or an already-moved job: expected under races
    _log(log, {"mutator_done": seed})


def _reader(root_text: str, ids: list[str], seed: int, log_text: str, stop_text: str) -> None:
    root, log, stop = Path(root_text), Path(log_text), Path(stop_text)
    rng = random.Random(seed)
    reads = 0
    while not stop.exists():
        reads += 1
        job_id = rng.choice(ids)
        try:
            manifest = queue.wait_for_job(root, job_id, 0)
            if manifest.get("state") == "cancelled" and job_states.exit_status(manifest) != 4:
                _log(log, {"reader_error": f"cancelled job {job_id} not exit 4"})
            listed = [item["id"] for item in queue.list_jobs(root, 1000)]
            if len(listed) != len(set(listed)):
                _log(log, {"reader_error": "duplicate ids in list"})
        except queue.QueueError as exc:
            _log(log, {"reader_error": f"{job_id}: {exc}"})
    _log(log, {"reads": reads})


class CancelRaceTests(BrokerHarness, unittest.TestCase):
    def test_racing_processes_keep_every_invariant(self):
        queue.safe_root(self.root)
        ids = [str(self.submit()["id"]) for _ in range(30)]
        log = Path(self.temporary.name) / "race.log"
        stop = Path(self.temporary.name) / "stop"
        log.touch()
        context = multiprocessing.get_context("spawn")
        mutators = [
            context.Process(target=_mutator, args=(str(self.root), ids, seed, str(log)))
            for seed in range(4)
        ]
        readers = [
            context.Process(target=_reader, args=(str(self.root), ids, 100 + seed, str(log), str(stop)))
            for seed in range(2)
        ]
        for process in (*readers, *mutators):
            process.start()
        for process in mutators:
            process.join(120)
        stop.touch()
        for process in readers:
            process.join(30)
        for process in (*mutators, *readers):
            self.assertEqual(process.exitcode, 0)
        job_cancel.reconcile(self.root)

        records = [json.loads(line) for line in log.read_text().splitlines()]
        errors = [item["reader_error"] for item in records if "reader_error" in item]
        self.assertEqual(errors, [])
        landed = {item["cancel"] for item in records if item.get("result") in LANDED}
        self.assertTrue(landed, "the race should cancel at least one job")
        self.assertEqual(sum("mutator_done" in item for item in records), len(mutators))
        reads = [item["reads"] for item in records if "reads" in item]
        self.assertEqual(len(reads), len(readers))
        self.assertTrue(all(count > 0 for count in reads), reads)
        for job_id in ids:
            places = [
                state for state in job_states.STATE_DIRECTORIES
                if (self.root / state / job_id).is_dir()
            ]
            self.assertEqual(len(places), 1, (job_id, places))
            if job_id in landed:
                # A Pi-local request is never made here (all owners are Macs),
                # so every landed cancel is final.
                self.assertEqual(places, ["cancelled"], job_id)
            if places == ["cancelled"]:
                manifest = queue.load_json(self.root / "cancelled" / job_id / "manifest.json")
                self.assertEqual(manifest["state"], "cancelled")


if __name__ == "__main__":
    unittest.main()
