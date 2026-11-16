"""Real signals delivered to a real `run --cancel-on-timeout` process.

Each case stops the process at one boundary (before the job is published,
after publication but before the wait, during the wait, after the job already
finished) and checks the job's fate and the exit status.
"""

import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import unittest

from van_compute import queue

from pi.tests.compute.van_compute_test_fixtures import BrokerHarness

REPO = Path(__file__).resolve().parents[3]

# Runs the frontend with optional patches that pause or act at a boundary.
HARNESS = r"""
import os, signal, sys
from van_compute import frontend, queue

mode = sys.argv[1]
real_wait = queue.wait_for_job

def paused(*args, **kwargs):
    print("published", file=sys.stderr, flush=True)
    sys.stdin.readline()
    return real_wait(*args, **kwargs)

def finish_then_signal(root, job_id, *args, **kwargs):
    claimed = queue.worker_claim(root, "m4mac.00")
    queue.worker_finish(root, job_id, "m4mac.00", 0, [], claimed["lease_token"])
    os.kill(os.getpid(), signal.SIGTERM)
    return real_wait(root, job_id, *args, **kwargs)

if mode == "pause-before-wait":
    queue.wait_for_job = paused
elif mode == "finish-then-signal":
    queue.wait_for_job = finish_then_signal
sys.exit(frontend.main(sys.argv[2:]))
"""


class FrontendSignalTests(BrokerHarness, unittest.TestCase):
    def setUp(self):
        super().setUp()
        queue.safe_root(self.root)

    def start(self, mode="plain", flag=True):
        command = [
            sys.executable, "-c", HARNESS, mode, "--root", str(self.root), "run", "local-python",
            "--source-root", str(self.source), "--wait", "30",
            *(["--cancel-on-timeout"] if flag else []),
        ]
        environment = {**os.environ, "PYTHONPATH": str(REPO), "PYTHONDONTWRITEBYTECODE": "1"}
        process = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=environment, cwd=REPO,
        )
        self.addCleanup(lambda: process.poll() is None and process.kill())
        return process

    def wait_for(self, predicate, what):
        deadline = time.monotonic() + 15
        while not predicate():
            self.assertLess(time.monotonic(), deadline, f"timed out waiting for {what}")
            time.sleep(0.02)

    def queued_ids(self):
        return [p.name for p in (self.root / "queued").iterdir() if queue.JOB_ID_RE.fullmatch(p.name)]

    def finish(self, process):
        stdout, stderr = process.communicate(timeout=20)
        return process.returncode, (json.loads(stdout) if stdout.strip() else None), stderr

    def assert_cancelled_by_signal(self, payload):
        self.assertEqual(payload["cancel_result"], "cancelled")
        self.assertEqual(payload["cancel_reason"], "caller-signal")
        self.assertTrue((self.root / "cancelled" / payload["id"]).is_dir())

    def test_a_signal_before_publication_lets_it_publish_then_cancels(self):
        with (self.root / ".queue.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            process = self.start()
            staging = self.root / "queued"
            self.wait_for(
                lambda: any((p / "manifest.json").exists() for p in staging.glob(".staging-*")),
                "the submission to reach the queue lock",
            )
            process.send_signal(signal.SIGTERM)
            time.sleep(0.2)
            fcntl.flock(lock, fcntl.LOCK_UN)
        code, payload, _ = self.finish(process)
        self.assertEqual(code, 4)
        self.assert_cancelled_by_signal(payload)
        self.assertEqual(payload["cancelled_from"], "queued")

    def test_a_signal_after_publication_before_the_wait_cancels(self):
        process = self.start("pause-before-wait")
        self.assertEqual(process.stderr.readline().strip(), "published")
        process.send_signal(signal.SIGHUP)
        time.sleep(0.2)
        process.stdin.write("\n")
        process.stdin.flush()
        code, payload, _ = self.finish(process)
        self.assertEqual(code, 4)
        self.assert_cancelled_by_signal(payload)
        self.assertTrue(payload["wait_interrupted"])

    def test_a_signal_during_the_wait_cancels_and_a_second_is_absorbed(self):
        process = self.start()
        self.wait_for(lambda: self.queued_ids(), "the job to be published")
        time.sleep(0.3)
        process.send_signal(signal.SIGINT)
        process.send_signal(signal.SIGTERM)
        started = time.monotonic()
        code, payload, _ = self.finish(process)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(code, 4)
        self.assert_cancelled_by_signal(payload)

    def test_a_signal_after_the_job_finished_exits_128_plus_the_signal(self):
        process = self.start("finish-then-signal")
        code, payload, _ = self.finish(process)
        self.assertEqual(code, 128 + signal.SIGTERM)
        self.assertEqual((payload["cancel_result"], payload["state"]), ("already-finished", "done"))

    def test_without_the_flag_signals_keep_their_default_effect(self):
        process = self.start(flag=False)
        self.wait_for(lambda: self.queued_ids(), "the job to be published")
        time.sleep(0.3)
        process.send_signal(signal.SIGTERM)
        code, _payload, _ = self.finish(process)
        self.assertEqual(code, -signal.SIGTERM)
        self.assertEqual(len(self.queued_ids()), 1)


if __name__ == "__main__":
    unittest.main()
