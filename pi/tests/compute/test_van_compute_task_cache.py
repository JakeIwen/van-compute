"""Protocol surface and shared store behavior of the opt-in task cache."""

from __future__ import annotations

import collections
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from van_compute import broker, protocol, queue, task_cache

REPO = Path(__file__).resolve().parents[3]
FakeUsage = collections.namedtuple("FakeUsage", "total used free")

TASK = {
    "name": "suite",
    "profile": "python-script",
    "source_paths": ["tools/run.py"],
    "minimum_inputs": 0,
    "maximum_inputs": 0,
    "argv": ["{source:tools/run.py}"],
    "outputs": [],
}


def load_task(directory: Path, **extra):
    (directory / protocol.REPO_MANIFEST).write_text(
        json.dumps({"schema_version": 1, "tasks": [{**TASK, **extra}]}),
        encoding="utf-8",
    )
    return protocol.load_repo_tasks(directory)["suite"]


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_uncached_task_keeps_its_execution_specification(self):
        task = load_task(self.directory)
        self.assertIsNone(task.cache)
        self.assertNotIn("cache", protocol.task_execution(task))
        self.assertNotIn("cache", protocol.validate_execution(protocol.task_execution(task)))

    def test_declared_cache_travels_in_the_execution_and_validates(self):
        task = load_task(self.directory, cache="obd-things-tests")
        execution = protocol.task_execution(task)
        self.assertEqual(execution["cache"], "obd-things-tests")
        self.assertEqual(protocol.validate_execution(execution)["cache"], "obd-things-tests")

    def test_bad_cache_names_are_rejected_in_declarations_and_executions(self):
        for bad in ("", "Upper", "a/b", "../x", ".hidden", 5, None, "x" * 65):
            with self.subTest(bad=bad):
                with self.assertRaises(protocol.ProtocolError):
                    load_task(self.directory, cache=bad)
                execution = protocol.task_execution(load_task(self.directory))
                with self.assertRaises(protocol.ProtocolError):
                    protocol.validate_execution({**execution, "cache": bad})

    def test_namespace_is_stable_slugged_and_distinct_per_source_root(self):
        first = protocol.task_cache_namespace("/home/pi/dev/Obd Things")
        self.assertEqual(first, protocol.task_cache_namespace("/home/pi/dev/Obd Things"))
        self.assertRegex(first, r"^obd-things-[0-9a-f]{16}$")
        other = protocol.task_cache_namespace("/home/other/Obd Things")
        self.assertNotEqual(first, other)
        for bad in (None, 7, "relative/path"):
            with self.assertRaises(protocol.ProtocolError):
                protocol.task_cache_namespace(bad)

    def test_tasks_verb_lists_the_declared_cache(self):
        load_task(self.directory, cache="warm")
        with mock.patch("builtins.print") as printed:
            queue.main(["tasks", "--source-root", str(self.directory)])
        listing = json.loads(printed.call_args.args[0])["tasks"]
        suite = next(item for item in listing if item["name"] == "suite")
        self.assertEqual(suite["cache"], "warm")
        load_task(self.directory)
        with mock.patch("builtins.print") as printed:
            queue.main(["tasks", "--source-root", str(self.directory)])
        suite = next(
            item for item in json.loads(printed.call_args.args[0])["tasks"]
            if item["name"] == "suite"
        )
        self.assertNotIn("cache", suite)


HOLDER = """
import sys, time
from pathlib import Path
from van_compute import task_cache
root, namespace, name, ready, release = sys.argv[1:6]
store = task_cache.TaskCacheStore(Path(root), 0)
with store.use(task_cache.CacheRequest(namespace, name)):
    Path(ready).write_text("held")
    deadline = time.time() + 30
    while not Path(release).exists() and time.time() < deadline:
        time.sleep(0.02)
"""

HAMMER = """
import sys
from pathlib import Path
from van_compute import task_cache
root, tag = sys.argv[1:3]
store = task_cache.TaskCacheStore(Path(root), 0)
request = task_cache.CacheRequest("ns", "shared")
for index in range(300):
    with store.use(request) as path:
        marker = Path(path) / f"{tag}-{index}"
        marker.write_text("x")
        assert marker.read_text() == "x", "cache vanished while shared lock was held"
"""

PRUNER = """
import sys
from pathlib import Path
from van_compute import task_cache
store = task_cache.TaskCacheStore(Path(sys.argv[1]), 0)
for index in range(600):
    store.prune(0)
    store.clear()
"""


def run_python(code, *arguments):
    environment = {**os.environ, "PYTHONPATH": str(REPO)}
    return subprocess.Popen(
        [sys.executable, "-c", code, *arguments],
        env={k: v for k, v in environment.items() if k != "PYTHONHOME"},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve() / "caches"
        self.store = task_cache.TaskCacheStore(self.root, 10 * 1024**3)
        self.processes = []

    def tearDown(self):
        for process in self.processes:
            process.kill()
            process.communicate()
        self.fix_modes()
        self.temporary.cleanup()

    def fix_modes(self):
        for path in sorted(self.root.rglob("*"), key=lambda p: len(p.parts)):
            if path.is_dir() and not path.is_symlink():
                path.chmod(0o700)

    def request(self, namespace="ns", name="cache"):
        return task_cache.CacheRequest(namespace, name)

    def fill(self, namespace, name, size, age):
        with self.store.use(self.request(namespace, name)) as path:
            (Path(path) / "blob").write_bytes(os.urandom(size))
        lock = self.root / namespace / f"{name}.lock"
        stamp = time.time() - age
        os.utime(lock, (stamp, stamp))

    def hold(self, namespace="ns", name="cache"):
        ready = self.root.parent / f"ready-{namespace}-{name}"
        release = self.root.parent / f"release-{namespace}-{name}"
        process = run_python(HOLDER, str(self.root), namespace, name, str(ready), str(release))
        self.processes.append(process)
        deadline = time.time() + 20
        while not ready.exists():
            if time.time() > deadline or process.poll() is not None:
                self.fail(f"holder did not start: {process.communicate()}")
            time.sleep(0.02)
        return release

    def test_second_use_sees_files_the_first_left(self):
        with self.store.use(self.request()) as first:
            (Path(first) / "stamp").write_text("one")
        with self.store.use(self.request()) as second:
            self.assertEqual((Path(second) / "stamp").read_text(), "one")
            self.assertEqual(Path(second), self.root / "ns" / "cache")
            self.assertEqual(stat_mode(second), 0o700)

    def test_same_name_in_two_namespaces_is_isolated(self):
        with self.store.use(self.request("repo-a")) as first:
            (Path(first) / "stamp").write_text("a")
        with self.store.use(self.request("repo-b")) as second:
            self.assertFalse((Path(second) / "stamp").exists())

    def test_prune_evicts_least_recently_used_to_the_cap(self):
        megabyte = 1024 * 1024
        self.fill("ns", "oldest", megabyte, age=300)
        self.fill("ns", "middle", megabyte, age=200)
        self.fill("ns", "newest", megabyte, age=100)
        self.store.max_bytes = int(2.5 * megabyte)
        report = self.store.prune(0)
        self.assertEqual(report.evicted, ("ns/oldest",))
        self.assertFalse((self.root / "ns" / "oldest").exists())
        self.assertTrue((self.root / "ns" / "middle" / "blob").exists())
        self.assertTrue((self.root / "ns" / "newest" / "blob").exists())
        self.assertTrue((self.root / "ns" / "oldest.lock").exists())

    def test_prune_evicts_until_the_free_space_target_is_met(self):
        for index, name in enumerate(("a", "b", "c")):
            self.fill("ns", name, 1024, age=300 - index * 100)

        def usage(_path):
            remaining = sum(1 for item in (self.root / "ns").iterdir() if item.is_dir())
            return FakeUsage(1000, 0, 1000 - 100 * remaining)

        with mock.patch.object(task_cache.shutil, "disk_usage", side_effect=usage):
            report = self.store.prune(850)
        self.assertEqual(report.evicted, ("ns/a", "ns/b"))

    def test_busy_cache_is_counted_but_never_evicted_by_prune_or_clear(self):
        self.fill("ns", "busy", 4096, age=500)
        self.fill("ns", "idle", 4096, age=100)
        release = self.hold("ns", "busy")
        self.store.max_bytes = 0
        report = self.store.prune(0)
        self.assertEqual(report.evicted, ("ns/idle",))
        self.assertTrue((self.root / "ns" / "busy" / "blob").exists())
        listing = {(i.namespace, i.name): i for i in self.store.caches()}
        self.assertTrue(listing[("ns", "busy")].in_use)
        cleared = self.store.clear()
        self.assertEqual(cleared["busy"], ["ns/busy"])
        release.write_text("go")
        self.processes[-1].communicate(timeout=20)
        self.assertEqual(self.store.clear()["cleared"], ["ns/busy"])
        self.assertFalse((self.root / "ns" / "busy").exists())

    def test_idle_unmeasurable_cache_is_evicted_and_removed_fully(self):
        with self.store.use(self.request("ns", "locked")) as path:
            hidden = Path(path) / "private"
            (hidden / "deeper").mkdir(parents=True)
            (hidden / "deeper" / "file").write_text("x")
            hidden.chmod(0)
        self.assertIsNone(self.store.caches()[0].size_bytes)
        report = self.store.prune(0)  # huge cap: only unmeasurable caches go
        self.assertEqual(report.evicted, ("ns/locked",))
        self.assertFalse((self.root / "ns" / "locked").exists())

    def test_symlinked_cache_directory_is_repaired_only_when_idle(self):
        victim = self.root.parent / "victim"
        victim.mkdir()
        (victim / "keep").write_text("keep")
        with self.store.use(self.request()):
            pass
        cache = self.root / "ns" / "cache"
        release = self.hold()
        cache.rename(self.root / "ns" / "moved")
        cache.symlink_to(victim)
        with self.assertRaises(task_cache.TaskCacheError):
            with self.store.use(self.request()):
                self.fail("a swapped cache directory must not be handed out")
        self.assertTrue(cache.is_symlink())
        release.write_text("go")
        self.processes[-1].communicate(timeout=20)
        with self.store.use(self.request()) as repaired:
            self.assertFalse(Path(repaired).is_symlink())
            self.assertTrue(Path(repaired).is_dir())
        self.assertEqual((victim / "keep").read_text(), "keep")

    def test_separate_processes_contend_with_prune_and_clear(self):
        pruner = run_python(PRUNER, str(self.root))
        hammers = [run_python(HAMMER, str(self.root), f"w{i}") for i in range(3)]
        self.processes.extend([pruner, *hammers])
        for process in (*hammers, pruner):
            _out, error = process.communicate(timeout=120)
            self.assertEqual(process.returncode, 0, error)

    def test_command_line_lists_and_clears(self):
        self.fill("ns", "one", 1024, age=10)
        base = [sys.executable, "-m", "van_compute.task_cache", "--root", str(self.root)]
        environment = {"PYTHONPATH": str(REPO), "PATH": os.environ["PATH"]}
        listed = subprocess.run(
            [*base, "list"], env=environment, capture_output=True, text=True, check=True
        )
        row = json.loads(listed.stdout)[0]
        self.assertEqual((row["namespace"], row["name"], row["in_use"]), ("ns", "one", False))
        cleared = subprocess.run(
            [*base, "clear", "ns/one"], env=environment, capture_output=True, text=True, check=True
        )
        self.assertEqual(json.loads(cleared.stdout)["cleared"], ["ns/one"])
        self.assertFalse((self.root / "ns" / "one").exists())

    def test_contended_prune_does_not_leak_descriptors(self):
        self.fill("ns", "one", 1024, age=10)
        lock = os.open(self.root / task_cache.PRUNE_LOCK_NAME, os.O_RDWR | os.O_CREAT)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX)
            before = len(os.listdir("/dev/fd"))
            for _ in range(30):
                self.assertTrue(self.store.prune(0).skipped)
            self.assertEqual(len(os.listdir("/dev/fd")), before)
        finally:
            os.close(lock)

    def make_deep_tree(self, namespace, name, depth=1500):
        with self.store.use(self.request(namespace, name)) as path:
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        for _ in range(depth):
            os.mkdir("d", 0o700, dir_fd=descriptor)
            child = os.open("d", os.O_RDONLY | os.O_DIRECTORY, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        os.close(descriptor)

    def test_a_very_deep_tree_is_listed_pruned_and_cleared(self):
        self.make_deep_tree("ns", "pruned")
        self.make_deep_tree("ns", "cleared")
        self.assertTrue(all(i.size_bytes is not None for i in self.store.caches()))
        self.assertEqual(self.store.clear("ns", "cleared")["cleared"], ["ns/cleared"])
        self.assertFalse((self.root / "ns" / "cleared").exists())
        # Directories can report zero blocks (APFS), so force eviction by free space.
        with mock.patch.object(
            task_cache.shutil, "disk_usage", return_value=FakeUsage(1, 1, 0)
        ):
            self.assertEqual(self.store.prune(1).evicted, ("ns/pruned",))
        self.assertFalse((self.root / "ns" / "pruned").exists())

    def test_one_failing_cache_does_not_stop_eviction_of_others(self):
        self.fill("ns", "bad", 1024, age=300)
        self.fill("ns", "good", 1024, age=200)
        real = task_cache._remove_entry

        def flaky(namespace_fd, name, device):
            if name == "bad":
                raise RecursionError("poisoned")
            return real(namespace_fd, name, device)

        self.store.max_bytes = 0
        with mock.patch.object(task_cache, "_remove_entry", side_effect=flaky):
            report = self.store.prune(0)
            cleared = self.store.clear()
        self.assertEqual(report.failed, ("ns/bad",))
        self.assertEqual(report.evicted, ("ns/good",))
        self.assertEqual(cleared["failed"], ["ns/bad"])

    def test_busy_unmeasurable_cache_keeps_idle_caches_evictable(self):
        self.fill("ns", "idle", 4096, age=100)
        self.fill("ns", "busy", 1024, age=500)
        release = self.hold("ns", "busy")
        hidden = self.root / "ns" / "busy" / "private"
        hidden.mkdir()
        hidden.chmod(0)
        self.assertIsNone(
            next(i for i in self.store.caches() if i.name == "busy").size_bytes
        )
        report = self.store.prune(0)  # cap is huge: only the unmeasurable one is over
        self.assertEqual(report.evicted, ("ns/idle",))
        self.assertTrue(hidden.exists())
        release.write_text("go")

    def test_pi_default_cache_root_follows_the_selected_queue_root(self):
        parsed = broker.build_parser().parse_args(["--root", "/srv/queue"])
        self.assertEqual(
            broker._validate_args(parsed).task_cache_root, Path("/srv/queue/task-caches")
        )
        parsed = broker.build_parser().parse_args(
            ["--root", "/srv/queue", "--task-cache-root", "/srv/elsewhere"]
        )
        self.assertEqual(
            broker._validate_args(parsed).task_cache_root, Path("/srv/elsewhere")
        )


def stat_mode(path):
    return os.stat(path).st_mode & 0o777


if __name__ == "__main__":
    unittest.main()
