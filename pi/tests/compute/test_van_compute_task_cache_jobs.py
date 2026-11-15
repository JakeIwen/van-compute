"""Cached jobs end to end: Pi broker, Mac worker and the Mac sandbox profile."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from macbook.scripts.van_compute_installer import constants as installer_constants
from van_compute import broker, engine, protocol, queue, registry_proxy, task_cache, worker
from pi.tests.compute.van_compute_test_fixtures import (
    BrokerHarness, DECLARATION, GOOD_HEALTH,
)

REPO = Path(__file__).resolve().parents[3]

STAMP_SCRIPT = """import os
from pathlib import Path

cache = Path(os.environ["XDG_CACHE_HOME"])
stamp = cache / "stamp"
prior = stamp.read_text() if stamp.exists() else ""
stamp.write_text(prior + "x")
print("prior=" + prior)
print("cache=" + str(cache))
"""


def stamp_task(name, **extra):
    return {
        "name": name,
        "profile": "python-script",
        "source_paths": ["tools/stamp.py"],
        "minimum_inputs": 0,
        "maximum_inputs": 0,
        "argv": ["{source:tools/stamp.py}"],
        "outputs": [],
        **extra,
    }


class PiBrokerCacheTests(BrokerHarness, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.store_root = Path(self.temporary.name).resolve() / "task-caches"
        self.args.task_cache_root = self.store_root
        self.args.task_cache_max_bytes = 1024**3
        (self.source / "tools" / "stamp.py").write_text(STAMP_SCRIPT, encoding="utf-8")
        declaration = {
            **DECLARATION,
            "tasks": [
                *DECLARATION["tasks"],
                stamp_task("cached-stamp", cache="warm"),
                stamp_task("other-cache-stamp", cache="cold"),
                stamp_task("uncached-stamp"),
            ],
        }
        (self.source / protocol.REPO_MANIFEST).write_text(
            json.dumps(declaration), encoding="utf-8"
        )
        self.bound = []

    def sandbox(self, executable, command, **paths):
        # Stand-in for bwrap: bwrap would bind this path at /job/cache and
        # export it as XDG_CACHE_HOME, so do the same with env.
        self.bound.append(paths["cache_root"])
        return [
            "/usr/bin/env",
            f"XDG_CACHE_HOME={paths['cache_root']}",
            *self.no_sandbox(executable, command, **paths),
        ]

    def run_once(self):
        with mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ), mock.patch.object(broker, "bubblewrap_command", side_effect=self.sandbox):
            return broker.run_once(self.args, health_reader=lambda: GOOD_HEALTH)

    def stdout_of(self, task):
        submitted = self.submit(task)
        result = self.run_once()
        self.assertEqual(result["action"], "executed", result)
        output = (self.root / "done" / submitted["id"] / "result" / "stdout.txt")
        return output.read_text(encoding="utf-8") if output.exists() else ""

    def test_cached_job_sees_the_stamp_a_previous_job_left(self):
        first = self.stdout_of("cached-stamp")
        second = self.stdout_of("cached-stamp")
        self.assertIn("prior=\n", first)
        self.assertIn("prior=x\n", second)
        namespace = protocol.task_cache_namespace(str(self.source.resolve()))
        expected = self.store_root / namespace / "warm"
        self.assertEqual(self.bound, [expected, expected])

    def test_uncached_and_differently_named_caches_do_not_share(self):
        self.stdout_of("cached-stamp")
        self.assertIn("prior=\n", self.stdout_of("other-cache-stamp"))
        self.assertIn("prior=\n", self.stdout_of("uncached-stamp"))
        self.assertIn("prior=\n", self.stdout_of("uncached-stamp"))
        self.assertTrue(str(self.bound[-1]).startswith(str(self.work.resolve())))

    def test_bubblewrap_binds_the_store_path_at_job_cache(self):
        self.stdout_of("cached-stamp")
        cache = self.bound[0]
        wrapped = broker.bubblewrap_command(
            sys.executable, ["/bin/true"],
            source_root=Path("/s"), inputs_root=Path("/i"), result_root=Path("/r"),
            home_root=Path("/h"), temporary_root=Path("/t"), cache_root=cache,
            job_id="job",
        )
        index = wrapped.index(str(cache))
        self.assertEqual(wrapped[index - 1], "--bind")
        self.assertEqual(wrapped[index + 1], "/job/cache")

    def test_cached_job_fails_closed_when_the_host_has_no_cache_root(self):
        self.args.task_cache_root = None
        submitted = self.submit("cached-stamp")
        self.run_once()
        manifest = queue.manifest_for(queue.safe_root(self.root), submitted["id"])
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(self.bound, [])

    def test_job_leaves_cache_unlocked_and_prunes_between_jobs(self):
        self.stdout_of("cached-stamp")
        listing = task_cache.TaskCacheStore(self.store_root, 0).caches()
        self.assertEqual([(i.name, i.in_use) for i in listing], [("warm", False)])
        self.args.task_cache_max_bytes = 1  # any content now exceeds the cap
        self.stdout_of("uncached-stamp")
        namespace = protocol.task_cache_namespace(str(self.source.resolve()))
        self.assertFalse((self.store_root / namespace / "warm").exists())


def stamp_manifest(job_id, source_root, cache=None):
    execution = {
        "profile": "python-script",
        "family": "python",
        "argv": ["{source:tools/stamp.py}"],
        "outputs": [],
        "datasets": [],
        "minimum_inputs": 0,
        "maximum_inputs": 0,
        "input_values": False,
    }
    if cache is not None:
        execution["cache"] = cache
    return {
        "id": job_id,
        "task": "stamp",
        "arguments": [],
        "inputs": [],
        "source_root": str(source_root),
        "execution": execution,
    }


class MacWorkerCacheTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.store = task_cache.TaskCacheStore(self.base / "task-caches", 1024**3)
        self.repo = self.base / "obd-things"
        self.jobs = 0

    def tearDown(self):
        self.temporary.cleanup()

    def run_job(self, cache=None, *, store="default", sandbox=None):
        self.jobs += 1
        job = self.base / f"job-{self.jobs}"
        source = job / "source"
        (source / "tools").mkdir(parents=True)
        (source / "tools" / "stamp.py").write_text(STAMP_SCRIPT, encoding="utf-8")
        result = job / "result"
        arguments = dict(
            source_root=source, input_paths=[], input_values=[], result_root=result,
            python=sys.executable, timeout=30, nice=0, maximum_file_size=1024 * 1024,
            allow_unsandboxed_dynamic=True,
            task_cache_store=self.store if store == "default" else store,
        )
        with mock.patch.object(worker, "process_group_resources", return_value=(0, 1)):
            exit_code, execution = worker.execute_job(
                stamp_manifest(f"20260101T00000{self.jobs}Z-0000000{self.jobs}", self.repo, cache),
                **arguments,
            )
        stdout = (result / "stdout.txt").read_text(encoding="utf-8")
        return exit_code, execution, stdout

    def test_cached_job_sees_the_stamp_a_previous_job_left(self):
        _code, first_execution, first = self.run_job("warm")
        code, execution, second = self.run_job("warm")
        self.assertEqual(code, 0)
        self.assertIn("prior=\n", first)
        self.assertIn("prior=x\n", second)
        self.assertEqual(execution["task_cache"], "warm")
        self.assertEqual(first_execution["task_cache"], "warm")

    def test_uncached_job_is_private_and_unlabelled(self):
        self.run_job("warm")
        _code, execution, stdout = self.run_job(None)
        self.assertIn("prior=\n", stdout)
        self.assertNotIn("task_cache", execution)

    def test_result_text_hides_the_persistent_cache_path(self):
        _code, _execution, stdout = self.run_job("warm")
        self.assertIn("cache={task-cache}/", stdout)
        self.assertNotIn(str(self.store.root), stdout)

    def test_sandbox_receives_the_store_path_for_cached_jobs_only(self):
        captured = []

        def passthrough(command, **kwargs):
            captured.append(kwargs["cache_root"])
            return list(command)

        with mock.patch.object(worker, "sandbox_command", side_effect=passthrough):
            self.run_job("warm")
            self.run_job(None)
        namespace = protocol.task_cache_namespace(str(self.repo))
        self.assertEqual(captured[0], self.store.root / namespace / "warm")
        self.assertEqual(captured[1], self.base / "job-2" / "cache")

    def test_cached_job_without_a_store_fails_closed(self):
        with self.assertRaises(task_cache.TaskCacheError):
            self.run_job("warm", store=None)


@unittest.skipUnless(
    sys.platform == "darwin" and Path("/usr/bin/sandbox-exec").exists(),
    "the Seatbelt profile only exists on macOS",
)
class MacSandboxProfileTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.profile = self.base / "sandbox.sb"
        self.profile.write_text(installer_constants.SANDBOX_PROFILE, encoding="utf-8")
        self.job = self.base / "job"
        self.selected = self.base / "cache-selected"
        self.other = self.base / "cache-other"
        for directory in (self.job, self.selected, self.other):
            directory.mkdir()
        self.sentinel = self.base / "sentinel"
        self.sentinel.write_text("private", encoding="utf-8")
        (self.other / "probe.txt").write_text("other", encoding="utf-8")
        self.link = self.selected / "link"
        self.link.symlink_to(self.sentinel)
        trial = subprocess.run(
            ["/usr/bin/sandbox-exec", "-p", "(version 1)(allow default)", "/usr/bin/true"],
            capture_output=True,
        )
        if trial.returncode != 0:
            self.skipTest("sandbox-exec cannot be applied from this process")
        # The profile applies only with a live proxy port to grant.
        proxy = registry_proxy.RegistryProxy(log=lambda _message: None)
        proxy.start()
        self.addCleanup(proxy.stop)
        self.proxy_port = proxy.port

    def tearDown(self):
        self.temporary.cleanup()

    def sandboxed(self, command, cache_root="selected"):
        wrapped = engine._mac_sandbox_command(
            command,
            profile=self.profile,
            job_root=self.job,
            source_root=self.job / "source",
            result_root=self.job / "result",
            environment={"HOME": str(self.job), "TMPDIR": str(self.job) + "/"},
            datasets={},
            worker_root=REPO,
            error_type=RuntimeError,
            cache_root=self.selected if cache_root == "selected" else cache_root,
            proxy_port=self.proxy_port,
        )
        # Jobs get a clean environment; the developer's HOME is not readable.
        environment = {"HOME": str(self.job), "TMPDIR": str(self.job) + "/", "PATH": "/usr/bin:/bin"}
        return subprocess.run(wrapped, capture_output=True, text=True, cwd=self.job, env=environment)

    def test_selected_cache_is_writable_and_siblings_are_not(self):
        script = (
            'echo ok > "$1/probe" && cat "$1/probe" || exit 10; '
            'echo x > "$2/probe" 2>/dev/null && exit 11; '
            'cat "$2/probe.txt" >/dev/null 2>&1 && exit 12; '
            'echo x > "$3" 2>/dev/null && exit 13; '
            'echo x > "$4" 2>/dev/null && exit 14; exit 0'
        )
        completed = self.sandboxed([
            "/bin/sh", "-c", script, "sh",
            str(self.selected), str(self.other), str(self.sentinel), str(self.link),
        ])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.sentinel.read_text(), "private")
        self.assertEqual((self.other / "probe.txt").read_text(), "other")

    def test_uncached_default_keeps_the_cache_inside_the_job_root(self):
        (self.job / "cache").mkdir()
        completed = self.sandboxed(
            ["/bin/sh", "-c", 'echo ok > "$1/probe"', "sh", str(self.job / "cache")],
            cache_root=None,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        completed = self.sandboxed(
            ["/bin/sh", "-c", 'echo x > "$1/probe"', "sh", str(self.selected)],
            cache_root=None,
        )
        self.assertNotEqual(completed.returncode, 0)

    def test_system_git_shim_runs_under_the_profile(self):
        # /usr/bin/git is an xcrun shim; repo tests build fixture repositories.
        selected = subprocess.run(["/usr/bin/xcode-select", "-p"], capture_output=True, text=True)
        if selected.stdout.strip() != "/Library/Developer/CommandLineTools":
            self.skipTest("needs the Command Line Tools as the selected developer directory")
        script = (
            'cd "$1" && /usr/bin/git init -q fixture && cd fixture && '
            '/usr/bin/git -c user.name=t -c user.email=t@example.invalid '
            'commit -q --allow-empty -m probe && /usr/bin/git rev-list --count HEAD'
        )
        completed = self.sandboxed(["/bin/sh", "-c", script, "sh", str(self.job)])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "1")

    def test_installer_probe_script_passes_under_the_profile(self):
        # The shipped profile reads /opt/homebrew but not an arbitrary venv.
        interpreter = os.path.realpath(getattr(sys, "_base_executable", sys.executable))
        if not interpreter.startswith("/opt/homebrew/"):
            self.skipTest("needs a Homebrew Python the profile can execute")
        completed = self.sandboxed([
            interpreter, "-c", installer_constants.SANDBOX_CACHE_PROBE,
            str(self.selected), str(self.other), str(self.sentinel), str(self.link),
        ])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self.sentinel.read_text(), "private")


if __name__ == "__main__":
    unittest.main()
