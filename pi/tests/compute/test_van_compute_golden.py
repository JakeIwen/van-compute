"""Byte-level snapshots of the pre-refactor compute protocol.

Regenerate deliberately, from the repository root::

    VAN_COMPUTE_GOLDEN_UPDATE=1 python -m unittest pi.tests.compute.test_van_compute_golden

Normal runs NEVER create missing goldens. Wire JSON and disk JSON are compared
without parsing/re-serializing them. ``observation`` files are explicitly test
adapters for Python return values, binary transfers, and non-JSON errors; the
queue currently returns empty stdout / plain-text stderr / status 2 on errors.
Metrics use Flask's actual jsonify boundary, as the dashboard routes do.

Determinism is injected at environmental boundaries, not into serializers:
* module-local datetime proxies advance UTC by one second per now() call;
* module-local monotonic proxies advance by 0.125 seconds per call;
* queue/broker secrets.token_hex share an increasing, length-preserving counter;
* queue os.fstat supplies fixed dev/ino/mtime_ns/ctime_ns ONLY for registered
  fixture inputs (mode/size and all source identity/hash checks remain real);
* real children run and are reaped, but wait outcomes receive fixed rusage and
  watchdog measurements; resource-limit/interrupted flags are injected here;
* disk-space readers return 16 GiB, hostname is golden-mac, and umask is 077;
* gzip's clock and tar's OS ownership/name lookups are fixed; the fixture child
  sets its output modes and mtimes itself. The real archive writers run unchanged.
* Path.write_text is a delegating observer of every execution.json write;
* only sandbox command construction is bypassed (the established test fakes).

The ONLY byte normalization rules are the explicit re.escape patterns in
normalization_rules(): this test's absolute temporary root -> <TMP> and the
running interpreter -> <PYTHON>. No keys, hashes, durations or numeric text are
removed or rewritten. Real process polling, sandbox enforcement and SSH are
not tested here; the existing execution/security suites cover those boundaries.
"""

from __future__ import annotations

import argparse
import base64
from contextlib import ExitStack, contextmanager, redirect_stderr
import datetime as dt
import difflib
import gzip
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import sys
import tarfile
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from flask import Flask, jsonify

# The ONE import/location indirection point for the subsequent package move.
from van_compute import queue
from van_compute import protocol
from van_compute import broker
from van_compute import metrics
from van_compute import frontend
from van_compute import worker
from pi.tests.compute import test_van_compute as queue_tests
from pi.tests.compute import test_van_compute_broker as broker_tests
EXAMPLE = Path(queue.__file__).resolve().parent / "configs/van-compute-obd.example.json"

GOLDEN_ROOT = Path(__file__).with_name("golden")
EPOCH = dt.datetime(2026, 7, 22, 12, 0, tzinfo=dt.timezone.utc)
FREE_BYTES = 16 * 1024**3
MAX_RESULT_BYTES = 1024 * 1024

# Same little offline workload on both hosts, with file/directory/nonzero variants.
# JSON stays path-free; worker stdout exercises real private-path scrubbing.
TASK_SCRIPT = """import argparse
import json
import os
from pathlib import Path
p = argparse.ArgumentParser()
p.add_argument('capture')
p.add_argument('--output', required=True)
p.add_argument('--directory', action='store_true')
p.add_argument('--fail', action='store_true')
a = p.parse_args()
if a.fail:
    print('task rejected input')
    raise SystemExit(7)
out = Path(a.output)
if a.directory:
    out.mkdir(mode=0o700)
    leaf = out / 'report.json'
else:
    leaf = out
leaf.write_text(json.dumps({'bytes': len(Path(a.capture).read_bytes()), 'ratio': 1.25}, indent=2, sort_keys=True) + '\\n')
os.chmod(leaf, 0o600)
os.utime(leaf, (1784721600, 1784721600))
if a.directory:
    os.chmod(out, 0o700)
    os.utime(out, (1784721600, 1784721600))
print('offline analysis complete')
if os.environ.get('XDG_CACHE_HOME'):
    print('source=' + str(Path.cwd()))
"""


def proxy(module, **overrides):
    """Avoid monkeypatching shared stdlib modules used by subprocess/threading."""
    return SimpleNamespace(**(vars(module) | overrides))


class StatView:
    def __init__(self, original, **overrides):
        self.original = original
        self.overrides = overrides

    def __getattr__(self, name):
        return self.overrides.get(name, getattr(self.original, name))


def normalization_rules(base):
    # Literal escaped absolute paths only; no wildcard path/numeric scrubbing.
    return (
        (re.compile(re.escape(str(base).encode("utf-8"))), b"<TMP>"),
        (re.compile(re.escape(sys.executable.encode("utf-8"))), b"<PYTHON>"),
    )


class InProcessRemote(worker.RemoteQueue):
    """Existing Remote fake pattern, retaining the REAL worker RPC argv builders.

    Override only the two SSH transport methods. Every command reaches the real
    queue CLI, and all upload bytes and finish arguments are recorded.
    """

    def __init__(self, case, prefix):
        super().__init__("unused.invalid", "unused-cli", "golden-mac.00")
        self.case = case
        self.prefix = prefix
        self.calls = []
        self.uploads = []

    def json_command(self, *arguments, input_file=None):
        data = b"" if input_file is None else input_file.read()
        self.calls.append({"argv": list(arguments), "stdin_base64": base64.b64encode(data).decode()})
        number = len(self.calls)
        if input_file is not None:
            relative = arguments[arguments.index("--path") + 1]
            self.uploads.append((relative, data))
            if relative.endswith(".json"):
                self.case.golden(f"{self.prefix}/upload-{relative}", data)
        code, stdout, stderr = self.case.invoke(queue, list(arguments), data)
        self.case.assertEqual((code, stderr), (0, b""))
        self.case.golden(f"{self.prefix}/rpc-{number:02d}-{arguments[1]}.json", stdout)
        return json.loads(stdout)

    def stream_to_file(self, arguments, destination):
        self.calls.append({"argv": list(arguments), "stdin_base64": ""})
        code, stdout, stderr = self.case.invoke(queue, list(arguments))
        self.case.assertEqual((code, stderr), (0, b""))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(stdout)
        # Source tar is deterministic, but store its digest rather than a large
        # base64 tar (the immutable source records pin each file independently).
        self.calls[-1]["stdout_bytes"] = len(stdout)
        self.calls[-1]["stdout_sha256"] = hashlib.sha256(stdout).hexdigest()


class ComputeGoldenTests(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.scenario = self._testMethodName.removeprefix("test_")
        self.seen = set()
        self.update = os.environ.get("VAN_COMPUTE_GOLDEN_UPDATE") == "1"
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        temporary = self.stack.enter_context(tempfile.TemporaryDirectory(prefix="compute-golden-"))
        self.base = Path(temporary).resolve()
        self.root = self.base / "queue"
        self.source = self.base / "source"
        self.source.mkdir()
        self.inputs = {}
        self.wall_ticks = itertools.count()
        self.monotonic_ticks = itertools.count()
        self.tokens = itertools.count(1)
        case = self

        class ClockDateTime(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                value = EPOCH + dt.timedelta(seconds=next(case.wall_ticks))
                return value.astimezone(tz) if tz else value.replace(tzinfo=None)

        fixed_dt = proxy(dt, datetime=ClockDateTime)
        fixed_time = proxy(time, monotonic=lambda: 100 + next(self.monotonic_ticks) * 0.125)
        fixed_secrets = proxy(secrets, token_hex=lambda n: f"{next(self.tokens):0{2*n}x}")
        for module in (queue, broker, worker):
            self.stack.enter_context(mock.patch.object(module, "dt", fixed_dt))
            self.stack.enter_context(mock.patch.object(module, "time", fixed_time))
        for module in (queue, broker):
            self.stack.enter_context(mock.patch.object(module, "secrets", fixed_secrets))
        self.stack.enter_context(mock.patch.object(queue, "os", proxy(os, fstat=self.input_fstat)))
        self.stack.enter_context(mock.patch.object(worker, "socket", proxy(socket, gethostname=lambda: "golden-mac")))
        fixed_disk = lambda _path: SimpleNamespace(total=FREE_BYTES * 2, used=FREE_BYTES, free=FREE_BYTES)
        self.stack.enter_context(mock.patch.object(broker, "shutil", proxy(shutil, disk_usage=fixed_disk)))
        self.stack.enter_context(mock.patch.object(gzip, "time", proxy(time, time=lambda: EPOCH.timestamp())))
        self.stack.enter_context(mock.patch.object(tarfile, "os", proxy(os, lstat=self.archive_lstat)))
        self.stack.enter_context(mock.patch.object(tarfile, "pwd", SimpleNamespace(getpwuid=lambda _uid: ("golden",))))
        self.stack.enter_context(mock.patch.object(tarfile, "grp", SimpleNamespace(getgrgid=lambda _gid: ("golden",))))
        old_umask = os.umask(0o077)
        self.addCleanup(os.umask, old_umask)
        self.capture = self.make_input("capture.log", b"(1.250000) can0 123#01020304\n")
        self.other = self.make_input("baseline.log", b"(1.000000) can0 123#05060708\n")
        self.configure_tasks()

    def input_fstat(self, descriptor):
        info = os.fstat(descriptor)
        index = self.inputs.get((info.st_dev, info.st_ino))
        if index is None:
            return info
        return StatView(info, st_dev=7, st_ino=1000 + index,
                        st_mtime_ns=1784721600000000000, st_ctime_ns=1784721600000000000)

    def archive_lstat(self, path, *args, **kwargs):
        info = os.lstat(path, *args, **kwargs)
        if Path(path).is_relative_to(self.base):
            return StatView(info, st_uid=501, st_gid=20)
        return info

    def make_input(self, name, data):
        path = self.base / name
        path.write_bytes(data)
        info = path.stat()
        self.inputs[(info.st_dev, info.st_ino)] = len(self.inputs)
        return path

    def configure_tasks(self):
        tools = self.source / "tools"
        tools.mkdir()
        (tools / "can_capture_summary.py").write_text(queue_tests.SUMMARY_STUB)
        (tools / "offline.py").write_text(TASK_SCRIPT)
        declaration = {"schema_version": 1, "tasks": []}
        for name, output, extra in (("golden-file", "report.json", []),
                                    ("golden-directory", "reports", ["--directory"])):
            declaration["tasks"].append({
                "name": name, "description": "Deterministic offline protocol fixture.",
                "profile": "python-script", "source_paths": ["tools/offline.py"],
                "minimum_inputs": 1, "maximum_inputs": 1,
                "argv": ["{source:tools/offline.py}", "{input:0}", "--output",
                         "{result:" + output + "}", *extra, "{arguments}"],
                "outputs": [output],
            })
        (self.source / protocol.REPO_MANIFEST).write_text(json.dumps(declaration, indent=2, sort_keys=True) + "\n")

    def golden(self, name, actual):
        self.assertIsInstance(actual, bytes)
        self.assertNotIn(name, self.seen, f"duplicate golden capture: {name}")
        self.seen.add(name)
        for pattern, replacement in normalization_rules(self.base):
            actual = pattern.sub(lambda _match: replacement, actual)
        path = GOLDEN_ROOT / self.scenario / name
        if self.update:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(actual)
            return
        self.assertTrue(path.is_file(), f"Missing golden {path}; regenerate explicitly with VAN_COMPUTE_GOLDEN_UPDATE=1")
        expected = path.read_bytes()
        if actual != expected:
            diff = "".join(difflib.unified_diff(
                expected.decode("utf-8", "backslashreplace").splitlines(keepends=True),
                actual.decode("utf-8", "backslashreplace").splitlines(keepends=True),
                fromfile=str(path), tofile="actual",
            ))
            self.fail(f"Golden bytes differ: {path}\n{diff}\nexpected {len(expected)} bytes; actual {len(actual)} bytes")

    def observation(self, name, value):
        # Test-owned Python/binary boundary, never used to reserialize wire/disk.
        self.golden(name, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())

    def finish_scenario(self):
        stored = {path.relative_to(GOLDEN_ROOT / self.scenario).as_posix()
                  for path in (GOLDEN_ROOT / self.scenario).rglob("*.json")}
        self.assertEqual(stored, self.seen, "Golden inventory changed: stale or unvisited artifacts")

    def invoke(self, module, arguments, stdin=b""):
        with io.BytesIO() as out, io.BytesIO() as err, io.BytesIO(stdin) as inp:
            with io.TextIOWrapper(out, encoding="utf-8", newline="\n", write_through=True) as stdout, \
                 io.TextIOWrapper(err, encoding="utf-8", newline="\n", write_through=True) as stderr, \
                 io.TextIOWrapper(inp, encoding="utf-8") as stream:
                with mock.patch.object(sys, "stdout", stdout), mock.patch.object(sys, "stderr", stderr), \
                     mock.patch.object(sys, "stdin", stream):
                    code = module.main(["--root", str(self.root), *arguments])
                stdout.flush()
                stderr.flush()
                return code, out.getvalue(), err.getvalue()

    def cli(self, name, arguments, *, module=queue, stdin=b"", code=0):
        result, stdout, stderr = self.invoke(module, arguments, stdin)
        self.assertEqual(result, code, stderr.decode())
        if code == 2:
            # Current error protocol is NOT JSON. Pin it honestly as transport data.
            self.assertEqual(stdout, b"")
            self.observation(name, {"exit_code": result, "stdout": stdout.decode(), "stderr": stderr.decode()})
            return None
        self.assertEqual(stderr, b"")
        self.golden(name, stdout)
        return json.loads(stdout)

    def snapshot_tree(self, label):
        for path in sorted(self.root.rglob("*.json")):
            # Source declaration is already fingerprinted in the manifest, but
            # snapshot it too: every JSON actually present in the queue is pinned.
            self.golden(f"{label}/{path.relative_to(self.root).as_posix()}", path.read_bytes())
        for path in sorted(self.root.rglob("*.tar.gz")):
            archive_label = f"{label}/{path.relative_to(self.root).as_posix()}"
            with tarfile.open(path, "r:gz") as archive:
                members = archive.getmembers()
                self.observation(f"{archive_label}/members-observation.json", [
                    {"name": member.name, "mode": member.mode, "mtime": member.mtime,
                     "uid": member.uid, "gid": member.gid, "uname": member.uname,
                     "gname": member.gname, "size": member.size,
                     "type": member.type.decode("ascii"), "pax_headers": member.pax_headers}
                    for member in members
                ])
                for member in members:
                    if member.isfile() and member.name.endswith(".json"):
                        with archive.extractfile(member) as handle:
                            self.golden(f"{archive_label}/{member.name}", handle.read())

    def submit(self, prefix, task="golden-file", arguments=()):
        return self.cli(f"{prefix}/submit.json", [
            "submit", task, "--source-root", str(self.source), "--input", str(self.capture),
            *["--arg=" + arg for arg in arguments],
        ])

    @contextmanager
    def execution_writes(self, prefix):
        original = Path.write_text
        writes = []

        def observe(path, data, *args, **kwargs):
            result = original(path, data, *args, **kwargs)
            if path.name == "execution.json" and path.is_relative_to(self.base):
                writes.append(path.read_bytes())
            return result

        with mock.patch.object(Path, "write_text", observe):
            yield writes
        # Compare outside production try/except blocks: a regression assertion
        # must never be mistaken for a worker/preparation failure.
        for index, raw in enumerate(writes, 1):
            self.golden(f"{prefix}/execution-write-{index:02d}.json", raw)

    @staticmethod
    def usage():
        return SimpleNamespace(ru_utime=0.0625, ru_stime=0.03125,
                               ru_maxrss=8 * 1024**2 if sys.platform == "darwin" else 8192,
                               ru_minflt=11, ru_majflt=2, ru_nvcsw=3, ru_nivcsw=4)

    def wait_child(self, process):
        # Do NOT mock Popen or execute_job: execute and reap the real bounded child.
        try:
            return process.wait(timeout=5)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    def broker_job(self, prefix, variant="file"):
        submitted = self.submit(prefix, "golden-directory" if variant == "directory" else "golden-file",
                                ["--fail"] if variant == "nonzero" else [])
        manifest, reason = broker.claim_local_job(self.root, grace_seconds=0, remote_max_age=45)
        self.assertIsNotNone(manifest, reason)
        self.assertEqual(manifest["id"], submitted["id"])
        self.golden(f"{prefix}/running-manifest.json", (self.root / "running" / manifest["id"] / "manifest.json").read_bytes())
        # Reuse the established harness's argument defaults and no_sandbox fake.
        harness = broker_tests.BrokerHarness()
        harness.setUp()
        try:
            args = harness.args
            args.root = self.root
            args.work_root = self.base / "broker-work"
            args.work_root.mkdir(exist_ok=True)
            args.max_result_bytes = MAX_RESULT_BYTES
        finally:
            harness.tearDown()

        def wait(process, **_kwargs):
            code = self.wait_child(process)
            limited = variant == "resource_limit"
            return broker.LocalProcessOutcome(
                137 if limited else code, self.usage(), False, False,
                "filesystem free space fell below execution safety threshold 1048576 bytes" if limited else None,
                None, 524288 if limited else FREE_BYTES,
            )

        with ExitStack() as patches:
            patches.enter_context(mock.patch.object(broker, "bubblewrap_command", broker_tests.BrokerHarness.no_sandbox))
            patches.enter_context(mock.patch.object(broker, "_wait_for_process", wait))
            if variant == "preparation_error":
                patches.enter_context(mock.patch.object(broker, "_copy_sources", side_effect=OSError("fixture source unavailable")))
            with self.execution_writes(prefix) as writes:
                payload = broker.execute_claimed_job(args, self.root, manifest)
        expected = 70 if variant == "preparation_error" else 137 if variant == "resource_limit" else 7 if variant == "nonzero" else 0
        self.assertEqual(payload["execution"]["exit_code"], expected)
        self.assertEqual(len(writes), 3 if variant in {"directory", "resource_limit"} else 2)
        if variant != "preparation_error":
            self.assertGreater(payload["execution"]["duration_seconds"], 0)
            self.assertIn("eligible_local_event_id", payload["execution"])
        self.observation(f"{prefix}/return-observation.json", payload)
        self.snapshot_tree(f"{prefix}/disk")
        return manifest["id"]

    def worker_job(self, prefix, variant="file"):
        self.submit(prefix, "golden-directory" if variant == "directory" else "golden-file",
                    ["--fail"] if variant == "nonzero" else [])
        remote = InProcessRemote(self, prefix)
        manifest = remote.claim()
        self.assertIsNotNone(manifest)
        self.golden(f"{prefix}/running-manifest.json", (self.root / "running" / manifest["id"] / "manifest.json").read_bytes())
        work = self.base / "worker-work"
        args = argparse.Namespace(
            work_root=work, python=sys.executable, timeout=30, nice=0,
            max_result_bytes=MAX_RESULT_BYTES, max_memory_bytes=64 * 1024**3,
            max_processes=10, min_free_bytes=0, executables={"python": sys.executable},
            datasets={}, sandbox_profile=self.base / "test.sb", allow_unsandboxed_dynamic=False,
            resource_manager=worker.SchedulerResourceManager(work, minimum_free_bytes=0,
                maximum_result_bytes=MAX_RESULT_BYTES, free_space_reader=lambda _path: FREE_BYTES),
        )

        def wait(process, **_kwargs):
            code = self.wait_child(process)
            interrupted = variant == "interrupted"
            return worker.AnalysisOutcome(143 if interrupted else code, self.usage(), False,
                                          interrupted, None, None, 12 * 1024**2, 2, FREE_BYTES)

        # Same bypass_sandbox pattern as the worker tests, no sandbox-exec needed.
        def bypass_sandbox(command, **_kwargs):
            return list(command)

        with ExitStack() as patches, redirect_stderr(io.StringIO()) as error:
            patches.enter_context(mock.patch.object(worker, "sandbox_command", bypass_sandbox))
            patches.enter_context(mock.patch.object(worker, "wait_for_analysis_process", wait))
            if variant == "preparation_error":
                patches.enter_context(mock.patch.object(worker, "prepare_job", side_effect=OSError("fixture source unavailable")))
            with self.execution_writes(prefix) as writes:
                if variant == "interrupted":
                    with self.assertRaises(worker.WorkerShutdown) as raised:
                        worker.run_claimed_job(args, remote, manifest)
                    self.observation(f"{prefix}/interruption-observation.json", {"exception": str(raised.exception)})
                    self.assertEqual(remote.uploads, [])
                    self.assertEqual(len(writes), 1)
                else:
                    payload = worker.run_claimed_job(args, remote, manifest)
                    expected = 70 if variant == "preparation_error" else 7 if variant == "nonzero" else 0
                    self.assertEqual(payload["execution"]["exit_code"], expected)
                    self.assertEqual(len(writes), 4 if variant == "directory" else 3)
                    self.assertEqual(remote.uploads[-1][0], "execution.json")
                    self.assertEqual(remote.uploads[-1][1], writes[-1])
                    if variant in {"file", "directory"}:
                        self.assertEqual(dict(remote.uploads)["stdout.txt"],
                                         b"offline analysis complete\nsource={job}/source\n")
                    self.assertGreater(payload["execution"]["timing"]["worker_attempt_active_seconds"], 0)
                    self.observation(f"{prefix}/return-observation.json", payload)
        self.observation(f"{prefix}/transport-observation.json", remote.calls)
        self.observation(f"{prefix}/stderr-observation.json", error.getvalue())
        self.snapshot_tree(f"{prefix}/disk")
        return manifest["id"]

    def test_queue_cli(self):
        # Copy the real example unmodified; catalog-export exercises two inputs
        # and caller arguments, unlike example tasks with fixed argv templates.
        (self.source / protocol.REPO_MANIFEST).write_bytes(EXAMPLE.read_bytes())
        (self.source / "tools/alfaobd_catalog.py").write_text("# snapshotted example source\n")
        builtin = self.submit("builtin", "can-capture-summary", ["--snapshot"])
        self.snapshot_tree("after-builtin-submit")
        repo = self.cli("submit-repository.json", [
            "submit", "alfaobd-catalog-export", "--source-root", str(self.source),
            "--input", str(self.capture), "--input", str(self.other), "--arg=--include-hidden",
        ])
        self.snapshot_tree("after-repository-submit")
        self.cli("tasks.json", ["tasks", "--source-root", str(self.source)])
        for name, identity, busy in (("capacity", "golden-mac", 0), ("busy-slot", "golden-mac.00", 1)):
            self.cli(f"heartbeat-{name}.json", ["worker", "heartbeat", "--worker", identity,
                "--protocol-version", str(protocol.WORKER_PROTOCOL_VERSION), "--slots-total", "10", "--slots-busy", str(busy)])
            self.snapshot_tree(f"after-heartbeat-{name}")
        self.cli("available.json", ["available"])
        remote = InProcessRemote(self, "remote")
        claimed = remote.claim()
        self.assertEqual(claimed["id"], builtin["id"])
        self.snapshot_tree("after-claim")
        lease = claimed["lease_token"]
        self.cli("status-running.json", ["status", claimed["id"]])
        self.cli("rejected-upload-observation.json", ["worker", "put-result", claimed["id"],
            "--worker", remote.worker, "--path", "summary.json", "--lease-token", "f" * 32],
            stdin=b'{"late": true}\n', code=2)
        # Exercise the binary CLI verbs via the real worker RPC builders too.
        remote.stream_input(claimed["id"], 0, self.base / "streamed-input", lease_token=lease)
        self.assertEqual((self.base / "streamed-input").read_bytes(), self.capture.read_bytes())
        remote.stream_source_bundle(claimed["id"], self.base / "source.tar", lease_token=lease)
        result = self.base / "summary.json"
        result.write_bytes(b'{\n  "bytes": 27,\n  "ratio": 1.25\n}\n')
        self.golden("put-result-stdin.json", result.read_bytes())
        remote.put_result(claimed["id"], "summary.json", result, lease_token=lease)
        self.snapshot_tree("after-put-result")
        remote.finish(claimed["id"], 0, ["summary.json"], lease_token=lease)
        self.snapshot_tree("after-finish")
        self.cli("status-done.json", ["status", claimed["id"]])
        self.cli("list.json", ["list"])
        self.cli("wait.json", ["wait", claimed["id"], "--timeout", "0"])
        self.cli("result.json", ["result", claimed["id"], "summary.json"])
        for name, args in (("inactive", ["status"]), ("enter", ["enter", "--owner", "golden-admin"]),
                           ("active", ["status"]), ("exit", ["exit", "--owner", "golden-admin"]),
                           ("exited", ["status"])):
            self.cli(f"maintenance-{name}.json", ["maintenance", *args])
            if name == "enter":
                self.golden("maintenance-marker.json", (self.root / queue.MAINTENANCE_FILE).read_bytes())
        self.assertFalse((self.root / queue.MAINTENANCE_FILE).exists())
        self.cli("missed-offload.json", ["missed-offload", "--profile", "can-log-batch", "--label", "golden manual analysis",
            "--reason", "agent-choice", "--duration-seconds", "2.75", "--cpu-seconds", "1.125",
            "--peak-rss-bytes", "8388608", "--input-bytes", "27"])
        self.snapshot_tree("final-disk")
        self.observation("remote/transport-observation.json", remote.calls)
        for name, args in (("tasks", ["tasks", "--source-root", str(self.source)]),
                           ("status", ["status", claimed["id"]]), ("queued-status", ["status", repo["id"]]),
                           ("list", ["list"])):
            self.cli(f"pi-compute-{name}.json", args, module=frontend)
        self.finish_scenario()

    def test_broker_file(self):
        self.broker_job("local")
        self.finish_scenario()

    def test_broker_directory(self):
        self.broker_job("local", "directory")
        self.finish_scenario()

    def test_broker_nonzero(self):
        self.broker_job("local", "nonzero")
        self.finish_scenario()

    def test_broker_preparation_error(self):
        self.broker_job("local", "preparation_error")
        self.finish_scenario()

    def test_broker_resource_limit(self):
        self.broker_job("local", "resource_limit")
        self.finish_scenario()

    def test_worker_file(self):
        self.worker_job("remote")
        self.finish_scenario()

    def test_worker_directory(self):
        self.worker_job("remote", "directory")
        self.finish_scenario()

    def test_worker_nonzero(self):
        self.worker_job("remote", "nonzero")
        self.finish_scenario()

    def test_worker_preparation_error(self):
        self.worker_job("remote", "preparation_error")
        self.finish_scenario()

    def test_worker_interrupted(self):
        self.worker_job("remote", "interrupted")
        self.finish_scenario()

    def test_metrics(self):
        # Run local first so the fresh Mac lease does not defer the broker.
        local = self.broker_job("local")
        remote = self.worker_job("remote")
        self.cli("manual-missed.json", ["missed-offload", "--profile", "python-script", "--label", "golden unoffloaded analysis",
            "--reason", "worker-unavailable", "--duration-seconds", "3.875", "--cpu-seconds", "1.625",
            "--peak-rss-bytes", "16777216", "--input-bytes", "27"])
        self.cli("capacity.json", ["worker", "heartbeat", "--worker", "golden-mac", "--protocol-version",
            str(protocol.WORKER_PROTOCOL_VERSION), "--slots-total", "10", "--slots-busy", "1"])
        self.snapshot_tree("fixture-tree")
        now = (EPOCH + dt.timedelta(seconds=40)).timestamp()
        reader = metrics.ComputeMetricsReader(self.root, clock=lambda: now)
        app = Flask(__name__)
        with app.app_context():
            report = reader.report(24)
            self.assertEqual(report["summary"]["jobs"], 1)
            self.assertEqual(report["benchmark"]["calibrated_workloads"], 1)
            self.assertEqual(report["eligible_local_work"]["events"], 2)
            for name, value in (("report", report), ("jobs-for-task", reader.jobs_for_task(24, "golden-file")),
                                ("mac-job-details", reader.job_details(remote)), ("pi-job-details", reader.job_details(local))):
                self.golden(f"dashboard-{name}.json", jsonify(value).get_data())
        self.finish_scenario()


if __name__ == "__main__":
    unittest.main()
