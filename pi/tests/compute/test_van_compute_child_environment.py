import json
import os
from contextlib import contextmanager
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from van_compute import broker, limited_child, protocol, worker
from pi.tests.compute.van_compute_test_fixtures import (
    DECLARATION, BrokerHarness,
    CHILD_ENV_SCRIPT,
    child_env_golden,
    normalized_child_capture,
)


def _prepare_broker_child_job(fixture):
    tool = fixture.source / "tools" / "capture_child.py"
    tool.write_text(CHILD_ENV_SCRIPT, encoding="utf-8")
    declaration = {
        "schema_version": 1,
        "tasks": [
            *DECLARATION["tasks"],
            {
                "name": "child-env-capture",
                "profile": "python-script",
                "source_paths": ["tools/capture_child.py"],
                "minimum_inputs": 0,
                "maximum_inputs": 0,
                "argv": [
                    "{source:tools/capture_child.py}",
                    "{result:child-env.json}",
                    "{arguments}",
                ],
                "outputs": ["child-env.json"],
            },
        ],
    }
    (fixture.source / protocol.REPO_MANIFEST).write_text(
        json.dumps(declaration), encoding="utf-8"
    )
    submitted = fixture.submit(
        "child-env-capture",
        arguments=["relative/argument"],
    )
    return submitted


def _run_broker_child_job(fixture):
    helper_calls = []
    real_popen = broker.subprocess.Popen

    def recording_popen(command, *args, **kwargs):
        helper_calls.append((list(command), dict(kwargs)))
        return real_popen(command, *args, **kwargs)

    with mock.patch.object(
        broker.subprocess, "Popen", side_effect=recording_popen
    ):
        result = fixture.run_once()
    return result, helper_calls


def _assert_broker_child_capture(test_case, fixture, submitted, result, calls):
    test_case.assertTrue(result["ok"], result)
    test_case.assertEqual(len(calls), 1)
    helper_command, helper_options = calls[0]
    test_case.assertEqual(
        helper_command[:9],
        [
            sys.executable,
            str(Path(limited_child.__file__).resolve()),
            "broker",
            str(fixture.args.max_memory_bytes),
            str(fixture.args.cpu_seconds),
            str(fixture.args.max_result_bytes),
            str(fixture.args.max_open_files),
            str(fixture.args.nice),
            "--",
        ],
    )
    temporary_root = Path(helper_options["cwd"])
    test_case.assertEqual(
        helper_command[9:],
        [
            sys.executable,
            str(temporary_root / "source" / "tools" / "capture_child.py"),
            str(temporary_root / "result" / "child-env.json"),
            "relative/argument",
        ],
    )
    raw_capture = (
        fixture.root / "done" / submitted["id"] / "result" / "child-env.json"
    ).read_bytes()
    captured = json.loads(raw_capture)
    test_case.assertEqual(
        captured["environment"]["HOME"], str(temporary_root / "home")
    )
    normalized = normalized_child_capture(
        raw_capture,
        temporary_root=temporary_root,
        python=helper_command[0],
    )
    golden = child_env_golden("broker")
    if os.environ.get("VAN_COMPUTE_GOLDEN_UPDATE") == "1":
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_bytes(normalized)
    test_case.assertTrue(golden.is_file())
    test_case.assertEqual(golden.read_bytes(), normalized)


@contextmanager
def broker_fixture():
    fixture = BrokerHarness()
    fixture.setUp()
    try:
        yield fixture
    finally:
        fixture.tearDown()


class BrokerChildEnvironmentTests(unittest.TestCase):
    def test_exec_helper_child_environment_and_argv_match_golden(self):
        with broker_fixture() as fixture:
            submitted = _prepare_broker_child_job(fixture)
            result, calls = _run_broker_child_job(fixture)
            _assert_broker_child_capture(self, fixture, submitted, result, calls)


def _worker_execution_spec():
    return {
        "profile": "python-script",
        "family": "python",
        "argv": [
            "{source:tools/capture_child.py}",
            "{result:child-env.json}",
            "{arguments}",
        ],
        "outputs": ["child-env.json"],
        "datasets": [],
        "minimum_inputs": 0,
        "maximum_inputs": 0,
        "input_values": False,
    }


@contextmanager
def worker_child_capture():
    execution = _worker_execution_spec()
    with tempfile.TemporaryDirectory(prefix="worker-child-env-") as directory:
        job_root = Path(directory) / "isolated" / "worker-job"
        source = job_root / "source"
        tool = source / "tools" / "capture_child.py"
        tool.parent.mkdir(parents=True)
        tool.write_text(CHILD_ENV_SCRIPT, encoding="utf-8")
        result = job_root / "result"
        helper_calls = []
        raw_capture = {}
        real_popen = subprocess.Popen
        real_scrub = worker.scrub_private_paths

        def recording_popen(command, *args, **kwargs):
            helper_calls.append((list(command), dict(kwargs)))
            return real_popen(command, *args, **kwargs)

        def observe_then_scrub(result_root, **kwargs):
            raw_capture["bytes"] = (result_root / "child-env.json").read_bytes()
            return real_scrub(result_root, **kwargs)

        with mock.patch.object(
            worker, "process_group_resources", return_value=(0, 1)
        ), mock.patch.object(
            worker,
            "sandbox_command",
            side_effect=lambda command, **_kwargs: list(command),
        ), mock.patch.object(
            worker.subprocess, "Popen", side_effect=recording_popen
        ), mock.patch.object(
            worker, "scrub_private_paths", side_effect=observe_then_scrub
        ):
            exit_code, _execution = worker.execute_job(
                {
                    "id": "20260722T120000Z-0000000a",
                    "task": "child-env-capture",
                    "arguments": ["relative/argument"],
                    "inputs": [],
                    "execution": execution,
                },
                source_root=source,
                input_paths=[],
                input_values=[],
                result_root=result,
                python=worker.sys.executable,
                timeout=30,
                nice=0,
                maximum_file_size=1024 * 1024,
                maximum_memory=512 * 1024 * 1024,
                sandbox_profile=job_root / "sandbox.sb",
            )
        yield {
            "exit_code": exit_code,
            "helper_calls": helper_calls,
            "raw_capture": raw_capture["bytes"],
            "source": source,
            "tool": tool,
            "result": result,
        }


def _assert_worker_child_command(test_case, capture):
    test_case.assertEqual(capture["exit_code"], 0)
    test_case.assertEqual(len(capture["helper_calls"]), 1)
    helper_command, helper_options = capture["helper_calls"][0]
    test_case.assertEqual(
        helper_command[:8],
        [
            sys.executable,
            str(Path(limited_child.__file__).resolve()),
            "worker",
            "0",
            "30",
            str(1024 * 1024),
            str(512 * 1024 * 1024),
            "--",
        ],
    )
    test_case.assertEqual(
        helper_command[8:],
        [
            worker.sys.executable,
            str(capture["tool"]),
            str(capture["result"] / "child-env.json"),
            "relative/argument",
        ],
    )
    capture["helper_command"] = helper_command
    capture["helper_options"] = helper_options


def _assert_worker_child_capture(test_case, capture):
    captured = json.loads(capture["raw_capture"])
    test_case.assertEqual(
        captured["environment"]["PYTHONPATH"], str(capture["source"])
    )
    test_case.assertNotIn(
        "VAN_COMPUTE_CHILD_PYTHONPATH", captured["environment"]
    )
    normalized = normalized_child_capture(
        capture["raw_capture"],
        temporary_root=Path(capture["helper_options"]["cwd"]).parent,
        python=capture["helper_command"][0],
    )
    golden = child_env_golden("worker")
    if os.environ.get("VAN_COMPUTE_GOLDEN_UPDATE") == "1":
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_bytes(normalized)
    test_case.assertTrue(golden.is_file())
    test_case.assertEqual(golden.read_bytes(), normalized)


class WorkerChildEnvironmentTests(unittest.TestCase):
    def test_exec_helper_child_environment_and_argv_match_golden(self):
        with worker_child_capture() as capture:
            _assert_worker_child_command(self, capture)
            _assert_worker_child_capture(self, capture)


if __name__ == "__main__":
    unittest.main()
