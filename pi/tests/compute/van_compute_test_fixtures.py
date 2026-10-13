import argparse
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest import mock

from van_compute import broker, config, protocol, queue, upgrade_gate
from van_compute import worker


CHILD_ENV_SCRIPT = """import json
import os
from pathlib import Path
import sys

Path(sys.argv[1]).write_text(
    json.dumps(
        {"argv": sys.argv, "environment": dict(os.environ)},
        indent=2,
        sort_keys=True,
    ) + "\\n",
    encoding="utf-8",
)
"""


def child_env_golden(name):
    return Path(__file__).with_name("golden") / "child_env" / f"{name}.json"


def normalized_child_capture(payload, *, temporary_root, python):
    replacements = []
    for value, marker in ((temporary_root, b"<TMP>"), (python, b"<PYTHON>")):
        literals = {
            os.path.abspath(os.fspath(value)),
            os.path.realpath(os.fspath(value)),
        }
        for literal in tuple(literals):
            if literal.startswith("/private/"):
                literals.add(literal.removeprefix("/private"))
            elif literal.startswith(("/tmp/", "/var/")):
                literals.add("/private" + literal)
        replacements.extend(
            (literal.encode("utf-8"), marker) for literal in literals
        )
    for literal, marker in sorted(
        replacements, key=lambda item: len(item[0]), reverse=True
    ):
        payload = payload.replace(literal, marker)
    return payload


def worker_config(**overrides):
    values = {
        "host": "pi@vanpi.lan",
        "remote_cli": "/home/pi/van_compute/scripts/van_compute.py",
        "remote_root": None,
        "worker": "m4mac",
        "work_root": Path("."),
        "control_path": Path("control.sock"),
        "python": sys.executable,
        "sqlite3": "/usr/bin/sqlite3",
        "rg": "/usr/bin/rg",
        "jadx": "/usr/bin/jadx",
        "timeout": 30,
        "connect_timeout": 5,
        "nice": 0,
        "max_result_bytes": 1024 * 1024,
        "max_memory_bytes": 512 * 1024 * 1024,
        "max_processes": worker.DEFAULT_MAX_PROCESSES,
        "min_free_bytes": worker.DEFAULT_MIN_FREE_BYTES,
        "heartbeat_interval": 0.01,
        "poll_interval": 0.01,
        "dataset_config": None,
        "dataset": (),
        "sandbox_profile": None,
        "allow_unsandboxed_dynamic": False,
        "serve": False,
        "run_once": True,
        "executables": {"python": sys.executable},
        "datasets": {},
    }
    values.update(overrides)
    if "resource_manager" not in overrides:
        values["resource_manager"] = worker.SchedulerResourceManager(
            values["work_root"],
            minimum_free_bytes=values["min_free_bytes"],
            maximum_result_bytes=values["max_result_bytes"],
        )
    return config.WorkerConfig(**values)


LOCAL_SCRIPT = """#!/usr/bin/env python3
import subprocess
import sys
import time

print('local result', flush=True)
if '--background' in sys.argv:
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    print(f'background={child.pid}', flush=True)
if '--fail' in sys.argv:
    raise SystemExit(7)
"""


SUMMARY_SCRIPT = """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument('capture')
parser.add_argument('--json', required=True)
args = parser.parse_args()
Path(args.json).write_text(json.dumps({'bytes': Path(args.capture).stat().st_size}))
print('summary complete')
"""


DECLARATION = {
    "schema_version": 1,
    "tasks": [
        {
            "name": "local-python",
            "profile": "python-script",
            "source_paths": ["tools/local_job.py"],
            "minimum_inputs": 0,
            "maximum_inputs": 0,
            "argv": ["{source:tools/local_job.py}", "{arguments}"],
            "outputs": [],
        },
        {
            "name": "decode-apk",
            "profile": "apk-analyze",
            "source_paths": [],
            "minimum_inputs": 1,
            "maximum_inputs": 1,
            "argv": ["-d", "{result:decoded}", "{input:0}"],
            "outputs": ["decoded"],
        },
        {
            "name": "missing-output",
            "profile": "python-script",
            "source_paths": ["tools/local_job.py"],
            "minimum_inputs": 0,
            "maximum_inputs": 0,
            "argv": ["{source:tools/local_job.py}"],
            "outputs": ["required.json"],
        },
    ],
}


GOOD_HEALTH = broker.HealthSnapshot(
    memory_available_bytes=4 * 1024 * 1024 * 1024,
    swap_total_bytes=1024 * 1024 * 1024,
    swap_used_bytes=0,
    load_1m=0.25,
    cpu_count=4,
    temperature_c=45.0,
    throttled_flags=0,
)


class RetentionHarness:
    NOW = dt.datetime(2026, 10, 13, 12, 0, tzinfo=dt.timezone.utc)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.git_home = base / "git-home"
        self.git_home.mkdir()
        self.git_environment = {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "HOME": str(self.git_home),
            "XDG_CONFIG_HOME": str(self.git_home / ".config"),
        }
        self.git_environment_patch = mock.patch.dict(os.environ, self.git_environment)
        self.git_environment_patch.start()
        self.source = base / "obd-things"
        self.source.mkdir()
        self.script_root = base / "compute-scripts"
        self.script_root.mkdir()
        (self.script_root / "van_compute.py").write_text(
            "#!/usr/bin/env python3\nNORMAL_QUEUE_CLI = True\n",
            encoding="utf-8",
        )
        self.script_root_patch = mock.patch.object(
            upgrade_gate,
            "DEFAULT_SCRIPT_ROOT",
            self.script_root,
        )
        self.submitter_count_patch = mock.patch.object(
            upgrade_gate,
            "active_submitter_count",
            return_value=0,
        )
        self.script_root_patch.start()
        self.submitter_count_patch.start()
        self.git("init", "-q")
        (self.source / ".gitignore").write_text("tmp/\n*.ignored\n", encoding="utf-8")
        (self.source / "README.md").write_text("fixture\n", encoding="utf-8")
        self.git("add", ".gitignore", "README.md")
        self.root = queue.safe_root(self.source / "tmp" / "compute")
        self._serial = 0

    def tearDown(self):
        self.submitter_count_patch.stop()
        self.script_root_patch.stop()
        self.git_environment_patch.stop()
        self.temporary.cleanup()

    def git(self, *arguments):
        environment = os.environ.copy()
        environment.update(self.git_environment)
        subprocess.run(
            ["git", "-C", str(self.source), *arguments],
            check=True,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def add_job(
        self,
        state,
        *,
        age_days=90,
        age_seconds=0,
        references=(),
        with_finished_at=True,
    ):
        self._serial += 1
        job_id = f"20260101T000000Z-{self._serial:08x}"
        path = self.root / state / job_id
        (path / "result").mkdir(parents=True)
        (path / "result" / "payload.bin").write_bytes(b"payload")
        manifest = {
            "schema_version": 1,
            "id": job_id,
            "state": state,
            "inputs": [{"job_id": item} for item in references],
        }
        if with_finished_at:
            finished = self.NOW - dt.timedelta(days=age_days, seconds=age_seconds)
            manifest["finished_at"] = finished.isoformat()
        queue.atomic_json(path / "manifest.json", manifest)
        return job_id

    def cite(self, relative, text, *, tracked=True):
        path = self.source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        if tracked:
            self.git("add", "-f", relative)
        return path


class BrokerHarness:
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.source = base / "obd-things"
        self.root = self.source / "tmp" / "compute"
        self.work = base / "broker-work" / "jobs"
        (self.source / "tools").mkdir(parents=True)
        (self.source / "tools" / "local_job.py").write_text(
            LOCAL_SCRIPT, encoding="utf-8"
        )
        (self.source / "tools" / "can_capture_summary.py").write_text(
            SUMMARY_SCRIPT, encoding="utf-8"
        )
        (self.source / protocol.REPO_MANIFEST).write_text(
            json.dumps(DECLARATION), encoding="utf-8"
        )
        self.apk = self.source / "tmp" / "sample.apk"
        self.apk.parent.mkdir(parents=True)
        self.apk.write_bytes(b"not really an apk")
        self.capture = self.source / "tmp" / "capture.log"
        self.capture.write_bytes(b"can capture bytes\n")
        self.args = config.BrokerConfig(
            root=self.root,
            work_root=self.work,
            once=False,
            self_test=False,
            remote_max_age=45.0,
            remote_grace=0.0,
            stale_running_age=300.0,
            poll_interval=5.0,
            timeout=30,
            cpu_seconds=30,
            # Darwin reserves a large virtual address range even for a tiny
            # interpreter; production validation caps this at the Pi's 1 GiB.
            max_memory_bytes=64 * 1024 * 1024 * 1024,
            max_result_bytes=1024 * 1024,
            min_work_free_bytes=0,
            max_open_files=128,
            nice=0,
            python=sys.executable,
            sqlite3="/usr/bin/sqlite3",
            bwrap=sys.executable,
            min_available_memory_mb=512,
            max_swap_used=0.20,
            max_swap_used_mb=512,
            max_load_per_cpu=1.25,
            max_temperature_c=75.0,
            health_thresholds=config.HealthThresholds(
                minimum_available_bytes=512 * 1024 * 1024,
                maximum_swap_used_fraction=0.20,
                maximum_load_per_cpu=1.25,
                maximum_temperature_c=75.0,
            ),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def submit(self, task="local-python", arguments=None):
        inputs = []
        if task == "decode-apk":
            inputs = [str(self.apk)]
        elif task == "can-capture-summary":
            inputs = [str(self.capture)]
        submit_args = argparse.Namespace(
            root=self.root,
            source_root=self.source,
            task=task,
            argument=list(arguments or []),
            input=inputs,
            input_value=None,
        )
        return queue.submit_job(submit_args)

    @staticmethod
    def no_sandbox(_executable, command, **paths):
        replacements = {
            "/job/source": str(paths["source_root"]),
            "/job/inputs": str(paths["inputs_root"]),
            "/job/result": str(paths["result_root"]),
            "/job/runtime/python/bin/python3": sys.executable,
            f"/job/runtime/python/bin/{Path(sys.executable).name}": sys.executable,
        }
        rendered = []
        for item in command:
            value = item
            for sandbox, host in replacements.items():
                if value == sandbox or value.startswith(f"{sandbox}/"):
                    value = host + value[len(sandbox) :]
                    break
            rendered.append(value)
        return rendered

    def run_once(self):
        with mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ), mock.patch.object(broker, "bubblewrap_command", side_effect=self.no_sandbox):
            return broker.run_once(self.args, health_reader=lambda: GOOD_HEALTH)
