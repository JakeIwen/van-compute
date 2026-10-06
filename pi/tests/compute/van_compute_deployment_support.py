from __future__ import annotations

import io
import json
from pathlib import Path
import plistlib

from macbook.scripts.van_compute_installer import constants as installer_constants
from macbook.scripts.van_compute_installer.models import DeploymentError, Options, SourceRelease
from macbook.scripts.van_compute_installer.orchestrator import Installer


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
COMPUTE_ROOT = REPOSITORY_ROOT / "van_compute"
INSTALLER = REPOSITORY_ROOT / "macbook" / "scripts" / "install_van_compute_worker.py"
SHIM = REPOSITORY_ROOT / "macbook" / "scripts" / "install_van_compute_worker.zsh"
QUEUE_CLI = COMPUTE_ROOT / "entrypoints" / "van_compute.py"
FRONTEND_CLI = COMPUTE_ROOT / "entrypoints" / "pi_compute.py"
UPGRADE_GATE = COMPUTE_ROOT / "upgrade_gate.py"
EXAMPLE_TASKS = COMPUTE_ROOT / "configs" / "van-compute-obd.example.json"
DASHBOARD_SERVICE = REPOSITORY_ROOT / "pi" / "services" / "van-dashboard.service"
BROKER_SERVICE = COMPUTE_ROOT / "configs" / "van-compute-broker.service"


class FakeCompleted:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr



class FakeLocal:
    def __init__(self, launch_states=None):
        self.calls = []
        self.launch_states = list(launch_states or [])

    def run(
        self,
        arguments,
        *,
        input_text=None,
        capture_output=False,
        check=True,
        timeout=None,
        cwd=None,
    ):
        arguments = list(arguments)
        self.calls.append((arguments, input_text, capture_output, check, timeout, cwd))
        if arguments[:2] == ["/bin/launchctl", "print"]:
            if self.launch_states:
                state = self.launch_states.pop(0)
            else:
                state = None
            return (
                FakeCompleted(0, state)
                if state is not None
                else FakeCompleted(113, "", "not found")
            )
        return FakeCompleted()



class FakeRemote:
    def __init__(self, responses=None, failures=None):
        self.calls = []
        self.scripts = {}
        self.responses = {
            name: list(values) if isinstance(values, (list, tuple)) else [values]
            for name, values in (responses or {}).items()
        }
        self.failures = dict(failures or {})

    def run(
        self,
        name,
        script,
        arguments=(),
        *,
        capture_output=False,
        timeout=None,
    ):
        self.calls.append((name, tuple(arguments), capture_output, timeout))
        self.scripts[name] = script
        failure = self.failures.get(name)
        if failure is not None:
            raise DeploymentError(str(failure))
        values = self.responses.get(name, [""])
        value = values.pop(0) if len(values) > 1 else values[0]
        return value

    def upload(self, name, sources, destination):
        self.calls.append((name, tuple(map(str, sources)), destination, None))
        failure = self.failures.get(name)
        if failure is not None:
            raise DeploymentError(str(failure))



class FakeLock:
    def __init__(self, events):
        self.events = events

    def close(self):
        self.events.append("lock-close")



class WorkflowInstaller(Installer):
    """Exercise Installer.execute while replacing only external phase bodies."""

    def __init__(self, options, source):
        self.events = []
        self._source = source
        self._release = Path("/fake/mac-release")
        self.out = io.StringIO()
        self.err = io.StringIO()
        self.fake_local = FakeLocal()
        super().__init__(
            options,
            environment={"HOME": "/fake"},
            home=Path("/fake"),
            script=INSTALLER,
            local=self.fake_local,
            remote=FakeRemote(),
            stdout=self.out,
            stderr=self.err,
            sleep=lambda _seconds: None,
        )

    def build_source_release(self):
        self.events.append("source")
        return self._source

    def deployment_current(self, _source):
        self.events.append("current")
        return False

    def ensure_cache_directories(self):
        pass  # Like the other filesystem phases, never write /fake in this harness.

    def preflight_local(self, _source):
        self.events.append("local-preflight")

    def acquire_lock_and_owner(self):
        self.events.append("lock")
        self.owner = "installer-00000000-0000-0000-0000-000000000000"
        return FakeLock(self.events)

    def remote_preflight(self):
        self.events.append("remote-preflight")
        self.state.upgrade_public_root = installer_constants.REMOTE_SCRIPTS
        return self.state.upgrade_public_root

    def prepare_mac_release(self, _source):
        self.events.append("prepare-mac")
        self.release = self._release
        return self._release

    def install_dataset(self, _source):
        self.events.append("dataset")

    def stage_remote_release(self, _source, _release):
        self.events.append("stage")
        self.state.remote_stage_created = True

    def validate_remote_stage(self, _source):
        self.events.append("validate-stage")

    def provision_remote_runtime(self):
        self.events.append("runtime")

    def maintenance_relation(self):
        self.events.append("maintenance-status")
        return "inactive"

    def active_queue_jobs(self):
        self.events.append("queue")
        return 0

    def drain_worker(self):
        self.events.append("drain")
        self.state.restore_previous_agent = True

    def acquire_submission_gate(self):
        self.events.append("gate")
        self.state.submission_gate_active = True

    def wait_for_submitter_drain(self):
        self.events.append("submitter-drain")

    def enter_maintenance(self):
        self.events.append("maintenance-enter")
        self.state.maintenance_active = True

    def cutover_remote(self, _source):
        self.events.append("cutover")
        self.state.cutover_started = True
        self.state.remote_stage_created = False

    def coordinator_seen(self):
        self.events.append("seen")
        return "old"

    def install_launchagent(self, _release, _source):
        self.events.append("install-agent")
        self.state.restore_previous_agent = False

    def wait_for_heartbeat(self, previous_seen):
        self.events.append(f"heartbeat:{previous_seen}")
        return {"workers": []}

    def finalize_upgrade(self):
        self.events.append("finalize")
        self.state.maintenance_active = False
        self.state.submission_gate_active = False

    def retire_legacy_layout(self):
        self.events.append("retire")

    def refresh_dashboard(self):
        self.events.append("dashboard")

    def prune_local_releases(self, _release):
        self.events.append("prune")

    def cleanup(self):
        self.events.append("cleanup")



class DeploymentFixtureMixin:

    def setUp(self):
        self.source = SourceRelease(
            files=(),
            source_fingerprint="a" * 64,
            deployment_fingerprint="b" * 64,
            pi_version="a" * 24,
            mac_version="b" * 24,
            dataset_source=None,
            dataset_fingerprint="none",
            allow_unsandboxed=False,
        )

    def make_installer(
        self, directory, *, options=None, local=None, remote=None, **kwargs
    ):
        return Installer(
            options or Options(),
            environment=kwargs.pop("environment", {}),
            home=Path(directory) / "home",
            script=INSTALLER,
            local=local or FakeLocal(),
            remote=remote or FakeRemote(),
            stdout=kwargs.pop("stdout", io.StringIO()),
            stderr=kwargs.pop("stderr", io.StringIO()),
            sleep=kwargs.pop("sleep", lambda _seconds: None),
            monotonic=kwargs.pop("monotonic", lambda: 0.0),
            **kwargs,
        )

    def make_source(self, marker):
        return SourceRelease(
            files=(),
            source_fingerprint=marker * 64,
            deployment_fingerprint=marker * 64,
            pi_version=marker * 24,
            mac_version=marker * 24,
            dataset_source=None,
            dataset_fingerprint="none",
            allow_unsandboxed=False,
        )

    def create_owned_release(self, installer, source):
        release = installer.paths.release_parent / source.mac_version
        (release / "app" / "van_compute").mkdir(parents=True)
        (release / "app" / "van_compute" / "worker.py").write_text(
            "# owned release\n", encoding="utf-8"
        )
        (release / installer_constants.SOURCE_HASH_FILE).write_text(
            source.source_fingerprint + "\n", encoding="utf-8"
        )
        (release / installer_constants.DEPLOYMENT_HASH_FILE).write_text(
            source.deployment_fingerprint + "\n", encoding="utf-8"
        )
        (release / installer_constants.PROVENANCE_FILE).write_text(
            json.dumps(installer.provenance(source, "mac-worker")) + "\n",
            encoding="utf-8",
        )
        if source.files:
            installer._copy_source_tree(release, source)
        python = release / "venv/bin/python"
        python.parent.mkdir(parents=True)
        python.write_text("# interpreter executed only by FakeLocal\n", encoding="utf-8")
        python.chmod(0o700)
        installer._write_manifest(release)
        return release

    def point_launchagent_at(self, installer, release):
        installer.paths.target_dir.mkdir(parents=True, exist_ok=True)
        payload = plistlib.loads(installer.paths.source_plist.read_bytes())
        payload["ProgramArguments"] = [
            str(release / "venv/bin/python"),
            "-P",
            "-m",
            "van_compute.worker",
            "--serve",
        ]
        payload["EnvironmentVariables"] = {"PYTHONPATH": str(release / "app")}
        installer.paths.target_plist.write_bytes(plistlib.dumps(payload))
