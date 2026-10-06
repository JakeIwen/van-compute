from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import stat
from typing import Sequence

from .constants import (
    DEPLOYMENT_HASH_FILE, LABEL, MANIFEST_FILE, PROVENANCE_FILE, REMOTE_RELEASES,
    SAFE_WORKER_RE, SOURCE_HASH_FILE,
)
from .models import DeploymentError, SourceRelease


INSTALLER_SOURCE_FILES = tuple(
    Path("macbook/scripts/van_compute_installer") / name
    for name in (
        "__init__.py",
        "cli.py",
        "constants.py",
        "core.py",
        "mac.py",
        "models.py",
        "orchestrator.py",
        "phases.py",
        "remote.py",
        "source.py",
    )
)


class SourceMixin:

    def _compute_source_directories(self) -> tuple[Path, Path, Path]:
        package = self.paths.repo_root / "van_compute"
        entrypoints = package / "entrypoints"
        configs = package / "configs"
        if (
            not package.is_dir()
            or package.is_symlink()
            or not entrypoints.is_dir()
            or entrypoints.is_symlink()
            or not configs.is_dir()
            or configs.is_symlink()
        ):
            raise DeploymentError(
                f"compute deployment source is missing or unsafe: {package}"
            )
        return package, entrypoints, configs

    def _require_installer_source_directory(self) -> None:
        package = self.paths.repo_root / "macbook/scripts/van_compute_installer"
        if not package.is_dir() or package.is_symlink():
            raise DeploymentError(
                f"compute deployment source is missing or unsafe: {package}"
            )

    def source_paths(self) -> tuple[Path, ...]:
        package, entrypoints, _configs = self._compute_source_directories()
        self._require_installer_source_directory()
        relative = [
            Path("macbook/scripts/install_van_compute_worker.zsh"),
            Path("macbook/scripts/install_van_compute_worker.py"),
            *INSTALLER_SOURCE_FILES,
            Path("macbook/launchagents") / f"{LABEL}.plist",
        ]
        relative.extend(
            path.relative_to(self.paths.repo_root)
            for path in sorted(package.glob("*.py"))
        )
        relative.extend(
            path.relative_to(self.paths.repo_root)
            for path in sorted(entrypoints.glob("*.py"))
        )
        relative.extend(
            (
                Path("van_compute/configs/van-compute-broker.service"),
                Path("van_compute/configs/van-compute-obd.example.json"),
            )
        )
        files = tuple(self.paths.repo_root / item for item in relative)
        for path in files:
            self._regular_file(path, "compute deployment source")
        return files

    def _fingerprint_source_tree(
        self, root: Path, relative_paths: Sequence[Path]
    ) -> str:
        digest = hashlib.sha256()
        for relative in relative_paths:
            path = root / relative
            self._regular_file(path, "compute deployment source")
            digest.update(relative.as_posix().encode() + b"\0")
            digest.update(bytes.fromhex(self._hash_file(path)))
        return digest.hexdigest()

    def _verify_staged_source(self, app_root: Path, source: SourceRelease) -> None:
        relative_paths = tuple(
            path.relative_to(self.paths.repo_root) for path in source.files
        )
        expected = {path.as_posix() for path in relative_paths}
        actual: set[str] = set()
        for path in app_root.rglob("*"):
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise DeploymentError(
                    f"staged deployment source contains a symlink: {path}"
                )
            if stat.S_ISDIR(details.st_mode):
                continue
            if not stat.S_ISREG(details.st_mode):
                raise DeploymentError(
                    f"staged deployment source contains a special entry: {path}"
                )
            if path.name != ".DS_Store":
                actual.add(path.relative_to(app_root).as_posix())
        if actual != expected:
            raise DeploymentError(
                "staged deployment source file set changed after planning"
            )
        fingerprint = self._fingerprint_source_tree(app_root, relative_paths)
        if fingerprint != source.source_fingerprint:
            raise DeploymentError("staged deployment source changed after planning")

    def dataset_source(self) -> Path | None:
        configured = self.environment.get("VAN_COMPUTE_DATASET_CONFIG", "")
        if configured:
            path = Path(configured).expanduser()
            self._regular_file(path, "VAN_COMPUTE_DATASET_CONFIG")
            return path.resolve()
        default = (
            self.paths.repo_root / "macbook" / "secrets" / "van-compute-datasets.json"
        )
        if default.exists() or default.is_symlink():
            self._regular_file(default, "default dataset configuration")
            return default.resolve()
        return None

    def allow_unsandboxed(self) -> bool:
        raw = self.environment.get("VAN_COMPUTE_ALLOW_UNSANDBOXED")
        if (
            raw is None
            and self.paths.target_plist.is_file()
            and not self.paths.target_plist.is_symlink()
        ):
            try:
                payload = plistlib.loads(self.paths.target_plist.read_bytes())
                arguments = payload.get("ProgramArguments", [])
                raw = "1" if "--allow-unsandboxed-dynamic" in arguments else "0"
            except (OSError, plistlib.InvalidFileException, AttributeError):
                raw = "0"
        raw = "0" if raw is None else raw
        if raw not in {"0", "1"}:
            raise DeploymentError("VAN_COMPUTE_ALLOW_UNSANDBOXED must be 0 or 1")
        return raw == "1"

    def build_source_release(self) -> SourceRelease:
        if not SAFE_WORKER_RE.fullmatch(self.worker):
            raise DeploymentError(
                "VAN_COMPUTE_WORKER must be a safe name no longer than 60 characters"
            )
        files = self.source_paths()
        relative_paths = tuple(path.relative_to(self.paths.repo_root) for path in files)
        source_fingerprint = self._fingerprint_source_tree(
            self.paths.repo_root, relative_paths
        )
        dataset_source = self.dataset_source()
        if dataset_source is not None:
            dataset_fingerprint = self._hash_file(dataset_source)
        elif (
            self.paths.dataset_target.is_file()
            and not self.paths.dataset_target.is_symlink()
        ):
            dataset_fingerprint = self._hash_file(self.paths.dataset_target)
        else:
            dataset_fingerprint = "none"
        allow_unsandboxed = self.allow_unsandboxed()
        deployment = (
            f"source={source_fingerprint} host={self.host} worker={self.worker} "
            f"unsandboxed={int(allow_unsandboxed)} dataset={dataset_fingerprint}"
        )
        deployment_fingerprint = hashlib.sha256(deployment.encode()).hexdigest()
        return SourceRelease(
            files=files,
            source_fingerprint=source_fingerprint,
            deployment_fingerprint=deployment_fingerprint,
            pi_version=source_fingerprint[:24],
            mac_version=deployment_fingerprint[:24],
            dataset_source=dataset_source,
            dataset_fingerprint=dataset_fingerprint,
            allow_unsandboxed=allow_unsandboxed,
        )

    def plan(self, source: SourceRelease) -> dict[str, object]:
        return {
            "dry_run": True,
            "mode": "coupled",
            "if_needed": self.options.if_needed,
            "rebuild": self.options.rebuild,
            "mac_build_layout": "<deployment24>-<build_uuid32> (legacy releases may be reused)",
            "source_root": str(self.paths.repo_root),
            "host": self.host,
            "worker": self.worker,
            "pi_release": f"{REMOTE_RELEASES}/{source.pi_version}",
            "mac_release": str(self.paths.release_parent / f"{source.mac_version}-<build_uuid32>"),
            "source_sha256": source.source_fingerprint,
            "deployment_sha256": source.deployment_fingerprint,
            "local_operations": [
                "validate sandbox-exec and local prerequisites",
                "provision or verify an immutable Mac worker release",
                "validate the worker sandbox and resource watchdog",
                "atomically install and reload the persistent LaunchAgent",
            ],
            "remote_operations": [
                "preflight paths, prerequisites, queue emptiness, and upgrade ownership",
                "stage and validate the complete release before fencing",
                "provision or verify the locked fallback runtime before fencing",
                "drain the worker, fence all submitters, and enter queue maintenance",
                "publish current/previous atomically and restart the broker",
                "require a fresh compatible worker heartbeat before releasing maintenance",
            ],
            "rollback": {
                "before_cutover": "restore the exact queue CLI and prior LaunchAgent",
                "after_cutover_started": "leave the queue fenced and rerun this installer forward",
            },
            "remote_probes_executed": False,
        }

    def provenance(self, source: SourceRelease, kind: str) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": 1,
            "kind": kind,
            "source_sha256": source.source_fingerprint,
            "source_root": str(self.paths.repo_root),
        }
        if kind == "mac-worker":
            payload.update(
                {
                    "deployment_sha256": source.deployment_fingerprint,
                    "host": self.host,
                    "worker": self.worker,
                    "allow_unsandboxed": source.allow_unsandboxed,
                    "dataset_sha256": source.dataset_fingerprint,
                }
            )
        return payload

    def _write_manifest(self, root: Path) -> None:
        records: dict[str, dict[str, object]] = {}
        for path in sorted(root.rglob("*")):
            if path == root / MANIFEST_FILE or path.name == ".DS_Store":
                continue
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise DeploymentError(f"release contains a symlink: {path}")
            if stat.S_ISDIR(details.st_mode):
                continue
            if not stat.S_ISREG(details.st_mode):
                raise DeploymentError(f"release contains a special entry: {path}")
            records[path.relative_to(root).as_posix()] = {
                "sha256": self._hash_file(path),
                "mode": stat.S_IMODE(details.st_mode),
            }
        (root / MANIFEST_FILE).write_text(
            json.dumps(
                {"schema_version": 1, "files": records}, indent=2, sort_keys=True
            )
            + "\n",
            encoding="utf-8",
        )
        os.chmod(root / MANIFEST_FILE, 0o600)

    def _release_markers(self, root: Path) -> dict[str, str]:
        if (
            root.parent != self.paths.release_parent
            or not re.fullmatch(r"[0-9a-f]{24}(?:-[0-9a-f]{32})?", root.name)
            or root.is_symlink()
            or not root.is_dir()
        ):
            raise DeploymentError(f"release is not an owned real directory: {root}")
        marker_values: dict[str, str] = {}
        for marker in (SOURCE_HASH_FILE, DEPLOYMENT_HASH_FILE):
            path = root / marker
            self._regular_file(path, f"release {marker}")
            value = path.read_text(encoding="utf-8").strip()
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise DeploymentError(f"release marker is invalid: {path}")
            marker_values[marker] = value
        if root.name.split("-", 1)[0] != marker_values[DEPLOYMENT_HASH_FILE][:24]:
            raise DeploymentError(
                f"release directory does not match its identity: {root}"
            )
        return marker_values

    def _verify_release_provenance(
        self, root: Path, marker_values: dict[str, str]
    ) -> None:
        provenance_path = root / PROVENANCE_FILE
        self._regular_file(provenance_path, "release provenance")
        try:
            provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raise DeploymentError(
                f"release provenance is invalid: {provenance_path}"
            ) from None
        if (
            not isinstance(provenance, dict)
            or provenance.get("schema_version") != 1
            or provenance.get("kind") != "mac-worker"
            or provenance.get("source_sha256") != marker_values[SOURCE_HASH_FILE]
            or provenance.get("deployment_sha256")
            != marker_values[DEPLOYMENT_HASH_FILE]
        ):
            raise DeploymentError(
                f"release provenance identity mismatch: {provenance_path}"
            )
        build_id = root.name.split("-", 1)[1] if "-" in root.name else None
        if provenance.get("build_id") != build_id:
            raise DeploymentError(f"release build identity mismatch: {root}")

    def _release_manifest_records(
        self, root: Path, *, verify_runtime: bool
    ) -> tuple[Path, dict[str, object]]:
        manifest_path = root / MANIFEST_FILE
        self._regular_file(manifest_path, "release manifest")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            records = manifest["files"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            raise DeploymentError(
                f"release manifest is invalid: {manifest_path}"
            ) from None
        if not isinstance(records, dict):
            raise DeploymentError(f"release manifest is invalid: {manifest_path}")
        for relative in records:
            name = Path(relative)
            if (
                not name.parts
                or name.is_absolute()
                or ".." in name.parts
                or name.as_posix() != relative
            ):
                raise DeploymentError(f"release manifest path is unsafe: {relative}")
        filtered = {
            relative: record
            for relative, record in records.items()
            if Path(relative).name != ".DS_Store"
            and (verify_runtime or Path(relative).parts[0] != "venv")
        }
        return manifest_path, filtered

    def _verify_release_manifest(
        self,
        root: Path,
        manifest_path: Path,
        records: dict[str, object],
        *,
        verify_runtime: bool,
    ) -> None:
        actual: set[str] = set()
        for path in root.rglob("*"):
            relative = path.relative_to(root)
            if not verify_runtime and relative.parts[0] == "venv":
                continue
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise DeploymentError(f"release contains a symlink: {path}")
            if stat.S_ISDIR(details.st_mode):
                continue
            if not stat.S_ISREG(details.st_mode):
                raise DeploymentError(f"release contains a special entry: {path}")
            if path != manifest_path and path.name != ".DS_Store":
                actual.add(path.relative_to(root).as_posix())
        if actual != set(records):
            raise DeploymentError(f"release manifest file set mismatch: {root}")
        for relative, record in records.items():
            path = root / relative
            if path.is_symlink() or not path.is_file() or not isinstance(record, dict):
                raise DeploymentError(f"release entry is unsafe: {path}")
            if self._hash_file(path) != record.get("sha256"):
                raise DeploymentError(f"release entry hash mismatch: {path}")
            if stat.S_IMODE(path.stat().st_mode) != record.get("mode"):
                raise DeploymentError(f"release entry mode mismatch: {path}")

    def _release_identity(
        self, root: Path, *, verify_runtime: bool = True
    ) -> tuple[str, str]:
        marker_values = self._release_markers(root)
        self._verify_release_provenance(root, marker_values)
        manifest_path, records = self._release_manifest_records(
            root, verify_runtime=verify_runtime
        )
        self._verify_release_manifest(
            root,
            manifest_path,
            records,
            verify_runtime=verify_runtime,
        )
        return (
            marker_values[SOURCE_HASH_FILE],
            marker_values[DEPLOYMENT_HASH_FILE],
        )

    def _verify_release(
        self, root: Path, source: SourceRelease, *, verify_runtime: bool = True
    ) -> None:
        source_hash, deployment_hash = self._release_identity(root, verify_runtime=verify_runtime)
        if (
            source_hash != source.source_fingerprint
            or deployment_hash != source.deployment_fingerprint
        ):
            raise DeploymentError(f"release marker mismatch: {root}")
        self._verify_staged_source(root / "app", source)

    def _copy_source_tree(self, staging: Path, source: SourceRelease) -> None:
        for source_path in source.files:
            relative = source_path.relative_to(self.paths.repo_root)
            destination = staging / "app" / relative
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(source_path, destination, follow_symlinks=False)
            executable = (
                relative.parts[:2] == ("macbook", "scripts")
                or relative.parts[:2] == ("van_compute", "entrypoints")
                or relative == Path("van_compute/upgrade_gate.py")
            )
            os.chmod(destination, 0o700 if executable else 0o600)
