from __future__ import annotations

import json
import os
from pathlib import Path
import shutil

from .constants import OLD_COMPUTE_ROOT, REMOTE_RELEASES
from .core import InstallerCore
from .mac import MacProvisionMixin
from .models import DeploymentError
from .phases import RemotePhasesMixin
from .remote import RemoteReleaseMixin
from .source import SourceMixin


class Installer(
    SourceMixin, MacProvisionMixin, RemoteReleaseMixin, RemotePhasesMixin, InstallerCore
):

    def prune_local_releases(self, current: Path) -> None:
        # Reusing the active release is not a new cutover. Its prior rollback
        # release cannot be inferred from mtimes (failed stages may be newer).
        if self.rebuilt_release or self.release_retention_ambiguous or self.prior_release == current:
            return
        protected = {current}
        if self.prior_release is not None:
            protected.add(self.prior_release)
        for candidate in self.paths.release_parent.iterdir():
            if candidate in protected:
                continue
            try:
                self._release_identity(candidate)
                if os.path.ismount(candidate) or any(
                    os.path.ismount(path) for path in candidate.rglob("*")
                ):
                    continue
            except (DeploymentError, OSError):
                # Unknown, malformed, foreign, or mounted entries are retained.
                continue
            shutil.rmtree(candidate)

    def execute(self) -> int:
        source = self.build_source_release()
        self.source = source
        if self.options.dry_run:
            print(
                json.dumps(self.plan(source), indent=2, sort_keys=True),
                file=self.stdout,
            )
            return 0
        self.ensure_cache_directories()
        if self.options.if_needed and not self.options.rebuild and self.deployment_current(source):
            self.say("van_compute deployment is current; skipping installer.")
            return 0
        if self.options.if_needed:
            self.say(
                "van_compute deployment changed or is unhealthy; running installer."
            )
        self.preflight_local(source)
        lock = self.acquire_lock_and_owner()
        try:
            self.capture_prior_release()
            self.remote_preflight()
            release = self.prepare_mac_release(source)
            self.install_dataset(source)
            self.stage_remote_release(source, release)
            self.validate_remote_stage(source)
            self.provision_remote_runtime()
            relation = self.maintenance_relation()
            if relation == "other":
                raise DeploymentError(
                    "the compute queue is in maintenance under a different installer; no changes were made"
                )
            if relation == "ours":
                self.state.maintenance_active = True
                self.state.cutover_started = True
                self.say("Resuming this Mac's interrupted protocol upgrade.")
            if self.active_queue_jobs():
                raise DeploymentError(
                    "the Pi compute queue has pending or running work; let it finish and rerun"
                )
            self.drain_worker()
            self.acquire_submission_gate()
            self.wait_for_submitter_drain()
            self.enter_maintenance()
            self.cutover_remote(source)
            previous_seen = self.coordinator_seen()
            self.install_launchagent(release, source)
            heartbeat = self.wait_for_heartbeat(previous_seen)
            self.finalize_upgrade()
            self.retire_legacy_layout()
            self.refresh_dashboard()
            self.prune_local_releases(release)
            print(json.dumps(heartbeat, indent=2, sort_keys=True), file=self.stdout)
            self.say(
                "Worker isolation: "
                + ("disabled" if source.allow_unsandboxed else "sandbox-exec validated")
            )
            self.say(
                "A dedicated worker account, container, or VM would provide stronger isolation "
                "but requires separate admin setup."
            )
            self.say(f"Installed Pi release: {REMOTE_RELEASES}/{source.pi_version}")
            self.say(f"Installed Mac release: {release}")
            self.say(
                "Dashboard: perform its separate package update after this first compute cutover; see pi/docs/compute/VAN_COMPUTE.md."
            )
            self.say(
                "Repository-wide updates remain available through: ./pi/sync_scripts.sh"
            )
            return 0
        finally:
            try:
                self.cleanup()
            finally:
                lock.close()
