import argparse
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import shutil
import threading
import unittest
from unittest import mock

from van_compute import queue, retention, upgrade_gate
from pi.tests.compute.van_compute_test_fixtures import RetentionHarness


class RetentionTests(RetentionHarness, unittest.TestCase):
    def run_prune(self, *arguments):
        output = StringIO()
        errors = StringIO()
        argv = [
            "--root",
            str(self.root),
            "prune",
            "--citations-root",
            str(self.source),
            "--now",
            self.NOW.isoformat(),
            *arguments,
        ]
        with redirect_stdout(output), redirect_stderr(errors):
            result = queue.main(argv)
        payload = json.loads(output.getvalue()) if output.getvalue() else None
        return result, payload, errors.getvalue()

    def test_done_and_failed_age_thresholds_are_strict(self):
        done_at_limit = self.add_job("done", age_days=60)
        done_old = self.add_job("done", age_days=60, age_seconds=1)
        failed_at_limit = self.add_job("failed", age_days=31)
        failed_old = self.add_job("failed", age_days=31, age_seconds=1)

        result, payload, errors = self.run_prune()

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["eligible_ids"], [done_old, failed_old])
        self.assertEqual(payload["summary"]["categories"]["eligible"]["count"], 2)
        self.assertEqual(payload["summary"]["categories"]["kept-young"]["count"], 2)
        self.assertNotIn(done_at_limit, payload["eligible_ids"])
        self.assertNotIn(failed_at_limit, payload["eligible_ids"])

    def test_keep_days_applies_one_upper_age_to_both_states(self):
        done_at_limit = self.add_job("done", age_days=30)
        done_old = self.add_job("done", age_days=30, age_seconds=1)
        failed_at_limit = self.add_job("failed", age_days=30)
        failed_old = self.add_job("failed", age_days=30, age_seconds=1)

        result, payload, errors = self.run_prune("--keep-days", "30")

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["retention_days"], {"done": 30, "failed": 30, "cancelled": 30})
        self.assertEqual(payload["eligible_ids"], [done_old, failed_old])
        self.assertNotIn(done_at_limit, payload["eligible_ids"])
        self.assertNotIn(failed_at_limit, payload["eligible_ids"])

    def test_budget_evicts_expired_before_older_unexpired_job(self):
        older_unexpired = self.add_job("done", age_days=50)
        expired = self.add_job("failed", age_days=40)
        _, baseline, _ = self.run_prune()
        expired_bytes = baseline["summary"]["by_state"]["failed"]["eligible"]["bytes"]
        maximum = baseline["history_budget"]["before_bytes"] - expired_bytes

        result, payload, errors = self.run_prune(
            "--max-history-bytes", str(maximum)
        )

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["eligible_ids"], [expired])
        self.assertNotIn(older_unexpired, payload["eligible_ids"])
        self.assertEqual(payload["history_budget"]["projected_bytes"], maximum)
        self.assertTrue(payload["history_budget"]["projected_satisfied"])

    def test_budget_selects_younger_uncited_jobs_oldest_first(self):
        oldest = self.add_job("done", age_days=20)
        middle = self.add_job("failed", age_days=10)
        newest = self.add_job("done", age_days=5)
        _, baseline, _ = self.run_prune()
        sizes = {
            job_id: retention._tree_size(
                next((self.root / state / job_id for state in ("done", "failed")
                      if (self.root / state / job_id).exists())),
                self.root.stat().st_dev,
                frozenset(),
            )
            for job_id in (oldest, middle, newest)
        }
        maximum = baseline["history_budget"]["before_bytes"] - sizes[oldest] - sizes[middle]

        result, payload, errors = self.run_prune(
            "--keep-days", "30", "--max-history-bytes", str(maximum)
        )

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["eligible_ids"], [oldest, middle])
        self.assertNotIn(newest, payload["eligible_ids"])
        self.assertEqual(
            {item["reason"] for item in payload["eligible"]},
            {"oldest uncited job selected to meet history budget"},
        )

    def test_budget_reports_protected_history_that_prevents_target(self):
        protected = self.add_job("done", age_days=10)
        self.cite("projects/example/findings/protected.md", protected)

        result, payload, errors = self.run_prune(
            "--keep-days", "30", "--max-history-bytes", "1"
        )

        self.assertEqual((result, errors), (1, ""))
        budget = payload["history_budget"]
        self.assertFalse(budget["projected_satisfied"])
        self.assertFalse(budget["after_satisfied"])
        self.assertEqual(budget["blockers"][0]["id"], protected)
        self.assertEqual(budget["blockers"][0]["category"], "protected")

    def test_preview_omits_blockers_when_projected_cleanup_meets_budget(self):
        eligible = self.add_job("done", age_days=61)
        protected = self.add_job("failed", age_days=10)
        self.cite("projects/example/findings/protected.md", protected)
        _, baseline, _ = self.run_prune()
        maximum = (
            baseline["history_budget"]["before_bytes"]
            - baseline["eligible"][0]["bytes"]
        )

        result, payload, errors = self.run_prune(
            "--max-history-bytes", str(maximum)
        )

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["eligible_ids"], [eligible])
        self.assertTrue(payload["history_budget"]["projected_satisfied"])
        self.assertFalse(payload["history_budget"]["after_satisfied"])
        self.assertEqual(payload["history_budget"]["blockers"], [])

    def test_budget_apply_deletes_in_reported_order_and_reports_after(self):
        expired = self.add_job("done", age_days=31)
        younger = self.add_job("failed", age_days=20)
        newest = self.add_job("done", age_days=5)
        _, baseline, _ = self.run_prune("--keep-days", "30")
        maximum = baseline["history_budget"]["before_bytes"] - sum(
            retention._tree_size(
                self.root / state / job_id,
                self.root.stat().st_dev,
                frozenset(),
            )
            for state, job_id in (("done", expired), ("failed", younger))
        )
        deleted_order = []
        original_delete = retention._delete_tree

        def record_delete(root, state, job_id, mounts, identity):
            deleted_order.append(job_id)
            return original_delete(root, state, job_id, mounts, identity)

        with mock.patch.object(retention, "_delete_tree", side_effect=record_delete):
            result, payload, errors = self.run_prune(
                "--apply",
                "--keep-days", "30",
                "--max-history-bytes", str(maximum),
            )

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(deleted_order, [expired, younger])
        self.assertEqual(payload["deleted_ids"], [expired, younger])
        self.assertEqual(payload["history_budget"]["after_bytes"], maximum)
        self.assertTrue(payload["history_budget"]["after_satisfied"])
        self.assertTrue((self.root / "done" / newest).is_dir())

    def test_budget_apply_reports_measured_bytes_when_delete_is_refused(self):
        job_id = self.add_job("done", age_days=31)
        _, baseline, _ = self.run_prune("--keep-days", "30")
        before = baseline["history_budget"]["before_bytes"]
        maximum = max(1, before - baseline["eligible"][0]["bytes"])

        with mock.patch.object(
            retention,
            "_delete_tree",
            side_effect=retention._UnsafeTarget("fixture refusal"),
        ):
            result, payload, errors = self.run_prune(
                "--apply",
                "--keep-days", "30",
                "--max-history-bytes", str(maximum),
            )

        self.assertEqual((result, errors), (1, ""))
        self.assertEqual(payload["deleted_ids"], [])
        self.assertEqual(payload["history_budget"]["after_bytes"], before)
        self.assertFalse(payload["history_budget"]["after_satisfied"])
        self.assertEqual(payload["history_budget"]["blockers"][0]["id"], job_id)

    def test_budget_includes_completed_state_directory_overhead(self):
        expected = sum(
            (self.root / state).stat().st_blocks * 512
            for state in ("done", "failed")
        )
        maximum = max(1, expected - 1)
        result, payload, errors = self.run_prune(
            "--max-history-bytes", str(maximum)
        )

        self.assertEqual((result, errors), (int(expected > maximum), ""))
        budget = payload["history_budget"]
        self.assertEqual(budget["before_bytes"], expected)
        self.assertEqual(budget["projected_bytes"], expected)
        if expected > maximum:
            self.assertEqual(
                budget["blockers"][0]["category"], "unreclaimable-overhead"
            )
        else:
            self.assertEqual(budget["blockers"], [])

    def test_duplicate_completed_job_ids_are_skipped(self):
        job_id = self.add_job("done")
        duplicate = self.root / "failed" / job_id
        shutil.copytree(self.root / "done" / job_id, duplicate)
        manifest = queue.load_json(duplicate / "manifest.json")
        manifest["state"] = "failed"
        queue.atomic_json(duplicate / "manifest.json", manifest)

        result, payload, errors = self.run_prune("--apply")

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["eligible_ids"], [])
        self.assertEqual(payload["deleted_ids"], [])
        self.assertEqual(payload["summary"]["categories"]["skipped"]["count"], 2)
        self.assertEqual(
            {item["reason"] for item in payload["skipped"]},
            {"job ID appears in multiple completed states"},
        )
        self.assertTrue((self.root / "done" / job_id).is_dir())
        self.assertTrue(duplicate.is_dir())

    def test_walkers_preserve_non_linux_mount_detection(self):
        job_id = self.add_job("done")
        job = self.root / "done" / job_id
        nested = job / "mounted"
        nested.mkdir()
        payload = nested / "payload"
        payload.write_bytes(b"x" * 8192)
        device = self.root.stat().st_dev

        with mock.patch.object(retention.sys, "platform", "darwin"), mock.patch.object(
            retention.os.path, "ismount", side_effect=lambda path: Path(path) == nested
        ):
            with self.assertRaisesRegex(retention._UnsafeTarget, "mount point"):
                retention._tree_sizes(job, device, frozenset())
            mounted_total = retention._completed_history_size(self.root, frozenset())

        normal_total = retention._completed_history_size(self.root, frozenset())
        self.assertGreaterEqual(normal_total - mounted_total, payload.stat().st_blocks * 512)

    def test_walkers_use_exact_linux_mount_membership(self):
        job_id = self.add_job("done")
        job = self.root / "done" / job_id
        nested = job / "mounted"
        nested.mkdir()
        (nested / "payload").write_bytes(b"x" * 8192)
        device = self.root.stat().st_dev
        mounts = frozenset({nested})

        with mock.patch.object(retention.sys, "platform", "linux"), mock.patch.object(
            retention.os.path,
            "ismount",
            side_effect=AssertionError("Linux should use mountinfo membership"),
        ):
            with self.assertRaisesRegex(retention._UnsafeTarget, "mount point"):
                retention._tree_sizes(job, device, mounts)
            retention._completed_history_size(self.root, mounts)

    def test_bare_and_path_form_citations_protect_jobs(self):
        bare = self.add_job("done")
        path_form = self.add_job("failed")
        self.cite(
            "projects/example/findings/jobs.md",
            f"Bare: {bare}\nPath: compute/done/{path_form}\n",
        )

        result, payload, _errors = self.run_prune()

        self.assertEqual(result, 0)
        self.assertEqual(payload["summary"]["categories"]["protected"]["count"], 2)
        self.assertEqual(payload["eligible_ids"], [])

    def test_citation_in_listed_filename_protects_job(self):
        job_id = self.add_job("done")
        self.cite(f"projects/example/findings/{job_id}.md", "no reference in body\n")

        result, payload, _errors = self.run_prune()

        self.assertEqual(result, 0)
        self.assertEqual(payload["summary"]["categories"]["protected"]["count"], 1)
        self.assertEqual(payload["eligible_ids"], [])

    def test_untracked_files_count_but_ignored_and_tmp_files_do_not(self):
        untracked = self.add_job("done")
        ignored = self.add_job("done")
        temporary = self.add_job("done")
        self.cite("projects/untracked.md", untracked, tracked=False)
        self.cite("projects/reference.ignored", ignored, tracked=False)
        self.cite("tmp/tracked-reference.md", temporary, tracked=True)

        result, payload, _errors = self.run_prune()

        self.assertEqual(result, 0)
        self.assertEqual(payload["summary"]["categories"]["protected"]["count"], 1)
        self.assertEqual(payload["eligible_ids"], [ignored, temporary])

    def test_manifest_reference_closure_is_transitive(self):
        oldest = self.add_job("done")
        middle = self.add_job("done", references=(oldest,))
        cited = self.add_job("done", references=(middle,))
        self.cite("projects/example/findings/chain.md", cited)

        result, payload, _errors = self.run_prune()

        self.assertEqual(result, 0)
        self.assertEqual(payload["protected_ids"], 3)
        self.assertEqual(payload["summary"]["categories"]["protected"]["count"], 3)
        self.assertEqual(payload["eligible_ids"], [])

    def test_missing_or_unparseable_finished_at_is_skipped(self):
        missing = self.add_job("done", with_finished_at=False)
        broken = self.add_job("failed")
        (self.root / "failed" / broken / "manifest.json").write_text(
            "{not-json", encoding="utf-8"
        )

        result, payload, _errors = self.run_prune()

        self.assertEqual(result, 0)
        self.assertEqual(payload["summary"]["categories"]["skipped"]["count"], 2)
        self.assertEqual(
            {item["id"] for item in payload["skipped"]},
            {missing, broken},
        )
        self.assertEqual(payload["eligible_ids"], [])

    def test_manifest_fifo_or_symlink_replacement_cannot_block_verified_parse(self):
        original_loader = queue.load_json_stream
        for kind in ("fifo", "symlink"):
            with self.subTest(kind=kind):
                job_id = self.add_job("done")
                manifest_path = self.root / "done" / job_id / "manifest.json"
                replacement_target = self.source.parent / f"{kind}-replacement.json"
                replacement_target.write_text("{}\n", encoding="utf-8")
                replaced = False

                def replace_before_parse(handle, path):
                    nonlocal replaced
                    if path == manifest_path and not replaced:
                        replaced = True
                        manifest_path.unlink()
                        if kind == "fifo":
                            os.mkfifo(manifest_path)
                        else:
                            manifest_path.symlink_to(replacement_target)
                    return original_loader(handle, path)

                with mock.patch.object(
                    queue,
                    "load_json_stream",
                    side_effect=replace_before_parse,
                ):
                    result, payload, errors = self.run_prune()

                self.assertEqual((result, errors), (0, ""))
                self.assertTrue(replaced)
                self.assertIn(job_id, payload["eligible_ids"])

    def test_malformed_protected_manifest_still_extends_raw_reference_closure(self):
        referenced = self.add_job("done")
        broken = self.add_job("done")
        (self.root / "done" / broken / "manifest.json").write_text(
            f"not-json chained={referenced}", encoding="utf-8"
        )
        self.cite("projects/example/findings/broken-chain.md", broken)

        result, payload, _errors = self.run_prune()

        self.assertEqual(result, 0)
        self.assertEqual(payload["protected_ids"], 2)
        self.assertEqual(payload["summary"]["categories"]["protected"]["count"], 1)
        self.assertEqual(payload["summary"]["categories"]["skipped"]["count"], 1)
        self.assertEqual(payload["eligible_ids"], [])

    def test_apply_gates_and_drains_submission_before_pruning_inputs(self):
        dependency = self.add_job("done")
        input_path = self.root / "done" / dependency / "result" / "payload.bin"
        tools = self.source / "tools"
        tools.mkdir()
        (tools / "can_capture_summary.py").write_text(
            "print('fixture')\n", encoding="utf-8"
        )
        arguments = argparse.Namespace(
            root=self.root,
            source_root=self.source,
            task="can-capture-summary",
            argument=["--snapshot"],
            input=[str(input_path)],
            input_value=None,
        )
        staged = threading.Event()
        release = threading.Event()
        outcome = {}
        original_atomic_json = queue.atomic_json
        original_acquire = upgrade_gate.acquire_submission_gate

        def pausing_atomic_json(path, payload, mode=0o600):
            original_atomic_json(path, payload, mode)
            if path.name == "manifest.json" and path.parent.name.startswith(".staging-"):
                staged.set()
                if not release.wait(5):
                    raise RuntimeError("test submission was not released")

        def submit():
            try:
                outcome["manifest"] = queue.submit_job(arguments)
            except BaseException as exc:
                outcome["error"] = exc

        def acquire_and_release(*args, **kwargs):
            original_acquire(*args, **kwargs)
            release.set()

        thread = threading.Thread(target=submit)
        with mock.patch.object(queue, "atomic_json", side_effect=pausing_atomic_json):
            thread.start()
            self.assertTrue(staged.wait(5))
            try:
                with mock.patch.object(
                    upgrade_gate,
                    "acquire_submission_gate",
                    side_effect=acquire_and_release,
                ), mock.patch.object(
                    upgrade_gate,
                    "active_submitter_count",
                    side_effect=lambda: int(thread.is_alive()),
                ):
                    result, payload, errors = self.run_prune("--apply")
            finally:
                release.set()
                thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertNotIn("error", outcome)
        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["deleted_ids"], [])
        self.assertTrue(input_path.is_file())

        claimed = queue.worker_claim(self.root, "m4mac.00")
        streamed = BytesIO()
        queue.stream_bounded_input(
            claimed,
            0,
            streamed,
            claimed["lease_token"],
        )
        self.assertEqual(streamed.getvalue(), b"payload")

    def test_failure_after_gate_publication_restores_submission_cli(self):
        job_id = self.add_job("done")
        cli = self.script_root / "van_compute.py"
        original_cli = cli.read_bytes()
        original_acquire = upgrade_gate.acquire_submission_gate

        for failure in (RuntimeError("fsync failed"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__):

                def publish_then_fail(*args, **kwargs):
                    original_acquire(*args, **kwargs)
                    self.assertNotEqual(cli.read_bytes(), original_cli)
                    raise failure

                with mock.patch.object(
                    upgrade_gate,
                    "acquire_submission_gate",
                    side_effect=publish_then_fail,
                ):
                    if isinstance(failure, KeyboardInterrupt):
                        with self.assertRaises(KeyboardInterrupt):
                            self.run_prune("--apply")
                    else:
                        result, payload, errors = self.run_prune("--apply")
                        self.assertEqual((result, payload), (2, None))
                        self.assertIn("cannot acquire submission gate", errors)

                self.assertEqual(cli.read_bytes(), original_cli)
                self.assertIsNone(upgrade_gate.upgrade_status(self.script_root))
                self.assertEqual(
                    sorted(path.name for path in self.script_root.iterdir()),
                    [".van-compute-upgrade.lock", "van_compute.py"],
                )
                self.assertTrue((self.root / "done" / job_id).is_dir())

    def test_apply_rescans_citations_before_deletion(self):
        job_id = self.add_job("done")
        original_scan = retention.scan_citations
        calls = 0

        def scan_with_new_citation(checkout):
            nonlocal calls
            calls += 1
            if calls == 2:
                self.cite("projects/new-reference.md", job_id, tracked=False)
            return original_scan(checkout)

        with mock.patch.object(
            retention,
            "scan_citations",
            side_effect=scan_with_new_citation,
        ):
            result, payload, errors = self.run_prune("--apply")

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(calls, 2)
        self.assertEqual(payload["citation_scans"], 2)
        self.assertEqual(payload["deleted_ids"], [])
        self.assertTrue((self.root / "done" / job_id).is_dir())

    def test_git_citation_scan_failure_deletes_nothing(self):
        job_id = self.add_job("done")
        not_git = self.source.parent / "not-a-checkout"
        not_git.mkdir()
        output = StringIO()
        errors = StringIO()
        argv = [
            "--root",
            str(self.root),
            "prune",
            "--apply",
            "--citations-root",
            str(not_git),
            "--now",
            self.NOW.isoformat(),
        ]

        with redirect_stdout(output), redirect_stderr(errors):
            result = queue.main(argv)

        self.assertEqual(result, 2)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("cannot scan citations", errors.getvalue())
        self.assertTrue((self.root / "done" / job_id).is_dir())

    def test_symlink_and_non_job_entries_are_never_deleted(self):
        eligible = self.add_job("done")
        external = self.source.parent / "external-job"
        external.mkdir()
        (external / "sentinel").write_text("keep", encoding="utf-8")
        symlink_id = "20260101T000000Z-deadbeef"
        (self.root / "done" / symlink_id).symlink_to(external, target_is_directory=True)
        non_job = self.root / "failed" / "not-a-job"
        non_job.mkdir()
        (non_job / "sentinel").write_text("keep", encoding="utf-8")

        result, payload, errors = self.run_prune("--apply")

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["deleted_ids"], [eligible])
        self.assertTrue(os.path.lexists(self.root / "done" / symlink_id))
        self.assertTrue((external / "sentinel").is_file())
        self.assertTrue((non_job / "sentinel").is_file())

    def test_replaced_job_directory_survives_identity_check(self):
        job_id = self.add_job("done")
        target = self.root / "done" / job_id
        displaced = self.root / "done" / f".replaced-{job_id}"
        original_delete = retention._delete_tree
        replaced = False

        def replace_then_delete(root, state, current_id, mounts, identity):
            nonlocal replaced
            if not replaced:
                replaced = True
                os.replace(target, displaced)
                (target / "result").mkdir(parents=True)
                (target / "result" / "replacement").write_text(
                    "keep", encoding="utf-8"
                )
                queue.atomic_json(
                    target / "manifest.json",
                    {
                        "id": job_id,
                        "state": "done",
                        "finished_at": "2026-01-01T00:00:00+00:00",
                    },
                )
            return original_delete(root, state, current_id, mounts, identity)

        with mock.patch.object(
            retention,
            "_delete_tree",
            side_effect=replace_then_delete,
        ):
            result, payload, errors = self.run_prune("--apply")

        self.assertEqual((result, errors), (0, ""))
        self.assertTrue(replaced)
        self.assertEqual(payload["deleted_ids"], [])
        self.assertTrue((target / "result" / "replacement").is_file())
        self.assertIn("changed since eligibility", payload["skipped"][0]["reason"])

    def test_maintenance_fence_refuses_apply(self):
        job_id = self.add_job("done")
        queue.set_maintenance(self.root, "installer", True)

        result, payload, errors = self.run_prune("--apply")

        self.assertEqual(result, 2)
        self.assertIsNone(payload)
        self.assertIn("maintenance is active", errors)
        self.assertTrue((self.root / "done" / job_id).is_dir())

    def test_upgrade_owner_or_gate_refuses_apply(self):
        job_id = self.add_job("done")
        scripts = self.source.parent / "active-upgrade-scripts"
        scripts.mkdir()
        owner = scripts / ".van-compute-upgrade-owner"
        target = scripts / "van_compute.py"
        valid_owner = "installer-12345678-1234-1234-1234-123456789abc"

        with mock.patch.object(upgrade_gate, "DEFAULT_SCRIPT_ROOT", scripts):
            owner.write_text(f"{valid_owner}\n", encoding="utf-8")
            result, payload, errors = self.run_prune("--apply")
            self.assertEqual(result, 2)
            self.assertIsNone(payload)
            self.assertIn("compute upgrade is active", errors)
            owner.unlink()

            target.write_text("UPGRADE_GATE = True\n", encoding="utf-8")
            result, payload, errors = self.run_prune("--apply")

        self.assertEqual(result, 2)
        self.assertIsNone(payload)
        self.assertIn("compute upgrade is active", errors)
        self.assertTrue((self.root / "done" / job_id).is_dir())

    def test_dry_run_lists_eligible_job_without_deleting_it(self):
        job_id = self.add_job("done")

        result, payload, errors = self.run_prune()

        self.assertEqual((result, errors), (0, ""))
        self.assertFalse(payload["apply"])
        self.assertEqual(payload["eligible_ids"], [job_id])
        self.assertEqual(payload["deleted_ids"], [])
        self.assertTrue((self.root / "done" / job_id).is_dir())

    def test_active_job_manifest_dependencies_are_protected(self):
        dependency = self.add_job("done")
        active = self.add_job("queued", references=(dependency,))
        active_manifest = self.root / "queued" / active / "manifest.json"
        manifest = queue.load_json(active_manifest)
        manifest["state"] = "running"
        queue.atomic_json(active_manifest, manifest)

        result, payload, errors = self.run_prune("--apply")

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["deleted_ids"], [])
        self.assertTrue((self.root / "done" / dependency).is_dir())
        self.assertTrue((self.root / "queued" / active).is_dir())
        self.assertEqual(payload["summary"]["categories"]["protected"]["count"], 1)

    def test_unreadable_active_manifest_refuses_apply(self):
        old_job = self.add_job("done")
        active = self.add_job("running")
        (self.root / "running" / active / "manifest.json").write_text(
            "{not-json", encoding="utf-8"
        )

        result, payload, errors = self.run_prune("--apply")

        self.assertEqual(result, 2)
        self.assertIsNone(payload)
        self.assertIn("active job manifest", errors)
        self.assertTrue((self.root / "done" / old_job).is_dir())

    def test_completed_job_with_active_copy_is_never_deleted(self):
        job_id = self.add_job("failed")
        active = self.root / "queued" / job_id
        active.mkdir()
        queue.atomic_json(
            active / "manifest.json",
            {"id": job_id, "state": "queued", "inputs": []},
        )

        result, payload, errors = self.run_prune("--apply")

        self.assertEqual((result, errors), (0, ""))
        self.assertEqual(payload["deleted_ids"], [])
        self.assertTrue((self.root / "failed" / job_id).is_dir())
        self.assertTrue(active.is_dir())
        self.assertEqual(payload["summary"]["categories"]["protected"]["count"], 1)


if __name__ == "__main__":
    unittest.main()
