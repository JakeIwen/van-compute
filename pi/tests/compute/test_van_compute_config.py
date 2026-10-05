from pathlib import Path
import threading
import unittest
from typing import Mapping

from van_compute import broker, worker
from van_compute.config import BrokerConfig, MissedOffloadRecord, WorkerConfig


class FakeReservation:
    def release(self) -> None:
        pass


class FakeResourceManager:
    def acquire(
        self,
        manifest: Mapping[str, object],
        stop_event: threading.Event | None,
        drain_event: threading.Event | None = None,
    ) -> FakeReservation:
        return FakeReservation()

    def require_free_reserve(self, path: Path, phase: str) -> None:
        pass


class RuntimeConfigTests(unittest.TestCase):
    def test_broker_validation_converts_parser_namespace_once(self):
        parsed = broker.build_parser().parse_args(["--once"])

        runtime = broker._validate_args(parsed)

        self.assertIsInstance(runtime, BrokerConfig)
        self.assertTrue(runtime.once)
        self.assertFalse(runtime.self_test)
        self.assertEqual(
            runtime.health_thresholds.minimum_available_bytes, 1536 * 1024 * 1024
        )
        self.assertEqual(runtime.max_result_bytes, broker.DEFAULT_MAX_RESULT_BYTES)

    def test_worker_conversion_keeps_raw_options_and_derived_fields(self):
        parsed = worker.build_parser().parse_args(
            ["--serve", "--dataset", "sample=/tmp"]
        )
        executables = {"python": "/usr/bin/python3"}
        datasets = {"sample": Path("/tmp")}
        manager = FakeResourceManager()

        runtime = WorkerConfig.from_namespace(
            parsed,
            work_root=Path("/var/tmp/van-compute"),
            executables=executables,
            datasets=datasets,
            resource_manager=manager,
        )

        self.assertIsInstance(runtime, WorkerConfig)
        self.assertTrue(runtime.serve)
        self.assertEqual(runtime.dataset, ("sample=/tmp",))
        self.assertIs(runtime.executables, executables)
        self.assertIs(runtime.datasets, datasets)
        self.assertIs(runtime.resource_manager, manager)
        self.assertIsNone(runtime.resource_admission_stop_event)

    def test_scheduler_drain_event_is_replaced_without_mutating_base_config(self):
        parsed = worker.build_parser().parse_args([])
        base = WorkerConfig.from_namespace(
            parsed,
            work_root=Path("/var/tmp/van-compute"),
            resource_manager=FakeResourceManager(),
        )
        stop_event = threading.Event()
        drain_event = threading.Event()

        class Remote:
            def __init__(self, worker_id):
                self.worker = worker_id

        scheduler = worker.PersistentScheduler(
            base,
            stop_event=stop_event,
            drain_event=drain_event,
            remote_factory=Remote,
        )

        self.assertIsNone(base.resource_admission_stop_event)
        self.assertIs(scheduler.args.resource_admission_stop_event, drain_event)
        self.assertIsNot(scheduler.args, base)

    def test_missed_offload_record_is_a_small_typed_attribute_record(self):
        record = MissedOffloadRecord(
            profile="repo-test",
            label="test",
            reason="worker-unavailable",
            duration_seconds=1.5,
            cpu_seconds=0.5,
            peak_rss_bytes=1024,
            input_bytes=128,
        )

        self.assertEqual(record.profile, "repo-test")
        self.assertEqual(record.input_bytes, 128)
        self.assertEqual(record.__class__.__name__, "MissedOffloadRecord")


if __name__ == "__main__":
    unittest.main()
