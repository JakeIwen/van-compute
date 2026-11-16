import contextlib
import dataclasses
from io import StringIO
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from van_compute import broker, config, pi_health, queue
from van_compute.can_activity import CanProbe, CanQuietTracker
from van_compute.config import HealthThresholds
from van_compute.pi_health import HealthProbeError
from van_compute.pi_health_monitor import HealthMonitor
from van_compute.pi_rates import Rates, RateSampler

from pi.tests.compute.van_compute_test_fixtures import BrokerHarness, GOOD_HEALTH

GIB = 1024**3
QUIET = CanProbe(False, (), None)

# The Pi at idle on 2026-11-14: 2.4 GiB available, 0.86 GiB of 3.9 GiB swap
# used, about 1 page/s of swap-in, 34% busy (iowait is about another 34%),
# load 5.97 from backup I/O wait, 50 C, no throttle flags.
IDLE_PI = pi_health.HealthSnapshot(
    memory_available_bytes=int(2.4 * GIB),
    swap_total_bytes=int(3.9 * GIB),
    swap_used_bytes=int(0.86 * GIB),
    load_1m=5.97,
    cpu_count=4,
    temperature_c=50.0,
    throttled_flags=0,
    can_active=False,
    can_held_locks=(),
    can_quiet_seconds=3600.0,
    can_error=None,
    swap_pages_per_second=1.0,
    cpu_busy_fraction=0.34,
)


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FixedRates:
    def sample(self):
        return Rates(1.0, 0.34)


def host_readers(**overrides):
    """Patch the /proc, sysfs and vcgencmd readers with the idle Pi."""
    values = {
        "_read_meminfo": {
            "MemAvailable": int(2.4 * GIB),
            "SwapTotal": int(3.9 * GIB),
            "SwapFree": int(3.04 * GIB),
        },
        "_read_temperature": 50.0,
        "_read_throttled": 0,
    }
    values.update(overrides)
    stack = contextlib.ExitStack()
    for name, value in values.items():
        stack.enter_context(mock.patch.object(pi_health, name, return_value=value))
    return stack


class SamplerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name)
        self.vmstat, self.stat = base / "vmstat", base / "stat"
        self.clock = FakeClock()
        self.window = None  # what the fake sleep writes while the clock moves
        self.write(0, 0, [0] * 10)
        self.sampler = RateSampler(
            vmstat_path=self.vmstat,
            stat_path=self.stat,
            clock=self.clock,
            sleep=self.sleep,
        )

    def write(self, swap_in, swap_out, cpu):
        self.vmstat.write_text(
            f"nr_free_pages 1\npswpin {swap_in}\npswpout {swap_out}\npgfault 9\n",
            encoding="ascii",
        )
        self.stat.write_text(
            "cpu  " + " ".join(map(str, cpu)) + "\ncpu0 1 2 3\n", encoding="ascii"
        )

    def sleep(self, seconds):
        self.clock.sleep(seconds)
        if self.window is not None:
            self.write(*self.window)

    def test_first_sample_measures_a_one_second_window(self):
        self.window = (100, 28, [100, 0, 50, 300, 400, 20, 10, 20, 7, 7])
        rates = self.sampler.sample()
        self.assertEqual(self.clock.sleeps, [1.0])
        self.assertEqual(rates.swap_pages_per_second, 128.0)

    def test_busy_excludes_idle_iowait_and_guest_time(self):
        self.window = (0, 0, [100, 0, 50, 300, 400, 20, 10, 20, 999, 999])
        rates = self.sampler.sample()
        # busy = 100+50+20+10+20 of a total of 900; guest fields add nothing
        # and iowait stays in the denominator.
        self.assertAlmostEqual(rates.cpu_busy_fraction, 200 / 900)

    def test_next_sample_uses_the_delta_since_the_previous_one(self):
        self.window = (10, 0, [10, 0, 0, 90, 0, 0, 0, 0, 0, 0])
        self.sampler.sample()
        self.clock.sleeps.clear()
        self.clock.now += 5.0  # five seconds since the window's second read
        self.write(60, 0, [60, 0, 0, 140, 0, 0, 0, 0, 0, 0])
        rates = self.sampler.sample()
        self.assertEqual(self.clock.sleeps, [])
        self.assertEqual(rates.swap_pages_per_second, 10.0)
        self.assertAlmostEqual(rates.cpu_busy_fraction, 0.5)

    def test_stale_previous_sample_takes_a_fresh_window(self):
        self.window = (0, 0, [1, 0, 0, 99, 0, 0, 0, 0, 0, 0])
        self.sampler.sample()
        self.clock.sleeps.clear()
        self.clock.now += 31.0
        self.window = (500, 0, [52, 0, 0, 248, 0, 0, 0, 0, 0, 0])
        self.write(10, 0, [2, 0, 0, 198, 0, 0, 0, 0, 0, 0])
        rates = self.sampler.sample()
        self.assertEqual(self.clock.sleeps, [1.0])
        self.assertEqual(rates.swap_pages_per_second, 490.0)

    def test_a_counter_that_went_down_takes_a_fresh_window(self):
        self.window = (1000, 0, [10, 0, 0, 90, 0, 0, 0, 0, 0, 0])
        self.sampler.sample()
        self.clock.sleeps.clear()
        # Time must pass, or the zero-elapsed window would force the re-window
        # by itself and the decrease path would go untested.
        self.clock.now += 5.0
        self.write(5, 0, [20, 0, 0, 180, 0, 0, 0, 0, 0, 0])  # swap counter reset
        self.window = (105, 0, [30, 0, 0, 270, 0, 0, 0, 0, 0, 0])
        rates = self.sampler.sample()
        self.assertEqual(self.clock.sleeps, [1.0])
        self.assertEqual(rates.swap_pages_per_second, 100.0)

    def assert_decrease_takes_a_fresh_window(self, later, fresh):
        self.window = (100, 100, [100, 0, 100, 700, 0, 0, 0, 0, 0, 0])
        self.sampler.sample()
        self.clock.sleeps.clear()
        self.clock.now += 5.0
        self.write(*later)
        self.window = fresh
        rates = self.sampler.sample()
        self.assertEqual(self.clock.sleeps, [1.0])
        return rates

    def test_a_swap_counter_decrease_is_not_masked_by_the_other_one_rising(self):
        # pswpin fell by 100 while pswpout rose by 600: the sum rises by 500.
        rates = self.assert_decrease_takes_a_fresh_window(
            (0, 700, [200, 0, 100, 800, 0, 0, 0, 0, 0, 0]),
            (10, 710, [250, 0, 100, 850, 0, 0, 0, 0, 0, 0]),
        )
        self.assertEqual(rates.swap_pages_per_second, 20.0)

    def test_a_cpu_field_decrease_is_not_masked_by_another_rising(self):
        # user fell by 50 while system rose by 150: busy and total both rise.
        rates = self.assert_decrease_takes_a_fresh_window(
            (100, 100, [50, 0, 250, 800, 0, 0, 0, 0, 0, 0]),
            (100, 100, [100, 0, 250, 850, 0, 0, 0, 0, 0, 0]),
        )
        self.assertAlmostEqual(rates.cpu_busy_fraction, 0.5)

    def test_a_negative_counter_is_a_probe_failure(self):
        self.vmstat.write_text("pswpin -5\npswpout 1\n", encoding="ascii")
        with self.assertRaises(HealthProbeError):
            self.sampler.sample()
        self.write(0, 0, [0] * 10)
        self.stat.write_text("cpu -1 2 3 4 5 6 7 8\n", encoding="ascii")
        with self.assertRaises(HealthProbeError):
            self.sampler.sample()

    def test_two_invalid_windows_raise(self):
        self.window = None  # counters never move, so no CPU time elapses
        with self.assertRaises(HealthProbeError):
            self.sampler.sample()

    def test_missing_fields_raise_instead_of_reporting_health(self):
        for vmstat, stat in (
            ("pswpin 5\n", "cpu 1 2 3 4 5 6 7 8\n"),
            ("pswpin 5\npswpout 1\n", "cpu 1 2 3 4 5\n"),
            ("pswpin 5\npswpout x\n", "cpu 1 2 3 4 5 6 7 8\n"),
            ("pswpin 5\npswpout 1\n", "intr 1 2 3 4 5 6 7 8\n"),
        ):
            with self.subTest(vmstat=vmstat, stat=stat):
                self.vmstat.write_text(vmstat, encoding="ascii")
                self.stat.write_text(stat, encoding="ascii")
                with self.assertRaises(HealthProbeError):
                    RateSampler(
                        vmstat_path=self.vmstat,
                        stat_path=self.stat,
                        clock=self.clock,
                        sleep=self.clock.sleep,
                    ).sample()
        with self.assertRaises(HealthProbeError):
            RateSampler(
                vmstat_path=self.vmstat.parent / "absent",
                stat_path=self.stat,
                clock=self.clock,
                sleep=self.clock.sleep,
            ).sample()


class GateTests(unittest.TestCase):
    thresholds = HealthThresholds()

    def reasons(self, **changes):
        snapshot = dataclasses.replace(IDLE_PI, **changes)
        return pi_health.assess_health(snapshot, self.thresholds).reasons

    def test_the_idle_pi_baseline_passes(self):
        assessment = pi_health.assess_health(IDLE_PI, self.thresholds)
        self.assertTrue(assessment.ok, assessment.reasons)

    def test_each_new_limit_defers_with_a_named_reason(self):
        cases = {
            "memory": (dict(memory_available_bytes=GIB - 1), "available memory"),
            "swap": (dict(swap_pages_per_second=1025.0), "swap paging"),
            "cpu": (dict(cpu_busy_fraction=0.91), "CPU busy"),
            "temperature": (dict(temperature_c=75.5), "temperature"),
            "throttle": (dict(throttled_flags=0x4), "throttle flags"),
            "can": (
                dict(can_active=True, can_held_locks=("active-diagnostics-can0.lock",)),
                "CAN transmit lock held: active-diagnostics-can0.lock",
            ),
            "can probe": (dict(can_error="CAN lock probe failed: boom"), "boom"),
            "quiet": (dict(can_quiet_seconds=59.0), "CAN quiet for 59 s"),
        }
        for name, (changes, text) in cases.items():
            with self.subTest(name):
                reasons = self.reasons(**changes)
                self.assertEqual(len(reasons), 1, reasons)
                self.assertIn(text, reasons[0])

    def test_limits_are_inclusive_at_the_boundary(self):
        self.assertEqual(
            self.reasons(
                memory_available_bytes=GIB,
                swap_pages_per_second=1024.0,
                cpu_busy_fraction=0.90,
                temperature_c=75.0,
                can_quiet_seconds=60.0,
            ),
            (),
        )

    def test_load_and_swap_used_no_longer_gate(self):
        self.assertEqual(
            self.reasons(load_1m=400.0, swap_used_bytes=IDLE_PI.swap_total_bytes), ()
        )

    def test_zero_quiet_requirement_still_defers_on_a_held_lock(self):
        relaxed = HealthThresholds(can_quiet_seconds=0.0)
        snapshot = dataclasses.replace(
            IDLE_PI, can_quiet_seconds=0.0, can_active=True, can_held_locks=("a.lock",)
        )
        self.assertFalse(pi_health.assess_health(snapshot, relaxed).ok)
        self.assertTrue(
            pi_health.assess_health(
                dataclasses.replace(IDLE_PI, can_quiet_seconds=0.0), relaxed
            ).ok
        )


class FlagTests(unittest.TestCase):
    def validate(self, *arguments):
        return broker._validate_args(broker.build_parser().parse_args(list(arguments)))

    def test_defaults_come_from_the_single_threshold_definition(self):
        runtime = self.validate("--once")
        self.assertEqual(runtime.health_thresholds, HealthThresholds())
        self.assertEqual(
            HealthThresholds(),
            HealthThresholds(
                minimum_available_bytes=1024 * 1024**2,
                maximum_swap_pages_per_second=1024.0,
                maximum_cpu_busy_fraction=0.90,
                maximum_temperature_c=75.0,
                can_quiet_seconds=60.0,
            ),
        )
        self.assertEqual(runtime.can_lock_dir, config.DEFAULT_CAN_LOCK_DIR)
        self.assertEqual(
            str(runtime.can_lock_dir), "/home/pi/dev/obd-things/tmp/locks"
        )
        self.assertEqual(runtime.can_quiet_seconds, 60.0)

    def test_new_flags_reach_the_thresholds(self):
        runtime = self.validate(
            "--min-available-memory-mb", "900",
            "--max-swap-pages-per-second", "50",
            "--max-cpu-busy", "0.5",
            "--can-quiet-seconds", "7",
            "--can-lock-dir", "/tmp/elsewhere",
        )
        self.assertEqual(
            runtime.health_thresholds,
            HealthThresholds(
                minimum_available_bytes=900 * 1024**2,
                maximum_swap_pages_per_second=50.0,
                maximum_cpu_busy_fraction=0.5,
                can_quiet_seconds=7.0,
            ),
        )
        self.assertEqual(runtime.can_lock_dir, Path("/tmp/elsewhere"))

    def test_removed_flags_fail_naming_the_replacement(self):
        for flag, value, replacement in (
            ("--max-swap-used", "0.2", "--max-swap-pages-per-second"),
            ("--max-swap-used-mb", "512", "--max-swap-pages-per-second"),
            ("--max-load-per-cpu", "1.25", "--max-cpu-busy"),
        ):
            with self.subTest(flag):
                with self.assertRaises(broker.BrokerError) as caught:
                    self.validate(flag, value)
                self.assertIn(flag, str(caught.exception))
                self.assertIn(replacement, str(caught.exception))

    def test_invalid_values_are_rejected(self):
        bad = (
            ("--min-available-memory-mb", "255"),
            # below the job's 768 MiB address-space ceiling
            ("--min-available-memory-mb", "512"),
            ("--max-swap-pages-per-second", "0"),
            ("--max-swap-pages-per-second", "inf"),
            ("--max-swap-pages-per-second", "nan"),
            ("--max-cpu-busy", "0"),
            ("--max-cpu-busy", "1.01"),
            ("--max-cpu-busy", "nan"),
            ("--can-quiet-seconds", "-1"),
            ("--can-quiet-seconds", "3601"),
            ("--can-quiet-seconds", "nan"),
            ("--can-quiet-seconds", "inf"),
            ("--max-temperature-c", "nan"),
        )
        for flag, value in bad:
            with self.subTest(flag=flag, value=value):
                with self.assertRaises(broker.BrokerError):
                    self.validate(flag, value)

    def test_boundary_values_are_accepted(self):
        runtime = self.validate(
            "--min-available-memory-mb", "768",
            "--max-cpu-busy", "1",
            "--can-quiet-seconds", "0",
        )
        self.assertEqual(runtime.health_thresholds.can_quiet_seconds, 0.0)
        self.assertTrue(math.isfinite(runtime.health_thresholds.maximum_cpu_busy_fraction))

    def test_health_is_exclusive_with_once_and_self_test(self):
        for other in ("--once", "--self-test"):
            with self.subTest(other):
                with contextlib.redirect_stderr(StringIO()), self.assertRaises(
                    SystemExit
                ):
                    broker.build_parser().parse_args(["--health", other])


class MonitorTests(BrokerHarness, unittest.TestCase):
    def monitor(self, probe, clock=None):
        clock = clock or FakeClock()
        return HealthMonitor(
            Path("/unused"),
            probe=probe,
            sampler=FixedRates(),
            clock=clock,
            sleep=clock.sleep,
        ), clock

    def run_gate(self, monitor):
        with host_readers(), mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ):
            return broker.run_once(self.args, monitor=monitor)

    def test_fresh_remote_lease_polls_still_observe_can(self):
        calls = []
        monitor, clock = self.monitor(lambda _dir: calls.append(1) or QUIET)
        queue.worker_heartbeat(
            queue.safe_root(self.root), "m4mac", slots_total=10, slots_busy=0
        )
        for _ in range(3):
            clock.now += 5
            result = broker.run_once(
                self.args, health_reader=lambda: GOOD_HEALTH, monitor=monitor
            )
            self.assertEqual(result["reason"], "fresh remote worker lease")
        self.assertEqual(len(calls), 3)
        self.assertEqual(monitor.tracker.quiet_seconds(clock()), 10.0)

    def test_maintenance_polls_observe_can_too(self):
        calls = []
        monitor, _clock = self.monitor(lambda _dir: calls.append(1) or QUIET)
        with mock.patch.object(queue, "maintenance_status", return_value="busy"):
            result = broker.run_once(
                self.args, health_reader=lambda: GOOD_HEALTH, monitor=monitor
            )
        self.assertEqual(result["reason"], "queue maintenance is active")
        self.assertEqual(len(calls), 1)

    def test_run_once_needs_a_monitor_or_a_reader(self):
        with self.assertRaises(broker.BrokerError):
            broker.run_once(self.args)

    def test_a_held_lock_defers_before_any_claim(self):
        submitted = self.submit()
        held = CanProbe(True, ("active-diagnostics-can0.lock",), None)
        monitor, _clock = self.monitor(lambda _dir: held)
        result = self.run_gate(monitor)
        self.assertEqual(result["action"], "deferred")
        self.assertEqual(result["reason"], "Pi health gate")
        self.assertIn("CAN transmit lock held", result["health"]["reasons"][0])
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())

    def test_a_restart_waits_one_quiet_period_then_runs(self):
        submitted = self.submit()
        monitor, clock = self.monitor(lambda _dir: QUIET)
        first = self.run_gate(monitor)
        self.assertEqual(first["reason"], "Pi health gate")
        self.assertIn("CAN quiet for 0 s", first["health"]["reasons"][0])
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())
        for _ in range(13):  # 65 s of 5 s polls
            clock.now += 5
            monitor.observe_can()
        with host_readers(), mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ), mock.patch.object(broker, "bubblewrap_command", side_effect=self.no_sandbox):
            result = broker.run_once(self.args, monitor=monitor)
        self.assertEqual(result["action"], "executed", result)

    def test_probe_exceptions_fail_closed(self):
        def boom(_dir):
            raise RuntimeError("proc vanished")

        monitor, _clock = self.monitor(boom)
        result = self.run_gate(monitor)
        self.assertEqual(result["reason"], "Pi health gate")
        self.assertIn("proc vanished", result["health"]["reasons"][0])

    def test_a_sampler_failure_is_a_probe_failure(self):
        class Broken:
            def sample(self):
                raise HealthProbeError("no counters")

        monitor = HealthMonitor(Path("/unused"), probe=lambda _d: QUIET, sampler=Broken())
        with host_readers(), mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ):
            result = broker.run_once(self.args, monitor=monitor)
        self.assertEqual(result["action"], "deferred")
        self.assertIn("health probe failed", result["reason"])

    def test_wait_for_quiet_stops_at_the_window_or_the_first_lock(self):
        monitor, clock = self.monitor(lambda _dir: QUIET)
        monitor.wait_for_quiet(2.0)
        self.assertEqual(clock.sleeps, [0.5] * 4)
        answers = iter([QUIET, QUIET, CanProbe(True, ("x.lock",), None)])
        monitor, clock = self.monitor(lambda _dir: next(answers))
        monitor.wait_for_quiet(60.0)
        self.assertEqual(len(clock.sleeps), 2)
        self.assertEqual(monitor.tracker.quiet_seconds(clock()), 0.0)
        monitor, clock = self.monitor(lambda _dir: QUIET)
        monitor.wait_for_quiet(0.0)
        self.assertEqual(clock.sleeps, [])


class CommandLineTests(BrokerHarness, unittest.TestCase):
    def arguments(self, *extra):
        return [
            "--root", str(self.root),
            "--work-root", str(self.work),
            "--python", __import__("sys").executable,
            "--bwrap", __import__("sys").executable,
            *extra,
        ]

    def main(self, arguments, probe=lambda _dir: QUIET, sampler=None, readers=None):
        clock = FakeClock()
        made = []

        def factory(lock_dir, **startup):
            made.append(lock_dir)
            return HealthMonitor(
                lock_dir,
                probe=probe,
                sampler=sampler or FixedRates(),
                clock=clock,
                sleep=clock.sleep,
                **startup,
            )

        output = StringIO()
        with (readers or host_readers()), mock.patch.object(
            broker, "HealthMonitor", factory
        ), mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ), contextlib.redirect_stdout(output):
            code = broker.main(arguments)
        return code, output.getvalue(), clock, made

    def test_health_prints_json_and_exits_zero_even_when_the_gate_fails(self):
        code, output, _clock, made = self.main(self.arguments("--health"))
        self.assertEqual(code, 0)
        report = json.loads(output)
        self.assertFalse(report["ok"])  # a fresh process has not seen 60 s of quiet
        self.assertEqual(report["snapshot"]["swap_pages_per_second"], 1.0)
        self.assertEqual(report["thresholds"]["can_quiet_seconds"], 60.0)
        self.assertEqual(made, [config.DEFAULT_CAN_LOCK_DIR])

        held = CanProbe(True, ("active-diagnostics-can1.lock",), None)
        code, output, _clock, _made = self.main(
            self.arguments("--health", "--can-quiet-seconds", "0"), probe=lambda _d: held
        )
        report = json.loads(output)
        self.assertEqual(code, 0)
        self.assertEqual(report["snapshot"]["can_held_locks"], ["active-diagnostics-can1.lock"])
        self.assertIn("CAN transmit lock held", report["reasons"][0])

        code, output, _clock, _made = self.main(
            self.arguments("--health", "--can-quiet-seconds", "0")
        )
        self.assertTrue(json.loads(output)["ok"])

    def test_health_works_while_the_service_holds_the_broker_lock(self):
        with broker.broker_lock(self.root):
            code, output, _clock, _made = self.main(self.arguments("--health"))
        self.assertEqual(code, 0)
        self.assertIn("snapshot", json.loads(output))

    def test_once_waits_out_the_quiet_window_before_deciding(self):
        code, output, clock, _made = self.main(
            self.arguments("--once", "--can-quiet-seconds", "2")
        )
        self.assertEqual(code, 0)
        self.assertEqual(clock.sleeps[:4], [0.5] * 4)
        self.assertEqual(json.loads(output)["reason"], "no eligible queued jobs")

    def test_once_with_zero_window_does_not_wait(self):
        code, output, clock, _made = self.main(
            self.arguments("--once", "--can-quiet-seconds", "0")
        )
        self.assertEqual(code, 0)
        self.assertEqual(clock.sleeps, [])

    def test_once_stops_waiting_and_defers_when_a_lock_appears(self):
        self.submit()
        answers = iter([QUIET, QUIET])

        def probe(_dir):
            return next(answers, CanProbe(True, ("active-diagnostics-can0.lock",), None))

        code, output, clock, _made = self.main(
            self.arguments("--once", "--can-quiet-seconds", "60"), probe=probe
        )
        self.assertEqual(code, 0)
        self.assertLess(len(clock.sleeps), 5)
        result = json.loads(output)
        self.assertEqual(result["action"], "deferred")
        self.assertIn("active-diagnostics-can0.lock", result["reason"])

    def test_once_defers_when_only_its_first_wait_saw_activity(self):
        # With no quiet window to wait out, the wait ends on the first (active)
        # observation; the later quiet ones inside run_once must not undo that.
        job = self.submit()
        answers = iter([CanProbe(True, ("active-diagnostics-can0.lock",), None)])
        code, output, _clock, _made = self.main(
            self.arguments("--once", "--can-quiet-seconds", "0"),
            probe=lambda _dir: next(answers, QUIET),
        )
        self.assertEqual(code, 0)
        result = json.loads(output)
        self.assertEqual(result["action"], "deferred")
        self.assertIn("CAN not quiet", result["reason"])
        self.assertTrue((self.root / "queued" / job["id"]).is_dir())

    def test_health_reports_a_failed_rate_read_as_unhealthy_json(self):
        class Broken:
            def sample(self):
                raise HealthProbeError("no counters")

        code, output, _clock, _made = self.main(
            self.arguments("--health"), sampler=Broken()
        )
        self.assertEqual(code, 0)
        report = json.loads(output)
        self.assertFalse(report["ok"])
        self.assertIn("no counters", report["reasons"][0])
        self.assertIn("thresholds", report)

    def test_health_reports_a_failed_memory_read_as_unhealthy_json(self):
        readers = mock.patch.object(
            pi_health, "_read_meminfo", side_effect=HealthProbeError("no meminfo")
        )
        code, output, _clock, _made = self.main(
            self.arguments("--health"), readers=readers
        )
        self.assertEqual(code, 0)
        report = json.loads(output)
        self.assertFalse(report["ok"])
        self.assertIn("no meminfo", report["reasons"][0])

    def test_health_with_invalid_arguments_still_exits_nonzero(self):
        error = StringIO()
        with contextlib.redirect_stderr(error):
            code, output, _clock, _made = self.main(
                self.arguments("--health", "--timeout", "0")
            )
        self.assertEqual(code, 2)
        self.assertEqual(output, "")

    def test_old_gate_flags_exit_with_the_replacement_named(self):
        error = StringIO()
        with contextlib.redirect_stderr(error):
            code = broker.main(self.arguments("--once", "--max-load-per-cpu", "2"))
        self.assertEqual(code, 2)
        self.assertIn("--max-cpu-busy", error.getvalue())


if __name__ == "__main__":
    unittest.main()
