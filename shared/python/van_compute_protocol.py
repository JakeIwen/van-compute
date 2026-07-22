#!/usr/bin/env python3
"""Shared, offline-only task definitions for the van compute worker.

This module deliberately contains no SSH, queue, or CAN access.  Both the Pi
submitter and the Mac worker import it so they agree on the exact source files,
input counts, argument allowlist, and command line for every task.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import re
from typing import Sequence


SCHEMA_VERSION = 1


class ProtocolError(ValueError):
    """A task request does not conform to the shared protocol."""


@dataclass(frozen=True)
class TaskDefinition:
    name: str
    description: str
    entrypoint: str
    source_paths: tuple[str, ...]
    minimum_inputs: int
    maximum_inputs: int
    fixed_arguments: tuple[str, ...] = ()
    result_json: str | None = None
    input_values: bool = False


TASKS = {
    task.name: task
    for task in (
        TaskDefinition(
            name="can-capture-summary",
            description="Stream and summarize one saved candump log.",
            entrypoint="tools/can_capture_summary.py",
            source_paths=("tools/can_capture_summary.py",),
            minimum_inputs=1,
            maximum_inputs=1,
            result_json="summary.json",
        ),
        TaskDefinition(
            name="can-capture-compare",
            description="Compare two can-capture-summary JSON reports.",
            entrypoint="tools/can_capture_compare.py",
            source_paths=("tools/can_capture_compare.py",),
            minimum_inputs=2,
            maximum_inputs=2,
            result_json="comparison.json",
        ),
        TaskDefinition(
            name="can-field-finder",
            description="Rank scalar fields across two or more saved candump logs.",
            entrypoint="tools/can_field_finder.py",
            source_paths=("tools/can_field_finder.py",),
            minimum_inputs=2,
            maximum_inputs=16,
            input_values=True,
        ),
        TaskDefinition(
            name="signal-correlate-analyze",
            description="Run only the offline analyze half of signal_correlate.py.",
            entrypoint="tools/signal_correlate.py",
            source_paths=(
                "tools/__init__.py",
                "tools/signal_correlate.py",
                "tools/ecu_discover.py",
                "lib",
            ),
            minimum_inputs=1,
            maximum_inputs=32,
            fixed_arguments=("analyze",),
        ),
    )
}


def get_task(name: str) -> TaskDefinition:
    try:
        return TASKS[name]
    except KeyError:
        raise ProtocolError(f"unknown task {name!r}") from None


def _finite_number(text: str, option: str, *, minimum: float | None = None) -> None:
    try:
        value = float(text)
    except ValueError:
        raise ProtocolError(f"{option} requires a number") from None
    if not math.isfinite(value) or (minimum is not None and value < minimum):
        qualifier = f" at least {minimum:g}" if minimum is not None else ""
        raise ProtocolError(f"{option} must be a finite number{qualifier}")


def _option_value(arguments: Sequence[str], index: int, name: str) -> tuple[str, int]:
    argument = arguments[index]
    prefix = f"{name}="
    if argument.startswith(prefix):
        return argument[len(prefix) :], index + 1
    if argument == name and index + 1 < len(arguments):
        return arguments[index + 1], index + 2
    raise ProtocolError(f"{name} requires a value")


def validate_task_arguments(task_name: str, arguments: Sequence[str]) -> tuple[str, ...]:
    """Validate and normalize the small option set exposed for an offline task."""
    arguments = tuple(arguments)
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if task_name == "can-capture-summary" and argument == "--snapshot":
            index += 1
            continue
        if task_name == "can-capture-compare" and (
            argument == "--rate-factor" or argument.startswith("--rate-factor=")
        ):
            value, index = _option_value(arguments, index, "--rate-factor")
            _finite_number(value, "--rate-factor", minimum=1.0000001)
            continue
        if task_name == "can-capture-compare" and (
            argument == "--coverage-delta"
            or argument.startswith("--coverage-delta=")
        ):
            value, index = _option_value(arguments, index, "--coverage-delta")
            _finite_number(value, "--coverage-delta", minimum=0)
            if float(value) > 1:
                raise ProtocolError("--coverage-delta must be at most 1")
            continue
        if task_name == "can-field-finder" and argument.startswith("--top="):
            value = argument.partition("=")[2]
            if not value.isdigit() or not 1 <= int(value) <= 1000:
                raise ProtocolError("--top must be an integer from 1 through 1000")
            index += 1
            continue
        if task_name == "signal-correlate-analyze" and (
            argument in {"--ground", "--top", "--r2-min"}
            or any(argument.startswith(f"{name}=") for name in ("--ground", "--top", "--r2-min"))
        ):
            name = next(
                name
                for name in ("--ground", "--top", "--r2-min")
                if argument == name or argument.startswith(f"{name}=")
            )
            value, index = _option_value(arguments, index, name)
            if name == "--top":
                if not value.isdigit() or not 1 <= int(value) <= 10000:
                    raise ProtocolError("--top must be an integer from 1 through 10000")
            elif name == "--r2-min":
                _finite_number(value, name)
                if not -1 <= float(value) <= 1:
                    raise ProtocolError("--r2-min must be between -1 and 1")
            elif not re.fullmatch(r"[0-9A-Fa-f]{4}:\d+:\d+:[<>|=]?[A-Za-z0-9]+", value):
                raise ProtocolError("--ground has an invalid slice specification")
            continue
        raise ProtocolError(f"argument {argument!r} is not allowed for {task_name}")
    return arguments


def validate_inputs(
    task_name: str,
    inputs: Sequence[dict[str, object]],
) -> TaskDefinition:
    task = get_task(task_name)
    if not task.minimum_inputs <= len(inputs) <= task.maximum_inputs:
        expected = (
            str(task.minimum_inputs)
            if task.minimum_inputs == task.maximum_inputs
            else f"{task.minimum_inputs}-{task.maximum_inputs}"
        )
        raise ProtocolError(f"{task_name} requires {expected} input file(s)")
    for item in inputs:
        value = item.get("value")
        if value is not None:
            if not task.input_values:
                raise ProtocolError(f"{task_name} does not accept input values")
            _finite_number(str(value), "input value")
    values = [item.get("value") for item in inputs]
    if task.input_values and any(value is not None for value in values) and any(
        value is None for value in values
    ):
        raise ProtocolError("provide a value for every input or for none of them")
    return task


def build_command(
    task_name: str,
    *,
    python: str,
    source_root: Path,
    input_paths: Sequence[Path],
    input_values: Sequence[object | None],
    result_root: Path,
    arguments: Sequence[str],
) -> list[str]:
    """Build one shell-free command after repeating all protocol validation."""
    task_inputs = [
        {"value": value} for value in input_values
    ]
    task = validate_inputs(task_name, task_inputs)
    normalized = validate_task_arguments(task_name, arguments)
    entrypoint = source_root / task.entrypoint
    command = [python, str(entrypoint), *task.fixed_arguments]
    for path, value in zip(input_paths, input_values):
        rendered = str(path)
        if value is not None:
            rendered = f"{rendered}={value}"
        command.append(rendered)
    command.extend(normalized)
    if task.result_json:
        command.extend(("--json", str(result_root / task.result_json)))
    return command
