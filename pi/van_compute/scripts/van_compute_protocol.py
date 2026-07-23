#!/usr/bin/env python3
"""Shell-free protocol shared by the van-compute queue and workers.

Four original CAN-analysis tasks remain built in for compatibility.  A source
repository may additionally declare offline jobs in ``.van-compute.json``.
Those declarations choose one of a small set of executable families and use
argument vectors with explicit path placeholders; they never contain a shell
command string.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import re
from typing import Mapping, Sequence


SCHEMA_VERSION = 1
# Increment this whenever a worker built from an older checkout must not use the
# current queue RPCs.  Unlike SCHEMA_VERSION, this covers the worker/queue
# command contract rather than the shape of repository task declarations.
WORKER_PROTOCOL_VERSION = 1
REPO_MANIFEST = ".van-compute.json"
MAX_REPO_TASKS = 128
MAX_SOURCE_PATHS = 512
MAX_ARGUMENTS = 256
MAX_OUTPUTS = 64
MAX_DATASETS = 16
MAX_INPUTS = 256
MAX_TOKEN_LENGTH = 4096
MAX_RESULT_BYTES = 128 * 1024 * 1024


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


@dataclass(frozen=True)
class RepoTaskDefinition:
    name: str
    description: str
    profile: str
    family: str
    source_paths: tuple[str, ...]
    minimum_inputs: int
    maximum_inputs: int
    argv: tuple[str, ...]
    outputs: tuple[str, ...]
    datasets: tuple[str, ...]
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


# A profile fixes the only executable family a repository declaration may use.
# In particular, CAN/SocketCAN, ADB, SSH, service managers, and shells are not
# executable families in this protocol.
PROFILE_FAMILIES = {
    "repo-test": "python",
    "python-script": "python",
    "python-module": "python",
    "apk-analyze": "jadx",
    "sqlite-readonly": "sqlite3",
    "corpus-search": "rg",
    "can-log-batch": "python",
}

PROFILE_DESCRIPTIONS = {
    "repo-test": "Run a repository test selection with Python pytest.",
    "python-script": "Run one snapshotted offline Python entrypoint.",
    "python-module": "Run one snapshotted offline Python module.",
    "apk-analyze": "Decode a submitted APK with jadx into the result directory.",
    "sqlite-readonly": "Query a submitted SQLite database in read-only mode.",
    "corpus-search": "Search staged source, inputs, or a read-only dataset with ripgrep.",
    "can-log-batch": "Run offline Python analysis over submitted CAN log files.",
}

_TOP_LEVEL_KEYS = {"schema_version", "tasks"}
_TASK_KEYS = {
    "name",
    "description",
    "profile",
    "source_paths",
    "minimum_inputs",
    "maximum_inputs",
    "argv",
    "outputs",
    "datasets",
    "input_values",
}
_REQUIRED_TASK_KEYS = _TASK_KEYS - {"description", "input_values", "datasets"}
_EXECUTION_KEYS = {
    "profile",
    "family",
    "argv",
    "outputs",
    "datasets",
    "minimum_inputs",
    "maximum_inputs",
    "input_values",
}
_NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
_DATASET_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}")
_MODULE_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
_PLACEHOLDER_RE = re.compile(r"\{(source|input|result|dataset):([^{}]+)\}")
_RESULT_PATH_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.-]*(?:/[A-Za-z0-9][A-Za-z0-9_.-]*)*"
)
_SQLITE_OUTPUT_OPTIONS = frozenset(
    {
        "-ascii",
        "-box",
        "-column",
        "-csv",
        "-header",
        "-html",
        "-json",
        "-line",
        "-list",
        "-markdown",
        "-noheader",
        "-quote",
        "-table",
        "-tabs",
    }
)
_IMPLICIT_RESULT_PATHS = tuple(
    Path(name) for name in ("stdout.txt", "stderr.txt", "execution.json")
)


def _unexpected_keys(payload: Mapping[str, object], allowed: set[str], context: str) -> None:
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        raise ProtocolError(f"{context} has unknown field(s): {', '.join(unexpected)}")


def _relative_path(text: object, context: str) -> str:
    if not isinstance(text, str) or not text or len(text) > MAX_TOKEN_LENGTH:
        raise ProtocolError(f"{context} must be a non-empty relative path")
    if "\x00" in text or "\n" in text or "\r" in text or "\\" in text:
        raise ProtocolError(f"{context} is not a safe relative path")
    path = Path(text)
    if path.is_absolute() or text.startswith("~") or any(part in {"", ".", ".."} for part in path.parts):
        raise ProtocolError(f"{context} is not a safe relative path")
    return path.as_posix()


def _source_path(text: object, context: str) -> str:
    relative = _relative_path(text, context)
    if relative == ".":
        raise ProtocolError(f"{context} cannot snapshot the repository root")
    if relative == REPO_MANIFEST:
        raise ProtocolError(
            f"{context} cannot declare {REPO_MANIFEST}; it is snapshotted automatically"
        )
    if any(part in {".git", "tmp", "__pycache__", ".pytest_cache"} for part in Path(relative).parts):
        raise ProtocolError(f"{context} cannot snapshot generated or repository-control directories")
    return relative


def _validate_source_roots(source_paths: Sequence[str], context: str) -> None:
    if len(set(source_paths)) != len(source_paths):
        raise ProtocolError(f"{context} contains duplicates")
    paths = [Path(source) for source in source_paths]
    for index, path in enumerate(paths):
        if any(
            other in path.parents
            for other_index, other in enumerate(paths)
            if other_index != index
        ):
            raise ProtocolError(f"{context} contains ancestor-overlapping paths")


def _output_path(text: object, context: str) -> str:
    relative = _relative_path(text, context)
    if not _RESULT_PATH_RE.fullmatch(relative):
        raise ProtocolError(
            f"{context} must use portable letters, digits, dots, underscores, hyphens, and slashes"
        )
    return relative


def _validate_output_roots(outputs: Sequence[str], context: str) -> None:
    if len(set(outputs)) != len(outputs):
        raise ProtocolError(f"{context} contains duplicates")
    paths = [Path(output) for output in outputs]
    if any(
        path == implicit or implicit in path.parents or path in implicit.parents
        for path in paths
        for implicit in _IMPLICIT_RESULT_PATHS
    ):
        raise ProtocolError(f"{context} overlaps an implicit worker result")
    for index, path in enumerate(paths):
        if any(
            other in path.parents
            for other_index, other in enumerate(paths)
            if other_index != index
        ):
            raise ProtocolError(f"{context} contains ancestor-overlapping paths")


def _bounded_string(value: object, context: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ProtocolError(f"{context} must be a non-empty string of at most {maximum} characters")
    if any(character in value for character in ("\x00", "\r", "\n")):
        raise ProtocolError(f"{context} contains a control character")
    return value


def _string_array(
    value: object,
    context: str,
    maximum: int,
    *,
    allow_empty: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > maximum or (not allow_empty and not value):
        qualifier = "1-" if not allow_empty else "0-"
        raise ProtocolError(f"{context} must be an array of {qualifier}{maximum} strings")
    result: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or len(item) > MAX_TOKEN_LENGTH:
            raise ProtocolError(f"{context}[{index}] must be a string of at most {MAX_TOKEN_LENGTH} characters")
        result.append(item)
    return tuple(result)


def _validate_literal_argument(
    argument: str,
    context: str,
    *,
    reject_braces: bool = True,
) -> None:
    if not argument or len(argument) > MAX_TOKEN_LENGTH:
        raise ProtocolError(f"{context} must be a non-empty string")
    if any(character in argument for character in ("\x00", "\r", "\n")):
        raise ProtocolError(f"{context} contains a control character")
    if reject_braces and ("{" in argument or "}" in argument):
        raise ProtocolError(f"{context} contains an unrecognized placeholder")
    # Absolute filesystem arguments would bypass the staged source/input roots.
    # A leading slash in an ordinary regex can be expressed as ``[/]``.
    path_candidate = argument.partition("=")[2] if "=" in argument else argument
    if path_candidate.startswith(("/", "~")) or ".." in Path(path_candidate).parts:
        raise ProtocolError(f"{context} cannot reference a path outside the staged job")


def _validate_profile_arguments(profile: str, arguments: Sequence[str]) -> None:
    """Block options that make an otherwise offline family execute commands."""
    lowered = [argument.strip().lower() for argument in arguments]
    if profile == "corpus-search" and any(
        argument in {"--pre", "--pre-glob", "--hostname-bin"}
        or argument.startswith(("--pre=", "--pre-glob=", "--hostname-bin="))
        for argument in lowered
    ):
        raise ProtocolError("corpus-search cannot use ripgrep command-execution helpers")
    if profile == "sqlite-readonly":
        if len(arguments) != 1:
            raise ProtocolError("sqlite-readonly requires exactly one SQL argument")
        _validate_readonly_sql(arguments[0], "sqlite-readonly query")


def _validate_readonly_sql(query: str, context: str) -> None:
    _validate_literal_argument(query, context, reject_braces=False)
    stripped = query.strip()
    if stripped.startswith(("-", ".")):
        raise ProtocolError("sqlite-readonly rejects option-like and dot-command arguments")
    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    if not stripped or ";" in stripped:
        raise ProtocolError("sqlite-readonly requires exactly one SQL statement")
    if not re.match(r"(?is)^(?:select|with|explain)\b", stripped):
        raise ProtocolError("sqlite-readonly SQL must begin with SELECT, WITH, or EXPLAIN")
    if re.search(
        r"(?i)\b(attach|detach|vacuum|insert|update|delete|replace|create|drop|alter|"
        r"reindex|analyze|pragma|load_extension|readfile|writefile)\b",
        stripped,
    ):
        raise ProtocolError("sqlite-readonly accepts read-only SQL only")


def _validate_sqlite_shape(
    argv: Sequence[str],
    *,
    minimum_inputs: int,
    maximum_inputs: int,
    input_values: bool,
    outputs: Sequence[str],
    datasets: Sequence[str],
) -> None:
    if minimum_inputs != 1 or maximum_inputs != 1 or input_values:
        raise ProtocolError("sqlite-readonly requires exactly one unvalued input database")
    if outputs or datasets:
        raise ProtocolError(
            "sqlite-readonly writes only captured stdout and cannot declare outputs or datasets"
        )
    if len(argv) < 2 or argv[-2] != "{input:0}":
        raise ProtocolError(
            "sqlite-readonly argv must end with {input:0} followed by one query"
        )
    options = argv[:-2]
    if any(option not in _SQLITE_OUTPUT_OPTIONS for option in options):
        raise ProtocolError("sqlite-readonly manifest options must be safe output-format options")
    query = argv[-1]
    if query != "{arguments}":
        _validate_readonly_sql(query, "sqlite-readonly fixed query")


def _output_contains(outputs: Sequence[str], relative: str) -> bool:
    candidate = Path(relative)
    return any(candidate == Path(root) or Path(root) in candidate.parents for root in outputs)


def _source_contains(source_paths: Sequence[str], relative: str) -> bool:
    candidate = Path(relative)
    return any(candidate == Path(root) or Path(root) in candidate.parents for root in source_paths)


def _validate_argv(
    argv: Sequence[str],
    *,
    source_paths: Sequence[str] | None = None,
    maximum_inputs: int | None = None,
    outputs: Sequence[str] = (),
    datasets: Sequence[str] = (),
) -> None:
    inputs_expansion = 0
    arguments_expansion = 0
    for index, argument in enumerate(argv):
        context = f"argv[{index}]"
        if argument == "{inputs}":
            inputs_expansion += 1
            if maximum_inputs == 0:
                raise ProtocolError("{inputs} is invalid for a task that accepts no inputs")
            continue
        if argument == "{arguments}":
            arguments_expansion += 1
            continue
        match = _PLACEHOLDER_RE.fullmatch(argument)
        if match:
            kind, value = match.groups()
            if kind == "input":
                if not value.isdigit():
                    raise ProtocolError(f"{context} has an invalid input index")
                if maximum_inputs is not None and int(value) >= maximum_inputs:
                    raise ProtocolError(f"{context} exceeds the task input range")
            elif kind == "source":
                relative = _source_path(value, context)
                if source_paths is not None and not _source_contains(source_paths, relative):
                    raise ProtocolError(f"{context} references source that is not declared")
            elif kind == "result":
                relative = _output_path(value, context)
                if not _output_contains(outputs, relative):
                    raise ProtocolError(f"{context} references an undeclared output")
            else:
                if not _DATASET_RE.fullmatch(value) or value not in datasets:
                    raise ProtocolError(f"{context} references an undeclared dataset")
            continue
        _validate_literal_argument(argument, context)
    if inputs_expansion > 1 or arguments_expansion > 1:
        raise ProtocolError("{inputs} and {arguments} may each appear at most once")


def _validate_profile_shape(
    profile: str,
    argv: Sequence[str],
    *,
    minimum_inputs: int,
    maximum_inputs: int,
    input_values: bool,
    outputs: Sequence[str],
    datasets: Sequence[str],
) -> None:
    input_tokens = {item for item in argv if item == "{inputs}" or item.startswith("{input:")}
    result_tokens = {item for item in argv if item.startswith("{result:")}
    if profile in {"python-script", "can-log-batch"}:
        if not argv or not argv[0].startswith("{source:") or not argv[0].endswith(".py}"):
            raise ProtocolError(
                f"{profile} argv must start with a Python source placeholder"
            )
    if profile == "python-module":
        if not argv or not _MODULE_RE.fullmatch(argv[0]):
            raise ProtocolError("python-module argv must start with a Python module name")
    if profile in {"apk-analyze", "sqlite-readonly", "can-log-batch"}:
        if minimum_inputs < 1 or not input_tokens:
            raise ProtocolError(f"{profile} must accept and reference at least one input")
    if profile == "apk-analyze" and not result_tokens:
        raise ProtocolError("apk-analyze must write to a declared result placeholder")
    if profile == "sqlite-readonly":
        _validate_sqlite_shape(
            argv,
            minimum_inputs=minimum_inputs,
            maximum_inputs=maximum_inputs,
            input_values=input_values,
            outputs=outputs,
            datasets=datasets,
        )
        return
    _validate_profile_arguments(
        profile,
        [item for item in argv if not (item.startswith("{") and item.endswith("}"))],
    )


def _profile_shape(task: RepoTaskDefinition) -> None:
    _validate_profile_shape(
        task.profile,
        task.argv,
        minimum_inputs=task.minimum_inputs,
        maximum_inputs=task.maximum_inputs,
        input_values=task.input_values,
        outputs=task.outputs,
        datasets=task.datasets,
    )


def _parse_repo_task(payload: object, index: int) -> RepoTaskDefinition:
    if not isinstance(payload, dict):
        raise ProtocolError(f"tasks[{index}] must be an object")
    _unexpected_keys(payload, _TASK_KEYS, f"tasks[{index}]")
    missing = sorted(_REQUIRED_TASK_KEYS - set(payload))
    if missing:
        raise ProtocolError(f"tasks[{index}] is missing field(s): {', '.join(missing)}")

    name = payload.get("name")
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise ProtocolError(f"tasks[{index}].name must use lowercase letters, digits, and hyphens")
    if name in TASKS:
        raise ProtocolError(f"repository task {name!r} cannot override a built-in task")
    profile = payload.get("profile")
    if not isinstance(profile, str) or profile not in PROFILE_FAMILIES:
        raise ProtocolError(f"tasks[{index}].profile is not an allowed offline profile")
    description_value = payload.get("description", PROFILE_DESCRIPTIONS[profile])
    description = _bounded_string(description_value, f"tasks[{index}].description", 500)

    raw_sources = payload.get("source_paths")
    if not isinstance(raw_sources, list) or len(raw_sources) > MAX_SOURCE_PATHS:
        raise ProtocolError(f"tasks[{index}].source_paths must contain 0-{MAX_SOURCE_PATHS} paths")
    source_paths = tuple(
        _source_path(item, f"tasks[{index}].source_paths[{source_index}]")
        for source_index, item in enumerate(raw_sources)
    )
    _validate_source_roots(source_paths, f"tasks[{index}].source_paths")

    minimum = payload.get("minimum_inputs")
    maximum = payload.get("maximum_inputs")
    if isinstance(minimum, bool) or not isinstance(minimum, int) or not 0 <= minimum <= MAX_INPUTS:
        raise ProtocolError(f"tasks[{index}].minimum_inputs must be from 0 through {MAX_INPUTS}")
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not minimum <= maximum <= MAX_INPUTS:
        raise ProtocolError(f"tasks[{index}].maximum_inputs must be from minimum_inputs through {MAX_INPUTS}")

    argv = _string_array(payload.get("argv"), f"tasks[{index}].argv", MAX_ARGUMENTS)
    raw_outputs = payload.get("outputs")
    if not isinstance(raw_outputs, list) or len(raw_outputs) > MAX_OUTPUTS:
        raise ProtocolError(f"tasks[{index}].outputs must be an array of 0-{MAX_OUTPUTS} paths")
    outputs = tuple(
        _output_path(item, f"tasks[{index}].outputs[{output_index}]")
        for output_index, item in enumerate(raw_outputs)
    )
    _validate_output_roots(outputs, f"tasks[{index}].outputs")
    raw_datasets = payload.get("datasets", [])
    if not isinstance(raw_datasets, list) or len(raw_datasets) > MAX_DATASETS:
        raise ProtocolError(f"tasks[{index}].datasets must be an array of 0-{MAX_DATASETS} names")
    datasets: tuple[str, ...] = tuple(raw_datasets) if all(
        isinstance(item, str) and _DATASET_RE.fullmatch(item) for item in raw_datasets
    ) else ()
    if len(datasets) != len(raw_datasets):
        raise ProtocolError(f"tasks[{index}].datasets contains an invalid name")
    if len(set(datasets)) != len(datasets):
        raise ProtocolError(f"tasks[{index}].datasets contains duplicates")
    input_values = payload.get("input_values", False)
    if not isinstance(input_values, bool):
        raise ProtocolError(f"tasks[{index}].input_values must be true or false")

    task = RepoTaskDefinition(
        name=name,
        description=description,
        profile=profile,
        family=PROFILE_FAMILIES[profile],
        source_paths=source_paths,
        minimum_inputs=minimum,
        maximum_inputs=maximum,
        argv=argv,
        outputs=outputs,
        datasets=datasets,
        input_values=input_values,
    )
    _validate_argv(
        task.argv,
        source_paths=task.source_paths,
        maximum_inputs=task.maximum_inputs,
        outputs=task.outputs,
        datasets=task.datasets,
    )
    _profile_shape(task)
    return task


def load_repo_tasks(source_root: Path) -> dict[str, RepoTaskDefinition]:
    """Load and strictly validate ``source_root/.van-compute.json`` if present."""
    manifest = source_root / REPO_MANIFEST
    if not manifest.exists():
        return {}
    if manifest.is_symlink() or not manifest.is_file():
        raise ProtocolError(f"{manifest} must be a regular, non-symlink file")
    try:
        with manifest.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"cannot read {manifest}: {exc}") from None
    if not isinstance(payload, dict):
        raise ProtocolError(f"{manifest} must contain a JSON object")
    _unexpected_keys(payload, _TOP_LEVEL_KEYS, REPO_MANIFEST)
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ProtocolError(f"{REPO_MANIFEST} schema_version must be {SCHEMA_VERSION}")
    raw_tasks = payload.get("tasks")
    if not isinstance(raw_tasks, list) or len(raw_tasks) > MAX_REPO_TASKS:
        raise ProtocolError(f"{REPO_MANIFEST} tasks must be an array of 0-{MAX_REPO_TASKS} objects")
    tasks: dict[str, RepoTaskDefinition] = {}
    for index, raw_task in enumerate(raw_tasks):
        task = _parse_repo_task(raw_task, index)
        if task.name in tasks:
            raise ProtocolError(f"duplicate repository task {task.name!r}")
        tasks[task.name] = task
    return tasks


def get_task(
    name: str,
    repo_tasks: Mapping[str, RepoTaskDefinition] | None = None,
) -> TaskDefinition | RepoTaskDefinition:
    if name in TASKS:
        return TASKS[name]
    if repo_tasks is not None and name in repo_tasks:
        return repo_tasks[name]
    raise ProtocolError(f"unknown task {name!r}")


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


def validate_task_arguments(
    task_name: str,
    arguments: Sequence[str],
    task: TaskDefinition | RepoTaskDefinition | None = None,
) -> tuple[str, ...]:
    """Validate legacy options or bounded literal arguments for a repo task."""
    arguments = tuple(arguments)
    if len(arguments) > MAX_ARGUMENTS:
        raise ProtocolError(f"no more than {MAX_ARGUMENTS} task arguments are allowed")
    if isinstance(task, RepoTaskDefinition):
        if arguments and "{arguments}" not in task.argv:
            raise ProtocolError(f"{task.name} does not expose submitted arguments")
        for index, argument in enumerate(arguments):
            _validate_literal_argument(
                argument,
                f"argument[{index}]",
                reject_braces=False,
            )
        if task.profile != "sqlite-readonly" or "{arguments}" in task.argv:
            _validate_profile_arguments(task.profile, arguments)
        return arguments

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
            argument == "--coverage-delta" or argument.startswith("--coverage-delta=")
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
    task: TaskDefinition | RepoTaskDefinition | None = None,
) -> TaskDefinition | RepoTaskDefinition:
    task = task or get_task(task_name)
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


def task_execution(task: TaskDefinition | RepoTaskDefinition) -> dict[str, object] | None:
    """Return the portable execution specification embedded in a queued job."""
    if not isinstance(task, RepoTaskDefinition):
        return None
    return {
        "profile": task.profile,
        "family": task.family,
        "argv": list(task.argv),
        "outputs": list(task.outputs),
        "datasets": list(task.datasets),
        "minimum_inputs": task.minimum_inputs,
        "maximum_inputs": task.maximum_inputs,
        "input_values": task.input_values,
    }


def validate_execution(payload: object) -> dict[str, object]:
    """Validate an embedded repo-task execution spec on either host."""
    if not isinstance(payload, dict):
        raise ProtocolError("execution must be an object")
    _unexpected_keys(payload, _EXECUTION_KEYS, "execution")
    missing = sorted(_EXECUTION_KEYS - set(payload))
    if missing:
        raise ProtocolError(f"execution is missing field(s): {', '.join(missing)}")
    profile = payload.get("profile")
    family = payload.get("family")
    if not isinstance(profile, str) or profile not in PROFILE_FAMILIES:
        raise ProtocolError("execution profile is not allowed")
    if family != PROFILE_FAMILIES[profile]:
        raise ProtocolError("execution family does not match its profile")
    minimum = payload.get("minimum_inputs")
    maximum = payload.get("maximum_inputs")
    input_values = payload.get("input_values")
    if isinstance(minimum, bool) or not isinstance(minimum, int) or not 0 <= minimum <= MAX_INPUTS:
        raise ProtocolError(f"execution.minimum_inputs must be from 0 through {MAX_INPUTS}")
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not minimum <= maximum <= MAX_INPUTS:
        raise ProtocolError(
            f"execution.maximum_inputs must be from minimum_inputs through {MAX_INPUTS}"
        )
    if not isinstance(input_values, bool):
        raise ProtocolError("execution.input_values must be true or false")
    argv = _string_array(payload.get("argv"), "execution.argv", MAX_ARGUMENTS)
    raw_outputs = payload.get("outputs")
    if not isinstance(raw_outputs, list) or len(raw_outputs) > MAX_OUTPUTS:
        raise ProtocolError(f"execution.outputs must be an array of 0-{MAX_OUTPUTS} paths")
    outputs = tuple(
        _output_path(item, f"execution.outputs[{index}]")
        for index, item in enumerate(raw_outputs)
    )
    _validate_output_roots(outputs, "execution.outputs")
    raw_datasets = payload.get("datasets")
    if not isinstance(raw_datasets, list) or len(raw_datasets) > MAX_DATASETS or not all(
        isinstance(item, str) and _DATASET_RE.fullmatch(item) for item in raw_datasets
    ):
        raise ProtocolError(f"execution.datasets must be an array of 0-{MAX_DATASETS} names")
    datasets = tuple(raw_datasets)
    if len(set(datasets)) != len(datasets):
        raise ProtocolError("execution.datasets contains duplicates")
    _validate_argv(
        argv,
        maximum_inputs=maximum,
        outputs=outputs,
        datasets=datasets,
    )
    _validate_profile_shape(
        profile,
        argv,
        minimum_inputs=minimum,
        maximum_inputs=maximum,
        input_values=input_values,
        outputs=outputs,
        datasets=datasets,
    )
    return {
        "profile": profile,
        "family": family,
        "argv": argv,
        "outputs": outputs,
        "datasets": datasets,
        "minimum_inputs": minimum,
        "maximum_inputs": maximum,
        "input_values": input_values,
    }


def _render_input(path: Path, value: object | None) -> str:
    rendered = str(path)
    return f"{rendered}={value}" if value is not None else rendered


def _dynamic_command_prefix(
    profile: str,
    family: str,
    *,
    python: str,
    executables: Mapping[str, str] | None,
) -> list[str]:
    binaries = {"python": python, "jadx": "jadx", "sqlite3": "sqlite3", "rg": "rg"}
    if executables:
        for name, value in executables.items():
            if name in binaries and isinstance(value, str) and value:
                binaries[name] = value
    if profile == "repo-test":
        return [binaries[family], "-m", "pytest"]
    if profile == "python-module":
        return [binaries[family], "-m"]
    if profile == "sqlite-readonly":
        return [binaries[family], "-safe", "-readonly", "-batch"]
    return [binaries[family]]


def build_command(
    task_name: str,
    *,
    python: str,
    source_root: Path,
    input_paths: Sequence[Path],
    input_values: Sequence[object | None],
    result_root: Path,
    arguments: Sequence[str],
    execution: object | None = None,
    executables: Mapping[str, str] | None = None,
    datasets: Mapping[str, str | Path] | None = None,
) -> list[str]:
    """Build one shell-free command after repeating all protocol validation."""
    if len(input_paths) != len(input_values):
        raise ProtocolError("input paths and values have different lengths")
    if execution is not None:
        spec = validate_execution(execution)
        dynamic_inputs = [{"value": value} for value in input_values]
        if not spec["minimum_inputs"] <= len(dynamic_inputs) <= spec["maximum_inputs"]:
            raise ProtocolError(
                f"{task_name} requires {spec['minimum_inputs']}-{spec['maximum_inputs']} input file(s)"
            )
        values_present = [item["value"] for item in dynamic_inputs]
        if any(value is not None for value in values_present):
            if not spec["input_values"]:
                raise ProtocolError(f"{task_name} does not accept input values")
            for value in values_present:
                if value is not None:
                    _finite_number(str(value), "input value")
            if any(value is None for value in values_present):
                raise ProtocolError("provide a value for every input or for none of them")
        normalized = tuple(arguments)
        for index, argument in enumerate(normalized):
            _validate_literal_argument(
                argument,
                f"argument[{index}]",
                reject_braces=False,
            )
        if (
            spec["profile"] == "sqlite-readonly"
            and "{arguments}" not in spec["argv"]
            and normalized
        ):
            raise ProtocolError(f"{task_name} does not expose submitted arguments")
        if spec["profile"] != "sqlite-readonly" or "{arguments}" in spec["argv"]:
            _validate_profile_arguments(str(spec["profile"]), normalized)
        command = _dynamic_command_prefix(
            str(spec["profile"]),
            str(spec["family"]),
            python=python,
            executables=executables,
        )
        for token in spec["argv"]:
            if token == "{inputs}":
                command.extend(
                    _render_input(path, value)
                    for path, value in zip(input_paths, input_values)
                )
            elif token == "{arguments}":
                command.extend(normalized)
            else:
                match = _PLACEHOLDER_RE.fullmatch(token)
                if not match:
                    command.append(token)
                    continue
                kind, value = match.groups()
                if kind == "source":
                    command.append(str(source_root / _source_path(value, "source placeholder")))
                elif kind == "result":
                    relative = _output_path(value, "result placeholder")
                    if not _output_contains(spec["outputs"], relative):
                        raise ProtocolError("result placeholder is not declared")
                    command.append(str(result_root / relative))
                elif kind == "input":
                    index = int(value)
                    if index >= len(input_paths):
                        raise ProtocolError(f"input placeholder {index} is out of range")
                    command.append(_render_input(input_paths[index], input_values[index]))
                else:
                    if value not in spec["datasets"] or not datasets or value not in datasets:
                        raise ProtocolError(f"dataset {value!r} is not configured on this worker")
                    command.append(str(datasets[value]))
        return command

    task_inputs = [{"value": value} for value in input_values]
    task = validate_inputs(task_name, task_inputs)
    normalized = validate_task_arguments(task_name, arguments)
    entrypoint = source_root / task.entrypoint
    command = [python, str(entrypoint), *task.fixed_arguments]
    for path, value in zip(input_paths, input_values):
        command.append(_render_input(path, value))
    command.extend(normalized)
    if task.result_json:
        command.extend(("--json", str(result_root / task.result_json)))
    return command
