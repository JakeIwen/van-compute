#!/usr/bin/env python3
"""Run the complete test suite, optionally from an offline wheelhouse."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Sequence
import zipfile


REPO = Path(__file__).resolve().parent
REQUIREMENTS = REPO / "requirements-test.txt"
MAX_WHEEL_BYTES = 128 * 1024 * 1024
MAX_WHEELHOUSE_BYTES = 512 * 1024 * 1024


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wheelhouse",
        type=Path,
        help="ZIP containing offline wheels for the current platform",
    )
    return parser


def _extract_wheels(archive: Path, destination: Path) -> tuple[Path, ...]:
    extracted: list[Path] = []
    names: set[str] = set()
    total = 0
    with zipfile.ZipFile(archive) as bundle:
        for item in bundle.infolist():
            name = Path(item.filename).name
            if item.is_dir() or not name.endswith(".whl"):
                continue
            if name in names:
                raise RuntimeError(f"wheelhouse has duplicate basename: {name}")
            if item.file_size > MAX_WHEEL_BYTES:
                raise RuntimeError(f"wheel exceeds extraction limit: {name}")
            total += item.file_size
            if total > MAX_WHEELHOUSE_BYTES:
                raise RuntimeError("wheelhouse exceeds extraction limit")
            target = destination / name
            with bundle.open(item) as source, target.open("xb") as output:
                shutil.copyfileobj(source, output)
            names.add(name)
            extracted.append(target)
    if not extracted:
        raise RuntimeError("wheelhouse contains no .whl files")
    return tuple(extracted)


def _run_suite(site: Path | None = None) -> int:
    environment = os.environ.copy()
    roots = [str(REPO)] if site is None else [str(site), str(REPO)]
    environment["PYTHONPATH"] = os.pathsep.join(roots)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    command = [
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        "pi/tests/compute",
        "-f",
    ]
    return subprocess.run(command, cwd=REPO, env=environment, check=False).returncode


def run(wheelhouse: Path | None) -> int:
    if wheelhouse is None:
        return _run_suite()
    with tempfile.TemporaryDirectory(prefix="van-compute-tests-") as raw:
        temporary = Path(raw)
        wheels = temporary / "wheels"
        site = temporary / "site"
        wheels.mkdir()
        site.mkdir()
        _extract_wheels(wheelhouse, wheels)
        install = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-index",
                "--only-binary=:all:",
                "--target",
                str(site),
                "--find-links",
                str(wheels),
                "-r",
                str(REQUIREMENTS),
            ],
            cwd=REPO,
            check=False,
        )
        return install.returncode or _run_suite(site)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args.wheelhouse)
    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
        print(f"test setup failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
