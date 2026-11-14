"""Installer probe proving the Mac sandbox profile's CACHE_ROOT grant is narrow."""

from __future__ import annotations

from pathlib import Path
import tempfile

from .constants import SANDBOX_CACHE_PROBE
from .models import DeploymentError


def make_probe_caches(parent: Path) -> tuple[Path, Path]:
    """Create sibling selected/other cache directories, never nested, so the
    probe proves the profile grants only the selected one."""
    selected = Path(tempfile.mkdtemp(prefix=".sandbox-cache-selected.", dir=parent))
    other = Path(tempfile.mkdtemp(prefix=".sandbox-cache-other.", dir=parent))
    return selected, other


def run_cache_probe(
    local,
    staging: Path,
    sandbox_test: Path,
    sentinel: Path,
    prefix: list[str],
    cache_selected: Path,
    cache_other: Path,
) -> None:
    link = cache_selected / "sentinel-link"
    link.symlink_to(sentinel)
    # Pre-existing, so the probe sees a permission denial rather than the
    # ENOENT that the profile's global metadata read grant would give.
    other_file = cache_other / "probe.txt"
    other_file.write_text("other", encoding="utf-8")
    before = sentinel.read_bytes()
    local.run(
        prefix
        + [
            str(staging / "venv/bin/python"),
            "-c",
            SANDBOX_CACHE_PROBE,
            str(cache_selected),
            str(cache_other),
            str(sentinel),
            str(link),
        ],
        cwd=sandbox_test,
    )
    if (
        sentinel.read_bytes() != before
        or other_file.read_text(encoding="utf-8") != "other"
    ):
        raise DeploymentError("sandbox wrote outside its selected cache")
