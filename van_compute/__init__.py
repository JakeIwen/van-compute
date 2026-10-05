"""Pin compute package imports to the immutable release directory."""

from pathlib import Path

__path__ = [str(Path(__file__).resolve().parent)]
