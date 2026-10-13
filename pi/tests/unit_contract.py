"""Small semantic parsers shared by deployment contract tests."""

from __future__ import annotations

import shlex


def parse_directives(text: str) -> dict[str, list[str]]:
    """Parse the simple key/value directives used by repository unit files."""
    directives: dict[str, list[str]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        directives.setdefault(key, []).append(value)
    return directives


def parse_environment(directives: dict[str, list[str]]) -> dict[str, str]:
    """Return Environment= assignments after shell-style unquoting."""
    environment: dict[str, str] = {}
    for declaration in directives.get("Environment", []):
        for assignment in shlex.split(declaration):
            name, separator, value = assignment.partition("=")
            if separator:
                environment[name] = value
    return environment


def command_arguments(directive: str) -> list[str]:
    """Parse one ExecStart-like directive into its executable arguments."""
    return shlex.split(directive)
