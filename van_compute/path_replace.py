"""Replace several byte strings in one pass over the data.

Applying replacements one after another rewrites text an earlier replacement
produced: a label such as `{job}/tmp/` contains `/tmp`, which a later
`/tmp` -> `{...}` rule would hit again.  One regular-expression alternation,
longest private string first, matches each position once and never looks at
replacement output.
"""

from __future__ import annotations

import re
from typing import Iterable


def replace_paths_once(
    data: bytes, replacements: Iterable[tuple[bytes, bytes]]
) -> bytes:
    table: dict[bytes, bytes] = {}
    for private, logical in replacements:
        if private:
            table.setdefault(private, logical)  # first of equal paths wins
    if not table:
        return data
    pattern = re.compile(
        b"|".join(re.escape(key) for key in sorted(table, key=len, reverse=True))
    )
    return pattern.sub(lambda match: table[match.group(0)], data)
