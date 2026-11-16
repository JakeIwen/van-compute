"""The Pi side of the Mac worker's lease audit: which running jobs it holds.

`queue.py worker leases --worker <base>` lists every `running/` job whose
worker is `<base>` or one of its slots (`<base>.NN`), with its attempt and
whether a cancel was requested.  It is read-only: no lock, no heartbeat
write, no lease tokens.  A job missing from a complete reply is no longer the
slot's to run (cancelled, requeued or finished), so the Mac stops it.
"""

from __future__ import annotations

from pathlib import Path

from van_compute import job_cancel, queue


def leases_for(root: Path, base: str) -> dict[str, object]:
    queue.validate_worker(base)
    leases: list[dict[str, object]] = []
    complete = True
    for path in sorted((root / "running").iterdir()):
        if not queue.JOB_ID_RE.fullmatch(path.name) or path.is_symlink():
            continue
        try:
            manifest = queue.load_json(path / "manifest.json")
        except queue.QueueError:
            # Moved out of running/ while listed: it is not a lease any more.
            # Present but unreadable: the reply cannot vouch for absences.
            if path.is_dir():
                complete = False
            continue
        worker = str(manifest.get("worker", ""))
        if worker == base or worker.startswith(f"{base}."):
            leases.append(
                {
                    "worker": worker,
                    "job": path.name,
                    "attempt": manifest.get("attempt"),
                    "cancel_requested": job_cancel.is_cancel_requested(manifest),
                }
            )
    return {"worker": base, "complete": complete, "leases": leases}
