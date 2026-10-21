"""Pure age and allocated-space decisions for completed-job retention."""

from __future__ import annotations

from dataclasses import dataclass, replace
import datetime as dt
from typing import Iterable, Protocol, Sized


class CompletedJob(Protocol):
    job_id: str
    state: str
    size: int
    reclaimable_size: int
    category: str
    reason: str
    finished_at: str | None


class CitationScanLike(Protocol):
    files_scanned: int
    job_ids: Sized


@dataclass(frozen=True)
class RetentionPlan:
    eligible_ids: tuple[str, ...]
    budget_selected_ids: frozenset[str]
    before_bytes: int
    projected_bytes: int
    maximum_bytes: int | None
    projected_satisfied: bool
    blockers: tuple[dict[str, object], ...]


def retention_settings(
    defaults: dict[str, int],
    keep_days: int | None,
    maximum_bytes: int | None,
) -> dict[str, int]:
    for name, value in (("keep_days", keep_days), ("max_history_bytes", maximum_bytes)):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
        ):
            raise ValueError(f"{name} must be a positive integer")
    if keep_days is None:
        return dict(defaults)
    return {state: keep_days for state in defaults}


def _finished(job: CompletedJob) -> dt.datetime:
    if job.finished_at is None:
        return dt.datetime.max.replace(tzinfo=dt.timezone.utc)
    return dt.datetime.fromisoformat(job.finished_at).astimezone(dt.timezone.utc)


def plan_retention(
    jobs: Iterable[CompletedJob],
    maximum_bytes: int | None,
    *,
    before_bytes: int | None = None,
) -> RetentionPlan:
    items = tuple(jobs)
    before = sum(item.size for item in items) if before_bytes is None else before_bytes
    expired = sorted(
        (item for item in items if item.category == "eligible"),
        key=lambda item: (_finished(item), item.state, item.job_id),
    )
    selected = list(expired)
    projected = before - sum(item.reclaimable_size for item in selected)
    budget_selected: list[CompletedJob] = []

    if maximum_bytes is not None and projected > maximum_bytes:
        younger = sorted(
            (item for item in items if item.category == "kept-young"),
            key=lambda item: (_finished(item), item.state, item.job_id),
        )
        for item in younger:
            selected.append(item)
            budget_selected.append(item)
            projected -= item.reclaimable_size
            if projected <= maximum_bytes:
                break

    satisfied = maximum_bytes is None or projected <= maximum_bytes
    blockers: tuple[dict[str, object], ...] = ()
    if not satisfied:
        blockers = tuple(
            {
                "id": item.job_id,
                "state": item.state,
                "category": item.category,
                "bytes": item.size,
                "reason": item.reason,
            }
            for item in items
            if item.category in {"protected", "skipped"}
        )
        if not blockers:
            blockers = (
                {
                    "id": None,
                    "state": "history",
                    "category": "unreclaimable-overhead",
                    "bytes": projected,
                    "reason": (
                        "completed-history directory or shared-block overhead "
                        "prevents the target"
                    ),
                },
            )
    return RetentionPlan(
        eligible_ids=tuple(item.job_id for item in selected),
        budget_selected_ids=frozenset(item.job_id for item in budget_selected),
        before_bytes=before,
        projected_bytes=max(0, projected),
        maximum_bytes=maximum_bytes,
        projected_satisfied=satisfied,
        blockers=blockers,
    )


def mark_budget_candidates(jobs: Iterable[CompletedJob], plan: RetentionPlan) -> list:
    return [
        replace(
            item,
            category="eligible",
            reason="oldest uncited job selected to meet history budget",
        )
        if item.job_id in plan.budget_selected_ids
        else item
        for item in jobs
    ]


def mark_duplicate_ids(jobs: Iterable[CompletedJob]) -> list:
    items = tuple(jobs)
    counts: dict[str, int] = {}
    for item in items:
        counts[item.job_id] = counts.get(item.job_id, 0) + 1
    return [
        replace(
            item,
            category="skipped",
            reason="job ID appears in multiple completed states",
            identity=None,
            reclaimable_size=0,
        )
        if counts[item.job_id] > 1
        else item
        for item in items
    ]


def order_for_deletion(
    jobs: Iterable[CompletedJob], eligible_ids: tuple[str, ...]
) -> list:
    items = tuple(jobs)
    by_id = {item.job_id: item for item in items}
    selected = [by_id[job_id] for job_id in eligible_ids]
    selected_ids = frozenset(eligible_ids)
    return selected + [item for item in items if item.job_id not in selected_ids]


def budget_payload(
    plan: RetentionPlan,
    jobs: Iterable[CompletedJob],
    deleted_ids: Iterable[str],
    *,
    apply: bool,
    after_bytes: int | None = None,
) -> dict[str, object]:
    items = tuple(jobs)
    deleted = frozenset(deleted_ids)
    if after_bytes is None:
        after_bytes = plan.before_bytes - sum(
            item.size for item in items if item.job_id in deleted
        )
    after_satisfied = plan.maximum_bytes is None or after_bytes <= plan.maximum_bytes
    blocked = not (after_satisfied if apply else plan.projected_satisfied)
    blockers = list(plan.blockers) if blocked else []
    if blocked:
        known = {(item["state"], item["id"]) for item in blockers}
        blockers.extend(
            {
                "id": item.job_id,
                "state": item.state,
                "category": item.category,
                "bytes": item.size,
                "reason": item.reason,
            }
            for item in items
            if item.category in {"protected", "skipped"}
            and (item.state, item.job_id) not in known
        )
    return {
        "accounting": "allocated_bytes",
        "maximum_bytes": plan.maximum_bytes,
        "before_bytes": plan.before_bytes,
        "projected_bytes": plan.projected_bytes,
        "after_bytes": after_bytes,
        "projected_satisfied": plan.projected_satisfied,
        "after_satisfied": after_satisfied,
        "blockers": blockers,
    }


def retention_payload(
    *,
    apply: bool,
    citations_root: object,
    now: dt.datetime,
    citation_scans: int,
    scan: CitationScanLike,
    protected: Sized,
    decisions: Iterable[CompletedJob],
    deleted: list[str],
    plan: RetentionPlan,
    retention_days: dict[str, int],
    after_bytes: int | None = None,
) -> dict[str, object]:
    items = tuple(decisions)
    by_id = {item.job_id: item for item in items}
    categories = {
        name: {"count": 0, "bytes": 0}
        for name in ("protected", "kept-young", "skipped", "eligible")
    }
    by_state = {
        state: {name: {"count": 0, "bytes": 0} for name in categories}
        for state in retention_days
    }
    for item in items:
        for bucket in (categories[item.category], by_state[item.state][item.category]):
            bucket["count"] += 1
            bucket["bytes"] += item.size
    return {
        "apply": apply,
        "citations_root": str(citations_root),
        "now": now.isoformat(),
        "citation_scans": citation_scans,
        "citation_files_scanned": scan.files_scanned,
        "direct_citation_ids": len(scan.job_ids),
        "protected_ids": len(protected),
        "retention_days": dict(retention_days),
        "history_budget": budget_payload(
            plan, items, deleted, apply=apply, after_bytes=after_bytes
        ),
        "summary": {"categories": categories, "by_state": by_state},
        "eligible": [
            {
                "id": by_id[job_id].job_id,
                "state": by_id[job_id].state,
                "bytes": by_id[job_id].size,
                "reason": by_id[job_id].reason,
            }
            for job_id in plan.eligible_ids
        ],
        "eligible_ids": list(plan.eligible_ids),
        "deleted_ids": deleted,
        "skipped": [
            {"id": item.job_id, "state": item.state, "reason": item.reason}
            for item in items
            if item.category == "skipped"
        ],
    }
