"""The sync state machine — pure coordination logic, no I/O.

Two responsibilities, both deterministic and unit-tested:

1. Per-table *schema* state: IN_SYNC <-> SCHEMA_DRIFT <-> RECONCILING, driven by
   the SchemaDiff from schema_diff.py.

2. Per-row *reverse-apply* decisions for lake -> Postgres sync: given an incoming
   lakehouse change, the current Postgres row, the conflict policy, and the
   last lake version already applied, decide whether to apply it, skip it as a
   duplicate (idempotency / loop-prevention), or skip it because Postgres won
   the conflict. Conflict resolution itself is delegated to the type-engine (#4).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ztap_typeengine import ConflictPolicy, Row, resolve
from ztap_typeengine.conflict import ConflictError

from .schema_diff import SchemaDiff


class SyncState(str, Enum):
    IN_SYNC = "in_sync"
    SCHEMA_DRIFT = "schema_drift"
    RECONCILING = "reconciling"
    ERROR = "error"


def next_schema_state(current: SyncState, diff: SchemaDiff) -> SyncState:
    """Schema-state transition from an observed diff."""
    if diff.in_sync:
        return SyncState.IN_SYNC
    return SyncState.SCHEMA_DRIFT


class ReverseAction(str, Enum):
    APPLY = "apply"                       # write the lakehouse row into Postgres
    APPLY_MERGE = "apply_merge"           # write merged values into Postgres
    SKIP_DUPLICATE = "skip_duplicate"     # already applied (idempotency / loop guard)
    SKIP_PG_WINS = "skip_pg_wins"         # Postgres holds the winning version
    ERROR = "error"                       # policy=error and a real conflict exists


@dataclass(frozen=True)
class ReverseDecision:
    action: ReverseAction
    reason: str
    values: Optional[dict] = None  # the values to write (for APPLY / APPLY_MERGE)


def decide_reverse_apply(
    lake_row: Row,
    pg_row: Optional[Row],
    *,
    policy: ConflictPolicy,
    last_applied_version: Optional[int] = None,
    source_of_truth: str = "postgres",
    merge_fn=None,
) -> ReverseDecision:
    """Decide what to do with an incoming lakehouse change.

    ``lake_row`` is the change coming from the lakehouse side. ``pg_row`` is the
    current Postgres row for the same key (None if it does not exist yet).
    ``last_applied_version`` is the highest lake version already applied for this
    key — the idempotency / loop-prevention ledger.
    """
    # 1. Idempotency / loop-prevention: never apply a lake change we've already
    #    applied (or an older one). This is what stops a reverse-sync write from
    #    echoing back through CDC and being re-applied in a loop.
    if last_applied_version is not None and lake_row.version is not None \
            and lake_row.version <= last_applied_version:
        return ReverseDecision(
            ReverseAction.SKIP_DUPLICATE,
            f"lake version {lake_row.version} <= last applied {last_applied_version}",
        )

    # 2. Row absent in Postgres -> straight insert (no conflict possible).
    if pg_row is None:
        return ReverseDecision(
            ReverseAction.APPLY, "no existing Postgres row; inserting", values=lake_row.values
        )

    # 3. Real potential conflict: delegate to the type-engine's policy engine.
    try:
        res = resolve(
            pg_row, lake_row, policy,
            source_of_truth=source_of_truth, merge_fn=merge_fn,
        )
    except ConflictError as e:
        return ReverseDecision(ReverseAction.ERROR, str(e))

    if res.merged_values is not None:
        return ReverseDecision(ReverseAction.APPLY_MERGE, res.reason, values=res.merged_values)
    if res.winner.side == "lakehouse":
        return ReverseDecision(ReverseAction.APPLY, res.reason, values=lake_row.values)
    return ReverseDecision(ReverseAction.SKIP_PG_WINS, res.reason)
