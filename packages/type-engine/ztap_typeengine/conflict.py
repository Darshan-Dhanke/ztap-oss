"""Conflict resolution for bidirectional Postgres <-> lakehouse sync.

When the same primary key is updated on both sides before a sync cycle runs,
*something* has to decide which write wins. Leaving this undefined is how you
get silent divergence. This module makes the policy explicit and deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional


class ConflictPolicy(str, Enum):
    LAST_WRITE_WINS = "last_write_wins"
    SOURCE_OF_TRUTH = "source_of_truth"  # one side always wins
    MERGE = "merge"  # column-level merge via a user callback
    ERROR = "error"  # refuse to resolve; surface for human/ops handling


@dataclass(frozen=True)
class Row:
    """A versioned row from one side of the sync."""

    side: str  # "postgres" or "lakehouse"
    key: Any
    values: dict
    # Monotonic version: a commit timestamp (epoch micros) or LSN-derived int.
    # Higher == newer. Required for LAST_WRITE_WINS.
    version: Optional[int] = None


@dataclass(frozen=True)
class ConflictResolution:
    winner: Row
    reason: str
    merged_values: Optional[dict] = None


class ConflictError(RuntimeError):
    def __init__(self, pg: Row, lake: Row, msg: str):
        super().__init__(msg)
        self.pg = pg
        self.lake = lake


def resolve(
    pg: Row,
    lake: Row,
    policy: ConflictPolicy,
    *,
    source_of_truth: str = "postgres",
    merge_fn: Optional[Callable[[Row, Row], dict]] = None,
) -> ConflictResolution:
    """Resolve a conflict between a Postgres row and a lakehouse row.

    Both rows are assumed to share the same primary key. Returns the winning
    Row plus a human-readable reason (which belongs in an audit log).
    """
    if pg.key != lake.key:
        raise ValueError(f"cannot resolve rows with different keys: {pg.key!r} vs {lake.key!r}")

    if policy is ConflictPolicy.LAST_WRITE_WINS:
        if pg.version is None or lake.version is None:
            raise ConflictError(
                pg, lake,
                "last_write_wins requires a version on both rows; refusing to guess",
            )
        if pg.version == lake.version:
            # Identical version, differing data => genuine ambiguity. Fall back
            # to source_of_truth deterministically rather than coin-flip.
            winner = pg if source_of_truth == "postgres" else lake
            return ConflictResolution(
                winner=winner,
                reason=f"equal versions ({pg.version}); broke tie via source_of_truth={source_of_truth}",
            )
        winner = pg if pg.version > lake.version else lake
        return ConflictResolution(
            winner=winner,
            reason=f"last_write_wins: {winner.side} version {winner.version} > "
                   f"{(lake if winner is pg else pg).version}",
        )

    if policy is ConflictPolicy.SOURCE_OF_TRUTH:
        if source_of_truth not in ("postgres", "lakehouse"):
            raise ValueError(f"invalid source_of_truth: {source_of_truth!r}")
        winner = pg if source_of_truth == "postgres" else lake
        return ConflictResolution(
            winner=winner,
            reason=f"source_of_truth={source_of_truth} wins unconditionally",
        )

    if policy is ConflictPolicy.MERGE:
        if merge_fn is None:
            raise ValueError("merge policy requires a merge_fn callback")
        merged = merge_fn(pg, lake)
        # Winner side is nominal for merge; we attach merged values explicitly.
        return ConflictResolution(
            winner=pg if source_of_truth == "postgres" else lake,
            reason="column-level merge via merge_fn",
            merged_values=merged,
        )

    if policy is ConflictPolicy.ERROR:
        raise ConflictError(
            pg, lake,
            f"conflict on key {pg.key!r} and policy=error; manual resolution required",
        )

    raise ValueError(f"unknown policy {policy!r}")
