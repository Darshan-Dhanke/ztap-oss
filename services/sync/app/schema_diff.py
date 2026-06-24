"""Schema diffing between Postgres and the catalog/lakehouse side.

Pure, no I/O. Given a Postgres table's columns and the columns currently
registered in Unity Catalog (Delta types), compute what drifted. The type-engine
is the single source of truth for what a Postgres column *should* be in Delta,
so a "type change" means the PG column now maps to a different Delta type than
what UC has registered.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ztap_typeengine import map_pg_to_delta


@dataclass(frozen=True)
class PgColumn:
    name: str
    pg_type: str
    nullable: bool = True


@dataclass(frozen=True)
class TargetColumn:
    name: str
    delta_type: str  # e.g. "STRING", "DECIMAL(10,2)", "ARRAY<INTEGER>"


@dataclass(frozen=True)
class ColumnChange:
    name: str
    from_type: str
    to_type: str


@dataclass(frozen=True)
class SchemaDiff:
    # columns present in PG but not yet in the target (need adding)
    added: list[PgColumn] = field(default_factory=list)
    # columns in the target but no longer in PG (dropped upstream)
    removed: list[TargetColumn] = field(default_factory=list)
    # columns whose PG->Delta mapping no longer matches the registered type
    type_changed: list[ColumnChange] = field(default_factory=list)

    @property
    def in_sync(self) -> bool:
        return not (self.added or self.removed or self.type_changed)

    def summary(self) -> dict:
        return {
            "in_sync": self.in_sync,
            "added": [c.name for c in self.added],
            "removed": [c.name for c in self.removed],
            "type_changed": [
                {"column": c.name, "from": c.from_type, "to": c.to_type}
                for c in self.type_changed
            ],
        }


def _norm(delta_type: str) -> str:
    return delta_type.strip().upper()


def diff_schema(pg_cols: list[PgColumn], target_cols: list[TargetColumn]) -> SchemaDiff:
    """Compare PG columns against the registered target columns."""
    target_by_name = {c.name: c for c in target_cols}
    pg_by_name = {c.name: c for c in pg_cols}

    added: list[PgColumn] = []
    type_changed: list[ColumnChange] = []
    for col in pg_cols:
        expected = map_pg_to_delta(col.pg_type).delta_type
        tgt = target_by_name.get(col.name)
        if tgt is None:
            added.append(col)
        elif _norm(expected) != _norm(tgt.delta_type):
            type_changed.append(
                ColumnChange(name=col.name, from_type=tgt.delta_type, to_type=expected)
            )

    removed = [c for c in target_cols if c.name not in pg_by_name]
    return SchemaDiff(added=added, removed=removed, type_changed=type_changed)
