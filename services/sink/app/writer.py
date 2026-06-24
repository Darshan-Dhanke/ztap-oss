"""Delta Lake writer — appends batches of CDC rows to a Delta table in MinIO.

Uses delta-rs (the `deltalake` package), so no Spark/JVM is required. Schema is
inferred per batch and merged into the table, so a later ALTER TABLE ADD COLUMN
on the Postgres side lands as a Delta schema evolution rather than a failure.
"""

from __future__ import annotations

import json
import logging
from typing import Iterable

import pyarrow as pa
from deltalake import write_deltalake

log = logging.getLogger("ztap.sink.writer")


def _normalize_value(v):
    """Make a value safe for a homogeneous Arrow column.

    Nested dict/list values (e.g. a jsonb column that Debezium happened to emit
    as an object) are serialized to JSON text — matching the type-engine's
    json-text encoding so both sides agree.
    """
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), sort_keys=True)
    return v


def rows_to_arrow(rows: list[dict]) -> pa.Table:
    """Build a pyarrow Table from CDC row dicts with a stable column union.

    Different messages may carry different columns (schema evolution mid-batch);
    we take the union of keys and fill missing ones with None so the batch is
    rectangular.

    Columns that are entirely null in this batch are dropped: delta-rs cannot
    infer a type for an all-null column, and forcing one (e.g. string) would
    collide with the real type when a non-null value arrives in a later batch.
    Omitting them lets ``schema_mode="merge"`` add the column with its correct
    type on the first batch that actually carries a value; earlier rows simply
    read back as null for it.
    """
    columns: list[str] = []
    seen = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                columns.append(k)
    data = {}
    for c in columns:
        vals = [_normalize_value(r.get(c)) for r in rows]
        if all(v is None for v in vals):
            continue  # skip all-null column this batch
        data[c] = vals
    return pa.table(data)


class DeltaWriter:
    def __init__(self, storage_options: dict):
        self.storage_options = storage_options

    def append(self, table_uri: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        table = rows_to_arrow(rows)
        write_deltalake(
            table_uri,
            table,
            mode="append",
            schema_mode="merge",
            storage_options=self.storage_options,
        )
        log.info("wrote %d rows to %s", len(rows), table_uri)
        return len(rows)
