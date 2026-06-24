"""Pure transforms for the Delta sink — no I/O, fully unit-testable.

The Debezium connector is configured with the ExtractNewRecordState SMT and
JSON-without-schema converters, so each Kafka message value is already a flat
row dict. This module turns that into the record we persist to Delta, plus the
CDC metadata, and derives the target Delta table location from the topic name.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional


# Debezium adds these with the default "__" prefix via add.fields / rewrite.
_META_OP = "__op"          # c (create), u (update), d (delete), r (snapshot read)
_META_TS = "__ts_ms"
_META_DELETED = "__deleted"

# topic.prefix is "ztap.<project>"; schema is "proj_<project>"; so a full topic
# looks like: ztap.<project>.proj_<project>.<table>
_TOPIC_RE = re.compile(r"^ztap\.(?P<project>[a-z0-9_]+)\.proj_(?P=project)\.(?P<table>[A-Za-z0-9_]+)$")


@dataclass(frozen=True)
class SinkRecord:
    project: str
    table: str
    row: dict           # the data columns plus _op / _ts_ms / _deleted markers
    op: str             # c | u | d | r
    deleted: bool


class TransformError(ValueError):
    pass


def parse_topic(topic: str) -> tuple[str, str]:
    """Return (project, table) for a ztap CDC topic, or raise TransformError."""
    m = _TOPIC_RE.match(topic)
    if not m:
        raise TransformError(f"topic {topic!r} is not a ztap CDC topic")
    return m.group("project"), m.group("table")


def storage_location(project: str, table: str, bucket: str = "warehouse") -> str:
    """Must match the storage_location the control plane registered in UC."""
    return f"s3://{bucket}/{project}/{table}"


def to_record(topic: str, value: Optional[dict]) -> Optional[SinkRecord]:
    """Convert a Kafka message value into a SinkRecord.

    Returns None for tombstone messages (value is None), which carry no state.
    """
    project, table = parse_topic(topic)
    if value is None:
        return None  # tombstone: the rewrite SMT already emitted the delete row

    op = str(value.get(_META_OP, "")) or "r"
    deleted = str(value.get(_META_DELETED, "false")).lower() == "true"

    # Strip Debezium metadata fields; keep data columns, then re-attach clean
    # CDC markers so downstream readers can tell snapshots/inserts from deletes.
    row = {k: v for k, v in value.items() if not k.startswith("__")}
    if not row:
        raise TransformError(f"message on {topic!r} has no data columns: {value!r}")
    row["_op"] = op
    row["_ts_ms"] = value.get(_META_TS)
    row["_deleted"] = deleted

    return SinkRecord(project=project, table=table, row=row, op=op, deleted=deleted)
