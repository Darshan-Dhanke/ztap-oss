"""Postgres <-> Delta type mapping.

The goal is *honesty about lossiness*. The single biggest source of silent
corruption in Postgres<->lakehouse sync is a type that has no native Delta
equivalent (jsonb, interval, cidr, uuid, arrays) being coerced without anyone
recording that a coercion happened. So every mapping carries an explicit
``lossy`` flag and a canonical encoding, and round-tripping a lossy type
returns the *documented substitute* rather than pretending nothing changed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import re


class TypeMappingError(ValueError):
    """Raised when a type cannot be mapped at all (as opposed to lossily)."""


@dataclass(frozen=True)
class LogicalType:
    """A normalized description of a column type on either side."""

    # The Delta/Spark SQL type string, e.g. "STRING", "DECIMAL(10,2)",
    # "ARRAY<LONG>", "TIMESTAMP".
    delta_type: str
    # Whether converting *into* this Delta type from Postgres loses information
    # (semantics, precision, or the native type identity).
    lossy: bool = False
    # How the value is physically encoded in Delta. For lossy types this is the
    # contract both sync directions must agree on.
    encoding: str = "native"
    # Human-readable note explaining the lossiness, surfaced in logs/manifests.
    note: str = ""
    # The Postgres type we will reconstruct to on the reverse path. For lossless
    # types this equals the original; for lossy types it is the agreed
    # substitute (which may differ from the original — that's the whole point).
    reverse_pg_type: str = ""

    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Postgres -> Delta
# ---------------------------------------------------------------------------

# Exact, lossless mappings keyed by normalized Postgres type name.
_PG_TO_DELTA_LOSSLESS: dict[str, tuple[str, str]] = {
    # pg_name: (delta_type, reverse_pg_type)
    "boolean": ("BOOLEAN", "boolean"),
    "bool": ("BOOLEAN", "boolean"),
    "smallint": ("SHORT", "smallint"),
    "int2": ("SHORT", "smallint"),
    "integer": ("INTEGER", "integer"),
    "int": ("INTEGER", "integer"),
    "int4": ("INTEGER", "integer"),
    "bigint": ("LONG", "bigint"),
    "int8": ("LONG", "bigint"),
    "real": ("FLOAT", "real"),
    "float4": ("FLOAT", "real"),
    "double precision": ("DOUBLE", "double precision"),
    "float8": ("DOUBLE", "double precision"),
    "text": ("STRING", "text"),
    "date": ("DATE", "date"),
    "bytea": ("BINARY", "bytea"),
}

# Types Delta cannot represent natively. We pick a canonical lossy encoding and
# record exactly what is lost. reverse_pg_type is the substitute we rebuild to.
_PG_TO_DELTA_LOSSY: dict[str, dict] = {
    "jsonb": dict(
        delta_type="STRING",
        encoding="json-text",
        note="jsonb stored as canonical JSON text; binary jsonb ordering/dedup is not preserved",
        reverse_pg_type="jsonb",
    ),
    "json": dict(
        delta_type="STRING",
        encoding="json-text",
        note="json stored as text; whitespace/key-order preserved as-is",
        reverse_pg_type="json",
    ),
    "uuid": dict(
        delta_type="STRING",
        encoding="uuid-text",
        note="uuid stored as canonical 36-char text; no native UUID type in Delta",
        reverse_pg_type="uuid",
    ),
    "interval": dict(
        delta_type="LONG",
        encoding="interval-micros",
        note="interval stored as total microseconds; month/year components are NOT month-safe and are flattened using 30-day months",
        reverse_pg_type="interval",
    ),
    "cidr": dict(
        delta_type="STRING",
        encoding="cidr-text",
        note="cidr stored as text; netmask validation not enforced by Delta",
        reverse_pg_type="cidr",
    ),
    "inet": dict(
        delta_type="STRING",
        encoding="inet-text",
        note="inet stored as text",
        reverse_pg_type="inet",
    ),
    "macaddr": dict(
        delta_type="STRING",
        encoding="macaddr-text",
        note="macaddr stored as text",
        reverse_pg_type="macaddr",
    ),
    "money": dict(
        delta_type="DECIMAL(19,2)",
        encoding="money-decimal",
        note="money mapped to DECIMAL(19,2); locale/currency symbol is dropped",
        reverse_pg_type="numeric(19,2)",
    ),
    "tsvector": dict(
        delta_type="STRING",
        encoding="tsvector-text",
        note="full-text search vector flattened to text; lexeme positions lost",
        reverse_pg_type="tsvector",
    ),
}

_NUMERIC_RE = re.compile(r"^(?:numeric|decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_BARE_NUMERIC_RE = re.compile(r"^(?:numeric|decimal)$")
_VARCHAR_RE = re.compile(r"^(?:varchar|character varying|char|character|bpchar)\s*(?:\(\s*\d+\s*\))?$")
_TIMESTAMP_RE = re.compile(r"^timestamp(?:\s*\(\d+\))?(?:\s+with(?:out)?\s+time\s+zone)?$")
_TIMESTAMPTZ_ALIAS_RE = re.compile(r"^timestamptz$")
_ARRAY_BRACKET_RE = re.compile(r"^(.*?)\s*\[\s*\]$")


def _normalize(pg_type: str) -> str:
    return " ".join(pg_type.strip().lower().split())


def map_pg_to_delta(pg_type: str) -> LogicalType:
    """Map a Postgres type string to its Delta LogicalType.

    Raises TypeMappingError for types we refuse to guess at.
    """
    t = _normalize(pg_type)
    if not t:
        raise TypeMappingError("empty postgres type")

    # Arrays: "integer[]" or "text[]" -> ARRAY<element>. Postgres arrays are
    # multi-dimensional and 1-based; Delta arrays are 1-D and 0-based. That is a
    # real semantic difference, so arrays are always lossy.
    m = _ARRAY_BRACKET_RE.match(t)
    if m:
        element = map_pg_to_delta(m.group(1))
        return LogicalType(
            delta_type=f"ARRAY<{element.delta_type}>",
            lossy=True,
            encoding="array-1d",
            note=(
                "Postgres arrays are multi-dimensional and 1-based; flattened to "
                "a 1-D 0-based Delta ARRAY. " + (element.note or "")
            ).strip(),
            reverse_pg_type=f"{element.reverse_pg_type}[]",
        )

    if t in _PG_TO_DELTA_LOSSLESS:
        delta_type, reverse = _PG_TO_DELTA_LOSSLESS[t]
        return LogicalType(delta_type=delta_type, lossy=False, reverse_pg_type=reverse)

    if t in _PG_TO_DELTA_LOSSY:
        return LogicalType(lossy=True, **_PG_TO_DELTA_LOSSY[t])

    # numeric(p,s) -> DECIMAL(p,s), lossless within Delta's 38-digit cap.
    m = _NUMERIC_RE.match(t)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        if p > 38:
            return LogicalType(
                delta_type="DECIMAL(38,{})".format(min(s, 38)),
                lossy=True,
                encoding="decimal-clamped",
                note=f"numeric({p},{s}) exceeds Delta's 38-digit precision; clamped to 38",
                reverse_pg_type=f"numeric({p},{s})",
            )
        return LogicalType(delta_type=f"DECIMAL({p},{s})", reverse_pg_type=f"numeric({p},{s})")

    # Unparameterized numeric/decimal: arbitrary precision in PG, not in Delta.
    if _BARE_NUMERIC_RE.match(t):
        return LogicalType(
            delta_type="DECIMAL(38,18)",
            lossy=True,
            encoding="decimal-default",
            note="unparameterized numeric has arbitrary precision in Postgres; pinned to DECIMAL(38,18)",
            reverse_pg_type="numeric",
        )

    if _VARCHAR_RE.match(t):
        # Length limits are not enforced by Delta STRING. Not lossy for data,
        # but the constraint is dropped — record it.
        return LogicalType(
            delta_type="STRING",
            lossy=False,
            note="varchar/char length constraint not enforced in Delta STRING",
            reverse_pg_type="text",
        )

    if _TIMESTAMPTZ_ALIAS_RE.match(t) or "with time zone" in t and t.startswith("timestamp"):
        # timestamptz -> Delta TIMESTAMP (which is instant/UTC). Lossless for the
        # instant, but the original session tz offset is not stored.
        return LogicalType(
            delta_type="TIMESTAMP",
            lossy=False,
            encoding="utc-instant",
            note="timestamptz stored as UTC instant; original input offset not retained",
            reverse_pg_type="timestamp with time zone",
        )

    if _TIMESTAMP_RE.match(t):
        return LogicalType(
            delta_type="TIMESTAMP_NTZ",
            lossy=False,
            reverse_pg_type="timestamp without time zone",
        )

    if t in ("time", "timetz", "time with time zone", "time without time zone"):
        return LogicalType(
            delta_type="STRING",
            lossy=True,
            encoding="time-text",
            note="Delta has no TIME type; stored as ISO time text",
            reverse_pg_type="time",
        )

    raise TypeMappingError(f"no mapping defined for postgres type {pg_type!r}")


# ---------------------------------------------------------------------------
# Delta -> Postgres (reverse path, used by reverse-sync lakehouse->pg)
# ---------------------------------------------------------------------------

_DELTA_TO_PG: dict[str, str] = {
    "BOOLEAN": "boolean",
    "SHORT": "smallint",
    "BYTE": "smallint",
    "INTEGER": "integer",
    "INT": "integer",
    "LONG": "bigint",
    "BIGINT": "bigint",
    "FLOAT": "real",
    "DOUBLE": "double precision",
    "STRING": "text",
    "BINARY": "bytea",
    "DATE": "date",
    "TIMESTAMP": "timestamp with time zone",
    "TIMESTAMP_NTZ": "timestamp without time zone",
}

_DELTA_DECIMAL_RE = re.compile(r"^DECIMAL\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
_DELTA_ARRAY_RE = re.compile(r"^ARRAY<\s*(.+?)\s*>$", re.IGNORECASE)


def map_delta_to_pg(delta_type: str) -> str:
    """Best-effort reverse mapping for a Delta type with no ztap encoding hint.

    When the engine controls both sides it should pass the LogicalType
    (carrying reverse_pg_type) rather than calling this. This exists for Delta
    tables that originated outside ztap.
    """
    t = delta_type.strip()
    up = t.upper()
    if up in _DELTA_TO_PG:
        return _DELTA_TO_PG[up]
    m = _DELTA_DECIMAL_RE.match(t)
    if m:
        return f"numeric({m.group(1)},{m.group(2)})"
    m = _DELTA_ARRAY_RE.match(t)
    if m:
        return f"{map_delta_to_pg(m.group(1))}[]"
    if up.startswith("MAP<") or up.startswith("STRUCT<"):
        # No clean Postgres equivalent; jsonb is the pragmatic landing spot.
        return "jsonb"
    raise TypeMappingError(f"no reverse mapping for delta type {delta_type!r}")


def roundtrip_pg(pg_type: str) -> tuple[str, bool]:
    """Return (reconstructed_pg_type, is_lossy) for pg -> delta -> pg.

    A lossless mapping reconstructs to an equivalent Postgres type. A lossy one
    reconstructs to the *documented substitute*, and the caller is expected to
    surface ``is_lossy=True`` rather than treat the round-trip as identity.
    """
    lt = map_pg_to_delta(pg_type)
    return lt.reverse_pg_type, lt.lossy
