"""Adapter: type-engine LogicalType -> Unity Catalog column descriptor.

Unity Catalog's table API wants each column expressed as three parallel things:
a ``type_name`` enum, a freeform ``type_text``, and a Spark-style ``type_json``
(plus precision/scale for decimals). The type-engine already decided the Delta
type; this module just translates that into UC's wire shape. It is the seam
where component #4 (type mapping) meets Unity Catalog.
"""

from __future__ import annotations

import json
import re

from ztap_typeengine import map_pg_to_delta
from ztap_typeengine.mapping import LogicalType

_DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),(\d+)\)$")
_ARRAY_RE = re.compile(r"^ARRAY<(.+)>$")

# type-engine delta type token -> (UC type_name enum, spark json type string)
_SCALAR = {
    "BOOLEAN": ("BOOLEAN", "boolean"),
    "SHORT": ("SHORT", "short"),
    "INTEGER": ("INT", "integer"),
    "INT": ("INT", "integer"),
    "LONG": ("LONG", "long"),
    "FLOAT": ("FLOAT", "float"),
    "DOUBLE": ("DOUBLE", "double"),
    "STRING": ("STRING", "string"),
    "BINARY": ("BINARY", "binary"),
    "DATE": ("DATE", "date"),
    "TIMESTAMP": ("TIMESTAMP", "timestamp"),
    "TIMESTAMP_NTZ": ("TIMESTAMP_NTZ", "timestamp_ntz"),
}


def _spark_json_type(delta_type: str):
    """Return the value that goes in a Spark StructField's "type" key."""
    m = _DECIMAL_RE.match(delta_type)
    if m:
        return f"decimal({m.group(1)},{m.group(2)})"
    m = _ARRAY_RE.match(delta_type)
    if m:
        return {
            "type": "array",
            "elementType": _spark_json_type(m.group(1)),
            "containsNull": True,
        }
    if delta_type in _SCALAR:
        return _SCALAR[delta_type][1]
    raise ValueError(f"cannot express delta type {delta_type!r} as Spark JSON")


def _uc_type_name(delta_type: str) -> tuple[str, int | None, int | None]:
    """Return (type_name_enum, precision, scale)."""
    m = _DECIMAL_RE.match(delta_type)
    if m:
        return "DECIMAL", int(m.group(1)), int(m.group(2))
    if _ARRAY_RE.match(delta_type):
        return "ARRAY", None, None
    if delta_type in _SCALAR:
        return _SCALAR[delta_type][0], None, None
    raise ValueError(f"no UC type_name for delta type {delta_type!r}")


def uc_column(name: str, pg_type: str, position: int, nullable: bool) -> tuple[dict, LogicalType]:
    """Build a UC column dict for a Postgres column, plus the LogicalType.

    The returned LogicalType carries the ``lossy`` flag and note, so callers can
    surface exactly which columns were converted lossily.
    """
    lt = map_pg_to_delta(pg_type)
    type_name, precision, scale = _uc_type_name(lt.delta_type)
    field_json = {
        "name": name,
        "type": _spark_json_type(lt.delta_type),
        "nullable": nullable,
        "metadata": {},
    }
    col = {
        "name": name,
        "type_text": lt.delta_type.lower(),
        "type_name": type_name,
        "type_json": json.dumps(field_json),
        "position": position,
        "nullable": nullable,
    }
    if precision is not None:
        col["type_precision"] = precision
        col["type_scale"] = scale
    return col, lt
