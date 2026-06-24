"""ztap type-engine: Postgres <-> Delta/Lakehouse type mapping and conflict resolution.

This is custom component #4 in the ztap-oss architecture. It is deliberately
infrastructure-free: a pure-Python, config-driven engine so it can be unit
tested in isolation and reused by both the sync service and the control plane.
"""

from .mapping import (
    LogicalType,
    TypeMappingError,
    map_pg_to_delta,
    map_delta_to_pg,
    roundtrip_pg,
)
from .conflict import (
    ConflictPolicy,
    ConflictResolution,
    Row,
    resolve,
)

__all__ = [
    "LogicalType",
    "TypeMappingError",
    "map_pg_to_delta",
    "map_delta_to_pg",
    "roundtrip_pg",
    "ConflictPolicy",
    "ConflictResolution",
    "Row",
    "resolve",
]

__version__ = "0.1.0"
