"""Adapter: type-engine LogicalType -> Unity Catalog column descriptor.

The implementation now lives in the shared type-engine package
(``ztap_typeengine.uc``) so both the control plane and the sync service use the
same logic. This module re-exports it for backwards compatibility.
"""

from ztap_typeengine.uc import uc_column, _spark_json_type, _uc_type_name  # noqa: F401

__all__ = ["uc_column"]
