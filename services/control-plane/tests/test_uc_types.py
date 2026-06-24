"""Unit tests for the type-engine -> Unity Catalog column adapter."""

import json

import pytest

from app.uc_types import uc_column


def test_integer_maps_to_uc_int():
    col, lt = uc_column("id", "integer", 0, False)
    assert col["type_name"] == "INT"
    assert col["type_text"] == "integer"
    assert col["nullable"] is False
    assert lt.lossy is False
    assert json.loads(col["type_json"])["type"] == "integer"


def test_bigint_maps_to_long():
    col, _ = uc_column("n", "bigint", 1, True)
    assert col["type_name"] == "LONG"
    assert json.loads(col["type_json"])["type"] == "long"


def test_decimal_carries_precision_scale():
    col, _ = uc_column("amt", "numeric(12,4)", 2, True)
    assert col["type_name"] == "DECIMAL"
    assert col["type_precision"] == 12
    assert col["type_scale"] == 4
    assert json.loads(col["type_json"])["type"] == "decimal(12,4)"


def test_jsonb_is_string_and_lossy():
    col, lt = uc_column("doc", "jsonb", 0, True)
    assert col["type_name"] == "STRING"
    assert lt.lossy is True


def test_uuid_is_string_and_lossy():
    col, lt = uc_column("uid", "uuid", 0, False)
    assert col["type_name"] == "STRING"
    assert lt.lossy is True


def test_interval_is_long_and_lossy():
    col, lt = uc_column("dur", "interval", 0, True)
    assert col["type_name"] == "LONG"
    assert lt.lossy is True


def test_array_maps_to_uc_array_with_nested_json():
    col, lt = uc_column("tags", "integer[]", 0, True)
    assert col["type_name"] == "ARRAY"
    assert lt.lossy is True
    tj = json.loads(col["type_json"])
    assert tj["type"]["type"] == "array"
    assert tj["type"]["elementType"] == "integer"


def test_timestamptz_maps_to_timestamp():
    col, _ = uc_column("ts", "timestamp with time zone", 0, True)
    assert col["type_name"] == "TIMESTAMP"


def test_unknown_type_raises():
    with pytest.raises(Exception):
        uc_column("x", "some_enum_type", 0, True)
