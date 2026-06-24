import pytest

from ztap_typeengine import (
    map_pg_to_delta,
    map_delta_to_pg,
    roundtrip_pg,
    TypeMappingError,
)


@pytest.mark.parametrize(
    "pg_type, delta_type",
    [
        ("integer", "INTEGER"),
        ("int4", "INTEGER"),
        ("bigint", "LONG"),
        ("boolean", "BOOLEAN"),
        ("double precision", "DOUBLE"),
        ("text", "STRING"),
        ("date", "DATE"),
        ("bytea", "BINARY"),
        ("numeric(10,2)", "DECIMAL(10,2)"),
        ("numeric(38,0)", "DECIMAL(38,0)"),
    ],
)
def test_lossless_pg_to_delta(pg_type, delta_type):
    lt = map_pg_to_delta(pg_type)
    assert lt.delta_type == delta_type
    assert lt.lossy is False


@pytest.mark.parametrize(
    "pg_type, expected_delta, expected_encoding",
    [
        ("jsonb", "STRING", "json-text"),
        ("uuid", "STRING", "uuid-text"),
        ("interval", "LONG", "interval-micros"),
        ("cidr", "STRING", "cidr-text"),
        ("inet", "STRING", "inet-text"),
        ("money", "DECIMAL(19,2)", "money-decimal"),
    ],
)
def test_lossy_types_are_flagged(pg_type, expected_delta, expected_encoding):
    lt = map_pg_to_delta(pg_type)
    assert lt.delta_type == expected_delta
    assert lt.encoding == expected_encoding
    assert lt.lossy is True, f"{pg_type} must be flagged lossy"
    assert lt.note, "lossy mapping must carry an explanatory note"


def test_arrays_are_lossy_and_nest():
    lt = map_pg_to_delta("integer[]")
    assert lt.delta_type == "ARRAY<INTEGER>"
    assert lt.lossy is True
    assert lt.reverse_pg_type == "integer[]"


def test_array_of_uuid_propagates_inner_note():
    lt = map_pg_to_delta("uuid[]")
    assert lt.delta_type == "ARRAY<STRING>"
    assert lt.lossy is True
    assert "uuid" in lt.note.lower()


def test_numeric_over_38_digits_clamps_lossy():
    lt = map_pg_to_delta("numeric(40,5)")
    assert lt.lossy is True
    assert lt.delta_type == "DECIMAL(38,5)"


def test_bare_numeric_is_lossy():
    lt = map_pg_to_delta("numeric")
    assert lt.lossy is True
    assert lt.delta_type == "DECIMAL(38,18)"


def test_timestamptz_vs_timestamp():
    tz = map_pg_to_delta("timestamptz")
    assert tz.delta_type == "TIMESTAMP"
    ntz = map_pg_to_delta("timestamp without time zone")
    assert ntz.delta_type == "TIMESTAMP_NTZ"


def test_varchar_length_constraint_dropped_but_not_data_lossy():
    lt = map_pg_to_delta("varchar(255)")
    assert lt.delta_type == "STRING"
    assert lt.lossy is False
    assert "length" in lt.note.lower()


def test_unknown_type_raises():
    with pytest.raises(TypeMappingError):
        map_pg_to_delta("some_custom_enum")


def test_empty_type_raises():
    with pytest.raises(TypeMappingError):
        map_pg_to_delta("   ")


@pytest.mark.parametrize(
    "delta_type, pg_type",
    [
        ("INTEGER", "integer"),
        ("LONG", "bigint"),
        ("STRING", "text"),
        ("DECIMAL(10,2)", "numeric(10,2)"),
        ("ARRAY<LONG>", "bigint[]"),
        ("MAP<STRING,STRING>", "jsonb"),
        ("STRUCT<a:INT>", "jsonb"),
    ],
)
def test_delta_to_pg(delta_type, pg_type):
    assert map_delta_to_pg(delta_type) == pg_type


def test_roundtrip_lossless():
    recon, lossy = roundtrip_pg("bigint")
    assert recon == "bigint"
    assert lossy is False


def test_roundtrip_lossy_returns_substitute_and_flags():
    recon, lossy = roundtrip_pg("jsonb")
    assert recon == "jsonb"
    assert lossy is True  # critical: round-trip is NOT silently treated as identity
