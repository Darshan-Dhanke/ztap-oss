import json

from app.writer import rows_to_arrow, _normalize_value


def test_rows_to_arrow_basic():
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    t = rows_to_arrow(rows)
    assert t.num_rows == 2
    assert set(t.column_names) == {"id", "name"}
    assert t.column("id").to_pylist() == [1, 2]


def test_rows_to_arrow_column_union_fills_missing():
    # second row gained a column (schema evolution mid-batch)
    rows = [{"id": 1}, {"id": 2, "extra": "x"}]
    t = rows_to_arrow(rows)
    assert set(t.column_names) == {"id", "extra"}
    assert t.column("extra").to_pylist() == [None, "x"]


def test_normalize_serializes_nested_json_stably():
    assert _normalize_value({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    assert _normalize_value([1, 2, 3]) == "[1,2,3]"


def test_normalize_passthrough_scalars():
    assert _normalize_value(5) == 5
    assert _normalize_value("hi") == "hi"
    assert _normalize_value(None) is None


def test_rows_to_arrow_drops_all_null_column():
    # 'note' is null in every row -> dropped (delta-rs can't type a null column)
    rows = [{"id": 1, "note": None}, {"id": 2, "note": None}]
    t = rows_to_arrow(rows)
    assert t.column_names == ["id"]


def test_rows_to_arrow_keeps_partially_null_column():
    rows = [{"id": 1, "note": None}, {"id": 2, "note": "x"}]
    t = rows_to_arrow(rows)
    assert set(t.column_names) == {"id", "note"}
    assert t.column("note").to_pylist() == [None, "x"]


def test_rows_to_arrow_normalizes_jsonb_object():
    rows = [{"id": 1, "doc": {"hello": "ztap"}}]
    t = rows_to_arrow(rows)
    assert t.column("doc").to_pylist() == ['{"hello":"ztap"}']
