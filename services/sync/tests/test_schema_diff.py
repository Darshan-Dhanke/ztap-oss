from app.schema_diff import PgColumn, TargetColumn, diff_schema


def test_in_sync_when_identical():
    pg = [PgColumn("id", "bigint"), PgColumn("name", "text")]
    tgt = [TargetColumn("id", "LONG"), TargetColumn("name", "STRING")]
    d = diff_schema(pg, tgt)
    assert d.in_sync
    assert d.summary()["in_sync"] is True


def test_added_column_detected():
    pg = [PgColumn("id", "bigint"), PgColumn("status", "text")]
    tgt = [TargetColumn("id", "LONG")]
    d = diff_schema(pg, tgt)
    assert [c.name for c in d.added] == ["status"]
    assert not d.in_sync


def test_removed_column_detected():
    pg = [PgColumn("id", "bigint")]
    tgt = [TargetColumn("id", "LONG"), TargetColumn("legacy", "STRING")]
    d = diff_schema(pg, tgt)
    assert [c.name for c in d.removed] == ["legacy"]


def test_type_change_detected_via_typeengine():
    # column existed as text/STRING, now it's bigint -> LONG
    pg = [PgColumn("val", "bigint")]
    tgt = [TargetColumn("val", "STRING")]
    d = diff_schema(pg, tgt)
    assert len(d.type_changed) == 1
    assert d.type_changed[0].from_type == "STRING"
    assert d.type_changed[0].to_type == "LONG"


def test_decimal_type_normalized_match():
    pg = [PgColumn("amount", "numeric(10,2)")]
    tgt = [TargetColumn("amount", "decimal(10,2)")]  # case differs only
    d = diff_schema(pg, tgt)
    assert d.in_sync


def test_combined_diff_summary():
    pg = [PgColumn("id", "bigint"), PgColumn("status", "text"), PgColumn("val", "bigint")]
    tgt = [TargetColumn("id", "LONG"), TargetColumn("val", "STRING"), TargetColumn("old", "STRING")]
    d = diff_schema(pg, tgt)
    s = d.summary()
    assert s["added"] == ["status"]
    assert s["removed"] == ["old"]
    assert s["type_changed"][0]["column"] == "val"
