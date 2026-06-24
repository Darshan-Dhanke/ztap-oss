import pytest

from app.transform import (
    parse_topic, storage_location, to_record, SinkRecord, TransformError,
)


def test_parse_topic_ok():
    assert parse_topic("ztap.analytics.proj_analytics.events") == ("analytics", "events")


def test_parse_topic_with_underscores_in_project():
    assert parse_topic("ztap.my_proj.proj_my_proj.orders") == ("my_proj", "orders")


@pytest.mark.parametrize("topic", [
    "ztap.analytics.events",                       # missing proj_ schema segment
    "other.analytics.proj_analytics.events",       # wrong prefix
    "ztap.a.proj_b.events",                         # project/schema mismatch
])
def test_parse_topic_rejects_bad(topic):
    with pytest.raises(TransformError):
        parse_topic(topic)


def test_storage_location_matches_control_plane_convention():
    assert storage_location("analytics", "events") == "s3://warehouse/analytics/events"
    assert storage_location("analytics", "events", bucket="lake") == "s3://lake/analytics/events"


def test_to_record_insert():
    value = {"id": 1, "name": "a", "__op": "c", "__ts_ms": 1700, "__deleted": "false"}
    rec = to_record("ztap.p.proj_p.t", value)
    assert isinstance(rec, SinkRecord)
    assert rec.project == "p" and rec.table == "t"
    assert rec.op == "c"
    assert rec.deleted is False
    assert rec.row["id"] == 1 and rec.row["name"] == "a"
    assert rec.row["_op"] == "c" and rec.row["_deleted"] is False
    # metadata fields stripped from data
    assert "__op" not in rec.row and "__ts_ms" not in rec.row


def test_to_record_delete_is_flagged():
    value = {"id": 5, "name": None, "__op": "d", "__ts_ms": 1800, "__deleted": "true"}
    rec = to_record("ztap.p.proj_p.t", value)
    assert rec.deleted is True
    assert rec.op == "d"
    assert rec.row["_deleted"] is True


def test_to_record_snapshot_defaults_to_r():
    value = {"id": 9, "__deleted": "false"}  # no __op -> snapshot read
    rec = to_record("ztap.p.proj_p.t", value)
    assert rec.op == "r"


def test_to_record_tombstone_returns_none():
    assert to_record("ztap.p.proj_p.t", None) is None


def test_to_record_no_data_columns_raises():
    with pytest.raises(TransformError):
        to_record("ztap.p.proj_p.t", {"__op": "c", "__deleted": "false"})
