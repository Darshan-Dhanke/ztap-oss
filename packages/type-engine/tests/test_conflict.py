import pytest

from ztap_typeengine import ConflictPolicy, Row, resolve
from ztap_typeengine.conflict import ConflictError


def _pair(pg_v, lake_v, pg_ver, lake_ver, key="k1"):
    pg = Row(side="postgres", key=key, values=pg_v, version=pg_ver)
    lake = Row(side="lakehouse", key=key, values=lake_v, version=lake_ver)
    return pg, lake


def test_lww_newer_postgres_wins():
    pg, lake = _pair({"x": 2}, {"x": 1}, pg_ver=200, lake_ver=100)
    res = resolve(pg, lake, ConflictPolicy.LAST_WRITE_WINS)
    assert res.winner.side == "postgres"


def test_lww_newer_lakehouse_wins():
    pg, lake = _pair({"x": 2}, {"x": 9}, pg_ver=100, lake_ver=300)
    res = resolve(pg, lake, ConflictPolicy.LAST_WRITE_WINS)
    assert res.winner.side == "lakehouse"


def test_lww_equal_versions_breaks_tie_deterministically():
    pg, lake = _pair({"x": 2}, {"x": 9}, pg_ver=100, lake_ver=100)
    res = resolve(pg, lake, ConflictPolicy.LAST_WRITE_WINS, source_of_truth="lakehouse")
    assert res.winner.side == "lakehouse"
    assert "equal versions" in res.reason


def test_lww_missing_version_refuses():
    pg, lake = _pair({"x": 2}, {"x": 9}, pg_ver=None, lake_ver=100)
    with pytest.raises(ConflictError):
        resolve(pg, lake, ConflictPolicy.LAST_WRITE_WINS)


def test_source_of_truth_postgres():
    pg, lake = _pair({"x": 2}, {"x": 9}, pg_ver=1, lake_ver=999)
    res = resolve(pg, lake, ConflictPolicy.SOURCE_OF_TRUTH, source_of_truth="postgres")
    assert res.winner.side == "postgres"  # wins despite older version


def test_error_policy_raises():
    pg, lake = _pair({"x": 2}, {"x": 9}, pg_ver=1, lake_ver=2)
    with pytest.raises(ConflictError):
        resolve(pg, lake, ConflictPolicy.ERROR)


def test_merge_uses_callback():
    pg, lake = _pair({"a": 1, "b": 0}, {"a": 0, "b": 5}, pg_ver=1, lake_ver=2)

    def merge_fn(p, l):
        return {"a": p.values["a"], "b": l.values["b"]}

    res = resolve(pg, lake, ConflictPolicy.MERGE, merge_fn=merge_fn)
    assert res.merged_values == {"a": 1, "b": 5}


def test_merge_without_callback_raises():
    pg, lake = _pair({"a": 1}, {"a": 2}, pg_ver=1, lake_ver=2)
    with pytest.raises(ValueError):
        resolve(pg, lake, ConflictPolicy.MERGE)


def test_mismatched_keys_raise():
    pg = Row(side="postgres", key="k1", values={}, version=1)
    lake = Row(side="lakehouse", key="k2", values={}, version=1)
    with pytest.raises(ValueError):
        resolve(pg, lake, ConflictPolicy.LAST_WRITE_WINS)
