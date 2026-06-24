import pytest

from ztap_typeengine import ConflictPolicy, Row

from app.schema_diff import PgColumn, TargetColumn, diff_schema
from app.state_machine import (
    SyncState, next_schema_state, decide_reverse_apply, ReverseAction,
)


# --- schema state transitions ---

def test_state_in_sync():
    d = diff_schema([PgColumn("id", "bigint")], [TargetColumn("id", "LONG")])
    assert next_schema_state(SyncState.IN_SYNC, d) == SyncState.IN_SYNC


def test_state_drift_on_added_column():
    d = diff_schema([PgColumn("id", "bigint"), PgColumn("x", "text")], [TargetColumn("id", "LONG")])
    assert next_schema_state(SyncState.IN_SYNC, d) == SyncState.SCHEMA_DRIFT


# --- reverse-apply decisions ---

def _lake(key, values, version):
    return Row(side="lakehouse", key=key, values=values, version=version)

def _pg(key, values, version):
    return Row(side="postgres", key=key, values=values, version=version)


def test_insert_when_no_pg_row():
    lake = _lake("k", {"id": "k", "v": 1}, version=10)
    d = decide_reverse_apply(lake, None, policy=ConflictPolicy.LAST_WRITE_WINS)
    assert d.action == ReverseAction.APPLY
    assert d.values == {"id": "k", "v": 1}


def test_idempotent_skip_when_already_applied():
    lake = _lake("k", {"v": 1}, version=5)
    pg = _pg("k", {"v": 0}, version=3)
    d = decide_reverse_apply(lake, pg, policy=ConflictPolicy.LAST_WRITE_WINS,
                             last_applied_version=5)
    assert d.action == ReverseAction.SKIP_DUPLICATE


def test_older_lake_change_is_skipped_as_duplicate():
    lake = _lake("k", {"v": 1}, version=4)
    pg = _pg("k", {"v": 9}, version=10)
    d = decide_reverse_apply(lake, pg, policy=ConflictPolicy.LAST_WRITE_WINS,
                             last_applied_version=7)
    assert d.action == ReverseAction.SKIP_DUPLICATE


def test_lake_wins_lww_applies():
    lake = _lake("k", {"v": 99}, version=20)
    pg = _pg("k", {"v": 1}, version=10)
    d = decide_reverse_apply(lake, pg, policy=ConflictPolicy.LAST_WRITE_WINS)
    assert d.action == ReverseAction.APPLY
    assert d.values == {"v": 99}


def test_pg_wins_lww_skips():
    lake = _lake("k", {"v": 99}, version=5)
    pg = _pg("k", {"v": 1}, version=50)
    d = decide_reverse_apply(lake, pg, policy=ConflictPolicy.LAST_WRITE_WINS)
    assert d.action == ReverseAction.SKIP_PG_WINS


def test_source_of_truth_postgres_always_skips_lake():
    lake = _lake("k", {"v": 99}, version=999)
    pg = _pg("k", {"v": 1}, version=1)
    d = decide_reverse_apply(lake, pg, policy=ConflictPolicy.SOURCE_OF_TRUTH,
                             source_of_truth="postgres")
    assert d.action == ReverseAction.SKIP_PG_WINS


def test_merge_policy_returns_merged_values():
    lake = _lake("k", {"a": 0, "b": 5}, version=2)
    pg = _pg("k", {"a": 1, "b": 0}, version=1)
    d = decide_reverse_apply(
        lake, pg, policy=ConflictPolicy.MERGE,
        merge_fn=lambda p, l: {"a": p.values["a"], "b": l.values["b"]},
    )
    assert d.action == ReverseAction.APPLY_MERGE
    assert d.values == {"a": 1, "b": 5}


def test_error_policy_surfaces_error():
    lake = _lake("k", {"v": 99}, version=2)
    pg = _pg("k", {"v": 1}, version=1)
    d = decide_reverse_apply(lake, pg, policy=ConflictPolicy.ERROR)
    assert d.action == ReverseAction.ERROR
