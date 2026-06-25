"""I/O layer for the sync service.

Talks to Postgres and Unity Catalog to:
  * detect and reconcile schema drift between a PG table and its UC registration
  * apply lakehouse-originated row changes back into Postgres, governed by the
    conflict policy and an idempotency ledger (loop-prevention).

The decision logic lives in the pure modules (schema_diff, state_machine); this
file only does the reads/writes those decisions imply.
"""

from __future__ import annotations

from typing import Any, Optional

import httpx
import psycopg
from psycopg.rows import dict_row

from ztap_typeengine import ConflictPolicy, Row, uc_column

from .schema_diff import PgColumn, TargetColumn, diff_schema, SchemaDiff
from .state_machine import (
    SyncState, next_schema_state, decide_reverse_apply, ReverseAction, ReverseDecision,
)
from .settings import settings


class ReconcileError(RuntimeError):
    pass


# --- Postgres introspection -------------------------------------------------

def _pg_columns(schema: str, table: str) -> list[PgColumn]:
    with psycopg.connect(settings.pg_dsn(), autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type,
                   NOT a.attnotnull AS nullable
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (schema, table),
        ).fetchall()
    return [PgColumn(name=r[0], pg_type=r[1], nullable=r[2]) for r in rows]


# --- Unity Catalog ----------------------------------------------------------

def _uc_columns(catalog: str, table: str) -> list[TargetColumn]:
    url = f"{settings.uc_url}/api/2.1/unity-catalog/tables/{catalog}.cdc.{table}"
    with httpx.Client(timeout=15) as c:
        r = c.get(url)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        cols = r.json().get("columns", [])
    return [TargetColumn(name=c["name"], delta_type=c.get("type_text", "")) for c in cols]


def _reregister_uc_table(name: str, catalog: str, table: str, pg_cols: list[PgColumn]) -> None:
    """Re-register the UC table to match the current Postgres schema.

    UC EXTERNAL tables are metadata pointing at the Delta location, so dropping
    and recreating the registration is a safe way to evolve the schema — the
    data in MinIO is untouched, and the sink's schema_mode=merge already added
    the new column to the Delta files.
    """
    base = f"{settings.uc_url}/api/2.1/unity-catalog/tables"
    storage = f"s3://{settings.bucket}/{name}/{table}"
    columns = [uc_column(c.name, c.pg_type, i, c.nullable)[0] for i, c in enumerate(pg_cols)]
    payload = {
        "name": table, "catalog_name": catalog, "schema_name": "cdc",
        "table_type": "EXTERNAL", "data_source_format": "DELTA",
        "storage_location": storage, "columns": columns,
    }
    with httpx.Client(timeout=20) as c:
        c.delete(f"{base}/{catalog}.cdc.{table}")  # idempotent drop
        r = c.post(base, json=payload)
        if r.status_code >= 400 and r.status_code != 409:
            raise ReconcileError(f"UC re-register failed: {r.status_code}: {r.text}")


def reconcile_schema(name: str, table: str) -> dict:
    """Detect drift between PG and UC for a table and reconcile UC to match PG."""
    schema = f"proj_{name}"
    catalog = f"ztap_{name}"
    pg_cols = _pg_columns(schema, table)
    if not pg_cols:
        raise ReconcileError(f"table {schema}.{table} not found")
    uc_cols = _uc_columns(catalog, table)

    diff = diff_schema(pg_cols, uc_cols)
    state = next_schema_state(SyncState.IN_SYNC, diff)

    reconciled = False
    if not diff.in_sync:
        _reregister_uc_table(name, catalog, table, pg_cols)
        reconciled = True
        state = SyncState.IN_SYNC

    return {
        "table": f"{catalog}.cdc.{table}",
        "state": state.value,
        "diff": diff.summary(),
        "reconciled": reconciled,
    }


# --- reverse sync (lakehouse -> Postgres) -----------------------------------

def _ensure_ledger() -> None:
    with psycopg.connect(settings.pg_dsn(), autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ztap_control")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ztap_control.sync_applied (
                project text NOT NULL,
                tbl     text NOT NULL,
                pk      text NOT NULL,
                lake_version bigint,
                applied_at timestamptz DEFAULT now(),
                PRIMARY KEY (project, tbl, pk)
            )
            """
        )


def _last_applied(conn, project: str, table: str, pk: str) -> Optional[int]:
    row = conn.execute(
        "SELECT lake_version FROM ztap_control.sync_applied "
        "WHERE project=%s AND tbl=%s AND pk=%s",
        (project, table, str(pk)),
    ).fetchone()
    # the reverse_sync connection uses dict_row, so index by column name
    return row["lake_version"] if row else None


def _current_pg_row(conn, schema: str, table: str, pk_col: str, pk: Any,
                    version_col: Optional[str]) -> Optional[Row]:
    rec = conn.execute(
        f'SELECT * FROM "{schema}"."{table}" WHERE "{pk_col}" = %s', (pk,)
    ).fetchone()
    if rec is None:
        return None
    version = rec.get(version_col) if version_col else None
    return Row(side="postgres", key=pk, values=dict(rec), version=version)


def reverse_sync(
    name: str,
    table: str,
    rows: list[dict],
    *,
    pk_col: str,
    version_col: Optional[str] = None,
    policy: str = "last_write_wins",
    source_of_truth: str = "postgres",
) -> dict:
    """Apply lakehouse-originated rows into Postgres with conflict resolution.

    Each row must contain the primary key column and (for last_write_wins) a
    version. Returns a per-row report of the action taken.
    """
    _ensure_ledger()
    schema = f"proj_{name}"
    pol = ConflictPolicy(policy)
    results = []

    with psycopg.connect(settings.pg_dsn(), row_factory=dict_row) as conn:
        conn.autocommit = False
        # Only ever write columns that actually exist in the Postgres table, so
        # extra lakehouse-side columns (e.g. an inbox's _lake_version marker) are
        # ignored rather than causing an "column does not exist" error.
        pg_cols = {
            row["column_name"]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            ).fetchall()
        }
        for r in rows:
            if pk_col not in r:
                raise ReconcileError(f"row missing pk column {pk_col!r}: {r}")
            pk = r[pk_col]
            version = r.get(version_col) if version_col else None
            lake_row = Row(side="lakehouse", key=pk, values=r, version=version)

            pg_row = _current_pg_row(conn, schema, table, pk_col, pk, version_col)
            last = _last_applied(conn, name, table, str(pk))
            decision: ReverseDecision = decide_reverse_apply(
                lake_row, pg_row, policy=pol,
                last_applied_version=last, source_of_truth=source_of_truth,
            )

            if decision.action in (ReverseAction.APPLY, ReverseAction.APPLY_MERGE):
                vals = {k: v for k, v in (decision.values or r).items() if k in pg_cols}
                _upsert(conn, schema, table, pk_col, vals)
                _record_applied(conn, name, table, str(pk), version)
            results.append({"pk": pk, "action": decision.action.value, "reason": decision.reason})
        conn.commit()

    applied = sum(1 for x in results if x["action"].startswith("apply"))
    return {"table": f"{schema}.{table}", "applied": applied, "results": results}


def reverse_apply(name: str, table: str, changes: list[dict], *, pk_col: str) -> dict:
    """Apply already-resolved lakehouse changes (from the inbox CDF) to Postgres.

    ``changes`` is an ordered list of {"op": "upsert"|"delete", "row": {...}}.
    The lakehouse is authoritative for inbox writes, so there's no per-row
    conflict policy here — exactly-once + ordering are guaranteed by the watcher
    (it tracks the inbox's Delta version). Columns not present in the Postgres
    table (CDF metadata, etc.) are ignored.
    """
    schema = f"proj_{name}"
    applied = {"upsert": 0, "delete": 0}
    with psycopg.connect(settings.pg_dsn(), row_factory=dict_row) as conn:
        conn.autocommit = False
        pg_cols = {
            r["column_name"]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            ).fetchall()
        }
        for ch in changes:
            row = ch.get("row", {})
            if pk_col not in row:
                raise ReconcileError(f"change missing pk column {pk_col!r}: {row}")
            if ch.get("op") == "delete":
                conn.execute(f'DELETE FROM "{schema}"."{table}" WHERE "{pk_col}" = %s', (row[pk_col],))
                applied["delete"] += 1
            else:
                vals = {k: v for k, v in row.items() if k in pg_cols}
                _upsert(conn, schema, table, pk_col, vals)
                applied["upsert"] += 1
        conn.commit()
    return {"table": f"{schema}.{table}", "applied": applied}


def _upsert(conn, schema: str, table: str, pk_col: str, values: dict) -> None:
    cols = list(values.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(f'"{c}"' for c in cols)
    updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk_col)
    sql = (
        f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT ("{pk_col}") DO UPDATE SET {updates}'
        if updates else
        f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT ("{pk_col}") DO NOTHING'
    )
    conn.execute(sql, [values[c] for c in cols])


def _record_applied(conn, project: str, table: str, pk: str, version: Optional[int]) -> None:
    conn.execute(
        """
        INSERT INTO ztap_control.sync_applied (project, tbl, pk, lake_version)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (project, tbl, pk)
        DO UPDATE SET lake_version = EXCLUDED.lake_version, applied_at = now()
        """,
        (project, table, pk, version),
    )
