"""Continuous lakehouse -> Postgres reverse sync via Delta Change Data Feed.

Write normal INSERT / UPDATE / DELETE against a project's inbox Delta table
(`s3://warehouse/<project>/<table>_inbox`, CDF-enabled) from Trino/DBeaver, and
the change is applied to Postgres within one poll interval — no version numbers,
deletes included.

How it stays correct:
  * The inbox table has Change Data Feed enabled, so every insert/update/delete
    is recorded. The watcher reads the CDF *since the Delta version it last
    processed* (tracked in ztap_control.reverse_watch), collapses it to the net
    change per primary key, and applies upserts/deletes to Postgres.
  * Tracking the processed Delta version gives exactly-once and ordering — a row
    is never applied twice.
  * Loop prevention is structural: the inbox is written only from the lakehouse
    side; Postgres CDC flows to the *main* feed table, never the inbox.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import logging
import os
import time

import boto3
import httpx
import psycopg
from botocore.config import Config
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("reverse-watcher")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("WAREHOUSE_BUCKET", "warehouse")
SYNC_URL = os.getenv("SYNC_URL", "http://sync:8000")
INTERVAL = int(os.getenv("WATCH_INTERVAL", "10"))
PG_DSN = (
    f"host={os.getenv('PG_HOST','postgres')} port={os.getenv('PG_PORT','5432')} "
    f"user={os.getenv('PG_USER','ztap')} password={os.getenv('PG_PASSWORD','ztap')} "
    f"dbname={os.getenv('PG_DB','ztap')}"
)
_INBOX_MARKER = "_inbox/_delta_log/"
_CDF_META = {"_change_type", "_commit_version", "_commit_timestamp"}


def _storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL": S3_ENDPOINT, "AWS_ACCESS_KEY_ID": S3_KEY,
        "AWS_SECRET_ACCESS_KEY": S3_SECRET, "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }


def _s3():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET, region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _json_safe(v):
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, (_dt.date, _dt.datetime, _dt.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    return v


def _ensure_state_table() -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ztap_control")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ztap_control.reverse_watch ("
            "project text, tbl text, last_version bigint, "
            "PRIMARY KEY (project, tbl))"
        )


def _last_version(project: str, table: str) -> int:
    with psycopg.connect(PG_DSN) as conn:
        row = conn.execute(
            "SELECT last_version FROM ztap_control.reverse_watch WHERE project=%s AND tbl=%s",
            (project, table),
        ).fetchone()
    return row[0] if row else -1


def _set_version(project: str, table: str, version: int) -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO ztap_control.reverse_watch (project, tbl, last_version) "
            "VALUES (%s,%s,%s) ON CONFLICT (project,tbl) "
            "DO UPDATE SET last_version = EXCLUDED.last_version",
            (project, table, version),
        )


def _pk_col(project: str, table: str) -> str | None:
    with psycopg.connect(PG_DSN) as conn:
        row = conn.execute(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            "WHERE i.indrelid=%s::regclass AND i.indisprimary",
            (f'"proj_{project}"."{table}"',),
        ).fetchone()
    return row[0] if row else None


def discover_inboxes(s3) -> list[str]:
    locs: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            i = key.find(_INBOX_MARKER)
            if i > 0:
                locs.add(key[: i + len("_inbox")])
    return sorted(locs)


def process_inbox(loc: str) -> None:
    project, inbox = loc.split("/", 1)
    table = inbox[: -len("_inbox")]
    pk = _pk_col(project, table)
    if not pk:
        return

    dt = DeltaTable(f"s3://{BUCKET}/{loc}", storage_options=_storage_options())
    current = dt.version()
    last = _last_version(project, table)
    if current <= last:
        return

    cdf = dt.load_cdf(starting_version=last + 1).read_all().to_pylist()
    # Collapse to the net change per PK: order by commit version, drop preimages,
    # keep the final state (a later delete wins over an earlier upsert, etc.).
    net: dict = {}
    for r in sorted(cdf, key=lambda x: x.get("_commit_version", 0)):
        ct = r.get("_change_type")
        if ct == "update_preimage":
            continue
        row = {k: _json_safe(v) for k, v in r.items() if k not in _CDF_META}
        if pk not in row:
            continue
        op = "delete" if ct == "delete" else "upsert"
        net[row[pk]] = {"op": op, "row": row}

    changes = list(net.values())
    if changes:
        resp = httpx.post(
            f"{SYNC_URL}/projects/{project}/tables/{table}/reverse-apply",
            json={"changes": changes, "pk_col": pk}, timeout=30,
        )
        if resp.status_code >= 400:
            log.warning("reverse-apply %s.%s failed: %s %s", project, table, resp.status_code, resp.text)
            return
        log.info("inbox %s -> proj_%s.%s: %s (delta v%d..%d)",
                 loc, project, table, resp.json().get("applied"), last + 1, current)
    _set_version(project, table, current)


def main() -> None:
    log.info("reverse-watcher (CDF) starting: bucket=%s sync=%s interval=%ss", BUCKET, SYNC_URL, INTERVAL)
    _ensure_state_table()
    s3 = _s3()
    while True:
        try:
            for loc in discover_inboxes(s3):
                try:
                    process_inbox(loc)
                except Exception as e:  # noqa: BLE001
                    log.warning("inbox %s error: %s", loc, e)
        except Exception as e:  # noqa: BLE001
            log.warning("cycle error: %s", e)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
