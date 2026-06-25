"""Continuous lakehouse -> Postgres reverse sync (component #2, continuous mode).

Makes reverse sync automatic: write into a project's *inbox* Delta table from
Trino/DBeaver, and the change shows up in Postgres within one poll interval — no
manual API call.

Loop prevention by design: the inbox table (``s3://warehouse/<project>/<table>_inbox``)
is written *only* by external/Trino writers. Postgres writes flow forward into
the main feed table, never the inbox, so applying inbox rows back to Postgres
cannot echo into the inbox. On top of that, the sync service's idempotency
ledger (keyed by primary key + ``_lake_version``) skips anything already applied,
so re-reading the whole inbox each cycle is safe.

Inbox schema convention: the Postgres table's columns plus a monotonic
``_lake_version bigint`` used as the conflict version (last-write-wins).
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
VERSION_COL = os.getenv("VERSION_COL", "_lake_version")
INTERVAL = int(os.getenv("WATCH_INTERVAL", "10"))

PG_DSN = (
    f"host={os.getenv('PG_HOST','postgres')} port={os.getenv('PG_PORT','5432')} "
    f"user={os.getenv('PG_USER','ztap')} password={os.getenv('PG_PASSWORD','ztap')} "
    f"dbname={os.getenv('PG_DB','ztap')}"
)

_INBOX_MARKER = "_inbox/_delta_log/"


def _storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL": S3_ENDPOINT,
        "AWS_ACCESS_KEY_ID": S3_KEY,
        "AWS_SECRET_ACCESS_KEY": S3_SECRET,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }


def _s3():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET, region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def discover_inboxes(s3) -> list[str]:
    """Return inbox locations relative to the bucket, e.g. 'sales/orders_inbox'."""
    locs: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            i = key.find(_INBOX_MARKER)
            if i > 0:
                locs.add(key[: i + len("_inbox")])
    return sorted(locs)


def _pk_col(project: str, table: str) -> str | None:
    """Primary key column of proj_<project>.<table>, via Postgres."""
    schema = f"proj_{project}"
    with psycopg.connect(PG_DSN) as conn:
        row = conn.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            """,
            (f'"{schema}"."{table}"',),
        ).fetchone()
    return row[0] if row else None


def _json_safe(v):
    """Coerce delta-rs values into JSON-serializable ones for the sync API.
    Decimals/dates become strings, which Postgres accepts for numeric/date cols."""
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, (_dt.date, _dt.datetime, _dt.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    return v


def process_inbox(loc: str) -> None:
    project, inbox = loc.split("/", 1)        # 'sales', 'orders_inbox'
    table = inbox[: -len("_inbox")]            # 'orders'
    pk = _pk_col(project, table)
    if not pk:
        log.warning("no primary key for proj_%s.%s; skipping inbox", project, table)
        return

    dt = DeltaTable(f"s3://{BUCKET}/{loc}", storage_options=_storage_options())
    rows = [{k: _json_safe(v) for k, v in r.items()} for r in dt.to_pyarrow_table().to_pylist()]
    if not rows:
        return

    resp = httpx.post(
        f"{SYNC_URL}/projects/{project}/tables/{table}/reverse-sync",
        json={
            "rows": rows,
            "pk_col": pk,
            # Inbox writes are explicit lakehouse intent, so the lakehouse wins;
            # _lake_version still drives idempotency (apply each version once).
            "version_col": VERSION_COL,
            "policy": "source_of_truth",
            "source_of_truth": "lakehouse",
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        log.warning("reverse-sync %s.%s failed: %s %s", project, table, resp.status_code, resp.text)
        return
    applied = resp.json().get("applied", 0)
    if applied:
        log.info("inbox %s -> proj_%s.%s: %d row(s) applied", loc, project, table, applied)


def main() -> None:
    log.info("reverse-watcher starting: bucket=%s sync=%s interval=%ss", BUCKET, SYNC_URL, INTERVAL)
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
