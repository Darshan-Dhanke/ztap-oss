"""Auto-register every Delta table in the lakehouse into Trino, on a loop.

Trino's file metastore is in-container and ephemeral, and the delta_lake
connector needs each table registered by its storage location before it can be
queried. This sidecar makes that automatic: it scans MinIO for Delta tables
(directories containing a ``_delta_log``), and registers any it finds in Trino's
``delta.lakehouse`` schema. Running on a loop means tables created later (new
projects, new sink output) appear within one interval, and registrations are
re-created automatically if Trino restarts.

Naming mirrors Postgres exactly: a table at ``s3://warehouse/<project>/<table>``
is registered as ``delta.proj_<project>.<table>`` — same schema name and same
table name as the Postgres side (``proj_<project>.<table>``). Only the catalog
differs (``delta`` vs the Postgres connection), which is what marks the source.
"""

from __future__ import annotations

import logging
import os
import time

import boto3
from botocore.config import Config
from trino.dbapi import connect
from trino.exceptions import TrinoUserError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("trino-init")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("WAREHOUSE_BUCKET", "warehouse")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
# Trino schema is named to match the Postgres schema: proj_<project>.
SCHEMA_PREFIX = os.getenv("TRINO_SCHEMA_PREFIX", "proj_")
INTERVAL = int(os.getenv("REGISTER_INTERVAL", "30"))

_DELTA_MARKER = "/_delta_log/"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def discover_tables(client) -> list[str]:
    """Return distinct table locations (relative to the bucket), e.g. 'lake/orders'."""
    locs: set[str] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            i = key.find(_DELTA_MARKER)
            if i > 0:
                locs.add(key[:i])
    return sorted(locs)


def _split(loc: str) -> tuple[str, str]:
    """'lake/orders' -> (schema 'proj_lake', table 'orders')."""
    parts = loc.split("/")
    project = parts[0]
    table = "_".join(parts[1:]) if len(parts) > 1 else project
    return f"{SCHEMA_PREFIX}{project}", table


def ensure_schema(cur, schema: str, project: str) -> None:
    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS delta.{schema} "
        f"WITH (location = 's3://{BUCKET}/{project}')"
    )
    cur.fetchall()


def register(cur, schema: str, table: str, location: str) -> bool:
    try:
        cur.execute(
            f"CALL delta.system.register_table("
            f"schema_name => '{schema}', table_name => '{table}', "
            f"table_location => '{location}')"
        )
        cur.fetchall()
        log.info("registered delta.%s.%s -> %s", schema, table, location)
        return True
    except TrinoUserError as e:
        if "already exists" in str(e).lower():
            return False
        log.warning("register %s.%s failed: %s", schema, table, e)
        return False


def reconcile(cur, current_locs: list[str]) -> int:
    """Drop registrations whose Delta data no longer exists in MinIO, so Trino's
    view always matches ground truth (and stale test tables disappear)."""
    current = set(current_locs)
    cur.execute("SHOW SCHEMAS FROM delta")
    schemas = [r[0] for r in cur.fetchall() if r[0].startswith(SCHEMA_PREFIX)]
    removed = 0
    for schema in schemas:
        project = schema[len(SCHEMA_PREFIX):]
        cur.execute(f"SHOW TABLES FROM delta.{schema}")
        tables = [r[0] for r in cur.fetchall()]
        for table in tables:
            if f"{project}/{table}" not in current:
                try:
                    cur.execute(
                        f"CALL delta.system.unregister_table("
                        f"schema_name => '{schema}', table_name => '{table}')"
                    )
                    cur.fetchall()
                    log.info("unregistered stale delta.%s.%s", schema, table)
                    removed += 1
                except TrinoUserError as e:
                    log.warning("unregister %s.%s failed: %s", schema, table, e)
        cur.execute(f"SHOW TABLES FROM delta.{schema}")
        if not cur.fetchall():
            try:
                cur.execute(f"DROP SCHEMA delta.{schema}")
                cur.fetchall()
                log.info("dropped empty schema delta.%s", schema)
            except TrinoUserError as e:
                log.warning("drop schema %s failed: %s", schema, e)
    return removed


def cycle(client) -> None:
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="ztap-trino-init", catalog="delta")
    try:
        cur = conn.cursor()
        locs = discover_tables(client)
        schemas_done: set[str] = set()
        new = 0
        for loc in locs:
            project = loc.split("/")[0]
            schema, table = _split(loc)
            if schema not in schemas_done:
                ensure_schema(cur, schema, project)
                schemas_done.add(schema)
            new += register(cur, schema, table, f"s3://{BUCKET}/{loc}")
        removed = reconcile(cur, locs)
        log.info("sync complete: %d delta table(s) visible (%d new, %d stale removed)",
                 len(locs), new, removed)
    finally:
        conn.close()


def main() -> None:
    log.info("trino-init starting: bucket=%s trino=%s:%s interval=%ss",
             BUCKET, TRINO_HOST, TRINO_PORT, INTERVAL)
    client = s3_client()
    while True:
        try:
            cycle(client)
        except Exception as e:  # noqa: BLE001
            log.warning("cycle error (will retry): %s", e)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
