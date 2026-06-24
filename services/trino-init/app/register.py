"""Auto-register every Delta table in the lakehouse into Trino, on a loop.

Trino's file metastore is in-container and ephemeral, and the delta_lake
connector needs each table registered by its storage location before it can be
queried. This sidecar makes that automatic: it scans MinIO for Delta tables
(directories containing a ``_delta_log``), and registers any it finds in Trino's
``delta.lakehouse`` schema. Running on a loop means tables created later (new
projects, new sink output) appear within one interval, and registrations are
re-created automatically if Trino restarts.

Naming: a table at ``s3://warehouse/<project>/<table>`` is registered as
``delta.lakehouse.<project>_<table>``.
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
SCHEMA = os.getenv("TRINO_SCHEMA", "lakehouse")
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


def ensure_schema(cur) -> None:
    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS delta.{SCHEMA} "
        f"WITH (location = 's3://{BUCKET}/{SCHEMA}')"
    )
    cur.fetchall()


def register(cur, loc: str) -> bool:
    name = loc.replace("/", "_")
    location = f"s3://{BUCKET}/{loc}"
    try:
        cur.execute(
            f"CALL delta.system.register_table("
            f"schema_name => '{SCHEMA}', table_name => '{name}', "
            f"table_location => '{location}')"
        )
        cur.fetchall()
        log.info("registered delta.%s.%s -> %s", SCHEMA, name, location)
        return True
    except TrinoUserError as e:
        if "already exists" in str(e).lower():
            return False
        log.warning("register %s failed: %s", name, e)
        return False


def cycle(client) -> None:
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="ztap-trino-init", catalog="delta")
    try:
        cur = conn.cursor()
        ensure_schema(cur)
        locs = discover_tables(client)
        new = sum(register(cur, loc) for loc in locs)
        log.info("sync complete: %d delta table(s) visible (%d newly registered)", len(locs), new)
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
