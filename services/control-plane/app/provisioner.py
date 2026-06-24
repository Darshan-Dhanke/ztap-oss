"""Provisioning logic for a ztap "project".

A project is the unified abstraction the control plane hands you. Creating one
wires together, in order:

    1. a Postgres schema           (the lightweight "branch" of the compute DB)
    2. a Unity Catalog catalog     (governance / metadata registration)
    3. a Debezium connector        (WAL capture for that schema -> Kafka)

Teardown runs the same steps in *reverse*, and is best-effort: a failure to
delete one resource still attempts the rest, so a half-created project can
always be cleaned up.

Every step is idempotent where it reasonably can be, and each external call is
isolated so a failure in one subsystem reports clearly which step failed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

import httpx
import psycopg

from .settings import Settings


_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,48}$")


class ProvisionError(RuntimeError):
    def __init__(self, step: str, detail: str):
        super().__init__(f"[{step}] {detail}")
        self.step = step
        self.detail = detail


def validate_name(name: str) -> None:
    if not _NAME_RE.match(name):
        raise ProvisionError(
            "validate",
            f"invalid project name {name!r}: must match {_NAME_RE.pattern} "
            "(lowercase, starts with a letter, 2-49 chars)",
        )


@dataclass
class ProjectState:
    name: str
    status: str = "pending"
    schema: Optional[str] = None
    catalog: Optional[str] = None
    connector: Optional[str] = None
    cdc_topic_prefix: Optional[str] = None
    steps_completed: list = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Provisioner:
    def __init__(self, settings: Settings):
        self.s = settings

    # -- Postgres helpers ---------------------------------------------------

    def _pg_dsn(self) -> str:
        return (
            f"host={self.s.pg_host} port={self.s.pg_port} "
            f"user={self.s.pg_user} password={self.s.pg_password} dbname={self.s.pg_db}"
        )

    def ensure_registry(self) -> None:
        """Create the control-plane bookkeeping table if absent."""
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ztap_control.projects (
                    name        text PRIMARY KEY,
                    status      text NOT NULL,
                    schema_name text,
                    catalog     text,
                    connector   text,
                    created_at  timestamptz DEFAULT now()
                )
                """
            )

    def _ensure_control_schema(self) -> None:
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS ztap_control")

    # -- individual provisioning steps -------------------------------------

    def _create_schema(self, schema: str) -> None:
        try:
            with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
                conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        except Exception as e:  # noqa: BLE001
            raise ProvisionError("postgres-schema", str(e)) from e

    def _drop_schema(self, schema: str) -> None:
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    def _create_catalog(self, catalog: str) -> None:
        if self.s.offline:
            return
        url = f"{self.s.uc_url}/api/2.1/unity-catalog/catalogs"
        try:
            with httpx.Client(timeout=15) as c:
                r = c.post(url, json={"name": catalog, "comment": "ztap project catalog"})
                if r.status_code == 409:
                    return  # already exists -> idempotent
                if r.status_code >= 400:
                    raise ProvisionError("unity-catalog", f"{r.status_code}: {r.text}")
        except httpx.HTTPError as e:
            raise ProvisionError("unity-catalog", str(e)) from e

    def _drop_catalog(self, catalog: str) -> None:
        if self.s.offline:
            return
        url = f"{self.s.uc_url}/api/2.1/unity-catalog/catalogs/{catalog}?force=true"
        with httpx.Client(timeout=15) as c:
            c.delete(url)

    def _create_schema_uc(self, catalog: str, schema: str = "cdc") -> None:
        """Create a Unity Catalog schema under the project catalog.

        Without this the catalog is an empty shell. The 'cdc' schema is where
        tables captured from Postgres get registered (see register_table).
        """
        if self.s.offline:
            return
        url = f"{self.s.uc_url}/api/2.1/unity-catalog/schemas"
        try:
            with httpx.Client(timeout=15) as c:
                r = c.post(url, json={"name": schema, "catalog_name": catalog})
                if r.status_code == 409:
                    return
                if r.status_code >= 400:
                    raise ProvisionError("unity-catalog-schema", f"{r.status_code}: {r.text}")
        except httpx.HTTPError as e:
            raise ProvisionError("unity-catalog-schema", str(e)) from e

    def _create_publication(self, name: str, schema: str) -> None:
        """Create a schema-scoped publication that auto-includes future tables.

        Postgres 15+ supports ``FOR TABLES IN SCHEMA``, which captures every
        table in the project schema, including ones created after the connector
        starts. We manage the publication ourselves and tell Debezium not to
        (``publication.autocreate.mode=disabled``) — Debezium's own
        ``filtered`` mode only enumerates tables that exist at creation time,
        which silently misses later tables.
        """
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s", (f"ztap_pub_{name}",)
            ).fetchone()
            if not exists:
                conn.execute(f'CREATE PUBLICATION ztap_pub_{name} FOR TABLES IN SCHEMA "{schema}"')

    def _create_connector(self, name: str, connector: str, schema: str, topic_prefix: str) -> None:
        if self.s.offline:
            return
        try:
            self._create_publication(name, schema)
        except Exception as e:  # noqa: BLE001
            raise ProvisionError("postgres-publication", str(e)) from e
        config = {
            "name": connector,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": self.s.pg_host,
                "database.port": str(self.s.pg_port),
                "database.user": self.s.pg_user,
                "database.password": self.s.pg_password,
                "database.dbname": self.s.pg_db,
                "topic.prefix": topic_prefix,
                "schema.include.list": schema,
                "plugin.name": "pgoutput",
                "slot.name": f"ztap_{name}",
                "publication.name": f"ztap_pub_{name}",
                "publication.autocreate.mode": "disabled",
                "snapshot.mode": "initial",
                # Emit numeric/decimal as exact decimal strings ("19.99") rather
                # than the default base64-encoded two's-complement bytes, so the
                # sink can land them in a Delta column without decoding.
                "decimal.handling.mode": "string",
                # Emit plain JSON rows (no Connect schema envelope) and flatten
                # the Debezium change event to just the new row state, so the
                # sink can consume rows directly. Deletes are rewritten to a row
                # carrying __deleted=true; __op / __ts_ms expose CDC metadata.
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.unwrap.add.fields": "op,ts_ms",
            },
        }
        url = f"{self.s.connect_url}/connectors"
        try:
            with httpx.Client(timeout=20) as c:
                r = c.post(url, json=config)
                if r.status_code == 409:
                    return  # connector already exists
                if r.status_code >= 400:
                    raise ProvisionError("debezium", f"{r.status_code}: {r.text}")
        except httpx.HTTPError as e:
            raise ProvisionError("debezium", str(e)) from e

    def _drop_connector(self, connector: str) -> None:
        if self.s.offline:
            return
        with httpx.Client(timeout=20) as c:
            c.delete(f"{self.s.connect_url}/connectors/{connector}")

    def _drop_replication_objects(self, name: str) -> None:
        """Drop the publication and replication slot Debezium leaves behind.

        An orphaned replication slot pins WAL forever and will eventually fill
        the disk, so teardown must remove it explicitly. A slot that is still
        *active* (Debezium's walsender hasn't disconnected yet) cannot be
        dropped, so we terminate its backend first and retry briefly.
        """
        import time

        slot_name = f"ztap_{name}"
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            conn.execute(f"DROP PUBLICATION IF EXISTS ztap_pub_{name}")
            for attempt in range(10):
                row = conn.execute(
                    "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                ).fetchone()
                if row is None:
                    return  # gone
                active, active_pid = row
                if active and active_pid is not None:
                    # Kick the lingering walsender so the slot becomes droppable.
                    conn.execute("SELECT pg_terminate_backend(%s)", (active_pid,))
                    time.sleep(0.5)
                    continue
                conn.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
                return

    # -- public orchestration ----------------------------------------------

    def create(self, name: str) -> ProjectState:
        validate_name(name)
        state = ProjectState(name=name)
        schema = f"proj_{name}"
        catalog = f"ztap_{name}"
        connector = f"ztap-{name}-cdc"
        topic_prefix = f"ztap.{name}"

        self._ensure_control_schema()
        self.ensure_registry()

        self._create_schema(schema)
        state.schema = schema
        state.steps_completed.append("postgres-schema")

        self._create_catalog(catalog)
        self._create_schema_uc(catalog, "cdc")
        state.catalog = catalog
        state.steps_completed.append("unity-catalog")

        self._create_connector(name, connector, schema, topic_prefix)
        state.connector = connector
        state.cdc_topic_prefix = topic_prefix
        state.steps_completed.append("debezium")

        state.status = "ready"
        self._persist(state)
        return state

    def _persist(self, state: ProjectState) -> None:
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            conn.execute(
                """
                INSERT INTO ztap_control.projects (name, status, schema_name, catalog, connector)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    status = EXCLUDED.status,
                    schema_name = EXCLUDED.schema_name,
                    catalog = EXCLUDED.catalog,
                    connector = EXCLUDED.connector
                """,
                (state.name, state.status, state.schema, state.catalog, state.connector),
            )

    def list(self) -> list[dict]:
        self._ensure_control_schema()
        self.ensure_registry()
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            rows = conn.execute(
                "SELECT name, status, schema_name, catalog, connector, created_at "
                "FROM ztap_control.projects ORDER BY created_at"
            ).fetchall()
        return [
            {
                "name": r[0], "status": r[1], "schema": r[2],
                "catalog": r[3], "connector": r[4], "created_at": str(r[5]),
            }
            for r in rows
        ]

    def get(self, name: str) -> Optional[dict]:
        for p in self.list():
            if p["name"] == name:
                return p
        return None

    def _introspect_table(self, schema: str, table: str) -> list[tuple[str, str, bool]]:
        """Return [(column, pg_type, nullable)] for a live Postgres table.

        Uses format_type() so the type strings (e.g. 'numeric(10,2)',
        'integer[]', 'timestamp with time zone') are exactly what the
        type-engine parses.
        """
        with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
            rows = conn.execute(
                """
                SELECT a.attname,
                       format_type(a.atttypid, a.atttypmod) AS type,
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
        return [(r[0], r[1], r[2]) for r in rows]

    def register_table(self, name: str, table: str) -> dict:
        """Register a Postgres table from the project schema into Unity Catalog.

        Every column is mapped through the type-engine; the response reports
        which columns were converted lossily (jsonb/uuid/interval/array/...),
        so the lossiness is visible at registration time rather than discovered
        as corruption later.
        """
        from .uc_types import uc_column

        validate_name(name)
        pg_schema = f"proj_{name}"
        catalog = f"ztap_{name}"

        cols_meta = self._introspect_table(pg_schema, table)
        if not cols_meta:
            raise ProvisionError("introspect", f"table {pg_schema}.{table} not found or has no columns")

        uc_columns = []
        lossy_columns = []
        for pos, (col, pg_type, nullable) in enumerate(cols_meta):
            try:
                col_dict, lt = uc_column(col, pg_type, pos, nullable)
            except Exception as e:  # noqa: BLE001
                raise ProvisionError("type-mapping", f"column {col} ({pg_type}): {e}") from e
            uc_columns.append(col_dict)
            if lt.lossy:
                lossy_columns.append({"column": col, "pg_type": pg_type,
                                      "delta_type": lt.delta_type, "note": lt.note})

        storage_location = f"s3://warehouse/{name}/{table}"
        if not self.s.offline:
            url = f"{self.s.uc_url}/api/2.1/unity-catalog/tables"
            payload = {
                "name": table,
                "catalog_name": catalog,
                "schema_name": "cdc",
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": storage_location,
                "columns": uc_columns,
            }
            try:
                with httpx.Client(timeout=20) as c:
                    r = c.post(url, json=payload)
                    if r.status_code >= 400 and r.status_code != 409:
                        raise ProvisionError("unity-catalog-table", f"{r.status_code}: {r.text}")
            except httpx.HTTPError as e:
                raise ProvisionError("unity-catalog-table", str(e)) from e

        return {
            "registered": f"{catalog}.cdc.{table}",
            "storage_location": storage_location,
            "columns": [{"name": c["name"], "delta_type": c["type_text"]} for c in uc_columns],
            "lossy_columns": lossy_columns,
        }

    def delete(self, name: str) -> dict:
        """Teardown in reverse order, best-effort."""
        validate_name(name)
        errors: list[str] = []
        connector = f"ztap-{name}-cdc"
        catalog = f"ztap_{name}"
        schema = f"proj_{name}"

        for step, fn in (
            ("debezium", lambda: self._drop_connector(connector)),
            ("replication", lambda: self._drop_replication_objects(name)),
            ("unity-catalog", lambda: self._drop_catalog(catalog)),
            ("postgres-schema", lambda: self._drop_schema(schema)),
        ):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                errors.append(f"{step}: {e}")

        try:
            with psycopg.connect(self._pg_dsn(), autocommit=True) as conn:
                conn.execute("DELETE FROM ztap_control.projects WHERE name = %s", (name,))
                # Clear the reverse-sync idempotency ledger for this project so a
                # recreated project starts clean. The table is created lazily by
                # the sync service, so guard for its existence first.
                ledger = conn.execute(
                    "SELECT to_regclass('ztap_control.sync_applied')"
                ).fetchone()[0]
                if ledger:
                    conn.execute(
                        "DELETE FROM ztap_control.sync_applied WHERE project = %s", (name,)
                    )
        except Exception as e:  # noqa: BLE001
            errors.append(f"registry: {e}")

        return {"name": name, "deleted": True, "errors": errors}
