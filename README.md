# ztap-oss

An **educational, open-source reconstruction** of the architecture Databricks
describes for Lakebase. **Not affiliated with or endorsed by Databricks.** This
project wires together existing open-source systems (PostgreSQL, Unity Catalog,
Debezium, Kafka, MinIO) and adds the connective components they don't ship.
Each bundled tool remains under its own license — see
[docs/LICENSES.md](docs/LICENSES.md) and consult the upstream repos before use.
**Not intended for commercial or as-a-service deployment.**

> The architecture (storage/compute split, branching, WAL→lakehouse CDC, a
> unified control plane) is an idea, and ideas aren't copyrightable. This repo
> reconstructs that idea from OSS parts in original code — it contains no
> Databricks source or documentation text.

## What's built (Phase 1)

This is the realistic first slice: the OSS data plane wired together, plus the
two custom "connective tissue" components that are fully testable.

- **Data plane** (`docker-compose.yml`): Postgres (logical replication) →
  Debezium → Kafka (KRaft); Unity Catalog for governance; MinIO as the
  lakehouse object store.
- **Custom #3 — control-plane API** (`services/control-plane`): the unified
  "project" abstraction. One `POST /projects` provisions a PG schema + Unity
  Catalog catalog + Debezium CDC connector, wired together; `DELETE` tears them
  down in reverse.
- **Custom #4 — type-engine** (`packages/type-engine`): Postgres↔Delta type
  mapping + conflict resolution, the piece most likely to cause silent
  corruption. Pure Python, honest about every lossy conversion.

Deliberately **not** in Phase 1 (documented as design stubs in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)): the suspend/resume-aware
connection proxy (#1) and the bidirectional sync state machine (#2).

## Quick start

```bash
cp .env.example .env

# unit tests for the custom components (no Docker needed)
make test-unit

# build + boot the full data plane
make up
make ps

# end-to-end smoke test: provision a project, prove CDC flows, tear down
make smoke
```

Then open:
- Control plane API docs — http://localhost:18000/docs
- Unity Catalog — http://localhost:18080
- MinIO console — http://localhost:19001 (minioadmin / minioadmin)
- Kafka Connect — http://localhost:18083/connectors

## Create a project by hand

```bash
curl -X POST localhost:18000/projects -H 'content-type: application/json' \
  -d '{"name":"analytics"}'

curl localhost:18000/projects
```

You get back a Postgres schema `proj_analytics`, a Unity Catalog catalog
`ztap_analytics` (with a `cdc` schema), and a running Debezium connector
streaming that schema's WAL to Kafka topics under `ztap.analytics.*`.

### Register a table into Unity Catalog (UC + type-engine wired together)

```bash
# create a table in the project schema, then register it
docker exec ztap-postgres psql -U ztap -d ztap -c \
  "CREATE TABLE proj_analytics.events (id bigint primary key, doc jsonb, dur interval, tags int[]);"

curl -X POST localhost:18000/projects/analytics/tables \
  -H 'content-type: application/json' -d '{"table":"events"}'
```

The control plane introspects the live Postgres table, maps every column
through the **type-engine**, creates the table in Unity Catalog
(`ztap_analytics.cdc.events`), and returns a `lossy_columns` list naming each
column whose conversion loses information (here `doc`/`dur`/`tags`) with the
reason. That's the silent-corruption surface made visible at registration time.

## Layout

```
docker-compose.yml          OSS data plane (offset ports, own network)
services/control-plane/     custom #3 — FastAPI control plane + tests
packages/type-engine/       custom #4 — type mapping + conflict engine + tests
scripts/smoke_test.sh       end-to-end integration test
docs/                       ARCHITECTURE.md, LICENSES.md
```

## License

Original code is Apache-2.0 (see `LICENSE`). Bundled images are each under their
own upstream license — see [docs/LICENSES.md](docs/LICENSES.md).
