# ztap-oss

An **educational, open-source reconstruction of the architecture Databricks
describes for Lakebase** — a transactional Postgres front end whose every change
is continuously captured into a governed Delta lakehouse, with a unified control
plane that provisions and lifecycles the whole thing as one "project."

**Not affiliated with or endorsed by Databricks.** The architecture (storage/
compute split, branching, WAL→lakehouse CDC, a unified control plane) is an idea,
and ideas aren't copyrightable — this repo reconstructs that idea from open-source
parts in original code. It contains no Databricks source or documentation text.
Each bundled component remains under its own license (see
[docs/LICENSES.md](docs/LICENSES.md)). **Not intended for commercial or
as-a-service deployment.**

---

## What it does

```
   ┌─────────────────────────── control plane (FastAPI) ───────────────────────────┐
   │   POST /projects  →  Postgres schema + Unity Catalog catalog + Debezium connector │
   └───────────────────────────────────────────────────────────────────────────────┘

   write ──▶ ┌──────────┐  WAL   ┌──────────┐  topic  ┌─────────┐  consume  ┌────────┐
   (OLTP)    │ Postgres │ ─────▶ │ Debezium │ ──────▶ │  Kafka  │ ────────▶ │  sink  │
             └────┬─────┘        └──────────┘         └─────────┘           └───┬────┘
                  │ proxy fronts :15432 (suspend/resume)                        │ writes Delta
                  │                                                             ▼
             ┌────▼──────────┐                                          ┌──────────────┐
   query ◀── │ proxy /15432  │                                          │ MinIO (S3)   │
             └───────────────┘            Trino reads ◀──────────────── │ Delta tables │
                                          (delta_lake)                   └──────┬───────┘
             ┌───────────────┐                                                 │ metadata
   sync ◀──▶ │ sync service  │  reverse-sync (lakehouse → Postgres)     ┌──────▼───────┐
             │ schema reconcile│  + conflict resolution + idempotency    │ Unity Catalog│
             └───────────────┘                                          └──────────────┘
```

A row written to Postgres is captured by Debezium, streamed through Kafka, and
appended by the sink as a Delta Lake table in MinIO — queryable in Trino under
**the same `schema.table` name as Postgres**. Schema changes and lakehouse→Postgres
writes are reconciled by the sync service.

---

## The stack

**Assembled open-source systems** (pulled as stock images, each under its own license):

| Component | Role |
|-----------|------|
| PostgreSQL 16 | transactional "compute" (logical replication on) |
| Debezium + Kafka (KRaft) | change data capture, WAL → topics |
| MinIO | S3-compatible object store (the lakehouse) |
| Unity Catalog (OSS) | catalog / governance metadata |
| Trino (`delta_lake`) | analytical SQL over the Delta tables |

**Original "connective tissue" components** (Apache-2.0, written here):

| # | Component | Path | What it does |
|---|-----------|------|--------------|
| 3 | Control plane | `services/control-plane` | FastAPI "project" abstraction + a web dashboard; one call provisions PG schema + UC catalog + Debezium connector and tears them down in reverse |
| 4 | Type engine | `packages/type-engine` | Postgres↔Delta type mapping + conflict resolution; pure Python, honest about every lossy conversion (`jsonb`, `uuid`, `interval`, arrays, oversized `numeric`, …) |
| — | Delta sink | `services/sink` | consumes the CDC stream and writes real Delta Lake tables to MinIO via delta-rs (no Spark); captures insert/update/delete |
| 2 | Sync service | `services/sync` | reconciles Postgres↔Unity-Catalog schema drift, and applies lakehouse→Postgres changes through the type-engine's conflict policy with an idempotency ledger (loop-prevention) |
| 1 | Connection proxy | `services/proxy` | Go Postgres proxy that holds a connection open during a (simulated) compute cold-start, wakes it, and transparently proxies the session |
| — | Trino auto-register | `services/trino-init` | scans MinIO and keeps Trino's view of the Delta tables in sync both ways (registers new tables, unregisters deleted ones) |

The four numbered components are the gaps the OSS tools don't ship — the same
decomposition the project set out to rebuild. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detail.

---

## Setup

**Prerequisites:** Docker (Desktop or Engine) with ~10 GB RAM available.

```bash
git clone https://github.com/Darshan-Dhanke/ztap-oss.git
cd ztap-oss
cp .env.example .env

docker compose up -d --build        # or: make up
docker compose ps                   # wait until services are healthy (~1–2 min)
```

Then open the console: **http://localhost:18000/**

| Surface | URL |
|---------|-----|
| ztap console (dashboard) | http://localhost:18000/ |
| Control-plane API docs | http://localhost:18000/docs |
| Sync service API docs | http://localhost:18001/docs |
| MinIO console (Delta files) | http://localhost:19001 — `minioadmin` / `minioadmin` |
| Trino (analytical SQL) | http://localhost:18090 |
| Postgres (direct) | `localhost:55432` — db/user/pass `ztap` |
| Postgres (through suspend/resume proxy) | `localhost:15432` |

Ports are deliberately offset so the stack won't collide with a standard local
Postgres/Kafka/MinIO setup.

> On Windows `cmd.exe`, use double quotes and escape inner quotes in curl bodies:
> `curl -X POST localhost:18000/projects -H "content-type: application/json" -d "{\"name\":\"sales\"}"`
> — or just use the **Create project** button on the dashboard.

---

## Walkthrough

### 1. Create a project

```bash
curl -X POST localhost:18000/projects \
  -H 'content-type: application/json' -d '{"name":"sales"}'
```

You get back a Postgres schema `proj_sales`, a Unity Catalog catalog `ztap_sales`
(with a `cdc` schema), and a running Debezium connector streaming `proj_sales`'s
WAL to Kafka topics under `ztap.sales.*`.

### 2. Create a table and register it in the catalog

```bash
docker exec ztap-postgres psql -U ztap -d ztap -c \
  "CREATE TABLE proj_sales.orders(id bigint primary key, customer text, amount numeric(10,2), doc jsonb);"

curl -X POST localhost:18000/projects/sales/tables \
  -H 'content-type: application/json' -d '{"table":"orders"}'
```

The control plane introspects the live table, maps every column through the
type-engine, registers it in Unity Catalog, and returns a `lossy_columns` list
flagging any column whose conversion loses information (here, `doc`/`jsonb` →
`STRING`).

### 3. Write in Postgres, read in the lakehouse (automatic CDC)

```bash
docker exec ztap-postgres psql -U ztap -d ztap -c \
  "INSERT INTO proj_sales.orders VALUES (1,'alice',19.99,'{\"sku\":\"A1\"}'),(2,'bob',5.50,'{\"sku\":\"B2\"}');"
```

A few seconds later, the same data is queryable in Trino under the **identical
name** (`delta.proj_sales.orders`):

```bash
docker exec ztap-trino trino --catalog delta --schema proj_sales \
  --execute "SELECT id, customer, amount, _op FROM orders ORDER BY _ts_ms"
```

The Delta table is an append-only **change feed** (`_op` = `c`/`u`/`d`,
`_ts_ms`, `_deleted`). To see current state, collapse it:

```sql
SELECT id, customer, CAST(amount AS decimal(10,2)) AS amount
FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY _ts_ms DESC) rn
      FROM delta.proj_sales.orders) t
WHERE rn = 1 AND NOT _deleted ORDER BY id;
```

### 4. Push a change from the lakehouse side, back to Postgres

```bash
curl -X POST localhost:18001/projects/sales/tables/orders/reverse-sync \
  -H 'content-type: application/json' \
  -d '{"rows":[{"id":3,"customer":"carol","amount":42.00}],"pk_col":"id","policy":"last_write_wins"}'
```

`carol` now exists in `proj_sales.orders` in Postgres — and, because that write
hits Postgres, it flows forward through CDC back into the lakehouse too.

### 5. Inspect the Delta transaction log

```bash
docker exec ztap-trino trino --catalog delta --schema proj_sales \
  --execute 'SELECT version, operation, timestamp FROM "orders$history"'
```

### 6. See the proxy cold-start

The proxy idles to `suspended` after ~30s (scale-to-zero simulation). Wake it
with a connection through `:15432` (the dashboard's **Wake compute** button does
this), and watch `wake_count` increment at http://localhost:18002/state.

### Naming

A table is identified the **same way on both sides** — only the catalog differs,
which is what marks the source:

| Postgres | `proj_<project>.<table>` |
|----------|--------------------------|
| Trino    | `delta.proj_<project>.<table>` |

---

## Testing

```bash
make test-unit     # 103 Python + 8 Go unit tests (no Docker needed for Python)
make test          # unit + bring up stack + all integration suites
```

Individual integration suites (run against a running stack):

| Suite | What it proves |
|-------|----------------|
| `make smoke`      | provision → insert → CDC event lands on Kafka → teardown |
| `make edge`       | nasty type mappings into UC, idempotency, bad input, clean teardown |
| `make sink-test`  | insert/update/delete → readable back from the Delta table in MinIO |
| `make sync-test`  | schema evolution reconcile + reverse-sync with conflict resolution |
| `make proxy-test` | cold-start wake through the proxy returns correctly |
| `make query-delta`| register + query a Delta table via Trino |

All integration tests self-clean (they remove their own Delta data), and
`trino-init` reconciles Trino's view so no stale tables accumulate.

---

## Limitations (read these)

This is a faithful reconstruction of the *architecture*, not a production system.
Deliberate, honest gaps:

- **The proxy simulates compute suspend/resume.** There is no separated
  scale-to-zero compute to actually stop; the proxy implements the connection
  hold + cold-start state machine and transparent proxying, but the "suspend" is
  a state flag plus a wake delay. (Neon's real autoscaling control plane —
  NeonVM/QEMU microVMs on K8s — is out of scope.)
- **Reverse sync (lakehouse → Postgres) is API-triggered, not continuous.** A
  raw `INSERT` in Trino/DBeaver does not propagate to Postgres; you call the sync
  service. A continuous "watch Delta and auto-apply" worker is the one piece left
  as triggered-only — it's the hardest part of bidirectional sync.
- **Unity Catalog OSS is the metadata + grants foundation only** — no managed
  lineage, no Catalog Explorer UI, no Delta Sharing (those are commercial).
- **Trino's metastore is the file metastore (ephemeral).** Registrations don't
  survive a Trino restart on their own; the `trino-init` sidecar re-creates them
  within ~30s.
- **Single-node, no HA, no auth/RBAC/TLS.** For learning and local use only.

---

## Repository layout

```
docker-compose.yml          the full stack (offset ports, own network)
services/control-plane/     #3 control plane (FastAPI) + dashboard + tests
services/type-engine via packages/type-engine/   #4 type mapping + conflict engine + tests
services/sink/              Delta sink (Kafka CDC → Delta on MinIO) + tests
services/sync/              #2 schema reconcile + reverse sync + tests
services/proxy/             #1 suspend/resume Postgres proxy (Go) + tests
services/trino/             Trino delta_lake catalog config
services/trino-init/        auto-register/reconcile Delta tables in Trino
scripts/                    integration tests + helpers (query_delta, mirror_images)
docs/                       ARCHITECTURE.md, LICENSES.md
```

## License

Original code is Apache-2.0 (see `LICENSE`). Bundled images are each under their
own upstream license — see [docs/LICENSES.md](docs/LICENSES.md).
