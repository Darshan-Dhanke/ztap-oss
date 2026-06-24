# ztap-oss architecture

An educational reconstruction of the *architecture* a Lakebase-style system
describes: a transactional Postgres front end whose changes are continuously
captured into a governed lakehouse, with a unified control plane that
provisions and lifecycles the whole thing as one "project".

This is the **idea** rebuilt from open-source parts — not a copy of anyone's
code or docs.

## Data flow (Phase 1, what actually boots today)

```
                         ┌─────────────────────────────┐
   POST /projects        │     control plane (#3)      │
  ───────────────────▶   │   FastAPI orchestration     │
                         └──────┬───────┬───────┬───────┘
            creates schema      │       │       │  registers connector
         ┌──────────────────────┘       │       └──────────────────────┐
         ▼                              ▼                              ▼
   ┌───────────┐   WAL/pgoutput   ┌──────────┐   topic    ┌──────────────────┐
   │ Postgres  │ ───────────────▶ │ Debezium │ ─────────▶ │ Kafka (KRaft)    │
   │ (compute) │                  │ connector│            │ ztap.<proj>.*    │
   └───────────┘                  └──────────┘            └──────────────────┘
         │                                                         │
         │ catalog + cdc schema                          Delta sink│ (custom, Phase 2)
         ▼                                                         ▼
   ┌──────────────┐    storage_location points here ───▶   ┌──────────────┐
   │ Unity Catalog│ ◀ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │ MinIO (S3)   │
   │ (governance) │       (registered table metadata)      │ Delta tables │
   └──────────────┘                                        └──────────────┘
```

The loop is closed: a row written to Postgres is captured by Debezium, lands on
Kafka, and the **Delta sink** appends it as a Delta Lake table in MinIO at the
exact `storage_location` Unity Catalog registered for that table.

### Unity Catalog is populated, not decorative

`POST /projects/{name}/tables` introspects a live Postgres table, maps every
column through the **type-engine (#4)**, and registers the table in Unity
Catalog under `ztap_<name>.cdc.<table>` with correct Delta types. The response
lists `lossy_columns` — each column whose conversion loses information, with the
reason — so lossiness is visible at registration time. This is the seam where
component #4 meets the catalog.

What's still Phase 2: the *data* sink (writing actual Delta files into MinIO at
the registered `storage_location` from the Kafka CDC stream). Today the catalog
holds the table's schema/metadata; the Parquet/Delta files are not yet written.

## The four custom components

| # | Component | Status | Where |
|---|-----------|--------|-------|
| 1 | Suspend/resume-aware connection proxy | **built + tested** | `services/proxy` |
| 2 | Bidirectional sync state machine (schema evolution) | **built + tested** | `services/sync` |
| 3 | Control-plane API (the "project" abstraction) | **built + tested** | `services/control-plane` |
| 4 | Type mapping + conflict-resolution engine | **built + tested** | `packages/type-engine` |
| + | Delta sink (CDC → Delta tables in MinIO) | **built + tested** | `services/sink` |

All four custom components plus the Delta sink are now built and tested. #4 is
the silent-corruption risk, #3 the developer-experience surface; the **Delta
sink** closed the data loop so Unity Catalog's tables have Delta files behind
them; **#2** reconciles PG↔UC schema drift and applies lakehouse→Postgres
changes through the #4 conflict engine; **#1** is the connection proxy below.

### Component #1 — suspend/resume-aware connection proxy (`services/proxy`)

A small Go TCP proxy in front of Postgres. When the (simulated) compute is
suspended after an idle period, an incoming connection is **held open** while
the proxy "wakes" it, then transparently spliced through to the backend — the
client only experiences the cold-start latency.

What is real: Postgres wire-protocol handling for SSL/GSS/startup negotiation,
**buffering the startup packet** during the cold start, serializing concurrent
connections through a single wake, and byte-level transparent proxying of a real
`psql` session. What is simulated: suspending the *compute* itself (a state
machine + a configurable wake delay), because this stack has no separated
scale-to-zero compute to actually stop. A `/state` and `/suspend` HTTP API make
the behavior observable and testable.

### Component #2 — the sync state machine (`services/sync`)

Two responsibilities, both with a pure, unit-tested core and a thin I/O layer:

- **Schema evolution (PG → catalog).** `POST /…/reconcile-schema` introspects
  the live Postgres table, diffs it against the UC registration *through the
  type-engine*, and — if a column was added/removed/retyped — re-registers the
  UC EXTERNAL table to match (safe: the data lives in Delta/MinIO; the sink's
  `schema_mode=merge` already evolved the files).
- **Reverse sync (lakehouse → PG).** `POST /…/reverse-sync` applies
  lakehouse-originated rows into Postgres. Each row is run through
  `decide_reverse_apply`: an **idempotency ledger** (`ztap_control.sync_applied`)
  drops changes already applied — this is the loop-prevention that stops a
  reverse write from echoing back via CDC — and genuine conflicts are resolved
  by the **#4 conflict engine** (last-write-wins / source-of-truth / merge).

## What this is NOT

- Not Neon's autoscaling / scale-to-zero control plane (NeonVM/QEMU microVMs on
  K8s) — that is its own multi-project effort and out of scope. The proxy (#1)
  simulates compute suspend/resume; it does not actually stop a compute node.
- Not the governance *product* (managed lineage, Catalog Explorer UI, Delta
  Sharing) — Unity Catalog OSS gives the metadata model and grants only.
- Not for commercial or as-a-service deployment.

## Ports (offset to avoid colliding with a standard dev stack)

| Service | Host port | In-container |
|---------|-----------|--------------|
| Postgres | 55432 | 5432 |
| MinIO API / console | 19000 / 19001 | 9000 / 9001 |
| Unity Catalog | 18080 | 8080 |
| Kafka (external) | 19092 | 9092 |
| Delta sink | (no port; background consumer) | — |
| Sync service | 18001 | 8000 |
| Proxy (Postgres wire) | 15432 | 5432 |
| Proxy (state/control API) | 18002 | 8000 |
| Kafka Connect / Debezium | 18083 | 8083 |
| Control plane API | 18000 | 8000 |
