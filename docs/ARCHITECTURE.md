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
| 1 | Suspend/resume-aware connection proxy | **design only** | — |
| 2 | Bidirectional sync state machine (schema evolution) | **next** | — |
| 3 | Control-plane API (the "project" abstraction) | **built + tested** | `services/control-plane` |
| 4 | Type mapping + conflict-resolution engine | **built + tested** | `packages/type-engine` |
| + | Delta sink (CDC → Delta tables in MinIO) | **built + tested** | `services/sink` |

Components #3 and #4 were the ones buildable and verifiable end-to-end in a
single pass, so they came first. #4 is the silent-corruption risk; #3 is the
developer-experience surface. The **Delta sink** (Phase 2) closes the data loop
so Unity Catalog's registered tables actually have Delta files behind them. #1
(systems-programming heavy, coupled to a Neon-style cplane/auth) and #2 (the
bidirectional sync state machine, the hardest) remain ahead.

## What this is NOT

- Not Neon's autoscaling / scale-to-zero control plane (NeonVM/QEMU microVMs on
  K8s) — that is its own multi-project effort and out of scope.
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
| Kafka Connect / Debezium | 18083 | 8083 |
| Control plane API | 18000 | 8000 |
