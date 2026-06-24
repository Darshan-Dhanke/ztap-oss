"""ztap-oss sync service (custom component #2).

The stateful coordinator for keeping Postgres and the lakehouse consistent:
  * schema-evolution reconciliation (PG <-> Unity Catalog)
  * reverse sync (lakehouse -> Postgres) governed by the conflict policy and an
    idempotency ledger that prevents reverse writes from echoing back in a loop.
"""

from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .reconciler import reconcile_schema, reverse_sync, ReconcileError

app = FastAPI(
    title="ztap-oss sync service",
    version="0.1.0",
    description="Bidirectional sync state machine. Educational reconstruction; "
                "not affiliated with or endorsed by Databricks.",
)


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "ztap-sync"}


@app.post("/projects/{name}/tables/{table}/reconcile-schema")
def reconcile(name: str, table: str):
    """Detect PG<->UC schema drift for a table and reconcile UC to match PG."""
    try:
        return reconcile_schema(name, table)
    except ReconcileError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class ReverseSyncRequest(BaseModel):
    rows: list[dict] = Field(..., description="lakehouse-originated rows to apply")
    pk_col: str = Field(..., description="primary key column name")
    version_col: Optional[str] = Field(
        None, description="column holding a monotonic version for last_write_wins"
    )
    policy: str = Field("last_write_wins",
                        description="last_write_wins | source_of_truth | error")
    source_of_truth: str = Field("postgres", description="postgres | lakehouse")


@app.post("/projects/{name}/tables/{table}/reverse-sync")
def reverse(name: str, table: str, req: ReverseSyncRequest):
    """Apply lakehouse rows into Postgres with conflict resolution + idempotency."""
    try:
        return reverse_sync(
            name, table, req.rows,
            pk_col=req.pk_col, version_col=req.version_col,
            policy=req.policy, source_of_truth=req.source_of_truth,
        )
    except ReconcileError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
