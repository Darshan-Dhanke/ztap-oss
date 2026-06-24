"""ztap-oss control plane (custom component #3).

A thin FastAPI orchestration layer that exposes the "project" abstraction:
one call provisions a Postgres schema + Unity Catalog catalog + Debezium CDC
connector, all wired together; deletion tears them down in reverse.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .provisioner import Provisioner, ProvisionError
from .settings import settings

app = FastAPI(
    title="ztap-oss control plane",
    version="0.1.0",
    description="Educational reconstruction of a Lakebase-style control plane. "
                "Not affiliated with or endorsed by Databricks.",
)

prov = Provisioner(settings)


class CreateProjectRequest(BaseModel):
    name: str = Field(..., examples=["analytics"])


@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "ztap-control-plane"}


@app.get("/config")
def config():
    return {
        "pg": f"{settings.pg_host}:{settings.pg_port}/{settings.pg_db}",
        "unity_catalog": settings.uc_url,
        "kafka_connect": settings.connect_url,
        "kafka_bootstrap": settings.kafka_bootstrap,
        "offline": settings.offline,
    }


@app.post("/projects", status_code=201)
def create_project(req: CreateProjectRequest):
    try:
        state = prov.create(req.name)
    except ProvisionError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return state.to_dict()


@app.get("/projects")
def list_projects():
    return {"projects": prov.list()}


@app.get("/projects/{name}")
def get_project(name: str):
    p = prov.get(name)
    if p is None:
        raise HTTPException(status_code=404, detail=f"project {name!r} not found")
    return p


class RegisterTableRequest(BaseModel):
    table: str = Field(..., examples=["events"])


@app.post("/projects/{name}/tables", status_code=201)
def register_table(name: str, req: RegisterTableRequest):
    """Introspect a Postgres table in the project schema, map its columns
    through the type-engine, and register it in Unity Catalog. The response
    lists any columns whose mapping is lossy."""
    if prov.get(name) is None:
        raise HTTPException(status_code=404, detail=f"project {name!r} not found")
    try:
        return prov.register_table(name, req.table)
    except ProvisionError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.delete("/projects/{name}")
def delete_project(name: str):
    try:
        return prov.delete(name)
    except ProvisionError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
