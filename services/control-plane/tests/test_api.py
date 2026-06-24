"""API tests that stub the Provisioner so no live infra/DB is required."""

import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.provisioner import ProjectState, ProvisionError


class FakeProvisioner:
    def __init__(self):
        self._store = {}

    def create(self, name):
        if name == "boom":
            raise ProvisionError("postgres-schema", "simulated failure")
        st = ProjectState(
            name=name, status="ready", schema=f"proj_{name}",
            catalog=f"ztap_{name}", connector=f"ztap-{name}-cdc",
            cdc_topic_prefix=f"ztap.{name}",
            steps_completed=["postgres-schema", "unity-catalog", "debezium"],
        )
        self._store[name] = st
        return st

    def list(self):
        return [
            {"name": n, "status": s.status, "schema": s.schema,
             "catalog": s.catalog, "connector": s.connector, "created_at": "now"}
            for n, s in self._store.items()
        ]

    def get(self, name):
        s = self._store.get(name)
        if not s:
            return None
        return {"name": name, "status": s.status, "schema": s.schema,
                "catalog": s.catalog, "connector": s.connector, "created_at": "now"}

    def delete(self, name):
        self._store.pop(name, None)
        return {"name": name, "deleted": True, "errors": []}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main, "prov", FakeProvisioner())
    return TestClient(main.app)


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_create_and_get_project(client):
    r = client.post("/projects", json={"name": "analytics"})
    assert r.status_code == 201
    body = r.json()
    assert body["schema"] == "proj_analytics"
    assert body["catalog"] == "ztap_analytics"
    assert body["connector"] == "ztap-analytics-cdc"
    assert body["status"] == "ready"

    r2 = client.get("/projects/analytics")
    assert r2.status_code == 200
    assert r2.json()["name"] == "analytics"


def test_list_projects(client):
    client.post("/projects", json={"name": "one"})
    client.post("/projects", json={"name": "two"})
    r = client.get("/projects")
    names = {p["name"] for p in r.json()["projects"]}
    assert {"one", "two"} <= names


def test_get_missing_project_404(client):
    r = client.get("/projects/nope")
    assert r.status_code == 404


def test_provision_error_returns_400(client):
    r = client.post("/projects", json={"name": "boom"})
    assert r.status_code == 400
    assert "simulated failure" in r.json()["detail"]


def test_delete_project(client):
    client.post("/projects", json={"name": "gone"})
    r = client.delete("/projects/gone")
    assert r.status_code == 200
    assert r.json()["deleted"] is True
