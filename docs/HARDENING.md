# Hardening (auth / TLS / RBAC)

ztap-oss ships **open by default** — every service uses default credentials over
plain HTTP — because it's a local learning stack and that keeps "clone and run"
friction-free. None of it is suitable for a shared or production deployment as-is.
This document is the honest checklist for closing that gap. Most of it is
configuration, not new code.

> Rule of thumb: nothing here is hard individually; the cost is that there are
> many services and each has its own auth/TLS surface.

## 1. Secrets — stop using the defaults (do this first, it's free)

Everything reads from `.env`. Replace every default before exposing anything:

```dotenv
POSTGRES_PASSWORD=<long-random>
MINIO_ROOT_USER=<non-default>
MINIO_ROOT_PASSWORD=<long-random>
GRAFANA_USER=<non-default>
GRAFANA_PASSWORD=<long-random>
```

Then update the compute Postgres (`services/compute`) and any hardcoded
`minioadmin`/`compute` strings in the catalog/properties files to match. Prefer
Docker secrets or an external secrets manager over `.env` for anything real.

## 2. Don't publish internal ports

In `docker-compose.yml`, only the proxy (`15432`) and the dashboards really need
host ports. Remove the `ports:` mapping from postgres/kafka/minio/connect/uc/
trino/prometheus for anything that shouldn't be reachable from the host, and let
services talk over the internal `ztap` network only.

## 3. Per-service auth + TLS

| Service | Auth | TLS |
|---------|------|-----|
| Postgres | already password auth; switch `pg_hba` to `scram-sha-256`, strong password | `ssl=on` with a server cert; clients use `sslmode=verify-full` |
| MinIO | real access keys; create scoped users/policies instead of root | drop certs in `/root/.minio/certs`; use `https://` endpoints everywhere (update Trino/sink/watcher `S3_ENDPOINT`) |
| Kafka | enable `SASL_SSL` (SCRAM); set `sasl.*` on every client (connect, sink, watcher, kafka-exporter) | broker keystore + client truststore |
| Kafka Connect | enable the REST extension for basic-auth; restrict the connector creds | front with TLS / mTLS |
| Unity Catalog | OSS auth is limited — front it with a reverse proxy (OAuth2/JWT) for real access control | terminate TLS at the proxy |
| Trino | `http-server.authentication.type=PASSWORD` (file or LDAP/OAuth2); `access-control` rules | `http-server.https.enabled=true` + keystore |
| Grafana | already admin-auth — **set a strong password and disable anonymous** (`GF_AUTH_ANONYMOUS_ENABLED=false`) | terminate TLS or run behind a TLS reverse proxy |
| Control plane / sync | add an API key / OAuth2 middleware (FastAPI dependency) | terminate TLS at an ingress |

## 4. Service-to-service auth

The custom services currently trust the network. For multi-tenant use:
- give each its own Kafka SASL principal and Postgres role with least privilege,
- require a shared bearer token (or mTLS) between control-plane ↔ sync ↔ watcher,
- scope MinIO credentials per project rather than using root.

## 5. RBAC / data-level security

- Unity Catalog OSS has the grant model but engines don't fully *enforce* it —
  enforce access at the query engine (Trino access-control rules) and at MinIO
  (per-bucket/prefix policies) until UC enforcement matures.
- Postgres row-level security (`CREATE POLICY`) for tenant isolation on the OLTP
  side.

## Quick win for a shared demo

If you just need a not-wide-open demo (not production): do **1** (real secrets) +
**2** (unpublish internal ports) + Grafana anonymous off + Trino password auth.
That removes the "anyone on the network owns everything" problem without the full
TLS/SASL build-out.
