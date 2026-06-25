# Bundled component licenses

ztap-oss does not redistribute any of the systems below — it pulls their stock
container images at runtime via Docker Compose. **Each remains under its own
license.** Consult the upstream project before you use it. This file is a
convenience pointer, not legal advice.

| Component | Image | License | Notes |
|-----------|-------|---------|-------|
| PostgreSQL | `postgres:16` | PostgreSQL License (permissive) | the "compute" |
| MinIO | `minio/minio` | AGPL-3.0 | run as a stock service; do not fork/embed |
| Unity Catalog | `unitycatalog/unitycatalog` | Apache-2.0 | catalog/governance foundation only |
| Apache Kafka | `apache/kafka` | Apache-2.0 | event backbone (KRaft mode) |
| Debezium | `debezium/connect` | Apache-2.0 | WAL CDC |
| Trino | `trinodb/trino` | Apache-2.0 | analytical SQL over the Delta tables |
| Prometheus | `prom/prometheus` | Apache-2.0 | metrics scraping |
| kafka-exporter | `danielqsj/kafka-exporter` | MIT | Kafka consumer-lag metrics |
| Grafana | `grafana/grafana` | AGPL-3.0 | metrics dashboards (stock image) |

ztap's own **sink service** is original Apache-2.0 code; the Python libraries it
depends on are permissive: `deltalake`/delta-rs (Apache-2.0), `pyarrow`
(Apache-2.0), `confluent-kafka` Python client (Apache-2.0).

The **connection proxy** (`services/proxy`) is original Apache-2.0 Go code with
**no third-party dependencies** (Go standard library only).

## Things to actually watch

- **MinIO is AGPL-3.0.** Running the stock image as a service you publish a
  compose file for is fine ("mere aggregation"). The copyleft obligation
  triggers if you *modify and distribute* MinIO itself. (An earlier draft of
  this project considered S3/object stores generically; the AGPL note applies
  to MinIO specifically.)
- **Grafana is AGPL-3.0 and now wired in** as a stock image (observability).
  Pulling the unmodified image into this compose file is "mere aggregation" and
  fine; the copyleft obligation would only trigger if you *modify and
  distribute* Grafana itself. Don't offer it as-a-service.
- The restrictions in AGPL / ELv2 are about **offering the thing as a managed
  service to third parties**. This project is explicitly not that.

## ztap's own code

Everything original here — the type-engine, the control-plane API, the smoke
tests — is Apache-2.0 (see top-level `LICENSE`).
