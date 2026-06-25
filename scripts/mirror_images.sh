#!/usr/bin/env bash
# Mirror the upstream images this stack uses (and the custom-built ztap images)
# into your own Docker Hub namespace, renamed with a ztap- prefix, so they are
# archived under your control and immune to upstream version churn / removal.
#
# Usage:   docker login                 # once, if not already authenticated
#          bash scripts/mirror_images.sh [namespace]
# Default namespace: darshandhanke07
#
# After mirroring, copy the printed image overrides into .env so the stack pulls
# your mirrored base images instead of the upstream ones.
set -uo pipefail

NS="${1:-darshandhanke07}"

# "<source image>=<dest repo name>" — tag becomes NS/<dest repo>:<source tag>
MAP=(
  "postgres:16=ztap-postgres"
  "minio/minio:latest=ztap-minio"
  "minio/mc:latest=ztap-minio-mc"
  "unitycatalog/unitycatalog:latest=ztap-unity-catalog"
  "apache/kafka:3.8.0=ztap-kafka"
  "debezium/connect:2.7.3.Final=ztap-debezium-connect"
  "trinodb/trino:450=ztap-trino"
  "busybox:1.36=ztap-busybox"
  "danielqsj/kafka-exporter:v1.7.0=ztap-kafka-exporter"
  "prom/prometheus:v2.54.1=ztap-prometheus"
  "grafana/grafana:11.2.0=ztap-grafana"
  # custom-built ztap services (built by docker compose)
  "ztap-oss-control-plane:latest=ztap-control-plane"
  "ztap-oss-sink:latest=ztap-sink"
  "ztap-oss-sync:latest=ztap-sync"
  "ztap-oss-proxy:latest=ztap-proxy"
  "ztap-oss-trino-init:latest=ztap-trino-init"
  "ztap-oss-reverse-watcher:latest=ztap-reverse-watcher"
)

echo "Mirroring ${#MAP[@]} images to namespace '$NS'..."
ok=0; fail=0
for entry in "${MAP[@]}"; do
  src="${entry%%=*}"
  repo="${entry##*=}"
  tag="${src##*:}"
  dest="$NS/$repo:$tag"

  if ! docker image inspect "$src" >/dev/null 2>&1; then
    echo "  SKIP $src (not present locally)"; fail=$((fail+1)); continue
  fi
  echo "  -> $src  =>  $dest"
  docker tag "$src" "$dest" || { echo "     tag failed"; fail=$((fail+1)); continue; }
  if docker push "$dest" >/dev/null 2>&1; then
    echo "     pushed"; ok=$((ok+1))
  else
    echo "     PUSH FAILED (are you 'docker login'-ed as $NS?)"; fail=$((fail+1))
  fi
done

echo ""
echo "done: $ok pushed, $fail skipped/failed"
echo ""
echo "Add these to .env to make the stack pull your mirrored base images:"
echo "  POSTGRES_IMAGE=$NS/ztap-postgres:16"
echo "  MINIO_IMAGE=$NS/ztap-minio:latest"
echo "  MC_IMAGE=$NS/ztap-minio-mc:latest"
echo "  UC_IMAGE=$NS/ztap-unity-catalog:latest"
echo "  KAFKA_IMAGE=$NS/ztap-kafka:3.8.0"
echo "  CONNECT_IMAGE=$NS/ztap-debezium-connect:2.7.3.Final"
echo "  TRINO_IMAGE=$NS/ztap-trino:450"
