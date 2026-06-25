#!/usr/bin/env bash
# End-to-end smoke test for the ztap-oss data plane.
#
# Verifies the full provisioning + CDC path against the running stack:
#   1. control plane is healthy
#   2. create a project  -> PG schema + UC catalog + Debezium connector
#   3. the Postgres schema actually exists
#   4. the Unity Catalog catalog actually exists
#   5. the Debezium connector is RUNNING
#   6. inserting a row produces a CDC event on the Kafka topic
#   7. teardown removes everything
#
# Ports match docker-compose.yml (offset to avoid collisions).
set -euo pipefail

# On Windows Git Bash / MSYS, absolute args like "/opt/kafka/..." passed to
# `docker exec` get rewritten to "C:/Program Files/Git/opt/kafka/...". Disable
# that path conversion so in-container paths are passed through verbatim.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CP=http://localhost:18000
UC=http://localhost:18080
CONNECT=http://localhost:18083
PROJECT=smoke
PGHOST=localhost
PGPORT=55432
PGUSER=${POSTGRES_USER:-ztap}
PGPASS=${POSTGRES_PASSWORD:-ztap}
PGDB=${POSTGRES_DB:-ztap}

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

echo "== 1. control plane health =="
curl -sf "$CP/healthz" >/dev/null && pass "control plane healthy" || fail "control plane not healthy"

echo "== 2. create project '$PROJECT' =="
# clean any prior run first (ignore errors)
curl -s -X DELETE "$CP/projects/$PROJECT" >/dev/null || true
RESP=$(curl -sf -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$PROJECT\"}")
echo "    -> $RESP"
echo "$RESP" | grep -q '"status": *"ready"' && pass "project created" || fail "project not ready"

echo "== 3. postgres schema exists =="
docker exec ztap-postgres psql -U "$PGUSER" -d "$PGDB" -tAc \
  "SELECT 1 FROM information_schema.schemata WHERE schema_name='proj_${PROJECT}'" \
  | grep -q 1 && pass "schema proj_${PROJECT} exists" || fail "schema missing"

echo "== 4. unity catalog registered =="
curl -sf "$UC/api/2.1/unity-catalog/catalogs/ztap_${PROJECT}" >/dev/null \
  && pass "catalog ztap_${PROJECT} exists" || fail "catalog missing"

echo "== 5. debezium connector running =="
for i in $(seq 1 20); do
  STATE=$(curl -sf "$CONNECT/connectors/ztap-${PROJECT}-cdc/status" | grep -o '"state":"[A-Z]*"' | head -1 || true)
  [ "$STATE" = '"state":"RUNNING"' ] && break
  sleep 2
done
[ "$STATE" = '"state":"RUNNING"' ] && pass "connector RUNNING" || fail "connector not running ($STATE)"

echo "== 6. CDC event flows on insert =="
docker exec ztap-postgres psql -U "$PGUSER" -d "$PGDB" -c \
  "CREATE TABLE IF NOT EXISTS proj_${PROJECT}.events (id serial primary key, payload jsonb, ts timestamptz default now());" >/dev/null
docker exec ztap-postgres psql -U "$PGUSER" -d "$PGDB" -c \
  "INSERT INTO proj_${PROJECT}.events (payload) VALUES ('{\"hello\":\"ztap\"}');" >/dev/null
TOPIC="ztap.${PROJECT}.proj_${PROJECT}.events"
echo "    waiting for a message on $TOPIC ..."
MSG=$(docker exec ztap-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --from-beginning --timeout-ms 30000 --max-messages 1 2>/dev/null || true)
echo "$MSG" | grep -q 'hello' && pass "CDC event observed on Kafka" || fail "no CDC event on $TOPIC"

echo "== 7. teardown =="
curl -sf -X DELETE "$CP/projects/$PROJECT" >/dev/null && pass "project deleted" || fail "delete failed"
# let any in-flight sink flush land, then remove the Delta data so no stale
# table is left behind (the sink writes async, ~3s after the CDC event)
sleep 6
docker exec ztap-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rm -r --force local/warehouse/${PROJECT} >/dev/null 2>&1" || true

echo ""
echo "ALL SMOKE TESTS PASSED"
