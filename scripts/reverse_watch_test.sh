#!/usr/bin/env bash
# Component #2 (continuous) integration test: lakehouse -> Postgres via inbox.
# Proves that writing to a project's inbox Delta table from Trino is auto-applied
# to Postgres by the reverse-watcher (no manual API call), handles updates by
# _lake_version, and is idempotent (no duplicates / no loop).
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CP=http://localhost:18000
P=rwtest
PSQL="docker exec ztap-postgres psql -U ztap -d ztap -tAc"
TRINO="docker exec ztap-trino trino --catalog delta --schema proj_${P} --execute"

ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }

echo "== setup: project + table + inbox =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
docker exec ztap-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rm -r --force local/warehouse/$P >/dev/null 2>&1" || true
sleep 1
curl -s -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$P\"}" >/dev/null
$PSQL "CREATE TABLE proj_${P}.items (id bigint primary key, label text, score numeric(6,2));" >/dev/null
bash scripts/make_inbox.sh "$P" items "id bigint, label varchar, score decimal(6,2)" >/dev/null 2>&1
pass "project, table and inbox created"

echo "== insert via Trino inbox -> auto-applied to Postgres =="
$TRINO "INSERT INTO items_inbox VALUES (1,'alpha',1.50,100)" >/dev/null 2>&1
for i in $(seq 1 15); do
  V=$($PSQL "SELECT label FROM proj_${P}.items WHERE id=1;" 2>/dev/null | tr -d '[:space:]')
  [ "$V" = "alpha" ] && break; sleep 3
done
[ "$V" = "alpha" ] && pass "insert auto-applied (label=alpha)" || fail "insert not applied ($V)"

echo "== update via inbox (higher _lake_version) -> Postgres updates =="
$TRINO "INSERT INTO items_inbox VALUES (1,'alpha_v2',9.99,200)" >/dev/null 2>&1
for i in $(seq 1 8); do
  V=$($PSQL "SELECT label FROM proj_${P}.items WHERE id=1;" 2>/dev/null | tr -d '[:space:]')
  [ "$V" = "alpha_v2" ] && break; sleep 3
done
[ "$V" = "alpha_v2" ] && pass "update auto-applied (label=alpha_v2)" || fail "update not applied ($V)"

echo "== idempotency: still exactly one row for id=1 (no loop/dup) =="
sleep 12  # let several watcher cycles run
CNT=$($PSQL "SELECT count(*) FROM proj_${P}.items WHERE id=1;" | tr -d '[:space:]')
[ "$CNT" = "1" ] && pass "exactly one row (idempotent, no loop)" || fail "row count=$CNT"

echo "== teardown =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
docker exec ztap-minio sh -c "mc rm -r --force local/warehouse/$P >/dev/null 2>&1" || true
pass "torn down"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "REVERSE-WATCH TEST PASSED" || { echo "REVERSE-WATCH TEST FAILED"; exit 1; }
