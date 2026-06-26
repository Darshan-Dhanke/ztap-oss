#!/usr/bin/env bash
# Component #2 integration test: schema evolution + reverse sync.
#   A) ALTER TABLE ADD COLUMN in Postgres -> reconcile -> UC schema updated
#   B) lakehouse -> Postgres reverse sync with conflict resolution + idempotency
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CP=http://localhost:18000
SYNC=http://localhost:18001
UC=http://localhost:18080
P=synctest
PSQL="docker exec -e PGPASSWORD=cloud_admin ztap-neon-compute psql -h localhost -p 55433 -U cloud_admin -d postgres"

ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }
has(){ echo "$1" | grep -q "$2"; }

echo "== setup =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null; sleep 1
curl -s -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$P\"}" >/dev/null
$PSQL -c "CREATE TABLE proj_$P.items (id bigint primary key, name text, v bigint);" >/dev/null
curl -s -X POST "$CP/projects/$P/tables" -H 'content-type: application/json' -d '{"table":"items"}' >/dev/null
pass "project + table registered"

echo "== A: schema in sync initially =="
R=$(curl -s -X POST "$SYNC/projects/$P/tables/items/reconcile-schema")
echo "    $R"
has "$R" '"in_sync":true' && pass "reported in_sync" || fail "not in_sync at start"

echo "== A: ALTER TABLE ADD COLUMN in Postgres =="
$PSQL -c "ALTER TABLE proj_$P.items ADD COLUMN status text;" >/dev/null
R=$(curl -s -X POST "$SYNC/projects/$P/tables/items/reconcile-schema")
echo "    $R"
has "$R" '"added":\["status"\]' && pass "drift detected (status added)" || fail "drift not detected"
has "$R" '"reconciled":true' && pass "reconciled UC" || fail "did not reconcile"

echo "== A: UC table now has the new column =="
TBL=$(curl -s "$UC/api/2.1/unity-catalog/tables/ztap_$P.cdc.items")
has "$TBL" '"name":"status"' && pass "UC table has 'status' column" || fail "UC missing status column"

echo "== A: second reconcile is a no-op (in sync again) =="
R=$(curl -s -X POST "$SYNC/projects/$P/tables/items/reconcile-schema")
has "$R" '"in_sync":true' && pass "back in sync" || fail "still drifted"

echo "== B: seed a Postgres row =="
$PSQL -c "INSERT INTO proj_$P.items (id,name,v,status) VALUES (1,'orig',1,'a');" >/dev/null

echo "== B1: lakehouse newer (v=5) wins under last_write_wins =="
R=$(curl -s -X POST "$SYNC/projects/$P/tables/items/reverse-sync" -H 'content-type: application/json' \
  -d '{"rows":[{"id":1,"name":"from_lake","v":5,"status":"b"}],"pk_col":"id","version_col":"v","policy":"last_write_wins"}')
echo "    $R"
has "$R" '"action":"apply"' && pass "lake newer -> applied" || fail "lake newer not applied"
VAL=$($PSQL -tAc "SELECT name FROM proj_$P.items WHERE id=1;")
[ "$VAL" = "from_lake" ] && pass "Postgres row updated to lake value" || fail "PG row not updated ($VAL)"

echo "== B2: idempotency — replay same change (v=5) is skipped =="
R=$(curl -s -X POST "$SYNC/projects/$P/tables/items/reverse-sync" -H 'content-type: application/json' \
  -d '{"rows":[{"id":1,"name":"from_lake","v":5,"status":"b"}],"pk_col":"id","version_col":"v","policy":"last_write_wins"}')
echo "    $R"
has "$R" '"action":"skip_duplicate"' && pass "replay skipped (loop-prevention)" || fail "replay not skipped"

echo "== B3: stale lakehouse change (v=3) loses to current PG (v=5) =="
R=$(curl -s -X POST "$SYNC/projects/$P/tables/items/reverse-sync" -H 'content-type: application/json' \
  -d '{"rows":[{"id":1,"name":"stale","v":3,"status":"z"}],"pk_col":"id","version_col":"v","policy":"last_write_wins"}')
echo "    $R"
# v=3 <= last applied v=5 -> duplicate guard catches it first (also correct)
has "$R" '"action":"skip' && pass "stale change not applied" || fail "stale change wrongly applied"
VAL=$($PSQL -tAc "SELECT name FROM proj_$P.items WHERE id=1;")
[ "$VAL" = "from_lake" ] && pass "Postgres still holds winning value" || fail "PG clobbered by stale ($VAL)"

echo "== B4: brand-new key inserted from lakehouse =="
R=$(curl -s -X POST "$SYNC/projects/$P/tables/items/reverse-sync" -H 'content-type: application/json' \
  -d '{"rows":[{"id":2,"name":"new_from_lake","v":1,"status":"c"}],"pk_col":"id","version_col":"v","policy":"last_write_wins"}')
has "$R" '"action":"apply"' && pass "new key applied" || fail "new key not applied"
CNT=$($PSQL -tAc "SELECT count(*) FROM proj_$P.items WHERE id=2;")
[ "$CNT" = "1" ] && pass "new row present in Postgres" || fail "new row missing"

echo "== teardown =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
sleep 6  # let any in-flight sink flush land before removing the Delta data
docker exec ztap-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rm -r --force local/warehouse/$P >/dev/null 2>&1" || true
pass "torn down"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "SYNC INTEGRATION TEST PASSED" || { echo "SYNC TEST FAILED"; exit 1; }
