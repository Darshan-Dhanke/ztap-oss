#!/usr/bin/env bash
# Continuous lakehouse -> Postgres via the CDF inbox. Proves that normal
# INSERT / UPDATE / DELETE on the inbox Delta table (from Trino) are applied to
# Postgres by the reverse-watcher — no version numbers, deletes included.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CP=http://localhost:18000
P=rwtest
PSQL="docker exec -e PGPASSWORD=cloud_admin ztap-neon-compute psql -h localhost -p 55433 -U cloud_admin -d postgres -tAc"
TRINO="docker exec ztap-trino trino --catalog delta --schema proj_${P} --execute"

ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }
wait_for(){ for i in $(seq 1 15); do [ "$($PSQL "$1" 2>/dev/null | tr -d '[:space:]')" = "$2" ] && return 0; sleep 3; done; return 1; }

echo "== setup: project + table + CDF inbox =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
$PSQL "DELETE FROM ztap_control.reverse_watch WHERE project='$P';" >/dev/null 2>&1 || true
docker exec ztap-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rm -r --force local/warehouse/$P >/dev/null 2>&1" || true
sleep 1
curl -s -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$P\"}" >/dev/null
$PSQL "CREATE TABLE proj_${P}.items (id bigint primary key, label text, score numeric(6,2));" >/dev/null
bash scripts/make_inbox.sh "$P" items "id bigint, label varchar, score decimal(6,2)" >/dev/null 2>&1
pass "project, table and CDF inbox created"

echo "== lakehouse INSERT -> Postgres =="
$TRINO "INSERT INTO items_inbox VALUES (1,'alpha',1.50),(2,'beta',2.50)" >/dev/null 2>&1
wait_for "SELECT count(*) FROM proj_${P}.items;" "2" && pass "2 rows inserted into Postgres" || fail "insert not applied"

echo "== lakehouse UPDATE -> Postgres =="
$TRINO "UPDATE items_inbox SET label='alpha_v2' WHERE id=1" >/dev/null 2>&1
wait_for "SELECT label FROM proj_${P}.items WHERE id=1;" "alpha_v2" && pass "update applied (alpha_v2)" || fail "update not applied"

echo "== lakehouse DELETE -> Postgres =="
$TRINO "DELETE FROM items_inbox WHERE id=2" >/dev/null 2>&1
wait_for "SELECT count(*) FROM proj_${P}.items WHERE id=2;" "0" && pass "delete propagated (id=2 gone)" || fail "delete not propagated"

echo "== idempotency / no loop: state stable =="
sleep 12
CNT=$($PSQL "SELECT count(*) FROM proj_${P}.items;" | tr -d '[:space:]')
LBL=$($PSQL "SELECT label FROM proj_${P}.items WHERE id=1;" | tr -d '[:space:]')
{ [ "$CNT" = "1" ] && [ "$LBL" = "alpha_v2" ]; } && pass "stable: 1 row, id=1=alpha_v2 (no loop)" || fail "unstable: count=$CNT label=$LBL"

echo "== teardown =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
docker exec ztap-minio sh -c "mc rm -r --force local/warehouse/$P >/dev/null 2>&1" || true
$PSQL "DELETE FROM ztap_control.reverse_watch WHERE project='$P';" >/dev/null 2>&1 || true
pass "torn down"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "REVERSE-WATCH (CDF) TEST PASSED" || { echo "REVERSE-WATCH TEST FAILED"; exit 1; }
