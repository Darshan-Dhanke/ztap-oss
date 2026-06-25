#!/usr/bin/env bash
# Edge-case integration tests against the running stack.
# Exercises the nasty type mappings into Unity Catalog, idempotency, invalid
# input handling, and teardown completeness.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CP=http://localhost:18000
UC=http://localhost:18080
PROJ=edge
PGEXEC="docker exec ztap-postgres psql -U ztap -d ztap -tAc"

ok=0; bad=0
check() { # check <desc> <condition-cmd...>
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "  PASS: $desc"; ok=$((ok+1)); else echo "  FAIL: $desc"; bad=$((bad+1)); fi
}
contains() { echo "$1" | grep -q "$2"; }

echo "== setup: fresh project =="
curl -s -X DELETE "$CP/projects/$PROJ" >/dev/null
RESP=$(curl -s -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$PROJ\"}")
contains "$RESP" '"status": *"ready"' && echo "  project ready" || { echo "  could not create project: $RESP"; exit 1; }

echo "== EDGE 1: idempotent re-create (POST same project again) =="
RESP2=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$PROJ\"}")
[ "$RESP2" = "201" ] && { echo "  PASS: re-create is idempotent (201)"; ok=$((ok+1)); } || { echo "  FAIL: re-create returned $RESP2"; bad=$((bad+1)); }

echo "== EDGE 2: invalid project names rejected (400) =="
for bad_name in "1bad" "With-Caps" "has space" "a"; do
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$bad_name\"}")
  [ "$code" = "400" ] && { echo "  PASS: '$bad_name' -> 400"; ok=$((ok+1)); } || { echo "  FAIL: '$bad_name' -> $code"; bad=$((bad+1)); }
done

echo "== EDGE 3: register table with ALL the nasty types =="
$PGEXEC "CREATE TABLE IF NOT EXISTS proj_${PROJ}.wide (
  id bigint primary key,
  doc jsonb,
  uid uuid,
  dur interval,
  tags integer[],
  big_money numeric(40,5),
  ts timestamptz,
  net cidr,
  price money,
  name varchar(50)
);" >/dev/null
REG=$(curl -s -X POST "$CP/projects/$PROJ/tables" -H 'content-type: application/json' -d '{"table":"wide"}')
echo "    response: $REG"

# Each of these columns must be reported lossy.
for col in doc uid dur tags big_money net price; do
  contains "$REG" "\"column\":\"$col\"" && { echo "  PASS: $col flagged lossy"; ok=$((ok+1)); } || { echo "  FAIL: $col NOT flagged lossy"; bad=$((bad+1)); }
done
# id (bigint) and ts (timestamptz) and name(varchar) must NOT be lossy.
for col in '"column":"id"' '"column":"ts"' '"column":"name"'; do
  contains "$REG" "$col" && { echo "  FAIL: $col wrongly flagged lossy"; bad=$((bad+1)); } || { echo "  PASS: $col not lossy (correct)"; ok=$((ok+1)); }
done

echo "== EDGE 4: UC table actually exists with mapped columns =="
TBL=$(curl -s "$UC/api/2.1/unity-catalog/tables/ztap_${PROJ}.cdc.wide")
contains "$TBL" '"name":"wide"' && { echo "  PASS: UC table created"; ok=$((ok+1)); } || { echo "  FAIL: UC table missing"; bad=$((bad+1)); }
contains "$TBL" '"type_name":"DECIMAL"' && { echo "  PASS: decimal mapped"; ok=$((ok+1)); } || { echo "  FAIL: decimal missing"; bad=$((bad+1)); }
contains "$TBL" '"type_name":"ARRAY"' && { echo "  PASS: array mapped"; ok=$((ok+1)); } || { echo "  FAIL: array missing"; bad=$((bad+1)); }
contains "$TBL" '"type_name":"LONG"' && { echo "  PASS: bigint->LONG mapped"; ok=$((ok+1)); } || { echo "  FAIL: long missing"; bad=$((bad+1)); }

echo "== EDGE 5: numeric(40,5) clamped to DECIMAL(38,5) =="
contains "$REG" 'decimal(38,5)' && { echo "  PASS: oversized numeric clamped"; ok=$((ok+1)); } || { echo "  FAIL: clamp missing"; bad=$((bad+1)); }

echo "== EDGE 6: register on non-existent project -> 404 =="
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$CP/projects/nope/tables" -H 'content-type: application/json' -d '{"table":"x"}')
[ "$code" = "404" ] && { echo "  PASS: 404"; ok=$((ok+1)); } || { echo "  FAIL: got $code"; bad=$((bad+1)); }

echo "== EDGE 7: register non-existent table -> 400 =="
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$CP/projects/$PROJ/tables" -H 'content-type: application/json' -d '{"table":"ghost"}')
[ "$code" = "400" ] && { echo "  PASS: 400"; ok=$((ok+1)); } || { echo "  FAIL: got $code"; bad=$((bad+1)); }

echo "== EDGE 8: teardown removes catalog (and its schema/table) =="
curl -s -X DELETE "$CP/projects/$PROJ" >/dev/null
docker exec ztap-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rm -r --force local/warehouse/$PROJ >/dev/null 2>&1" || true
sleep 1
code=$(curl -s -o /dev/null -w "%{http_code}" "$UC/api/2.1/unity-catalog/catalogs/ztap_${PROJ}")
[ "$code" = "404" ] && { echo "  PASS: catalog gone (404)"; ok=$((ok+1)); } || { echo "  FAIL: catalog still $code"; bad=$((bad+1)); }
SLOT=$($PGEXEC "SELECT count(*) FROM pg_replication_slots WHERE slot_name='ztap_${PROJ}';")
[ "$SLOT" = "0" ] && { echo "  PASS: no orphan replication slot"; ok=$((ok+1)); } || { echo "  FAIL: orphan slot count=$SLOT"; bad=$((bad+1)); }

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "ALL EDGE CASES PASSED" || { echo "SOME EDGE CASES FAILED"; exit 1; }
