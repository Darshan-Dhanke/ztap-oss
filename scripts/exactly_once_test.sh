#!/usr/bin/env bash
# #5 exactly-once sink test. Writes rows, then RESETS the Kafka consumer group to
# earliest and restarts the sink. If exactly-once holds, the sink resumes from
# the offset Delta already committed (via the app-transaction marker) and writes
# zero duplicates — the Delta row count is unchanged.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CP=http://localhost:18000
P=eo
PSQL="docker exec -e PGPASSWORD=cloud_admin ztap-neon-compute psql -h localhost -p 55433 -U cloud_admin -d postgres -c"
# count rows in the Delta table via delta-rs inside the sink container
delta_count() {
  docker exec ztap-sink python -c "
from deltalake import DeltaTable
so={'AWS_ENDPOINT_URL':'http://minio:9000','AWS_ACCESS_KEY_ID':'minioadmin','AWS_SECRET_ACCESS_KEY':'minioadmin','AWS_REGION':'us-east-1','AWS_ALLOW_HTTP':'true'}
try:
    print(DeltaTable('s3://warehouse/${P}/t', storage_options=so).to_pyarrow_table().num_rows)
except Exception:
    print(-1)
" 2>/dev/null | tr -d '[:space:]'
}

ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }

echo "== setup =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
docker exec ztap-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rm -r --force local/warehouse/$P >/dev/null 2>&1" || true
sleep 1
curl -s -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$P\"}" >/dev/null
$PSQL "CREATE TABLE proj_${P}.t (id bigint primary key, val text);" >/dev/null
curl -s -X POST "$CP/projects/$P/tables" -H 'content-type: application/json' -d '{"table":"t"}' >/dev/null
$PSQL "INSERT INTO proj_${P}.t VALUES (1,'a'),(2,'b'),(3,'c');" >/dev/null
echo "  inserted 3 rows; waiting for sink flush..."
for i in $(seq 1 12); do [ "$(delta_count)" = "3" ] && break; sleep 3; done
N1=$(delta_count)
[ "$N1" = "3" ] && pass "3 change rows in Delta" || fail "expected 3, got $N1"

echo "== app-transaction marker present (proves idempotent commit) =="
TXN=$(docker exec ztap-sink python -c "
from deltalake import DeltaTable
so={'AWS_ENDPOINT_URL':'http://minio:9000','AWS_ACCESS_KEY_ID':'minioadmin','AWS_SECRET_ACCESS_KEY':'minioadmin','AWS_REGION':'us-east-1','AWS_ALLOW_HTTP':'true'}
print(len(DeltaTable('s3://warehouse/${P}/t', storage_options=so).transaction_versions()))
" 2>/dev/null | tr -d '[:space:]')
[ "${TXN:-0}" -ge 1 ] && pass "Delta has $TXN app-transaction marker(s)" || fail "no app-transaction marker"

echo "== reset Kafka offsets to earliest + restart sink (force reprocess) =="
docker stop ztap-sink >/dev/null
docker exec ztap-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group ztap-delta-sink --reset-offsets --to-earliest --all-topics --execute >/dev/null 2>&1 || true
docker start ztap-sink >/dev/null
echo "  sink restarted; waiting to confirm NO duplicate writes..."
sleep 25
N2=$(delta_count)
[ "$N2" = "3" ] && pass "still 3 rows after reprocess (exactly-once)" || fail "duplicates! count=$N2"

echo "== teardown =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
docker exec ztap-minio sh -c "mc rm -r --force local/warehouse/$P >/dev/null 2>&1" || true
pass "torn down"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "EXACTLY-ONCE TEST PASSED" || { echo "EXACTLY-ONCE TEST FAILED"; exit 1; }
