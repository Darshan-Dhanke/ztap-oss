#!/usr/bin/env bash
# Phase 2 integration test: prove the Delta sink closes the loop.
#   Postgres insert/update/delete -> CDC -> Kafka -> sink -> Delta in MinIO
# Verifies the Delta table exists at the location UC registered and that the
# change feed (insert/update/delete) is readable back via delta-rs.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CP=http://localhost:18000
P=sinktest
PSQL="docker exec ztap-postgres psql -U ztap -d ztap"

pass(){ echo "  PASS: $1"; }
fail(){ echo "  FAIL: $1"; exit 1; }

echo "== setup =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
docker exec ztap-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic "ztap.$P.proj_$P.orders" >/dev/null 2>&1 || true
docker exec ztap-minio mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 || true
docker exec ztap-minio mc rm -r --force "local/warehouse/$P" >/dev/null 2>&1 || true

curl -s -X POST "$CP/projects" -H 'content-type: application/json' -d "{\"name\":\"$P\"}" >/dev/null
$PSQL -c "CREATE TABLE proj_$P.orders (id bigint primary key, customer text, amount numeric(10,2), doc jsonb);" >/dev/null
curl -s -X POST "$CP/projects/$P/tables" -H 'content-type: application/json' -d '{"table":"orders"}' >/dev/null
pass "project + table registered"

echo "== insert / update / delete =="
$PSQL -c "INSERT INTO proj_$P.orders VALUES (1,'alice',19.99,'{\"sku\":\"A1\"}'),(2,'bob',5.50,'{\"sku\":\"B2\"}');" >/dev/null
$PSQL -c "UPDATE proj_$P.orders SET amount=29.99 WHERE id=1;" >/dev/null
$PSQL -c "DELETE FROM proj_$P.orders WHERE id=2;" >/dev/null
echo "  waiting for sink to flush..."
sleep 14

echo "== Delta files present in MinIO =="
docker exec ztap-minio mc ls -r "local/warehouse/$P/orders" 2>/dev/null | grep -q '_delta_log' \
  && pass "Delta _delta_log present" || fail "no Delta log written"

echo "== read back via delta-rs and assert change feed =="
python - "$P" <<'PY' || exit 1
import sys
from deltalake import DeltaTable
P = sys.argv[1]
so = {"AWS_ENDPOINT_URL":"http://localhost:19000","AWS_ACCESS_KEY_ID":"minioadmin",
      "AWS_SECRET_ACCESS_KEY":"minioadmin","AWS_REGION":"us-east-1","AWS_ALLOW_HTTP":"true"}
dt = DeltaTable(f"s3://warehouse/{P}/orders", storage_options=so)
rows = dt.to_pyarrow_table().to_pylist()
ops = sorted(r["_op"] for r in rows)
assert any(r["_op"]=="c" for r in rows), "no insert captured"
assert any(r["_op"]=="u" and r["amount"]=="29.99" for r in rows), "update not captured"
assert any(r["_op"]=="d" and r["_deleted"] for r in rows), "delete not captured"
print("  PASS: insert/update/delete all present in Delta change feed", ops)
PY

echo "== teardown =="
curl -s -X DELETE "$CP/projects/$P" >/dev/null
docker exec ztap-minio mc rm -r --force "local/warehouse/$P" >/dev/null 2>&1 || true
pass "torn down"
echo ""
echo "SINK INTEGRATION TEST PASSED"
