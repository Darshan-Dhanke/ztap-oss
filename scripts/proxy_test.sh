#!/usr/bin/env bash
# Component #1 integration test: REAL suspend/resume-aware connection proxy.
# The proxy fronts a dedicated "compute" Postgres (ztap-compute) and actually
# stops/starts that container via the Docker API. This test proves:
#   - a query works transparently through the proxy
#   - forcing suspend genuinely stops the container (Docker reports "exited")
#   - the next connection genuinely starts it (a real, measured cold start) and
#     still returns the correct result
# The psql client runs from the always-on platform Postgres (ztap-postgres),
# connecting through the proxy to ztap-compute.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

API=http://localhost:18002
PSQL="docker exec -e PGPASSWORD=compute ztap-postgres psql -h ztap-proxy -p 5432 -U compute -d compute -tAc"

ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }
cstate(){ docker inspect -f '{{.State.Status}}' ztap-compute 2>/dev/null; }

echo "== 1. proxy healthy and in real mode =="
curl -sf "$API/healthz" >/dev/null && pass "proxy healthy" || fail "proxy not healthy"
MODE=$(curl -s "$API/state" | grep -o '"mode":"[^"]*"')
echo "    $MODE"
echo "$MODE" | grep -q 'real' && pass "real (docker) mode" || fail "not in real mode"

echo "== 2. transparent query through proxy (warm) =="
# ensure compute is up first
$PSQL "SELECT 1" >/dev/null 2>&1
N=$($PSQL "SELECT count(*) FROM metrics;" 2>&1 | tr -d '[:space:]')
[ "$N" = "3" ] && pass "got 3 rows through proxy" || fail "warm query failed: $N"

echo "== 3. force suspend really STOPS the container =="
curl -s -X POST "$API/suspend" >/dev/null
for i in $(seq 1 10); do [ "$(cstate)" = "exited" ] && break; sleep 1; done
[ "$(cstate)" = "exited" ] && pass "ztap-compute is exited (real stop)" || fail "container not stopped ($(cstate))"
curl -s "$API/state" | grep -q '"container_running":false' && pass "proxy reports container down" || fail "proxy state wrong"

echo "== 4. reconnect triggers a REAL cold start and returns correctly =="
WAKES_BEFORE=$(curl -s "$API/state" | grep -o '"wake_count":[0-9]*' | grep -o '[0-9]*')
OUT=$($PSQL "SELECT name FROM metrics WHERE name='cpu_pct';" 2>&1 | tr -d '[:space:]')
[ "$OUT" = "cpu_pct" ] && pass "cold-start query returned correctly" || fail "cold query failed: $OUT"
[ "$(cstate)" = "running" ] && pass "ztap-compute is running again (real start)" || fail "container not started"

echo "== 5. cold start was real and measured =="
ST=$(curl -s "$API/state")
WAKES_AFTER=$(echo "$ST" | grep -o '"wake_count":[0-9]*' | grep -o '[0-9]*')
CS=$(echo "$ST" | grep -o '"last_cold_start_ms":[0-9]*' | grep -o '[0-9]*$')
[ "$WAKES_AFTER" -gt "$WAKES_BEFORE" ] && pass "wake_count incremented ($WAKES_BEFORE -> $WAKES_AFTER)" || fail "wake_count not incremented"
[ "${CS:-0}" -gt 0 ] && pass "measured cold start: ${CS}ms" || fail "no measured cold start"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "PROXY INTEGRATION TEST PASSED" || { echo "PROXY TEST FAILED"; exit 1; }
