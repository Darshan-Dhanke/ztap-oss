#!/usr/bin/env bash
# Component #1 integration test: REAL suspend/resume-aware connection proxy.
# After the Neon cutover the proxy fronts the Neon compute (ztap-neon-compute)
# and actually stops/starts that container via the Docker API — real Neon
# compute scale-to-zero. This test proves:
#   - a query works transparently through the proxy
#   - forcing suspend genuinely stops the Neon compute (Docker reports "exited")
#   - the next connection genuinely starts it (a real cold start) and still
#     returns the correct result
# The psql client is a throwaway postgres:16 container sharing the proxy's
# network namespace (so it stays up while the proxy stops the Neon compute).
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

API=http://localhost:18002
# client -> proxy (localhost:5432 in the proxy's netns) -> Neon compute
PSQL="docker run --rm --network container:ztap-proxy -e PGPASSWORD=cloud_admin -e PGCONNECT_TIMEOUT=120 postgres:16 psql -h localhost -p 5432 -U cloud_admin -d postgres -tAc"

ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }
cstate(){ docker inspect -f '{{.State.Status}}' ztap-neon-compute 2>/dev/null; }

echo "== 1. proxy healthy and in real mode =="
curl -sf "$API/healthz" >/dev/null && pass "proxy healthy" || fail "proxy not healthy"
MODE=$(curl -s "$API/state" | grep -o '"mode":"[^"]*"')
echo "    $MODE"
echo "$MODE" | grep -q 'real' && pass "real (docker) mode" || fail "not in real mode"

echo "== 2. transparent query through proxy (warm) =="
$PSQL "SELECT 1" >/dev/null 2>&1
OUT=$($PSQL "SELECT 42;" 2>/dev/null | tr -d '[:space:]')
[ "$OUT" = "42" ] && pass "got 42 through proxy" || fail "warm query failed: $OUT"

echo "== 3. force suspend really STOPS the Neon compute =="
curl -s -X POST "$API/suspend" >/dev/null
for i in $(seq 1 15); do [ "$(cstate)" = "exited" ] && break; sleep 1; done
[ "$(cstate)" = "exited" ] && pass "ztap-neon-compute is exited (real stop)" || fail "compute not stopped ($(cstate))"
curl -s "$API/state" | grep -q '"container_running":false' && pass "proxy reports compute down" || fail "proxy state wrong"

echo "== 4. reconnect triggers a REAL Neon cold start and returns correctly =="
WAKES_BEFORE=$(curl -s "$API/state" | grep -o '"wake_count":[0-9]*' | grep -o '[0-9]*')
OUT=$($PSQL "SELECT 7;" 2>/dev/null | tr -d '[:space:]')
[ "$OUT" = "7" ] && pass "cold-start query returned correctly" || fail "cold query failed: $OUT"
[ "$(cstate)" = "running" ] && pass "ztap-neon-compute is running again (real start)" || fail "compute not started"

echo "== 5. cold start was real and measured =="
ST=$(curl -s "$API/state")
WAKES_AFTER=$(echo "$ST" | grep -o '"wake_count":[0-9]*' | grep -o '[0-9]*')
CS=$(echo "$ST" | grep -o '"last_cold_start_ms":[0-9]*' | grep -o '[0-9]*$')
[ "$WAKES_AFTER" -gt "$WAKES_BEFORE" ] && pass "wake_count incremented ($WAKES_BEFORE -> $WAKES_AFTER)" || fail "wake_count not incremented"
[ "${CS:-0}" -gt 0 ] && pass "measured Neon cold start: ${CS}ms" || fail "no measured cold start"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "PROXY INTEGRATION TEST PASSED" || { echo "PROXY TEST FAILED"; exit 1; }
