#!/usr/bin/env bash
# Component #1 integration test: suspend/resume-aware connection proxy.
# Proves a real psql session works through the proxy, that forcing a suspend
# makes the next connection trigger a (simulated) cold-start wake, and that the
# query still returns correctly through the wake — transparently to the client.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

API=http://localhost:18002
# psql is run from inside the postgres container, connecting to the proxy by its
# container name on the shared docker network (no host psql needed).
PSQL_VIA_PROXY="docker exec -e PGPASSWORD=ztap ztap-postgres psql -h ztap-proxy -p 5432 -U ztap -d ztap -tAc"

ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }
has(){ echo "$1" | grep -q "$2"; }

echo "== 1. proxy healthy =="
curl -sf "$API/healthz" >/dev/null && pass "proxy healthy" || fail "proxy not healthy"

echo "== 2. transparent query through proxy (warm) =="
OUT=$($PSQL_VIA_PROXY "SELECT 42;" 2>&1)
[ "$OUT" = "42" ] && pass "got 42 through proxy" || fail "warm query failed: $OUT"

echo "== 3. force compute suspend =="
S=$(curl -s -X POST "$API/suspend")
echo "    $S"
has "$S" '"state":"suspended"' && pass "state suspended" || fail "did not suspend"

echo "== 4. cold-start: next connection wakes compute and still returns =="
WAKES_BEFORE=$(curl -s "$API/state" | grep -o '"wake_count":[0-9]*' | grep -o '[0-9]*')
START=$(date +%s%3N 2>/dev/null || date +%s)
OUT=$($PSQL_VIA_PROXY "SELECT 'woke';" 2>&1)
END=$(date +%s%3N 2>/dev/null || date +%s)
[ "$OUT" = "woke" ] && pass "cold-start query returned correctly" || fail "cold query failed: $OUT"

echo "== 5. wake was counted and state is active again =="
ST=$(curl -s "$API/state")
echo "    $ST"
WAKES_AFTER=$(echo "$ST" | grep -o '"wake_count":[0-9]*' | grep -o '[0-9]*')
[ "$WAKES_AFTER" -gt "$WAKES_BEFORE" ] && pass "wake_count incremented ($WAKES_BEFORE -> $WAKES_AFTER)" \
  || fail "wake_count not incremented"
has "$ST" '"state":"active"' && pass "state active after wake" || fail "state not active"

echo "== 6. subsequent query is warm (no extra wake) =="
$PSQL_VIA_PROXY "SELECT 1;" >/dev/null 2>&1
WAKES_FINAL=$(curl -s "$API/state" | grep -o '"wake_count":[0-9]*' | grep -o '[0-9]*')
[ "$WAKES_FINAL" = "$WAKES_AFTER" ] && pass "warm query caused no extra wake" || fail "unexpected extra wake"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "PROXY INTEGRATION TEST PASSED" || { echo "PROXY TEST FAILED"; exit 1; }
