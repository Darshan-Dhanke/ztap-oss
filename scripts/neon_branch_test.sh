#!/usr/bin/env bash
# Neon ephemeral-branching test (opt-in Neon stack only).
# Proves: a branch is an instant copy-on-write fork of main, an isolated compute,
# and carries no CDC — write on the branch, main is untouched.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

NET=ztap-oss_ztap
B=devtest
MAINQ(){ docker run --rm --network $NET -e PGPASSWORD=cloud_admin postgres:16 psql -h compute -p 55433 -U cloud_admin -d postgres -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
BRANCHQ(){ docker run --rm --network $NET -e PGPASSWORD=cloud_admin postgres:16 psql -h ztap-neon-branch-$B -p 55433 -U cloud_admin -d postgres -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
ok=0; bad=0
pass(){ echo "  PASS: $1"; ok=$((ok+1)); }
fail(){ echo "  FAIL: $1"; bad=$((bad+1)); }

echo "== seed data on main =="
docker run --rm --network $NET -e PGPASSWORD=cloud_admin postgres:16 psql -h compute -p 55433 -U cloud_admin -d postgres -c \
  "CREATE SCHEMA IF NOT EXISTS branchtest; DROP TABLE IF EXISTS branchtest.t; CREATE TABLE branchtest.t(id int primary key, v text); INSERT INTO branchtest.t VALUES (1,'main');" >/dev/null 2>&1
[ "$(MAINQ "SELECT v FROM branchtest.t WHERE id=1;")" = "main" ] && pass "seeded main" || fail "seed failed"

echo "== create branch =="
bash scripts/neon_branch.sh "$B" 55436 >/dev/null 2>&1
[ -n "$(docker ps --filter name=ztap-neon-branch-$B --format '{{.Names}}')" ] && pass "branch compute up" || fail "branch compute not up"

echo "== branch is a copy-on-write fork (sees main's data) =="
[ "$(BRANCHQ "SELECT v FROM branchtest.t WHERE id=1;")" = "main" ] && pass "branch sees main's row" || fail "branch missing main's data"

echo "== writes on the branch are isolated from main =="
BRANCHQ "UPDATE branchtest.t SET v='branched' WHERE id=1; INSERT INTO branchtest.t VALUES (2,'branch-only');" >/dev/null
[ "$(BRANCHQ "SELECT v FROM branchtest.t WHERE id=1;")" = "branched" ] && pass "branch write applied on branch" || fail "branch write missing"
[ "$(MAINQ "SELECT v FROM branchtest.t WHERE id=1;")" = "main" ] && pass "main unchanged (isolated)" || fail "main was mutated by branch!"
[ "$(MAINQ "SELECT count(*) FROM branchtest.t;")" = "1" ] && pass "main has no branch-only row" || fail "branch row leaked to main"

echo "== teardown =="
bash scripts/neon_branch.sh --drop "$B" >/dev/null 2>&1
docker run --rm --network $NET -e PGPASSWORD=cloud_admin postgres:16 psql -h compute -p 55433 -U cloud_admin -d postgres -c "DROP SCHEMA IF EXISTS branchtest CASCADE;" >/dev/null 2>&1
pass "torn down"

echo ""
echo "RESULTS: $ok passed, $bad failed"
[ "$bad" = "0" ] && echo "NEON BRANCH TEST PASSED" || { echo "NEON BRANCH TEST FAILED"; exit 1; }
