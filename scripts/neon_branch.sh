#!/usr/bin/env bash
# Create an ephemeral Neon branch — a copy-on-write fork of the main timeline
# plus a throwaway compute attached to it. This is the dev/test/migration story:
# branches are instant, isolated, and carry NO CDC (CDC stays on main, which is
# the system of record). Drop a branch with: scripts/neon_branch.sh --drop <name>
#
# Usage:
#   scripts/neon_branch.sh <name> [host_port]     # create (default port 55435)
#   scripts/neon_branch.sh --drop <name>          # tear down the branch compute
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

PS=http://localhost:19898            # pageserver HTTP API (host-mapped)
NET=ztap-oss_ztap
py(){ python -c "import sys,json;d=json.load(sys.stdin);$1"; }

if [ "${1:-}" = "--drop" ]; then
  NAME="${2:?usage: neon_branch.sh --drop <name>}"
  docker rm -f "ztap-neon-branch-${NAME}" >/dev/null 2>&1 && echo "dropped branch compute ztap-neon-branch-${NAME}" || echo "no such branch compute"
  exit 0
fi

NAME="${1:?usage: neon_branch.sh <name> [host_port]}"
PORT="${2:-55435}"

tenant=$(curl -s "$PS/v1/tenant" | py "print(d[0]['id'])")
# main timeline = the one with no ancestor
main_tl=$(curl -s "$PS/v1/tenant/$tenant/timeline" | py "print([t['timeline_id'] for t in d if not t.get('ancestor_timeline_id')][0])")
branch_tl=$(python -c "import secrets;print(secrets.token_hex(16))")
echo "tenant=$tenant  main_timeline=$main_tl  new_branch=$branch_tl"

echo "creating copy-on-write branch timeline ..."
curl -s -X POST "$PS/v1/tenant/$tenant/timeline/" -H 'content-type: application/json' \
  -d "{\"new_timeline_id\":\"$branch_tl\",\"pg_version\":16,\"ancestor_timeline_id\":\"$main_tl\"}" \
  | py "print('  branch timeline created:', d.get('timeline_id'))"

docker rm -f "ztap-neon-branch-${NAME}" >/dev/null 2>&1 || true
echo "starting throwaway compute for branch '$NAME' on host port $PORT (no CDC) ..."
docker run -d --name "ztap-neon-branch-${NAME}" --network "$NET" \
  -e TENANT_ID="$tenant" -e TIMELINE_ID="$branch_tl" -e PG_VERSION=16 \
  -p "${PORT}:55433" --entrypoint /shell/compute.sh ztap-neon-compute >/dev/null

echo "waiting for the branch compute to accept connections ..."
for i in $(seq 1 40); do
  if docker run --rm --network "$NET" -e PGPASSWORD=cloud_admin postgres:16 \
       pg_isready -h "ztap-neon-branch-${NAME}" -p 55433 -U cloud_admin >/dev/null 2>&1; then
    echo ""
    echo "branch '$NAME' ready:"
    echo "  host:        localhost:${PORT}  (cloud_admin / postgres)"
    echo "  in-network:  ztap-neon-branch-${NAME}:55433"
    echo "  timeline:    $branch_tl  (forked from main $main_tl)"
    echo "  drop with:   scripts/neon_branch.sh --drop $NAME"
    exit 0
  fi
  sleep 3
done
echo "branch compute did not become ready in time; check: docker logs ztap-neon-branch-${NAME}"
exit 1
