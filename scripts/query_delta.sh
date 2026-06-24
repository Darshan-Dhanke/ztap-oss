#!/usr/bin/env bash
# Query the Delta tables the sink wrote to MinIO, via Trino.
#
# Trino's file metastore is in-container and ephemeral, so a Delta table must be
# "registered" (by its storage location) before you can query it. This helper
# does that idempotently, then runs a query.
#
# Usage:
#   scripts/query_delta.sh [project] [table] ["SQL"]
# Examples:
#   scripts/query_delta.sh                       # lake/orders, SELECT *
#   scripts/query_delta.sh lake orders "SELECT count(*) FROM %T"
#     (%T is replaced with the fully-qualified table name)
set -uo pipefail

PROJECT="${1:-lake}"
TABLE="${2:-orders}"
REG="${PROJECT}_${TABLE}"                       # registered name in Trino
FQ="delta.lakehouse.${REG}"
LOC="s3://warehouse/${PROJECT}/${TABLE}"
SQL="${3:-SELECT * FROM %T ORDER BY 1}"
SQL="${SQL//%T/$FQ}"

T(){ docker exec ztap-trino trino --output-format ALIGNED --execute "$1" 2>&1 | grep -vE 'terminal|jline|Log logr'; }

echo "ensuring schema + registration for $LOC ..."
T "CREATE SCHEMA IF NOT EXISTS delta.lakehouse WITH (location = 's3://warehouse/lakehouse')" >/dev/null
# register_table errors if already registered — that's fine, ignore it
docker exec ztap-trino trino --execute \
  "CALL delta.system.register_table(schema_name => 'lakehouse', table_name => '${REG}', table_location => '${LOC}')" \
  >/dev/null 2>&1 || true

echo "query: $SQL"
echo ""
T "$SQL"
