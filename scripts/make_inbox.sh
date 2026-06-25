#!/usr/bin/env bash
# Create the lakehouse write surface ("inbox") for a project table. Run normal
# INSERT / UPDATE / DELETE against it from Trino/DBeaver and the reverse-watcher
# applies each change to Postgres (proj_<project>.<table>) within ~10s.
#
# The inbox is a Change-Data-Feed-enabled Delta table whose columns mirror the
# Postgres table (no bookkeeping columns — the watcher tracks progress itself).
#
# Usage: scripts/make_inbox.sh <project> <table> "<col1 type1, col2 type2, ...>"
# Example:
#   scripts/make_inbox.sh demo orders "id bigint, customer varchar, amount decimal(10,2)"
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

PROJECT="${1:?usage: make_inbox.sh <project> <table> \"<cols>\"}"
TABLE="${2:?usage: make_inbox.sh <project> <table> \"<cols>\"}"
COLS="${3:?provide column defs matching the Postgres table, e.g. \"id bigint, customer varchar, amount decimal(10,2)\"}"

SCHEMA="proj_${PROJECT}"
INBOX="${TABLE}_inbox"
LOC="s3://warehouse/${PROJECT}/${INBOX}"

T(){ docker exec ztap-trino trino --catalog delta --execute "$1" 2>&1 | grep -vE 'terminal|jline|Log logr'; }

echo "creating CDF-enabled inbox delta.${SCHEMA}.${INBOX} at ${LOC} ..."
T "CREATE SCHEMA IF NOT EXISTS delta.${SCHEMA} WITH (location = 's3://warehouse/${PROJECT}')" >/dev/null
T "DROP TABLE IF EXISTS delta.${SCHEMA}.${INBOX}" >/dev/null
T "CREATE TABLE delta.${SCHEMA}.${INBOX} (${COLS})
   WITH (location = '${LOC}', change_data_feed_enabled = true)"

echo ""
echo "Done. From Trino/DBeaver, run normal DML on delta.${SCHEMA}.${INBOX}:"
echo "  INSERT INTO delta.${SCHEMA}.${INBOX} VALUES (...);"
echo "  UPDATE delta.${SCHEMA}.${INBOX} SET <col>=<v> WHERE ...;"
echo "  DELETE FROM delta.${SCHEMA}.${INBOX} WHERE ...;"
echo "The reverse-watcher applies each change to Postgres ${SCHEMA}.${TABLE} within ~10s."
