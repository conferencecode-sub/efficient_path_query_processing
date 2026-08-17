#!/usr/bin/env bash
# Runs Q2 on Kùzu (experiments/SOA-GDBMS/kuzu_run.py), starter_node=9,
# min_length=2, over the bundled generic nodes.csv/edges.csv.
#
# Capped at --max-len 2 deliberately, not a typo: Kùzu 0.11.2 segfaults
# (SIGSEGV, core dumped) on this exact query at length>=3, confirmed with
# and without an added is_trail(path) clause -- isolated to the
# PROPERTIES(RELS(path),'color')/RANGE/ANY color-adjacency check once the
# intermediate row count reaches ~12k+ (is_trail(path) alone, without that
# chain, does NOT crash at the same length). This is a real Kùzu engine bug,
# not a query-logic issue -- see README.md for the isolation steps. Only
# length=2 is usable here until/unless a Kùzu version fixes it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-2}"
KUZU_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

rm -rf "$KUZU_DIR/kuzu_db"
python3 "$KUZU_DIR/kuzu_run.py" \
  --nodes "$DATASET_DIR/nodes.csv" \
  --edges "$DATASET_DIR/edges.csv" \
  --query q2 \
  --starter 9 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source psutil \
  --fresh-db \
  --csv "results/kuzu_q2.csv"
rm -rf "$KUZU_DIR/kuzu_db"

echo "Completed -- results in results/kuzu_q2.csv"
