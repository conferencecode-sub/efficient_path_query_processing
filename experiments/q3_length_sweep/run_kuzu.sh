#!/usr/bin/env bash
# Runs Q3 on Kùzu (experiments/SOA-GDBMS/kuzu_run.py), starter_node=9,
# min_length=2, over the bundled generic nodes.csv/edges.csv.
#
# Unlike Q2, this does NOT crash -- confirmed at length up to 4 (253/2054/
# 10488 paths, matching every other engine exactly). Q3's Cypher uses a
# `list_reduce` short-circuit (CASE WHEN w > acc THEN w ELSE NULL END)
# rather than Q2's PROPERTIES/RANGE/ANY chain, which is apparently what
# avoids the engine bug documented in ../q2_length_sweep/README.md.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-4}"
KUZU_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

rm -rf "$KUZU_DIR/kuzu_db"
python3 "$KUZU_DIR/kuzu_run.py" \
  --nodes "$DATASET_DIR/nodes.csv" \
  --edges "$DATASET_DIR/edges.csv" \
  --query q3 \
  --starter 9 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source psutil \
  --fresh-db \
  --csv "results/kuzu_q3.csv"
rm -rf "$KUZU_DIR/kuzu_db"

echo "Completed -- results in results/kuzu_q3.csv"
