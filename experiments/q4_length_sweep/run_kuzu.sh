#!/usr/bin/env bash
# Runs Q4 on Kùzu (experiments/SOA-GDBMS/kuzu_run.py), starter_node=9,
# min_length=2, over the bundled generic nodes.csv/edges.csv.
#
# No crash here (like Q3, unlike Q2), so all lengths 2-4 run by default.
# Uses Q4_MAX_MIN_BOUND=20 in kuzu_run.py (fixed 2026-08-13 from a stale
# timestamp-scale constant that made the constraint a silent no-op --
# see README.md), matching q4_aggregate.py's own MAX_MIN_BOUND.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-4}"
KUZU_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

rm -rf "$KUZU_DIR/kuzu_db"
python3 "$KUZU_DIR/kuzu_run.py" \
  --nodes "$DATASET_DIR/nodes.csv" \
  --edges "$DATASET_DIR/edges.csv" \
  --query q4 \
  --starter 9 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source psutil \
  --fresh-db \
  --csv "results/kuzu_q4.csv"
rm -rf "$KUZU_DIR/kuzu_db"

echo "Completed -- results in results/kuzu_q4.csv"
