#!/usr/bin/env bash
# Runs Q1 on Kùzu (experiments/SOA-GDBMS/kuzu_run.py), starter_node=383,
# min_length=2, over the bundled LG.csv/LG_V.csv.
#
# Capped at --max-len 2 deliberately, not a typo: Kùzu's own Cypher
# formulation of Q1 checks trail/monotonicity/region/range only *after*
# fully materializing each candidate path (no early filtering), so it has
# the same combinatorial-blowup character as the DuckDB baseline -- when
# tried at length 3, it was still running (3.7GB+ RSS and climbing) after
# 3+ minutes and had to be killed. If you want to push further anyway
# (e.g. on beefier hardware), raise MAX_LEN below, but expect it to take a
# while and use a lot of memory. See README.md.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-2}"
KUZU_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

rm -rf "$KUZU_DIR/kuzu_db"
python3 "$KUZU_DIR/kuzu_run.py" \
  --nodes "$DATASET_DIR/LG_V.csv" \
  --edges "$DATASET_DIR/LG.csv" \
  --query q1 \
  --starter 383 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source psutil \
  --fresh-db \
  --csv "results/kuzu_q1.csv"
rm -rf "$KUZU_DIR/kuzu_db"

echo "Completed -- results in results/kuzu_q1.csv"
