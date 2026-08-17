#!/usr/bin/env bash
# Example: run Q1 (regex + selective aggregate) against the bundled sample
# dataset. Kùzu is embedded (no server) -- this runs directly, no setup
# beyond `pip install kuzu`.
set -euo pipefail

DATASET_DIR="../../ReCAP/simple_dataset"
OUTPUT_DIR="results"
mkdir -p "$OUTPUT_DIR"

python3 kuzu_run.py \
  --nodes "$DATASET_DIR/LG_V.csv" \
  --edges "$DATASET_DIR/LG.csv" \
  --query q1 \
  --starter 383 \
  --min-len 2 \
  --max-len 8 \
  --warmup 1 \
  --runs 3 \
  --memory-source psutil \
  --fresh-db \
  --csv "$OUTPUT_DIR/kuzu_q1.csv"

echo "Completed -- results in $OUTPUT_DIR/kuzu_q1.csv"
