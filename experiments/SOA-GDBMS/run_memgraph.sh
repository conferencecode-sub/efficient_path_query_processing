#!/usr/bin/env bash
# Example: run Q1 (regex + selective aggregate) against the bundled sample
# dataset. Requires the `memgraph` docker container to be running first:
#   docker start memgraph
# 2026-08-14: this container publishes its bolt port on the HOST as 7688,
# not 7687 (7687 is Neo4j's port, already in use by the real local Neo4j
# install -- see run_neo4j.sh) -- confirm with `docker ps` if this ever
# changes.
set -euo pipefail

DATASET_DIR="../../ReCAP/simple_dataset"
OUTPUT_DIR="results"
mkdir -p "$OUTPUT_DIR"

MEMGRAPH_URI="${MEMGRAPH_URI:-bolt://localhost:7688}"

python3 memgraph_run.py \
  --uri "$MEMGRAPH_URI" \
  --nodes "$DATASET_DIR/LG_V.csv" \
  --edges "$DATASET_DIR/LG.csv" \
  --query q1 \
  --starter 383 \
  --min-len 2 \
  --max-len 8 \
  --warmup 1 \
  --runs 3 \
  --memory-source docker --container memgraph \
  --fresh-db \
  --csv "$OUTPUT_DIR/memgraph_q1.csv"

echo "Completed -- results in $OUTPUT_DIR/memgraph_q1.csv"
