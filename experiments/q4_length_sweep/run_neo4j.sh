#!/usr/bin/env bash
# Runs Q4 (max-min trail) on Neo4j (experiments/SOA-GDBMS/neo4j_run.py),
# starter_node=9, min_length=2, over the bundled generic nodes.csv/edges.csv.
#
# Works cleanly at every length 2-4. Uses Q4_MAX_MIN_BOUND=20 in
# neo4j_run.py (fixed 2026-08-14 from the same stale timestamp-scale
# constant found in kuzu_run.py and the old-prototype q4 scripts -- see
# README.md), matching q4_aggregate.py's own bound.
# Requires the local Neo4j install running -- see experiments/SOA-GDBMS/
# run_neo4j.sh for how to start/stop it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-4}"
SOA_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

python3 "$SOA_DIR/neo4j_run.py" \
  --uri bolt://localhost:7687 --user neo4j --password password \
  --nodes "$DATASET_DIR/nodes.csv" \
  --edges "$DATASET_DIR/edges.csv" \
  --query q4 \
  --starter 9 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source local --process-name java \
  --fresh-db \
  --csv "results/neo4j_q4.csv"

echo "Completed -- results in results/neo4j_q4.csv"
