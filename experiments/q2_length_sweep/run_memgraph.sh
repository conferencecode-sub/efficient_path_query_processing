#!/usr/bin/env bash
# Runs Q2 (two-color trail) on Memgraph (experiments/SOA-GDBMS/
# memgraph_run.py), starter_node=9, min_length=2, over the bundled generic
# nodes.csv/edges.csv.
#
# Works cleanly at every length 2-4, unlike Kùzu -- same relationship-
# uniqueness reasoning as Neo4j (see run_neo4j.sh).
# Requires `docker start memgraph` first (bolt on host port 7688, not 7687
# -- Neo4j already owns that one).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-4}"
SOA_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

python3 "$SOA_DIR/memgraph_run.py" \
  --uri bolt://localhost:7688 \
  --nodes "$DATASET_DIR/nodes.csv" \
  --edges "$DATASET_DIR/edges.csv" \
  --query q2 \
  --starter 9 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source docker --container memgraph \
  --fresh-db \
  --csv "results/memgraph_q2.csv"

echo "Completed -- results in results/memgraph_q2.csv"
