#!/usr/bin/env bash
# Runs Q1 on Memgraph (experiments/SOA-GDBMS/memgraph_run.py), starter_node
# =383, min_length=2, over the bundled LG.csv/LG_V.csv.
#
# Capped at --max-len 2 deliberately, same reason as run_neo4j.sh: no early
# filtering + this regex/dataset's branching factor makes length>=3
# impractical -- confirmed directly: length=3 alone took ~71.4s for a
# single execution (worse than Neo4j's ~14.3s for the same query).
# Requires the `memgraph` docker container running (docker start memgraph)
# -- note it publishes bolt on host port 7688, not 7687 (Neo4j's port).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-2}"
SOA_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

python3 "$SOA_DIR/memgraph_run.py" \
  --uri bolt://localhost:7688 \
  --nodes "$DATASET_DIR/LG_V.csv" \
  --edges "$DATASET_DIR/LG.csv" \
  --query q1 \
  --starter 383 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source docker --container memgraph \
  --fresh-db \
  --csv "results/memgraph_q1.csv"

echo "Completed -- results in results/memgraph_q1.csv"
