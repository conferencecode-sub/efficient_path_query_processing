#!/usr/bin/env bash
# Runs Q1 on Neo4j (experiments/SOA-GDBMS/neo4j_run.py), starter_node=383,
# min_length=2, over the bundled LG.csv/LG_V.csv.
#
# Capped at --max-len 2 deliberately: Neo4j's Q1 Cypher (like every other
# engine's) has no early filtering, and this regex/dataset's ~59x-per-hop
# branching (documented throughout this pilot) makes it genuinely
# intractable here past length 2 -- confirmed directly: length=3 alone
# took ~14.3s for a single execution, length=4 didn't finish in 180s.
# Requires the local Neo4j install running -- see run_neo4j.sh in
# experiments/SOA-GDBMS/ for how to start/stop it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

MAX_LEN="${1:-2}"
SOA_DIR="../SOA-GDBMS"
DATASET_DIR="../../ReCAP/simple_dataset"

python3 "$SOA_DIR/neo4j_run.py" \
  --uri bolt://localhost:7687 --user neo4j --password password \
  --nodes "$DATASET_DIR/LG_V.csv" \
  --edges "$DATASET_DIR/LG.csv" \
  --query q1 \
  --starter 383 \
  --min-len 2 \
  --max-len "$MAX_LEN" \
  --warmup 1 \
  --runs 3 \
  --memory-source local --process-name java \
  --fresh-db \
  --csv "results/neo4j_q1.csv"

echo "Completed -- results in results/neo4j_q1.csv"
