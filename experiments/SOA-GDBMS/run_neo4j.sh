#!/usr/bin/env bash
# Example: run Q1 (regex + selective aggregate) against the bundled sample
# dataset. Requires a running Neo4j server reachable at $NEO4J_URI.
#
# 2026-08-14: a real local Neo4j Community 5.24.0 install exists at
# ~/neo4j/neo4j-community-5.24.0 (auth disabled in its conf, but the
# driver still accepts any credentials). Start/stop it with:
#   export JAVA_HOME="$HOME/java/JVM21"; export PATH="$JAVA_HOME/bin:$PATH"
#   cd ~/neo4j/neo4j-community-5.24.0 && ./bin/neo4j start   # (or stop)
# It's a bare process, not a container, so --memory-source is "local"
# (matched by process name "java") rather than "docker".
set -euo pipefail

DATASET_DIR="../../ReCAP/simple_dataset"
OUTPUT_DIR="results"
mkdir -p "$OUTPUT_DIR"

NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-password}"

python3 neo4j_run.py \
  --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASSWORD" \
  --nodes "$DATASET_DIR/LG_V.csv" \
  --edges "$DATASET_DIR/LG.csv" \
  --query q1 \
  --starter 383 \
  --min-len 2 \
  --max-len 8 \
  --warmup 1 \
  --runs 3 \
  --memory-source local --process-name java \
  --fresh-db \
  --csv "$OUTPUT_DIR/neo4j_q1.csv"

echo "Completed -- results in $OUTPUT_DIR/neo4j_q1.csv"
