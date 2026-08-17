#!/usr/bin/env bash
# Reruns the full Q2 length-sweep pilot: new compiler (Standard + Optimized),
# old prototype (DuckDB baseline, ReCAP-inline, ReCAP-UDF), Kùzu (k=2 only
# -- see run_kuzu.sh for why), Neo4j, and Memgraph (both all lengths 2-4,
# no crash here). Writes/overwrites every CSV under results/. See README.md
# for the results table. Requires Neo4j and Memgraph running -- see
# experiments/SOA-GDBMS/run_neo4j.sh/run_memgraph.sh for how to start them.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "############################################################"
echo "# ReCAP-new (Standard + Optimized)"
echo "############################################################"
./run_new_compiler.sh

echo
echo "############################################################"
echo "# Old prototype (duckdb-baseline, recap-inline, recap-udf)"
echo "############################################################"
./run_old_prototype.sh

echo
echo "############################################################"
echo "# Kùzu (length 2 only -- see run_kuzu.sh)"
echo "############################################################"
./run_kuzu.sh

echo
echo "############################################################"
echo "# Neo4j"
echo "############################################################"
./run_neo4j.sh

echo
echo "############################################################"
echo "# Memgraph"
echo "############################################################"
./run_memgraph.sh

echo
echo "All done -- see results/*.csv"
