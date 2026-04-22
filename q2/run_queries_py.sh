# Path to the compiled executable.

UDF_PY_BIN="path/to/recap_udf.py"  # Update with actual path to UDF Python script
DDB_PY_BIN="path/to/recap_duckdb.py"  # Update with actual path to DuckDB Python script
RECAP_PY_BIN="path/to/recap_main.py"  # Update with actual path to main ReCAP Python script

# Path to graph data, as well as NFA data. Update these paths as necessary. 
# Ideally, use the nodes and edges .csv files from the dataset folder (not LG).
NODES_PATH="path/to/nodes.csv"  # Update with actual path to nodes CSV
EDGES_PATH="path/to/edges.csv"  # Update with actual path to edges CSV
NFA_NODES_PATH="path/to/nfa_nodes.csv"  # Update with actual path to NFA nodes CSV
NFA_EDGES_PATH="path/to/nfa.csv"  # Update with actual path to NFA edges CSV

# Run
$UDF_PY_BIN \
    --nodes "$NODES_PATH" \
    --edges "$EDGES_PATH" \
    --nfanodes "$NFA_NODES_PATH" \
    --nfa "$NFA_EDGES_PATH" 