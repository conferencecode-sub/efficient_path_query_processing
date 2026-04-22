# Efficient Path Query Processing in Relational Database Systems

ReCAP is a system for efficiently evaluating **Regular Expression Path Queries (RPQs) with property constraints** on graphs. It leverages DuckDB's recursive SQL and Nondeterministic Finite Automata (NFA) to match paths that satisfy both structural (regex) and data-level (stateful) constraints.

## Overview

Graph path queries often need to enforce constraints beyond simple connectivity — e.g., "find all paths where edge weights are strictly increasing" or "paths that alternate between two colors." ReCAP handles these by encoding constraint state as JSON objects propagated through recursive SQL queries.

Four queries are implemented:

| Query | Constraint | Directory |
|-------|-----------|-----------|
| Q1 — General ReCAP | Combined property and regex constraints | `q1/` |
| Q2 — Two-Color Trail | Path must have two consecutive adjacent edges with same color | `q2/` |
| Q3 — Monotonic Trail | Edge timestamps must be strictly increasing | `q3/` |
| Q4 — Max-Min Trail | Difference between max and min edge tiemestamp must stay within a bound | `q4/` |

Each query has three variants:
- **DuckDB** — baseline pure DuckDB implementation (`duckdb_<query>.py`)
- **Inline** — ReCAP with constraints embedded directly in SQL `WHERE` clauses (`recap_<query>_inline.py`)
- **UDF** — ReCAP with constraints implemented as Python User-Defined Functions (`recap_<query>_UDF.py`)

## Requirements

- Python 3.8+
- DuckDB 1.2.1

Install Python dependencies:

```bash
bash dependencies.bash
```

## Data Format

Queries take four CSV inputs:

**nodes.csv** — graph nodes
```
id,name,label
70,70,Start
81,81,End
```

**edges.csv** — graph edges
```
src,dst,label,weight,color
1,3,e,1,magenta
```

**nfa_nodes.csv** — NFA states
```
id,type
0,initial
0,accepting
```

**nfa.csv** — NFA transitions
```
from_state,to_state,label
0,0,e
```

A sample dataset is provided in `simple_dataset/`.

## Usage

```bash
python3 <query_script>.py \
  --nodes /path/to/nodes.csv \
  --edges /path/to/edges.csv \
  --nfanodes /path/to/nfa_nodes.csv \
  --nfa /path/to/nfa.csv \
  --index True
```

Example with the sample dataset:

```bash
python3 q3/recap_monotonic_trail_inline.py \
  --nodes simple_dataset/nodes.csv \
  --edges simple_dataset/edges.csv \
  --nfanodes simple_dataset/nfa_nodes.csv \
  --nfa simple_dataset/nfa.csv 
```

Once you define the paths, you can execute said query by running the .sh file:

```bash
bash q3/run_queries_py.sh
```

## Project Structure

```
ReCAP/
├── q1/                    # General ReCAP query (combined monotonicity + trail)
├── q2/                    # Two-Color Trail query
├── q3/                    # Monotonic Trail query
├── q4/                    # Max-Min Trail query
├── SOA-GDBMS/             # Benchmarks against Neo4j, Memgraph, Kuzu
├── simple_dataset/        # Sample graph + NFA data
├── visual_ReCAP/          # Browser-based ReCAP workbench with NFA visualization and DuckDB query execution
└── dependencies.bash      # Python dependency installer
```

## visual_ReCAP

`visual_ReCAP/` is a local prototype for exploring ReCAP path queries with a browser UI and Python backend.
It includes:
- a front-end for entering regexes, previewing graph data, and visualizing generated NFA transitions
- a backend that generates NFA CSVs, injects Python UDF code into `UDF.py`, and runs DuckDB queries
- a sample dataset in `ReCAP_src/simple_dataset/`

Run it with:

```bash
python3 server.py
```

Then open:

```text
http://127.0.0.1:8000
```

For full details, see `visual_ReCAP/README.md`.

## Benchmarks

The `SOA-GDBMS/` directory contains scripts to benchmark ReCAP against:
- **Neo4j**
- **Memgraph**
- **Kuzu**
