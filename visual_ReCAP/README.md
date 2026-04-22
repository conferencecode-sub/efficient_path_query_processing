# ReCAP Query Workbench

This project is a local prototype for experimenting with ReCAP-style path queries over the sample graph dataset in `ReCAP_src/simple_dataset`.

It has two parts:

- a browser UI for entering regexes, editing ReCAP UDFs, previewing data, and running queries
- a small local Python backend that generates NFA CSV files, injects UDF code into `UDF.py`, and runs DuckDB

## How To Run

From the project root:

```bash
python3 -m pip install -r requirements.txt
python3 server.py
```

Then open:

[http://127.0.0.1:8000](http://127.0.0.1:8000)

To expose the UI from a remote server, bind to all interfaces:

```bash
RECAP_HOST=0.0.0.0 RECAP_PORT=8000 python3 server.py
```

Then open the server URL, for example `http://SERVER_HOSTNAME:8000`.

Important:

- Use `python3 server.py`.
- Do not use `python3 -m http.server 8000`; the UI depends on API routes in `server.py`.
- Confirm the visible build stamp in the UI after refreshing.

## Project Layout

- `index.html`: frontend structure
- `styles.css`: frontend styling
- `script.js`: frontend behavior and API calls
- `server.py`: local backend for dataset preview, NFA generation, and ReCAP execution
- `ReCAP_src/py_file/UDF.py`: DuckDB execution script with a generated-function injection section
- `ReCAP_src/simple_dataset/LG.csv`: sample edge dataset
- `ReCAP_src/simple_dataset/LG_V.csv`: sample node dataset
- `ReCAP_src/simple_dataset/nfa.csv`: generated NFA transitions
- `ReCAP_src/simple_dataset/nfa_nodes.csv`: generated NFA node metadata

## What Works

- The UI can preview 10 rows from `LG.csv`.
- The UI can accept a simple regex and render a corresponding NFA.
- The NFA visualization now matches the generated transition table for postfix `+`. For example, `purchase+` renders `q0 -> q1` and `q1 -> q1`, with `q1` accepting.
- The UI shows the generated `nfa.csv` transition table under the NFA visualization.
- The backend writes generated NFA files to:
  - `ReCAP_src/simple_dataset/nfa.csv`
  - `ReCAP_src/simple_dataset/nfa_nodes.csv`
- The naive Python editor supports `is_viable_d_final(D)`.
- The advanced editor exposes five editable functions:
  - `init_d`
  - `update_d`
  - `is_viable_d`
  - `finalize_d`
  - `is_viable_d_final`
- `update_d` and `is_viable_d` have `With CASE` / `Without CASE` helper buttons.
- `With CASE` generates branch stubs from the current NFA transition rows, using `P_nfa_state` and `T_to_state`.
- `Run ReCAP` injects the current function code into `UDF.py`, regenerates the NFA files, and runs DuckDB locally.
- The result is shown as:

```text
Number of paths [l, u] from s are: X
```

## What Is Partial

- Regex support is intentionally simple.
- The current parser supports basic labels and postfix `+`, with limited support for concatenated tokens.
- The visual NFA and generated CSVs are aligned for the supported regex subset.
- The UI table only displays `nfa.csv`; `nfa_nodes.csv` is still generated on disk but not shown in the NFA panel.
- The Python UDF editors are textareas, not full IDE/editor components.
- The `With CASE` templates are scaffolds. They insert the correct transition branches, but users still need to fill in meaningful logic.
- The browser-side Python runner uses a small sample `D`; the full DuckDB run uses edges from `LG.csv`.

## What Does Not Work Yet

- Full regex support is not implemented yet.
- Parentheses, alternation such as `a|b`, and rich Thompson construction are not production-ready.
- Automatic optimized SQL generation is not implemented yet.
- Dictionary flattening and function inlining are not implemented yet.
- There is no full transition-table authoring UI yet where each row independently stores label, `is_viable_d`, and `update_d`.
- There is no robust validation that user-written Python functions have the correct signatures or return types before execution.
- Long or high-branching queries can still be slow with the standard construction.

## Data Available To UDFs

Even if the data preview shows only a subset of columns, the backend passes these `LG.csv` fields into the Python UDF edge dictionaries:

- `edge_id`
- `timestamp_ms`
- `src`
- `dst`
- `amount`
- `label`
- `location_region`
- `purchase_pattern`
- `age_group`
- `risk_score`
- `anomaly`

The backend also provides convenience aliases:

- `id` maps to `edge_id`
- `time` maps to `timestamp_ms`
- `day` currently maps to `hour_of_day`

## Execution Notes

- Generated functions are inserted between these markers in `ReCAP_src/py_file/UDF.py`:

```python
# BEGIN GENERATED FUNCTIONS
# END GENERATED FUNCTIONS
```

- The backend has a timeout to prevent very expensive queries from hanging indefinitely.
- Queries such as `purchase+` from high-degree vertices can explode quickly with larger upper-hop bounds.
- If a label does not exist in `LG.csv`, for example `Domestic`, the query should return `0` quickly.

## Recommended Next Steps

- Add a real transition-table editor where each NFA row can carry custom `update_d` and `is_viable_d` behavior.
- Implement full regex parsing and Thompson construction.
- Add signature and return-type validation for the five UDF functions.
- Push common pruning logic into `is_viable_d` templates so ReCAP can prune during recursion, not only at finalization.
- Add a generated SQL preview once the standard construction is stable.
- Later, add optimized SQL generation passes such as dictionary flattening and function inlining.
