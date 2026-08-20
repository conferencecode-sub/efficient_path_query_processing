# ReCAP Compiler

Implementation of ReCAP ("Efficient Path Query Processing in Relational
Database Systems"): a compiler that takes a path query -- a label regex plus
a *selective aggregate* over edge/vertex properties -- and a property graph,
and produces optimized recursive SQL that early-filters non-viable paths
during exploration instead of generating every path first and filtering
afterward. Runs on DuckDB.

## Quick start

```bash
cd compiler
pip install -e '.[dev]'
python3 -m pytest tests/ -v          # 162 tests, all on the bundled sample dataset
python3 demo_pipeline.py             # see it run end to end, no browser needed
```

For the browser workbench instead of the script:

```bash
cd compiler
pip install -e '.[ui]'
streamlit run webapp/app.py
```

See `compiler/README.md` for what each of these actually does, and
`compiler/CHECKLIST.md` for build status (one row per pipeline stage) and
implementation notes for anyone extending this.

## Where things are

- **`compiler/`** -- the compiler itself, fully self-contained (bundles its
  own small sample dataset in `compiler/sample_data/`, no external data
  needed). Start here.
- **`new_compiler_requirements/compiler_reqs.md`** -- the functional
  requirements spec the compiler is built against.
- **`ReCAP/`** -- an earlier prototype's code (`q1`/`q2`/`q3` query scripts)
  and the real-graph datasets used by the larger reproduction pipeline under
  `experiments/`. This is a separate git repository and is **not** required
  for the compiler itself (see Quick start above) -- only for reproducing
  the experiments in `experiments/`.
- **`alternative_explorations/`** -- standalone experiments exploring
  compatibility with segmented/non-forward path evaluation strategies;
  design in `navigation_style_experiment.md`, runnable implementation in
  `navigation_experiment/` (see its own README).
- **`experiments/`** -- the full real-data evaluation campaign (multiple
  datasets/engines, up to ~100M edges). Has its own per-experiment READMEs;
  out of scope for this top-level README since it depends on large external
  datasets not bundled here.
