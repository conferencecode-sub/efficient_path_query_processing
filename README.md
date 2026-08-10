# ReCAP Compiler

Repo for the "ReCAP" paper ("Efficient Path Query Processing in Relational
Database Systems") SIGMOD 2027 Round-2 revision. ReCAP compiles a path query
(label regex + selective aggregate over edge/vertex properties) into
optimized recursive SQL that early-filters "doomed" paths during exploration.

## Where things are

- **`compiler/`** -- the compiler implementation, built against the spec
  below. Start here: `compiler/README.md` (setup + layout) and
  `compiler/CHECKLIST.md` (build progress, one row per pipeline stage).
- **`new_compiler_requirements/compiler_reqs.md`** -- the functional
  requirements spec the compiler is built against; each requirement traces
  to the reviewer concern it answers (Section 11). (This folder was
  `requirements/` until 2026-08-07.) The same folder also has
  `recap_compiler_requirements_FULL.md`, a newer draft with two differences:
  it un-demotes the negative-stability verifier (Module H) back into the
  main spec, and it adds an all-new optional Part II (Module J: LLM-assisted
  selective-aggregate authoring) -- not yet reconciled with `compiler_reqs.md`.
- **`info_background/`** -- the paper (`Efficient_Path_Query_Processing_RelDBMS-2.pdf`)
  and the Round-2 reviews + author feedback (`new_reviews.pdf`).
- **`old_requirements/README.md`** -- the old prototype's README (browser UI +
  Python/DuckDB backend). Documents what did/didn't work; superseded by
  `compiler/`.
- **`ReCAP/`** -- the old prototype's code (`q1`/`q2`/`q3` query scripts +
  `simple_dataset`, the sample graph used throughout this repo). Has its own
  nested git repo.
- **`alternative_explorations/`** -- standalone experiments answering specific
  reviewer critiques that don't belong in the compiler itself. Currently: a
  fragment-splitting/navigation-style experiment for R4.O2 -- design in
  `navigation_style_experiment.md`, runnable implementation in
  `navigation_experiment/` (see its own README for how to run it).

## Quick start: compiler

```bash
cd compiler
pip install -e '.[dev]'
python3 -m pytest tests/ -v
```

To see it running instead of just tests passing, either run
`python3 demo_pipeline.py` (script), or `pip install -e '.[ui]' && streamlit
run webapp/app.py` (browser UI). See `compiler/README.md` for both, and
`compiler/CHECKLIST.md` for what's implemented so far vs. planned next.
