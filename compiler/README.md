# ReCAP Compiler

Implementation of the compiler specified in `../requirements/compiler_reqs.md`:
regex + selective aggregate + property graph -> optimized recursive SQL,
executed on DuckDB. See `CHECKLIST.md` for what's built so far and what's
next.

## Layout

```
compiler/
├── CHECKLIST.md              build progress, one row per pipeline stage
├── pyproject.toml
├── src/recap_compiler/
│   ├── errors.py              error taxonomy (Section 7 of the spec)
│   ├── ingestion.py           Stage A: load graph, select start vertices
│   ├── regex_frontend.py      Stage B: regex -> epsilon-free NFA
│   └── transitions.py         Stage C: NFA -> T(from_state,to_state,label), q0, Q_F
└── tests/                     one test file per module above
```

## Setup

```bash
cd compiler
pip install -e '.[dev]'
```

## Running tests

```bash
cd compiler
python3 -m pytest tests/ -v
```

## Status

A (ingestion), B (regex frontend), and C (NFA -> transitions relation) are
implemented and tested. Everything downstream (selective-aggregate frontend,
SQL generation, execution, the optimizer, the workbench UI) is designed but
not yet built -- see `CHECKLIST.md` for the order and rationale.
