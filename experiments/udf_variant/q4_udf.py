"""Q4's selective aggregate (see `experiments/q4_length_sweep/q4_aggregate.py`
for the SQL source of truth), hand-translated 1:1 into Python UDFs for the
udf-variant ablation. `D` is a JSON string throughout.

`is_viable_d` depends on `bound` (q4_aggregate.py's own MAX_MIN_BOUND=20.0
for the generic toy dataset, or 1_209_600_000 -- two weeks in ms -- for the
real LDBC100 timestamp-cohesion semantics), so this module exposes
`make_udfs(bound=...)` returning the five functions bound to that value,
mirroring `q4_aggregate(bound=...)`'s own factory shape."""
from __future__ import annotations

import json
from types import SimpleNamespace

MAX_MIN_BOUND = 20.0


def init_d():
    return json.dumps({"edge_ids": [], "max_weight": -1e308, "min_weight": 1e308})


def update_d(d_json, from_state, to_state, e):
    d = json.loads(d_json)
    d["edge_ids"] = d["edge_ids"] + [e["edge_id"]]
    d["max_weight"] = max(d["max_weight"], e["weight"])
    d["min_weight"] = min(d["min_weight"], e["weight"])
    return json.dumps(d)


def is_viable_d_final(d_json):
    return True


def finalize_d(d_json):
    return d_json


def make_udfs(bound: float = MAX_MIN_BOUND):
    def is_viable_d(d_json, from_state, to_state, e):
        d = json.loads(d_json)
        ok = e["edge_id"] not in d["edge_ids"]
        ok = ok and (max(d["max_weight"], e["weight"]) - min(d["min_weight"], e["weight"]) <= bound)
        return ok

    return SimpleNamespace(
        init_d=init_d, update_d=update_d, is_viable_d=is_viable_d,
        is_viable_d_final=is_viable_d_final, finalize_d=finalize_d,
    )
