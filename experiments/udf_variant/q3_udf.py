"""Q3's selective aggregate (see `experiments/q3_length_sweep/q3_aggregate.py`
for the SQL source of truth), hand-translated 1:1 into Python UDFs for the
udf-variant ablation. `D` is a JSON string throughout."""
from __future__ import annotations

import json


def init_d():
    return json.dumps({"edge_ids": [], "last_weight": None})


def update_d(d_json, from_state, to_state, e):
    d = json.loads(d_json)
    d["edge_ids"] = d["edge_ids"] + [e["edge_id"]]
    d["last_weight"] = e["weight"]
    return json.dumps(d)


def is_viable_d(d_json, from_state, to_state, e):
    d = json.loads(d_json)
    ok = e["edge_id"] not in d["edge_ids"]
    ok = ok and (d["last_weight"] is None or e["weight"] > d["last_weight"])
    return ok


def is_viable_d_final(d_json):
    return True


def finalize_d(d_json):
    return d_json
