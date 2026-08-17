"""Q2's selective aggregate (see `experiments/q2_length_sweep/q2_aggregate.py`
for the SQL source of truth), hand-translated 1:1 into Python UDFs for the
udf-variant ablation. `D` is a JSON string throughout."""
from __future__ import annotations

import json


def init_d():
    return json.dumps({"edge_ids": [], "last_color": None, "constraint_done": False})


def update_d(d_json, from_state, to_state, e):
    d = json.loads(d_json)
    d["edge_ids"] = d["edge_ids"] + [e["edge_id"]]
    d["constraint_done"] = d["constraint_done"] or (
        d["last_color"] is not None and e["color"] == d["last_color"])
    d["last_color"] = e["color"]
    return json.dumps(d)


def is_viable_d(d_json, from_state, to_state, e):
    d = json.loads(d_json)
    return e["edge_id"] not in d["edge_ids"]


def is_viable_d_final(d_json):
    return json.loads(d_json)["constraint_done"]


def finalize_d(d_json):
    return d_json
