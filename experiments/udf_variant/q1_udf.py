"""Q1's selective aggregate (see `experiments/q1_length_sweep/q1_aggregate.py`
for the SQL source of truth), hand-translated 1:1 into Python UDFs for the
udf-variant ablation. `D` is a JSON string throughout, matching the old
prototype's own `py_*` convention."""
from __future__ import annotations

import json

NORMAL_LABELS = {"transfer", "purchase", "sale"}


def init_d():
    return json.dumps({
        "edge_ids": [], "last_timestamp_ms": None, "region": None,
        "max_norm_score": -1e308, "min_norm_score": 1e308,
        "last_norm_score": None, "total_amount": 0.0,
    })


def update_d(d_json, from_state, to_state, e):
    d = json.loads(d_json)
    d["edge_ids"] = d["edge_ids"] + [e["edge_id"]]
    d["last_timestamp_ms"] = e["timestamp_ms"]
    d["region"] = d["region"] if d["region"] is not None else e["location_region"]
    if e["label"] in NORMAL_LABELS:
        d["max_norm_score"] = max(d["max_norm_score"], e["risk_score"])
        d["min_norm_score"] = min(d["min_norm_score"], e["risk_score"])
        d["last_norm_score"] = e["risk_score"]
    d["total_amount"] = d["total_amount"] + e["amount"]
    return json.dumps(d)


def is_viable_d(d_json, from_state, to_state, e):
    d = json.loads(d_json)
    ok = e["edge_id"] not in d["edge_ids"]
    ok = ok and (d["last_timestamp_ms"] is None or e["timestamp_ms"] > d["last_timestamp_ms"])
    ok = ok and (d["region"] is None or e["location_region"] == d["region"])
    if e["label"] in NORMAL_LABELS:
        ok = ok and (max(d["max_norm_score"], e["risk_score"])
                     - min(d["min_norm_score"], e["risk_score"]) <= 20)
    return ok


def is_viable_d_final(d_json):
    d = json.loads(d_json)
    return d["total_amount"] >= 1000 and d["last_norm_score"] >= 40


def finalize_d(d_json):
    return d_json
