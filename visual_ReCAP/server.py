#!/usr/bin/env python3
import csv
import json
import os
import re
import subprocess
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
SRC_ROOT = ROOT / "ReCAP_src"
DATASET_DIR = SRC_ROOT / "simple_dataset"
PY_FILE = SRC_ROOT / "py_file" / "UDF.py"
LG_CSV = DATASET_DIR / "LG.csv"
LG_V_CSV = DATASET_DIR / "LG_V.csv"
NFA_CSV = DATASET_DIR / "nfa.csv"
NFA_NODES_CSV = DATASET_DIR / "nfa_nodes.csv"
GENERATED_START = "# BEGIN GENERATED FUNCTIONS"
GENERATED_END = "# END GENERATED FUNCTIONS"
PREVIEW_COLUMNS = ["edge_id", "src", "dst", "label", "amount", "risk_score", "anomaly"]
MAX_RUN_SECONDS = 15


def parse_simple_regex(regex: str):
    normalized = regex.strip()
    if not normalized:
        raise ValueError("Regex cannot be empty.")
    if re.search(r"[()|]", normalized):
        raise ValueError("This first backend version supports concatenation with optional postfix '+' only.")

    tokens = normalized.split()
    if len(tokens) == 1:
        tokens = re.findall(r"[A-Za-z0-9_]+(?:\+)?", normalized)

    if not tokens or "".join(tokens).replace("+", "") == "":
        raise ValueError("Could not parse the regex into labels.")

    parsed = []
    for token in tokens:
        if not re.fullmatch(r"[A-Za-z0-9_]+(?:\+)?", token):
            raise ValueError(f"Unsupported token '{token}'. Use labels like purchase or purchase+.")
        quantifier = "+" if token.endswith("+") else ""
        label = token[:-1] if quantifier else token
        parsed.append({"label": label, "quantifier": quantifier})
    return parsed


def write_nfa_files(regex: str):
    tokens = parse_simple_regex(regex)
    final_state = len(tokens)

    with NFA_CSV.open("w", newline="") as edge_file:
        writer = csv.writer(edge_file)
        writer.writerow(["from_state", "to_state", "label"])
        for index, token in enumerate(tokens):
            writer.writerow([index, index + 1, token["label"]])
            if token["quantifier"] == "+":
                writer.writerow([index + 1, index + 1, token["label"]])

    with NFA_NODES_CSV.open("w", newline="") as node_file:
        writer = csv.writer(node_file)
        writer.writerow(["id", "type"])
        writer.writerow([0, "initial"])
        writer.writerow([final_state, "accepting"])

    return {
        "nodes": [
            {"id": 0, "type": "initial"},
            {"id": final_state, "type": "accepting"},
        ],
        "edges": list(csv.DictReader(NFA_CSV.open())),
    }


def preview_dataset(limit: int):
    rows = []
    with LG_CSV.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader):
            if index >= limit:
                break
            rows.append({column: row[column] for column in PREVIEW_COLUMNS})
    return {"columns": PREVIEW_COLUMNS, "rows": rows}


def build_generated_functions(payload):
    mode = payload["mode"]
    if mode == "naive":
        naive_code = payload["naiveCode"].strip()
        return "\n\n".join(
            [
                "def init_d():\n    return []",
                "def update_d(D, P_nfa_state, T_to_state, Edge):\n    return D + [Edge]",
                "def is_viable_d(D, P_nfa_state, T_to_state, Edge):\n    return True",
                "def finalize_d(D):\n    return D",
                naive_code,
            ]
        )

    advanced = payload["advancedCode"]
    return "\n\n".join(
        [
            advanced["init_d"].strip(),
            advanced["update_d"].strip(),
            advanced["is_viable_d"].strip(),
            advanced["finalize_d"].strip(),
            advanced["is_viable_d_final"].strip(),
        ]
    )


def patch_udf_file(generated_functions: str):
    content = PY_FILE.read_text()
    pattern = re.compile(
        rf"{re.escape(GENERATED_START)}.*?{re.escape(GENERATED_END)}",
        re.DOTALL,
    )
    replacement = f"{GENERATED_START}\n{generated_functions}\n{GENERATED_END}"
    updated = pattern.sub(replacement, content)
    PY_FILE.write_text(updated)


def run_recap(payload):
    write_nfa_files(payload["regex"])
    patch_udf_file(build_generated_functions(payload))

    command = [
        sys.executable,
        str(PY_FILE),
        "--edges",
        str(LG_CSV),
        "--nodes",
        str(LG_V_CSV),
        "--nfanodes",
        str(NFA_NODES_CSV),
        "--nfa",
        str(NFA_CSV),
        "--start",
        str(int(payload["startNode"])),
        "--min-length",
        str(int(payload["lowerBound"])),
        "--max-length",
        str(int(payload["upperBound"])),
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    combined_output = "\n".join(part for part in [result.stdout.strip(), result.stderr.strip()] if part)
    match = re.search(
        r"Number of paths \[(\d+), (\d+)\] from (\d+) are: (\d+)",
        combined_output,
    )
    return {
        "ok": result.returncode == 0 and bool(match),
        "count": int(match.group(4)) if match else None,
        "message": match.group(0) if match else combined_output or "ReCAP run finished.",
        "stdout": combined_output,
    }

class ReCAPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/dataset-preview":
            limit = int(parse_qs(parsed.query).get("limit", ["10"])[0])
            self._send_json(preview_dataset(limit))
            return
        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")

        try:
            if parsed.path == "/api/generate-nfa":
                self._send_json(write_nfa_files(payload["regex"]))
                return
            if parsed.path == "/api/run-recap":
                result = run_recap(payload)
                self._send_json(result, status=200 if result["ok"] else 500)
                return
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=400)
            return

        self._send_json({"ok": False, "error": "Unknown endpoint"}, status=404)


def main():
    host = os.environ.get("RECAP_HOST", "127.0.0.1")
    port = int(os.environ.get("RECAP_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), ReCAPRequestHandler)
    print(f"Serving ReCAP UI at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
