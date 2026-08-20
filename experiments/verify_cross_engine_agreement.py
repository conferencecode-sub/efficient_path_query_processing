#!/usr/bin/env python3
"""Cross-system result-agreement verifier for the ReCAP evaluation.

Replays the already-saved per-system result CSVs under experiments/ (nothing
is re-executed against a live database) and checks that, at every point where
more than one system reports a result count for the same query/dataset/length,
those counts agree. Point-by-point equivalence was already checked live when
each experiment was run; this script re-derives the same conclusion directly
from the CSVs on disk, in one place, so it can be rerun on demand without any
DB server being up.

Usage:
    python3 verify_cross_engine_agreement.py [--report FILE]
"""
import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent


class Checkpoint:
    __slots__ = ("family", "key", "values", "note")

    def __init__(self, family, key, note=""):
        self.family = family
        self.key = key
        self.values = defaultdict(list)  # engine_label -> [(result_or_None, error_or_None, source_path), ...]
        self.note = note

    def add(self, engine, result, error=None, source=""):
        # Append rather than overwrite: two different source files sometimes
        # reuse the same engine label (e.g. an old toy-dataset run and a
        # later real-dataset run both labeled "duckdb-baseline"). Silently
        # picking the last one written would hide that collision.
        self.values[engine].append((result, error, source))

    def same_engine_conflicts(self):
        """Engines where multiple source files disagree under the same label
        (a data-hygiene problem, distinct from a cross-system mismatch)."""
        conflicts = {}
        for engine, entries in self.values.items():
            results = {r for r, err, src in entries if r is not None}
            if len(results) > 1:
                conflicts[engine] = entries
        return conflicts

    def numeric_results(self):
        """One canonical result per engine label, only for engines whose
        entries agree with each other; engines with same_engine_conflicts
        are excluded here (reported separately)."""
        out = {}
        for engine, entries in self.values.items():
            results = {r for r, err, src in entries if r is not None}
            if len(results) == 1:
                out[engine] = next(iter(results))
        return out

    def agrees(self):
        vals = set(self.numeric_results().values())
        return len(vals) <= 1

    def failed_engines(self):
        return [e for e, entries in self.values.items() if all(r is None for r, err, src in entries)]


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def to_int(s):
    if s is None or s == "":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Family 1: generic "engine,[query,start,]{len|length},...,result,..." CSVs
# used by qN_length_sweep, e2e3_real_data, SOA-GDBMS, e7_scale_sweep.
#
# Some qN_length_sweep/results/ directories accumulated files from two
# different eras: an original small "toy dataset" pilot (kuzu/memgraph/
# neo4j/old_prototype_qN.csv, starter_node=9 or 383) and a later run of the
# new compiler against the *real* dataset (new_compiler_qN.csv), reusing the
# same directory/filename pattern without a "_real" suffix. Verified directly
# against each run_new_compiler.py's DATASET_DIR/START_VERTEX and file
# mtimes (2026-08-18 session):
#   - q2_length_sweep/new_compiler_q2(_udf).csv: Bitcoin, start=3999 -- same
#     dataset+start vertex as e2e3_real_data's Q2 files; numbers verified to
#     match exactly at every length. Reassigned there for comparison.
#   - q4_length_sweep/new_compiler_q4(_udf).csv: LDBC100, start=24189256063073
#     -- same as e2e3_real_data's Q4 files; numbers verified to match exactly
#     at every length both sides completed. Reassigned there for comparison.
#   - q3_length_sweep/new_compiler_q3(_udf).csv: Datagen-7.6, start=4398046568596
#     -- no other saved result anywhere uses this exact dataset+start vertex
#     (e2e3_real_data's Q3 is Reddit, start=31470: a different experiment).
#     Excluded from cross-checking; listed separately as "unverifiable" since
#     nothing else to compare it against exists on disk.
# ---------------------------------------------------------------------------

REASSIGN_TO_FAMILY = {
    ("q2_length_sweep", "new_compiler_q2.csv"): "e2e3_real_data",
    ("q2_length_sweep", "new_compiler_q2_udf.csv"): "e2e3_real_data",
    ("q4_length_sweep", "new_compiler_q4.csv"): "e2e3_real_data",
    ("q4_length_sweep", "new_compiler_q4_udf.csv"): "e2e3_real_data",
}

EXCLUDE_NO_COUNTERPART = {
    ("q3_length_sweep", "new_compiler_q3.csv"):
        "uses the Datagen-7.6 dataset (start=4398046568596); no other saved "
        "result on disk uses this exact dataset+start vertex to cross-check "
        "against (e2e3_real_data's Q3 is Reddit, start=31470 -- a different experiment).",
    ("q3_length_sweep", "new_compiler_q3_udf.csv"):
        "same reason as new_compiler_q3.csv (Datagen-7.6, start=4398046568596).",
}


def collect_generic_family(family_name, csv_files):
    checkpoints = defaultdict(lambda: Checkpoint(family_name, None))
    for path in csv_files:
        rows = read_csv(path)
        if not rows:
            continue
        header = rows[0].keys()
        if "engine" not in header or "result" not in header:
            continue
        length_col = "length" if "length" in header else ("len" if "len" in header else None)
        if length_col is None:
            continue
        query_match = re.search(r"_q(\d+)", path.stem)
        default_query = f"q{query_match.group(1)}" if query_match else None
        for row in rows:
            engine = row["engine"]
            query = row.get("query") or default_query or "?"
            length = row[length_col]
            key = (query, length)
            success = row.get("success")
            errored = success == "0" or (row.get("error") or "").strip() != ""
            result = None if errored else to_int(row["result"])
            cp = checkpoints[key]
            cp.family = family_name
            cp.key = key
            cp.add(engine, result, row.get("error") or None, str(path.relative_to(EXPERIMENTS_DIR)))
    return list(checkpoints.values())


# ---------------------------------------------------------------------------
# Family 2: FinBench (e6_finbench, e6_finbench_sf1, e6_finbench_sf10).
# tcrN.csv = ReCAP's own run (result, reference_result columns).
# {kuzu,memgraph,neo4j}_tcrN.csv = competitor run vs. the same reference.
# ---------------------------------------------------------------------------

def collect_finbench_family(dir_path, family_name):
    checkpoints = defaultdict(lambda: Checkpoint(family_name, None))
    for n in (1, 5, 8):
        ref_path = dir_path / f"tcr{n}.csv"
        if not ref_path.exists():
            continue
        for row in read_csv(ref_path):
            length = row["length"]
            key = (f"tcr{n}", length)
            cp = checkpoints[key]
            cp.family = family_name
            cp.key = key
            errored = (row.get("error") or "").strip() != ""
            cp.add("recap", None if errored else to_int(row["result"]), row.get("error") or None,
                   str(ref_path.relative_to(EXPERIMENTS_DIR)))
            cp.add("independent-reference", to_int(row.get("reference_result")), None,
                   str(ref_path.relative_to(EXPERIMENTS_DIR)))
        for engine in ("kuzu", "memgraph", "neo4j"):
            comp_path = dir_path / f"{engine}_tcr{n}.csv"
            if not comp_path.exists():
                continue
            for row in read_csv(comp_path):
                length = row["length"]
                key = (f"tcr{n}", length)
                cp = checkpoints[key]
                cp.family = family_name
                cp.key = key
                errored = (row.get("error") or "").strip() != ""
                cp.add(engine, None if errored else to_int(row["result"]), row.get("error") or None,
                       str(comp_path.relative_to(EXPERIMENTS_DIR)))
                # cross-check the reference embedded alongside this competitor's
                # own run against the reference used for ReCAP's own run above.
                embedded_ref = to_int(row.get("reference_result"))
                if embedded_ref is not None:
                    cp.add(f"independent-reference (embedded in {engine} run)", embedded_ref, None,
                           str(comp_path.relative_to(EXPERIMENTS_DIR)))
    return list(checkpoints.values())


# ---------------------------------------------------------------------------
# Family 3: navigation-style experiment (monolithic vs. naive-split vs. split).
# ---------------------------------------------------------------------------

def collect_navigation_family(dir_path, family_name):
    path = dir_path / "results" / "three_way_comparison.csv"
    checkpoints = []
    if not path.exists():
        return checkpoints
    for row in read_csv(path):
        key = (row["start_vertex"], row["length"])
        cp = Checkpoint(family_name, key)
        cp.add("monolithic", to_int(row["mono_count"]), None, str(path.relative_to(EXPERIMENTS_DIR)))
        cp.add("naive-split", to_int(row["naive_count"]), None, str(path.relative_to(EXPERIMENTS_DIR)))
        cp.add("seam-aware-split", to_int(row["split_count"]), None, str(path.relative_to(EXPERIMENTS_DIR)))
        checkpoints.append(cp)
    return checkpoints


# ---------------------------------------------------------------------------
# Family 4: E5 handcrafted vs. \CompilerOpt vs. split realization of Q1.
# All three are alternate implementations of the *same* full query and are
# expected to agree exactly (unlike E4 below).
# ---------------------------------------------------------------------------

def collect_e5_family(dir_path, family_name):
    path = dir_path / "results" / "e5_q1_metaverse.csv"
    checkpoints = defaultdict(lambda: Checkpoint(family_name, None))
    if not path.exists():
        return []
    for row in read_csv(path):
        key = (row["length"],)
        cp = checkpoints[key]
        cp.family = family_name
        cp.key = key
        cp.add(row["config"], to_int(row["result"]), None, str(path.relative_to(EXPERIMENTS_DIR)))
    return list(checkpoints.values())


# ---------------------------------------------------------------------------
# Family 5: E4 isolation. Configs 2 (regex+late-property) and 3
# (regex+early=\CompilerOpt) compute the SAME final answer by construction
# and must agree; config 1 (regex-only, unfiltered) is a different metric by
# design and is reported separately, not folded into the agreement check.
# ---------------------------------------------------------------------------

def collect_e4_family(dir_path, family_name):
    path = dir_path / "results" / "e4_isolation.csv"
    checkpoints = defaultdict(lambda: Checkpoint(family_name, None))
    regex_only = {}
    if not path.exists():
        return [], regex_only
    for row in read_csv(path):
        key = (row["length"],)
        if row["config"] == "1-regex-only":
            regex_only[row["length"]] = to_int(row["result"])
            continue
        cp = checkpoints[key]
        cp.family = family_name
        cp.key = key
        cp.add(row["config"], to_int(row["result"]), None, str(path.relative_to(EXPERIMENTS_DIR)))
    return list(checkpoints.values()), regex_only


GENERIC_FAMILY_DIRS = [
    "q1_length_sweep",
    "q2_length_sweep",
    "q3_length_sweep",
    "q4_length_sweep",
    "e2e3_real_data",
    "SOA-GDBMS",
    "e7_scale_sweep",
]


def build_generic_file_groups():
    """Map family_name -> [csv paths], honoring REASSIGN_TO_FAMILY (files that
    belong, by verified dataset+start-vertex, to a different family's
    directory) and EXCLUDE_NO_COUNTERPART (files with no counterpart to
    compare against anywhere on disk)."""
    groups = defaultdict(list)
    unverifiable = []
    for dir_name in GENERIC_FAMILY_DIRS:
        results_dir = EXPERIMENTS_DIR / dir_name / "results"
        if not results_dir.exists():
            continue
        for path in sorted(results_dir.glob("*.csv")):
            excl_key = (dir_name, path.name)
            if excl_key in EXCLUDE_NO_COUNTERPART:
                unverifiable.append((dir_name, path, EXCLUDE_NO_COUNTERPART[excl_key]))
                continue
            target_family = REASSIGN_TO_FAMILY.get(excl_key, dir_name)
            groups[target_family].append(path)
    return groups, unverifiable

FINBENCH_FAMILIES = ["e6_finbench", "e6_finbench_sf1", "e6_finbench_sf10"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=EXPERIMENTS_DIR / "verification_report.md",
                         help="Where to write the markdown report (default: experiments/verification_report.md)")
    args = parser.parse_args()

    lines = []
    total_checkpoints = 0
    total_mismatches = 0
    mismatch_details = []

    def emit(s=""):
        print(s)
        lines.append(s)

    emit("# Cross-system result-agreement report")
    emit()
    emit("Replays saved result CSVs under `experiments/` and checks that every system")
    emit("reporting a result count for the same query/dataset/length agrees exactly.")
    emit("No database server is contacted; this is a replay of prior runs, not a rerun.")
    emit()

    all_families = []
    generic_groups, unverifiable = build_generic_file_groups()
    for family_name, files in generic_groups.items():
        all_families.append((family_name, collect_generic_family(family_name, files)))

    for name in FINBENCH_FAMILIES:
        dpath = EXPERIMENTS_DIR / name / "results"
        if not dpath.exists():
            continue
        all_families.append((name, collect_finbench_family(dpath, name)))

    nav_dir = EXPERIMENTS_DIR / "alternative_explorations" / "navigation_experiment"
    if nav_dir.exists():
        all_families.append(("navigation_experiment", collect_navigation_family(nav_dir, "navigation_experiment")))

    e5_dir = EXPERIMENTS_DIR / "e5_handcrafted_vs_recap"
    if e5_dir.exists():
        all_families.append(("e5_handcrafted_vs_recap", collect_e5_family(e5_dir, "e5_handcrafted_vs_recap")))

    e4_regex_only_note = None
    e4_dir = EXPERIMENTS_DIR / "e4_isolation"
    if e4_dir.exists():
        e4_checkpoints, e4_regex_only = collect_e4_family(e4_dir, "e4_isolation")
        all_families.append(("e4_isolation", e4_checkpoints))
        if e4_regex_only:
            e4_regex_only_note = e4_regex_only

    total_conflicts = 0
    for family_name, checkpoints in sorted(all_families):
        checkpoints = [cp for cp in checkpoints if len(cp.values) >= 2]
        if not checkpoints:
            continue
        family_mismatches = [cp for cp in checkpoints if not cp.agrees()]
        conflict_checkpoints = [cp for cp in checkpoints if cp.same_engine_conflicts()]
        total_checkpoints += len(checkpoints)
        total_mismatches += len(family_mismatches)
        total_conflicts += len(conflict_checkpoints)

        emit(f"## {family_name}")
        emit()
        systems = sorted({e for cp in checkpoints for e in cp.values})
        emit(f"- Systems compared: {', '.join(systems)}")
        emit(f"- Checkpoints (query/dataset/length points with >=2 systems reporting): {len(checkpoints)}")
        emit(f"- Agreeing: {len(checkpoints) - len(family_mismatches)}")
        emit(f"- Disagreeing: {len(family_mismatches)}")
        incomplete = [cp for cp in checkpoints if cp.failed_engines()]
        if incomplete:
            emit(f"- Checkpoints where at least one system errored/timed out (excluded from mismatch count, reported for completeness): {len(incomplete)}")
        if family_mismatches:
            emit()
            emit("**Mismatches:**")
            for cp in family_mismatches:
                detail = ", ".join(f"{e}={r}" for e, r in sorted(cp.numeric_results().items()))
                emit(f"  - key={cp.key}: {detail}")
                mismatch_details.append((family_name, cp.key, detail))
        if conflict_checkpoints:
            emit()
            emit("**Same-engine-label conflicts** (two different source files used the same system label at this checkpoint and disagree -- a data-hygiene issue in the CSVs themselves, kept separate from the cross-system mismatch count above):")
            for cp in conflict_checkpoints:
                for engine, entries in cp.same_engine_conflicts().items():
                    detail = ", ".join(f"{r}@{src}" for r, err, src in entries)
                    emit(f"  - key={cp.key}, engine={engine}: {detail}")
        emit()

    if unverifiable:
        emit("## Files excluded: no counterpart to verify against")
        emit()
        emit("These saved result files could not be cross-checked because nothing else on disk")
        emit("was run against the same dataset/start vertex -- listed here for transparency")
        emit("rather than silently dropped:")
        emit()
        for dir_name, path, reason in unverifiable:
            emit(f"  - `{path.relative_to(EXPERIMENTS_DIR)}`: {reason}")
        emit()

    if e4_regex_only_note:
        emit("## e4_isolation: excluded metric (by design)")
        emit()
        emit("Config `1-regex-only` measures the *unfiltered* automaton-exploration count, a")
        emit("deliberately different metric from configs 2/3's filtered final answer -- not")
        emit("included in the agreement check above. Values for reference:")
        for length, result in sorted(e4_regex_only_note.items(), key=lambda kv: int(kv[0])):
            emit(f"  - length={length}: {result}")
        emit()

    emit("## Summary")
    emit()
    emit(f"- Total checkpoints checked: {total_checkpoints}")
    emit(f"- Total mismatches: {total_mismatches}")
    emit(f"- Total same-engine-label conflicts (data hygiene, not a cross-system mismatch): {total_conflicts}")
    if unverifiable:
        emit(f"- Files excluded as unverifiable (no counterpart on disk): {len(unverifiable)}")
    if total_mismatches == 0:
        emit()
        emit("**All systems agree at every checkpoint replayed from saved results.**")
    else:
        emit()
        emit("**Mismatches found -- see detail above before trusting any cross-system claim.**")

    args.report.write_text("\n".join(lines) + "\n")
    print(f"\n(report written to {args.report.relative_to(EXPERIMENTS_DIR.parent)})", file=sys.stderr)
    return 1 if total_mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
