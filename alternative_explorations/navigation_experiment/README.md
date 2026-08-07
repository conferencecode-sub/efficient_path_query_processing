# Navigation-style experiment: Phase 1 (regex + monotonic time only)

Answers R4.O2 ("could the approach be split into segments / explored
non-forward") for one concrete case: Q1 split at its rare-label seam
(`(transfer|purchase|sale)+` -> `(phishing|scam)+`), merged back together, and
checked against the ordinary left-to-right baseline. See
`../navigation_style_experiment.md` for the full design and the stretch goal
(genuine directional reversal, not implemented here).

**Scope of this cut:** only the regex (NFA) constraint and the
monotonic-timestamp constraint are enforced. Region matching, the risk-score
window, the amount threshold, and the explicit trail (no-repeat-edge) check
from the original Q1 query are all dropped for now, to keep the seam-
composition argument easy to verify. (Monotonic time already rules out reusing
the same edge twice — an edge can't have a timestamp greater than itself — so
dropping the explicit trail check doesn't introduce repeats.) All vertices in
the dataset are used as start vertices by default, rather than the single fixed
`starter_node` in the original `ReCAP/q1` scripts -- pass `--start-vertex` to
restrict to one (see "Scaling to deeper paths" below).

## Files

- `common.py` — shared data loader (`LG.csv` / `LG_V.csv` + the hardcoded Q1 NFA,
  matching `ReCAP/q1/recap_gen_recap_inline.py`).
- `baseline_monolithic.py` — plan (i): the ordinary left-to-right query, one
  recursive CTE, all vertices seeded as roots at state 0.
- `seeded_split.py` — plan (iii): F1 (states `{0,1}`, all vertices) computes seam
  rows; F2 (states `{1,2}`) is seeded directly from F1's boundary rows and
  continues to the accepting state. Reports F1/F2 timing and seam-row count
  separately.
- `check_equivalence.py` — runs both over the same data/window and diffs the
  result sets with SQL `EXCEPT` in both directions.

## Running

From this directory (paths default to `ReCAP/simple_dataset/LG_V.csv` /
`LG.csv`; pass `--nodes`/`--edges` to point elsewhere):

```bash
# Baseline only (typical left-to-right)
python3 baseline_monolithic.py --min-length 2 --max-length 6

# Split + merge only
python3 seeded_split.py --min-length 2 --max-length 6

# Both, plus the equivalence check -- this is the actual experiment
python3 check_equivalence.py --min-length 2 --max-length 6
```

`--max-length` is the main lever: increase it to see how the seam-row count and
the F1-vs-F2 timing split shift as paths get longer. `check_equivalence.py`
should print `MATCH` every time by construction (see the correctness note in
section 3 of `navigation_style_experiment.md`); a `MISMATCH` means a bug in the
split, not a research finding.

## Scaling to deeper paths: `--start-vertex`

With only regex + monotonic-time pruning and *all* 1,320 vertices as starts,
the search blows up fast: `transfer|purchase|sale` cover ~92% of edges and
self-loop at state 1, average out-degree is ~59.5, and row counts grow roughly
20x per extra hop. `--max-length` beyond 3 does not finish in reasonable time
(or memory) in that mode.

Pass `--start-vertex` to restrict to one vertex, matching the original `q1`
scripts' fixed `starter_node` (`383`) -- this stays tractable at much deeper
`--max-length` and is a stronger demonstration of seam pruning (the split
runs noticeably faster than the monolithic baseline once paths get long):

```bash
python3 check_equivalence.py --min-length 2 --max-length 6 --start-vertex 383
# baseline (monolithic):  10577068 paths in ~26s
# seeded split + merge:   10577068 paths in ~5.5s (F1 seam rows: 13416055)
# MATCH: result sets are identical.
```

## Not covered here

- Plan (ii), the "naive"/unseeded split that isolates how much the seam filter
  in F2 actually prunes vs. filtering only at the very end. Left out to keep
  this first cut minimal; would be a `naive_split.py` sibling if pursued next.
- Phase 2 (genuine directional reversal: point-to-point query, reversed-edge
  fragment, real two-sided merge) — see section 4 of
  `navigation_style_experiment.md`. Deliberately out of scope for this cut.
