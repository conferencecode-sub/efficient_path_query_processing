# Navigation-style experiment: Phase 1 (regex + monotonic time only)

Answers the design question of whether the approach could be split into
segments / explored non-forward, for one concrete case: Q1 split at its rare-label seam
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
- `naive_split.py` — plan (ii): F1 is unchanged, but F2 (states `{1,2}`) explores
  independently from *every* vertex, unseeded, remembering only its own
  first/last vertex and first/last timestamp. The cross-fragment monotonicity
  check (`F1.last_time < F2.first_time`) is enforced once, as a final join
  predicate, instead of inline on every hop. Reports F1/F2/join timing plus the
  size of F2's unseeded intermediate, to isolate how much seam-level filtering
  in the seeded version actually saves.
- `check_equivalence.py` — runs all three plans over the same data/window and
  diffs the result sets against the baseline with SQL `EXCEPT ALL` (bag
  semantics; see the comment in the script for why plain `EXCEPT` would
  silently hide a mismatch here).

## Running

From this directory (paths default to `ReCAP/simple_dataset/LG_V.csv` /
`LG.csv`; pass `--nodes`/`--edges` to point elsewhere):

```bash
# Baseline only (typical left-to-right)
python3 baseline_monolithic.py --min-length 2 --max-length 6

# Seeded split + merge only
python3 seeded_split.py --min-length 2 --max-length 6

# Naive split + final join only
python3 naive_split.py --min-length 2 --max-length 6

# All three, plus the equivalence checks -- this is the actual experiment
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
# baseline (monolithic):  10577068 paths in ~24s
# seeded split + merge:   10577068 paths in ~5.2s (F1 seam rows: 13416055)
# naive split + join:     10577068 paths in ~3.5s (F1 seam rows: 13416055, F2 unseeded suffix rows: 107540)
# MATCH (baseline vs. seeded split): result sets are identical.
# MATCH (baseline vs. naive split): result sets are identical.
```

Note the naive split actually beats the seeded split here, which is worth
stating honestly rather than assuming seam-inline filtering always wins: with
a single start vertex, F1's boundary set is huge (13.4M walks, many reaching
the same seam vertices at different prefix lengths/times), so seeded split's
F2 re-runs essentially the same `{1,2}` expansion once per boundary row.
Naive split's F2 instead expands once per *distinct* vertex (1,320 roots,
producing only 107K suffix rows) and defers the monotonicity check to a single
final join. Which plan wins depends on the ratio of boundary rows to distinct
seam vertices, not on seam-filtering being universally better or worse.

## Not covered here

- Phase 2 (genuine directional reversal: point-to-point query, reversed-edge
  fragment, real two-sided merge) — see section 4 of
  `navigation_style_experiment.md`. Deliberately out of scope for this cut.
