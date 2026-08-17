# E1 rerun on real datasets (2026-08-14): runtime + memory side by side

Standard (Stage E, DuckDB SQL macros) vs Optimized (Stage F, flattened/inlined SQL), new
compiler only, against the real paper datasets (`experiments/datasets/`). Source CSVs:
`experiments/q{1,2,3,4}_length_sweep/results/new_compiler_q{1,2,3,4}.csv`.

`peak_rss_mb` = this process's own peak resident memory (`psutil`, isolated per
length/variant via subprocess). `peak_buffer_mb` = DuckDB's internal buffer-manager
stat (excludes Python/pandas/connection overhead — always <= `peak_rss_mb`).

## Q1 — Metaverse (1,320 vertices / 78,600 edges), start vertex 383 (high-degree)

| ℓ | result | standard ms | optimized ms | speedup | standard RSS MB | optimized RSS MB | standard buf MB | optimized buf MB |
|---|---|---|---|---|---|---|---|---|
| 2 | 23 | 46.7 | 23.7 | 2.0x | 488 | 472 | 28.3 | 26.2 |
| 3 | 95 | 71.4 | 34.5 | 2.1x | 528 | 463 | 30.5 | 28.4 |
| 4 | 264 | 100.0 | 58.0 | 1.7x | 527 | 508 | 33.0 | 30.9 |
| 5 | 466 | 140.1 | 76.7 | 1.8x | 554 | 517 | 34.0 | 39.9 |
| 6 | 711 | 189.4 | 108.6 | 1.7x | 607 | 588 | 43.0 | 40.9 |
| 7 | 745 | 233.4 | 124.3 | 1.9x | 665 | 531 | 43.0 | 40.8 |
| 8 | 840 | 251.4 | 146.7 | 1.7x | 622 | 632 | 43.0 | 40.9 |
| 9 | 852 | 272.3 | 158.5 | 1.7x | 638 | 561 | 43.0 | 40.8 |
| 10 | 878 | 336.2 | 164.9 | 2.0x | 660 | 534 | 43.0 | 40.8 |

## Q2 — Bitcoin (5,882 vertices / 35,593 edges), start vertex 3999 (median-degree)

| ℓ | result | standard ms | optimized ms | speedup | standard RSS MB | optimized RSS MB | standard buf MB | optimized buf MB |
|---|---|---|---|---|---|---|---|---|
| 2 | 34 | 11.7 | 8.6 | 1.4x | 383 | 415 | 9.9 | 9.0 |
| 3 | 1,666 | 17.7 | 15.4 | 1.1x | 410 | 404 | 17.2 | 16.4 |
| 4 | 52,508 | 46.2 | 42.6 | 1.1x | 507 | 482 | 30.7 | 30.0 |
| 5 | 2,743,095 | 791.8 | 713.6 | 1.1x | 1,373 | 1,329 | 541.7 | 532.6 |

Stopped at ℓ=5 (ℓ=6 didn't finish in 150s — no early filtering on Q2's color
constraint, so growth is inherent, not implementation-dependent).

## Q3 — Datagen-7.6 (754,147 vertices / 84,325,976 directed edges), start vertex 4398046568596 (low-degree)

| ℓ | result | standard ms | optimized ms | speedup | standard RSS GB | optimized RSS GB | standard buf GB | optimized buf GB |
|---|---|---|---|---|---|---|---|---|
| 2 | 6,435 | 57.6 | 52.4 | 1.1x | 19.9 | 19.3 | 8.8 | 8.8 |
| 3 | 374,725 | 427.1 | 339.2 | 1.3x | 18.0 | 19.5 | 8.8 | 8.8 |
| 4 | 18,744,888 | 3,002.5 | 2,517.2 | 1.2x | 17.9 | 17.6 | 10.7 | 10.7 |

Stopped at ℓ=4 (per-hop growth ~55-58x despite full early filtering on the
monotonicity constraint). RSS/buffer memory here are overwhelmingly the one-time
cost of loading+bidirecting the 84.3M-edge graph, not query state — nearly flat
across ℓ and across variants.

## Q4 — LDBC100 (448,626 vertices / 19,941,198 edges), start vertex 24189256063073 (median-degree)

Uses the paper's actual Q4 semantics (max-min *timestamp* over 2 weeks =
1,209,600,000 ms), not the generic 0-100-weight stand-in the toy dataset needed.

| ℓ | result | standard ms | optimized ms | speedup | standard RSS MB | optimized RSS MB | standard buf MB | optimized buf MB |
|---|---|---|---|---|---|---|---|---|
| 2 | 422 | 61.8 | 37.8 | 1.6x | 4,114 | 4,087 | 1,753 | 1,729 |
| 3 | 4,621 | 74.8 | 48.1 | 1.6x | 4,177 | 3,968 | 1,754 | 1,732 |
| 4 | 38,444 | 121.7 | 90.6 | 1.3x | 4,114 | 4,078 | 1,783 | 1,750 |
| 5 | 279,035 | 198.0 | 134.7 | 1.5x | 4,073 | 3,984 | 1,919 | 1,888 |
| 6 | 1,818,120 | 472.4 | 433.9 | 1.1x | 4,173 | 4,089 | 2,426 | 2,402 |
| 7 | 10,704,627 | 2,319.0 | 2,146.8 | 1.1x | 4,239 | 4,228 | 3,355 | 3,352 |
| 8 | 57,615,956 | 13,520.4 | 12,408.7 | 1.1x | 11,052 | 11,049 | 10,266 | 10,259 |

Stopped at ℓ=8 (ℓ=9 didn't finish in 590s after a 5.4x jump from ℓ=7→8).

## Headline

None of these come close to the paper's reported 152x-346x Standard-vs-Optimized
speedup — expected, not a regression: the new compiler's "Standard" is DuckDB SQL
macros, not the paper's actual Standard implementation (Python UDFs with per-call
JSON parsing), so it never had that overhead to remove. The real, smaller effect
(1.1x-2.1x) is struct-field access vs. flattened columns (confirmed via `EXPLAIN`
in an earlier session — no macro-call dispatch overhead exists in DuckDB's plan
either way). Memory is essentially unaffected by Standard vs Optimized at every
scale tested — the two variants differ in generated-SQL *shape*, not in how much
intermediate state they must hold.
