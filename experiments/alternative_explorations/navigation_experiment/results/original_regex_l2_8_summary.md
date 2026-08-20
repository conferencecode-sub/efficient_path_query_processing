# Q1's original regex, monolithic vs. naive-split vs. seeded-split

`(transfer|purchase|sale)+ (phishing|scam)+`, start vertex 383, LG.csv/LG_V.csv
(Metaverse-style dataset, 1,320 vertices / 78,600 edges). Regenerated from
`original_regex_l2_8_combined.csv` (produced by `run_side_by_side.py`, run
2026-08-19; ℓ=2-6 in one pass, ℓ=7 and ℓ=8 each run separately due to
runtime).

| ℓ | path count | mono | naive-split | seeded-split | naive vs mono | split vs mono |
|---|---|---|---|---|---|---|
| 2 | 719 | 10.6ms | 18.9ms | 14.4ms | 0.6x | 0.7x |
| 3 | 8,440 | 25.7ms | 39.7ms | 27.2ms | 0.6x | 0.9x |
| 4 | 125,267 | 182.2ms | 122.7ms | 89.0ms | 1.5x | 2.0x |
| 5 | 1,201,453 | 2.42s | 449.2ms | 557.4ms | 5.4x | 4.3x |
| 6 | 9,241,189 | 21.8s | 3.49s | 4.91s | 6.2x | 4.4x |
| 7 | 61,430,146 | 189.1s | 28.8s | 40.7s | 6.6x | 4.6x |
| 8 | 394,855,868 | 37.3 min | 4.92 min | 5.36 min | 7.6x | 7.0x |

Path counts confirmed identical across all three methods at every ℓ from 2
to 8, verified via `EXCEPT ALL` bag-semantics diff (not just count
equality) -- `mono_vs_naive_match`/`mono_vs_split_match` are `True` on
every row of the underlying CSV.

Stopped at ℓ=8 (37.3 minutes for monolithic alone; ~10-12x growth per hop
observed from ℓ=6→8 puts ℓ=9 at several hours and ℓ=10 at multiple days,
extrapolated rather than attempted).
