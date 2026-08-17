# E7 — Scale check on Datagen-7.7 (2026-08-14)

Per `experiments/new_experiments_checklist/recap_experiments_requirements.md`'s E7
("run the early-filtering queries on Datagen-7.7 ... to answer 'why only modest
graphs?'"). Scope decided with the user: **Q3 and Q4 only** — Q1 needs labels
(transfer/purchase/phishing/...) and region/risk/amount/timestamp columns that a
plain Graphalytics graph doesn't have, and fabricating them was explicitly rejected
in favor of staying real-data-only. The memory half of E7 ("a query where early
filtering doesn't collapse the state") is already covered by the existing
Q2/Bitcoin numbers in `q2_length_sweep/README.md` (2.7M paths, 1.3GB RSS at ℓ=5,
no early filtering) — no new run needed for that part.

## Dataset: Datagen-7.7 (`experiments/datasets/datagen7.7/`)

**Real data-quality finding.** The raw `.e` file has 26,894,900 well-formed
`src dst weight` lines and one final **truncated** line (`8796109644722
10995117360684`, missing its weight value and the trailing newline) — the file
was evidently cut off mid-write during extraction. Loaded with
`read_csv(..., ignore_errors=true)` to drop just that one row. The remaining
26,894,900 bidirect (graph is undirected per `dataset.properties`) to
53,789,800 edges, matching tab:realdata's "53.7M" exactly. Vertex count
(13,180,508) matches "13.1M" directly. Note: `dataset.properties` itself claims
32,791,267 edges, which matches neither the raw line count nor the paper's own
table — apparently stale, not trusted by the loader.

**Real degree-distribution finding.** This graph is far sparser than Datagen-7.6
(avg degree ~4 vs ~111) and extremely skewed: q25=1, median=1, q75=2 out of
10,933,040 distinct vertices with edges — 55% of vertices have out-degree
exactly 1, and max out-degree is only 2,084. The paper's own low/medium/high
out-degree-quartile methodology degenerates here (q25 and median coincide), so
Q3 uses a degree-1 vertex (matches q25/median exactly) and Q4 uses a degree-2
vertex (this graph's own q75, the best available "medium" anchor, not a true
statistical median).

## Real compiler bug found and fixed

First attempt at Q3 crashed: `Conversion Error: Type INT64 with value
13194146057717 can't be cast ... INT32 ... column v`. Root cause, confirmed by
reading `standard_sql.build_standard_query` and `optimizer.build_optimized_query`
directly: both build the recursive CTE's anchor as `FROM (VALUES ({v}), ...)
AS s(v)` with a bare, untyped literal. DuckDB infers a `VALUES` clause's column
type from the literal itself — Q3's start vertex (`3184`) is small enough to
fit INT32, so `v` got typed `INTEGER`, and the recursive term then tried to
insert a real destination vertex id (up to ~13 trillion, needing BIGINT) into
that narrowed column. This had never been hit before because every other
dataset's chosen start vertex happened to be large enough itself to force
BIGINT inference by luck (e.g. LDBC100's `24189256063073`) — Datagen-7.7 is the
first case combining huge vertex ids *and* a low-degree (hence often small-id)
required start vertex.

Fixed with a one-token change in both files: `f"({v})"` → `f"({v}::BIGINT)"` in
the `seed_values` construction. 121 existing tests still passed; added 2
regression tests (`test_anchor_handles_vertex_ids_beyond_int32_range` in
`test_standard_sql.py`, `test_optimized_anchor_handles_vertex_ids_beyond_int32_range`
in `test_optimizer.py`) reproducing the exact shape (small start vertex, huge
destination id). 123 tests pass now.

## Results

### Q3 (monotonic trail), start vertex 3184 (out-degree 1)

| ℓ | result | standard ms | optimized ms | standard RSS GB | optimized RSS GB |
|---|---|---|---|---|---|
| 2 | 34 | 29.2 | 18.1 | 12.9 | 12.6 |
| 4 | 3,579 | 84.4 | 59.7 | 12.4 | 12.1 |
| 6 | 156,155 | 317.4 | 282.2 | 12.4 | 13.5 |
| 8 | 3,619,820 | 1,916.0 | 1,516.6 | 12.6 | 12.9 |
| 10 | 53,588,257 | 18,897.6 | 15,431.7 | 15.0 | 15.0 |

Full data in `results/new_compiler_q3_datagen77.csv`. **Reached the paper's full
ℓ≤10 range** — the first real-data query in this revision to do so on a >10M-edge
graph, confirming the hypothesis from the planning discussion: Datagen-7.7's low
branching factor (sparse graph) makes it far more tractable for deep sweeps than
Datagen-7.6's denser structure (which capped at ℓ=4). This is a stronger "ReCAP
scales to large graphs" data point than 7.6 gave us. RSS is fairly flat (~12-15GB,
dominated by loading+bidirecting the 53.7M-edge graph) until ℓ=10, where
intermediate cardinality itself (53.6M paths) starts to matter.

### Q4 (max-min-weight trail, bound=0.1), start vertex 245996 (out-degree 2)

| ℓ | result | standard ms | optimized ms | standard RSS GB | optimized RSS GB |
|---|---|---|---|---|---|
| 2 | 494 | 43.0 | 33.3 | 13.1 | 13.3 |
| 3 | 3,951 | 77.8 | 70.4 | 12.4 | 13.1 |
| 4 | 239,316 | 181.4 | 163.8 | 12.5 | 13.4 |
| 5 | 3,403,054 | 651.8 | 603.4 | 12.0 | 13.4 |
| 6 | 147,378,357 | 24,694.7 | 22,564.1 | 22.1 | 22.0 |

Full data in `results/new_compiler_q4_datagen77.csv`. Stopped at ℓ=6: growth
5→6 was already 43x, and ℓ=7 didn't finish in 500s. **Real finding on bound
selectivity:** `bound=0.1` was chosen against a very narrow, skewed weight
distribution (p10=p25=p50=1.0, p90=1.29, max=1.75 — most edges weigh ~1.0), but
the blowup pattern here looks close to "no early filtering at all" rather than
a genuinely selective constraint — most paths trivially satisfy a max-min
range of 0.1 when most edges share the same weight. Unlike Q3 (which reached
ℓ=10), a tighter bound would likely be needed to see the deep-scaling story on
this specific query/dataset pairing; not pursued further this round since Q3
already delivers E7's core "reaches full ℓ range" claim.

Standard vs Optimized speedup on both queries here (1.1-1.3x) is consistent
with the E1 rerun's finding: this new compiler's "Standard" is DuckDB SQL
macros, not the paper's Python-UDF implementation, so the gap is structurally
smaller than the paper's own 152x-346x claim.
