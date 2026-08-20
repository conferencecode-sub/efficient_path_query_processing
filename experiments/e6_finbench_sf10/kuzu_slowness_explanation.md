# Why is Kùzu so much slower than Neo4j/Memgraph on TCR5/TCR8, and why does the gap shrink as ℓ grows?

## The two things to explain

1. Kùzu is dramatically slower than Neo4j/Memgraph on TCR5/TCR8 -- not just a
   little slower, orders of magnitude, and the gap **grows with scale
   factor**.
2. At a *fixed* scale factor, the ratio between Kùzu and Neo4j/Memgraph
   **shrinks as ℓ increases** -- e.g. TCR8 at SF10 goes from ~862x slower at
   ℓ=2 down to ~12.6x slower at ℓ=5. That looks like Kùzu "getting better,"
   but it isn't -- it's Neo4j/Memgraph getting *worse* faster than Kùzu is.

## Evidence: cost scales with total dataset size, not with the query itself

TCR5, ℓ=2 (a single specific start vertex, one hop out -- about as local a
query as this benchmark has), across scale factors:

| | SF0.1 | SF1 | SF10 |
|---|---|---|---|
| Kùzu | 452.5 ms | 4,038.1 ms | 38,279.6 ms |
| Neo4j | 80.2 ms | 64.1 ms | 18.2 ms |
| Memgraph | 3.4 ms | 4.7 ms | 15.3 ms |

Neo4j/Memgraph's ℓ=2 cost is flat across a 100x growth in dataset size (both
stay in the tens-of-ms range) -- consistent with index-free adjacency /
pointer-chasing from the one specific start vertex, which never has to touch
data unrelated to that vertex's own local neighborhood. Kùzu's ℓ=2 cost grows
**~9-10x every time the scale factor grows 10x** -- almost exactly linear in
total dataset size, despite the query itself being maximally local.

This isn't a query-compilation/JIT warm-up artifact: the per-query warmup fix
(added for the TCR1 cold-start issue) barely moved Kùzu's TCR5/TCR8 ℓ=2
numbers at all (TCR8: 56.4s → 55.3s). Whatever is costing this much time is
being *executed*, not just *compiled*, on every run.

## Root cause, found directly via `EXPLAIN` (not inferred)

Ran `EXPLAIN` on Kùzu's own TCR8 query (ℓ=2) against the SF10 database. The
physical plan contains:

```
SCAN_NODE_TABLE[0]   Tables: Node   Properties: .id      <- full scan, no predicate
SCAN_NODE_TABLE[2]   Tables: Node   Properties: (feeds SEMI_MASKER)
        │                              │
        └──────────────┬───────────────┘
                        ▼
                 SEMI_MASKER[8]   Operators: SCAN_NODE_TABLE[0], SCAN_NODE_TABLE[2]
                        │
                 RECURSIVE_EXTEND[6]
```

and, separately, elsewhere in the same plan:

```
PRIMARY_KEY_SCAN_NODE_TABLE[13]   <- the actual indexed start-vertex lookup
```

So the plan does **two full, unfiltered scans of the entire `Node` table**
(803,622 rows at SF10) to build a semi-join mask feeding the recursive
extension -- in addition to, not instead of, the indexed point lookup for the
one real start vertex. This is a real mechanism (Kùzu's semi-mask/frontier
pruning strategy for recursive joins), not a bug: it likely exists to prune
which nodes can *possibly* participate in the recursive join before doing the
actual multi-hop expansion. But building that mask costs a full table scan
regardless of how selective the start vertex is or how short ℓ is -- which is
exactly the "scales with total dataset size, flat with ℓ" signature observed
above.

Neo4j/Memgraph's own execution model (index-free adjacency: each node
directly holds pointers to its own relationships) has no equivalent
whole-graph step -- traversal only ever touches nodes/edges actually reached
by expanding from the start vertex, so its cost is a function of local
fan-out and path length, not total graph size.

## Why the *ratio* shrinks as ℓ grows

The `SCAN_NODE_TABLE`/`SEMI_MASKER` cost above is close to **fixed per
query** -- it depends on total node count, not on ℓ or on how many candidate
paths actually exist. Neo4j/Memgraph have close to **zero** fixed cost but a
**steep** per-hop growth rate (their own runtime roughly tracks the
candidate-path explosion, which grows 7-15x per extra hop on TCR8). Kùzu's
own *marginal* per-hop cost is comparatively modest once the fixed scan/mask
cost is already paid -- its total runtime only grows ~1.6-2.9x per hop over
the same range.

So at ℓ=2, Kùzu's huge, ℓ-independent fixed cost totally dominates a
still-small Neo4j/Memgraph runtime, producing the ~862x gap. As ℓ grows,
Neo4j/Memgraph's own cost explodes far faster than Kùzu's marginal cost does,
so the ratio closes -- not because Kùzu improved, but because Neo4j/Memgraph
degraded faster. Both engines are still slower in absolute terms than
ReCAP throughout; this whole comparison is Kùzu-vs-its-own-competitor-peers,
independent of ReCAP's own numbers.

## What would confirm this further (not yet done)

`EXPLAIN` (not `EXPLAIN ANALYZE`) only shows the static plan shape, not real
per-operator row counts/timings for this query at SF10 scale -- Kùzu's
`ANALYZE` variant would give actual measured cardinalities for the
`SCAN_NODE_TABLE`/`SEMI_MASKER` operators and could confirm the full-scan
cost quantitatively rather than just structurally. Worth doing if this needs
to go in the paper with a fully quantified breakdown rather than the
plan-shape-level evidence here.
