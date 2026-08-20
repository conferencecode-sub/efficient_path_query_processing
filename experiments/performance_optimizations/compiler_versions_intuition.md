# Std vs. Opt vs. UDF: the real intuition, and which claim each one actually supports

## TL;DR

There are **three** compiled variants floating around this project, not two,
and the original "banked on inlining" framing conflates numbers that belong
to different variants:

| Variant | What it is | Calls anything at runtime? | `D` representation | Measured gap vs. the *next* variant down |
|---|---|---|---|---|
| **Python UDF** (old hand-built prototype; reproduced today as the `experiments/udf_variant/` ablation) | Definition 8's 5 functions registered as real Python functions | Yes — crosses into the Python interpreter on every hop | JSON string, `json.dumps`/`json.loads` per call | — |
| **`\CompilerStd`** (Stage E) | Same 5 functions registered as native DuckDB **SQL macros** | Text-substituted at bind time, no runtime dispatch | One struct-typed column | **11–168x faster** than Python UDF (`fig:recap_udf_variant`) |
| **`\CompilerOpt`** (Stage F) | Same 5 function *bodies*, spliced directly into the query as plain expressions | Nothing — no macro, no call site at all | Flattened: one plain column per key | **1.1–2.1x faster** than `\CompilerStd` (`tab:recap_real_data`) |

The big win (orders of magnitude) is **Python → SQL**. The optimization
question ("do we inline?") is about the *much smaller* remaining gap,
**SQL macro → inlined SQL**. These are not the same claim, and the original
framing needs to stop presenting them as one.

There's a third number entirely separate from both of these: ReCAP's own
early-filtering abstraction vs. SOA graph DBMS (Neo4j/Memgraph/Kùzu) —
150–5000x+ depending on the query. That number comes from the *algorithm*
(pruning doomed paths early via negatively-stable selective aggregates), and
has nothing to do with SQL codegen at all — it holds even for the
unoptimized `\CompilerStd` output.

---

## Do we inline? Yes — but "inline" means two different things

**`\CompilerStd`: no.** It calls the five functions as SQL macros, passing
the whole edge row and `D` as parameters, once per hop.

**`\CompilerOpt`: yes, completely.** No macro, no function, no call site of
any kind survives in the generated SQL. Every `D.<key>` reference and every
function body is spliced in as a plain column expression.

Here's the subtlety that makes this confusing: **DuckDB already
text-substitutes a macro call before it ever plans the query.** Confirmed
via `EXPLAIN` — no macro name appears anywhere in `\CompilerStd`'s physical
plan. So at the level of "does the SQL text get expanded inline," the answer
is yes *even for `\CompilerStd`*, for free, courtesy of the engine.

That's exactly why the original claim needs tightening. "We inline the
functions" is *not* what separates `\CompilerOpt` from `\CompilerStd` in any
way that matters for performance — DuckDB does that part regardless of
which variant you use. What actually separates them is something one level
lower:

> To call a macro at all — even one that gets text-substituted away —
> DuckDB first has to **pack** the entire edge row into a struct just to
> bind it as the macro's parameter, then the macro body has to **unpack**
> the one or two fields it actually needs. That packing/unpacking survives
> the text substitution. It's paid on every hop of every path, for both
> `update_d` and `is_viable_d`.
>
> `\CompilerOpt` doesn't call anything, so there's nothing to bind a
> parameter to, so there's no row to pack. Each field is read directly off
> a real column.

**Dictionary flattening** and **function inlining** are the two rewrites
that get you there: flattening gives each key of `D` its own column instead
of a struct field; inlining removes the call site so there's no parameter
to bind `D` (or the edge row) to in the first place. They're independent —
you could flatten `D` and still call macros with it, or inline calls that
still return a struct — but the compiler always does both together.

## A minimal worked example

Toy aggregate: running sum of edge weights, cap the trail at 3 edges
(factorized — state-independent).

```
init_d          = {total_weight: 0.0, edge_count: 0}
update_d        = {total_weight: D.total_weight + e.weight,
                    edge_count:  D.edge_count + 1}
is_viable_d     = D.edge_count < 3
```

**`\CompilerStd`** — every recursive step builds a struct to call
`update_d`, unpacks two fields inside it, builds another struct to call
`is_viable_d`, unpacks one field inside it:

```sql
UNION ALL
SELECT e.dst AS v, t.to_state AS q,
       update_d(p.D, p.q, t.to_state, e) AS D,     -- <- whole row packed to call this
       p.path_length + 1
FROM paths p JOIN edges e ON e.src = p.v
JOIN transitions t ON t.from_state = p.q AND t.label = e.label
WHERE p.path_length < 3
  AND is_viable_d(p.D, p.q, t.to_state, e)          -- <- and this
```

**`\CompilerOpt`** — two scalar columns, read and written directly, nothing
to pack or unpack:

```sql
UNION ALL
SELECT e.dst AS v, t.to_state AS q,
       (p.total_weight + e.weight) AS total_weight, -- inlined: D.total_weight -> p.total_weight
       (p.edge_count + 1) AS edge_count,
       p.path_length + 1
FROM paths p JOIN edges e ON e.src = p.v
JOIN transitions t ON t.from_state = p.q AND t.label = e.label
WHERE p.path_length < 3
  AND (p.edge_count < 3)                            -- inlined, no macro call
```

## Why the measured gap is only 1.1–2.1x, and why that's expected, not disappointing

The packing/unpacking cost is real (measured: `tab:recap_real_data`), but
it's still **native DuckDB struct manipulation** — vectorized, in-process,
no interpreter crossing. That puts a low ceiling on how big this
particular gap can be. It's not nothing (never slower, up to 2.1x faster,
and the isolation experiment shows *how much* is dataset-dependent: 81–91%
of Q1's gap is inlining alone on a wide 15-column edge table, essentially
0% on Q3's narrow 5-column table, the rest being flattening) — but it was
never going to look like the Python-UDF number, because it isn't fixing the
same kind of problem. Python UDFs pay interpreter dispatch + JSON
(de)serialization *per call*; SQL macros pay struct pack/unpack *per call*,
which is orders of magnitude cheaper per call even before you remove it
entirely.

## The three claims, kept separate (this is the fix for "banked on inlining")

1. **ReCAP's abstraction vs. SOA graph DBMS**: 150–5000x+, from early
   filtering / negatively-stable selective aggregates pruning doomed paths
   before they're fully materialized. Algorithmic. Holds for `\CompilerStd`
   too — Q2's own results prove this directly: `\CompilerStd` *alone*,
   with **none** of the flattening/inlining optimization, already beats
   plain DuckDB by 2.9–6.8x even on a query with no early-filtering
   opportunity at all — because the win there is ReCAP's incremental
   aggregate design piggybacking on the join, not any SQL-level rewrite.
2. **SQL vs. Python UDFs**: 11–168x, from never leaving DuckDB's own
   execution engine. This is what the old paper's Table 1 and "1–2 orders
   of magnitude" language were actually measuring — and it's a property of
   compiling to SQL *at all* (i.e., `\CompilerStd` already has it), not of
   the flattening/inlining pass.
3. **`\CompilerStd` vs. `\CompilerOpt`** (dictionary flattening + function
   inlining): 1.1–2.1x, from removing struct pack/unpack on macro calls
   that DuckDB would otherwise text-substitute anyway. Real, general
   (applies automatically to any `SelectiveAggregate`, no hand-tuning),
   and never negative — but the smallest of the three, and the *only* one
   this specific optimization pass is entitled to claim credit for.

If the paper's original framing attributed (1) or (2)'s magnitude to
inlining, that's the thing to fix: inlining/flattening only ever owns (3).
