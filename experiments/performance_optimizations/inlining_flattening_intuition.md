# Intuition: Dictionary Flattening + Function Inlining (Stage F)

## The problem, in one sentence

`CompilerStd` (Stage E) makes every hop of the path call five DuckDB SQL
**macros** (`init_d`, `update_d`, `is_viable_d`, `is_viable_d_final`,
`finalize_d`) and carries the running dictionary `D` around as one
struct-typed column. DuckDB expands a macro call by bind-time text
substitution — no runtime dispatch, confirmed via `EXPLAIN` (no macro name
survives into the physical plan). So the macro call *itself* is free. What
isn't free: to call `update_d(D, from_state, to_state, e)`, DuckDB first
has to **pack** every column of the current row into a struct just to bind
it to the macro's parameters, then the macro body **unpacks** the one or
two fields it actually needs (`D.total_weight`, `e.weight`, ...). That
pack/unpack is paid on every single hop of every path.

`CompilerOpt` (Stage F) removes this by generating different SQL from the
*same* aggregate spec — no macros, no struct column, just plain scalar
columns and inline expressions. Two independent rewrites, both done
automatically by the compiler (`recap_compiler/optimizer.py`,
`build_optimized_query`), not by hand, and not per-query:

- **Dictionary flattening:** each key of `D` becomes its own typed
  column on the `paths` relation, instead of one field nested inside a
  struct.
- **Function inlining:** every `D.<key>` reference becomes a real
  column reference (`p.<key>`); every `e.<column>` is left alone (it was
  already a real column); there is no macro call left anywhere.

Both are static, source-to-source SQL rewrites performed once at
compile time on the aggregate's five bodies — the same bodies `CompilerStd`
pastes verbatim into macros. Nothing about them depends on the specific
query being run; they're generic over any `SelectiveAggregate`.

## A minimal worked example

Toy aggregate: "keep a running sum of edge weights, and cap the trail at
3 edges." Two dictionary keys, factorized (state-independent):

```
init_d          = {total_weight: 0.0, edge_count: 0}
update_d        = {total_weight: D.total_weight + e.weight,
                    edge_count:  D.edge_count + 1}
is_viable_d     = D.edge_count < 3
is_viable_d_final = TRUE
finalize_d      = D
```

### CompilerStd (Stage E) — macros + struct column

```sql
CREATE OR REPLACE MACRO init_d() AS
    ({total_weight: 0.0, edge_count: 0});
CREATE OR REPLACE MACRO update_d(D, from_state, to_state, e) AS
    ({total_weight: D.total_weight + e.weight, edge_count: D.edge_count + 1});
CREATE OR REPLACE MACRO is_viable_d(D, from_state, to_state, e) AS
    (D.edge_count < 3);

WITH RECURSIVE paths AS (
    SELECT s.v AS v, q0 AS q, init_d() AS D, 0 AS path_length
    FROM (VALUES (383)) AS s(v)
    UNION ALL
    SELECT e.dst AS v, t.to_state AS q,
           update_d(p.D, p.q, t.to_state, e) AS D,   -- <- whole row packed to call this
           p.path_length + 1 AS path_length
    FROM paths p
    JOIN edges e ON e.src = p.v
    JOIN transitions t ON t.from_state = p.q AND t.label = e.label
    WHERE p.path_length < 3
      AND is_viable_d(p.D, p.q, t.to_state, e)        -- <- and this
)
SELECT v, q, D, path_length, finalize_d(D) AS result
FROM paths
WHERE q IN (q_accept) AND is_viable_d_final(D);
```

Every recursive step: build a struct to call `update_d`, unpack two fields
inside it, build another struct to call `is_viable_d`, unpack one field
inside it. `D` itself is a struct column the whole way through.

### CompilerOpt (Stage F) — flattened columns, inlined expressions

```sql
WITH RECURSIVE paths AS (
    SELECT s.v AS v, q0 AS q,
           (0.0) AS total_weight, (0) AS edge_count,   -- flattened: D's keys are real columns
           0 AS path_length
    FROM (VALUES (383)) AS s(v)
    UNION ALL
    SELECT e.dst AS v, t.to_state AS q,
           (p.total_weight + e.weight) AS total_weight, -- inlined: D.total_weight -> p.total_weight
           (p.edge_count + 1) AS edge_count,
           p.path_length + 1 AS path_length
    FROM paths p
    JOIN edges e ON e.src = p.v
    JOIN transitions t ON t.from_state = p.q AND t.label = e.label
    WHERE p.path_length < 3
      AND (p.edge_count < 3)                            -- inlined, no macro call
)
SELECT v, q, {total_weight: total_weight, edge_count: edge_count} AS D,
       path_length,
       ({total_weight: total_weight, edge_count: edge_count}) AS result
FROM paths
WHERE q IN (q_accept) AND (TRUE);
```

No macro call anywhere. `D` only gets reconstructed as a struct in the
*output* columns, for a readable apples-to-apples comparison against
`CompilerStd`'s result shape — internally, the recursion only ever touches
`total_weight`/`edge_count` as plain scalar columns. Each hop reads and
writes two scalars directly; there is nothing to pack or unpack.

(This is a simplified illustration for intuition — real generated SQL,
e.g. for `q3_aggregate()`/`q4_aggregate()`, has the same shape but with the
actual `sqlglot`-rewritten expressions; see
`recap_compiler/optimizer.py::build_optimized_query` and
`recap_compiler/standard_sql.py::build_standard_query` for the exact
templates.)

## Where this maps onto `old.tex`

`old.tex`'s "Dictionary Flattening" and "Function Inlining" subsections are
describing *exactly* this Stage F pass:

- "Dictionary flattening removes one level of nesting from the selective
  aggregate's dictionary `D`... we add a column `K` directly to the Paths
  relation" = `_decompose_struct` in `optimizer.py`.
- "Function inlining rewrites each UDF as SQL and substitutes its body at
  every call site... `isvalid` becomes a `CASE` over NFA states $q,q'$...
  for factorized ReCAPs, every `CASE` collapses to an unconditional
  expression" = `_rewrite_node`/`_flatten_update_d`/`_flatten_is_viable_d`,
  including the factorized-vs-non-factorized `CASE`
  behavior described there almost verbatim.
- The passage explicitly says inlining's real contribution is *not*
  "UDF calls are gone" (DuckDB's own macro substitution already expands
  those for free, confirmed via `EXPLAIN` — no macro name survives) but
  "the packing/unpacking that substitution still needs whenever a function
  call binds a whole edge row to a single parameter" — which is precisely
  the mechanism this file walks through above.

## The attribution question

**You can claim this.** It is not something DuckDB (or any other engine)
does on its own — DuckDB's macro expansion is bind-time text substitution
*and nothing more*; it still forces the struct pack/unpack every time a
macro parameter binds to a whole row. Stage F is the thing that removes
that cost, and it's ReCAP-compiler code (`optimizer.py`), not an engine
feature you're borrowing credit for.

The one framing worth being deliberate about: "automatic" doesn't mean
"free" or "not our contribution" — it means the compiler applies this
rewrite mechanically to *any* `SelectiveAggregate`, at compile time, with
no query-specific hand-tuning. That's a stronger claim than "we
hand-optimized one query's SQL": it's a general compiler optimization pass
that every ReCAP query benefits from automatically, which is exactly the
distinction `figures.tex`'s own prose already draws ("generates both a
standard and an optimized query directly from a query specification, with
no hand-tuning"). `old.tex`'s existing "we integrate... via two
complementary transformations" phrasing already attributes this
correctly — first person plural, describing what the compiler (built by
this project) does, not what DuckDB does for you.
