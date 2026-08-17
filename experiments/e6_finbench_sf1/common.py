"""Shared loader for E6/SF1 (LDBC FinBench TCR1/TCR5/TCR8), SF1 data generated
this session (see `experiments/datasets/finbench_sf1/README.md` for
provenance) -- a second, larger scale-factor sibling of
`experiments/e6_finbench/common.py`'s SF0.1 loader; identical logic, only
`DATASET_DIR` differs. Unions FinBench's separate edge tables (transfer,
withdraw, deposit, signIn, personOwnAccount) into one `edges` table with the
`src, dst, label, edge_id, timestamp_ms, amount` shape ReCAP's compiler
expects.

**Direction note, worked out from the reference Cypher directly, not
assumed:** `deposit` is `(loan:Loan)-[edge1:deposit]->(src:Account)` (Loan
-> Account, so `loanId AS src, accountId AS dst`) and `signIn` is
`(other:Account)<-[edge2:signIn]-(medium:Medium)` (Medium -> Account, but
TCR1 needs to *arrive at* the medium as the final hop of a forward walk,
i.e. the reverse direction). Rather than building general path reversal
(explicitly out of scope, see the E5 write-up's own deferred stretch goal),
this materializes the one reversed direction TCR1 actually needs as its own
forward-directed pseudo-edge, `signedInBy` (`accountId AS src, mediumId AS
dst`) -- a standard, narrow technique for a single-direction traversal
engine, not a general reversal planner. It's also pre-filtered to
`medium.isBlocked = true` at construction time, which is exactly
equivalent to the Cypher's own `Medium {isBlocked: true}` node-label
filter -- ReCAP's aggregates only see edge properties (FR-13's `e.<column>`
convention), not vertex properties, so folding the vertex filter into which
edges exist at all avoids needing vertex-property support for this one
query.

`START_TIME`/`END_TIME` reused unchanged from the SF0.1 loader -- confirmed
SF1's own generated timestamp range (~1584412876953..1672531196874 across
transfer/withdraw/deposit/signIn) still falls entirely within
[1.5e12, 1.7e12], so no window adjustment was needed for the larger scale.
"""
import os

import duckdb

_HERE = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(_HERE, "..", "datasets", "finbench_sf1")

START_TIME = 1_500_000_000_000  # confirmed still covers SF1's own generated range
END_TIME = 1_700_000_000_000


def load_data(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(f"""
        CREATE TABLE edges AS
        SELECT ROW_NUMBER() OVER () - 1 AS edge_id, * FROM (
            SELECT fromId AS src, toId AS dst, 'transfer' AS label, createTime AS timestamp_ms, amount
            FROM read_csv_auto('{DATASET_DIR}/transfer/*.csv', delim='|')
            UNION ALL
            SELECT fromId AS src, toId AS dst, 'withdraw' AS label, createTime AS timestamp_ms, amount
            FROM read_csv_auto('{DATASET_DIR}/withdraw/*.csv', delim='|')
            UNION ALL
            SELECT loanId AS src, accountId AS dst, 'deposit' AS label, createTime AS timestamp_ms, amount
            FROM read_csv_auto('{DATASET_DIR}/deposit/*.csv', delim='|')
            UNION ALL
            SELECT personId AS src, accountId AS dst, 'own' AS label, createTime AS timestamp_ms, NULL AS amount
            FROM read_csv_auto('{DATASET_DIR}/personOwnAccount/*.csv', delim='|')
            UNION ALL
            SELECT s.accountId AS src, s.mediumId AS dst, 'signedInBy' AS label,
                   s.createTime AS timestamp_ms, NULL AS amount
            FROM read_csv_auto('{DATASET_DIR}/signIn/*.csv', delim='|') s
            JOIN read_csv_auto('{DATASET_DIR}/medium/*.csv', delim='|') m ON m.id = s.mediumId
            WHERE m.isBlocked
        )
    """)
    conn.execute("CREATE TABLE nodes AS SELECT src AS id FROM edges UNION SELECT dst AS id FROM edges")
    conn.execute(f"""
        CREATE TABLE loan AS SELECT * FROM read_csv_auto('{DATASET_DIR}/loan/*.csv', delim='|')
    """)
    conn.execute("CREATE INDEX idx_edges_src ON edges(src)")


if __name__ == "__main__":
    conn = duckdb.connect()
    load_data(conn)
    print(conn.execute("SELECT label, COUNT(*) FROM edges GROUP BY label ORDER BY label").fetchall())
