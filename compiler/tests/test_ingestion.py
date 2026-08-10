import duckdb
import pytest

from recap_compiler.errors import IngestionError
from recap_compiler.ingestion import load_graph, select_start_vertices


def _write_csv(path, header, rows):
    path.write_text(header + "\n" + "\n".join(rows) + "\n")


@pytest.fixture
def conn():
    return duckdb.connect(":memory:")


@pytest.fixture
def edges_csv(tmp_path):
    path = tmp_path / "edges.csv"
    # A small star: 1 has high out-degree, 2/3/4 have out-degree 1, 5 has none.
    _write_csv(path, "src,dst,label,amount", [
        "1,2,a,10",
        "1,3,a,20",
        "1,4,b,30",
        "2,5,a,40",
        "3,5,b,50",
        "4,5,a,60",
    ])
    return str(path)


def test_load_graph_infers_vertices_from_edges(conn, edges_csv):
    handle = load_graph(conn, edges_csv)
    ids = {row[0] for row in conn.execute("SELECT id FROM nodes ORDER BY id").fetchall()}
    assert ids == {1, 2, 3, 4, 5}
    assert conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0] == 6


def test_load_graph_synthesizes_edge_id_when_source_lacks_one(conn, edges_csv):
    load_graph(conn, edges_csv)  # edges_csv fixture has no edge_id column of its own
    edge_ids = [row[0] for row in conn.execute("SELECT edge_id FROM edges ORDER BY edge_id").fetchall()]
    assert edge_ids == list(range(6))  # unique, 0-based, one per row


def test_load_graph_preserves_existing_edge_id(conn, tmp_path):
    path = tmp_path / "edges_with_id.csv"
    _write_csv(path, "edge_id,src,dst,label", ["100,1,2,a", "200,2,3,a"])
    load_graph(conn, str(path))
    edge_ids = {row[0] for row in conn.execute("SELECT edge_id FROM edges").fetchall()}
    assert edge_ids == {100, 200}  # real ids kept, not overwritten by synthetic ones


def test_load_graph_with_explicit_vertices(conn, edges_csv, tmp_path):
    vertices_path = tmp_path / "nodes.csv"
    _write_csv(vertices_path, "id,region", ["1,east", "2,west", "3,east", "4,west", "5,east"])
    load_graph(conn, edges_csv, vertices_source=str(vertices_path))
    rows = conn.execute("SELECT id, region FROM nodes ORDER BY id").fetchall()
    assert rows == [(1, "east"), (2, "west"), (3, "east"), (4, "west"), (5, "east")]


def test_load_graph_missing_required_column_raises(conn, tmp_path):
    bad_edges = tmp_path / "bad_edges.csv"
    _write_csv(bad_edges, "source,target,label", ["1,2,a"])
    with pytest.raises(IngestionError) as exc_info:
        load_graph(conn, str(bad_edges))
    assert exc_info.value.category == "E-INPUT"
    assert "src" in str(exc_info.value)


def test_load_graph_missing_file_raises(conn):
    with pytest.raises(IngestionError):
        load_graph(conn, "/nonexistent/path/edges.csv")


def test_load_graph_from_existing_duckdb_table(conn, edges_csv):
    conn.execute(f"CREATE TABLE raw_edges AS SELECT * FROM read_csv_auto('{edges_csv}')")
    handle = load_graph(conn, "raw_edges")
    assert conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0] == 6


def test_type_override_applies_cast(conn, edges_csv):
    load_graph(conn, edges_csv, edge_type_overrides={"amount": "DOUBLE"})
    dtype = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'edges' AND column_name = 'amount'"
    ).fetchone()[0]
    assert dtype == "DOUBLE"


def test_type_override_unknown_column_raises(conn, edges_csv):
    with pytest.raises(IngestionError):
        load_graph(conn, edges_csv, edge_type_overrides={"nonexistent": "DOUBLE"})


def test_select_start_vertices_by_ids(conn, edges_csv):
    handle = load_graph(conn, edges_csv)
    assert select_start_vertices(handle, ids=[3, 1]) == [1, 3]


def test_select_start_vertices_by_predicate(conn, edges_csv, tmp_path):
    vertices_path = tmp_path / "nodes.csv"
    _write_csv(vertices_path, "id,region", ["1,east", "2,west", "3,east", "4,west", "5,east"])
    handle = load_graph(conn, edges_csv, vertices_source=str(vertices_path))
    assert select_start_vertices(handle, predicate="region = 'west'") == [2, 4]


def test_select_start_vertices_by_degree_band(conn, edges_csv):
    handle = load_graph(conn, edges_csv)
    # out-degree: 1->3, 2->1, 3->1, 4->1, 5->0
    high = select_start_vertices(handle, degree_band="high")
    low = select_start_vertices(handle, degree_band="low")
    assert 1 in high
    assert 5 in low


def test_select_start_vertices_requires_exactly_one_mode(conn, edges_csv):
    handle = load_graph(conn, edges_csv)
    with pytest.raises(IngestionError):
        select_start_vertices(handle)
    with pytest.raises(IngestionError):
        select_start_vertices(handle, ids=[1], predicate="1=1")
