"""Tests for the HNSW vector backend in the vector-search router.

The LanceDB-backed endpoints are covered in `test_vector_search.py`; this
file covers the LadybugDB/HNSW path, which interpolates table and column
names straight into Cypher and therefore leans entirely on
`_validate_identifier` to stay safe.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.routers.databases.vector_search import (
  _build_hnsw_index,
  _require_writer,
  _search_hnsw_index,
  _validate_identifier,
)

MODULE = "robosystems.graph_api.routers.databases.vector_search"


def _service_with(conn):
  service = MagicMock()
  service.db_manager.connection_pool.get_connection.return_value.__enter__.return_value = conn
  service.db_manager.rebuild_vector_index.return_value = True
  return service


def _vector_query(conn):
  """The QUERY_VECTOR_INDEX statement the search issued."""
  return next(
    c.args[0] for c in conn.execute.call_args_list if "QUERY_VECTOR_INDEX" in c.args[0]
  )


@pytest.mark.unit
class TestIdentifierValidation:
  """These values are interpolated into Cypher without parameterization."""

  @pytest.mark.parametrize("value", ["Fact", "_private", "Table_1", "a"])
  def test_accepts_plain_identifiers(self, value):
    _validate_identifier(value)  # must not raise

  @pytest.mark.parametrize(
    "value",
    [
      "Fact; MATCH (n) DETACH DELETE n",
      "Fact) RETURN 1 //",
      "1Fact",
      "Fact-1",
      "Fact Name",
      "",
      "'; DROP",
      "Fact\nMATCH",
      "../etc",
    ],
  )
  def test_rejects_injection_shaped_values(self, value):
    with pytest.raises(ValueError, match="Invalid"):
      _validate_identifier(value)

  def test_error_names_the_offending_label(self):
    with pytest.raises(ValueError, match="Invalid column"):
      _validate_identifier("bad-col", "column")


@pytest.mark.unit
class TestRequireWriter:
  def test_allows_writer(self, monkeypatch):
    monkeypatch.delenv("LBUG_ROLE", raising=False)
    _require_writer()  # must not raise

  def test_blocks_replica_with_501(self, monkeypatch):
    """Replicas have no writable graph -- building there would corrupt the
    read path or silently no-op."""
    monkeypatch.setenv("LBUG_ROLE", "replica")

    with pytest.raises(HTTPException) as exc:
      _require_writer()

    assert exc.value.status_code == 501


@pytest.mark.unit
class TestBuildHnswIndex:
  def test_builds_and_reports_row_count(self):
    conn = MagicMock()
    result = MagicMock()
    result.get_as_list.return_value = [[1500]]
    conn.execute.return_value = result
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      out = _build_hnsw_index("kg1", "Fact")

    assert out["row_count"] == 1500
    assert out["table_name"] == "Fact"
    service.db_manager.rebuild_vector_index.assert_called_once_with(
      conn, "Fact", "embedding"
    )

  def test_checkpoints_after_building(self):
    """Without the CHECKPOINT the index lives only in the WAL and is lost on
    the next engine restart."""
    conn = MagicMock()
    conn.execute.return_value = MagicMock(get_as_list=MagicMock(return_value=[[0]]))
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      _build_hnsw_index("kg1", "Fact")

    assert any(c.args[0] == "CHECKPOINT" for c in conn.execute.call_args_list), (
      "index build must be checkpointed"
    )

  def test_raises_when_rebuild_reports_failure(self):
    conn = MagicMock()
    service = _service_with(conn)
    service.db_manager.rebuild_vector_index.return_value = False

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      with pytest.raises(RuntimeError, match="Failed to build HNSW index"):
        _build_hnsw_index("kg1", "Fact")

  def test_row_count_failure_degrades_to_zero(self):
    conn = MagicMock()

    def execute(sql):
      if sql.startswith("MATCH"):
        raise RuntimeError("count failed")
      return MagicMock()

    conn.execute.side_effect = execute
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      out = _build_hnsw_index("kg1", "Fact")

    assert out["row_count"] == 0, "a failed count must not fail the build"

  @pytest.mark.parametrize("bad", ["Fact; DELETE", "1Fact", "Fact-x"])
  def test_rejects_unsafe_table_name_before_touching_the_graph(self, bad):
    with patch(f"{MODULE}._get_ladybug_service") as mock_service:
      with pytest.raises(ValueError, match="Invalid table_name"):
        _build_hnsw_index("kg1", bad)

    mock_service.assert_not_called()

  def test_rejects_unsafe_column(self):
    with patch(f"{MODULE}._get_ladybug_service") as mock_service:
      with pytest.raises(ValueError, match="Invalid column"):
        _build_hnsw_index("kg1", "Fact", column="embedding; DROP")

    mock_service.assert_not_called()


@pytest.mark.unit
class TestSearchHnswIndex:
  def _conn_returning(self, rows):
    conn = MagicMock()
    result = MagicMock()
    result.get_as_list.return_value = rows
    conn.execute.return_value = result
    return conn

  def test_maps_selected_columns_onto_results(self):
    conn = self._conn_returning([["f1", "Revenue", 0.12], ["f2", "Cost", 0.30]])
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      out = _search_hnsw_index(
        "kg1", "Fact", [0.1, 0.2], limit=2, select_columns=["identifier", "label"]
      )

    assert out["total"] == 2
    assert out["results"][0] == {
      "identifier": "f1",
      "label": "Revenue",
      "distance": 0.12,
    }

  def test_flattens_node_dicts_when_no_columns_selected(self):
    conn = self._conn_returning([[{"identifier": "f1", "value": 10}, 0.12]])
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      out = _search_hnsw_index("kg1", "Fact", [0.1], limit=5)

    assert out["results"][0] == {"identifier": "f1", "value": 10, "distance": 0.12}

  def test_keeps_scalar_nodes_under_a_node_key(self):
    conn = self._conn_returning([["opaque", 0.5]])
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      out = _search_hnsw_index("kg1", "Fact", [0.1], limit=5)

    assert out["results"][0] == {"node": "opaque", "distance": 0.5}

  def test_over_fetches_then_applies_the_caller_limit(self):
    """HNSW is approximate, so the query over-fetches and re-sorts; the
    over-fetch is capped so a large limit can't blow up the scan."""
    conn = self._conn_returning([])
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      _search_hnsw_index("kg1", "Fact", [0.1], limit=10)

    query = _vector_query(conn)
    assert "20)" in query, "should over-fetch 2x"
    assert query.rstrip().endswith("LIMIT 10")

  def test_over_fetch_is_capped_at_200(self):
    conn = self._conn_returning([])
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      _search_hnsw_index("kg1", "Fact", [0.1], limit=150)

    query = _vector_query(conn)
    assert "200)" in query

  def test_installs_vector_extension_when_load_fails(self):
    conn = MagicMock()
    calls = []

    def execute(sql):
      calls.append(sql)
      if sql == "LOAD EXTENSION vector" and calls.count("LOAD EXTENSION vector") == 1:
        raise RuntimeError("not installed")
      return MagicMock(get_as_list=MagicMock(return_value=[]))

    conn.execute.side_effect = execute
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      _search_hnsw_index("kg1", "Fact", [0.1], limit=5)

    assert "INSTALL vector" in calls
    assert calls.count("LOAD EXTENSION vector") == 2

  def test_derives_index_name_from_table(self):
    conn = self._conn_returning([])
    service = _service_with(conn)

    with patch(f"{MODULE}._get_ladybug_service", return_value=service):
      _search_hnsw_index("kg1", "FactSet", [0.1], limit=5)

    query = _vector_query(conn)
    assert "'factset_vec_index'" in query

  @pytest.mark.parametrize("bad", ["Fact; DELETE", "1Fact"])
  def test_rejects_unsafe_table_name(self, bad):
    with patch(f"{MODULE}._get_ladybug_service") as mock_service:
      with pytest.raises(ValueError, match="Invalid table_name"):
        _search_hnsw_index("kg1", bad, [0.1], limit=5)

    mock_service.assert_not_called()

  def test_rejects_unsafe_select_column(self):
    """select_columns land in the RETURN clause -- a crafted value would let
    a caller project arbitrary Cypher."""
    with patch(f"{MODULE}._get_ladybug_service") as mock_service:
      with pytest.raises(ValueError, match="Invalid column"):
        _search_hnsw_index(
          "kg1", "Fact", [0.1], limit=5, select_columns=["identifier", "x) RETURN 1 //"]
        )

    mock_service.assert_not_called()
