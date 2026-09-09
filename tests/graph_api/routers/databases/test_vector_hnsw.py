"""Tests for the HNSW vector backend in the vector-search router.

The LanceDB-backed endpoints are covered in `test_vector_search.py`; this
file covers the LadybugDB/HNSW path, which interpolates table and column
names straight into Cypher and therefore leans entirely on
`_validate_identifier` to stay safe.

Only the build half of that path lives here. HNSW indexes are searched in
Cypher via CALL QUERY_VECTOR_INDEX, so the search route refuses them.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from robosystems.graph_api.app import create_app
from robosystems.graph_api.routers.databases.vector_search import (
  _build_hnsw_index,
  _require_writer,
  _validate_identifier,
)
from tests.graph_api.conftest import FakeQueryResult

MODULE = "robosystems.graph_api.routers.databases.vector_search"


def _service_with(conn):
  service = MagicMock()
  service.db_manager.connection_pool.get_connection.return_value.__enter__.return_value = conn
  service.db_manager.rebuild_vector_index.return_value = True
  return service


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
    conn.execute.return_value = FakeQueryResult([[1500]])
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
    conn.execute.return_value = FakeQueryResult([[0]])
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
      return FakeQueryResult()

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
class TestSearchRejectsHnsw:
  """HNSW indexes live inside the graph and are searched in Cypher.

  This route used to carry a second, parallel HNSW reader that silently
  returned zero rows: it read results via ``get_as_list()``, a method the
  engine does not have, behind a ``hasattr`` guard with no fallback. Nothing
  called it — the client method never sends ``backend`` — so every search
  answered 200 with an empty list. The reader is gone; the route now says
  where to go instead.
  """

  @pytest.fixture
  def client(self, monkeypatch):
    monkeypatch.setenv("GRAPH_BACKEND_TYPE", "ladybug")
    return TestClient(create_app())

  def test_rejects_hnsw_backend_with_400(self, client):
    response = client.post(
      "/databases/kg1/tables/Fact/vector/search",
      json={"backend": "hnsw", "embedding": [0.1] * 384, "limit": 10},
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST

  def test_names_the_cypher_call_that_replaces_it(self, client):
    """A caller who lands here needs the working query, not just a refusal."""
    response = client.post(
      "/databases/kg1/tables/FactSet/vector/search",
      json={"backend": "hnsw", "embedding": [0.1] * 384},
    )

    detail = response.json()["detail"]
    assert "QUERY_VECTOR_INDEX" in detail
    assert "'FactSet', 'factset_vec_index'" in detail
    assert "/databases/kg1/query" in detail

  def test_never_silently_returns_an_empty_result_set(self, client):
    """The regression this replaces: 200 OK with zero rows and no error."""
    response = client.post(
      "/databases/kg1/tables/Fact/vector/search",
      json={"backend": "hnsw", "embedding": [0.1] * 384},
    )

    assert response.status_code != status.HTTP_200_OK
    assert "results" not in response.json()

  def test_lance_backend_is_still_served_here(self, client):
    with patch(f"{MODULE}._get_lance_manager") as mock_manager:
      mock_manager.return_value.search.return_value = {
        "results": [{"id": "d1", "distance": 0.1}],
        "total": 1,
        "execution_time_ms": 4.5,
      }
      response = client.post(
        "/databases/kg1/tables/Fact/vector/search",
        json={"backend": "lance", "embedding": [0.1] * 384},
      )

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["total"] == 1
