"""Tests for the extensions feature gate.

Covers `require_graph_extension` and `load_graph_metadata` in
`robosystems/middleware/extensions.py`. Locks in the auth-layer
contract that lets command writes never land on:

- graphs whose `schema_extensions` doesn't list the required extension
- repository graphs (shared repos like SEC) — ingestion pipelines are
  the only legitimate write path there

Missing graph rows surface as 403 (not 404) to preserve the
enumeration-safe posture already used in `check_graph_access`.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.extensions import (
  GraphExtensionContext,
  load_graph_metadata,
  require_graph_extension,
)

MODULE = "robosystems.middleware.extensions"
GRAPH_ID = "kg01234567890abcdef"


def _graph(
  *,
  graph_type: str = "entity",
  schema_extensions: list[str] | None = None,
  is_repository: bool = False,
) -> MagicMock:
  """Build a stub Graph row with just the fields the gate reads."""
  stub = MagicMock()
  stub.graph_type = graph_type
  stub.schema_extensions = schema_extensions if schema_extensions is not None else []
  stub.is_repository = is_repository
  return stub


# ---------------------------------------------------------------------------
# load_graph_metadata
# ---------------------------------------------------------------------------


class TestLoadGraphMetadata:
  def test_entity_graph_returns_context(self) -> None:
    session = MagicMock()
    with patch(
      f"{MODULE}.Graph.get_by_id",
      return_value=_graph(
        graph_type="entity",
        schema_extensions=["roboledger", "roboinvestor"],
      ),
    ) as mock_get:
      meta = load_graph_metadata(GRAPH_ID, session)

    mock_get.assert_called_once_with(GRAPH_ID, session)
    assert meta.graph_type == "entity"
    assert meta.schema_extensions == ("roboledger", "roboinvestor")
    assert meta.is_repository is False

  def test_repository_graph_flags_is_repository(self) -> None:
    with patch(
      f"{MODULE}.Graph.get_by_id",
      return_value=_graph(
        graph_type="repository",
        schema_extensions=["roboledger"],
        is_repository=True,
      ),
    ):
      meta = load_graph_metadata("sec", MagicMock())

    assert meta.is_repository is True
    assert meta.graph_type == "repository"

  def test_generic_graph_has_empty_extensions(self) -> None:
    with patch(
      f"{MODULE}.Graph.get_by_id",
      return_value=_graph(graph_type="generic", schema_extensions=[]),
    ):
      meta = load_graph_metadata(GRAPH_ID, MagicMock())

    assert meta.graph_type == "generic"
    assert meta.schema_extensions == ()

  def test_missing_graph_raises_403_not_404(self) -> None:
    """Enumeration-safe: never leak "graph exists but you can't see it"
    vs. "graph doesn't exist" — both are 403."""
    with patch(f"{MODULE}.Graph.get_by_id", return_value=None):
      with pytest.raises(HTTPException) as exc_info:
        load_graph_metadata("kgdoesnotexist00", MagicMock())

    assert exc_info.value.status_code == 403
    assert "Access denied" in str(exc_info.value.detail)

  def test_none_schema_extensions_normalized_to_empty_tuple(self) -> None:
    """Legacy rows may store None; callers should always see a tuple."""
    graph = _graph(graph_type="entity")
    graph.schema_extensions = None
    with patch(f"{MODULE}.Graph.get_by_id", return_value=graph):
      meta = load_graph_metadata(GRAPH_ID, MagicMock())

    assert meta.schema_extensions == ()


# ---------------------------------------------------------------------------
# require_graph_extension
# ---------------------------------------------------------------------------


class TestRequireGraphExtension:
  def _call_dep(self, extension: str, graph: MagicMock | None) -> GraphExtensionContext:
    """Invoke the inner dependency with mocks for session + graph loader."""
    session = MagicMock()
    dep = require_graph_extension(extension)
    with patch(f"{MODULE}.Graph.get_by_id", return_value=graph):
      return dep(graph_id=GRAPH_ID, session=session)

  def test_passes_on_entity_graph_with_extension(self) -> None:
    meta = self._call_dep(
      "roboledger",
      _graph(graph_type="entity", schema_extensions=["roboledger"]),
    )
    assert meta.graph_type == "entity"
    assert "roboledger" in meta.schema_extensions

  def test_passes_on_entity_graph_with_multiple_extensions(self) -> None:
    meta = self._call_dep(
      "roboinvestor",
      _graph(
        graph_type="entity",
        schema_extensions=["roboledger", "roboinvestor"],
      ),
    )
    assert "roboinvestor" in meta.schema_extensions

  def test_rejects_repository_graph_even_when_extension_present(self) -> None:
    """SEC has roboledger in its manifest, but commands still reject.
    Shared repos are never writable via the extensions surface."""
    with pytest.raises(HTTPException) as exc_info:
      self._call_dep(
        "roboledger",
        _graph(
          graph_type="repository",
          schema_extensions=["roboledger"],
          is_repository=True,
        ),
      )
    assert exc_info.value.status_code == 403
    assert "repository graphs" in str(exc_info.value.detail)

  def test_rejects_repository_graph_flagged_by_is_repository_only(self) -> None:
    """Defense-in-depth: `is_repository=True` alone is enough to reject."""
    with pytest.raises(HTTPException) as exc_info:
      self._call_dep(
        "roboledger",
        _graph(
          graph_type="entity",  # inconsistent, but reject anyway
          schema_extensions=["roboledger"],
          is_repository=True,
        ),
      )
    assert exc_info.value.status_code == 403
    assert "repository graphs" in str(exc_info.value.detail)

  def test_rejects_generic_graph(self) -> None:
    """Custom-schema graphs have no extensions and must 403."""
    with pytest.raises(HTTPException) as exc_info:
      self._call_dep(
        "roboledger",
        _graph(graph_type="generic", schema_extensions=[]),
      )
    assert exc_info.value.status_code == 403
    assert "not provisioned" in str(exc_info.value.detail)

  def test_rejects_subgraph_ids_before_loading_metadata(self) -> None:
    """A subgraph has no tenant schema. The route pattern admits
    `{parent}_{name}` ids, so the dependency refuses them up front (403, the
    GraphQL endpoint's answer) instead of letting the session factory reject
    the id inside a handler as a 404 or a 500."""
    dep = require_graph_extension("roboledger")
    with patch(f"{MODULE}.Graph.get_by_id") as get_by_id:
      with pytest.raises(HTTPException) as exc_info:
        dep(graph_id=f"{GRAPH_ID}_dev", session=MagicMock())
    assert exc_info.value.status_code == 403
    assert "target the parent graph" in str(exc_info.value.detail)
    get_by_id.assert_not_called()

  def test_rejects_entity_graph_without_extension(self) -> None:
    """Entity graph with empty schema_extensions → 403."""
    with pytest.raises(HTTPException) as exc_info:
      self._call_dep(
        "roboledger",
        _graph(graph_type="entity", schema_extensions=[]),
      )
    assert exc_info.value.status_code == 403
    assert "roboledger is not provisioned" in str(exc_info.value.detail)

  def test_rejects_entity_graph_with_other_extension_only(self) -> None:
    """Ledger-only graph calling roboinvestor ops → 403."""
    with pytest.raises(HTTPException) as exc_info:
      self._call_dep(
        "roboinvestor",
        _graph(graph_type="entity", schema_extensions=["roboledger"]),
      )
    assert exc_info.value.status_code == 403
    assert "roboinvestor is not provisioned" in str(exc_info.value.detail)

  def test_rejects_missing_graph_as_403(self) -> None:
    """Missing graph → 403, same as check_graph_access enumeration posture."""
    with pytest.raises(HTTPException) as exc_info:
      self._call_dep("roboledger", None)
    assert exc_info.value.status_code == 403
    assert "Access denied" in str(exc_info.value.detail)

  def test_factory_produces_independent_deps_per_extension(self) -> None:
    """Each factory call binds its own extension name."""
    ledger_dep = require_graph_extension("roboledger")
    investor_dep = require_graph_extension("roboinvestor")

    graph_ledger_only = _graph(graph_type="entity", schema_extensions=["roboledger"])
    with patch(f"{MODULE}.Graph.get_by_id", return_value=graph_ledger_only):
      # Ledger passes
      ledger_dep(graph_id=GRAPH_ID, session=MagicMock())
      # Investor rejects on the same graph
      with pytest.raises(HTTPException):
        investor_dep(graph_id=GRAPH_ID, session=MagicMock())
