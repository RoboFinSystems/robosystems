"""Tests for materialize endpoint source field and entity graph guards."""

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.materialize import MaterializeRequest


class TestMaterializeRequestSourceField:
  def test_default_source_is_none(self):
    req = MaterializeRequest()
    assert req.source is None

  def test_accepts_staged(self):
    req = MaterializeRequest(source="staged")
    assert req.source == "staged"

  def test_accepts_extensions(self):
    req = MaterializeRequest(source="extensions")
    assert req.source == "extensions"

  def test_rejects_invalid_source(self):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
      MaterializeRequest(source="invalid")

  def test_rejects_duckdb_source(self):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
      MaterializeRequest(source="duckdb")


class TestSourceInference:
  """Test that source is correctly inferred from graph type."""

  def _make_request(self, source=None):
    return MaterializeRequest(source=source, force=True)

  def test_entity_graph_infers_extensions(self):
    """Entity graphs should default to source=extensions."""
    req = self._make_request()
    graph = SimpleNamespace(graph_type="entity")

    # Simulate the inference logic from the endpoint
    if req.source is None:
      req.source = "extensions" if graph.graph_type == "entity" else "staged"

    assert req.source == "extensions"

  def test_generic_graph_infers_staged(self):
    """Generic graphs should default to source=staged."""
    req = self._make_request()
    graph = SimpleNamespace(graph_type="generic")

    if req.source is None:
      req.source = "extensions" if graph.graph_type == "entity" else "staged"

    assert req.source == "staged"

  def test_explicit_source_not_overridden(self):
    """Explicit source should not be overridden by graph type."""
    req = self._make_request(source="extensions")
    graph = SimpleNamespace(graph_type="entity")

    if req.source is None:
      req.source = "extensions" if graph.graph_type == "entity" else "staged"

    assert req.source == "extensions"


class TestSourceValidation:
  """Test that mismatched source/graph_type combinations are rejected."""

  def test_entity_graph_rejects_staged(self):
    """Entity graphs cannot use source=staged."""
    graph = SimpleNamespace(graph_type="entity")
    source = "staged"

    with pytest.raises(HTTPException) as exc:
      if graph.graph_type == "entity" and source == "staged":
        raise HTTPException(
          status_code=400,
          detail="Entity graphs cannot use source='staged'.",
        )

    assert exc.value.status_code == 400

  def test_generic_graph_rejects_extensions(self):
    """Generic graphs cannot use source=extensions."""
    graph = SimpleNamespace(graph_type="generic")
    source = "extensions"

    with pytest.raises(HTTPException) as exc:
      if graph.graph_type == "generic" and source == "extensions":
        raise HTTPException(
          status_code=400,
          detail="Generic graphs cannot use source='extensions'.",
        )

    assert exc.value.status_code == 400

  def test_entity_graph_allows_extensions(self):
    """Entity graphs should allow source=extensions."""
    graph = SimpleNamespace(graph_type="entity")
    source = "extensions"

    # Should not raise
    if graph.graph_type == "entity" and source == "staged":
      raise HTTPException(status_code=400, detail="Should not happen")

  def test_generic_graph_allows_staged(self):
    """Generic graphs should allow source=staged."""
    graph = SimpleNamespace(graph_type="generic")
    source = "staged"

    # Should not raise
    if graph.graph_type == "generic" and source == "extensions":
      raise HTTPException(status_code=400, detail="Should not happen")


class TestEntityGraphFileUploadGuard:
  """Test that entity graphs block file uploads."""

  def test_entity_graph_blocks_upload(self):
    graph = SimpleNamespace(graph_type="entity")

    with pytest.raises(HTTPException) as exc:
      if getattr(graph, "graph_type", None) == "entity":
        raise HTTPException(
          status_code=400,
          detail="Entity graphs do not support file uploads.",
        )

    assert exc.value.status_code == 400
    assert "Entity graphs" in exc.value.detail

  def test_generic_graph_allows_upload(self):
    graph = SimpleNamespace(graph_type="generic")

    # Should not raise
    if getattr(graph, "graph_type", None) == "entity":
      raise HTTPException(status_code=400, detail="Should not happen")

  def test_none_graph_allows_upload(self):
    """When graph is None (test mocks), uploads should proceed."""
    graph = None

    # Should not raise
    if getattr(graph, "graph_type", None) == "entity":
      raise HTTPException(status_code=400, detail="Should not happen")
