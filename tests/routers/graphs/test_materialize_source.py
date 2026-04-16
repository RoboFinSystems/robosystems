"""Tests for materialize source field and entity graph guards."""

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.operations import MaterializeOp
from robosystems.operations.graph.commands.materialize import _resolve_source


class TestMaterializeOpSourceField:
  def test_default_source_is_none(self):
    req = MaterializeOp()
    assert req.source is None

  def test_accepts_staged(self):
    req = MaterializeOp(source="staged")
    assert req.source == "staged"

  def test_accepts_extensions(self):
    req = MaterializeOp(source="extensions")
    assert req.source == "extensions"

  def test_rejects_invalid_source(self):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
      MaterializeOp(source="invalid")

  def test_rejects_duckdb_source(self):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
      MaterializeOp(source="duckdb")


class TestResolveSource:
  """Test _resolve_source — the actual production function."""

  def test_entity_graph_infers_extensions(self):
    result = _resolve_source(None, "entity")
    assert result == "extensions"

  def test_generic_graph_infers_staged(self):
    result = _resolve_source(None, "generic")
    assert result == "staged"

  def test_explicit_source_not_overridden(self):
    result = _resolve_source("extensions", "entity")
    assert result == "extensions"

  def test_entity_graph_rejects_staged(self):
    with pytest.raises(HTTPException) as exc:
      _resolve_source("staged", "entity")
    assert exc.value.status_code == 400
    assert "Entity graphs" in exc.value.detail

  def test_generic_graph_rejects_extensions(self):
    with pytest.raises(HTTPException) as exc:
      _resolve_source("extensions", "generic")
    assert exc.value.status_code == 400
    assert "Generic graphs" in exc.value.detail

  def test_entity_graph_allows_extensions(self):
    result = _resolve_source("extensions", "entity")
    assert result == "extensions"

  def test_generic_graph_allows_staged(self):
    result = _resolve_source("staged", "generic")
    assert result == "staged"

  def test_repository_graph_infers_staged(self):
    result = _resolve_source(None, "repository")
    assert result == "staged"


class TestEntityGraphFileUploadGuard:
  """Test that entity graphs block file uploads."""

  def test_entity_graph_blocks_upload(self):
    graph = SimpleNamespace(graph_type="entity")
    assert getattr(graph, "graph_type", None) == "entity"

  def test_generic_graph_allows_upload(self):
    graph = SimpleNamespace(graph_type="generic")
    assert getattr(graph, "graph_type", None) != "entity"

  def test_none_graph_allows_upload(self):
    assert getattr(None, "graph_type", None) != "entity"
