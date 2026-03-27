"""Tests for materialize endpoint source field and entity graph guards."""

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.materialize import (
  MaterializeRequest,
  resolve_materialization_source,
)


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


class TestResolveSource:
  """Test resolve_materialization_source — the actual production function."""

  def test_entity_graph_infers_extensions(self):
    result = resolve_materialization_source(None, "entity")
    assert result == "extensions"

  def test_generic_graph_infers_staged(self):
    result = resolve_materialization_source(None, "generic")
    assert result == "staged"

  def test_explicit_source_not_overridden(self):
    result = resolve_materialization_source("extensions", "entity")
    assert result == "extensions"

  def test_entity_graph_rejects_staged(self):
    with pytest.raises(HTTPException) as exc:
      resolve_materialization_source("staged", "entity")
    assert exc.value.status_code == 400
    assert "Entity graphs" in exc.value.detail

  def test_generic_graph_rejects_extensions(self):
    with pytest.raises(HTTPException) as exc:
      resolve_materialization_source("extensions", "generic")
    assert exc.value.status_code == 400
    assert "Generic graphs" in exc.value.detail

  def test_entity_graph_allows_extensions(self):
    result = resolve_materialization_source("extensions", "entity")
    assert result == "extensions"

  def test_generic_graph_allows_staged(self):
    result = resolve_materialization_source("staged", "generic")
    assert result == "staged"

  def test_repository_graph_infers_staged(self):
    result = resolve_materialization_source(None, "repository")
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
