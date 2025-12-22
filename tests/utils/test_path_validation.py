import tempfile
from pathlib import Path

import pytest
from fastapi import HTTPException

from robosystems.utils.path_validation import (
  get_duckdb_staging_path,
  get_lbug_database_path,
  validate_graph_id,
)


class TestValidateGraphId:
  def test_valid_alphanumeric_id(self):
    result = validate_graph_id("graph123")
    assert result == "graph123"

  def test_valid_with_underscores(self):
    result = validate_graph_id("my_graph_123")
    assert result == "my_graph_123"

  def test_valid_with_hyphens(self):
    result = validate_graph_id("my-graph-123")
    assert result == "my-graph-123"

  def test_valid_mixed_case(self):
    result = validate_graph_id("MyGraph123")
    assert result == "MyGraph123"

  def test_valid_all_uppercase(self):
    result = validate_graph_id("GRAPH")
    assert result == "GRAPH"

  def test_empty_string_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_graph_id("")
    assert exc_info.value.status_code == 400
    assert "cannot be empty" in exc_info.value.detail

  def test_path_traversal_double_dot_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_graph_id("../etc/passwd")
    assert exc_info.value.status_code == 400
    assert "illegal characters" in exc_info.value.detail

  def test_path_traversal_forward_slash_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_graph_id("graph/test")
    assert exc_info.value.status_code == 400
    assert "illegal characters" in exc_info.value.detail

  def test_path_traversal_backslash_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_graph_id("graph\\test")
    assert exc_info.value.status_code == 400
    assert "illegal characters" in exc_info.value.detail

  def test_null_byte_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_graph_id("graph\x00test")
    assert exc_info.value.status_code == 400
    assert "illegal characters" in exc_info.value.detail

  def test_special_characters_raises_error(self):
    invalid_ids = ["graph@test", "graph#test", "graph$test", "graph%test"]
    for invalid_id in invalid_ids:
      with pytest.raises(HTTPException) as exc_info:
        validate_graph_id(invalid_id)
      assert exc_info.value.status_code == 400
      assert "only alphanumeric" in exc_info.value.detail

  def test_space_character_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_graph_id("my graph")
    assert exc_info.value.status_code == 400
    assert "only alphanumeric" in exc_info.value.detail

  def test_unicode_characters_raises_error(self):
    with pytest.raises(HTTPException) as exc_info:
      validate_graph_id("graph™")
    assert exc_info.value.status_code == 400
    assert "only alphanumeric" in exc_info.value.detail


class TestGetLadybugDatabasePath:
  def test_valid_graph_id_returns_path(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      result = get_lbug_database_path("test_graph", base_path=tmpdir)
      assert result == Path(tmpdir) / "test_graph.lbug"
      assert result.suffix == ".lbug"

  def test_path_stays_within_base_directory(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      result = get_lbug_database_path("my_graph_123", base_path=tmpdir)
      resolved = result.resolve()
      base_resolved = Path(tmpdir).resolve()
      assert str(resolved).startswith(str(base_resolved))

  def test_invalid_graph_id_raises_error(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      with pytest.raises(HTTPException) as exc_info:
        get_lbug_database_path("../etc/passwd", base_path=tmpdir)
      assert exc_info.value.status_code == 400

  def test_empty_graph_id_raises_error(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      with pytest.raises(HTTPException) as exc_info:
        get_lbug_database_path("", base_path=tmpdir)
      assert exc_info.value.status_code == 400

  def test_uses_env_config_when_no_base_path(self):
    result = get_lbug_database_path("test_graph")
    assert "test_graph.lbug" in str(result)
    assert result.suffix == ".lbug"

  def test_path_format_consistency(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      result1 = get_lbug_database_path("graph1", base_path=tmpdir)
      result2 = get_lbug_database_path("graph2", base_path=tmpdir)
      assert result1.parent == result2.parent
      assert result1.name == "graph1.lbug"
      assert result2.name == "graph2.lbug"


class TestGetDuckDBStagingPath:
  def test_valid_graph_id_returns_path(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      result = get_duckdb_staging_path("test_graph", base_path=tmpdir)
      assert result == Path(tmpdir) / "test_graph.duckdb"
      assert result.suffix == ".duckdb"

  def test_path_stays_within_base_directory(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      result = get_duckdb_staging_path("my_graph_123", base_path=tmpdir)
      resolved = result.resolve()
      base_resolved = Path(tmpdir).resolve()
      assert str(resolved).startswith(str(base_resolved))

  def test_invalid_graph_id_raises_error(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      with pytest.raises(HTTPException) as exc_info:
        get_duckdb_staging_path("../etc/passwd", base_path=tmpdir)
      assert exc_info.value.status_code == 400

  def test_empty_graph_id_raises_error(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      with pytest.raises(HTTPException) as exc_info:
        get_duckdb_staging_path("", base_path=tmpdir)
      assert exc_info.value.status_code == 400

  def test_uses_env_config_when_no_base_path(self):
    result = get_duckdb_staging_path("test_graph")
    assert "test_graph.duckdb" in str(result)
    assert result.suffix == ".duckdb"

  def test_path_format_consistency(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      result1 = get_duckdb_staging_path("graph1", base_path=tmpdir)
      result2 = get_duckdb_staging_path("graph2", base_path=tmpdir)
      assert result1.parent == result2.parent
      assert result1.name == "graph1.duckdb"
      assert result2.name == "graph2.duckdb"

  def test_different_from_lbug_path(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      lbug_path = get_lbug_database_path("graph", base_path=tmpdir)
      duckdb_path = get_duckdb_staging_path("graph", base_path=tmpdir)
      assert lbug_path != duckdb_path
      assert lbug_path.suffix == ".lbug"
      assert duckdb_path.suffix == ".duckdb"


class TestPathValidationIntegration:
  def test_same_graph_id_different_extensions(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      lbug = get_lbug_database_path("my_graph", base_path=tmpdir)
      duckdb = get_duckdb_staging_path("my_graph", base_path=tmpdir)

      assert lbug.parent == duckdb.parent
      assert lbug.stem == duckdb.stem == "my_graph"
      assert lbug.suffix == ".lbug"
      assert duckdb.suffix == ".duckdb"

  def test_validation_applied_consistently(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      invalid_id = "../etc/passwd"

      with pytest.raises(HTTPException):
        get_lbug_database_path(invalid_id, base_path=tmpdir)

      with pytest.raises(HTTPException):
        get_duckdb_staging_path(invalid_id, base_path=tmpdir)
