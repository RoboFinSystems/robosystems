"""Tests for LadybugDB database path utility functions."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from robosystems.operations.lbug.path_utils import (
  ensure_lbug_directory,
  get_lbug_database_path,
)


class TestGetLadybugDatabasePath:
  """Test cases for get_lbug_database_path function."""

  @patch(
    "robosystems.operations.lbug.path_utils.env.LBUG_DATABASE_PATH", "/data/lbug-dbs"
  )
  def test_get_lbug_database_path_default(self):
    """Test getting database path with default base path."""
    result = get_lbug_database_path("test_db")

    assert isinstance(result, Path)
    assert str(result) == "/data/lbug-dbs/test_db.lbug"

  def test_get_lbug_database_path_with_base_path(self):
    """Test getting database path with custom base path."""
    result = get_lbug_database_path("custom_db", base_path="/custom/path")

    assert isinstance(result, Path)
    assert str(result) == "/custom/path/custom_db.lbug"

  @patch(
    "robosystems.operations.lbug.path_utils.env.LBUG_DATABASE_PATH",
    "/var/lbug-dbs/databases",
  )
  def test_get_lbug_database_path_sec_repository(self):
    """Test getting path for SEC shared repository."""
    result = get_lbug_database_path("sec")

    assert isinstance(result, Path)
    assert str(result) == "/var/lbug-dbs/databases/sec.lbug"
    assert result.name == "sec.lbug"

  def test_get_lbug_database_path_with_special_chars(self):
    """Test database path with special characters in name."""
    result = get_lbug_database_path("kg-123_test", base_path="/tmp")

    assert isinstance(result, Path)
    assert str(result) == "/tmp/kg-123_test.lbug"

  @patch(
    "robosystems.operations.lbug.path_utils.env.LBUG_DATABASE_PATH", "/prod/lbug-dbs"
  )
  def test_get_lbug_database_path_multiple_calls(self):
    """Test multiple calls with different database names."""
    result1 = get_lbug_database_path("db1")
    result2 = get_lbug_database_path("db2")
    result3 = get_lbug_database_path("db3", base_path="/custom")

    assert str(result1) == "/prod/lbug-dbs/db1.lbug"
    assert str(result2) == "/prod/lbug-dbs/db2.lbug"
    assert str(result3) == "/custom/db3.lbug"


class TestEnsureLadybugDirectory:
  """Test cases for ensure_lbug_directory function."""

  def test_ensure_lbug_directory_creates_parent(self):
    """Test that parent directory is created if it doesn't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "subdir" / "test.lbug"

      # Directory shouldn't exist yet
      assert not db_path.parent.exists()

      # Ensure directory
      ensure_lbug_directory(db_path)

      # Parent directory should now exist
      assert db_path.parent.exists()
      assert db_path.parent.is_dir()

  def test_ensure_lbug_directory_existing_parent(self):
    """Test with already existing parent directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "test.lbug"

      # Parent already exists
      assert db_path.parent.exists()

      # Should not raise error
      ensure_lbug_directory(db_path)

      # Parent should still exist
      assert db_path.parent.exists()

  def test_ensure_lbug_directory_nested_paths(self):
    """Test creating deeply nested directory structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "level1" / "level2" / "level3" / "database.lbug"

      # None of the intermediate directories exist
      assert not db_path.parent.exists()

      # Ensure directory
      ensure_lbug_directory(db_path)

      # All parent directories should be created
      assert db_path.parent.exists()
      assert (db_path.parent.parent).exists()
      assert (db_path.parent.parent.parent).exists()

  def test_ensure_lbug_directory_with_string_path(self):
    """Test with string path instead of Path object."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path_str = f"{temp_dir}/subdir/test.lbug"

      # Should accept string and create parent
      ensure_lbug_directory(db_path_str)

      # Parent should exist
      assert Path(db_path_str).parent.exists()

  def test_ensure_lbug_directory_multiple_calls(self):
    """Test multiple calls to ensure_lbug_directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "databases" / "test.lbug"

      # First call creates directory
      ensure_lbug_directory(db_path)
      assert db_path.parent.exists()

      # Second call should not fail (exist_ok=True)
      ensure_lbug_directory(db_path)
      assert db_path.parent.exists()

  @patch("pathlib.Path.mkdir")
  def test_ensure_lbug_directory_calls_mkdir_correctly(self, mock_mkdir):
    """Test that mkdir is called with correct parameters."""
    db_path = Path("/test/path/database.lbug")

    ensure_lbug_directory(db_path)

    # Should call mkdir with parents=True and exist_ok=True
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
