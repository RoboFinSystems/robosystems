"""Tests for Kuzu database path utility functions."""

from pathlib import Path
from unittest.mock import patch
import tempfile

from robosystems.operations.kuzu.path_utils import (
  get_kuzu_database_path,
  ensure_kuzu_directory,
)


class TestGetKuzuDatabasePath:
  """Test cases for get_kuzu_database_path function."""

  @patch("robosystems.operations.kuzu.path_utils.env.KUZU_DATABASE_PATH", "/data/kuzu")
  def test_get_kuzu_database_path_default(self):
    """Test getting database path with default base path."""
    result = get_kuzu_database_path("test_db")

    assert isinstance(result, Path)
    assert str(result) == "/data/kuzu/test_db.kuzu"

  def test_get_kuzu_database_path_with_base_path(self):
    """Test getting database path with custom base path."""
    result = get_kuzu_database_path("custom_db", base_path="/custom/path")

    assert isinstance(result, Path)
    assert str(result) == "/custom/path/custom_db.kuzu"

  @patch(
    "robosystems.operations.kuzu.path_utils.env.KUZU_DATABASE_PATH",
    "/var/kuzu/databases",
  )
  def test_get_kuzu_database_path_sec_repository(self):
    """Test getting path for SEC shared repository."""
    result = get_kuzu_database_path("sec")

    assert isinstance(result, Path)
    assert str(result) == "/var/kuzu/databases/sec.kuzu"
    assert result.name == "sec.kuzu"

  def test_get_kuzu_database_path_with_special_chars(self):
    """Test database path with special characters in name."""
    result = get_kuzu_database_path("kg-123_test", base_path="/tmp")

    assert isinstance(result, Path)
    assert str(result) == "/tmp/kg-123_test.kuzu"

  @patch("robosystems.operations.kuzu.path_utils.env.KUZU_DATABASE_PATH", "/prod/kuzu")
  def test_get_kuzu_database_path_multiple_calls(self):
    """Test multiple calls with different database names."""
    result1 = get_kuzu_database_path("db1")
    result2 = get_kuzu_database_path("db2")
    result3 = get_kuzu_database_path("db3", base_path="/custom")

    assert str(result1) == "/prod/kuzu/db1.kuzu"
    assert str(result2) == "/prod/kuzu/db2.kuzu"
    assert str(result3) == "/custom/db3.kuzu"


class TestEnsureKuzuDirectory:
  """Test cases for ensure_kuzu_directory function."""

  def test_ensure_kuzu_directory_creates_parent(self):
    """Test that parent directory is created if it doesn't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "subdir" / "test.kuzu"

      # Directory shouldn't exist yet
      assert not db_path.parent.exists()

      # Ensure directory
      ensure_kuzu_directory(db_path)

      # Parent directory should now exist
      assert db_path.parent.exists()
      assert db_path.parent.is_dir()

  def test_ensure_kuzu_directory_existing_parent(self):
    """Test with already existing parent directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "test.kuzu"

      # Parent already exists
      assert db_path.parent.exists()

      # Should not raise error
      ensure_kuzu_directory(db_path)

      # Parent should still exist
      assert db_path.parent.exists()

  def test_ensure_kuzu_directory_nested_paths(self):
    """Test creating deeply nested directory structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "level1" / "level2" / "level3" / "database.kuzu"

      # None of the intermediate directories exist
      assert not db_path.parent.exists()

      # Ensure directory
      ensure_kuzu_directory(db_path)

      # All parent directories should be created
      assert db_path.parent.exists()
      assert (db_path.parent.parent).exists()
      assert (db_path.parent.parent.parent).exists()

  def test_ensure_kuzu_directory_with_string_path(self):
    """Test with string path instead of Path object."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path_str = f"{temp_dir}/subdir/test.kuzu"

      # Should accept string and create parent
      ensure_kuzu_directory(db_path_str)

      # Parent should exist
      assert Path(db_path_str).parent.exists()

  def test_ensure_kuzu_directory_multiple_calls(self):
    """Test multiple calls to ensure_kuzu_directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
      db_path = Path(temp_dir) / "databases" / "test.kuzu"

      # First call creates directory
      ensure_kuzu_directory(db_path)
      assert db_path.parent.exists()

      # Second call should not fail (exist_ok=True)
      ensure_kuzu_directory(db_path)
      assert db_path.parent.exists()

  @patch("pathlib.Path.mkdir")
  def test_ensure_kuzu_directory_calls_mkdir_correctly(self, mock_mkdir):
    """Test that mkdir is called with correct parameters."""
    db_path = Path("/test/path/database.kuzu")

    ensure_kuzu_directory(db_path)

    # Should call mkdir with parents=True and exist_ok=True
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
