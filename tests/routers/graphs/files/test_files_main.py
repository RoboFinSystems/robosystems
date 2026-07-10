"""Comprehensive unit tests for the file management router (main.py)."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.files import main as files_module


def _make_mock_user(user_id="user-123"):
  user = Mock()
  user.id = user_id
  return user


def _patch_require_graph_access():
  return patch(
    "robosystems.middleware.billing.enforcement.require_graph_access",
    lambda *args, **kwargs: None,
  )


def _patch_universal_repo(return_value=None):
  if return_value is None:
    return_value = Mock()
  return patch.object(
    files_module,
    "get_universal_repository",
    new_callable=AsyncMock,
    return_value=return_value,
  )


def _make_file_record(
  file_id="file-1",
  graph_id="kg01234567890abcdef",
  file_name="data.parquet",
  file_format="parquet",
  file_size_bytes=1024,
  row_count=100,
  upload_status="uploaded",
  upload_method="presigned_url",
  created_at=None,
  uploaded_at=None,
  s3_key="user/graph/table/data.parquet",
  table_id="table-1",
  duckdb_status=None,
  duckdb_row_count=None,
  duckdb_staged_at=None,
  graph_status=None,
  graph_ingested_at=None,
):
  return SimpleNamespace(
    id=file_id,
    graph_id=graph_id,
    table_id=table_id,
    file_name=file_name,
    file_format=file_format,
    file_size_bytes=file_size_bytes,
    row_count=row_count,
    upload_status=upload_status,
    upload_method=upload_method,
    created_at=created_at,
    uploaded_at=uploaded_at,
    s3_key=s3_key,
    duckdb_status=duckdb_status,
    duckdb_row_count=duckdb_row_count,
    duckdb_staged_at=duckdb_staged_at,
    graph_status=graph_status,
    graph_ingested_at=graph_ingested_at,
  )


@pytest.mark.unit
class TestListFiles:
  """Test the list_files endpoint."""

  @pytest.mark.asyncio
  async def test_list_files_all_graph(self):
    """Test listing all files in a graph without filters."""
    file_records = [
      _make_file_record(file_id="f1", file_size_bytes=1024),
      _make_file_record(file_id="f2", file_size_bytes=2048),
    ]

    class FakeQuery:
      def filter(self, *args, **kwargs):
        return self

      def all(self):
        return file_records

    fake_db = SimpleNamespace(query=lambda model: FakeQuery())

    with _patch_require_graph_access(), _patch_universal_repo():
      result = await files_module.list_files(
        graph_id="kg01234567890abcdef",
        table_name=None,
        file_status=None,
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=fake_db,
      )

    assert result.graph_id == "kg01234567890abcdef"
    assert result.total_files == 2
    assert result.total_size_bytes == 3072
    assert result.table_name is None

  @pytest.mark.asyncio
  async def test_list_files_filtered_by_table_name(self):
    """Test listing files filtered by table name."""
    table_record = SimpleNamespace(id="table-1", table_name="Entity")
    file_records = [
      _make_file_record(file_id="f1", file_name="entities.parquet"),
    ]

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        files_module.GraphTable,
        "get_by_name",
        return_value=table_record,
      ),
      patch.object(
        files_module.GraphFile,
        "get_all_for_table",
        return_value=file_records,
      ),
    ):
      result = await files_module.list_files(
        graph_id="kg01234567890abcdef",
        table_name="Entity",
        file_status=None,
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=SimpleNamespace(),
      )

    assert result.table_name == "Entity"
    assert result.total_files == 1

  @pytest.mark.asyncio
  async def test_list_files_table_not_found(self):
    """Test listing files with a table name that does not exist."""
    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        files_module.GraphTable,
        "get_by_name",
        return_value=None,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await files_module.list_files(
          graph_id="kg01234567890abcdef",
          table_name="NonExistentTable",
          file_status=None,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=SimpleNamespace(),
        )
      assert exc_info.value.status_code == 404
      assert "not found" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_list_files_filtered_by_status(self):
    """Test listing files filtered by upload status."""
    file_records = [
      _make_file_record(file_id="f1", upload_status="uploaded"),
      _make_file_record(file_id="f2", upload_status="pending"),
      _make_file_record(file_id="f3", upload_status="uploaded"),
    ]

    class FakeQuery:
      def filter(self, *args, **kwargs):
        return self

      def all(self):
        return file_records

    fake_db = SimpleNamespace(query=lambda model: FakeQuery())

    with _patch_require_graph_access(), _patch_universal_repo():
      result = await files_module.list_files(
        graph_id="kg01234567890abcdef",
        table_name=None,
        file_status="uploaded",
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=fake_db,
      )

    assert result.total_files == 2
    for f in result.files:
      assert f.upload_status == "uploaded"

  @pytest.mark.asyncio
  async def test_list_files_graph_not_found(self):
    """Test listing files when graph is not found."""
    with (
      _patch_require_graph_access(),
      patch.object(
        files_module,
        "get_universal_repository",
        new_callable=AsyncMock,
        return_value=None,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await files_module.list_files(
          graph_id="kg01234567890abcdef",
          table_name=None,
          file_status=None,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_list_files_empty_graph(self):
    """Test listing files in a graph with no files."""

    class FakeQuery:
      def filter(self, *args, **kwargs):
        return self

      def all(self):
        return []

    fake_db = SimpleNamespace(query=lambda model: FakeQuery())

    with _patch_require_graph_access(), _patch_universal_repo():
      result = await files_module.list_files(
        graph_id="kg01234567890abcdef",
        table_name=None,
        file_status=None,
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=fake_db,
      )

    assert result.total_files == 0
    assert result.total_size_bytes == 0
    assert result.files == []


@pytest.mark.unit
class TestGetFile:
  """Test the get_file endpoint with multi-layer status."""

  @pytest.mark.asyncio
  async def test_returns_enhanced_status_layers(self):
    """Test that get_file returns multi-layer status information."""
    uploaded_time = datetime(2025, 1, 20, 10, 0, 0, tzinfo=UTC)
    duckdb_time = datetime(2025, 1, 20, 10, 1, 0, tzinfo=UTC)
    graph_time = datetime(2025, 1, 20, 10, 5, 0, tzinfo=UTC)

    file_record = _make_file_record(
      file_id="file-123",
      graph_id="kg01234567890abcdef",
      file_size_bytes=1048576,
      row_count=5000,
      upload_status="uploaded",
      uploaded_at=uploaded_time,
      duckdb_status="staged",
      duckdb_row_count=5000,
      duckdb_staged_at=duckdb_time,
      graph_status="ingested",
      graph_ingested_at=graph_time,
    )
    table_record = SimpleNamespace(id="table-1", table_name="Entity")

    with (
      _patch_universal_repo(),
      patch.object(
        files_module.GraphFile,
        "get_by_id",
        return_value=file_record,
      ),
      patch.object(
        files_module.GraphTable,
        "get_by_id",
        return_value=table_record,
      ),
    ):
      result = await files_module.get_file(
        graph_id="kg01234567890abcdef",
        file_id="file-123",
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=Mock(),
      )

    assert result.file_id == "file-123"
    assert result.layers is not None
    assert result.layers.s3.status == "uploaded"
    assert result.layers.s3.size_bytes == 1048576
    assert result.layers.s3.row_count == 5000
    assert result.layers.duckdb.status == "staged"
    assert result.layers.duckdb.row_count == 5000
    assert result.layers.graph.status == "ingested"

  @pytest.mark.asyncio
  async def test_file_not_found(self):
    """Test get_file returns 404 when file does not exist."""
    with (
      _patch_universal_repo(),
      patch.object(files_module.GraphFile, "get_by_id", return_value=None),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await files_module.get_file(
          graph_id="kg01234567890abcdef",
          file_id="file-404",
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_file_wrong_graph(self):
    """Test get_file returns 404 when file belongs to different graph."""
    file_record = _make_file_record(
      file_id="file-123",
      graph_id="kg_other_graph_abcde",
    )

    with (
      _patch_universal_repo(),
      patch.object(
        files_module.GraphFile,
        "get_by_id",
        return_value=file_record,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await files_module.get_file(
          graph_id="kg01234567890abcdef",
          file_id="file-123",
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_graph_not_found(self):
    """Test get_file returns 404 when graph is not found."""
    with patch.object(
      files_module,
      "get_universal_repository",
      new_callable=AsyncMock,
      return_value=None,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await files_module.get_file(
          graph_id="kg01234567890abcdef",
          file_id="file-123",
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_returns_pending_layers_when_not_staged(self):
    """Test file without duckdb/graph ingestion has pending layer values."""
    file_record = _make_file_record(
      file_id="file-pending",
      graph_id="kg01234567890abcdef",
      upload_status="uploaded",
      duckdb_status="pending",
      graph_status="pending",
    )
    table_record = SimpleNamespace(id="table-1", table_name="Entity")

    with (
      _patch_universal_repo(),
      patch.object(
        files_module.GraphFile,
        "get_by_id",
        return_value=file_record,
      ),
      patch.object(
        files_module.GraphTable,
        "get_by_id",
        return_value=table_record,
      ),
    ):
      result = await files_module.get_file(
        graph_id="kg01234567890abcdef",
        file_id="file-pending",
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=Mock(),
      )

    assert result.layers.duckdb.status == "pending"
    assert result.layers.duckdb.timestamp is None
    assert result.layers.graph.status == "pending"
    assert result.layers.graph.timestamp is None
