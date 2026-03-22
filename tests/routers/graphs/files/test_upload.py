"""Comprehensive unit tests for the file upload router."""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.tables import (
  FileStatusUpdate,
  FileUploadRequest,
)
from robosystems.routers.graphs.files import upload as upload_module


def _make_mock_user(user_id="user-123"):
  user = Mock()
  user.id = user_id
  return user


def _make_upload_request(
  file_name="data.parquet",
  content_type="application/x-parquet",
  table_name="Entity",
):
  return FileUploadRequest(
    file_name=file_name,
    content_type=content_type,
    table_name=table_name,
  )


def _patch_require_graph_access():
  return patch(
    "robosystems.middleware.billing.enforcement.require_graph_access",
    lambda *args, **kwargs: None,
  )


_SENTINEL = object()


def _patch_universal_repo(return_value=_SENTINEL):
  if return_value is _SENTINEL:
    return_value = Mock()
  return patch.object(
    upload_module,
    "get_universal_repository",
    new_callable=AsyncMock,
    return_value=return_value,
  )


def _patch_s3_client(presigned_url="https://s3.example.com/presigned"):
  mock_s3 = Mock()
  mock_s3.s3_client.generate_presigned_url = Mock(return_value=presigned_url)
  mock_s3.s3_client.head_object = Mock(return_value={"ContentLength": 1024})
  mock_s3.s3_client.get_object = Mock(
    return_value={"Body": Mock(read=Mock(return_value=b"[]"))}
  )
  return patch.object(upload_module, "S3Client", return_value=mock_s3), mock_s3


@pytest.mark.unit
class TestCreateFileUploadFormatValidation:
  """Test file format validation in create_file_upload."""

  @pytest.mark.asyncio
  async def test_accepts_parquet_content_type(self):
    request = _make_upload_request(
      file_name="data.parquet", content_type="application/x-parquet"
    )
    mock_table = Mock(id="table-1", table_name="Entity")
    mock_file = Mock(id="file-1")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
      patch.object(upload_module.GraphFile, "create", return_value=mock_file),
    ):
      p, _ = _patch_s3_client()
      with p:
        result = await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
        assert result.file_id == "file-1"

  @pytest.mark.asyncio
  async def test_accepts_csv_content_type(self):
    request = _make_upload_request(file_name="data.csv", content_type="text/csv")
    mock_table = Mock(id="table-1", table_name="Entity")
    mock_file = Mock(id="file-1")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
      patch.object(upload_module.GraphFile, "create", return_value=mock_file),
    ):
      p, _ = _patch_s3_client()
      with p:
        result = await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
        assert result.file_id == "file-1"

  @pytest.mark.asyncio
  async def test_accepts_json_content_type(self):
    request = _make_upload_request(
      file_name="data.json", content_type="application/json"
    )
    mock_table = Mock(id="table-1", table_name="Entity")
    mock_file = Mock(id="file-1")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
      patch.object(upload_module.GraphFile, "create", return_value=mock_file),
    ):
      p, _ = _patch_s3_client()
      with p:
        result = await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
        assert result.file_id == "file-1"

  @pytest.mark.asyncio
  async def test_rejects_unsupported_content_type(self):
    request = _make_upload_request(
      file_name="data.xlsx",
      content_type="application/vnd.openxmlformats",
    )
    mock_table = Mock(id="table-1", table_name="Entity")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "Unsupported file format" in exc_info.value.detail


@pytest.mark.unit
class TestCreateFileUploadExtensionMatching:
  """Test file extension matching with content type."""

  @pytest.mark.asyncio
  async def test_rejects_mismatched_extension_parquet(self):
    request = _make_upload_request(
      file_name="data.csv", content_type="application/x-parquet"
    )
    mock_table = Mock(id="table-1", table_name="Entity")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "does not match content type" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_rejects_mismatched_extension_csv(self):
    request = _make_upload_request(file_name="data.json", content_type="text/csv")
    mock_table = Mock(id="table-1", table_name="Entity")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "does not match content type" in exc_info.value.detail


@pytest.mark.unit
class TestCreateFileUploadFileNameValidation:
  """Test file name validation in create_file_upload."""

  @pytest.mark.asyncio
  async def test_rejects_path_traversal_double_dot(self):
    # Use a csv extension so extension check passes, then path traversal fires
    request = _make_upload_request(
      file_name="..data..csv.csv",  # contains ".." but also passes extension
      content_type="text/csv",
    )
    mock_table = Mock(id="table-1", table_name="Entity")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "invalid characters" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_rejects_forward_slash_in_name(self):
    # The extension check on "subdir/data.parquet" sees extension "parquet", which matches.
    # Then the path traversal check fires on "/".
    request = _make_upload_request(
      file_name="subdir/data.parquet",
      content_type="application/x-parquet",
    )
    mock_table = Mock(id="table-1", table_name="Entity")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "invalid characters" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_rejects_backslash_in_name(self):
    request = _make_upload_request(
      file_name="subdir\\data.parquet",
      content_type="application/x-parquet",
    )
    mock_table = Mock(id="table-1", table_name="Entity")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "invalid characters" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_rejects_too_long_file_name(self):
    long_name = "a" * 252 + ".csv"  # 256 chars total
    request = _make_upload_request(
      file_name=long_name,
      content_type="text/csv",
    )
    mock_table = Mock(id="table-1", table_name="Entity")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "255 characters" in exc_info.value.detail


@pytest.mark.unit
class TestCreateFileUploadPresignedURL:
  """Test presigned URL generation and S3 key construction."""

  @pytest.mark.asyncio
  async def test_generates_presigned_url(self):
    request = _make_upload_request()
    mock_table = Mock(id="table-1", table_name="Entity")
    mock_file = Mock(id="file-1")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
      patch.object(upload_module.GraphFile, "create", return_value=mock_file),
    ):
      p, mock_s3 = _patch_s3_client("https://bucket.s3.aws.com/signed")
      with p:
        result = await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
        assert result.upload_url == "https://bucket.s3.aws.com/signed"
        mock_s3.s3_client.generate_presigned_url.assert_called_once()

  @pytest.mark.asyncio
  async def test_s3_key_includes_user_graph_table_file(self):
    request = _make_upload_request(file_name="data.parquet", table_name="Transaction")
    mock_table = Mock(id="table-1", table_name="Transaction")
    mock_file = Mock(id="file-1")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=mock_table,
      ),
      patch.object(upload_module.GraphFile, "create", return_value=mock_file),
    ):
      p, _ = _patch_s3_client()
      with p:
        result = await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user("user-456"),
          _rate_limit=None,
          db=Mock(),
        )
        # S3 key format: user-staging/{user_id}/{graph_id}/{table_name}/{file_id}/{file_name}
        assert result.s3_key.startswith(
          "user-staging/user-456/kg01234567890abcdef/Transaction/"
        )
        assert result.s3_key.endswith("/data.parquet")


@pytest.mark.unit
class TestCreateFileUploadTableAutoCreation:
  """Test table auto-creation on first file upload."""

  @pytest.mark.asyncio
  async def test_auto_creates_table_when_not_exists(self):
    request = _make_upload_request(table_name="NewTable")
    mock_created_table = Mock(id="table-new", table_name="NewTable")
    mock_file = Mock(id="file-1")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(upload_module.GraphTable, "get_by_name", return_value=None),
      patch.object(
        upload_module.GraphTable,
        "create",
        return_value=mock_created_table,
      ) as mock_create,
    ):
      p, _ = _patch_s3_client()
      with p:
        with patch.object(
          upload_module.GraphFile,
          "create",
          return_value=mock_file,
        ):
          await upload_module.create_file_upload(
            graph_id="kg01234567890abcdef",
            request=request,
            current_user=_make_mock_user(),
            _rate_limit=None,
            db=Mock(),
          )
          mock_create.assert_called_once()

  @pytest.mark.asyncio
  async def test_skips_table_creation_when_exists(self):
    request = _make_upload_request(table_name="Entity")
    existing_table = Mock(id="table-1", table_name="Entity")
    mock_file = Mock(id="file-1")

    with (
      _patch_require_graph_access(),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphTable,
        "get_by_name",
        return_value=existing_table,
      ),
      patch.object(upload_module.GraphTable, "create") as mock_create,
    ):
      p, _ = _patch_s3_client()
      with p:
        with patch.object(
          upload_module.GraphFile,
          "create",
          return_value=mock_file,
        ):
          await upload_module.create_file_upload(
            graph_id="kg01234567890abcdef",
            request=request,
            current_user=_make_mock_user(),
            _rate_limit=None,
            db=Mock(),
          )
          mock_create.assert_not_called()


@pytest.mark.unit
class TestCreateFileUploadSharedRepoAccess:
  """Test shared repository write protection."""

  @pytest.mark.asyncio
  async def test_rejects_upload_to_shared_repository(self):
    request = _make_upload_request()

    with (
      _patch_require_graph_access(),
      patch.object(
        upload_module,
        "is_shared_repository_or_subgraph",
        return_value=True,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="sec",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_requires_table_name(self):
    request = FileUploadRequest(
      file_name="data.parquet",
      content_type="application/x-parquet",
    )

    with _patch_require_graph_access():
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "table_name is required" in exc_info.value.detail


@pytest.mark.unit
class TestCreateFileUploadGraphNotFound:
  """Test graph not found behavior."""

  @pytest.mark.asyncio
  async def test_returns_404_when_graph_not_found(self):
    request = _make_upload_request()

    with (
      _patch_require_graph_access(),
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(return_value=None),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.create_file_upload(
          graph_id="kg01234567890abcdef",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404


@pytest.mark.unit
class TestUpdateFileStatusValidation:
  """Test file status update validation."""

  @pytest.mark.asyncio
  async def test_rejects_shared_repo_status_update(self):
    request = FileStatusUpdate(status="uploaded")

    with patch.object(
      upload_module, "is_shared_repository_or_subgraph", return_value=True
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="sec",
          file_id="file-1",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_rejects_invalid_status(self):
    request = FileStatusUpdate(status="pending")
    mock_file = Mock()
    mock_file.graph_id = "kg01234567890abcdef"

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="kg01234567890abcdef",
          file_id="file-1",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "Invalid status" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_returns_404_when_file_not_found(self):
    request = FileStatusUpdate(status="uploaded")

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(upload_module.GraphFile, "get_by_id", return_value=None),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="kg01234567890abcdef",
          file_id="file-missing",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_returns_404_when_file_graph_mismatch(self):
    request = FileStatusUpdate(status="uploaded")
    mock_file = Mock()
    mock_file.graph_id = "kg_different_graph_id"

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="kg01234567890abcdef",
          file_id="file-1",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_disable_file_succeeds(self):
    request = FileStatusUpdate(status="disabled")
    mock_db = Mock()
    mock_file = Mock()
    mock_file.graph_id = "kg01234567890abcdef"
    mock_file.upload_status = "uploaded"

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
    ):
      result = await upload_module.update_file(
        background_tasks=Mock(),
        graph_id="kg01234567890abcdef",
        file_id="file-1",
        request=request,
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=mock_db,
      )
      assert result["upload_status"] == "disabled"
      mock_db.commit.assert_called()

  @pytest.mark.asyncio
  async def test_archive_file_succeeds(self):
    request = FileStatusUpdate(status="archived")
    mock_db = Mock()
    mock_file = Mock()
    mock_file.graph_id = "kg01234567890abcdef"
    mock_file.upload_status = "uploaded"

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
    ):
      result = await upload_module.update_file(
        background_tasks=Mock(),
        graph_id="kg01234567890abcdef",
        file_id="file-1",
        request=request,
        current_user=_make_mock_user(),
        _rate_limit=None,
        db=mock_db,
      )
      assert result["upload_status"] == "archived"


@pytest.mark.unit
class TestUpdateFileFileSizeValidation:
  """Test file size validation in update_file."""

  @pytest.mark.asyncio
  async def test_rejects_empty_file(self):
    request = FileStatusUpdate(status="uploaded")
    mock_file = Mock()
    mock_file.graph_id = "kg01234567890abcdef"
    mock_file.s3_key = "user-staging/test/key"
    mock_file.file_format = "parquet"

    mock_s3 = Mock()
    mock_s3.s3_client.head_object = Mock(return_value={"ContentLength": 0})

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
      patch.object(upload_module, "S3Client", return_value=mock_s3),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="kg01234567890abcdef",
          file_id="file-1",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "empty" in exc_info.value.detail.lower()

  @pytest.mark.asyncio
  async def test_rejects_oversized_file(self):
    request = FileStatusUpdate(status="uploaded")
    mock_file = Mock()
    mock_file.graph_id = "kg01234567890abcdef"
    mock_file.s3_key = "user-staging/test/key"
    mock_file.file_format = "parquet"

    oversized_bytes = 200 * 1024 * 1024  # 200 MB, exceeds MAX_FILE_SIZE_MB=100

    mock_s3 = Mock()
    mock_s3.s3_client.head_object = Mock(
      return_value={"ContentLength": oversized_bytes}
    )

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
      patch.object(upload_module, "S3Client", return_value=mock_s3),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="kg01234567890abcdef",
          file_id="file-1",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 400
      assert "exceeds maximum" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_rejects_file_exceeding_storage_quota(self):
    request = FileStatusUpdate(status="uploaded")
    mock_file = Mock()
    mock_file.graph_id = "kg01234567890abcdef"
    mock_file.s3_key = "user-staging/test/key"
    mock_file.file_format = "csv"

    file_size = 5 * 1024 * 1024  # 5 MB

    mock_s3 = Mock()
    mock_s3.s3_client.head_object = Mock(return_value={"ContentLength": file_size})

    mock_graph = Mock()
    mock_graph.graph_tier = "ladybug-standard"

    # Create a mock table with huge existing storage
    mock_table = Mock()
    mock_table.total_size_bytes = 11 * 1024 * 1024 * 1024  # 11 GB

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
      patch.object(upload_module, "S3Client", return_value=mock_s3),
      patch.object(upload_module.Graph, "get_by_id", return_value=mock_graph),
      patch.object(
        upload_module.GraphTierConfig,
        "get_backup_limits",
        return_value={"max_backup_size_gb": 10},
      ),
      patch.object(
        upload_module.GraphTable,
        "get_all_for_graph",
        return_value=[mock_table],
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="kg01234567890abcdef",
          file_id="file-1",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 413
      assert "Storage limit exceeded" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_s3_head_object_failure_returns_404(self):
    request = FileStatusUpdate(status="uploaded")
    mock_file = Mock()
    mock_file.graph_id = "kg01234567890abcdef"
    mock_file.s3_key = "user-staging/test/key"
    mock_file.file_format = "parquet"

    mock_s3 = Mock()
    mock_s3.s3_client.head_object = Mock(side_effect=Exception("Not found"))

    with (
      patch.object(
        upload_module, "is_shared_repository_or_subgraph", return_value=False
      ),
      _patch_universal_repo(),
      patch.object(
        upload_module.GraphFile,
        "get_by_id",
        return_value=mock_file,
      ),
      patch.object(upload_module, "S3Client", return_value=mock_s3),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await upload_module.update_file(
          background_tasks=Mock(),
          graph_id="kg01234567890abcdef",
          file_id="file-1",
          request=request,
          current_user=_make_mock_user(),
          _rate_limit=None,
          db=Mock(),
        )
      assert exc_info.value.status_code == 404
      assert "not found in S3" in exc_info.value.detail
