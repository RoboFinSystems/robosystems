"""
Extended unit tests for LadybugDB backup manager operations.

Covers format conversion, content type helpers, download methods,
create/restore flows, lifecycle management, and database stats/export routing.
"""

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.aws.s3 import BackupMetadata, S3BackupAdapter
from robosystems.operations.graph.engine.backup_manager import (
  BackupFormat,
  BackupJob,
  BackupManager,
  BackupType,
  RestoreJob,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_adapter():
  return MagicMock(spec=S3BackupAdapter)


@pytest.fixture
def manager(s3_adapter):
  return BackupManager(s3_adapter=s3_adapter)


def _mock_repository():
  """Build a mock repository that works as an async context manager."""
  repo = MagicMock()
  repo.__aenter__ = AsyncMock(return_value=repo)
  repo.__aexit__ = AsyncMock(return_value=None)
  repo.execute_single = AsyncMock(return_value={"count": 0})
  repo.execute_query = AsyncMock(return_value=[])
  return repo


# ===========================================================================
# _get_content_type_and_filename
# ===========================================================================


@pytest.mark.unit
class TestGetContentTypeAndFilename:
  """Exhaustive coverage of _get_content_type_and_filename for every format."""

  def test_csv_format(self, manager):
    ct, fn = manager._get_content_type_and_filename("csv", "bk1")
    assert ct == "text/csv"
    assert fn == "bk1.csv.zip"

  def test_json_format(self, manager):
    ct, fn = manager._get_content_type_and_filename("json", "bk2")
    assert ct == "application/json"
    assert fn == "bk2.json.zip"

  def test_parquet_format(self, manager):
    ct, fn = manager._get_content_type_and_filename("parquet", "bk3")
    assert ct == "application/octet-stream"
    assert fn == "bk3.parquet.zip"

  def test_full_dump_format(self, manager):
    ct, fn = manager._get_content_type_and_filename("full_dump", "bk4")
    assert ct == "application/zip"
    assert fn == "bk4_database.lbug.zip"

  def test_unknown_format_falls_back(self, manager):
    ct, fn = manager._get_content_type_and_filename("xml", "bk5")
    assert ct == "application/octet-stream"
    assert fn == "bk5.zip"

  def test_empty_format_falls_back(self, manager):
    ct, fn = manager._get_content_type_and_filename("", "bk6")
    assert ct == "application/octet-stream"
    assert fn == "bk6.zip"


# ===========================================================================
# _convert_csv_to_json
# ===========================================================================


@pytest.mark.unit
class TestConvertCsvToJson:
  @pytest.mark.asyncio
  async def test_csv_to_json_returns_encoded_bytes(self, manager, tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("name,value\nalpha,1\nbeta,2\n")

    result = await manager._convert_csv_to_json(csv_file)

    assert isinstance(result, bytes)
    parsed = json.loads(result)
    assert len(parsed) == 2
    assert parsed[0]["name"] == "alpha"
    assert parsed[1]["value"] == 2

  @pytest.mark.asyncio
  async def test_csv_to_json_empty_csv(self, manager, tmp_path):
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("col_a,col_b\n")

    result = await manager._convert_csv_to_json(csv_file)

    parsed = json.loads(result)
    assert parsed == []

  @pytest.mark.asyncio
  async def test_csv_to_json_returns_utf8(self, manager, tmp_path):
    csv_file = tmp_path / "utf.csv"
    csv_file.write_text("city\nTokyo\nZurich\n")

    result = await manager._convert_csv_to_json(csv_file)

    assert result.decode("utf-8")  # must be valid UTF-8


# ===========================================================================
# _convert_json_to_csv
# ===========================================================================


@pytest.mark.unit
class TestConvertJsonToCsv:
  @pytest.mark.asyncio
  async def test_json_to_csv_returns_encoded_bytes(self, manager, tmp_path):
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"a": 1, "b": 2}, {"a": 3, "b": 4}]))

    result = await manager._convert_json_to_csv(json_file)

    assert isinstance(result, bytes)
    text = result.decode("utf-8")
    lines = text.strip().split("\n")
    assert len(lines) == 3  # header + 2 rows
    assert "a" in lines[0]
    assert "b" in lines[0]

  @pytest.mark.asyncio
  async def test_json_to_csv_empty_list(self, manager, tmp_path):
    json_file = tmp_path / "empty.json"
    json_file.write_text("[]")

    result = await manager._convert_json_to_csv(json_file)

    assert isinstance(result, bytes)
    text = result.decode("utf-8").strip()
    # Empty DataFrame produces just a newline or empty string
    assert len(text) <= 1  # just newline or empty


# ===========================================================================
# _convert_full_dump_to_zip
# ===========================================================================


@pytest.mark.unit
class TestConvertFullDumpToZip:
  @pytest.mark.asyncio
  async def test_produces_valid_zip(self, manager, tmp_path):
    dump_file = tmp_path / "my_graph"
    dump_file.write_bytes(b"LBUG_BINARY_CONTENT")

    result = await manager._convert_full_dump_to_zip(dump_file)

    assert isinstance(result, bytes)
    with zipfile.ZipFile(io.BytesIO(result), "r") as zf:
      names = zf.namelist()
      assert "database/my_graph.lbug" in names
      assert "metadata.json" in names

  @pytest.mark.asyncio
  async def test_metadata_json_contents(self, manager, tmp_path):
    dump_file = tmp_path / "graph123"
    dump_file.write_bytes(b"\x00\x01\x02")

    result = await manager._convert_full_dump_to_zip(dump_file)

    with zipfile.ZipFile(io.BytesIO(result), "r") as zf:
      meta = json.loads(zf.read("metadata.json"))
      assert meta["database_type"] == "ladybug"
      assert meta["format"] == "full_dump"
      assert "export_timestamp" in meta
      assert "database_files" in meta["contents"]
      assert "metadata" in meta["contents"]

  @pytest.mark.asyncio
  async def test_database_file_preserved(self, manager, tmp_path):
    original_data = b"IMPORTANT_DB_DATA_" * 100
    dump_file = tmp_path / "dbfile"
    dump_file.write_bytes(original_data)

    result = await manager._convert_full_dump_to_zip(dump_file)

    with zipfile.ZipFile(io.BytesIO(result), "r") as zf:
      stored = zf.read("database/dbfile.lbug")
      assert stored == original_data


# ===========================================================================
# _convert_backup_format (routing)
# ===========================================================================


@pytest.mark.unit
class TestConvertBackupFormat:
  @pytest.mark.asyncio
  async def test_csv_to_json_routing(self, manager):
    with patch.object(
      manager, "_convert_csv_to_json", new_callable=AsyncMock
    ) as mock_conv:
      mock_conv.return_value = b'[{"x":1}]'

      data, ct, fn = await manager._convert_backup_format(
        b"csv_data", "csv", "json", "bk99"
      )

      mock_conv.assert_called_once()
      assert ct == "application/json"
      assert fn == "bk99.json"
      assert data == b'[{"x":1}]'

  @pytest.mark.asyncio
  async def test_json_to_csv_routing(self, manager):
    with patch.object(
      manager, "_convert_json_to_csv", new_callable=AsyncMock
    ) as mock_conv:
      mock_conv.return_value = b"a,b\n1,2\n"

      data, ct, fn = await manager._convert_backup_format(
        b"json_data", "json", "csv", "bk100"
      )

      mock_conv.assert_called_once()
      assert ct == "text/csv"
      assert fn == "bk100.csv"

  @pytest.mark.asyncio
  async def test_full_dump_to_zip_routing(self, manager):
    with patch.object(
      manager, "_convert_full_dump_to_zip", new_callable=AsyncMock
    ) as mock_conv:
      mock_conv.return_value = b"ZIP_BYTES"

      data, ct, fn = await manager._convert_backup_format(
        b"dump_data", "full_dump", "zip", "bk101"
      )

      mock_conv.assert_called_once()
      assert ct == "application/zip"
      assert fn == "bk101_database.lbug.zip"

  @pytest.mark.asyncio
  async def test_unsupported_conversion_raises(self, manager):
    with pytest.raises(ValueError, match="not supported"):
      await manager._convert_backup_format(b"data", "parquet", "csv", "bk102")

  @pytest.mark.asyncio
  async def test_same_format_unsupported_raises(self, manager):
    with pytest.raises(ValueError, match="not supported"):
      await manager._convert_backup_format(b"data", "csv", "parquet", "bk103")


# ===========================================================================
# get_backup_download_url
# ===========================================================================


@pytest.mark.unit
class TestGetBackupDownloadUrl:
  @pytest.mark.asyncio
  async def test_returns_presigned_url_for_completed_backup(self, manager):
    mock_backup = MagicMock()
    mock_backup.is_completed = True
    mock_backup.s3_bucket = "my-bucket"
    mock_backup.s3_key = "backups/graph1/file.zip"
    mock_backup.created_at = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)

    mock_s3_client = MagicMock()
    mock_s3_client.generate_presigned_url.return_value = (
      "https://s3.example.com/presigned"
    )
    manager.s3_adapter.s3_client = mock_s3_client

    with (
      patch("robosystems.database.session") as mock_sl,
      patch("robosystems.models.core.graph.graph_backup.GraphBackup") as mock_gb,
    ):
      mock_session = MagicMock()
      mock_sl.return_value = mock_session
      mock_gb.get_by_id_and_graph.return_value = mock_backup

      url = await manager.get_backup_download_url("graph1", "bk_abc", 7200)

      assert url == "https://s3.example.com/presigned"

  @pytest.mark.asyncio
  async def test_returns_none_for_missing_backup(self, manager):
    with (
      patch("robosystems.database.session") as mock_sl,
      patch("robosystems.models.core.graph.graph_backup.GraphBackup") as mock_gb,
    ):
      mock_session = MagicMock()
      mock_sl.return_value = mock_session
      mock_gb.get_by_id_and_graph.return_value = None

      url = await manager.get_backup_download_url("graph1", "bk_none")

      assert url is None

  @pytest.mark.asyncio
  async def test_returns_none_for_incomplete_backup(self, manager):
    mock_backup = MagicMock()
    mock_backup.is_completed = False
    mock_backup.status = "in_progress"

    with (
      patch("robosystems.database.session") as mock_sl,
      patch("robosystems.models.core.graph.graph_backup.GraphBackup") as mock_gb,
    ):
      mock_session = MagicMock()
      mock_sl.return_value = mock_session
      mock_gb.get_by_id_and_graph.return_value = mock_backup

      url = await manager.get_backup_download_url("graph1", "bk_inc")

      assert url is None

  @pytest.mark.asyncio
  async def test_localstack_hostname_replacement(self, manager):
    mock_backup = MagicMock()
    mock_backup.is_completed = True
    mock_backup.s3_bucket = "test-bucket"
    mock_backup.s3_key = "test-key"
    mock_backup.created_at = datetime(2025, 1, 1, tzinfo=UTC)

    mock_s3_client = MagicMock()
    mock_s3_client.generate_presigned_url.return_value = (
      "http://localstack:4566/test-bucket/test-key"
    )
    manager.s3_adapter.s3_client = mock_s3_client

    with (
      patch("robosystems.database.session") as mock_sl,
      patch("robosystems.models.core.graph.graph_backup.GraphBackup") as mock_gb,
    ):
      mock_session = MagicMock()
      mock_sl.return_value = mock_session
      mock_gb.get_by_id_and_graph.return_value = mock_backup

      url = await manager.get_backup_download_url("graph1", "bk_ls")

      assert "localhost:4566" in url
      assert "localstack:" not in url


# ===========================================================================
# download_backup
# ===========================================================================


@pytest.mark.unit
class TestDownloadBackup:
  @pytest.mark.asyncio
  async def test_download_without_conversion(self, manager, s3_adapter):
    s3_adapter.get_backup_metadata = AsyncMock(
      return_value={
        "backup_format": "json",
      }
    )
    s3_adapter.download_backup = AsyncMock(return_value=b'[{"a":1}]')

    data, ct, fn = await manager.download_backup("g1", "bk_1")

    assert data == b'[{"a":1}]'
    assert ct == "application/json"
    assert fn == "bk_1.json.zip"

  @pytest.mark.asyncio
  async def test_download_with_format_conversion(self, manager, s3_adapter):
    s3_adapter.get_backup_metadata = AsyncMock(
      return_value={
        "backup_format": "csv",
      }
    )
    s3_adapter.download_backup = AsyncMock(return_value=b"name,val\na,1\n")

    with patch.object(
      manager, "_convert_backup_format", new_callable=AsyncMock
    ) as mock_conv:
      mock_conv.return_value = (b'[{"name":"a"}]', "application/json", "bk_2.json")

      data, ct, fn = await manager.download_backup("g1", "bk_2", target_format="json")

      mock_conv.assert_called_once_with(b"name,val\na,1\n", "csv", "json", "bk_2")
      assert ct == "application/json"

  @pytest.mark.asyncio
  async def test_download_same_target_format_skips_conversion(
    self, manager, s3_adapter
  ):
    s3_adapter.get_backup_metadata = AsyncMock(
      return_value={
        "backup_format": "csv",
      }
    )
    s3_adapter.download_backup = AsyncMock(return_value=b"csv_data")

    data, ct, fn = await manager.download_backup("g1", "bk_3", target_format="csv")

    assert data == b"csv_data"
    assert ct == "text/csv"

  @pytest.mark.asyncio
  async def test_download_not_found_raises(self, manager, s3_adapter):
    s3_adapter.get_backup_metadata = AsyncMock(return_value=None)

    with pytest.raises(ValueError, match="not found"):
      await manager.download_backup("g1", "bk_missing")

  @pytest.mark.asyncio
  async def test_download_full_dump_format(self, manager, s3_adapter):
    s3_adapter.get_backup_metadata = AsyncMock(
      return_value={
        "backup_format": "full_dump",
      }
    )
    s3_adapter.download_backup = AsyncMock(return_value=b"binary_dump")

    data, ct, fn = await manager.download_backup("g1", "bk_fd")

    assert ct == "application/zip"
    assert "database.lbug.zip" in fn


# ===========================================================================
# create_backup (full flow)
# ===========================================================================


@pytest.mark.unit
class TestCreateBackup:
  @pytest.mark.asyncio
  async def test_create_backup_full_flow(self, manager, s3_adapter):
    mock_metadata = MagicMock(spec=BackupMetadata)
    mock_metadata.original_size = 5000
    mock_metadata.compression_ratio = 0.4

    s3_adapter.upload_backup = AsyncMock(return_value=mock_metadata)

    with (
      patch.object(
        manager, "_get_database_stats", new_callable=AsyncMock
      ) as mock_stats,
      patch.object(manager, "_export_database", new_callable=AsyncMock) as mock_export,
    ):
      mock_stats.return_value = {"node_count": 200, "relationship_count": 100}
      mock_export.return_value = (b"exported_data", ".lbug.zip")

      job = BackupJob(
        graph_id="test_graph",
        backup_format=BackupFormat.FULL_DUMP,
        backup_type=BackupType.FULL,
      )

      result = await manager.create_backup(job)

      assert result is mock_metadata
      mock_stats.assert_called_once_with("test_graph")
      mock_export.assert_called_once_with(
        "test_graph", BackupFormat.FULL_DUMP, BackupType.FULL
      )
      s3_adapter.upload_backup.assert_called_once()

      call_kwargs = s3_adapter.upload_backup.call_args
      assert call_kwargs.kwargs["graph_id"] == "test_graph"
      assert call_kwargs.kwargs["backup_data"] == b"exported_data"
      assert call_kwargs.kwargs["metadata"]["node_count"] == 200
      assert call_kwargs.kwargs["metadata"]["relationship_count"] == 100

  @pytest.mark.asyncio
  async def test_create_backup_propagates_stats_failure(self, manager):
    with patch.object(
      manager, "_get_database_stats", new_callable=AsyncMock
    ) as mock_stats:
      mock_stats.side_effect = ConnectionError("db unreachable")

      job = BackupJob(
        graph_id="test_graph",
      )

      with pytest.raises(ConnectionError, match="db unreachable"):
        await manager.create_backup(job)

  @pytest.mark.asyncio
  async def test_create_backup_propagates_export_failure(self, manager):
    with (
      patch.object(
        manager, "_get_database_stats", new_callable=AsyncMock
      ) as mock_stats,
      patch.object(manager, "_export_database", new_callable=AsyncMock) as mock_export,
    ):
      mock_stats.return_value = {"node_count": 0, "relationship_count": 0}
      mock_export.side_effect = RuntimeError("export exploded")

      job = BackupJob(
        graph_id="test_graph",
      )

      with pytest.raises(RuntimeError, match="export exploded"):
        await manager.create_backup(job)

  @pytest.mark.asyncio
  async def test_create_backup_metadata_fields(self, manager, s3_adapter):
    mock_metadata = MagicMock(spec=BackupMetadata)
    mock_metadata.original_size = 1000
    mock_metadata.compression_ratio = 0.5

    s3_adapter.upload_backup = AsyncMock(return_value=mock_metadata)

    with (
      patch.object(
        manager, "_get_database_stats", new_callable=AsyncMock
      ) as mock_stats,
      patch.object(manager, "_export_database", new_callable=AsyncMock) as mock_export,
    ):
      mock_stats.return_value = {"node_count": 10, "relationship_count": 5}
      mock_export.return_value = (b"data", ".csv.zip")

      job = BackupJob(
        graph_id="test_graph",
        backup_format=BackupFormat.CSV,
        backup_type=BackupType.FULL,
        compression=True,
      )

      await manager.create_backup(job)

      uploaded_metadata = s3_adapter.upload_backup.call_args.kwargs["metadata"]
      assert uploaded_metadata["backup_format"] == "csv"
      assert uploaded_metadata["database_engine"] == "graph"
      assert uploaded_metadata["compression_enabled"] is True

  @pytest.mark.asyncio
  async def test_create_backup_incremental_type(self, manager, s3_adapter):
    mock_metadata = MagicMock(spec=BackupMetadata)
    mock_metadata.original_size = 100
    mock_metadata.compression_ratio = 0.3

    s3_adapter.upload_backup = AsyncMock(return_value=mock_metadata)

    with (
      patch.object(
        manager, "_get_database_stats", new_callable=AsyncMock
      ) as mock_stats,
      patch.object(manager, "_export_database", new_callable=AsyncMock) as mock_export,
    ):
      mock_stats.return_value = {"node_count": 1, "relationship_count": 0}
      mock_export.return_value = (b"inc_data", ".json.zip")

      job = BackupJob(
        graph_id="test_graph",
        backup_format=BackupFormat.JSON,
        backup_type=BackupType.INCREMENTAL,
      )

      await manager.create_backup(job)

      assert s3_adapter.upload_backup.call_args.kwargs["backup_type"] == "incremental"


# ===========================================================================
# restore_backup
# ===========================================================================


@pytest.mark.unit
class TestRestoreBackup:
  def _make_restore_job(self, graph_id="test_graph", **overrides):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.s3_key = "backups/test_graph/file.zip"
    metadata.timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    metadata.backup_type = "full"
    defaults = {
      "graph_id": graph_id,
      "backup_metadata": metadata,
      "backup_format": BackupFormat.FULL_DUMP,
      "create_new_database": True,
      "drop_existing": False,
      "verify_after_restore": True,
    }
    defaults.update(overrides)
    return RestoreJob(**defaults), metadata

  @pytest.mark.asyncio
  async def test_restore_full_flow(self, manager, s3_adapter):
    job, metadata = self._make_restore_job()

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"backup_bytes")

    with (
      patch.object(
        manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_val,
      patch.object(
        manager, "_ensure_database_exists", new_callable=AsyncMock
      ) as mock_ensure,
      patch.object(
        manager, "_import_backup_data", new_callable=AsyncMock
      ) as mock_import,
      patch.object(manager, "_verify_restore", new_callable=AsyncMock) as mock_verify,
    ):
      mock_val.return_value = True
      mock_verify.return_value = True

      result = await manager.restore_backup(job)

      assert result is True
      mock_val.assert_called_once_with(b"backup_bytes", metadata)
      mock_ensure.assert_called_once_with("test_graph")
      mock_import.assert_called_once_with(
        "test_graph", b"backup_bytes", BackupFormat.FULL_DUMP, None, True
      )
      mock_verify.assert_called_once_with("test_graph", metadata)

  @pytest.mark.asyncio
  async def test_restore_threads_system_backup_opt_out(self, manager, s3_adapter):
    """`create_system_backup=False` must reach the importer.

    Only `_import_full_dump` snapshots the database before overwriting it, so
    a job that drops the flag here would force a snapshot the caller declined.
    """
    job, _ = self._make_restore_job(create_system_backup=False)

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"backup_bytes")

    with (
      patch.object(
        manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_val,
      patch.object(manager, "_ensure_database_exists", new_callable=AsyncMock),
      patch.object(
        manager, "_import_backup_data", new_callable=AsyncMock
      ) as mock_import,
      patch.object(manager, "_verify_restore", new_callable=AsyncMock) as mock_verify,
    ):
      mock_val.return_value = True
      mock_verify.return_value = True

      assert await manager.restore_backup(job) is True
      assert mock_import.call_args.args[4] is False

  @pytest.mark.asyncio
  async def test_restore_with_drop_existing(self, manager, s3_adapter):
    job, metadata = self._make_restore_job(drop_existing=True)

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"data")

    with (
      patch.object(
        manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_val,
      patch.object(
        manager, "_drop_database_if_exists", new_callable=AsyncMock
      ) as mock_drop,
      patch.object(manager, "_ensure_database_exists", new_callable=AsyncMock),
      patch.object(manager, "_import_backup_data", new_callable=AsyncMock),
      patch.object(manager, "_verify_restore", new_callable=AsyncMock) as mock_verify,
    ):
      mock_val.return_value = True
      mock_verify.return_value = True

      result = await manager.restore_backup(job)

      assert result is True
      mock_drop.assert_called_once_with("test_graph")

  @pytest.mark.asyncio
  async def test_restore_without_verify(self, manager, s3_adapter):
    job, _ = self._make_restore_job(verify_after_restore=False)

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"data")

    with (
      patch.object(
        manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_val,
      patch.object(manager, "_ensure_database_exists", new_callable=AsyncMock),
      patch.object(manager, "_import_backup_data", new_callable=AsyncMock),
      patch.object(manager, "_verify_restore", new_callable=AsyncMock) as mock_verify,
    ):
      mock_val.return_value = True

      result = await manager.restore_backup(job)

      assert result is True
      mock_verify.assert_not_called()

  @pytest.mark.asyncio
  async def test_restore_integrity_failure(self, manager, s3_adapter):
    job, _ = self._make_restore_job()

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"corrupt_data")

    with patch.object(
      manager, "_validate_backup_integrity", new_callable=AsyncMock
    ) as mock_val:
      mock_val.return_value = False

      with pytest.raises(ValueError, match="Backup integrity check failed"):
        await manager.restore_backup(job)

  @pytest.mark.asyncio
  async def test_restore_verification_failure_returns_false(self, manager, s3_adapter):
    job, metadata = self._make_restore_job()

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"data")

    with (
      patch.object(
        manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_val,
      patch.object(manager, "_ensure_database_exists", new_callable=AsyncMock),
      patch.object(manager, "_import_backup_data", new_callable=AsyncMock),
      patch.object(manager, "_verify_restore", new_callable=AsyncMock) as mock_verify,
    ):
      mock_val.return_value = True
      mock_verify.return_value = False

      result = await manager.restore_backup(job)

      assert result is False

  @pytest.mark.asyncio
  async def test_restore_downloads_by_timestamp_when_no_s3_key(
    self, manager, s3_adapter
  ):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.s3_key = None  # no s3_key
    metadata.timestamp = datetime(2025, 3, 1, tzinfo=UTC)
    metadata.backup_type = "full"

    job = RestoreJob(
      graph_id="test_graph",
      backup_metadata=metadata,
      backup_format=BackupFormat.FULL_DUMP,
    )

    s3_adapter.download_backup_by_timestamp = AsyncMock(return_value=b"ts_data")

    with (
      patch.object(
        manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_val,
      patch.object(manager, "_ensure_database_exists", new_callable=AsyncMock),
      patch.object(manager, "_import_backup_data", new_callable=AsyncMock),
      patch.object(manager, "_verify_restore", new_callable=AsyncMock) as mock_verify,
    ):
      mock_val.return_value = True
      mock_verify.return_value = True

      result = await manager.restore_backup(job)

      assert result is True
      s3_adapter.download_backup_by_timestamp.assert_called_once_with(
        graph_id="test_graph",
        timestamp=metadata.timestamp,
        backup_type="full",
      )

  @pytest.mark.asyncio
  async def test_restore_without_create_new_database(self, manager, s3_adapter):
    job, metadata = self._make_restore_job(create_new_database=False)

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"data")

    with (
      patch.object(
        manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_val,
      patch.object(
        manager, "_ensure_database_exists", new_callable=AsyncMock
      ) as mock_ensure,
      patch.object(manager, "_import_backup_data", new_callable=AsyncMock),
      patch.object(manager, "_verify_restore", new_callable=AsyncMock) as mock_verify,
    ):
      mock_val.return_value = True
      mock_verify.return_value = True

      await manager.restore_backup(job)

      mock_ensure.assert_not_called()


# ===========================================================================
# delete_old_backups
# ===========================================================================


@pytest.mark.unit
class TestDeleteOldBackups:
  @pytest.mark.asyncio
  async def test_deletes_expired_backups(self, manager, s3_adapter):
    now = datetime.now(UTC)
    old_date = now - timedelta(days=100)
    recent_date = now - timedelta(days=10)

    backups = [
      {
        "key": "test_graph/backup-20240101_120000-full.zip",
        "graph_id": "test_graph",
        "backup_type": "full",
        "last_modified": old_date,
        "size": 1000,
      },
      {
        "key": "test_graph/backup-20250101_120000-full.zip",
        "graph_id": "test_graph",
        "backup_type": "full",
        "last_modified": recent_date,
        "size": 2000,
      },
    ]

    s3_adapter.list_backups = AsyncMock(return_value=backups)
    s3_adapter.delete_backup = AsyncMock(return_value=True)

    deleted = await manager.delete_old_backups("test_graph", 30)

    assert deleted == 1
    s3_adapter.delete_backup.assert_called_once()

  @pytest.mark.asyncio
  async def test_no_backups_returns_zero(self, manager, s3_adapter):
    s3_adapter.list_backups = AsyncMock(return_value=[])

    deleted = await manager.delete_old_backups("test_graph", 30)

    assert deleted == 0

  @pytest.mark.asyncio
  async def test_all_recent_returns_zero(self, manager, s3_adapter):
    now = datetime.now(UTC)
    recent = now - timedelta(days=5)

    backups = [
      {
        "key": "g/backup-20250601_120000-full.zip",
        "graph_id": "g",
        "backup_type": "full",
        "last_modified": recent,
        "size": 500,
      },
    ]

    s3_adapter.list_backups = AsyncMock(return_value=backups)
    s3_adapter.delete_backup = AsyncMock(return_value=True)

    deleted = await manager.delete_old_backups("g", 30)

    assert deleted == 0
    s3_adapter.delete_backup.assert_not_called()

  @pytest.mark.asyncio
  async def test_failed_delete_not_counted(self, manager, s3_adapter):
    now = datetime.now(UTC)
    old_date = now - timedelta(days=100)

    backups = [
      {
        "key": "g/backup-20240101_120000-full.zip",
        "graph_id": "g",
        "backup_type": "full",
        "last_modified": old_date,
        "size": 1000,
      },
    ]

    s3_adapter.list_backups = AsyncMock(return_value=backups)
    s3_adapter.delete_backup = AsyncMock(return_value=False)

    deleted = await manager.delete_old_backups("g", 30)

    assert deleted == 0

  @pytest.mark.asyncio
  async def test_delete_exception_handled_gracefully(self, manager, s3_adapter):
    now = datetime.now(UTC)
    old_date = now - timedelta(days=200)

    backups = [
      {
        "key": "g/backup-20230101_120000-full.zip",
        "graph_id": "g",
        "backup_type": "full",
        "last_modified": old_date,
        "size": 500,
      },
    ]

    s3_adapter.list_backups = AsyncMock(return_value=backups)
    s3_adapter.delete_backup = AsyncMock(side_effect=Exception("S3 error"))

    deleted = await manager.delete_old_backups("g", 30)

    assert deleted == 0


# ===========================================================================
# _get_database_stats
# ===========================================================================


@pytest.mark.unit
class TestGetDatabaseStats:
  @pytest.mark.asyncio
  async def test_returns_node_and_relationship_counts(self, manager):
    mock_repo = _mock_repository()

    async def side_effect(query):
      if "count(n)" in query:
        return {"count": 42}
      if "count(r)" in query:
        return {"count": 17}
      return None

    mock_repo.execute_single = AsyncMock(side_effect=side_effect)

    with patch(
      "robosystems.operations.graph.engine.backup_manager.get_universal_repository",
      new_callable=AsyncMock,
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repo

      stats = await manager._get_database_stats("test_graph")

      assert stats == {"node_count": 42, "relationship_count": 17}

  @pytest.mark.asyncio
  async def test_returns_zero_when_results_are_none(self, manager):
    mock_repo = _mock_repository()
    mock_repo.execute_single = AsyncMock(return_value=None)

    with patch(
      "robosystems.operations.graph.engine.backup_manager.get_universal_repository",
      new_callable=AsyncMock,
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repo

      stats = await manager._get_database_stats("empty_graph")

      assert stats == {"node_count": 0, "relationship_count": 0}

  @pytest.mark.asyncio
  async def test_calls_get_universal_repository_with_read(self, manager):
    mock_repo = _mock_repository()

    with patch(
      "robosystems.operations.graph.engine.backup_manager.get_universal_repository",
      new_callable=AsyncMock,
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repo

      await manager._get_database_stats("my_graph")

      mock_get_repo.assert_called_once_with("my_graph", operation_type="read")


# ===========================================================================
# _export_database (routing)
# ===========================================================================


@pytest.mark.unit
class TestExportDatabase:
  @pytest.mark.asyncio
  async def test_routes_to_csv_export(self, manager):
    with patch.object(manager, "_export_to_csv", new_callable=AsyncMock) as mock_exp:
      mock_exp.return_value = (b"csv_data", ".csv.zip")

      result = await manager._export_database("g1", BackupFormat.CSV, BackupType.FULL)

      mock_exp.assert_called_once_with("g1", BackupType.FULL)
      assert result == (b"csv_data", ".csv.zip")

  @pytest.mark.asyncio
  async def test_routes_to_json_export(self, manager):
    with patch.object(manager, "_export_to_json", new_callable=AsyncMock) as mock_exp:
      mock_exp.return_value = (b"json_data", ".json.zip")

      result = await manager._export_database("g1", BackupFormat.JSON, BackupType.FULL)

      mock_exp.assert_called_once_with("g1", BackupType.FULL)
      assert result == (b"json_data", ".json.zip")

  @pytest.mark.asyncio
  async def test_routes_to_parquet_export(self, manager):
    with patch.object(
      manager, "_export_to_parquet", new_callable=AsyncMock
    ) as mock_exp:
      mock_exp.return_value = (b"pq_data", ".parquet.zip")

      result = await manager._export_database(
        "g1", BackupFormat.PARQUET, BackupType.FULL
      )

      mock_exp.assert_called_once_with("g1", BackupType.FULL)
      assert result == (b"pq_data", ".parquet.zip")

  @pytest.mark.asyncio
  async def test_routes_to_full_dump_export(self, manager):
    with patch.object(manager, "_export_full_dump", new_callable=AsyncMock) as mock_exp:
      mock_exp.return_value = (b"dump_data", ".lbug.zip")

      result = await manager._export_database(
        "g1", BackupFormat.FULL_DUMP, BackupType.FULL
      )

      mock_exp.assert_called_once_with("g1", BackupType.FULL)
      assert result == (b"dump_data", ".lbug.zip")

  @pytest.mark.asyncio
  async def test_raises_for_unsupported_format(self, manager):
    with pytest.raises(ValueError, match="Unsupported backup format"):
      await manager._export_database("g1", "unknown_format", BackupType.FULL)


# ===========================================================================
# list_backups
# ===========================================================================


@pytest.mark.unit
class TestListBackups:
  @pytest.mark.asyncio
  async def test_delegates_to_s3_adapter(self, manager, s3_adapter):
    expected = [{"id": "b1"}, {"id": "b2"}]
    s3_adapter.list_backups = AsyncMock(return_value=expected)

    result = await manager.list_backups("test_graph")

    assert result == expected
    s3_adapter.list_backups.assert_called_once_with("test_graph")

  @pytest.mark.asyncio
  async def test_delegates_with_none_graph_id(self, manager, s3_adapter):
    s3_adapter.list_backups = AsyncMock(return_value=[])

    result = await manager.list_backups(None)

    s3_adapter.list_backups.assert_called_once_with(None)
    assert result == []

  @pytest.mark.asyncio
  async def test_returns_empty_list(self, manager, s3_adapter):
    s3_adapter.list_backups = AsyncMock(return_value=[])

    result = await manager.list_backups("no_backups")

    assert result == []


# ===========================================================================
# _verify_restore
# ===========================================================================


@pytest.mark.unit
class TestVerifyRestore:
  @pytest.mark.asyncio
  async def test_returns_true_when_counts_match(self, manager):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.node_count = 100
    metadata.relationship_count = 50

    with patch.object(
      manager, "_get_database_stats", new_callable=AsyncMock
    ) as mock_stats:
      mock_stats.return_value = {"node_count": 100, "relationship_count": 50}

      result = await manager._verify_restore("g1", metadata)

      assert result is True

  @pytest.mark.asyncio
  async def test_returns_false_when_node_count_differs(self, manager):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.node_count = 100
    metadata.relationship_count = 50

    with patch.object(
      manager, "_get_database_stats", new_callable=AsyncMock
    ) as mock_stats:
      mock_stats.return_value = {"node_count": 99, "relationship_count": 50}

      result = await manager._verify_restore("g1", metadata)

      assert result is False

  @pytest.mark.asyncio
  async def test_returns_false_when_relationship_count_differs(self, manager):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.node_count = 100
    metadata.relationship_count = 50

    with patch.object(
      manager, "_get_database_stats", new_callable=AsyncMock
    ) as mock_stats:
      mock_stats.return_value = {"node_count": 100, "relationship_count": 49}

      result = await manager._verify_restore("g1", metadata)

      assert result is False

  @pytest.mark.asyncio
  async def test_returns_false_on_exception(self, manager):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.node_count = 10
    metadata.relationship_count = 5

    with patch.object(
      manager, "_get_database_stats", new_callable=AsyncMock
    ) as mock_stats:
      mock_stats.side_effect = Exception("db error")

      result = await manager._verify_restore("g1", metadata)

      assert result is False


# ===========================================================================
# _validate_backup_integrity
# ===========================================================================


@pytest.mark.unit
class TestValidateBackupIntegrity:
  @pytest.mark.asyncio
  async def test_matching_checksum_returns_true(self, manager):
    data = b"test backup data for integrity"
    expected = hashlib.sha256(data).hexdigest()

    metadata = MagicMock()
    metadata.checksum = expected

    result = await manager._validate_backup_integrity(data, metadata)
    assert result is True

  @pytest.mark.asyncio
  async def test_mismatched_checksum_returns_false(self, manager):
    metadata = MagicMock()
    metadata.checksum = "aaaa" * 16

    result = await manager._validate_backup_integrity(b"any data", metadata)
    assert result is False

  @pytest.mark.asyncio
  async def test_none_checksum_returns_false(self, manager):
    metadata = MagicMock()
    metadata.checksum = None

    result = await manager._validate_backup_integrity(b"data", metadata)
    assert result is False

  @pytest.mark.asyncio
  async def test_empty_data_with_matching_checksum(self, manager):
    data = b""
    expected = hashlib.sha256(data).hexdigest()

    metadata = MagicMock()
    metadata.checksum = expected

    result = await manager._validate_backup_integrity(data, metadata)
    assert result is True


# ===========================================================================
# RestoreJob validation
# ===========================================================================


@pytest.mark.unit
class TestRestoreJobValidation:
  def test_csv_format_raises(self):
    metadata = MagicMock()
    with pytest.raises(ValueError, match="Restore is only supported for full dump"):
      RestoreJob(
        graph_id="test_graph",
        backup_metadata=metadata,
        backup_format=BackupFormat.CSV,
      )

  def test_json_format_raises(self):
    metadata = MagicMock()
    with pytest.raises(ValueError, match="Restore is only supported for full dump"):
      RestoreJob(
        graph_id="test_graph",
        backup_metadata=metadata,
        backup_format=BackupFormat.JSON,
      )

  def test_parquet_format_raises(self):
    metadata = MagicMock()
    with pytest.raises(ValueError, match="Restore is only supported for full dump"):
      RestoreJob(
        graph_id="test_graph",
        backup_metadata=metadata,
        backup_format=BackupFormat.PARQUET,
      )

  def test_full_dump_format_succeeds(self):
    metadata = MagicMock()
    job = RestoreJob(
      graph_id="test_graph",
      backup_metadata=metadata,
      backup_format=BackupFormat.FULL_DUMP,
    )
    assert job.backup_format == BackupFormat.FULL_DUMP


# ===========================================================================
# _drop_database_if_exists
# ===========================================================================


@pytest.mark.unit
class TestDropDatabaseIfExists:
  @pytest.mark.asyncio
  async def test_removes_file_database(self, manager):
    with (
      patch(
        "robosystems.operations.graph.engine.backup_manager.MultiTenantUtils"
      ) as mock_mt,
      patch("os.path.exists", return_value=True),
      patch("os.path.isfile", return_value=True),
      patch("os.remove") as mock_remove,
    ):
      mock_mt.get_database_path_for_graph.return_value = "/data/test.lbug"

      await manager._drop_database_if_exists("test_graph")

      mock_remove.assert_called_once_with("/data/test.lbug")

  @pytest.mark.asyncio
  async def test_removes_directory_database(self, manager):
    with (
      patch(
        "robosystems.operations.graph.engine.backup_manager.MultiTenantUtils"
      ) as mock_mt,
      patch("os.path.exists", return_value=True),
      patch("os.path.isfile", return_value=False),
      patch("shutil.rmtree") as mock_rmtree,
    ):
      mock_mt.get_database_path_for_graph.return_value = "/data/test_dir"

      await manager._drop_database_if_exists("test_graph")

      mock_rmtree.assert_called_once_with("/data/test_dir")

  @pytest.mark.asyncio
  async def test_noop_when_path_not_exists(self, manager):
    with (
      patch(
        "robosystems.operations.graph.engine.backup_manager.MultiTenantUtils"
      ) as mock_mt,
      patch("os.path.exists", return_value=False),
      patch("os.remove") as mock_remove,
      patch("shutil.rmtree") as mock_rmtree,
    ):
      mock_mt.get_database_path_for_graph.return_value = "/data/nothing"

      await manager._drop_database_if_exists("test_graph")

      mock_remove.assert_not_called()
      mock_rmtree.assert_not_called()


# ===========================================================================
# _ensure_database_exists
# ===========================================================================


@pytest.mark.unit
class TestEnsureDatabaseExists:
  @pytest.mark.asyncio
  async def test_creates_database_via_repository(self, manager):
    mock_repo = _mock_repository()

    with patch(
      "robosystems.operations.graph.engine.backup_manager.get_universal_repository",
      new_callable=AsyncMock,
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repo

      await manager._ensure_database_exists("new_graph")

      mock_get_repo.assert_called_once_with("new_graph", operation_type="write")
      mock_repo.execute_query.assert_called_once()


# ===========================================================================
# export_backup
# ===========================================================================


@pytest.mark.unit
class TestExportBackup:
  @pytest.mark.asyncio
  async def test_export_returns_data(self, manager, s3_adapter):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.metadata = {}
    metadata.s3_key = "backups/g1/file.zip"
    metadata.checksum = hashlib.sha256(b"original_data").hexdigest()

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"original_data")

    result = await manager.export_backup(metadata)

    assert result == b"original_data"

  @pytest.mark.asyncio
  async def test_export_downloads_by_timestamp_when_no_s3_key(
    self, manager, s3_adapter
  ):
    data = b"ts_backup"
    metadata = MagicMock(spec=BackupMetadata)
    metadata.metadata = {}
    metadata.s3_key = None
    metadata.graph_id = "g2"
    metadata.timestamp = datetime(2025, 6, 1, tzinfo=UTC)
    metadata.backup_type = "full"
    metadata.checksum = hashlib.sha256(data).hexdigest()

    s3_adapter.download_backup_by_timestamp = AsyncMock(return_value=data)

    result = await manager.export_backup(metadata)

    assert result == data
    s3_adapter.download_backup_by_timestamp.assert_called_once()

  @pytest.mark.asyncio
  async def test_export_raises_on_integrity_failure(self, manager, s3_adapter):
    metadata = MagicMock(spec=BackupMetadata)
    metadata.metadata = {}
    metadata.s3_key = "key"
    metadata.checksum = "wrong_checksum"

    s3_adapter.download_backup_by_key = AsyncMock(return_value=b"data")

    with pytest.raises(ValueError, match="Backup integrity check failed"):
      await manager.export_backup(metadata)


# ===========================================================================
# _convert_backup_to_format
# ===========================================================================


@pytest.mark.unit
class TestConvertBackupToFormat:
  @pytest.mark.asyncio
  async def test_original_format_returns_data_unchanged(self, manager):
    metadata = MagicMock()
    result = await manager._convert_backup_to_format(b"my_data", "original", metadata)
    assert result == b"my_data"

  @pytest.mark.asyncio
  async def test_non_original_format_returns_data_unchanged_for_now(self, manager):
    metadata = MagicMock()
    result = await manager._convert_backup_to_format(b"my_data", "csv", metadata)
    # Current implementation returns original data for non-original formats
    assert result == b"my_data"


# ===========================================================================
# BackupManager property lazy-init
# ===========================================================================


@pytest.mark.unit
class TestBackupManagerProperties:
  def test_s3_adapter_lazy_init(self):
    manager = BackupManager(s3_adapter=None)
    with patch(
      "robosystems.operations.graph.engine.backup_manager.S3BackupAdapter"
    ) as mock_cls:
      mock_instance = MagicMock()
      mock_cls.return_value = mock_instance

      adapter = manager.s3_adapter

      assert adapter is mock_instance
      mock_cls.assert_called_once()

  def test_s3_adapter_returns_provided(self):
    provided = MagicMock(spec=S3BackupAdapter)
    manager = BackupManager(s3_adapter=provided)

    assert manager.s3_adapter is provided

  def test_graph_router_lazy_init(self):
    manager = BackupManager(s3_adapter=MagicMock())

    with patch("robosystems.middleware.graph.router.get_graph_router") as mock_get:
      mock_router = MagicMock()
      mock_get.return_value = mock_router

      router = manager.graph_router

      assert router is mock_router
      mock_get.assert_called_once()

  def test_graph_router_returns_provided(self):
    provided_router = MagicMock()
    manager = BackupManager(s3_adapter=MagicMock(), graph_router=provided_router)

    assert manager.graph_router is provided_router


# ===========================================================================
# BackupJob validation edge cases
# ===========================================================================


@pytest.mark.unit
class TestBackupJobValidation:
  def test_every_format_is_accepted(self):
    for backup_format in BackupFormat:
      job = BackupJob(graph_id="test_graph", backup_format=backup_format)
      assert job.backup_format == backup_format

  def test_invalid_graph_id_raises(self):
    with pytest.raises(ValueError):
      BackupJob(graph_id="invalid id spaces")

  def test_timestamp_auto_assigned(self):
    before = datetime.now(UTC)
    job = BackupJob(graph_id="test_graph")
    after = datetime.now(UTC)

    assert job.timestamp is not None
    assert before <= job.timestamp <= after

  def test_explicit_timestamp_preserved(self):
    ts = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
    job = BackupJob(graph_id="test_graph", timestamp=ts)

    assert job.timestamp == ts


# ===========================================================================
# _export_full_dump
# ===========================================================================


@pytest.mark.unit
class TestExportFullDump:
  @pytest.mark.asyncio
  async def test_successful_export(self, manager):
    mock_graph_client = MagicMock()
    mock_graph_client.download_backup = AsyncMock(
      return_value={"backup_data": b"binary_dump", "size_bytes": 11}
    )

    mock_router = MagicMock()
    mock_router.get_repository = AsyncMock(return_value=mock_graph_client)
    manager._graph_router = mock_router

    data, ext = await manager._export_full_dump("test_graph", BackupType.FULL)

    assert data == b"binary_dump"
    assert ext == ".lbug.zip"

  @pytest.mark.asyncio
  async def test_missing_backup_data_raises(self, manager):
    mock_graph_client = MagicMock()
    mock_graph_client.download_backup = AsyncMock(return_value={})

    mock_router = MagicMock()
    mock_router.get_repository = AsyncMock(return_value=mock_graph_client)
    manager._graph_router = mock_router

    with pytest.raises(RuntimeError, match="Failed to get backup from Graph API"):
      await manager._export_full_dump("test_graph", BackupType.FULL)

  @pytest.mark.asyncio
  async def test_none_response_raises(self, manager):
    mock_graph_client = MagicMock()
    mock_graph_client.download_backup = AsyncMock(return_value=None)

    mock_router = MagicMock()
    mock_router.get_repository = AsyncMock(return_value=mock_graph_client)
    manager._graph_router = mock_router

    with pytest.raises(RuntimeError, match="Failed to get backup from Graph API"):
      await manager._export_full_dump("test_graph", BackupType.FULL)

  @pytest.mark.asyncio
  async def test_graph_api_error_propagates(self, manager):
    mock_graph_client = MagicMock()
    mock_graph_client.download_backup = AsyncMock(
      side_effect=ConnectionError("graph-api unreachable")
    )

    mock_router = MagicMock()
    mock_router.get_repository = AsyncMock(return_value=mock_graph_client)
    manager._graph_router = mock_router

    with pytest.raises(ConnectionError, match="graph-api unreachable"):
      await manager._export_full_dump("test_graph", BackupType.FULL)


# ===========================================================================
# _import_backup_data (routing)
# ===========================================================================


@pytest.mark.unit
class TestImportBackupData:
  @pytest.mark.asyncio
  async def test_routes_csv(self, manager):
    with patch.object(manager, "_import_from_csv", new_callable=AsyncMock) as mock_imp:
      await manager._import_backup_data("g1", b"csv_data", BackupFormat.CSV, None)
      mock_imp.assert_called_once_with("g1", b"csv_data", None)

  @pytest.mark.asyncio
  async def test_routes_json(self, manager):
    with patch.object(manager, "_import_from_json", new_callable=AsyncMock) as mock_imp:
      await manager._import_backup_data("g1", b"json_data", BackupFormat.JSON, None)
      mock_imp.assert_called_once_with("g1", b"json_data", None)

  @pytest.mark.asyncio
  async def test_routes_parquet(self, manager):
    with patch.object(
      manager, "_import_from_parquet", new_callable=AsyncMock
    ) as mock_imp:
      await manager._import_backup_data("g1", b"pq_data", BackupFormat.PARQUET, None)
      mock_imp.assert_called_once_with("g1", b"pq_data", None)

  @pytest.mark.asyncio
  async def test_routes_full_dump(self, manager):
    with patch.object(manager, "_import_full_dump", new_callable=AsyncMock) as mock_imp:
      await manager._import_backup_data(
        "g1", b"dump_data", BackupFormat.FULL_DUMP, None
      )
      mock_imp.assert_called_once_with("g1", b"dump_data", None, True)

  @pytest.mark.asyncio
  async def test_full_dump_receives_system_backup_flag(self, manager):
    """Full dump is the only importer that overwrites in place, so it is the
    only one that takes the snapshot decision."""
    with patch.object(manager, "_import_full_dump", new_callable=AsyncMock) as mock_imp:
      await manager._import_backup_data(
        "g1", b"dump_data", BackupFormat.FULL_DUMP, None, False
      )
      mock_imp.assert_called_once_with("g1", b"dump_data", None, False)

  @pytest.mark.asyncio
  async def test_raises_for_unsupported_format(self, manager):
    with pytest.raises(ValueError, match="Unsupported backup format"):
      await manager._import_backup_data("g1", b"data", "xml", None)

  @pytest.mark.asyncio
  async def test_passes_progress_tracker(self, manager):
    tracker = MagicMock()
    with patch.object(manager, "_import_from_csv", new_callable=AsyncMock) as mock_imp:
      await manager._import_backup_data("g1", b"csv_data", BackupFormat.CSV, tracker)
      mock_imp.assert_called_once_with("g1", b"csv_data", tracker)


# ===========================================================================
# health_check
# ===========================================================================


@pytest.mark.unit
class TestHealthCheck:
  def test_healthy_when_both_healthy(self, manager, s3_adapter):
    s3_adapter.health_check.return_value = {"status": "healthy"}

    mock_repo = _mock_repository()
    mock_repo.__enter__ = MagicMock(return_value=mock_repo)
    mock_repo.__exit__ = MagicMock(return_value=None)

    async def mock_get_repo(*args, **kwargs):
      return mock_repo

    with patch(
      "robosystems.middleware.graph.get_universal_repository",
      side_effect=mock_get_repo,
    ):
      result = manager.health_check()

    assert result["status"] == "healthy"
    assert result["s3"]["status"] == "healthy"
    assert result["graph_database"]["status"] == "healthy"

  def test_unhealthy_when_s3_unhealthy(self, manager, s3_adapter):
    s3_adapter.health_check.return_value = {"status": "unhealthy"}

    mock_repo = _mock_repository()
    mock_repo.__enter__ = MagicMock(return_value=mock_repo)
    mock_repo.__exit__ = MagicMock(return_value=None)

    async def mock_get_repo(*args, **kwargs):
      return mock_repo

    with patch(
      "robosystems.middleware.graph.get_universal_repository",
      side_effect=mock_get_repo,
    ):
      result = manager.health_check()

    assert result["status"] == "unhealthy"

  def test_unhealthy_when_graph_fails(self, manager, s3_adapter):
    s3_adapter.health_check.return_value = {"status": "healthy"}

    async def mock_get_repo(*args, **kwargs):
      raise Exception("db down")

    with patch(
      "robosystems.middleware.graph.get_universal_repository",
      side_effect=mock_get_repo,
    ):
      result = manager.health_check()

    assert result["status"] == "unhealthy"
    assert result["graph_database"]["status"] == "unhealthy"


# ===========================================================================
# create_backup_manager factory
# ===========================================================================


@pytest.mark.unit
class TestCreateBackupManagerFactory:
  def test_factory_returns_backup_manager(self):
    from robosystems.operations.graph.engine.backup_manager import create_backup_manager

    with patch(
      "robosystems.operations.graph.engine.backup_manager.S3BackupAdapter"
    ) as mock_s3:
      mock_s3.return_value = MagicMock()
      mgr = create_backup_manager()

      assert isinstance(mgr, BackupManager)
