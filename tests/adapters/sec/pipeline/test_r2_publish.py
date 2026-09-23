"""Tests for the SEC R2 publish asset and the snapshot stats file it writes."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.r2_publish import sec_lbug_r2_published
from robosystems.dagster.assets.shared_repositories.publish import publish_to_r2

ASSET = "robosystems.adapters.sec.pipeline.r2_publish"
HELPER = "robosystems.dagster.assets.shared_repositories.publish"
STATS = {"filers": 8525, "filings": 76816}


@pytest.mark.unit
class TestSecLbugR2Published:
  """The asset counts the graph before the cut and hands the stats on."""

  @patch(f"{ASSET}.publish_to_r2")
  @patch(f"{ASSET}.capture_snapshot_stats", new_callable=AsyncMock)
  @patch("robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion")
  @patch(f"{ASSET}.env")
  def test_captures_stats_then_publishes(
    self, mock_env, mock_get_client, mock_capture, mock_publish
  ):
    mock_env.ENVIRONMENT = "prod"
    client = AsyncMock()
    mock_get_client.return_value = client
    mock_capture.return_value = STATS
    mock_publish.return_value = MaterializeResult(metadata={})
    db = MagicMock()

    context = build_asset_context()
    sec_lbug_r2_published(context, db)

    mock_capture.assert_awaited_once_with(client)
    client.close.assert_awaited_once()
    mock_publish.assert_called_once_with(
      context, graph_id="sec", db_resource=db, stats=STATS
    )

  @patch(f"{ASSET}.publish_to_r2")
  @patch(f"{ASSET}.capture_snapshot_stats", new_callable=AsyncMock)
  @patch("robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion")
  @patch(f"{ASSET}.env")
  def test_stats_failure_still_publishes_the_download(
    self, mock_env, mock_get_client, mock_capture, mock_publish
  ):
    mock_env.ENVIRONMENT = "prod"
    mock_get_client.return_value = AsyncMock()
    mock_capture.side_effect = TimeoutError("count timed out")
    mock_publish.return_value = MaterializeResult(metadata={})

    sec_lbug_r2_published(build_asset_context(), MagicMock())

    assert mock_publish.call_args.kwargs["stats"] is None

  @patch(f"{ASSET}.publish_to_r2")
  @patch(f"{ASSET}.capture_snapshot_stats", new_callable=AsyncMock)
  @patch(f"{ASSET}.env")
  def test_dev_skips_the_count(self, mock_env, mock_capture, mock_publish):
    mock_env.ENVIRONMENT = "dev"
    mock_publish.return_value = MaterializeResult(metadata={})

    sec_lbug_r2_published(build_asset_context(), MagicMock())

    mock_capture.assert_not_awaited()
    assert mock_publish.call_args.kwargs["stats"] is None


@pytest.fixture
def r2_env():
  with (
    patch(f"{HELPER}.env") as env,
    patch(f"{HELPER}._run_r2_backup") as backup,
    patch(f"{HELPER}._upsert_r2_backup_record"),
    patch("boto3.client") as client_factory,
  ):
    env.ENVIRONMENT = "prod"
    env.R2_BUCKET_NAME = "robosystems-downloads"
    env.get_r2_config.return_value = {"endpoint_url": "https://r2.example"}
    backup.return_value = {
      "status": "completed",
      "result": {"original_size_bytes": 3000, "compression_ratio": 0.33},
    }
    client = MagicMock()
    client.head_object.return_value = {
      "ContentLength": 1000,
      "LastModified": datetime(2026, 9, 30, 4, 0, tzinfo=UTC),
    }
    client_factory.return_value = client
    yield SimpleR2(backup=backup, client=client)


class SimpleR2:
  def __init__(self, backup, client):
    self.backup = backup
    self.client = client


@pytest.mark.unit
class TestPublishToR2StatsFile:
  def test_writes_stats_beside_the_archive(self, r2_env):
    result = publish_to_r2(
      build_asset_context(), graph_id="sec", db_resource=MagicMock(), stats=STATS
    )

    r2_env.client.put_object.assert_called_once()
    kwargs = r2_env.client.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "robosystems-downloads"
    assert kwargs["Key"] == "downloads/sec/sec.stats.json"
    assert kwargs["ContentType"] == "application/json"
    body = json.loads(kwargs["Body"])
    assert body == {
      "graph_id": "sec",
      "archive_key": "downloads/sec/sec.lbug.zst",
      "compressed_size_bytes": 1000,
      "original_size_bytes": 3000,
      "snapshot_at": "2026-09-30T04:00:00+00:00",
      "stats": STATS,
    }
    assert result.metadata["stats_key"] == "downloads/sec/sec.stats.json"

  def test_no_stats_no_file(self, r2_env):
    result = publish_to_r2(
      build_asset_context(), graph_id="sec", db_resource=MagicMock()
    )
    r2_env.client.put_object.assert_not_called()
    assert result.metadata["stats_key"] == ""

  def test_failed_backup_writes_no_stats(self, r2_env):
    r2_env.backup.return_value = {"status": "failed", "error": "disk full"}
    with pytest.raises(RuntimeError, match="disk full"):
      publish_to_r2(
        build_asset_context(), graph_id="sec", db_resource=MagicMock(), stats=STATS
      )
    r2_env.client.put_object.assert_not_called()
