"""Unit tests for graph limits API models."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.limits import (
  BackupLimits,
  ContentLimits,
  CopyOperationLimits,
  CreditLimits,
  GraphLimitsResponse,
  QueryLimits,
  RateLimits,
  StorageLimits,
  SubgraphLimits,
)


@pytest.mark.unit
class TestStorageLimits:
  def test_valid_limits(self):
    model = StorageLimits(
      current_usage_gb=2.5,
      max_storage_gb=10.0,
      approaching_limit=False,
    )
    assert model.current_usage_gb == 2.5
    assert model.approaching_limit is False

  def test_current_usage_optional(self):
    model = StorageLimits(max_storage_gb=10.0, approaching_limit=False)
    assert model.current_usage_gb is None


@pytest.mark.unit
class TestQueryLimits:
  def test_valid_limits(self):
    model = QueryLimits(
      max_timeout_seconds=30,
      chunk_size=1000,
      max_rows_per_query=10000,
      concurrent_queries=1,
    )
    assert model.max_timeout_seconds == 30


@pytest.mark.unit
class TestCopyOperationLimits:
  def test_valid_limits(self):
    model = CopyOperationLimits(
      max_file_size_gb=1.0,
      timeout_seconds=900,
      concurrent_operations=1,
      max_files_per_operation=100,
      daily_copy_operations=25,
      supported_formats=["parquet", "csv", "json"],
    )
    assert "parquet" in model.supported_formats


@pytest.mark.unit
class TestBackupLimits:
  def test_valid_limits(self):
    model = BackupLimits(
      max_backup_size_gb=10.0,
      backup_retention_days=7,
      max_backups_per_day=2,
    )
    assert model.max_backups_per_day == 2


@pytest.mark.unit
class TestRateLimits:
  def test_valid_limits(self):
    model = RateLimits(
      requests_per_minute=60,
      requests_per_hour=1000,
      burst_capacity=10,
    )
    assert model.burst_capacity == 10


@pytest.mark.unit
class TestCreditLimits:
  def test_valid_limits(self):
    model = CreditLimits(
      monthly_ai_credits=8000,
      current_balance=7500,
    )
    assert model.monthly_ai_credits == 8000


@pytest.mark.unit
class TestContentLimits:
  def test_valid_limits(self):
    model = ContentLimits(
      max_rows_per_copy=2000000,
      max_single_table_rows=5000000,
      chunk_size_rows=1000000,
    )
    assert model.max_rows_per_copy == 2000000


@pytest.mark.unit
class TestSubgraphLimits:
  def test_valid_limits(self):
    model = SubgraphLimits(
      current_count=1,
      max_allowed=3,
      remaining=2,
      approaching_limit=False,
    )
    assert model.remaining == 2

  def test_uncapped_tier(self):
    model = SubgraphLimits(current_count=7, approaching_limit=False)
    assert model.max_allowed is None
    assert model.remaining is None

  def test_negative_count_rejected(self):
    with pytest.raises(ValidationError):
      SubgraphLimits(current_count=-1, approaching_limit=False)


@pytest.mark.unit
class TestGraphLimitsResponse:
  def test_user_graph_limits(self):
    storage = StorageLimits(
      current_usage_gb=2.5,
      max_storage_gb=10.0,
      approaching_limit=False,
    )
    queries = QueryLimits(
      max_timeout_seconds=30,
      chunk_size=1000,
      max_rows_per_query=10000,
      concurrent_queries=1,
    )
    copy_ops = CopyOperationLimits(
      max_file_size_gb=1.0,
      timeout_seconds=900,
      concurrent_operations=1,
      max_files_per_operation=100,
      daily_copy_operations=25,
      supported_formats=["parquet", "csv"],
    )
    backups = BackupLimits(
      max_backup_size_gb=10.0,
      backup_retention_days=7,
      max_backups_per_day=2,
    )
    rate_limits = RateLimits(
      requests_per_minute=60,
      requests_per_hour=1000,
      burst_capacity=10,
    )
    credits = CreditLimits(
      monthly_ai_credits=8000,
      current_balance=7500,
    )

    model = GraphLimitsResponse(
      graph_id="kg1234567890abcdef",
      subscription_tier="ladybug-standard",
      graph_tier="ladybug-standard",
      is_shared_repository=False,
      storage=storage,
      queries=queries,
      copy_operations=copy_ops,
      backups=backups,
      rate_limits=rate_limits,
      credits=credits,
    )
    assert model.graph_id == "kg1234567890abcdef"
    assert model.is_shared_repository is False
    assert model.credits is not None
    assert model.content is None
    assert model.subgraphs is None

  def test_shared_repository_limits(self):
    storage = StorageLimits(max_storage_gb=100.0, approaching_limit=False)
    queries = QueryLimits(
      max_timeout_seconds=120,
      chunk_size=2000,
      max_rows_per_query=10000,
      concurrent_queries=1,
    )
    copy_ops = CopyOperationLimits(
      max_file_size_gb=5.0,
      timeout_seconds=600,
      concurrent_operations=2,
      max_files_per_operation=200,
      daily_copy_operations=50,
      supported_formats=["parquet"],
    )
    backups = BackupLimits(
      max_backup_size_gb=50.0,
      backup_retention_days=30,
      max_backups_per_day=4,
    )
    rate_limits = RateLimits(
      requests_per_minute=120,
      requests_per_hour=2000,
      burst_capacity=20,
    )

    model = GraphLimitsResponse(
      graph_id="sec",
      subscription_tier="ladybug-standard",
      graph_tier="ladybug-shared",
      is_shared_repository=True,
      storage=storage,
      queries=queries,
      copy_operations=copy_ops,
      backups=backups,
      rate_limits=rate_limits,
    )
    assert model.is_shared_repository is True
    assert model.credits is None
    assert model.content is None

  def test_json_round_trip(self):
    storage = StorageLimits(max_storage_gb=10.0, approaching_limit=False)
    queries = QueryLimits(
      max_timeout_seconds=30,
      chunk_size=1000,
      max_rows_per_query=10000,
      concurrent_queries=1,
    )
    copy_ops = CopyOperationLimits(
      max_file_size_gb=1.0,
      timeout_seconds=900,
      concurrent_operations=1,
      max_files_per_operation=100,
      daily_copy_operations=25,
      supported_formats=["parquet"],
    )
    backups = BackupLimits(
      max_backup_size_gb=10.0,
      backup_retention_days=7,
      max_backups_per_day=2,
    )
    rate_limits = RateLimits(
      requests_per_minute=60,
      requests_per_hour=1000,
      burst_capacity=10,
    )

    model = GraphLimitsResponse(
      graph_id="kg123",
      subscription_tier="ladybug-standard",
      graph_tier="ladybug-standard",
      is_shared_repository=False,
      storage=storage,
      queries=queries,
      copy_operations=copy_ops,
      backups=backups,
      rate_limits=rate_limits,
    )
    json_str = model.model_dump_json()
    restored = GraphLimitsResponse.model_validate_json(json_str)
    assert restored.graph_id == "kg123"
    assert restored.storage.max_storage_gb == 10.0
