"""Test GraphUsage model functionality."""

import pytest
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import GraphUsage
from robosystems.models.iam.graph_usage import UsageEventType


class TestGraphUsage:
  """Test cases for GraphUsage model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session
    self.test_user_id = "test_user_123"
    self.test_graph_id = "test_graph_456"

  def test_create_usage_tracking_entry(self):
    """Test creating a basic usage tracking entry."""
    usage = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      billing_year=2024,
      billing_month=1,
      billing_day=15,
      billing_hour=10,
    )

    assert usage.user_id == self.test_user_id
    assert usage.graph_id == self.test_graph_id
    assert usage.event_type == "api_call"
    assert usage.graph_tier == "standard"
    assert usage.recorded_at is None  # Not set until session add

    self.session.add(usage)
    self.session.commit()

    assert usage.id is not None
    assert usage.recorded_at is not None

  def test_usage_event_type_enum_values(self):
    """Test all UsageEventType enum values."""
    # Storage events
    assert UsageEventType.STORAGE_SNAPSHOT.value == "storage_snapshot"
    assert UsageEventType.STORAGE_GROWTH.value == "storage_growth"
    assert UsageEventType.STORAGE_CLEANUP.value == "storage_cleanup"

    # Credit events
    assert UsageEventType.CREDIT_CONSUMPTION.value == "credit_consumption"
    assert UsageEventType.CREDIT_ALLOCATION.value == "credit_allocation"
    assert UsageEventType.CREDIT_REFUND.value == "credit_refund"

    # API events
    assert UsageEventType.API_CALL.value == "api_call"
    assert UsageEventType.QUERY_EXECUTION.value == "query_execution"
    assert UsageEventType.MCP_CALL.value == "mcp_call"
    assert UsageEventType.AGENT_CALL.value == "agent_call"
    assert UsageEventType.IMPORT_OPERATION.value == "import_operation"
    assert UsageEventType.BACKUP_OPERATION.value == "backup_operation"
    assert UsageEventType.SYNC_OPERATION.value == "sync_operation"
    assert UsageEventType.ANALYTICS_QUERY.value == "analytics_query"

    # Performance events
    assert UsageEventType.SLOW_QUERY.value == "slow_query"
    assert UsageEventType.HIGH_MEMORY.value == "high_memory"
    assert UsageEventType.ERROR_EVENT.value == "error_event"

  def test_record_storage_usage(self):
    """Test recording storage usage."""
    storage_bytes = 5 * 1024**3  # 5 GB in bytes

    usage = GraphUsage.record_storage_usage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="enterprise",
      storage_bytes=storage_bytes,
      session=self.session,
      storage_delta_gb=0.5,
      instance_id="i-1234567890",
      region="us-east-1",
    )

    assert usage.id is not None
    assert usage.event_type == UsageEventType.STORAGE_SNAPSHOT.value
    assert usage.storage_bytes == storage_bytes
    assert usage.storage_gb == 5.0
    assert usage.storage_delta_gb == 0.5
    assert usage.instance_id == "i-1234567890"
    assert usage.region == "us-east-1"
    assert usage.billing_year is not None
    assert usage.billing_month is not None
    assert usage.billing_day is not None
    assert usage.billing_hour is not None

  def test_record_storage_usage_without_auto_commit(self):
    """Test recording storage usage without auto-commit."""
    usage = GraphUsage.record_storage_usage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="standard",
      storage_bytes=1024**3,  # 1 GB
      session=self.session,
      auto_commit=False,
    )

    # Should be added to session but not committed
    assert usage in self.session.new
    self.session.rollback()

    # After rollback, should not be in database
    result = self.session.query(GraphUsage).filter_by(id=usage.id).first()
    assert result is None

  def test_record_storage_usage_with_error(self):
    """Test recording storage usage with database error."""
    with patch.object(self.session, "commit", side_effect=SQLAlchemyError("DB error")):
      with pytest.raises(SQLAlchemyError):
        GraphUsage.record_storage_usage(
          user_id=self.test_user_id,
          graph_id=self.test_graph_id,
          graph_tier="standard",
          storage_bytes=1024**3,
          session=self.session,
        )

  def test_record_credit_consumption(self):
    """Test recording credit consumption."""
    usage = GraphUsage.record_credit_consumption(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="premium",
      operation_type="agent_call",
      credits_consumed=Decimal("150.50"),
      base_credit_cost=Decimal("100.00"),
      session=self.session,
      duration_ms=2500,
      cached_operation=False,
      status_code=200,
      metadata={"model": "claude-4-opus", "tokens": 5000},
    )

    assert usage.id is not None
    assert usage.event_type == UsageEventType.CREDIT_CONSUMPTION.value
    assert usage.operation_type == "agent_call"
    assert usage.credits_consumed == Decimal("150.50")
    assert usage.base_credit_cost == Decimal("100.00")
    assert usage.duration_ms == 2500
    assert usage.cached_operation is False
    assert usage.status_code == 200
    assert "claude-4-opus" in usage.event_metadata

  def test_record_credit_consumption_without_metadata(self):
    """Test recording credit consumption without metadata."""
    usage = GraphUsage.record_credit_consumption(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="standard",
      operation_type="mcp_call",
      credits_consumed=Decimal("50"),
      base_credit_cost=Decimal("50"),
      session=self.session,
    )

    assert usage.event_metadata is None

  def test_record_api_usage(self):
    """Test recording API usage."""
    usage = GraphUsage.record_api_usage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="enterprise",
      operation_type="query",
      session=self.session,
      duration_ms=1500,
      status_code=200,
      request_size_kb=2.5,
      response_size_kb=15.8,
      cached_operation=True,
      user_agent="RoboSystemsClient/1.0",
      ip_address="192.168.1.1",
      metadata={"query_type": "cypher"},
    )

    assert usage.id is not None
    assert usage.event_type == UsageEventType.QUERY_EXECUTION.value
    assert usage.operation_type == "query"
    assert usage.duration_ms == 1500
    assert usage.status_code == 200
    assert usage.request_size_kb == 2.5
    assert usage.response_size_kb == 15.8
    assert usage.cached_operation is True
    assert usage.user_agent == "RoboSystemsClient/1.0"
    assert usage.ip_address == "192.168.1.1"
    assert "cypher" in usage.event_metadata

  def test_record_api_usage_with_error(self):
    """Test recording API usage with error information."""
    usage = GraphUsage.record_api_usage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="standard",
      operation_type="import",
      session=self.session,
      status_code=500,
      error_type="DatabaseError",
      error_message="Connection timeout",
    )

    assert usage.event_type == UsageEventType.IMPORT_OPERATION.value
    assert usage.status_code == 500
    assert usage.error_type == "DatabaseError"
    assert usage.error_message == "Connection timeout"

  def test_event_type_mapping_in_record_api_usage(self):
    """Test event type mapping for different operations."""
    operations_map = {
      "query": UsageEventType.QUERY_EXECUTION.value,
      "mcp_call": UsageEventType.MCP_CALL.value,
      "agent_call": UsageEventType.AGENT_CALL.value,
      "import": UsageEventType.IMPORT_OPERATION.value,
      "backup": UsageEventType.BACKUP_OPERATION.value,
      "sync": UsageEventType.SYNC_OPERATION.value,
      "analytics": UsageEventType.ANALYTICS_QUERY.value,
      "unknown_op": UsageEventType.API_CALL.value,  # Default
    }

    for operation, expected_event_type in operations_map.items():
      usage = GraphUsage.record_api_usage(
        user_id=self.test_user_id,
        graph_id=self.test_graph_id,
        graph_tier="standard",
        operation_type=operation,
        session=self.session,
      )
      assert usage.event_type == expected_event_type

  def test_get_monthly_storage_summary(self):
    """Test getting monthly storage summary."""
    # Create storage snapshots
    for day in [1, 2, 3]:
      for hour in [0, 12]:
        usage = GraphUsage(
          user_id=self.test_user_id,
          graph_id=self.test_graph_id,
          event_type=UsageEventType.STORAGE_SNAPSHOT.value,
          graph_tier="enterprise",
          storage_bytes=(day * 1024**3),  # Increase each day
          storage_gb=float(day),
          billing_year=2024,
          billing_month=1,
          billing_day=day,
          billing_hour=hour,
          recorded_at=datetime(2024, 1, day, hour, 0, 0, tzinfo=timezone.utc),
        )
        self.session.add(usage)

    self.session.commit()

    # Get summary
    summary = GraphUsage.get_monthly_storage_summary(
      user_id=self.test_user_id, year=2024, month=1, session=self.session
    )

    assert self.test_graph_id in summary
    graph_summary = summary[self.test_graph_id]

    assert graph_summary["graph_tier"] == "enterprise"
    assert graph_summary["measurement_count"] == 6  # 3 days * 2 hours
    assert graph_summary["max_storage_gb"] == 3.0
    assert graph_summary["min_storage_gb"] == 1.0
    assert graph_summary["total_gb_hours"] == 12.0  # 1+1+2+2+3+3
    assert graph_summary["avg_storage_gb"] == 2.0

  def test_get_monthly_storage_summary_no_data(self):
    """Test getting monthly storage summary with no data."""
    # Use a different user ID to avoid conflicts with other tests
    different_user = "no_data_user_999"

    summary = GraphUsage.get_monthly_storage_summary(
      user_id=different_user, year=2024, month=1, session=self.session
    )

    assert summary == {}

  def test_get_monthly_credit_summary(self):
    """Test getting monthly credit summary."""
    # Create credit consumption records
    operations = [
      ("agent_call", Decimal("100"), Decimal("80"), 1000),
      ("agent_call", Decimal("150"), Decimal("120"), 1500),
      ("mcp_call", Decimal("50"), Decimal("50"), 500),
      ("query", Decimal("10"), Decimal("10"), 100),
    ]

    for op_type, credits, base_cost, duration in operations:
      usage = GraphUsage(
        user_id=self.test_user_id,
        graph_id=self.test_graph_id,
        event_type=UsageEventType.CREDIT_CONSUMPTION.value,
        operation_type=op_type,
        graph_tier="premium",
        credits_consumed=credits,
        base_credit_cost=base_cost,
        duration_ms=duration,
        cached_operation=(op_type == "query"),
        billing_year=2024,
        billing_month=1,
        billing_day=15,
        billing_hour=10,
      )
      self.session.add(usage)

    self.session.commit()

    # Get summary
    summary = GraphUsage.get_monthly_credit_summary(
      user_id=self.test_user_id, year=2024, month=1, session=self.session
    )

    assert self.test_graph_id in summary
    graph_summary = summary[self.test_graph_id]

    assert graph_summary["graph_tier"] == "premium"
    assert graph_summary["total_credits_consumed"] == 310.0  # 100+150+50+10
    assert graph_summary["total_base_cost"] == 260.0  # 80+120+50+10
    assert graph_summary["transaction_count"] == 4
    assert graph_summary["cached_operations"] == 1
    assert graph_summary["billable_operations"] == 3

    # Check operation breakdown
    assert "agent_call" in graph_summary["operation_breakdown"]
    agent_breakdown = graph_summary["operation_breakdown"]["agent_call"]
    assert agent_breakdown["count"] == 2
    assert agent_breakdown["credits"] == 250.0
    assert agent_breakdown["avg_duration_ms"] == 1250

  def test_get_performance_insights(self):
    """Test getting performance insights."""
    # Clean up any existing usage records to ensure test isolation
    self.session.query(GraphUsage).delete()
    self.session.commit()

    # Create various performance records
    now = datetime.now(timezone.utc)

    operations = [
      ("query", 100, 200, None),
      ("query", 200, 200, None),
      ("query", 6000, 200, Decimal("10")),  # Slow query
      ("agent_call", 1500, 200, Decimal("100")),
      ("import", 10000, 200, Decimal("50")),  # Slow import
    ]

    for op_type, duration, status, credits in operations:
      usage = GraphUsage(
        user_id=self.test_user_id,
        graph_id=self.test_graph_id,
        event_type=UsageEventType.API_CALL.value,
        operation_type=op_type,
        graph_tier="standard",
        duration_ms=duration,
        status_code=status,
        credits_consumed=credits,
        recorded_at=now - timedelta(days=5),
        billing_year=now.year,
        billing_month=now.month,
        billing_day=now.day,
        billing_hour=now.hour,
      )
      self.session.add(usage)

    self.session.commit()

    # Get insights
    insights = GraphUsage.get_performance_insights(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      session=self.session,
      days=30,
    )

    assert insights["graph_id"] == self.test_graph_id
    assert insights["total_operations"] == 5
    assert "operation_stats" in insights

    # Check query stats
    assert "query" in insights["operation_stats"]
    query_stats = insights["operation_stats"]["query"]
    assert query_stats["count"] == 3
    assert query_stats["max_duration_ms"] == 6000
    assert query_stats["min_duration_ms"] == 100

    # Check slow queries
    assert len(insights["slow_queries"]) == 2  # 6000ms query and 10000ms import
    assert insights["slow_queries"][0]["duration_ms"] in [6000, 10000]

  def test_performance_insights_no_data(self):
    """Test performance insights with no data."""
    # Clean up any existing usage records to ensure test isolation
    self.session.query(GraphUsage).delete()
    self.session.commit()

    insights = GraphUsage.get_performance_insights(
      user_id=self.test_user_id, graph_id=self.test_graph_id, session=self.session
    )

    assert insights["message"] == "No performance data available"

  def test_calculate_performance_score(self):
    """Test performance score calculation."""
    # Test various scenarios
    excellent_stats = {"query": {"count": 100, "avg_duration_ms": 50}}
    score = GraphUsage._calculate_performance_score(excellent_stats)
    assert score == 100

    good_stats = {"query": {"count": 100, "avg_duration_ms": 300}}
    score = GraphUsage._calculate_performance_score(good_stats)
    assert score == 90

    poor_stats = {"query": {"count": 100, "avg_duration_ms": 7000}}
    score = GraphUsage._calculate_performance_score(poor_stats)
    assert score == 50

    empty_stats = {}
    score = GraphUsage._calculate_performance_score(empty_stats)
    assert score == 100

  def test_cleanup_old_records(self):
    """Test cleaning up old records."""
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=400)
    recent_date = now - timedelta(days=100)

    # Create old and recent records
    old_record = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      recorded_at=old_date,
      billing_year=old_date.year,
      billing_month=old_date.month,
      billing_day=old_date.day,
      billing_hour=old_date.hour,
    )

    recent_record = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      recorded_at=recent_date,
      billing_year=recent_date.year,
      billing_month=recent_date.month,
      billing_day=recent_date.day,
      billing_hour=recent_date.hour,
    )

    self.session.add(old_record)
    self.session.add(recent_record)
    self.session.commit()

    # Clean up records older than 365 days
    result = GraphUsage.cleanup_old_records(session=self.session, older_than_days=365)

    assert result["deleted_records"] == 1
    assert result["total_processed"] == 1

    # Verify old record is deleted, recent one remains
    remaining = self.session.query(GraphUsage).all()
    assert len(remaining) == 1
    # Compare without timezone since database may not preserve timezone info
    assert remaining[0].recorded_at.replace(tzinfo=None) == recent_date.replace(
      tzinfo=None
    )

  def test_cleanup_old_records_keep_summaries(self):
    """Test cleanup keeping monthly summaries."""
    old_date = datetime.now(timezone.utc) - timedelta(days=400)

    # Create different types of old records
    api_record = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      recorded_at=old_date,
      billing_year=old_date.year,
      billing_month=old_date.month,
      billing_day=old_date.day,
      billing_hour=old_date.hour,
    )

    storage_record = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier="standard",
      recorded_at=old_date,
      billing_year=old_date.year,
      billing_month=old_date.month,
      billing_day=old_date.day,
      billing_hour=old_date.hour,
    )

    self.session.add(api_record)
    self.session.add(storage_record)
    self.session.commit()

    # Clean up with summary preservation
    result = GraphUsage.cleanup_old_records(
      session=self.session, older_than_days=365, keep_monthly_summaries=True
    )

    assert result["deleted_records"] == 1  # Only API_CALL deleted
    assert result["preserved_summaries"] == 1  # Storage snapshot preserved
    assert result["total_processed"] == 2

  def test_cleanup_without_auto_commit(self):
    """Test cleanup without auto-commit."""
    # Clean up any existing usage records to ensure test isolation
    self.session.query(GraphUsage).delete()
    self.session.commit()

    old_date = datetime.now(timezone.utc) - timedelta(days=400)

    old_record = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      recorded_at=old_date,
      billing_year=old_date.year,
      billing_month=old_date.month,
      billing_day=old_date.day,
      billing_hour=old_date.hour,
    )
    self.session.add(old_record)
    self.session.commit()

    result = GraphUsage.cleanup_old_records(
      session=self.session, older_than_days=365, auto_commit=False
    )

    assert result["deleted_records"] == 1

    # Rollback and verify record still exists
    self.session.rollback()
    remaining = self.session.query(GraphUsage).all()
    assert len(remaining) == 1

  def test_get_metadata(self):
    """Test metadata parsing."""
    metadata = {"key": "value", "nested": {"data": "test"}}

    usage = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      event_metadata=json.dumps(metadata),
      billing_year=2024,
      billing_month=1,
      billing_day=1,
      billing_hour=0,
    )

    parsed = usage.get_metadata()
    assert parsed == metadata

  def test_get_metadata_empty(self):
    """Test metadata parsing with empty metadata."""
    usage = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      event_metadata=None,
      billing_year=2024,
      billing_month=1,
      billing_day=1,
      billing_hour=0,
    )

    parsed = usage.get_metadata()
    assert parsed == {}

  def test_get_metadata_invalid_json(self):
    """Test metadata parsing with invalid JSON."""
    usage = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      event_metadata="invalid json",
      billing_year=2024,
      billing_month=1,
      billing_day=1,
      billing_hour=0,
    )

    parsed = usage.get_metadata()
    assert parsed == {}

  def test_to_dict(self):
    """Test conversion to dictionary."""
    # Clean up any existing usage records to ensure test isolation
    self.session.query(GraphUsage).delete()
    self.session.commit()

    now = datetime.now(timezone.utc)
    metadata = {"test": "data"}

    usage = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.CREDIT_CONSUMPTION.value,
      operation_type="agent_call",
      graph_tier="premium",
      storage_gb=5.5,
      credits_consumed=Decimal("100.50"),
      base_credit_cost=Decimal("80.00"),
      duration_ms=1500,
      cached_operation=False,
      status_code=200,
      event_metadata=json.dumps(metadata),
      recorded_at=now,
      billing_year=2024,
      billing_month=1,
      billing_day=15,
      billing_hour=10,
    )

    self.session.add(usage)
    self.session.commit()

    result = usage.to_dict()

    assert result["id"] == usage.id
    assert result["user_id"] == self.test_user_id
    assert result["graph_id"] == self.test_graph_id
    assert result["event_type"] == UsageEventType.CREDIT_CONSUMPTION.value
    assert result["operation_type"] == "agent_call"
    assert result["graph_tier"] == "premium"
    assert result["storage_gb"] == 5.5
    assert result["credits_consumed"] == 100.50
    assert result["base_credit_cost"] == 80.00
    assert result["duration_ms"] == 1500
    assert result["cached_operation"] is False
    assert result["status_code"] == 200
    assert result["metadata"] == metadata
    # Compare timestamp without timezone since database may strip timezone info
    expected_time = now.replace(tzinfo=None).isoformat()
    assert result["recorded_at"] == expected_time

  def test_repr_method(self):
    """Test string representation."""
    usage = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.CREDIT_CONSUMPTION.value,
      credits_consumed=Decimal("50.25"),
      graph_tier="standard",
      billing_year=2024,
      billing_month=1,
      billing_day=1,
      billing_hour=0,
    )

    repr_str = repr(usage)
    assert "<GraphUsage credit_consumption" in repr_str
    assert f"graph={self.test_graph_id}" in repr_str
    assert "credits=50.25" in repr_str

  def test_nullable_fields(self):
    """Test nullable fields can be None."""
    usage = GraphUsage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      event_type=UsageEventType.API_CALL.value,
      graph_tier="standard",
      billing_year=2024,
      billing_month=1,
      billing_day=1,
      billing_hour=0,
      operation_type=None,
      instance_id=None,
      region=None,
      storage_bytes=None,
      credits_consumed=None,
    )

    self.session.add(usage)
    self.session.commit()

    assert usage.operation_type is None
    assert usage.instance_id is None
    assert usage.region is None
    assert usage.storage_bytes is None
    assert usage.credits_consumed is None

  def test_composite_indexes(self):
    """Test that composite indexes are created correctly."""
    indexes = GraphUsage.__table__.indexes
    index_names = {idx.name for idx in indexes}

    expected_indexes = [
      "idx_user_graph_time",
      "idx_billing_period",
      "idx_event_type_time",
      "idx_graph_tier_time",
      "idx_credits_consumed",
      "idx_storage_billing",
      "idx_performance_analysis",
    ]

    for expected in expected_indexes:
      assert expected in index_names

  def test_billing_period_fields(self):
    """Test billing period fields are set correctly."""
    now = datetime(2024, 3, 15, 14, 30, 0, tzinfo=timezone.utc)

    with patch("robosystems.models.iam.graph_usage.datetime") as mock_datetime:
      mock_datetime.now.return_value = now
      mock_datetime.side_effect = datetime

      usage = GraphUsage.record_storage_usage(
        user_id=self.test_user_id,
        graph_id=self.test_graph_id,
        graph_tier="standard",
        storage_bytes=1024**3,
        session=self.session,
      )

    assert usage.billing_year == 2024
    assert usage.billing_month == 3
    assert usage.billing_day == 15
    assert usage.billing_hour == 14

  def test_edge_case_large_storage(self):
    """Test handling very large storage values."""
    large_storage = 10 * 1024**4  # 10 TB in bytes

    usage = GraphUsage.record_storage_usage(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="premium",
      storage_bytes=large_storage,
      session=self.session,
    )

    assert usage.storage_bytes == large_storage
    assert usage.storage_gb == pytest.approx(10240.0, rel=1e-3)

  def test_edge_case_zero_credits(self):
    """Test handling zero credit consumption."""
    usage = GraphUsage.record_credit_consumption(
      user_id=self.test_user_id,
      graph_id=self.test_graph_id,
      graph_tier="standard",
      operation_type="cached_query",
      credits_consumed=Decimal("0"),
      base_credit_cost=Decimal("0"),
      session=self.session,
      cached_operation=True,
    )

    assert usage.credits_consumed == Decimal("0")
    assert usage.cached_operation is True
