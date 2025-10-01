"""Test UserUsageTracking model functionality."""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import UserUsageTracking, User
from robosystems.models.iam.user_usage_tracking import UsageType


class TestUserUsageTracking:
  """Test cases for UserUsageTracking model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session

    # Create unique IDs for this test class
    import uuid

    self.unique_id = str(uuid.uuid4())[:8]

    # Create a test user
    self.user = User(
      email=f"usage_tracking_{self.unique_id}@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    self.session.add(self.user)
    self.session.commit()

  def test_create_usage_tracking_entry(self):
    """Test creating a basic usage tracking entry."""
    usage = UserUsageTracking(
      user_id=self.user.id,
      usage_type=UsageType.API_CALL.value,
      endpoint="/v1/test",
      resource_count=1,
    )

    assert usage.user_id == self.user.id
    assert usage.usage_type == "api_call"
    assert usage.endpoint == "/v1/test"
    assert usage.resource_count == 1
    assert usage.occurred_at is None  # Not set until session add

    self.session.add(usage)
    self.session.commit()

    assert usage.id is not None
    assert usage.occurred_at is not None

  def test_usage_type_enum_values(self):
    """Test all UsageType enum values."""
    assert UsageType.API_CALL.value == "api_call"
    assert UsageType.SEC_IMPORT.value == "sec_import"
    assert UsageType.GRAPH_CREATION.value == "graph_creation"
    assert UsageType.DATA_EXPORT.value == "data_export"

  def test_record_usage_method(self):
    """Test the record_usage class method."""
    usage = UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.SEC_IMPORT,
      session=self.session,
      graph_id=f"test_graph_usage_{self.unique_id}",
      resource_count=5,
    )

    assert usage.id is not None
    assert usage.user_id == self.user.id
    assert usage.usage_type == "sec_import"
    assert usage.graph_id == f"test_graph_usage_{self.unique_id}"
    assert usage.resource_count == 5

  def test_record_usage_without_auto_commit(self):
    """Test record_usage without auto-commit."""
    usage = UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.GRAPH_CREATION,
      session=self.session,
      auto_commit=False,
    )

    # Should be added to session but not committed
    assert usage in self.session.new
    self.session.rollback()

    # After rollback, should not be in database
    result = self.session.query(UserUsageTracking).filter_by(id=usage.id).first()
    assert result is None

  def test_record_usage_with_error(self):
    """Test record_usage handling database errors."""
    with patch.object(self.session, "commit", side_effect=SQLAlchemyError("DB error")):
      with pytest.raises(SQLAlchemyError):
        UserUsageTracking.record_usage(
          user_id=self.user.id, usage_type=UsageType.API_CALL, session=self.session
        )

  def test_get_usage_count_basic(self):
    """Test getting usage count for a user."""
    # Create some usage records
    for i in range(3):
      UserUsageTracking.record_usage(
        user_id=self.user.id,
        usage_type=UsageType.API_CALL,
        session=self.session,
        resource_count=2,
      )

    count = UserUsageTracking.get_usage_count(
      user_id=self.user.id, usage_type=UsageType.API_CALL, session=self.session
    )

    assert count == 6  # 3 records * 2 resources each

  def test_get_usage_count_with_time_range(self):
    """Test getting usage count within a time range."""
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    tomorrow = now + timedelta(days=1)

    # Create a usage record
    UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.DATA_EXPORT,
      session=self.session,
      resource_count=10,
    )

    # Count with different time ranges
    count_all = UserUsageTracking.get_usage_count(
      user_id=self.user.id, usage_type=UsageType.DATA_EXPORT, session=self.session
    )
    assert count_all == 10

    count_recent = UserUsageTracking.get_usage_count(
      user_id=self.user.id,
      usage_type=UsageType.DATA_EXPORT,
      session=self.session,
      since=yesterday,
      until=tomorrow,
    )
    assert count_recent == 10

    count_future = UserUsageTracking.get_usage_count(
      user_id=self.user.id,
      usage_type=UsageType.DATA_EXPORT,
      session=self.session,
      since=tomorrow,
    )
    assert count_future == 0

  def test_get_usage_count_no_records(self):
    """Test getting usage count when no records exist."""
    count = UserUsageTracking.get_usage_count(
      user_id=self.user.id, usage_type=UsageType.GRAPH_CREATION, session=self.session
    )
    assert count == 0

  def test_get_hourly_api_calls(self):
    """Test getting API calls for the current hour."""
    now = datetime.now(timezone.utc)

    # Create API calls at different times
    with patch("robosystems.models.iam.user_usage_tracking.datetime") as mock_datetime:
      # Current hour call
      mock_datetime.now.return_value = now
      mock_datetime.side_effect = datetime
      UserUsageTracking.record_usage(
        user_id=self.user.id,
        usage_type=UsageType.API_CALL,
        session=self.session,
        resource_count=3,
      )

      # Previous hour call (should not be counted)
      old_time = now - timedelta(hours=2)
      old_usage = UserUsageTracking(
        user_id=self.user.id,
        usage_type=UsageType.API_CALL.value,
        occurred_at=old_time,
        resource_count=5,
      )
      self.session.add(old_usage)
      self.session.commit()

    # Get hourly count
    count = UserUsageTracking.get_hourly_api_calls(self.user.id, self.session)
    assert count == 3  # Only current hour

  def test_get_daily_sec_imports(self):
    """Test getting SEC imports for the current day."""
    now = datetime.now(timezone.utc)

    # Create SEC imports at different times
    with patch("robosystems.models.iam.user_usage_tracking.datetime") as mock_datetime:
      # Today's import
      mock_datetime.now.return_value = now
      mock_datetime.side_effect = datetime
      UserUsageTracking.record_usage(
        user_id=self.user.id,
        usage_type=UsageType.SEC_IMPORT,
        session=self.session,
        resource_count=10,
      )

      # Yesterday's import (should not be counted)
      old_time = now - timedelta(days=2)
      old_usage = UserUsageTracking(
        user_id=self.user.id,
        usage_type=UsageType.SEC_IMPORT.value,
        occurred_at=old_time,
        resource_count=20,
      )
      self.session.add(old_usage)
      self.session.commit()

    # Get daily count
    count = UserUsageTracking.get_daily_sec_imports(self.user.id, self.session)
    assert count == 10  # Only today

  def test_cleanup_old_records(self):
    """Test cleaning up old usage records."""
    # Clean up any existing usage tracking records for test isolation
    self.session.query(UserUsageTracking).delete()
    self.session.commit()

    now = datetime.now(timezone.utc)

    # Create old and new records
    old_time = now - timedelta(days=100)
    new_time = now - timedelta(days=10)

    old_usage = UserUsageTracking(
      user_id=self.user.id, usage_type=UsageType.API_CALL.value, occurred_at=old_time
    )
    new_usage = UserUsageTracking(
      user_id=self.user.id, usage_type=UsageType.API_CALL.value, occurred_at=new_time
    )

    self.session.add(old_usage)
    self.session.add(new_usage)
    self.session.commit()

    # Clean up records older than 90 days
    deleted_count = UserUsageTracking.cleanup_old_records(
      session=self.session, older_than_days=90
    )

    assert deleted_count == 1

    # Verify old record is deleted, new one remains
    remaining = self.session.query(UserUsageTracking).all()
    assert len(remaining) == 1
    # Compare timestamps allowing for timezone differences
    remaining_time = remaining[0].occurred_at
    if remaining_time.tzinfo is None:
      remaining_time = remaining_time.replace(tzinfo=timezone.utc)
    assert remaining_time == new_time

  def test_cleanup_old_records_without_auto_commit(self):
    """Test cleanup without auto-commit."""
    # Clean up any existing usage tracking records for test isolation
    self.session.query(UserUsageTracking).delete()
    self.session.commit()

    old_time = datetime.now(timezone.utc) - timedelta(days=100)

    old_usage = UserUsageTracking(
      user_id=self.user.id, usage_type=UsageType.API_CALL.value, occurred_at=old_time
    )
    self.session.add(old_usage)
    self.session.commit()

    # Store the ID before deletion to avoid accessing the stale object
    old_usage_id = old_usage.id

    deleted_count = UserUsageTracking.cleanup_old_records(
      session=self.session, older_than_days=90, auto_commit=False
    )

    assert deleted_count == 1
    # The object would be deleted from database but not committed yet
    # Check that the record is no longer found in the database query
    result_before_rollback = (
      self.session.query(UserUsageTracking).filter_by(id=old_usage_id).first()
    )
    assert result_before_rollback is None

    # Rollback and verify record is restored
    self.session.rollback()
    result = self.session.query(UserUsageTracking).filter_by(id=old_usage_id).first()
    assert result is not None

  def test_cleanup_with_error(self):
    """Test cleanup handling database errors."""
    with patch.object(self.session, "commit", side_effect=SQLAlchemyError("DB error")):
      with pytest.raises(SQLAlchemyError):
        UserUsageTracking.cleanup_old_records(session=self.session, older_than_days=90)

  def test_get_user_usage_stats(self):
    """Test getting comprehensive usage statistics."""
    # Create various usage records
    UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.API_CALL,
      session=self.session,
      resource_count=5,
    )
    UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.SEC_IMPORT,
      session=self.session,
      resource_count=3,
    )
    UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.GRAPH_CREATION,
      session=self.session,
      resource_count=1,
    )

    # Get stats
    stats = UserUsageTracking.get_user_usage_stats(
      user_id=self.user.id, session=self.session, days_back=30
    )

    assert "api_call" in stats
    assert stats["api_call"]["total_count"] == 5
    assert stats["api_call"]["period_days"] == 30

    assert "sec_import" in stats
    assert stats["sec_import"]["total_count"] == 3

    assert "graph_creation" in stats
    assert stats["graph_creation"]["total_count"] == 1

    assert "data_export" in stats
    assert stats["data_export"]["total_count"] == 0

    assert "current_hour_api_calls" in stats
    assert stats["current_hour_api_calls"] == 5

    assert "current_day_sec_imports" in stats
    assert stats["current_day_sec_imports"] == 3

  def test_get_user_usage_stats_with_old_data(self):
    """Test usage stats excluding old data."""
    now = datetime.now(timezone.utc)

    # Create old record (outside 30-day window)
    old_time = now - timedelta(days=40)
    old_usage = UserUsageTracking(
      user_id=self.user.id,
      usage_type=UsageType.API_CALL.value,
      occurred_at=old_time,
      resource_count=100,
    )
    self.session.add(old_usage)

    # Create recent record
    UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.API_CALL,
      session=self.session,
      resource_count=10,
    )

    # Get stats for last 30 days
    stats = UserUsageTracking.get_user_usage_stats(
      user_id=self.user.id, session=self.session, days_back=30
    )

    # Should only include recent record
    assert stats["api_call"]["total_count"] == 10

  def test_repr_method(self):
    """Test string representation of usage tracking."""
    usage = UserUsageTracking(user_id=self.user.id, usage_type=UsageType.API_CALL.value)
    self.session.add(usage)
    self.session.commit()

    repr_str = repr(usage)
    assert f"<UserUsageTracking {usage.id}" in repr_str
    assert f"user={self.user.id}" in repr_str
    assert "type=api_call" in repr_str

  def test_composite_indexes(self):
    """Test that composite indexes are created correctly."""
    # This test verifies the indexes exist in the table definition
    indexes = UserUsageTracking.__table__.indexes
    index_names = {idx.name for idx in indexes}

    assert "idx_user_usage_type_time" in index_names
    assert "idx_usage_type_time" in index_names

  def test_nullable_fields(self):
    """Test nullable fields can be None."""
    usage = UserUsageTracking(
      user_id=self.user.id,
      usage_type=UsageType.API_CALL.value,
      endpoint=None,
      graph_id=None,
    )
    self.session.add(usage)
    self.session.commit()

    assert usage.endpoint is None
    assert usage.graph_id is None
    assert usage.resource_count == 1  # Default value

  def test_multiple_users_tracking(self):
    """Test tracking usage for multiple users."""
    # Create another user
    user2 = User(
      email="test2@example.com", name="Test User 2", password_hash="hashed_password"
    )
    self.session.add(user2)
    self.session.commit()

    # Record usage for both users
    UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.API_CALL,
      session=self.session,
      resource_count=5,
    )
    UserUsageTracking.record_usage(
      user_id=user2.id,
      usage_type=UsageType.API_CALL,
      session=self.session,
      resource_count=10,
    )

    # Get counts for each user
    count1 = UserUsageTracking.get_usage_count(
      user_id=self.user.id, usage_type=UsageType.API_CALL, session=self.session
    )
    count2 = UserUsageTracking.get_usage_count(
      user_id=user2.id, usage_type=UsageType.API_CALL, session=self.session
    )

    assert count1 == 5
    assert count2 == 10

  def test_endpoint_tracking(self):
    """Test tracking specific endpoints."""
    endpoints = ["/v1/user/profile", "/v1/graphs/graph/query", "/v1/user/profile"]

    for endpoint in endpoints:
      UserUsageTracking.record_usage(
        user_id=self.user.id,
        usage_type=UsageType.API_CALL,
        session=self.session,
        endpoint=endpoint,
      )

    # Query by endpoint
    profile_calls = (
      self.session.query(UserUsageTracking)
      .filter_by(user_id=self.user.id, endpoint="/v1/user/profile")
      .all()
    )

    assert len(profile_calls) == 2

  def test_graph_id_tracking(self):
    """Test tracking usage by graph ID."""
    graph_ids = ["graph1", "graph2", "graph1"]

    for graph_id in graph_ids:
      UserUsageTracking.record_usage(
        user_id=self.user.id,
        usage_type=UsageType.DATA_EXPORT,
        session=self.session,
        graph_id=graph_id,
      )

    # Query by graph_id
    graph1_exports = (
      self.session.query(UserUsageTracking)
      .filter_by(user_id=self.user.id, graph_id="graph1")
      .all()
    )

    assert len(graph1_exports) == 2

  def test_occurred_at_timezone(self):
    """Test that occurred_at is stored properly."""
    usage = UserUsageTracking.record_usage(
      user_id=self.user.id, usage_type=UsageType.API_CALL, session=self.session
    )

    # Database stores datetime as timezone-naive (converted from UTC)
    # The datetime should be a recent timestamp
    assert usage.occurred_at is not None
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    time_diff = abs((usage.occurred_at - now).total_seconds())
    assert time_diff < 5  # Should be within 5 seconds of now

  def test_edge_case_zero_resource_count(self):
    """Test handling zero resource count (edge case)."""
    usage = UserUsageTracking(
      user_id=self.user.id, usage_type=UsageType.API_CALL.value, resource_count=0
    )
    self.session.add(usage)
    self.session.commit()

    assert usage.resource_count == 0

    count = UserUsageTracking.get_usage_count(
      user_id=self.user.id, usage_type=UsageType.API_CALL, session=self.session
    )
    assert count == 0

  def test_edge_case_large_resource_count(self):
    """Test handling large resource counts."""
    large_count = 1000000

    usage = UserUsageTracking.record_usage(
      user_id=self.user.id,
      usage_type=UsageType.DATA_EXPORT,
      session=self.session,
      resource_count=large_count,
    )

    assert usage.resource_count == large_count

    count = UserUsageTracking.get_usage_count(
      user_id=self.user.id, usage_type=UsageType.DATA_EXPORT, session=self.session
    )
    assert count == large_count
