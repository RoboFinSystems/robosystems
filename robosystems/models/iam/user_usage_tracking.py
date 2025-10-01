"""User usage tracking for rate limiting and analytics."""

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import Column, String, DateTime, ForeignKey, Integer, Index
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from ...database import Model
from ...utils import default_usage_ulid


class UsageType(str, Enum):
  """Types of usage that can be tracked."""

  API_CALL = "api_call"
  SEC_IMPORT = "sec_import"
  GRAPH_CREATION = "graph_creation"
  DATA_EXPORT = "data_export"


class UserUsageTracking(Model):
  """Track user usage for rate limiting and analytics."""

  __tablename__ = "user_usage_tracking"

  id = Column(String, primary_key=True, default=default_usage_ulid)
  user_id = Column(String, ForeignKey("users.id"), nullable=False, index=True)
  usage_type = Column(String, nullable=False, index=True)  # UsageType enum value

  # Timestamp for the usage event
  occurred_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False, index=True
  )

  # Optional metadata about the usage
  endpoint = Column(String, nullable=True)  # API endpoint for API calls
  graph_id = Column(String, nullable=True)  # Graph ID if applicable
  resource_count = Column(
    Integer, default=1, nullable=False
  )  # Number of resources processed

  # Add composite indexes for efficient querying
  __table_args__ = (
    Index("idx_user_usage_type_time", "user_id", "usage_type", "occurred_at"),
    Index("idx_usage_type_time", "usage_type", "occurred_at"),
  )

  def __repr__(self) -> str:
    """String representation of the usage tracking entry."""
    return (
      f"<UserUsageTracking {self.id} user={self.user_id} type={self.usage_type} "
      f"at={self.occurred_at}>"
    )

  @classmethod
  def record_usage(
    cls,
    user_id: str,
    usage_type: UsageType,
    session: Session,
    endpoint: Optional[str] = None,
    graph_id: Optional[str] = None,
    resource_count: int = 1,
    auto_commit: bool = True,
  ) -> "UserUsageTracking":
    """Record a usage event for a user."""
    usage_record = cls(
      user_id=user_id,
      usage_type=usage_type.value,
      endpoint=endpoint,
      graph_id=graph_id,
      resource_count=resource_count,
    )

    session.add(usage_record)

    if auto_commit:
      try:
        session.commit()
        session.refresh(usage_record)
      except SQLAlchemyError:
        session.rollback()
        raise

    return usage_record

  @classmethod
  def get_usage_count(
    cls,
    user_id: str,
    usage_type: UsageType,
    session: Session,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
  ) -> int:
    """Get count of usage events for a user within a time period."""
    query = session.query(func.sum(cls.resource_count)).filter(
      cls.user_id == user_id, cls.usage_type == usage_type.value
    )

    if since:
      query = query.filter(cls.occurred_at >= since)
    if until:
      query = query.filter(cls.occurred_at <= until)

    result = query.scalar()
    return result if result is not None else 0

  @classmethod
  def get_hourly_api_calls(cls, user_id: str, session: Session) -> int:
    """Get API call count for the current hour."""
    now = datetime.now(timezone.utc)
    hour_start = now.replace(minute=0, second=0, microsecond=0)

    return cls.get_usage_count(
      user_id=user_id,
      usage_type=UsageType.API_CALL,
      session=session,
      since=hour_start,
      until=now,
    )

  @classmethod
  def get_daily_sec_imports(cls, user_id: str, session: Session) -> int:
    """Get SEC import count for the current day."""
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    return cls.get_usage_count(
      user_id=user_id,
      usage_type=UsageType.SEC_IMPORT,
      session=session,
      since=day_start,
      until=now,
    )

  @classmethod
  def cleanup_old_records(
    cls, session: Session, older_than_days: int = 90, auto_commit: bool = True
  ) -> int:
    """Clean up old usage tracking records to prevent unlimited growth."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    # Make cutoff_date timezone-naive to match database storage
    cutoff_date = cutoff_date.replace(tzinfo=None)

    deleted_count = session.query(cls).filter(cls.occurred_at < cutoff_date).delete()

    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise

    return deleted_count

  @classmethod
  def get_user_usage_stats(
    cls, user_id: str, session: Session, days_back: int = 30
  ) -> dict:
    """Get comprehensive usage statistics for a user."""
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=days_back)

    stats = {}

    for usage_type in UsageType:
      count = cls.get_usage_count(
        user_id=user_id,
        usage_type=usage_type,
        session=session,
        since=start_date,
        until=now,
      )
      stats[usage_type.value] = {"total_count": count, "period_days": days_back}

    # Add current period specific stats
    stats["current_hour_api_calls"] = cls.get_hourly_api_calls(user_id, session)
    stats["current_day_sec_imports"] = cls.get_daily_sec_imports(user_id, session)

    return stats
