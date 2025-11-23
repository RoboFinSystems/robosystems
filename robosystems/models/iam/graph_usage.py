"""
Graph Usage Tracking for Credit System

This model tracks comprehensive usage metrics for:
1. Storage usage (for storage overage billing)
2. Credit consumption analytics
3. Operation performance metrics
4. API usage patterns
5. Cost optimization insights

100% decoupled from subscription-based pricing logic.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Dict, Any
from enum import Enum

from sqlalchemy import (
  Column,
  String,
  DateTime,
  Float,
  Integer,
  Index,
  Numeric,
  Boolean,
  Text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...database import Model
from ...utils.ulid import generate_prefixed_ulid


class UsageEventType(str, Enum):
  """Types of usage events we track."""

  # Storage events
  STORAGE_SNAPSHOT = "storage_snapshot"  # Hourly storage measurements
  STORAGE_GROWTH = "storage_growth"  # Storage increase events
  STORAGE_CLEANUP = "storage_cleanup"  # Storage reduction events

  # Credit events
  CREDIT_CONSUMPTION = "credit_consumption"  # Credit usage events
  CREDIT_ALLOCATION = "credit_allocation"  # Monthly credit grants
  CREDIT_REFUND = "credit_refund"  # Credit refunds

  # API events
  API_CALL = "api_call"  # General API usage
  QUERY_EXECUTION = "query_execution"  # Cypher query execution
  MCP_CALL = "mcp_call"  # AI MCP operations
  AGENT_CALL = "agent_call"  # AI agent operations
  IMPORT_OPERATION = "import_operation"  # Data import operations
  BACKUP_OPERATION = "backup_operation"  # Backup operations
  SYNC_OPERATION = "sync_operation"  # Data sync operations
  ANALYTICS_QUERY = "analytics_query"  # Analytics operations

  # Performance events
  SLOW_QUERY = "slow_query"  # Queries over threshold
  HIGH_MEMORY = "high_memory"  # Memory usage spikes
  ERROR_EVENT = "error_event"  # Error occurrences


class GraphUsage(Model):
  """
  Comprehensive usage tracking for credit system and analytics.

  This model tracks all usage events for billing, analytics, and optimization.
  """

  __tablename__ = "graph_usage"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("usg"))

  # Core identification
  user_id = Column(String, nullable=False, index=True)
  graph_id = Column(String, nullable=False, index=True)

  # Event classification
  event_type = Column(String, nullable=False, index=True)  # UsageEventType
  operation_type = Column(String, nullable=True)  # Specific operation within event type

  # Graph tier and infrastructure
  graph_tier = Column(
    String, nullable=False, index=True
  )  # ladybug-standard, ladybug-large, ladybug-xlarge, etc.
  instance_id = Column(String, nullable=True)  # Infrastructure instance
  region = Column(String, nullable=True)  # AWS region

  # Storage metrics (for storage overage billing)
  storage_bytes = Column(Float, nullable=True)  # Total storage size in bytes
  storage_gb = Column(Float, nullable=True)  # Total storage in GB
  storage_delta_gb = Column(
    Float, nullable=True
  )  # Change in storage since last snapshot

  # Storage breakdown by type (all in GB)
  files_storage_gb = Column(Float, nullable=True)  # S3: User-uploaded files
  tables_storage_gb = Column(Float, nullable=True)  # S3: CSV/Parquet table imports
  graphs_storage_gb = Column(Float, nullable=True)  # EBS: LadybugDB database files
  subgraphs_storage_gb = Column(
    Float, nullable=True
  )  # EBS: Subgraph data (part of database)

  # Credit metrics
  credits_consumed = Column(Numeric(10, 2), nullable=True)  # Credits used
  base_credit_cost = Column(
    Numeric(10, 2), nullable=True
  )  # Base cost before multiplier

  # Performance metrics
  duration_ms = Column(Integer, nullable=True)  # Operation duration
  memory_mb = Column(Float, nullable=True)  # Memory usage
  cpu_percent = Column(Float, nullable=True)  # CPU usage

  # API metrics
  request_size_kb = Column(Float, nullable=True)  # Request payload size
  response_size_kb = Column(Float, nullable=True)  # Response payload size
  status_code = Column(Integer, nullable=True)  # HTTP status code

  # Cost and billing
  cached_operation = Column(Boolean, nullable=True)  # Whether operation was cached
  storage_overage_gb = Column(Float, nullable=True)  # Storage over included amount
  estimated_cost_cents = Column(Integer, nullable=True)  # Estimated cost in cents

  # Error tracking
  error_type = Column(String, nullable=True)  # Error classification
  error_message = Column(Text, nullable=True)  # Error details

  # Metadata
  event_metadata = Column("metadata", Text, nullable=True)  # JSON metadata
  user_agent = Column(String, nullable=True)  # Client user agent
  ip_address = Column(String, nullable=True)  # Client IP

  # Timing
  recorded_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False, index=True
  )

  # Billing period tracking
  billing_year = Column(Integer, nullable=False, index=True)
  billing_month = Column(Integer, nullable=False, index=True)
  billing_day = Column(Integer, nullable=False, index=True)
  billing_hour = Column(Integer, nullable=False)

  # Indexes for efficient querying
  __table_args__ = (
    Index("idx_user_graph_time", "user_id", "graph_id", "recorded_at"),
    Index("idx_billing_period", "billing_year", "billing_month", "user_id"),
    Index("idx_event_type_time", "event_type", "recorded_at"),
    Index("idx_graph_tier_time", "graph_tier", "recorded_at"),
    Index("idx_credits_consumed", "credits_consumed"),
    Index(
      "idx_storage_billing", "user_id", "graph_id", "billing_year", "billing_month"
    ),
    Index("idx_performance_analysis", "operation_type", "duration_ms", "recorded_at"),
  )

  def __repr__(self) -> str:
    return f"<GraphUsage {self.event_type} graph={self.graph_id} credits={self.credits_consumed}>"

  @classmethod
  def record_storage_usage(
    cls,
    user_id: str,
    graph_id: str,
    graph_tier: str,
    storage_bytes: float,
    session: Session,
    storage_delta_gb: Optional[float] = None,
    files_storage_gb: Optional[float] = None,
    tables_storage_gb: Optional[float] = None,
    graphs_storage_gb: Optional[float] = None,
    subgraphs_storage_gb: Optional[float] = None,
    instance_id: Optional[str] = None,
    region: Optional[str] = None,
    auto_commit: bool = True,
  ) -> "GraphUsage":
    """
    Record storage usage snapshot with breakdown by type.

    Args:
        user_id: User ID
        graph_id: Graph ID
        graph_tier: Subscription tier
        storage_bytes: Total storage in bytes
        session: Database session
        storage_delta_gb: Change since last snapshot
        files_storage_gb: S3 user-uploaded files storage
        tables_storage_gb: S3 table imports storage
        graphs_storage_gb: EBS main database storage
        subgraphs_storage_gb: EBS subgraph storage
        instance_id: Infrastructure instance ID
        region: AWS region
        auto_commit: Whether to commit immediately

    Returns:
        Created usage record
    """
    now = datetime.now(timezone.utc)
    storage_gb = storage_bytes / (1024**3)

    usage_record = cls(
      user_id=user_id,
      graph_id=graph_id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=graph_tier,
      storage_bytes=storage_bytes,
      storage_gb=storage_gb,
      storage_delta_gb=storage_delta_gb,
      files_storage_gb=files_storage_gb,
      tables_storage_gb=tables_storage_gb,
      graphs_storage_gb=graphs_storage_gb,
      subgraphs_storage_gb=subgraphs_storage_gb,
      instance_id=instance_id,
      region=region,
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
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
  def record_credit_consumption(
    cls,
    user_id: str,
    graph_id: str,
    graph_tier: str,
    operation_type: str,
    credits_consumed: Decimal,
    base_credit_cost: Decimal,
    session: Session,
    duration_ms: Optional[int] = None,
    cached_operation: bool = False,
    status_code: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
    auto_commit: bool = True,
  ) -> "GraphUsage":
    """Record credit consumption event."""
    import json

    now = datetime.now(timezone.utc)

    usage_record = cls(
      user_id=user_id,
      graph_id=graph_id,
      event_type=UsageEventType.CREDIT_CONSUMPTION.value,
      operation_type=operation_type,
      graph_tier=graph_tier,
      credits_consumed=credits_consumed,
      base_credit_cost=base_credit_cost,
      duration_ms=duration_ms,
      cached_operation=cached_operation,
      status_code=status_code,
      event_metadata=json.dumps(metadata) if metadata else None,
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
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
  def record_api_usage(
    cls,
    user_id: str,
    graph_id: str,
    graph_tier: str,
    operation_type: str,
    session: Session,
    duration_ms: Optional[int] = None,
    status_code: Optional[int] = None,
    request_size_kb: Optional[float] = None,
    response_size_kb: Optional[float] = None,
    cached_operation: bool = False,
    user_agent: Optional[str] = None,
    ip_address: Optional[str] = None,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    auto_commit: bool = True,
  ) -> "GraphUsage":
    """Record API usage event."""
    import json

    now = datetime.now(timezone.utc)

    # Determine event type based on operation
    event_type_map = {
      "query": UsageEventType.QUERY_EXECUTION.value,
      "mcp_call": UsageEventType.MCP_CALL.value,
      "agent_call": UsageEventType.AGENT_CALL.value,
      "import": UsageEventType.IMPORT_OPERATION.value,
      "backup": UsageEventType.BACKUP_OPERATION.value,
      "sync": UsageEventType.SYNC_OPERATION.value,
      "analytics": UsageEventType.ANALYTICS_QUERY.value,
    }

    event_type = event_type_map.get(operation_type, UsageEventType.API_CALL.value)

    usage_record = cls(
      user_id=user_id,
      graph_id=graph_id,
      event_type=event_type,
      operation_type=operation_type,
      graph_tier=graph_tier,
      duration_ms=duration_ms,
      status_code=status_code,
      request_size_kb=request_size_kb,
      response_size_kb=response_size_kb,
      cached_operation=cached_operation,
      user_agent=user_agent,
      ip_address=ip_address,
      error_type=error_type,
      error_message=error_message,
      event_metadata=json.dumps(metadata) if metadata else None,
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
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
  def get_monthly_storage_summary(
    cls,
    user_id: str,
    year: int,
    month: int,
    session: Session,
  ) -> Dict[str, Dict]:
    """Get monthly storage summary for billing."""
    # Get storage snapshots for the month
    records = (
      session.query(cls)
      .filter(
        cls.user_id == user_id,
        cls.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
        cls.billing_year == year,
        cls.billing_month == month,
      )
      .order_by(cls.graph_id, cls.recorded_at)
      .all()
    )

    graph_storage = {}

    for record in records:
      if record.graph_id not in graph_storage:
        graph_storage[record.graph_id] = {
          "graph_id": record.graph_id,
          "graph_tier": record.graph_tier,
          "measurements": [],
          "total_gb_hours": 0.0,
          "avg_storage_gb": 0.0,
          "max_storage_gb": 0.0,
          "min_storage_gb": float("inf"),
          "measurement_count": 0,
        }

      graph_storage[record.graph_id]["measurements"].append(
        {
          "timestamp": record.recorded_at,
          "storage_gb": record.storage_gb,
        }
      )

      # Update storage statistics
      if record.storage_gb > graph_storage[record.graph_id]["max_storage_gb"]:
        graph_storage[record.graph_id]["max_storage_gb"] = record.storage_gb
      if record.storage_gb < graph_storage[record.graph_id]["min_storage_gb"]:
        graph_storage[record.graph_id]["min_storage_gb"] = record.storage_gb

    # Calculate GB-hours for each graph
    for graph_id, data in graph_storage.items():
      measurements = data["measurements"]
      data["measurement_count"] = len(measurements)

      # Calculate total GB-hours (each measurement represents 1 hour)
      data["total_gb_hours"] = sum(m["storage_gb"] for m in measurements)

      # Calculate average storage
      if measurements:
        data["avg_storage_gb"] = data["total_gb_hours"] / len(measurements)

      # Clean up min storage if no measurements
      if data["min_storage_gb"] == float("inf"):
        data["min_storage_gb"] = 0.0

      # Remove raw measurements from result
      del data["measurements"]

    return graph_storage

  @classmethod
  def get_monthly_credit_summary(
    cls,
    user_id: str,
    year: int,
    month: int,
    session: Session,
  ) -> Dict[str, Dict]:
    """Get monthly credit consumption summary."""
    # Get credit consumption records for the month
    records = (
      session.query(cls)
      .filter(
        cls.user_id == user_id,
        cls.event_type == UsageEventType.CREDIT_CONSUMPTION.value,
        cls.billing_year == year,
        cls.billing_month == month,
      )
      .order_by(cls.graph_id, cls.recorded_at)
      .all()
    )

    graph_credits = {}

    for record in records:
      if record.graph_id not in graph_credits:
        graph_credits[record.graph_id] = {
          "graph_id": record.graph_id,
          "graph_tier": record.graph_tier,
          "total_credits_consumed": Decimal("0"),
          "total_base_cost": Decimal("0"),
          "operation_breakdown": {},
          "cached_operations": 0,
          "billable_operations": 0,
          "transaction_count": 0,
        }

      graph_data = graph_credits[record.graph_id]

      # Add to totals
      if record.credits_consumed is not None:
        graph_data["total_credits_consumed"] += record.credits_consumed
      if record.base_credit_cost is not None:
        graph_data["total_base_cost"] += record.base_credit_cost

      # Track operation breakdown
      op_type = record.operation_type or "unknown"
      if op_type not in graph_data["operation_breakdown"]:
        graph_data["operation_breakdown"][op_type] = {
          "count": 0,
          "credits": Decimal("0"),
          "avg_duration_ms": 0,
          "total_duration_ms": 0,
        }

      op_data = graph_data["operation_breakdown"][op_type]
      op_data["count"] += 1
      if record.credits_consumed is not None:
        op_data["credits"] += record.credits_consumed
      if record.duration_ms is not None:
        op_data["total_duration_ms"] += record.duration_ms
        op_data["avg_duration_ms"] = op_data["total_duration_ms"] / op_data["count"]

      # Count cached vs billable operations
      if record.cached_operation is True:
        graph_data["cached_operations"] += 1
      else:
        graph_data["billable_operations"] += 1

      graph_data["transaction_count"] += 1

    # Calculate averages
    for graph_id, data in graph_credits.items():
      # Convert Decimal to float for JSON serialization
      data["total_credits_consumed"] = float(data["total_credits_consumed"])
      data["total_base_cost"] = float(data["total_base_cost"])

      for op_type, op_data in data["operation_breakdown"].items():
        op_data["credits"] = float(op_data["credits"])

    return graph_credits

  @classmethod
  def get_performance_insights(
    cls,
    user_id: str,
    graph_id: str,
    session: Session,
    days: int = 30,
  ) -> Dict[str, Any]:
    """Get performance insights for cost optimization."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

    # Get performance data
    records = (
      session.query(cls)
      .filter(
        cls.user_id == user_id,
        cls.graph_id == graph_id,
        cls.recorded_at >= cutoff_date,
        cls.duration_ms.isnot(None),
      )
      .order_by(cls.recorded_at)
      .all()
    )

    if not records:
      return {"message": "No performance data available"}

    # Analyze performance by operation type
    operation_stats = {}
    slow_queries = []

    for record in records:
      op_type = record.operation_type or "unknown"

      if op_type not in operation_stats:
        operation_stats[op_type] = {
          "count": 0,
          "total_duration_ms": 0,
          "avg_duration_ms": 0,
          "max_duration_ms": 0,
          "min_duration_ms": float("inf"),
          "total_credits": Decimal("0"),
          "avg_credits": Decimal("0"),
        }

      stats = operation_stats[op_type]
      stats["count"] += 1
      stats["total_duration_ms"] += record.duration_ms

      if record.duration_ms > stats["max_duration_ms"]:
        stats["max_duration_ms"] = record.duration_ms
      if record.duration_ms < stats["min_duration_ms"]:
        stats["min_duration_ms"] = record.duration_ms

      if record.credits_consumed is not None:
        stats["total_credits"] += record.credits_consumed

      # Track slow queries (over 5 seconds)
      if record.duration_ms is not None and record.duration_ms > 5000:
        slow_queries.append(
          {
            "timestamp": record.recorded_at.isoformat(),
            "operation_type": op_type,
            "duration_ms": record.duration_ms,
            "credits_consumed": float(record.credits_consumed)
            if record.credits_consumed is not None
            else 0.0,
          }
        )

    # Calculate averages
    for op_type, stats in operation_stats.items():
      if stats["count"] > 0:
        stats["avg_duration_ms"] = stats["total_duration_ms"] / stats["count"]
        stats["avg_credits"] = stats["total_credits"] / stats["count"]

      if stats["min_duration_ms"] == float("inf"):
        stats["min_duration_ms"] = 0

      # Convert Decimal to float
      stats["total_credits"] = float(stats["total_credits"])
      stats["avg_credits"] = float(stats["avg_credits"])

    return {
      "graph_id": graph_id,
      "analysis_period_days": days,
      "total_operations": len(records),
      "operation_stats": operation_stats,
      "slow_queries": slow_queries[:10],  # Top 10 slow queries
      "performance_score": cls._calculate_performance_score(operation_stats),
    }

  @classmethod
  def _calculate_performance_score(cls, operation_stats: Dict) -> int:
    """Calculate performance score (0-100) based on operation stats."""
    if not operation_stats:
      return 100

    # Calculate weighted average duration
    total_ops = sum(stats["count"] for stats in operation_stats.values())
    weighted_avg_duration = (
      sum(
        stats["avg_duration_ms"] * stats["count"] for stats in operation_stats.values()
      )
      / total_ops
    )

    # Score based on average duration (lower is better)
    # 0-100ms = 100, 100-500ms = 90, 500-1000ms = 80, etc.
    if weighted_avg_duration < 100:
      return 100
    elif weighted_avg_duration < 500:
      return 90
    elif weighted_avg_duration < 1000:
      return 80
    elif weighted_avg_duration < 2000:
      return 70
    elif weighted_avg_duration < 5000:
      return 60
    else:
      return 50

  @classmethod
  def cleanup_old_records(
    cls,
    session: Session,
    older_than_days: int = 365,
    keep_monthly_summaries: bool = True,
    auto_commit: bool = True,
  ) -> Dict[str, int]:
    """Clean up old usage records with optional summary preservation."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=older_than_days)

    # Count records to be deleted
    total_count = session.query(cls).filter(cls.recorded_at < cutoff_date).count()

    if keep_monthly_summaries:
      # Delete detailed records but keep monthly summaries
      deleted_count = (
        session.query(cls)
        .filter(
          cls.recorded_at < cutoff_date,
          cls.event_type.in_(
            [
              UsageEventType.API_CALL.value,
              UsageEventType.CREDIT_CONSUMPTION.value,
            ]
          ),
        )
        .delete()
      )

      summary_count = total_count - deleted_count
    else:
      # Delete all old records
      deleted_count = session.query(cls).filter(cls.recorded_at < cutoff_date).delete()
      summary_count = 0

    if auto_commit:
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise

    return {
      "deleted_records": deleted_count,
      "preserved_summaries": summary_count,
      "total_processed": total_count,
    }

  def get_metadata(self) -> Dict[str, Any]:
    """Parse metadata JSON."""
    if self.event_metadata is None:
      return {}

    try:
      import json

      return json.loads(self.event_metadata or "{}")
    except Exception:
      return {}

  def to_dict(self) -> Dict[str, Any]:
    """Convert to dictionary for API responses."""
    return {
      "id": self.id,
      "user_id": self.user_id,
      "graph_id": self.graph_id,
      "event_type": self.event_type,
      "operation_type": self.operation_type,
      "graph_tier": self.graph_tier,
      "storage_gb": self.storage_gb,
      "credits_consumed": float(self.credits_consumed)
      if self.credits_consumed is not None
      else None,
      "base_credit_cost": float(self.base_credit_cost)
      if self.base_credit_cost is not None
      else None,
      "duration_ms": self.duration_ms,
      "cached_operation": self.cached_operation,
      "status_code": self.status_code,
      "recorded_at": self.recorded_at.isoformat(),
      "metadata": self.get_metadata(),
    }
