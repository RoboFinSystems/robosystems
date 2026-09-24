"""Fiscal calendar models — the rolling state of a graph's close cadence.

`FiscalCalendar` holds `closed_through_period` (system-maintained) and
`close_target_period` (user-set). Close processes the periods between them in
order; when they meet, the target advances one month. `FiscalCalendarEvent`
is the append-only audit log of every mutation. Isolation comes from the
tenant schema; `graph_id` is only a defensive discriminator.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
  Text,
  UniqueConstraint,
)

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class FiscalCalendar(ExtensionsBase):
  """Per-graph rolling close state pointer (tenant-scoped)."""

  __tablename__ = "fiscal_calendar"
  __table_args__ = (
    UniqueConstraint("graph_id", name="uq_fiscal_calendar_graph"),
    Index("idx_fiscal_calendar_graph", "graph_id"),
    CheckConstraint(
      "fiscal_year_start_month BETWEEN 1 AND 12",
      name="ck_fiscal_calendar_year_start_month",
    ),
    CheckConstraint(
      "close_target_period IS NULL "
      "OR closed_through_period IS NULL "
      "OR close_target_period >= closed_through_period",
      name="ck_fiscal_calendar_target_ge_through",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fcal"))
  graph_id = Column(String, nullable=False)

  # Period naming stays YYYY-MM regardless.
  fiscal_year_start_month = Column(Integer, nullable=False, default=1)

  # YYYY-MM
  closed_through_period = Column(String, nullable=True)
  close_target_period = Column(String, nullable=True)

  initialized_at = Column(DateTime, nullable=True)
  last_close_at = Column(DateTime, nullable=True)
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=True)
  updated_by = Column(String, nullable=True)

  def __repr__(self) -> str:
    return (
      f"<FiscalCalendar {self.graph_id} "
      f"closed_through={self.closed_through_period} "
      f"target={self.close_target_period}>"
    )


class FiscalCalendarEvent(ExtensionsBase):
  """Append-only audit log for fiscal calendar mutations (tenant-scoped)."""

  __tablename__ = "fiscal_calendar_events"
  __table_args__ = (
    Index("idx_fiscal_calendar_events_graph_time", "graph_id", "created_at"),
    CheckConstraint(
      "event_type IN ("
      "'initialized', 'target_changed', 'period_closed', 'period_reopened', "
      "'target_advanced_auto'"
      ")",
      name="ck_fiscal_calendar_events_event_type",
    ),
    CheckConstraint(
      "actor_type IN ('user', 'agent', 'system')",
      name="ck_fiscal_calendar_events_actor_type",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fce"))
  fiscal_calendar_id = Column(
    String,
    ForeignKey(
      "fiscal_calendar.id",
      name="fk_fiscal_calendar_events_calendar",
      ondelete="CASCADE",
    ),
    nullable=False,
  )
  graph_id = Column(String, nullable=False)

  # What happened
  event_type = Column(String, nullable=False)
  period = Column(String, nullable=True)  # YYYY-MM, when applicable
  from_value = Column(String, nullable=True)
  to_value = Column(String, nullable=True)

  # Who / why
  actor_id = Column(String, nullable=True)
  actor_type = Column(String, nullable=False, default="user")
  note = Column(Text, nullable=True)
  reason = Column(Text, nullable=True)  # required for reopens

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))

  def __repr__(self) -> str:
    return (
      f"<FiscalCalendarEvent {self.event_type} "
      f"graph={self.graph_id} period={self.period}>"
    )
