"""Event model: the real-world business event layer above the GL.

Status lifecycle: captured → classified → committed → pending → fulfilled,
with voided and superseded as terminal off-ramps. GL transactions and entries
link back via ``triggered_by_event_id``.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  Index,
  String,
  text,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid

# Canonical action vocabulary refining `event_category`, modelled on
# Valueflows. Custody-only vs rights-transfer is the load-bearing distinction
# (consignment, drop-shipping, escrow). Keep in sync with the `EventAction`
# Literal in models/api/event_block.py.
EVENT_ACTIONS: frozenset[str] = frozenset(
  {
    # Resource creation
    "produce",
    "raise",
    # Resource destruction
    "consume",
    "lower",
    # Resource use without consumption
    "use",
    "cite",
    # Work
    "work",
    "deliverService",
    # Custody only (physical possession, no rights transfer)
    "pickup",
    "dropoff",
    "accept",
    "transferCustody",
    # Rights only (ownership transfer, no physical movement)
    "transferAllRights",
    # Both (rights + custody together)
    "transfer",
    "move",
    # Transformation
    "modify",
    "combine",
    "separate",
    # Copy (new resource without removing source)
    "copy",
  }
)

# sorted() matches the migration's IN clause; autogenerate compares the text,
# so any other order produces spurious diffs.
_EVENT_ACTION_CHECK = (
  "event_action IS NULL OR event_action IN ("
  + ", ".join(f"'{v}'" for v in sorted(EVENT_ACTIONS))
  + ")"
)


class Event(ExtensionsBase):
  __tablename__ = "events"
  __table_args__ = (
    Index("idx_events_type", "event_type"),
    Index("idx_events_category", "event_category"),
    Index("idx_events_occurred_at", "occurred_at"),
    Index("idx_events_status", "status"),
    Index("idx_events_agent", "agent_id"),
    Index("idx_events_obligated_by", "obligated_by_event_id"),
    Index("idx_events_discharges", "discharges_event_id"),
    Index(
      "idx_events_source_external",
      "source",
      "external_id",
      unique=True,
      postgresql_where="external_id IS NOT NULL",
    ),
    # QuickBooks writeback marker lookups. Declared here so tenants
    # provisioned by `create_all` get it too.
    Index(
      "idx_events_qb_external_id",
      text("(metadata->>'qb_external_id')"),
      postgresql_where=text("metadata->>'qb_external_id' IS NOT NULL"),
    ),
    CheckConstraint(
      "status IN ('captured', 'classified', 'committed', 'pending', 'fulfilled', 'voided', 'superseded')",
      name="check_event_status",
    ),
    CheckConstraint(
      "(event_class = 'economic' AND event_category IN ("
      "'sales', 'purchase', 'financing', 'payroll', "
      "'treasury', 'adjustment', 'recognition', 'other')) "
      "OR (event_class = 'support' AND event_category IN ("
      "'control', 'approval', 'reconciliation', 'inquiry')) "
      "OR (event_class = 'operational' AND event_category IN ("
      "'pipeline', 'engagement', 'schedule', 'other'))",
      name="check_event_category",
    ),
    CheckConstraint(
      "event_class IN ('economic', 'support', 'operational')",
      name="check_event_class",
    ),
    CheckConstraint(
      "resource_type IN ('goods', 'services', 'money', 'right', 'obligation', 'information', 'labor') OR resource_type IS NULL",
      name="check_event_resource_type",
    ),
    # `source` deliberately has no CHECK: adapter and external sources are
    # validated against the graph's registered Connections at the ops layer,
    # so registering a connection opens a source without a schema change.
    CheckConstraint(_EVENT_ACTION_CHECK, name="check_event_action"),
    Index(
      "idx_events_action",
      "event_action",
      postgresql_where="event_action IS NOT NULL",
    ),
    # Reconciliation queue read path; nearly all rows are drift=false.
    Index(
      "idx_events_payload_drift",
      "payload_drift",
      postgresql_where="payload_drift = true",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("evt"))

  event_type = Column(String, nullable=False)
  event_category = Column(String, nullable=False)
  # economic    — a resource flows (REA economic event); drives the GL.
  # support     — controls, approvals, reconciliations, inquiries; non-posting.
  # operational — neither: a lead, a lifecycle change, a schedule set up.
  #               Filing these as economic would assert a resource flow.
  event_class = Column(String, nullable=False, default="economic")

  event_action = Column(String, nullable=True)

  # agent_id FKs agents(id) in the DB only.
  agent_id = Column(String, nullable=True)
  resource_type = Column(String, nullable=True)
  resource_element_id = Column(String, nullable=True)

  occurred_at = Column(DateTime, nullable=False)
  effective_at = Column(DateTime, nullable=True)

  status = Column(String, nullable=False, default="captured")

  source = Column(String, nullable=False)
  external_id = Column(String, nullable=True)
  external_url = Column(String, nullable=True)

  # Self-references below are application-validated, with no FK constraint.
  # Correction chain.
  replaced_by_event_id = Column(String, nullable=True)
  replaces_event_id = Column(String, nullable=True)

  # REA duality: the event that scheduled this one (e.g. a depreciation entry
  # pointing at asset_acquired), and the obligation this one settles (e.g.
  # cash_received pointing at sale_invoiced).
  obligated_by_event_id = Column(String, nullable=True)
  discharges_event_id = Column(String, nullable=True)

  amount = Column(BigInteger, nullable=True)  # signed cents
  currency = Column(String, nullable=False, default="USD")

  description = Column(String, nullable=True)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Set when a re-sync finds a changed payload for a committed/fulfilled
  # event; the live metadata stays immutable and the incoming payload is
  # stashed at metadata_['drift_payload'].
  payload_drift = Column(Boolean, nullable=False, default=False, server_default="false")

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Event {self.id} {self.event_type} {self.status}>"
