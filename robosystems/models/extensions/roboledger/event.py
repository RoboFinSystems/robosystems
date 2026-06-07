"""Event model — real-world business event layer (event-driven-ledger.md).

Status lifecycle: captured → classified → committed → pending → fulfilled,
with voided and superseded as terminal off-ramps. The triggered_by_event_id
column on transactions and entries links GL rows back to the originating
event for the audit chain.

Two REA duality links — economic relationships expressing obligation origin
(``obligated_by_event_id``) and obligation discharge (``discharges_event_id``)
— let events express duality at the event layer rather than only at the GL:
``obligated_by_event_id`` points at the event that scheduled this one
(forward materialization — e.g. a depreciation schedule entry pointing back
at the asset_acquired event); ``discharges_event_id`` points at the
obligation this event settles (e.g. cash_received pointing at the originating
sale_invoiced). Both are nullable, application-validated self-references,
matching the existing pattern for ``replaced_by_event_id``.

``event_class`` (``'economic' | 'support'``) is orthogonal to ``event_category``.
Economic events change resources; support events (control, approval,
reconciliation, inquiry) are audit-trail / value-chain primitives that
don't move resources.
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
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid

# Canonical 19-verb action vocabulary refining `event_category`.
# Vocabulary converges with Valueflows
# (Foster / Pavlik / Haugen v1.0) — we treat that as inspiration, not
# provenance. Canonical home is here. The 19 verbs disambiguate
# concepts ERPs typically conflate (custody-only vs. rights-transfer
# is the load-bearing distinction for consignment, drop-shipping,
# marketplace settlement, escrow).
#
# MUST stay in sync with:
#   - the CHECK constraint in
#     `migrations/extensions/versions/0012_event_action.py`
#   - the Pydantic `EventAction` Literal in
#     `robosystems/models/api/event_block.py`
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

# sorted() is load-bearing: the migration's hardcoded IN clause is in
# alphabetical order, and Alembic's autogenerate compares constraint
# expressions textually. Any reordering here produces spurious "alter
# check_event_action" diffs on every `just migrate-create`. Keep sorted.
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
    CheckConstraint(
      "status IN ('captured', 'classified', 'committed', 'pending', 'fulfilled', 'voided', 'superseded')",
      name="check_event_status",
    ),
    CheckConstraint(
      "(event_class = 'economic' AND event_category IN ("
      "'sales', 'purchase', 'financing', 'payroll', "
      "'treasury', 'adjustment', 'recognition', 'other')) "
      "OR (event_class = 'support' AND event_category IN ("
      "'control', 'approval', 'reconciliation', 'inquiry'))",
      name="check_event_category",
    ),
    CheckConstraint(
      "event_class IN ('economic', 'support')",
      name="check_event_class",
    ),
    CheckConstraint(
      "resource_type IN ('goods', 'services', 'money', 'right', 'obligation', 'information', 'labor') OR resource_type IS NULL",
      name="check_event_resource_type",
    ),
    # source enumerates where the event came from. Adapter-driven sources
    # (quickbooks, xero, plaid) emit events from incoming data; native
    # sources (manual, schedule, system) emit events from inside the
    # platform. Adding a new source means widening this CHECK and updating
    # the adapter that emits it.
    CheckConstraint(
      "source IN ('manual', 'system', 'schedule', 'quickbooks', 'xero', 'plaid')",
      name="check_event_source",
    ),
    CheckConstraint(_EVENT_ACTION_CHECK, name="check_event_action"),
    Index(
      "idx_events_action",
      "event_action",
      postgresql_where="event_action IS NOT NULL",
    ),
    # Reconciliation queue read path — surfaces committed/fulfilled events
    # whose adapter-side payload changed under us. Partial because the
    # vast majority of rows have drift=false at all times.
    Index(
      "idx_events_payload_drift",
      "payload_drift",
      postgresql_where="payload_drift = true",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("evt"))

  # Event identity
  event_type = Column(String, nullable=False)
  event_category = Column(String, nullable=False)
  event_class = Column(String, nullable=False, default="economic")

  # Canonical action verb — finer-grained than event_category. See EVENT_ACTIONS.
  event_action = Column(String, nullable=True)

  # REA primitives. agent_id FK to agents(id) enforced at the DB level.
  agent_id = Column(String, nullable=True)
  resource_type = Column(String, nullable=True)
  resource_element_id = Column(String, nullable=True)

  # Occurrence
  occurred_at = Column(DateTime, nullable=False)
  effective_at = Column(DateTime, nullable=True)

  # Lifecycle
  status = Column(String, nullable=False, default="captured")

  # Provenance
  source = Column(String, nullable=False)
  external_id = Column(String, nullable=True)
  external_url = Column(String, nullable=True)

  # Correction chain (self-referential FKs — enforced via migration, not ORM relationship)
  replaced_by_event_id = Column(String, nullable=True)
  replaces_event_id = Column(String, nullable=True)

  # Duality chain (REA): forward-materialization + settlement links.
  # Both are application-validated self-references; same pattern as the
  # correction chain above.
  obligated_by_event_id = Column(String, nullable=True)
  discharges_event_id = Column(String, nullable=True)

  # Economic value (minor currency units — cents, signed)
  amount = Column(BigInteger, nullable=True)
  currency = Column(String, nullable=False, default="USD")

  # Narrative
  description = Column(String, nullable=True)

  # Event-type-specific payload
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Drift flag: set true when an adapter re-sync surfaces a payload diff
  # against a `committed`/`fulfilled` event. The live `metadata_` payload
  # stays unchanged (committed entries are immutable to re-sync); the
  # incoming payload is stashed at
  # `metadata_['drift_payload']` for the reconciliation queue.
  payload_drift = Column(Boolean, nullable=False, default=False, server_default="false")

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Event {self.id} {self.event_type} {self.status}>"
