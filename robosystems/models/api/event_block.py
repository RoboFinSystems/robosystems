"""API models for the Event Block surface (event-driven-ledger.md)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EventCategory = Literal[
  "sales",
  "purchase",
  "financing",
  "payroll",
  "treasury",
  "adjustment",
  "recognition",
  "other",
]
ResourceType = Literal[
  "goods",
  "services",
  "money",
  "right",
  "obligation",
  "information",
  "labor",
]


class CreateEventBlockRequest(BaseModel):
  """Write surface for a single business event."""

  event_type: str = Field(
    ...,
    description="Open vocabulary: 'invoice_issued' | 'contract_signed' | 'bank_transaction' | ...",
  )
  event_category: EventCategory = Field(
    ...,
    description=(
      "REA economic classification. One of: sales, purchase, financing, "
      "payroll, treasury, adjustment, recognition, other."
    ),
  )

  # REA primitives
  agent_id: str | None = Field(None, description="Counterparty agent id")
  resource_type: ResourceType | None = Field(
    None,
    description=(
      "REA resource kind. One of: goods, services, money, right, obligation, "
      "information, labor."
    ),
  )
  resource_element_id: str | None = Field(
    None, description="Specific element being exchanged, if applicable"
  )

  # Occurrence
  occurred_at: datetime = Field(
    ..., description="When the event happened in the real world"
  )
  effective_at: datetime | None = Field(
    None,
    description="Accounting recognition date, if different from occurred_at",
  )

  # Provenance
  source: str = Field(
    ..., description="'quickbooks' | 'xero' | 'plaid' | 'native' | 'scheduled' | ..."
  )
  external_id: str | None = Field(
    None,
    description=(
      "Source-system dedup key. (source, external_id) is enforced unique "
      "when external_id is provided, so retries from external adapters are "
      "idempotent at the DB level."
    ),
  )
  external_url: str | None = Field(
    None, description="Deep link back to source-system record"
  )

  # Economic value (minor currency units — cents, signed)
  amount: int | None = Field(None, description="Cents, signed")
  currency: str = Field("USD", description="ISO 4217 currency code")

  # Payload
  description: str | None = None
  metadata: dict[str, Any] = Field(
    default_factory=dict, description="Event-type-specific payload"
  )
  dimension_ids: list[str] = Field(default_factory=list)

  apply_handlers: bool = Field(
    False,
    description=(
      "When True, resolves the event_type to a handler (Python registry "
      "first, then DSL) and fires it atomically with event creation."
    ),
  )


class EventBlockEnvelope(BaseModel):
  """Read projection for a single event block."""

  model_config = ConfigDict(from_attributes=True)

  id: str
  event_type: str
  event_category: str
  status: str
  occurred_at: datetime
  effective_at: datetime | None = None
  source: str
  external_id: str | None = None
  external_url: str | None = None
  amount: int | None = None
  currency: str
  description: str | None = None
  metadata: dict[str, Any]
  dimension_ids: list[str]

  agent_id: str | None = None
  resource_type: str | None = None
  resource_element_id: str | None = None

  replaced_by_event_id: str | None = None
  replaces_event_id: str | None = None

  created_at: datetime
  created_by: str


class UpdateEventBlockRequest(BaseModel):
  """Status transitions and field corrections for an event block.

  All fields except event_id are optional — only supplied fields are updated.
  """

  event_id: str

  # Status transition
  transition_to: (
    Literal["committed", "pending", "fulfilled", "voided", "superseded"] | None
  ) = Field(
    None,
    description=(
      "Status transition. Valid moves depend on current status: "
      "captured → committed | voided; classified → committed | pending | "
      "fulfilled | voided; committed → pending | fulfilled | voided; "
      "pending → fulfilled | voided. Terminal states (fulfilled, voided, "
      "superseded) accept no further transitions. Note: classified and "
      "fulfilled are usually set by handlers, not by callers, but the "
      "transition is allowed for corrections."
    ),
  )
  superseded_by_id: str | None = Field(
    None,
    description="New event id that replaces this one. Required when transition_to='superseded'.",
  )

  # Field corrections
  description: str | None = None
  effective_at: datetime | None = None
  metadata_patch: dict[str, Any] = Field(
    default_factory=dict,
    description="Key-value pairs merged into existing metadata (additive patch, not replace).",
  )


class ListEventBlocksRequest(BaseModel):
  """Filter parameters for listing event blocks."""

  event_type: str | None = None
  event_category: str | None = None
  status: str | None = None
  agent_id: str | None = None
  source: str | None = None
  limit: int = Field(50, ge=1, le=1000)
  offset: int = Field(0, ge=0)
