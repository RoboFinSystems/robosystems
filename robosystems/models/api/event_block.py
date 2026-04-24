"""API models for the Event Block surface (event-driven-ledger.md Phase 1).

Phase 1 ships the capture-only envelope: apply_handlers must be False (the
handler engine ships in Phase 3). The envelope shape is designed to be stable
across phases — Phase 3 populates matched_handlers and triggered_transactions.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class CreateEventBlockRequest(BaseModel):
  """Write surface for a single business event.

  Phase 1: apply_handlers must be False. Passing True raises 501.
  """

  event_type: str = Field(
    ...,
    description="Open vocabulary: 'invoice_issued' | 'contract_signed' | 'bank_transaction' | ...",
  )
  event_category: str = Field(
    ...,
    description="'sales' | 'purchase' | 'financing' | 'payroll' | 'treasury' | 'adjustment' | 'recognition' | 'other'",
  )

  # REA primitives (all nullable in Phase 1; agent_id populated once Phase 2 agents table lands)
  agent_id: str | None = Field(None, description="Counterparty agent id (Phase 2+)")
  resource_type: str | None = Field(
    None,
    description="'goods' | 'services' | 'money' | 'right' | 'obligation' | 'information' | 'labor'",
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
  external_id: str | None = Field(None, description="Source-system dedup key")
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

  # Phase 1: only False is accepted; True raises 501 (handler engine ships in Phase 3)
  apply_handlers: bool = Field(
    False,
    description="Must be False in Phase 1. Pass True only once the handler engine (Phase 3) is live.",
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
  transition_to: Literal["committed", "voided", "superseded"] | None = Field(
    None,
    description=(
      "Status transition. Valid moves depend on current status: "
      "captured → committed | voided; committed → pending | fulfilled | voided; "
      "pending → fulfilled | voided. Terminal states (fulfilled, voided, superseded) "
      "accept no further transitions."
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
