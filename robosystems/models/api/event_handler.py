"""API models for Event Handler — the dynamic event → transaction rule registry.

Phase 3 of the event-driven ledger. EventHandlers are the rules that fire
GL transactions when create-event-block is called with apply_handlers=True.

TransactionTemplate is the DSL shape. Phase 3 ships immediate postings only
(debit/credit legs + {{ expr }} interpolation). Recurring/schedule blocks
come in Phase 4 alongside revenue recognition.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TransactionTemplateLeg(BaseModel):
  """One side of a journal entry leg (debit or credit)."""

  element_id: str = Field(
    ...,
    description="Element ULID (elem_ prefixed) identifying the account to post to",
  )
  amount: str = Field(
    ...,
    description=(
      "Amount expression. Supports: "
      "'{{ event.amount }}' — raw event amount (cents); "
      "'{{ event.amount }} / 2' — half of event amount; "
      "'{{ event.metadata.fee_cents }}' — field from event metadata"
    ),
  )


class TransactionTemplateEntry(BaseModel):
  """One balanced entry (debit + credit pair) — the inner shape of entry_template."""

  debit: TransactionTemplateLeg
  credit: TransactionTemplateLeg


class TransactionTemplateItem(BaseModel):
  """One item in the transactions list — wraps entry_template to match the DSL shape."""

  entry_template: TransactionTemplateEntry


class TransactionTemplate(BaseModel):
  """The handler's output spec — one or more balanced entries to post.

  Wire shape::

      {
        "transactions": [{
          "entry_template": {
            "debit": {"element_id": "elem_...", "amount": "{{ event.amount }}"},
            "credit": {"element_id": "elem_...", "amount": "{{ event.amount }}"}
          }
        }]
      }
  """

  transactions: list[TransactionTemplateItem] = Field(
    ...,
    min_length=1,
    description="At least one debit/credit entry pair",
  )


class CreateEventHandlerRequest(BaseModel):
  name: str
  description: str | None = None

  # Match criteria
  event_type: str
  event_category: str | None = None
  match_source: str | None = None
  match_agent_type: str | None = None
  match_resource_type: str | None = None
  match_metadata_expression: dict | None = Field(
    None,
    description='JSONPath-style equality map, e.g. {"metadata.category": "payroll"}',
  )

  # Template
  transaction_template: TransactionTemplate

  # Priority + lifecycle
  priority: int = 0
  is_active: bool = True
  origin: Literal["hub", "tenant"] = "tenant"

  metadata: dict = Field(default_factory=dict)


class UpdateEventHandlerRequest(BaseModel):
  event_handler_id: str

  name: str | None = None
  description: str | None = None

  # Match criteria patches
  event_category: str | None = None
  match_source: str | None = None
  match_agent_type: str | None = None
  match_resource_type: str | None = None
  match_metadata_expression: dict | None = None

  # Template replacement (full replace, not patch)
  transaction_template: TransactionTemplate | None = None

  priority: int | None = None
  is_active: bool | None = None

  # Approval shortcut — True sets approved_by + approved_at; False clears them
  approve: bool | None = None

  metadata_patch: dict = Field(default_factory=dict)


class EventHandlerResponse(BaseModel):
  model_config = ConfigDict(from_attributes=True)

  id: str
  name: str
  description: str | None = None

  event_type: str
  event_category: str | None = None
  match_source: str | None = None
  match_agent_type: str | None = None
  match_resource_type: str | None = None
  match_metadata_expression: dict | None = None

  transaction_template: dict  # raw JSON for flexibility

  priority: int
  is_active: bool
  origin: str

  suggested_by: str | None = None
  confidence: float | None = None
  approved_by: str | None = None
  approved_at: datetime | None = None

  created_at: datetime | None = None
  updated_at: datetime | None = None
  created_by: str | None = None


class TransactionPreview(BaseModel):
  """A planned GL entry line from preview-event-block (no rows written)."""

  entry_index: int
  debit_element_id: str
  credit_element_id: str
  amount_cents: int
  interpolated_debit_amount: str
  interpolated_credit_amount: str


class PreviewEventBlockResponse(BaseModel):
  """Dry-run result — what would happen if this event block were created."""

  matched_handler: EventHandlerResponse | None = None
  planned_transactions: list[TransactionPreview] = Field(default_factory=list)
  validation_errors: list[str] = Field(default_factory=list)
  would_succeed: bool
