"""API models for Event Handler — the dynamic event → transaction rule registry.

EventHandlers map ``event_type`` plus optional match expressions onto a
transaction template. When ``create-event-block`` runs with
``apply_handlers=True``, the registry resolves the matching handler and the
engine evaluates its template to produce GL rows.

``TransactionTemplate`` is the DSL shape: immediate postings only
(debit/credit legs + ``{{ expr }}`` interpolation). Recurring / schedule-block
templates are not yet implemented — periodic side effects are produced today
via the obligation register (``schedule_entry_due`` events), not via DSL
template recurrence.
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
  """Register a new event-type → transaction-template rule.

  When ``create-event-block`` runs with ``apply_handlers=True``, the
  registry resolves the *highest-priority active* handler whose match
  criteria all match the event, then evaluates the
  ``transaction_template`` to produce GL rows. Match precedence: among
  active handlers for the same ``event_type``, the one with the most
  specific match (more match fields satisfied) wins; ties broken by
  ``priority`` desc, then ``created_at`` asc.

  All match fields except ``event_type`` are optional — leaving them
  unset matches anything. Use ``match_metadata_expression`` for
  fine-grained discrimination (e.g. only payroll categories).
  """

  name: str = Field(..., description="Human-readable handler name (unique per graph).")
  description: str | None = Field(
    None, description="Free-form description shown in admin UIs."
  )

  # Match criteria
  event_type: str = Field(
    ...,
    description=(
      "Event type to match. Matches the `event_type` field on incoming "
      "events from `create-event-block`."
    ),
  )
  event_category: str | None = Field(
    None,
    description="Optional category filter (e.g. 'expense', 'revenue').",
  )
  match_source: str | None = Field(
    None,
    description=(
      "Match the event's `source` field (e.g. 'quickbooks', 'plaid'). "
      "Useful when the same event_type comes from multiple integrations."
    ),
  )
  match_agent_type: str | None = Field(
    None,
    description="Match agent-emitted events by agent_type.",
  )
  match_resource_type: str | None = Field(
    None,
    description="Match resource-bound events by resource_type.",
  )
  match_metadata_expression: dict | None = Field(
    None,
    description=(
      "JSONPath-style equality map against event.metadata, e.g. "
      '{"category": "payroll"} or {"metadata.category": "payroll"}'
    ),
  )

  # Template
  transaction_template: TransactionTemplate = Field(
    ...,
    description=(
      "The DSL spec for the GL rows this handler produces. See "
      "`TransactionTemplate` for the shape."
    ),
  )

  # Priority + lifecycle
  priority: int = Field(
    0,
    description=(
      "Tiebreaker when multiple equally-specific handlers match. Higher = wins."
    ),
  )
  is_active: bool = Field(
    True, description="Inactive handlers are ignored at match time."
  )
  origin: Literal["hub", "tenant"] = Field(
    "tenant",
    description=(
      "Provenance of the handler. `tenant` = author by graph owner; "
      "`hub` = platform-shipped template (immutable for tenants)."
    ),
  )

  metadata: dict = Field(
    default_factory=dict,
    description="Free-form metadata stored alongside the handler.",
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "name": "QB Vendor Bill — Default Expense Posting",
          "event_type": "vendor_bill_received",
          "match_source": "quickbooks",
          "transaction_template": {
            "transactions": [
              {
                "entry_template": {
                  "debit": {
                    "element_id": "elem_coa_office_supplies",
                    "amount": "{{ event.amount }}",
                  },
                  "credit": {
                    "element_id": "elem_coa_accounts_payable",
                    "amount": "{{ event.amount }}",
                  },
                }
              }
            ]
          },
          "priority": 0,
        },
        {
          "name": "Payroll Run — Wages + Tax Liability",
          "event_type": "payroll_run_completed",
          "event_category": "expense",
          "match_metadata_expression": {"category": "payroll"},
          "transaction_template": {
            "transactions": [
              {
                "entry_template": {
                  "debit": {
                    "element_id": "elem_coa_wages_expense",
                    "amount": "{{ event.amount }}",
                  },
                  "credit": {
                    "element_id": "elem_coa_wages_payable",
                    "amount": "{{ event.amount }}",
                  },
                }
              },
              {
                "entry_template": {
                  "debit": {
                    "element_id": "elem_coa_payroll_tax_expense",
                    "amount": "{{ event.metadata.tax_cents }}",
                  },
                  "credit": {
                    "element_id": "elem_coa_payroll_tax_liability",
                    "amount": "{{ event.metadata.tax_cents }}",
                  },
                }
              },
            ]
          },
          "priority": 10,
        },
      ]
    }
  )


class UpdateEventHandlerRequest(BaseModel):
  """Update an existing event handler. All fields except
  ``event_handler_id`` are optional — pass only what changes.

  ``transaction_template`` is **fully replaced** when supplied (no
  partial template patches). ``metadata_patch`` does deep-merge into
  the existing metadata. ``approve=true`` sets ``approved_by`` and
  ``approved_at``; ``approve=false`` clears them.
  """

  event_handler_id: str = Field(..., description="The handler to update.")

  name: str | None = Field(None, description="New name. Omit to leave unchanged.")
  description: str | None = Field(None, description="New description.")

  # Match criteria patches
  event_category: str | None = None
  match_source: str | None = None
  match_agent_type: str | None = None
  match_resource_type: str | None = None
  match_metadata_expression: dict | None = None

  # Template replacement (full replace, not patch)
  transaction_template: TransactionTemplate | None = Field(
    None,
    description=(
      "Replacement template. Whole-template replace — no partial patch. "
      "Omit to leave unchanged."
    ),
  )

  priority: int | None = None
  is_active: bool | None = Field(
    None,
    description=(
      "Toggle activation. False removes the handler from match-time "
      "consideration without deleting it."
    ),
  )

  # Approval shortcut — True sets approved_by + approved_at; False clears them
  approve: bool | None = Field(
    None,
    description=(
      "Approval shortcut: True stamps approved_by/approved_at to the "
      "current user; False clears both."
    ),
  )

  metadata_patch: dict = Field(
    default_factory=dict,
    description=(
      "Deep-merged into the existing handler.metadata. Pass `{}` to "
      "leave metadata unchanged."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "event_handler_id": "evh_01HVF8T0M2YTAY3BBNRH0V0",
          "is_active": False,
        },
        {
          "event_handler_id": "evh_01HVF8T0M2YTAY3BBNRH0V0",
          "approve": True,
          "priority": 100,
        },
      ]
    }
  )


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
  handler_metadata: dict = Field(
    default_factory=dict,
    description=(
      "Handler-specific compute output. For Python handlers like "
      "'asset_disposed', includes NBV, gain/loss, accumulated depreciation. "
      "Empty for DSL-handler previews."
    ),
  )
