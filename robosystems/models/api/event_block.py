"""API models for the Event Block surface (event-driven-ledger.md)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EventClass = Literal["economic", "support"]
EventCategory = Literal[
  # Economic categories — change resources, drive GL postings.
  "sales",
  "purchase",
  "financing",
  "payroll",
  "treasury",
  "adjustment",
  "recognition",
  "other",
  # Support categories — value-chain / audit-trail primitives, no GL impact.
  # Valid only when event_class='support'; the DB CHECK enforces the pairing.
  "control",
  "approval",
  "reconciliation",
  "inquiry",
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

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Capture-only invoice (no GL impact yet)",
          "description": (
            "Persist an invoice event from QuickBooks for review. "
            "`apply_handlers=false` (default) leaves status='captured' "
            "and produces no GL entries — the inbox UI shows it for "
            "the bookkeeper to triage."
          ),
          "value": {
            "event_type": "invoice_issued",
            "event_category": "sales",
            "event_class": "economic",
            "agent_id": "ent_customer_acme",
            "resource_type": "services",
            "occurred_at": "2026-05-01T14:30:00Z",
            "source": "quickbooks",
            "external_id": "qb_inv_4521",
            "external_url": "https://app.qbo.intuit.com/app/invoice?txnId=4521",
            "amount": 250000,
            "currency": "USD",
            "description": "April consulting retainer — Acme Corp",
            "metadata": {
              "invoice_number": "INV-2026-0421",
              "terms": "net_30",
            },
            "dimension_ids": ["dim_dept_consulting"],
          },
        },
        {
          "summary": "Atomic capture + classify (fires handler)",
          "description": (
            "Persist a bank transaction and immediately resolve a "
            "matching event_handler to post the GL entries in the same "
            "transaction. Status lands at 'classified'. Use "
            "`preview-event-block` first to confirm the GL plan."
          ),
          "value": {
            "event_type": "bank_transaction",
            "event_category": "treasury",
            "event_class": "economic",
            "occurred_at": "2026-05-03T00:00:00Z",
            "source": "plaid",
            "external_id": "plaid_txn_abc123",
            "amount": -125000,
            "currency": "USD",
            "description": "ACH out — Office rent May 2026",
            "metadata": {
              "merchant_name": "Westlake Properties LLC",
              "category": ["Rent"],
            },
            "apply_handlers": True,
          },
        },
        {
          "summary": "Support event (no GL impact)",
          "description": (
            "Audit-trail primitive — a control execution. "
            "`event_class='support'` skips the economic value path "
            "entirely; `amount` is null."
          ),
          "value": {
            "event_type": "control_executed",
            "event_category": "control",
            "event_class": "support",
            "occurred_at": "2026-05-06T16:00:00Z",
            "source": "native",
            "description": "Monthly bank reconciliation completed",
            "metadata": {
              "control_id": "ctl_bank_recon_monthly",
              "reconciled_balance": 4287500,
            },
          },
        },
      ]
    }
  )

  event_type: str = Field(
    ...,
    description="Open vocabulary: 'invoice_issued' | 'contract_signed' | 'bank_transaction' | ...",
  )
  event_category: EventCategory = Field(
    ...,
    description=(
      "REA classification. Economic categories (sales, purchase, financing, "
      "payroll, treasury, adjustment, recognition, other) require "
      "event_class='economic'. Support categories (control, approval, "
      "reconciliation, inquiry) require event_class='support'. The DB CHECK "
      "rejects mismatched pairings."
    ),
  )
  event_class: EventClass = Field(
    "economic",
    description=(
      "REA event class. 'economic' events change resources and drive GL "
      "postings; 'support' events are audit-trail / value-chain primitives "
      "(typically captured with apply_handlers=False)."
    ),
  )

  # REA primitives
  agent_id: str | None = Field(
    None,
    description=(
      "ID of the counterparty agent (customer, vendor, employee, "
      "lender) involved in the event. `null` for internal-only events."
    ),
  )
  resource_type: ResourceType | None = Field(
    None,
    description=(
      "REA resource kind being exchanged. One of: `goods`, `services`, "
      "`money`, `right`, `obligation`, `information`, `labor`."
    ),
  )
  resource_element_id: str | None = Field(
    None,
    description=(
      "ID of the specific element being exchanged, when known (e.g. "
      "the cash account for a treasury movement, the inventory item "
      "for a sale)."
    ),
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
  amount: int | None = Field(
    None,
    description=(
      "Economic value of the event in **cents** of `currency`, signed. "
      "Sign convention follows the perspective of the entity that owns "
      "the graph: inflows positive, outflows negative. `null` for "
      "non-economic events (e.g. `event_class='support'`)."
    ),
  )
  currency: str = Field(
    "USD",
    description="ISO 4217 currency code for `amount`.",
  )

  # Payload
  description: str | None = Field(
    None,
    description="Free-text human-readable summary of the event.",
  )
  metadata: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Free-form payload, opaque to the event surface. When "
      "`apply_handlers=True`, the matched handler's metadata schema "
      "validates this dict — required keys depend on the handler "
      "registered for `event_type`. Use `preview-event-block` to "
      "discover the expected shape without writing."
    ),
  )
  dimension_ids: list[str] = Field(
    default_factory=list,
    description=(
      "IDs of dimension members tagging this event (e.g. "
      "department, fund, project). Propagate to the GL entries "
      "produced by the handler."
    ),
  )

  # REA duality links — economic relationships expressing obligation origin
  # (`obligated_by_event_id`) and obligation discharge (`discharges_event_id`).
  # Forward-materialization + settlement links. Both are application-validated
  # self-references; same nullable-FK pattern as the correction chain.
  obligated_by_event_id: str | None = Field(
    None,
    description=(
      "Forward-materialization link: the event that scheduled or obligated "
      "this one (e.g. depreciation entries point at the asset_acquired event)."
    ),
  )
  discharges_event_id: str | None = Field(
    None,
    description=(
      "Settlement link: the obligation this event discharges (e.g. "
      "cash_received pointing at the originating sale_invoiced)."
    ),
  )

  apply_handlers: bool = Field(
    False,
    description=(
      "When True, resolves the event_type to a handler (Python registry "
      "first, then DSL) and fires it atomically with event creation."
    ),
  )


class EventBlockEnvelope(BaseModel):
  """Read projection for a single event block.

  Returned by `create-event-block`, `update-event-block`,
  `preview-event-block`, and the read-side `get-event-block`. Mirrors
  the request shape plus server-assigned fields (id, status, audit
  timestamps) and the four nullable correction/duality links.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str = Field(..., description="Event ID (`evt_*` ULID).")
  event_type: str = Field(
    ...,
    description=(
      "Open-vocabulary event type (e.g. `invoice_issued`, "
      "`bank_transaction`, `control_executed`)."
    ),
  )
  event_category: str = Field(
    ...,
    description=(
      "REA category — economic (`sales`, `purchase`, `financing`, "
      "`payroll`, `treasury`, `adjustment`, `recognition`, `other`) "
      "or support (`control`, `approval`, `reconciliation`, `inquiry`)."
    ),
  )
  status: str = Field(
    ...,
    description=(
      "Lifecycle state. One of: `captured` (raw, pre-classification), "
      "`classified` (handler ran, GL pending), `committed` (GL "
      "entries posted), `pending` (committed but awaiting fulfillment "
      "of an obligation), `fulfilled` (obligation discharged), "
      "`voided` (canceled — terminal), `superseded` (replaced by a "
      "corrected event — terminal). See `UpdateEventBlockRequest"
      ".transition_to` for the valid transition graph."
    ),
  )
  occurred_at: datetime = Field(
    ..., description="When the event happened in the real world (UTC)."
  )
  effective_at: datetime | None = Field(
    None,
    description="Accounting recognition date, when different from `occurred_at`.",
  )
  source: str = Field(
    ...,
    description=(
      "Capture source (`quickbooks`, `xero`, `plaid`, `native`, "
      "`scheduled`, …). Used for adapter routing."
    ),
  )
  external_id: str | None = Field(
    None,
    description=(
      "Source-system dedup key. Unique with `source` when set, so "
      "adapter retries are idempotent."
    ),
  )
  external_url: str | None = Field(
    None, description="Deep link back to the source-system record."
  )
  amount: int | None = Field(
    None,
    description=(
      "Economic value in **cents** of `currency`, signed (inflows "
      "positive, outflows negative). `null` for non-economic events."
    ),
  )
  currency: str = Field(..., description="ISO 4217 currency code for `amount`.")
  description: str | None = Field(None, description="Free-text human-readable summary.")
  metadata: dict[str, Any] = Field(
    ...,
    description=(
      "Free-form payload — handler-specific keys when the event ran "
      "through a handler, otherwise whatever the adapter captured."
    ),
  )
  dimension_ids: list[str] = Field(
    ...,
    description=(
      "Dimension-member IDs tagging this event (department, fund, "
      "project). Propagate to GL entries produced by the handler."
    ),
  )

  event_class: str = Field(
    ...,
    description=(
      "REA event class — `economic` (drives GL postings) or `support` "
      "(audit-trail / value-chain primitive, no GL impact)."
    ),
  )

  agent_id: str | None = Field(
    None, description="Counterparty agent ID, when the event involves one."
  )
  resource_type: str | None = Field(
    None,
    description=(
      "REA resource kind being exchanged (`goods`, `services`, "
      "`money`, `right`, `obligation`, `information`, `labor`)."
    ),
  )
  resource_element_id: str | None = Field(
    None,
    description="Specific element ID being exchanged, when known.",
  )

  replaced_by_event_id: str | None = Field(
    None,
    description=(
      "ID of the event that replaces this one. Set when this event "
      "was superseded (`status='superseded'`); points forward in the "
      "correction chain."
    ),
  )
  replaces_event_id: str | None = Field(
    None,
    description=(
      "ID of the event this one replaces, when applicable. Points "
      "backward in the correction chain. Mirror of "
      "`replaced_by_event_id` on the predecessor."
    ),
  )

  obligated_by_event_id: str | None = Field(
    None,
    description=(
      "Forward-materialization link — the event that scheduled or "
      "obligated this one (e.g. depreciation entries point at the "
      "originating `asset_acquired` event)."
    ),
  )
  discharges_event_id: str | None = Field(
    None,
    description=(
      "Settlement link — the obligation this event discharges (e.g. "
      "`cash_received` pointing at the originating `sale_invoiced`)."
    ),
  )

  created_at: datetime = Field(..., description="Row creation timestamp (UTC).")
  created_by: str = Field(
    ...,
    description=(
      "ID of the user who captured the event. For adapter-sourced "
      "events, the system actor associated with the adapter."
    ),
  )


class UpdateEventBlockRequest(BaseModel):
  """Status transitions and field corrections for an event block.

  All fields except event_id are optional — only supplied fields are updated.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Captured → committed (fires handler)",
          "description": (
            "Promote a captured event to committed. The registered "
            "Python handler runs against the captured metadata to "
            "produce GL rows; if the handler fails (validation, closed "
            "period, unbalanced lines) the call returns 422."
          ),
          "value": {
            "event_id": "evt_abc123",
            "transition_to": "committed",
          },
        },
        {
          "summary": "Field correction without status change",
          "description": (
            "Reclassify the recognition date and append review metadata "
            "without moving status. `metadata_patch` merges keys into "
            "existing metadata."
          ),
          "value": {
            "event_id": "evt_abc123",
            "effective_at": "2026-04-30T23:59:59Z",
            "description": "Reviewed and approved by controller",
            "metadata_patch": {
              "reviewed_by": "usr_controller_jane",
              "review_note": "Recognized in April per terms",
            },
          },
        },
        {
          "summary": "Supersede with replacement",
          "description": (
            "Mark this event as superseded by a corrected one. "
            "`superseded_by_id` is required when "
            "`transition_to='superseded'`."
          ),
          "value": {
            "event_id": "evt_oldwrong",
            "transition_to": "superseded",
            "superseded_by_id": "evt_newcorrect",
          },
        },
      ]
    }
  )

  event_id: str = Field(
    ...,
    description="Target event ID.",
  )

  # Status transition
  transition_to: (
    Literal["committed", "pending", "fulfilled", "voided", "superseded"] | None
  ) = Field(
    None,
    description=(
      "Status transition. Valid moves depend on current status: "
      "captured → committed | voided | superseded; "
      "classified → committed | pending | fulfilled | voided | superseded; "
      "committed → pending | fulfilled | voided | superseded; "
      "pending → fulfilled | voided | superseded. Terminal states "
      "(fulfilled, voided, superseded) accept no further transitions. "
      "Note: classified and fulfilled are usually set by handlers, not by "
      "callers, but the transition is allowed for corrections."
    ),
  )
  superseded_by_id: str | None = Field(
    None,
    description="New event id that replaces this one. Required when transition_to='superseded'.",
  )

  # Field corrections
  description: str | None = Field(
    None,
    description=(
      "Replacement free-text summary. Unset = unchanged; pass an empty string to clear."
    ),
  )
  effective_at: datetime | None = Field(
    None,
    description=(
      "New accounting recognition date. Unset = unchanged. Useful "
      "when an event was captured against the wrong period."
    ),
  )
  metadata_patch: dict[str, Any] = Field(
    default_factory=dict,
    description="Key-value pairs merged into existing metadata (additive patch, not replace).",
  )

  # Duality late-binding (e.g. mark a payment as discharging an invoice
  # after the fact, when the link wasn't known at capture time).
  obligated_by_event_id: str | None = Field(
    None, description="Set/update the forward-materialization link."
  )
  discharges_event_id: str | None = Field(
    None, description="Set/update the settlement link."
  )
