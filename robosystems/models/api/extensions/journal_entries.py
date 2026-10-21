"""Journal entry write models — native accounting CRUD surface.

The Pydantic shapes used by:
  - `create-event-block(event_type='journal_entry_recorded')` for new
    entries and `event_type='journal_entry_reversed'` for reversals (via
    the metadata schema → `CreateJournalEntryRequest` /
    `ReverseJournalEntryRequest` adapter calls).
  - `update-journal-entry` / `delete-journal-entry` ops for in-place
    draft correction.

For display/read models of existing transactions and entries, see
`transactions.py` (`LedgerEntryResponse`, `LedgerTransactionDetailResponse`,
etc.).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ── Line item shapes ─────────────────────────────────────────────────────


class JournalEntryLineItemInput(BaseModel):
  """One debit/credit line in a journal entry create/update payload.

  Exactly one of `debit_amount` / `credit_amount` must be non-zero; the
  other must be zero. Amounts are in minor currency units (cents)."""

  element_id: str = Field(
    ..., description="Element ULID identifying the account to post to."
  )
  debit_amount: int = Field(
    0, description="Debit amount in cents. Must be 0 if `credit_amount` > 0."
  )
  credit_amount: int = Field(
    0, description="Credit amount in cents. Must be 0 if `debit_amount` > 0."
  )
  description: str | None = Field(
    None, description="Per-line memo (overrides the entry-level memo on this line)."
  )
  flow_qname: str | None = Field(
    None,
    description=(
      "The cash flow row this line's cash belongs to, an rs-gaap flow qname "
      "(`flowCandidates(entityId)` lists the rows this company's statement "
      "offers). Only for a loan drawn or repaid, capital put in or taken out, "
      "an asset bought or sold; omitted, the account's default flow applies. "
      "A flow set on one side of the entry tags the untagged lines on the "
      "other: the cash flow statement reads the cash line and the "
      "roll-forwards read the account line. On the group's consolidation "
      "entity, which moves no cash, every tagged line counts on its own, "
      "cash-effect signed (a credit is an inflow), so an elimination offsets "
      "the flow a member's line produced, and a tag does not spread to the "
      "other side. Refused when it is not a row of "
      "the entity's cash flow statement."
    ),
  )
  dimension_ids: list[str] | None = Field(
    None,
    description=(
      "The management dimension members this line carries (department, "
      "class, location, project), by id from `dimensions`; at most one per "
      "axis. An untagged line is read as the axis's Unallocated."
    ),
  )
  metadata: dict[str, Any] | None = Field(
    None,
    description=(
      "Optional per-line metadata stamped on `LineItem.metadata_`. "
      "Used to carry source-system fields the standard columns don't "
      "cover — e.g. an external flow-tag code that drives rollforward "
      "attribution (`transaction_description_code`), an external "
      "memo, or a cost-center hint. Pass-through is non-validating; "
      "the renderer / filter engine reads keys it knows about and "
      "ignores the rest. `None` is normalized to `{}` at persist "
      "time."
    ),
  )


class JournalEntryLineItemResponse(BaseModel):
  """One line in a journal entry response."""

  id: str
  element_id: str
  debit_amount: int
  credit_amount: int
  description: str | None = None
  line_order: int
  flow_qname: str | None = Field(
    None,
    description=(
      "The cash flow row this line carries, when tagged; null means the "
      "account's default flow."
    ),
  )
  dimension_ids: list[str] = Field(
    default_factory=list,
    description="The management dimension members the line carries, by id.",
  )


# ── Entry response ───────────────────────────────────────────────────────


# What a consolidating entry adjusts (spec ``consolidation`` §4.2); mirrors
# ``Entry.ADJUSTMENT_KINDS``.
AdjustmentKind = Literal[
  "elimination_investment",
  "elimination_intercompany",
  "elimination_unrealized_profit",
  "nci_allocation",
  "nci_distribution",
  "equity_method_pickup",
  "equity_method_distribution",
  "acquisition",
  "deconsolidation",
  "translation",
  "manual",
]


class JournalEntryResponse(BaseModel):
  """The common response shape for journal entry write operations."""

  id: str
  transaction_id: str | None = None
  type: str
  status: str
  posting_date: date
  memo: str | None = None
  provenance: str | None = None
  reversal_of: str | None = None
  posted_at: datetime | None = None
  line_items: list[JournalEntryLineItemResponse]
  total_debit: int
  total_credit: int
  adjustment_kind: str | None = None
  adjusts_entity_id: str | None = None


# ── Create ────────────────────────────────────────────────────────────────


class CreateJournalEntryRequest(BaseModel):
  """Create a new journal entry with balanced line items.

  Defaults to `status='draft'` for ongoing native writes (the normal
  workflow: draft → review → post via close-period). Pass
  `status='posted'` for historical data import where entries represent
  already-happened business events that don't need review.

  Total debit amount must equal total credit amount or the request
  is rejected with 422. `line_items` must contain at least two rows
  (at least one debit, at least one credit).
  """

  posting_date: date
  memo: str
  line_items: list[JournalEntryLineItemInput] = Field(..., min_length=2)
  type: Literal["standard", "adjusting", "closing", "reversing"] = "standard"
  status: Literal["draft", "posted"] = "draft"
  transaction_id: str | None = None
  # Transaction provenance; adapters pass their own source + connection_id
  # so re-syncs scope their deletes. Defaults to `native`.
  source: str | None = None
  connection_id: str | None = None
  # Business-event kind (e.g. `bill_paid`); distinct from the entry-level
  # `type`, which is the accounting role (standard / adjusting / ...).
  transaction_type: str = "journal_entry"
  # A consolidating entry in the group's consolidation entity: what it
  # adjusts, and the entity whose figures it adjusts. Refused on any other
  # entity's books.
  adjustment_kind: AdjustmentKind | None = None
  adjusts_entity_id: str | None = None


# ── Update ────────────────────────────────────────────────────────────────


class JournalEntryLineFlow(BaseModel):
  """A line's cash flow row, set in place."""

  line_id: str = Field(..., description="The line to re-tag, by id.")
  flow_qname: str | None = Field(
    None,
    description=(
      "The rs-gaap flow qname the line takes; null clears it, back to the "
      "account's default flow."
    ),
  )


class JournalEntryLineDimensions(BaseModel):
  """A line's management dimension members, set in place."""

  line_id: str = Field(..., description="The line to re-tag, by id.")
  dimension_ids: list[str] = Field(
    ...,
    description=(
      "The members the line carries from now on, replacing what it carried; "
      "empty clears them. At most one per axis."
    ),
  )


class UpdateJournalEntryRequest(BaseModel):
  """Update a draft journal entry, or re-tag any entry's flows.

  Posted entries accept only `line_flows` and `line_dimensions`: neither
  moves a balance, so each changes in place, closed month or not. Anything
  else on a posted entry is a reversal and a corrected entry.

  Omitted fields are left unchanged. If `line_items` is provided, the
  existing line items are replaced entirely (atomic delete + insert)
  and the new set must still balance. If `line_items` is omitted, the
  existing line items are left untouched.
  """

  entry_id: str = Field(..., description="The draft entry to update.")
  posting_date: date | None = Field(None, description="New posting date.")
  memo: str | None = Field(None, description="New entry-level memo.")
  type: Literal["standard", "adjusting", "closing", "reversing"] | None = Field(
    None,
    description="New entry type. `closing` should normally only be set by close-period.",
  )
  line_items: list[JournalEntryLineItemInput] | None = Field(
    None,
    description=(
      "Replacement line items. Whole-list replacement (not patch). The "
      "new set must still balance (total_debit == total_credit). Omit to "
      "leave existing lines untouched."
    ),
  )
  line_flows: list[JournalEntryLineFlow] | None = Field(
    None,
    min_length=1,
    description=(
      "Re-tag lines in place: each named line takes its `flow_qname` (null "
      "clears it), and when every line on that side of the entry then "
      "carries one flow, the lines on the other side not named here follow "
      "when they were in step with it — untagged, or carrying the flow the "
      "named lines carried before — so the cash line and the account line "
      "agree; a line tagged differently on purpose keeps its tag. Allowed on "
      "a posted entry, closed month included, and the only change one "
      "accepts; recorded on the entry as `flow_retags`, with what each "
      "changed line carried before. Not combinable with `line_items`, which "
      "replaces the lines (set `flow_qname` on them)."
    ),
  )
  line_dimensions: list[JournalEntryLineDimensions] | None = Field(
    None,
    min_length=1,
    description=(
      "Re-tag lines' management dimensions in place: each named line carries "
      "exactly the members given (empty clears them). A dimension moves no "
      "balance, so this is allowed on a posted entry, closed month included; "
      "recorded on the entry as `dimension_retags`, with what each line "
      "carried before. Not combinable with `line_items` (set "
      "`dimension_ids` on the new lines)."
    ),
  )

  @model_validator(mode="after")
  def _refuse_nulls_on_required_fields(self) -> UpdateJournalEntryRequest:
    # Omitted leaves a field unchanged; an explicit null would clear a column
    # the entry cannot be without.
    nulled = [
      name
      for name in ("posting_date", "type")
      if name in self.model_fields_set and getattr(self, name) is None
    ]
    if nulled:
      raise ValueError(f"{', '.join(nulled)} cannot be null; omit it to keep it.")
    return self

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "entry_id": "ent_01HVF8T0M2YTAY3BBNRH0V0",
          "memo": "Corrected vendor reference",
        },
        {
          "entry_id": "ent_01HVF8T0M2YTAY3BBNRH0V0",
          "line_items": [
            {"element_id": "elem_coa_office_supplies", "debit_amount": 12500},
            {"element_id": "elem_coa_accounts_payable", "credit_amount": 12500},
          ],
        },
      ]
    }
  )


# ── Delete ────────────────────────────────────────────────────────────────


class DeleteJournalEntryRequest(BaseModel):
  """Hard delete a draft journal entry. Posted entries cannot be
  deleted — reverse them instead."""

  entry_id: str = Field(..., description="The draft entry to delete.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"entry_id": "ent_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    }
  )


# ── Reverse ───────────────────────────────────────────────────────────────


class ReverseJournalEntryRequest(BaseModel):
  """Reverse a posted journal entry.

  Creates a new entry whose line items flip the originals
  (debits → credits, credits → debits), sets `reversal_of` on the new
  entry to the original's id, marks the original as
  `status='reversed'`, and posts the reversing entry immediately.

  This is how accountants correct posted entries — the original stays
  in the audit trail, the reversal cancels its effect, and a
  corrected entry can be created separately.
  """

  entry_id: str
  posting_date: date | None = None  # defaults to today
  memo: str | None = None  # auto-generated if None


class OpeningBalanceInput(BaseModel):
  """One account's balance on the day the books begin, in its normal
  direction: an asset as a debit, a liability or equity account as a credit
  (a contra account the other way)."""

  element_id: str = Field(..., description="The balance-sheet account, by id.")
  debit_amount: int = Field(0, ge=0, description="Debit balance in cents.")
  credit_amount: int = Field(0, ge=0, description="Credit balance in cents.")


class RecordOpeningBalanceRequest(BaseModel):
  """The balance sheet a ledger starts from."""

  entity_id: str | None = Field(
    None, description="The entity whose books start here; omit for the group parent."
  )
  balances: list[OpeningBalanceInput] = Field(
    ...,
    min_length=1,
    description=(
      "One balance per balance-sheet account (assets, liabilities, equity). "
      "An income or expense account is refused: income before the books "
      "begin is already in retained earnings."
    ),
  )
  equity_element_id: str | None = Field(
    None,
    description=(
      "The equity account that takes any difference between debits and "
      "credits, usually Opening Balance Equity. Required when the balances "
      "do not balance; a non-zero balance left there is a review item."
    ),
  )
  memo: str | None = Field(
    None, description="The entry's memo; defaults to 'Opening balances'."
  )
