"""QuickBooks transaction type → canonical event action verb mapping.

Single source of truth for the QB-side projection of the canonical
19-verb action vocabulary
(`models/extensions/roboledger/event.py:EVENT_ACTIONS`).

Three consumers must stay in sync:

1. **dbt SQL** (`dbt/models/ledger/transactions.sql`) — projects
   `event_action` directly during transform so downstream loaders get
   the column populated.
2. **OLTP loader** (`pipeline/loader._capture_transactions_as_events`)
   — forwards `txn["event_action"]` into the `Event` row.
3. **Tests** — parity check that the dbt CASE branches match this dict
   (a missing verb in either place is a silent drift).

Some QB tx_types are ambiguous (Invoice can be goods or services →
`transferAllRights` vs `deliverService`); we pick the most common
mapping for v1 and document the ambiguity inline. Operators can refine
post-capture via the update-event-block API.

QB returns tx_type with multiple spellings (`'BillPayment'` vs
`'Bill Payment'` etc.); both spellings carry the same action verb.
"""

from __future__ import annotations

from robosystems.models.extensions.roboledger.event import EVENT_ACTIONS

# Mapping: QB tx_type string (as it appears in dbt-staged transactions)
# → canonical action verb. Coarse but precise enough for v1; the
# `event_action` column is nullable so unmapped types fall through as
# NULL (operators / handlers can fill in post-capture).
QB_TXTYPE_TO_EVENT_ACTION: dict[str, str] = {
  # Sales side — goods/services delivered to a customer in exchange for
  # rights to payment. Goods → transferAllRights, services →
  # deliverService; v1 picks transferAllRights as the safer default
  # since QB doesn't always disambiguate at the transaction level.
  "Invoice": "transferAllRights",
  "SalesReceipt": "transferAllRights",
  "Sales Receipt": "transferAllRights",
  # Purchase side — counterparty delivers; we accept goods/services.
  "Bill": "accept",
  # Cash settlement — money moves both directions. `transfer` covers
  # the rights+custody dual; payments don't fit `transferAllRights`
  # because cash carries both axes together.
  "Payment": "transfer",
  "BillPayment": "transfer",
  "Bill Payment": "transfer",
  "Bill Payment (Credit Card)": "transfer",
  "Bill Payment (Check)": "transfer",
  "Expense": "transfer",
  "Cash Expense": "transfer",
  "Check": "transfer",
  "Credit Card Expense": "transfer",
  "Credit Card Credit": "transfer",
  "Deposit": "transfer",
  # Accounting reclassification — no resource movement, just changes
  # the ledger state of existing resources. `modify` is the closest
  # verb; manual journal entries that DO move resources should be
  # corrected by the operator post-capture.
  "JournalEntry": "modify",
  "Journal Entry": "modify",
  "Inventory Qty Adjust": "modify",
  "Inventory Adjustment": "modify",
}


def map_qb_txtype_to_event_action(tx_type: str | None) -> str | None:
  """Return the canonical action verb for a QB tx_type, or None if unmapped.

  Tolerates None / empty strings (returns None). Unknown types fall
  through as None — the `event_action` column accepts NULL and the
  operator surface can backfill via update-event-block.
  """
  if not tx_type:
    return None
  return QB_TXTYPE_TO_EVENT_ACTION.get(tx_type)


# Invariant: every mapped action must be one of the canonical 19 verbs.
# Checked at import time so a typo in the dict fails fast instead of
# producing rows that violate the DB CHECK.
_invalid = {v for v in QB_TXTYPE_TO_EVENT_ACTION.values() if v not in EVENT_ACTIONS}
if _invalid:
  raise RuntimeError(
    f"QB_TXTYPE_TO_EVENT_ACTION contains values not in EVENT_ACTIONS: {sorted(_invalid)}"
  )
