"""QuickBooks tx_type → canonical event action verb (``EVENT_ACTIONS``).

The CASE in ``dbt/models/ledger/transactions.sql`` must match this dict; a
test checks the parity. QB spells some tx_types several ways; every spelling
maps to the same verb.
"""

from __future__ import annotations

from robosystems.models.extensions.roboledger.event import EVENT_ACTIONS

# Unmapped types get a NULL event_action, refinable after capture.
QB_TXTYPE_TO_EVENT_ACTION: dict[str, str] = {
  # Goods vs services (deliverService) isn't distinguishable per
  # transaction in QB; transferAllRights is the default.
  "Invoice": "transferAllRights",
  "SalesReceipt": "transferAllRights",
  "Sales Receipt": "transferAllRights",
  "Bill": "accept",
  # Cash settlement: rights and custody move together.
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
  # Reclassification, no resource movement. A manual JE that does move
  # resources needs correcting after capture.
  "JournalEntry": "modify",
  "Journal Entry": "modify",
  "Inventory Qty Adjust": "modify",
  "Inventory Adjustment": "modify",
}


def map_qb_txtype_to_event_action(tx_type: str | None) -> str | None:
  if not tx_type:
    return None
  return QB_TXTYPE_TO_EVENT_ACTION.get(tx_type)


# Fail at import rather than on the DB CHECK constraint.
_invalid = {v for v in QB_TXTYPE_TO_EVENT_ACTION.values() if v not in EVENT_ACTIONS}
if _invalid:
  raise RuntimeError(
    f"QB_TXTYPE_TO_EVENT_ACTION contains values not in EVENT_ACTIONS: {sorted(_invalid)}"
  )
