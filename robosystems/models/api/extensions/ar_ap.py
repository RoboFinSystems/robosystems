"""AR/AP response models — open-balance projections derived from the event
duality chain (event-driven-ledger.md §5.1).

AR ("Accounts Receivable") = sum of ``invoice_issued`` events minus
matching ``payment_received`` discharges. AP ("Accounts Payable") =
sum of ``bill_received`` events minus matching ``bill_paid``
discharges. Numbers come straight from ``events.amount`` — no GL roll
required — so an open balance is auditable in one query.

Amounts are stored in minor currency units (cents); response models
preserve that for backend math. UI layers display-format on render.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class OpenBalanceAggregate(BaseModel):
  """Graph-wide open AR or open AP aggregate.

  ``total_open_cents`` is the sum of unsettled originating-event
  balances; ``counterparty_count`` is the number of distinct agents
  with at least one open invoice or bill. The currency is uniform
  per graph today (mixed-currency books would need a per-currency
  breakdown — out of scope for v1).
  """

  model_config = ConfigDict(from_attributes=True)

  total_open_cents: int = Field(
    ..., description="Sum of unsettled balances in minor currency units."
  )
  counterparty_count: int = Field(
    ..., description="Distinct agents with at least one open balance."
  )
  open_event_count: int = Field(
    ..., description="Distinct originating events with a nonzero open balance."
  )
  currency: str = Field("USD", description="Currency code (uniform per graph).")


class OpenBalanceByAgent(BaseModel):
  """Per-agent open balance row.

  Used for both the aging-by-counterparty list and the per-Agent
  GraphQL field. ``open_balance_cents`` reflects only the unsettled
  remainder (sum of originating amounts minus sum of discharges) so
  partial-payment chains net out correctly.
  """

  model_config = ConfigDict(from_attributes=True)

  agent_id: str
  open_balance_cents: int = Field(
    ...,
    description=(
      "Unsettled originating-event amounts minus discharges, in minor "
      "currency units. Positive for normal AR/AP; negative when overpaid."
    ),
  )
  open_event_count: int = Field(
    ..., description="Number of originating events with nonzero balance for this agent."
  )
  currency: str = Field("USD")
