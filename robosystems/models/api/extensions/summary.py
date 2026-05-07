"""Ledger summary response models."""

from datetime import date, datetime

from pydantic import BaseModel


class LedgerSummaryResponse(BaseModel):
  """High-level rollup of a graph's ledger state — counts plus the
  date-range bookends and integration sync timestamp.

  Used by dashboards and the onboarding wizard to answer "is this
  graph populated yet?" without walking every transaction. ``connection_count``
  reflects active integrations (QuickBooks / Plaid / etc.); a non-null
  ``last_sync_at`` means at least one connection has run.
  """

  graph_id: str
  account_count: int
  transaction_count: int
  entry_count: int
  line_item_count: int
  earliest_transaction_date: date | None = None
  latest_transaction_date: date | None = None
  connection_count: int = 0
  last_sync_at: datetime | None = None
