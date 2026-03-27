"""Ledger summary response models."""

from datetime import date, datetime

from pydantic import BaseModel


class LedgerSummaryResponse(BaseModel):
  graph_id: str
  account_count: int
  transaction_count: int
  entry_count: int
  line_item_count: int
  earliest_transaction_date: date | None = None
  latest_transaction_date: date | None = None
  connection_count: int = 0
  last_sync_at: datetime | None = None
