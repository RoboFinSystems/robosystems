"""Closing book response models."""

from pydantic import BaseModel


class ClosingBookItem(BaseModel):
  id: str
  name: str
  item_type: str  # statement, schedule, account_rollups, period_close, trial_balance
  structure_type: str | None = None  # income_statement, balance_sheet, etc.
  report_id: str | None = None  # for statement items — which report to fetch
  status: str | None = None  # for schedules: complete, draft, pending


class ClosingBookCategory(BaseModel):
  label: str  # Statements, Account Rollups, Schedules, Period Close
  items: list[ClosingBookItem]


class ClosingBookStructuresResponse(BaseModel):
  categories: list[ClosingBookCategory]
  has_data: bool  # at least one posted entry exists
