"""Closing book response models."""

from pydantic import BaseModel


class ClosingBookItem(BaseModel):
  """One row in the closing book — a navigable artifact for the
  period (statement, schedule, rollup, etc.).

  ``item_type`` discriminates: 'statement', 'schedule',
  'account_rollups', 'period_close', 'trial_balance'. Statement items
  carry ``report_id`` to fetch the rendered facts; schedule items
  carry ``status`` ('complete' | 'draft' | 'pending').
  """

  id: str
  name: str
  item_type: str
  structure_type: str | None = None
  report_id: str | None = None
  status: str | None = None


class ClosingBookCategory(BaseModel):
  """A grouping of closing-book items shown as a sidebar section
  (e.g. Statements, Account Rollups, Schedules, Period Close)."""

  label: str
  items: list[ClosingBookItem]


class ClosingBookStructuresResponse(BaseModel):
  """The closing book navigation tree — categories + items the UI
  uses to render the period-close workspace. ``has_data=False`` when
  the graph has no posted entries yet."""

  categories: list[ClosingBookCategory]
  has_data: bool
