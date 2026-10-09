"""The connect-time backfill window a bank feed keeps — pure, no Dagster.

Imported by the providers (the API process) as well as the assets, so it
lives apart from ``sync.py`` and its Dagster imports.
"""

from __future__ import annotations

from datetime import date, timedelta

# The most history a feed is asked for: Plaid pulls at most 730 days.
MAX_BACKFILL_DAYS = 730


def default_backfill_start(today: date | None = None) -> date:
  """The backfill start when none is given: as far back as the source goes.

  A first connection takes all the history it can; what keeps lines out of
  a month already closed is the capture fence (``bank_feed.load``), not the
  window. A provider materializes the date into the connection's sync
  config at connect, so Link asks the source for the same history the sync
  keeps and the window does not move afterwards.
  """
  today = today or date.today()
  return today - timedelta(days=MAX_BACKFILL_DAYS - 1)
