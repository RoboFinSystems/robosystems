"""The connect-time backfill window a bank feed keeps — pure, no Dagster.

Imported by the providers (the API process) as well as the assets, so it
lives apart from ``sync.py`` and its Dagster imports.
"""

from __future__ import annotations

from datetime import date


def default_backfill_start(today: date | None = None) -> date:
  """The backfill start when none is given: 1 January of last year.

  A provider materializes it into the connection's sync config at connect,
  so the window is pinned there — Link asks the source for the same history
  the sync keeps, and the date does not drift a year every January.
  """
  today = today or date.today()
  return date(today.year - 1, 1, 1)
