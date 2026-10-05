"""Precision-aware fact deduplication shared by the graph-backed views.

A filer often reports one figure twice under the same element and period: on
the statement face and, rounded, in the narrative. XBRL's rule for consistent
duplicates is to use the most precise value; this module applies it.

A duplicate shares its unit. The same figure in a second unit (a foreign
filer's own currency beside a dollar translation) is another fact, so callers
put the unit in their key.
"""

from collections.abc import Callable, Hashable
from typing import Any

UNKNOWN_PRECISION = float("-inf")


def precision_rank(decimals: Any) -> float:
  """Rank XBRL ``decimals`` so larger is more precise: ``INF`` highest,
  missing or unparseable lowest."""
  if decimals is None:
    return UNKNOWN_PRECISION
  text = str(decimals).strip()
  if not text:
    return UNKNOWN_PRECISION
  if text.upper() == "INF":
    return float("inf")
  try:
    return float(int(text))
  except ValueError:
    return UNKNOWN_PRECISION


def keep_most_precise(
  rows: list[dict[str, Any]],
  key: Callable[[dict[str, Any]], Hashable],
  rank: Callable[[dict[str, Any]], Any] | None = None,
) -> list[dict[str, Any]]:
  """Collapse ``rows`` to one per ``key(row)``, keeping the highest ``rank``
  (by default the most precise).

  Ties keep the first row seen. Output keeps first-seen key order, so a
  query's ``ORDER BY`` survives.
  """
  rank_of = rank or (lambda row: precision_rank(row.get("decimals")))
  position: dict[Hashable, int] = {}
  ranks: list[Any] = []
  deduped: list[dict[str, Any]] = []
  for row in rows:
    k = key(row)
    row_rank = rank_of(row)
    slot = position.get(k)
    if slot is None:
      position[k] = len(deduped)
      ranks.append(row_rank)
      deduped.append(row)
    elif row_rank > ranks[slot]:
      ranks[slot] = row_rank
      deduped[slot] = row
  return deduped


def units_reported_together(
  rows: list[dict[str, Any]], line: Callable[[dict[str, Any]], Hashable]
) -> list[str]:
  """The units of every ``line(row)`` that is reported in more than one,
  sorted; empty when each line has a single unit."""
  units_by_line: dict[Hashable, set[str]] = {}
  for row in rows:
    unit = row.get("unit")
    if unit:
      units_by_line.setdefault(line(row), set()).add(str(unit))
  mixed: set[str] = set()
  for units in units_by_line.values():
    if len(units) > 1:
      mixed |= units
  return sorted(mixed)


def mixed_units_note(units: list[str]) -> str:
  """What a reader must know when ``units_reported_together`` found any."""
  return (
    f"Some figures are reported in more than one unit ({', '.join(units)}); "
    "each fact carries `unit`. Compare and total within one unit only."
  )
