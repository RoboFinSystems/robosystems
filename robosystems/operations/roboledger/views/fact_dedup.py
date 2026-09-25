"""Precision-aware fact deduplication shared by the graph-backed views.

A filer often reports one figure twice under the same element and period: on
the statement face and, rounded, in the narrative. XBRL's rule for consistent
duplicates is to use the most precise value; this module applies it.
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
