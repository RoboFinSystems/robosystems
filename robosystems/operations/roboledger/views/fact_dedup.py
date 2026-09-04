"""Precision-aware fact deduplication shared by the graph-backed views.

A filer often reports one figure twice in one filing under the same element
and period: once on the face of a statement at the statement's precision and
once in the narrative, rounded (3M FY2024 research and development expense is
``1,085,000,000`` at ``decimals=-6`` on the income statement and
``1,100,000,000`` at ``decimals=-8`` in the text). Both are consolidated,
undimensioned facts with identical period identity, so a dedup that keeps
whichever row the engine returned first hands back the rounded figure about
half the time. XBRL's own rule for consistent duplicates is that the most
precise value is the one to use; this module applies it.
"""

from collections.abc import Callable, Hashable
from typing import Any

UNKNOWN_PRECISION = float("-inf")


def precision_rank(decimals: Any) -> float:
  """Order a fact's XBRL ``decimals`` so that a larger rank is more precise.

  ``"INF"`` (an exact value) outranks every finite precision; an integer
  string such as ``"-6"`` ranks as that integer, so ``-6`` (millions) beats
  ``-8`` (hundred-millions); a missing or unparseable value ranks below any
  stated precision.
  """
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
  rows: list[dict[str, Any]], key: Callable[[dict[str, Any]], Hashable]
) -> list[dict[str, Any]]:
  """Collapse ``rows`` to one per ``key(row)``, keeping the most precise.

  Precision is ``precision_rank(row["decimals"])``. When two rows tie, the
  first one seen stays: at equal precision two consistent duplicates carry
  the same value, so the choice cannot change the number. Output keeps the
  order in which each key was first seen, so a caller's ``ORDER BY`` on
  the query survives the dedup.
  """
  position: dict[Hashable, int] = {}
  ranks: list[float] = []
  deduped: list[dict[str, Any]] = []
  for row in rows:
    k = key(row)
    rank = precision_rank(row.get("decimals"))
    slot = position.get(k)
    if slot is None:
      position[k] = len(deduped)
      ranks.append(rank)
      deduped.append(row)
    elif rank > ranks[slot]:
      ranks[slot] = rank
      deduped[slot] = row
  return deduped
