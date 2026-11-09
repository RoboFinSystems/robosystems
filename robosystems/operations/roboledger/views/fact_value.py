"""A fact's number, read from its exact text.

LadybugDB 0.22 can project ``Fact.numeric_value`` as 0.0 when a filtered scan
straddles a compression segment boundary; the ``value`` string beside it is
never affected. Graph reads that return a fact's number return both, and take
the number from the text.
"""

import math
from typing import Any


def fact_number(raw: Any, numeric: Any) -> float | None:
  """The fact's number: ``raw`` parsed, or ``numeric`` when ``raw`` is not a
  finite number. A fact with no ``numeric_value`` has no number."""
  if numeric is None:
    return None
  try:
    parsed = float(raw)
  except TypeError, ValueError:
    return numeric
  return parsed if math.isfinite(parsed) else numeric
