"""Reading rows out of a LadybugDB ``QueryResult``.

One helper, because the alternative has already cost us a bug. Callers used
to write::

    rows = result.get_as_list() if hasattr(result, "get_as_list") else list(result)

``QueryResult`` has no ``get_as_list`` — it exposes ``has_next``/``get_next``,
``get_all`` and ``rows_as_dict`` — so that guard is always false and every
caller silently took the fallback. It reads as though either branch might
fire, which is what makes it dangerous: the vector-search HNSW reader wrote
the same guard *without* the ``else``, and answered HTTP 200 with zero rows
for every query until the branch was removed (PR #1368). Nothing raised,
because a missing method behind ``hasattr`` is not an error.

Use this instead of hand-rolling the loop. For rows keyed by column name,
the row-to-dict readers in ``service.py`` and ``engine.py`` do that job —
this one stays positional, and returns rows exactly as the engine yields
them so callers can keep their own shape handling.
"""

from typing import Any


def result_rows(result: Any) -> list[Any]:
  """Return every row of ``result`` as a list, in engine order.

  Rows come back as the engine yields them — no normalization — so a caller
  that distinguishes tuple rows from dict rows can still do so.
  """
  rows: list[Any] = []
  while result.has_next():
    rows.append(result.get_next())
  return rows
