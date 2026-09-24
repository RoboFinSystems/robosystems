"""Reading rows out of a LadybugDB ``QueryResult``.

``QueryResult`` has no ``get_as_list``; a ``hasattr`` guard for it is always
false and fails silently. Use ``result_rows`` rather than hand-rolling the
loop.
"""

from typing import Any


def result_rows(result: Any) -> list[Any]:
  """Return every row of ``result`` exactly as the engine yields it."""
  rows: list[Any] = []
  while result.has_next():
    rows.append(result.get_next())
  return rows
