"""Name the constraint an ``IntegrityError`` violated.

Commands that lose an insert race translate the error into their typed
conflict only for the expected constraint, so an unrelated FK or CHECK
failure is not hidden as a duplicate. Falls back to parsing the message when
the driver carries no diagnostic.
"""

from __future__ import annotations

import re

from sqlalchemy.exc import IntegrityError

_QUOTED_NAME = re.compile(r'(?:constraint|index) "([^"]+)"')


def violated_constraint(exc: IntegrityError) -> str | None:
  """The name of the constraint or unique index ``exc`` reports, if any."""
  orig = getattr(exc, "orig", None)
  diag = getattr(orig, "diag", None)
  name = getattr(diag, "constraint_name", None) if diag is not None else None
  if name:
    return str(name)
  match = _QUOTED_NAME.search(str(orig) if orig is not None else str(exc))
  return match.group(1) if match else None


def violates(exc: IntegrityError, *names: str) -> bool:
  """Whether ``exc`` fired one of the named constraints/indexes."""
  return violated_constraint(exc) in names


__all__ = ["violated_constraint", "violates"]
