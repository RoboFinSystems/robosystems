"""Name the constraint an ``IntegrityError`` violated.

A command that pre-checks for a conflict and then inserts still loses the
occasional race to a concurrent identical insert; the database answers with
an ``IntegrityError``, and the command translates it into the same typed
conflict its pre-check raises. That translation must be *specific*: catching
every ``IntegrityError`` as "duplicate" would report an unrelated fault — a
foreign key to a row that vanished, a CHECK on a bad value — as a benign
conflict and hide it. So the sites ask which constraint fired.

psycopg2 exposes it on ``exc.orig.diag.constraint_name``; when the driver
does not carry a diagnostic (a stub in tests, another driver), the message
is parsed for the ``"name"`` PostgreSQL quotes after ``constraint``/``index``.
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
