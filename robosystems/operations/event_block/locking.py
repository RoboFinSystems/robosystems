"""Row-lock policy for event writes.

Every path that decides an event's next status does so read-decide-write, and
the decision is only sound if nothing else can move the row in between. Two
writers that both read `captured` both fire the handler, and the event ends up
with two sets of GL rows — a ledger that still foots and is still wrong.

So the reads that feed those decisions take `FOR UPDATE`. This module holds the
half of that policy which is about *waiting*: how long a request-facing caller
waits for a conflicting writer before giving up, and what the failure is called.

The split that matters: **background jobs wait, request handlers do not.** A
sync or a Dagster sweep should block behind a conflicting approval rather than
fail its batch, so it takes the lock unbounded. An HTTP request that waits out a
multi-minute sync pins a pooled connection for that whole time, and enough of
them exhaust the extensions pool — so request-facing callers wrap their locking
work in `bounded_lock_wait`.
"""

from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

# How long a request-facing caller waits for a conflicting writer before giving
# up. Long enough to absorb another approval or a short write — those resolve in
# milliseconds, and failing them would be a spurious error — and short enough
# that a multi-minute sync returns an answer instead of holding the connection.
# Postgres raises SQLSTATE 55P03 on expiry, the same code `NOWAIT` raises, so
# this handler covers both if the zero-wait variant is ever wanted.
_LOCK_TIMEOUT_MS = 3000
_LOCK_NOT_AVAILABLE = "55P03"


class EventLockedError(Exception):
  """Raised when event rows needed by this operation are held by another writer.

  In practice that writer is a running sync or the obligation-promotion sweep,
  both of which lock their whole batch for the life of their transaction.
  Retryable — it is the one error on these operations the caller should try
  again rather than fix.
  """


@contextmanager
def bounded_lock_wait(session: Session, detail: str):
  """Bound this transaction's wait for a row lock and give the failure a name.

  `SET LOCAL` reverts at transaction end, so the bound applies to this
  operation only. Only 55P03 is translated: a connection fault keeps its own
  identity rather than reaching the caller as "retry in a moment".
  """
  session.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT_MS}ms'"))
  try:
    yield
  except OperationalError as exc:
    if getattr(exc.orig, "pgcode", None) == _LOCK_NOT_AVAILABLE:
      raise EventLockedError(detail) from exc
    raise


__all__ = ["EventLockedError", "bounded_lock_wait"]
