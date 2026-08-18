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

# Deadlock. Postgres has already aborted this transaction and rolled it back, so
# there is nothing to salvage — but it is retryable in exactly the sense 55P03
# is, and it must not reach the caller as an unhandled 500. It should be
# unreachable between the batch-locking reads, which all order by `id` so their
# acquisition sequence cannot diverge (see ORDERED_LOCK_KEY); this catches the
# paths that lock more than one row without a shared order — chiefly the
# supersede pair in `update_event_block`, where two callers superseding each
# other in opposite directions each hold what the other wants.
_DEADLOCK_DETECTED = "40P01"

_RETRYABLE_LOCK_STATES = frozenset({_LOCK_NOT_AVAILABLE, _DEADLOCK_DETECTED})


# Every batch-locking read over `events` must order by this column, and they
# must all use the *same* one.
#
# Two transactions that lock overlapping row sets in different orders deadlock:
# each ends up holding a row the other is waiting for. The sets do overlap — a
# pending `schedule_entry_due` obligation is matched by both the promotion
# sweep's predicate and `supersede_pending_obligations`' — and without an
# ORDER BY the acquisition sequence is whatever each query's plan happens to
# produce, which is not a property either query controls or a test would notice.
#
# `id` because it is the primary key: unique (so the order is total, never
# ambiguous), immutable (so a concurrent status write cannot reorder anything
# mid-scan), and present on every one of these reads. Ordering by `occurred_at`
# would satisfy none of those.
ORDERED_LOCK_KEY = "id"


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
  operation only. Only the two retryable lock states are translated — a
  connection fault keeps its own identity rather than reaching the caller as
  "retry in a moment".
  """
  session.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT_MS}ms'"))
  try:
    yield
  except OperationalError as exc:
    if getattr(exc.orig, "pgcode", None) in _RETRYABLE_LOCK_STATES:
      raise EventLockedError(detail) from exc
    raise


__all__ = ["EventLockedError", "bounded_lock_wait"]
