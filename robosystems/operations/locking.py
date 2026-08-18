"""Row-lock policy for state transitions.

**The shape this exists for**: load a row, branch on a state column, write that
column back. Approving an event, reversing a journal entry, closing or reopening
a fiscal period, filing a report — all of them. Unlocked, two callers read the
same state, both pass the guard, and both act: one event with two sets of GL
rows, one entry reversed twice, one period closed twice. Each of those leaves
books that still foot, which is why none of them announce themselves.

So the read that feeds the decision takes `FOR UPDATE`, and `lock_by_id` is the
one-call version of doing that correctly. This module also holds the half of the
policy about *waiting*: how long a request-facing caller waits for a conflicting
writer before giving up, and what the failure is called.

It lives at `operations/` root deliberately. It began under `event_block/`, which
made it invisible to every other subsystem with the same shape — and the second
and third instances of this bug were found in `roboledger/` only after someone
went looking. A shared discipline filed inside one of its consumers is a
discipline the next consumer will not find.

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
# acquisition sequence cannot diverge (see `ordered_lock_column`), and the
# supersede pair in `update_event_block` now locks both of its rows in that
# same order. So no path in this module is known to reach it.
#
# It stays translated as defense, and the honest limit is worth stating: a
# deadlock materializes when the conflicting writes are **flushed**, which for
# operations whose commit belongs to `extensions_session` happens outside any
# wrapper here. Covering those needs the translation at the transaction
# boundary, not at lock acquisition.
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
#
# Exported as the column itself, not its name, so the call sites `order_by` it
# rather than each hardcoding `Event.id` beside a comment pointing here — a
# constant nothing reads cannot keep anything in step with it.
def ordered_lock_column():
  """The column every batch-locking read over `events` must order by."""
  from robosystems.models.extensions.roboledger.event import Event

  return Event.id


class RowLockedError(Exception):
  """Raised when rows this operation must write are held by another writer.

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
      raise RowLockedError(detail) from exc
    raise


def lock_by_id(session: Session, entity, ident, detail: str):
  """Load one row by primary key, locked and refreshed, with a bounded wait.

  The correct form of the read that feeds a state transition, in one call, so
  each site does not have to remember all four parts:

  - `FOR UPDATE`, so a concurrent writer cannot move the row between the read
    and the write that follows it;
  - `populate_existing`, so a caller reusing a session gets the row as the
    database has it rather than as its identity map remembers it;
  - `session.flush()` first, because these sessions are `autoflush=False`
    (`db/extensions.py`) and the refresh would otherwise discard an in-flight
    change without a word;
  - a bounded wait, so a request blocked behind a long background writer
    returns a retryable error instead of pinning a pooled connection.

  Returns `None` when the row does not exist — callers raise their own typed
  not-found error, since that message is theirs to phrase.

  Not for multi-row locks: those must order by `ordered_lock_column()` in a
  single statement, or two callers acquiring the same rows in opposite orders
  deadlock.
  """
  session.flush()
  with bounded_lock_wait(session, detail):
    return session.get(entity, ident, with_for_update=True, populate_existing=True)


__all__ = [
  "RowLockedError",
  "bounded_lock_wait",
  "lock_by_id",
  "ordered_lock_column",
]
