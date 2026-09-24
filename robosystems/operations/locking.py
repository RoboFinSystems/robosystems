"""Row-lock policy for read-decide-write state transitions.

The read that feeds a state decision (approve an event, reverse an entry, close
a period) takes `FOR UPDATE`; unlocked, two callers both pass the guard and
both act, and the books still foot, so nothing announces it.

Background jobs wait; request handlers do not. A sync or sweep locks unbounded;
request-facing callers wrap their locking in `bounded_lock_wait` so they cannot
pin a pooled connection behind a multi-minute sync.
"""

from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

# Long enough to absorb a competing approval, short enough that a request
# blocked behind a sync returns instead of holding the connection. Expiry
# raises 55P03, the same code as `NOWAIT`.
_LOCK_TIMEOUT_MS = 3000
_LOCK_NOT_AVAILABLE = "55P03"

# Deadlock: retryable like 55P03. Not known to be reachable (batch locks share
# one order, see `ordered_lock_column`), translated as defense. A deadlock
# raised at flush/commit inside `extensions_session` bypasses these wrappers.
_DEADLOCK_DETECTED = "40P01"

_RETRYABLE_LOCK_STATES = frozenset({_LOCK_NOT_AVAILABLE, _DEADLOCK_DETECTED})


def ordered_lock_column():
  """The column every batch-locking read over `events` must order by.

  Overlapping batch locks (the promotion sweep and
  `supersede_pending_obligations` match the same pending obligations) deadlock
  unless they acquire in one order. The primary key is total and immutable.
  """
  from robosystems.models.extensions.roboledger.event import Event

  return Event.id


class RowLockedError(Exception):
  """Rows this operation must write are held by another writer (usually a sync
  or the promotion sweep). Retryable."""


@contextmanager
def bounded_lock_wait(session: Session, detail: str):
  """Bound this transaction's lock wait; translate lock timeout and deadlock
  (only those) to `RowLockedError`. `SET LOCAL` reverts at transaction end."""
  session.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT_MS}ms'"))
  try:
    yield
  except OperationalError as exc:
    if getattr(exc.orig, "pgcode", None) in _RETRYABLE_LOCK_STATES:
      raise RowLockedError(detail) from exc
    raise


def lock_by_id(session: Session, entity, ident, detail: str):
  """Load one row by primary key, `FOR UPDATE` and refreshed, with a bounded wait.

  Flushes first: extensions sessions are `autoflush=False`, and
  `populate_existing` would otherwise silently discard an in-flight change.
  Returns `None` when the row does not exist. Not for multi-row locks, which
  must order by `ordered_lock_column()` in one statement.
  """
  session.flush()
  with bounded_lock_wait(session, detail):
    return session.get(entity, ident, with_for_update=True, populate_existing=True)


# ── Period write fence ───────────────────────────────────────────────────
#
# Close commits mid-flow (QuickBooks markers), so no transaction-scoped lock
# can span it. An advisory lock on (graph, period) is the barrier: close and
# reopen hold it exclusive and session-scoped on a dedicated connection;
# period-affecting writers hold it shared and transaction-scoped. Lock order is
# always the fence first, then row locks, so writer and close cannot deadlock.

# Arbitrary but must stay stable: changing it splits in-flight lockers.
_PERIOD_FENCE_CLASS = 872401


def period_fence_ident(graph_id: str, period: str) -> str:
  """Stable identity string hashed in SQL as the fence's second key."""
  return f"{graph_id}:{period}"


def acquire_shared_period_fence(
  session: Session, graph_id: str, period: str, *, detail: str
) -> None:
  """Take a transaction-scoped shared fence on ``(graph_id, period)``.

  Waits up to the request lock timeout for an exclusive closer, then raises
  :class:`RowLockedError`.
  """
  with bounded_lock_wait(session, detail):
    session.execute(
      text("SELECT pg_advisory_xact_lock_shared(:classid, hashtext(:ident))"),
      {
        "classid": _PERIOD_FENCE_CLASS,
        "ident": period_fence_ident(graph_id, period),
      },
    )


@contextmanager
def exclusive_period_fence(
  graph_id: str, period: str, *, detail: str, wait_ms: int | None = None
):
  """Hold a session-scoped exclusive fence on ``(graph_id, period)``.

  Uses a dedicated connection so the lock survives the close's mid-flow
  commit. ``wait_ms`` defaults to the request wait; the background close passes
  a longer budget so a duplicate dispatch waits and sees "already closed".
  The connection is unlocked or invalidated before release: a pooled
  connection still holding the lock would block every later closer.
  """
  from robosystems.db.extensions import get_extensions_engine

  ident = period_fence_ident(graph_id, period)
  params = {"classid": _PERIOD_FENCE_CLASS, "ident": ident}
  wait = _LOCK_TIMEOUT_MS if wait_ms is None else wait_ms
  conn = get_extensions_engine().connect()
  acquired = False
  try:
    conn.execute(text(f"SET lock_timeout = '{wait}ms'"))
    try:
      conn.execute(
        text("SELECT pg_advisory_lock(:classid, hashtext(:ident))"),
        params,
      )
      acquired = True
    except OperationalError as exc:
      # Clear the session-level timeout before the connection returns to the
      # pool, or the next borrower inherits it.
      try:
        conn.rollback()
        conn.execute(text("RESET lock_timeout"))
        conn.commit()
      except Exception:
        conn.invalidate()
      if getattr(exc.orig, "pgcode", None) in _RETRYABLE_LOCK_STATES:
        raise RowLockedError(detail) from exc
      raise
    # RESET restores the connection default rather than pinning "wait forever".
    conn.execute(text("RESET lock_timeout"))
    conn.commit()
    yield
  finally:
    if acquired:
      try:
        conn.execute(
          text("SELECT pg_advisory_unlock(:classid, hashtext(:ident))"),
          params,
        )
        conn.commit()
      except Exception:
        conn.invalidate()
    conn.close()


__all__ = [
  "RowLockedError",
  "acquire_shared_period_fence",
  "bounded_lock_wait",
  "exclusive_period_fence",
  "lock_by_id",
  "ordered_lock_column",
  "period_fence_ident",
]
