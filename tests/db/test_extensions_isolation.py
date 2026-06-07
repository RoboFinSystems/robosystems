"""Multi-graph-per-org isolation: the `extensions_session` search_path contract.

The extensions DB is multi-tenant by **schema-per-graph**: a tenant's data lives
in its own PostgreSQL schema, reached by `SET search_path TO {schema}, public`.
The whole isolation story has two halves:

1. **Between tasks** — the worker disposes both connection pools after every
   task (even on failure), so a pooled connection can't carry a stale
   search_path from one task into the next. Covered by
   `tests/worker/test_cleanup.py` + `tests/worker/test_consumer.py`.
2. **Per session** — every tenant-scoped session re-sets search_path at session
   *start*, so even if a pooled connection previously served graph A, the next
   `extensions_session(B)` re-scopes it to B before any query runs. **This is
   the load-bearing primitive these tests pin** — it had no direct coverage
   (every other test mocks `extensions_session` rather than exercising it).

The failure mode both halves guard against is real: a prior Celery incident
leaked a search_path across tenants and caused cross-tenant writes (see
`worker/cleanup.py` and the `feedback_session_isolation` note). For the
multi-company-accountant launch — N client-company graphs under one org — this
is the #1 isolation risk, so the contract must not silently regress.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import robosystems.db.extensions as ext
from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session

# Two distinct, well-formed tenant graph_ids (kg + 16+ hex) standing in for two
# graphs under the same org — the multi-graph-per-org case.
GRAPH_A = "kg00000000000000aa"
GRAPH_B = "kg00000000000000bb"


def _drive(graph_id: str, *, session: MagicMock | None = None):
  """Run the REAL `extensions_session(graph_id)` with a mocked session factory.

  Returns `(session_mock, executed_sql)` where `executed_sql` is the ordered
  list of SQL strings the context manager ran — so we can assert exactly which
  `SET search_path` statement it issued (and that it ran first).
  """
  session = session or MagicMock(name="session")
  executed: list[str] = []
  session.execute.side_effect = lambda clause, *a, **k: executed.append(str(clause))
  factory = MagicMock(return_value=session)
  with patch.object(ext, "_get_session_factory", return_value=factory):
    with extensions_session(graph_id) as yielded:
      assert yielded is session  # the tenant op gets the scoped session
  return session, executed


def test_tenant_session_sets_scoped_search_path():
  """A tenant session's FIRST act is to scope search_path to its own schema."""
  session, executed = _drive(GRAPH_A)
  assert executed[0] == f"SET search_path TO {GRAPH_A}, public"
  session.commit.assert_called_once()
  session.close.assert_called_once()


def test_distinct_graphs_get_distinct_search_paths():
  """Two graphs under one org never share a scope — each binds its own schema."""
  _, executed_a = _drive(GRAPH_A)
  _, executed_b = _drive(GRAPH_B)
  assert executed_a[0] == f"SET search_path TO {GRAPH_A}, public"
  assert executed_b[0] == f"SET search_path TO {GRAPH_B}, public"
  assert executed_a[0] != executed_b[0]


def test_sequential_reuse_rescopes_to_new_graph():
  """Connection reuse can't leak: a session that served graph A is re-scoped to
  B on the next `extensions_session(B)`. This is the exact incident fix — the
  re-SET at session start overrides any path left on a reused pooled connection.
  """
  reused = MagicMock(name="reused-connection-session")
  _, executed_a = _drive(GRAPH_A, session=reused)
  _, executed_b = _drive(GRAPH_B, session=reused)  # same underlying session/conn
  assert executed_a[0] == f"SET search_path TO {GRAPH_A}, public"
  # The second use re-issues SET for B — B never inherits A's schema.
  assert executed_b[0] == f"SET search_path TO {GRAPH_B}, public"
  assert GRAPH_A not in executed_b[0]


def test_library_sentinel_binds_public_only():
  """The `library` sentinel reads the canonical library (`public`), with no
  tenant-schema binding — so a library read can't be scoped to a tenant."""
  _, executed = _drive(LIBRARY_GRAPH_ID)
  assert executed[0] == "SET search_path TO public"
  assert LIBRARY_GRAPH_ID not in executed[0]


@pytest.mark.parametrize(
  "bad_graph_id",
  [
    "public",  # not a tenant schema
    "kg0123",  # too short (< 16 hex)
    "kgZZZZZZZZZZZZZZZZ",  # non-hex
    "kg00000000000000aa; DROP SCHEMA public CASCADE; --",  # injection
    "kg00000000000000aa, evil",  # search_path widening
    "",  # empty
  ],
)
def test_rejects_unsafe_graph_id(bad_graph_id):
  """A graph_id that isn't a validated tenant schema is rejected before any
  `SET search_path` runs — defense-in-depth against schema-name injection."""
  session = MagicMock(name="session")
  factory = MagicMock(return_value=session)
  with patch.object(ext, "_get_session_factory", return_value=factory):
    with pytest.raises(ValueError):
      with extensions_session(bad_graph_id):
        pass
  # No tenant scoping was ever issued, and the session was cleaned up.
  for call in session.execute.call_args_list:
    assert "DROP" not in str(call).upper()
  session.rollback.assert_called_once()
  session.close.assert_called_once()


def test_rolls_back_and_closes_on_error():
  """A failed tenant op rolls back and closes the session — so a half-applied,
  still-scoped session is never returned to the pool for the next graph."""
  session = MagicMock(name="session")
  factory = MagicMock(return_value=session)
  with patch.object(ext, "_get_session_factory", return_value=factory):
    with pytest.raises(RuntimeError, match="boom"):
      with extensions_session(GRAPH_A):
        raise RuntimeError("boom")
  session.rollback.assert_called_once()
  session.commit.assert_not_called()
  session.close.assert_called_once()


def test_no_bypass_of_the_scoped_session_factory():
  """Structural guard: the raw session factory (which yields a session with NO
  search_path set) must only ever be consumed inside `extensions_session`.

  Any other call site would hand out an extensions session that skips the
  `SET search_path` — and could silently inherit a leaked path from a pooled
  connection. New bypasses fail here in CI rather than in a customer's books.
  """
  pkg_root = Path(ext.__file__).resolve().parents[1]  # …/robosystems
  offenders: list[str] = []
  for path in pkg_root.rglob("*.py"):
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
      if "_get_session_factory()" not in line:
        continue
      if "def _get_session_factory" in line:
        continue  # the definition itself
      rel = path.relative_to(pkg_root.parent).as_posix()
      if rel != "robosystems/db/extensions.py":
        offenders.append(f"{rel}:{lineno}")
  assert not offenders, (
    "The schema-scoped extensions session factory is consumed outside "
    "`extensions_session()` — these call sites skip `SET search_path` and risk "
    "inheriting a leaked tenant scope from a pooled connection:\n  "
    + "\n  ".join(offenders)
    + "\nRoute tenant DB access through `extensions_session(graph_id)`."
  )
