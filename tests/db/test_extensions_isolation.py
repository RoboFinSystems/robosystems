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
3. **On return** — each session also clears search_path back to `public` in its
   `finally`, before the connection returns to the pool. A plain (non-LOCAL)
   `SET` survives the pool's rollback-on-return, so without this the tenant
   scope would linger on the pooled connection; this is defense-in-depth for any
   future code path that reuses a connection without going through this manager.

The failure mode these layers guard against is real: a pooled connection that
keeps a search_path set by a previous tenant will write that tenant's schema
(see `worker/cleanup.py`). With N client-company graphs under one org, this is
the highest-severity isolation risk, so the contract must not silently regress.
"""

from __future__ import annotations

import re
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


def test_resets_search_path_to_public_on_return():
  """Defense-in-depth: a tenant session's LAST act is to clear search_path back
  to `public`, so the connection returns to the pool unbound. A plain SET
  survives the pool's rollback-on-return, so without this a later bypass could
  inherit the tenant scope."""
  session, executed = _drive(GRAPH_A)
  assert executed[0] == f"SET search_path TO {GRAPH_A}, public"
  assert executed[-1] == "SET search_path TO public"
  session.close.assert_called_once()


def test_resets_search_path_even_on_error():
  """The reset runs in `finally`, so even a failed tenant op leaves the pooled
  connection unbound rather than scoped to the failed tenant."""
  session = MagicMock(name="session")
  executed: list[str] = []
  session.execute.side_effect = lambda clause, *a, **k: executed.append(str(clause))
  factory = MagicMock(return_value=session)
  with patch.object(ext, "_get_session_factory", return_value=factory):
    with pytest.raises(RuntimeError, match="boom"):
      with extensions_session(GRAPH_A):
        raise RuntimeError("boom")
  assert executed[-1] == "SET search_path TO public"
  session.rollback.assert_called_once()
  session.close.assert_called_once()


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


# ---------------------------------------------------------------------------
# The "spatial" half of graph_id-keying: no cache key may be scoped on org_id.
# ---------------------------------------------------------------------------
#
# The DB-half guard above pins the *connection* boundary (search_path). This
# one pins the *cache* boundary. Per-graph tenant DATA must never be keyed on
# org_id: one org holds N client-company graphs (the multi-company accountant),
# so an org_id-keyed cache would serve one client's data for another — a
# cross-tenant bleed *inside* an org. Every idempotency / rate-limit / billing /
# auth key in the tree namespaces on `graph_id` or `user_id`; org_id appears
# only in route params, log lines, and error details (verified: 0 cache uses).
#
# Like `tests/migrations/test_extensions_tenant_fanout.py`, this is a line-level
# heuristic, not a proof. It flags an f-string that interpolates `org_id` *in
# key position* — the interpolation is bounded by key separators (`:` `_` `{`
# `}`) rather than prose whitespace — which cleanly separates a key
# (`f"reports:{org_id}"`) from prose (`f"org {org_id} not found"`). A genuinely
# org-LEVEL cache (org metadata / billing, correctly keyed on the org and *not*
# per-graph tenant data) opts out with a trailing `# org-cache: intentional`
# marker — a conscious declaration, mirroring the fan-out guard's opt-out.

_FSTRING = re.compile(r"""f(["'])(?P<body>(?:\\.|(?!\1).)*)\1""")
_ORG_INTERP = re.compile(r"\{[^{}]*org_id[^{}]*\}")
_KEY_SEP_BEFORE = set(":_{}")  # separator / prefix-interp close / start-of-key
_KEY_SEP_AFTER = set(":_\"'")  # separator / end-of-string
_ORG_CACHE_OPT_OUT = "org-cache: intentional"
# Contexts where an org_id-bearing f-string is a route/log/error, never a key.
_NON_CACHE_CONTEXT = (
  "logger",
  "logging.",
  ".log(",
  "raise ",
  "detail=",
  "HTTPException",
  "@router",
  "_make_request",
)


def _line_keys_org_id(line: str) -> bool:
  """True iff `line` builds an f-string that interpolates org_id in key position."""
  if "org_id" not in line or _ORG_CACHE_OPT_OUT in line:
    return False
  if any(tok in line for tok in _NON_CACHE_CONTEXT):
    return False
  for fm in _FSTRING.finditer(line):
    body = fm.group("body")
    if "org_id" not in body:
      continue
    for m in _ORG_INTERP.finditer(body):
      before = (
        body[m.start() - 1] if m.start() > 0 else "{"
      )  # start-of-body == key start
      after = body[m.end()] if m.end() < len(body) else '"'  # end-of-body == end-of-key
      if before in _KEY_SEP_BEFORE and after in _KEY_SEP_AFTER:
        return True
  return False


def test_no_org_keyed_cache_state():
  """Structural guard: no cache / idempotency / rate-limit / billing key may be
  namespaced on org_id. Two graphs under one org would collide → cross-tenant
  data bleed within an org (the multi-company-accountant failure mode). A new
  org-keyed cache fails here in CI, not in a customer's books.
  """
  # Self-test: the heuristic must still DISCRIMINATE. A regex that silently
  # stopped matching would turn this guard into a no-op that passes forever.
  assert _line_keys_org_id('return f"reports:{org_id}:{name}"')
  assert _line_keys_org_id('key = f"{ORG_PREFIX}{org_id}"')
  assert _line_keys_org_id('return f"{org_id}:settings"')
  assert not _line_keys_org_id('detail=f"Organization {org_id} not found"')
  assert not _line_keys_org_id('logger.error(f"org {org_id}: {e!s}")')
  assert not _line_keys_org_id('return f"idem:{graph_id}:{op}:{digest}"')

  pkg_root = Path(ext.__file__).resolve().parents[1]  # …/robosystems
  offenders: list[str] = []
  for path in pkg_root.rglob("*.py"):
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
      if line.lstrip().startswith("#"):
        continue
      if _line_keys_org_id(line):
        rel = path.relative_to(pkg_root.parent).as_posix()
        offenders.append(f"{rel}:{lineno}: {line.strip()}")
  assert not offenders, (
    "A cache/idempotency/rate-limit key is namespaced on org_id — two graphs "
    "under one org would collide (cross-tenant data bleed *inside* an org):\n  "
    + "\n  ".join(offenders)
    + "\nKey tenant data on graph_id (or user_id), never org_id. If this is a "
    "legitimate org-LEVEL cache (org metadata, not per-graph data), mark the "
    "line `# org-cache: intentional`."
  )
