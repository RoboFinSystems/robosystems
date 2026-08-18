"""Multi-graph-per-org isolation: the `extensions_session` search_path contract.

The extensions DB is multi-tenant by **schema-per-graph**: a tenant's data lives
in its own PostgreSQL schema, reached through ``search_path``. The isolation
story has two halves:

1. **Between tasks** — the worker disposes both connection pools after every
   task (even on failure), so a pooled connection can't carry stale state from
   one task into the next. Covered by `tests/worker/test_cleanup.py` +
   `tests/worker/test_consumer.py`.
2. **Per session** — the load-bearing primitive these tests pin. ``search_path``
   is *connection* state and a ``Session`` does not keep its connection:
   ``commit()`` returns it to the pool and the next statement checks out
   whichever connection the pool hands back — with whatever ``search_path`` the
   previous borrower left on it. So the binding is applied on **every**
   transaction the session begins (``after_begin``), and it is ``SET LOCAL`` so
   nothing is ever left on a pooled connection. A command that commits mid-flow
   and keeps going (close does, around its QuickBooks publish) stays on its own
   tenant; a raw connection returned to the pool by someone else cannot bind a
   session to the wrong schema.

These run against the real extensions database because the failure mode is a
connection hop, which a MagicMock session structurally cannot produce. The
mock-based tests below pin the plumbing (validation, rollback/close) only.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

import robosystems.db.extensions as ext
from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session

# Two distinct, well-formed tenant graph_ids (kg + 16+ hex) standing in for two
# graphs under the same org — the multi-graph-per-org case. The bind is
# fail-closed (a missing schema raises rather than falling through to
# ``public``), so the ``tenants`` fixture creates these two — empty is enough:
# these tests only ever ask the connection who it is.
GRAPH_A = "kg00000000000000aa"
GRAPH_B = "kg00000000000000bb"
# Never created: the graph a session must refuse to bind to.
GRAPH_UNPROVISIONED = "kg00000000000000de"

_WHOAMI = text("select pg_backend_pid(), current_setting('search_path')")


@pytest.fixture()
def engine():
  """The real extensions engine, or a skip when the database is unreachable.

  ``EXTENSIONS_DATABASE_URL`` always resolves to something, so connect before
  deciding. Disposed on the way in and out so no test inherits pool state.
  """
  try:
    eng = ext._get_engine()
    with eng.connect() as probe:
      probe.execute(text("SELECT 1"))
  except (OperationalError, RuntimeError) as exc:
    pytest.skip(f"extensions database unreachable: {exc}")
  eng.dispose()
  yield eng
  eng.dispose()


@pytest.fixture()
def tenants(engine):
  """Provision (empty) tenant schemas for GRAPH_A / GRAPH_B; drop after."""
  with engine.begin() as conn:
    for schema in (GRAPH_A, GRAPH_B):
      conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{GRAPH_UNPROVISIONED}" CASCADE'))
  yield
  with engine.begin() as conn:
    for schema in (GRAPH_A, GRAPH_B, GRAPH_UNPROVISIONED):
      conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _poison_pool(engine, *stamps: str) -> None:
  """Leave idle pooled connections bound to *other* schemas — the state a
  raw connection, or a pre-fix session, would leave behind."""
  conns = [engine.connect() for _ in stamps]
  for conn, stamp in zip(conns, stamps, strict=True):
    conn.execute(text(f"SET search_path TO {stamp}, public"))
    conn.commit()
  for conn in conns:
    conn.close()


def _idle_search_paths(engine) -> list[str]:
  conns = [engine.connect() for _ in range(engine.pool.checkedin())]
  paths = [conn.execute(_WHOAMI).one()[1] for conn in conns]
  for conn in conns:
    conn.close()
  return paths


def test_binding_survives_a_mid_flow_commit(engine, tenants):
  """The one that matters: commit inside the session, keep going, still on
  the same tenant — even when the pool hands back a connection another
  tenant last used."""
  _poison_pool(engine, GRAPH_B, "kg00000000000000cc", "kg00000000000000dd")
  with extensions_session(GRAPH_A) as session:
    before = session.execute(_WHOAMI).one()
    session.commit()
    after = session.execute(_WHOAMI).one()
  assert before[1] == f"{GRAPH_A}, public"
  assert after[1] == f"{GRAPH_A}, public", after


def test_binding_survives_a_rollback(engine, tenants):
  _poison_pool(engine, GRAPH_B, "kg00000000000000cc")
  with extensions_session(GRAPH_A) as session:
    session.execute(_WHOAMI)
    session.rollback()
    assert session.execute(_WHOAMI).one()[1] == f"{GRAPH_A}, public"


def test_binding_holds_inside_a_savepoint(engine, tenants):
  """Handlers dispatch inside ``begin_nested()``; the binding is the outer
  transaction's and a savepoint neither loses nor re-stamps it."""
  with extensions_session(GRAPH_A) as session:
    with session.begin_nested():
      assert session.execute(_WHOAMI).one()[1] == f"{GRAPH_A}, public"
    assert session.execute(_WHOAMI).one()[1] == f"{GRAPH_A}, public"


def test_nothing_is_left_on_the_pooled_connection(engine, tenants):
  """``SET LOCAL`` ends with the transaction: after the session, no idle
  connection carries the tenant — commit or rollback."""
  with extensions_session(GRAPH_A) as session:
    session.execute(_WHOAMI)
    session.commit()
  with pytest.raises(RuntimeError, match="boom"):
    with extensions_session(GRAPH_B) as session:
      session.execute(_WHOAMI)
      raise RuntimeError("boom")
  leaked = [p for p in _idle_search_paths(engine) if GRAPH_A in p or GRAPH_B in p]
  assert not leaked, leaked


def test_distinct_graphs_get_distinct_search_paths(engine, tenants):
  """Two graphs under one org never share a scope, in either order."""
  with extensions_session(GRAPH_A) as session:
    assert session.execute(_WHOAMI).one()[1] == f"{GRAPH_A}, public"
  with extensions_session(GRAPH_B) as session:
    assert session.execute(_WHOAMI).one()[1] == f"{GRAPH_B}, public"


def test_refuses_to_bind_a_schema_that_does_not_exist(engine, tenants):
  """The other silent landing. PostgreSQL skips a missing ``search_path``
  entry, and ``public`` holds a copy of every tenant table, so a session on
  a never-provisioned (or already-dropped) graph would run every unqualified
  statement against the shared template — no error. The bind must refuse,
  on the first statement, with the same code a genuinely missing schema
  produces, so the surfaces' "not initialized" translation applies."""
  from sqlalchemy.exc import ProgrammingError

  from robosystems.middleware.extensions import is_schema_missing

  with pytest.raises(ProgrammingError) as excinfo:
    with extensions_session(GRAPH_UNPROVISIONED) as session:
      session.execute(text("select current_schema()"))
  assert getattr(excinfo.value.orig, "pgcode", None) == "3F000"
  assert is_schema_missing(excinfo.value)
  assert GRAPH_UNPROVISIONED in str(excinfo.value)


def test_refuses_the_next_transaction_after_the_schema_is_dropped(engine, tenants):
  """Teardown drops the schema while a member's session may still be open:
  the transaction that began before the drop is the tenant's; the next one
  must not land on ``public``."""
  from sqlalchemy.exc import ProgrammingError

  with pytest.raises(ProgrammingError):
    with extensions_session(GRAPH_B) as session:
      assert session.execute(_WHOAMI).one()[1] == f"{GRAPH_B}, public"
      session.commit()
      with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA "{GRAPH_B}" CASCADE'))
      session.execute(_WHOAMI)


def test_library_sentinel_binds_public_only(engine):
  """The `library` sentinel reads the canonical library (`public`), with no
  tenant-schema binding — so a library read can't be scoped to a tenant."""
  _poison_pool(engine, GRAPH_A)
  with extensions_session(LIBRARY_GRAPH_ID) as session:
    assert session.execute(_WHOAMI).one()[1] == "public"


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
  """A graph_id that isn't a validated tenant schema is rejected before a
  session even exists — defense-in-depth against schema-name injection."""
  factory = MagicMock(name="factory")
  with patch.object(ext, "_get_session_factory", return_value=factory):
    with pytest.raises(ValueError):
      with extensions_session(bad_graph_id):
        pass
  factory.assert_not_called()


def test_rolls_back_and_closes_on_error():
  """A failed tenant op rolls back and closes the session — so a half-applied
  session is never handed to the next graph."""
  session = MagicMock(name="session")
  factory = MagicMock(return_value=session)
  with (
    patch.object(ext, "_get_session_factory", return_value=factory),
    patch.object(ext, "bind_search_path"),
  ):
    with pytest.raises(RuntimeError, match="boom"):
      with extensions_session(GRAPH_A):
        raise RuntimeError("boom")
  session.rollback.assert_called_once()
  session.commit.assert_not_called()
  session.close.assert_called_once()


def test_commits_and_closes_on_success():
  session = MagicMock(name="session")
  factory = MagicMock(return_value=session)
  with (
    patch.object(ext, "_get_session_factory", return_value=factory),
    patch.object(ext, "bind_search_path") as bind,
  ):
    with extensions_session(GRAPH_A) as yielded:
      assert yielded is session
  bind.assert_called_once_with(session, f"{GRAPH_A}, public", tenant_schema=GRAPH_A)
  session.commit.assert_called_once()
  session.close.assert_called_once()


def test_bind_statement_guards_tenants_and_not_the_library():
  """The guard is part of the bind — same round trip, no second query a
  caller could forget — and the library sentinel binds ``public`` unguarded."""
  tenant = ext._bind_statement(f"{GRAPH_A}, public", GRAPH_A)
  assert tenant.startswith("DO $$")
  assert f"to_regnamespace('{GRAPH_A}') IS NULL" in tenant
  assert "ERRCODE = '3F000'" in tenant
  assert tenant.endswith(f"SET LOCAL search_path TO {GRAPH_A}, public")
  assert ext._bind_statement("public", None) == "SET LOCAL search_path TO public"


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
