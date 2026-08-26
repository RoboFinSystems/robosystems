"""Per-worker database isolation for ``pytest -n`` (pytest-xdist).

Invoked from ``tests/__init__.py``, which is the earliest point that is
both after pytest-env has applied the ``[pytest] env`` block and before
anything imports ``robosystems``: ``robosystems.config.env`` reads
``DATABASE_URL`` at import, and ``robosystems.db.platform`` builds the
platform engine from it at import, so the rewrite has to land first.
Outside xdist (no ``PYTEST_XDIST_WORKER``) nothing here runs.

What is isolated, and what deliberately is not:

- **Platform test DB — per worker.** The session-scoped ``test_db`` fixture
  drops and recreates ``public``, and the autouse ``setup_database`` truncates
  ~25 tables after every test. Two workers on one database race on that
  truncation and read each other's rows. Each worker gets its own copy
  (``robosystems_test`` → ``robosystems_test_gw0``), created here if missing.
  ``TEST_DATABASE_URL`` (the fixtures) and ``DATABASE_URL`` (the app engine)
  both move, so the app under test and the fixtures that inspect it agree.
  The throwaway ``ext_*`` extensions schemas some tests build live in this
  database too, so they follow it.

- **Extensions DB — shared, with ``--dist loadfile``.** Its tests either use
  those uuid-named throwaway schemas (never collide) or a module-scoped tenant
  schema with a per-file name; ``loadfile`` keeps a file's tests on one worker,
  so a file never runs concurrently against itself. The taxonomy library it
  holds is read-only to tests, and cloning it per worker (``CREATE DATABASE …
  TEMPLATE extensions``) needs the template free of connections, which the
  local dev stack's API never leaves it.

- **Valkey and LocalStack — shared.** Tests that touch them live key by their
  own ids. The double ``-n auto`` run in the spec's verification is what
  proves that holds as the suite grows; a collision found there is fixed in
  the test, not by widening this module.
"""

from __future__ import annotations

import os
import warnings
from urllib.parse import urlsplit, urlunsplit

WORKER_ENV = "PYTEST_XDIST_WORKER"

# The platform test database under both names the suite uses for it.
PER_WORKER_URL_KEYS = ("TEST_DATABASE_URL", "DATABASE_URL")


def per_worker_url(url: str, worker: str) -> str:
  """``…/robosystems_test`` → ``…/robosystems_test_gw0``, everything else kept."""
  parts = urlsplit(url)
  name = parts.path.lstrip("/")
  return urlunsplit(parts._replace(path=f"/{name}_{worker}"))


def database_name(url: str) -> str:
  return urlsplit(url).path.lstrip("/")


def ensure_database(url: str) -> None:
  """Create the database ``url`` names if it does not exist yet.

  Connects to the server's ``postgres`` maintenance database with the same
  credentials; ``CREATE DATABASE`` cannot run inside a transaction, hence
  autocommit. Workers create distinct names, so two of them never race on
  the same one.
  """
  from sqlalchemy import create_engine, text
  from sqlalchemy.pool import NullPool

  name = database_name(url)
  maintenance = urlunsplit(urlsplit(url)._replace(path="/postgres"))
  engine = create_engine(maintenance, isolation_level="AUTOCOMMIT", poolclass=NullPool)
  try:
    with engine.connect() as conn:
      exists = conn.execute(
        text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": name}
      ).scalar()
      if not exists:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
  finally:
    engine.dispose()


def isolate_worker_databases(environ: dict[str, str] | None = None) -> str | None:
  """Point this xdist worker at its own platform test database.

  Returns the worker id when a rewrite happened, ``None`` otherwise. A
  Postgres that cannot be reached is a warning, not a failure: the URLs are
  still rewritten, and the tests that need the database fail on their own
  terms exactly as they would serially.
  """
  env = os.environ if environ is None else environ
  worker = env.get(WORKER_ENV)
  if not worker:
    return None

  targets = {
    key: per_worker_url(env[key], worker) for key in PER_WORKER_URL_KEYS if env.get(key)
  }
  for url in {*targets.values()}:
    try:
      ensure_database(url)
    except Exception as exc:  # pragma: no cover - needs Postgres down
      warnings.warn(
        f"xdist worker {worker}: could not create {database_name(url)!r} "
        f"({exc}); database-backed tests will fail on this worker",
        stacklevel=2,
      )
  env.update(targets)
  return worker
