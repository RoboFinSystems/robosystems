"""Guard for the demos' one direct-database step: the reset.

Every demo goes through the HTTP API except its ``_reset`` module, which
issues raw ``DELETE``s against whatever ``EXTENSIONS_DATABASE_URL`` points
at. The demos gate that on the *API* target being local — but the API URL
and the database URL are two different settings, and ``localhost:5432`` is
exactly where an SSM tunnel to production RDS lands. So before any reset
runs, ask the database itself:

1. the URL's host must be a loopback/local name (cheap, catches an edited
   ``.env.local``), and
2. the server must not be RDS — every RDS instance carries the ``rdsadmin``
   role; a local Postgres never does.

Refusal raises rather than returning False: a demo that reaches this point
with a non-local database is misconfigured, and the safe outcome is a stack
trace, not a skipped step that leaves stale demo state to be duplicated.
"""

from __future__ import annotations

from urllib.parse import urlparse

from sqlalchemy import create_engine, text

LOCAL_DB_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0", "postgres"})


class NonLocalDatabaseError(RuntimeError):
  """Raised when a demo reset would run against a database that is not local."""


def assert_local_extensions_db() -> None:
  """Refuse unless the extensions database is demonstrably a local Postgres."""
  from robosystems.db.extensions import get_extensions_database_url

  url = get_extensions_database_url()
  host = (urlparse(url).hostname or "").lower()
  if host not in LOCAL_DB_HOSTS:
    raise NonLocalDatabaseError(
      f"Demo reset refused: EXTENSIONS_DATABASE_URL host is '{host}', not a "
      "local Postgres. The reset issues raw DELETEs and only ever runs locally."
    )

  engine = create_engine(url)
  try:
    with engine.connect() as conn:
      on_rds = conn.execute(
        text("SELECT 1 FROM pg_roles WHERE rolname = 'rdsadmin'")
      ).first()
  finally:
    engine.dispose()
  if on_rds is not None:
    raise NonLocalDatabaseError(
      f"Demo reset refused: '{host}' answers as an Amazon RDS instance (the "
      "rdsadmin role is present) — a forwarded port, not a local database. "
      "Close the tunnel or point EXTENSIONS_DATABASE_URL at the local stack."
    )
