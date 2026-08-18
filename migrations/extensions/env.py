"""Alembic environment configuration for the extensions OLTP database.

This is separate from the platform alembic/env.py because:
- Different database (extensions vs robosystems)
- Different DeclarativeBase (ExtensionsBase vs Base)
- Schema-per-graph-id tenancy (public schema managed by Alembic,
  tenant schemas created at runtime by schema_provisioner)
"""

import sys
import time
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from robosystems.db.extensions import ExtensionsBase, get_extensions_database_url

# Import all models to register them on ExtensionsBase.metadata
from robosystems.models.extensions import *  # noqa: F403

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
  fileConfig(config.config_file_name)

# Set the database URL from environment variable
database_url = get_extensions_database_url()
if database_url:
  config.set_main_option("sqlalchemy.url", database_url)

# Target metadata for autogenerate
target_metadata = ExtensionsBase.metadata


def run_migrations_offline() -> None:
  """Run migrations in 'offline' mode."""
  url = config.get_main_option("sqlalchemy.url")
  context.configure(
    url=url,
    target_metadata=target_metadata,
    literal_binds=True,
    dialect_opts={"paramstyle": "named"},
  )

  with context.begin_transaction():
    context.run_migrations()


# Extensions migrations fan out over every tenant schema, and they run at
# daemon boot while the previous release's API tasks are still serving those
# tenants. Two things follow:
#
# - `transaction_per_migration`: each migration commits on its own, so a
#   fan-out that fails on tenant N leaves the migrations before it applied
#   (and `alembic_version` advanced) instead of rolling back an hour of DDL,
#   and the retry starts from the failed migration rather than from scratch.
# - `lock_timeout`: a per-tenant `ALTER TABLE` takes an ACCESS EXCLUSIVE lock;
#   behind one long-running reader it would otherwise wait forever, holding
#   the locks it already took on every earlier tenant and stalling their
#   requests. With a bound the migration fails fast on that tenant, releases
#   everything, and is retried after a pause. Statements can raise their own
#   `SET LOCAL lock_timeout` when they know better.
MIGRATION_LOCK_TIMEOUT = "30s"
MIGRATION_LOCK_RETRIES = 5
MIGRATION_LOCK_RETRY_PAUSE_SECONDS = 10.0


def _is_lock_timeout(exc: BaseException) -> bool:
  """psycopg2 raises `lock_not_available` (55P03) for a lock_timeout."""
  orig = getattr(exc, "orig", None)
  return getattr(orig, "pgcode", None) == "55P03"


def run_migrations_online() -> None:
  """Run migrations in 'online' mode."""
  connectable = engine_from_config(
    config.get_section(config.config_ini_section, {}),
    prefix="sqlalchemy.",
    poolclass=pool.NullPool,
  )

  attempt = 0
  while True:
    attempt += 1
    try:
      with connectable.connect() as connection:
        connection.execute(text(f"SET lock_timeout = '{MIGRATION_LOCK_TIMEOUT}'"))
        connection.commit()
        context.configure(
          connection=connection,
          target_metadata=target_metadata,
          transaction_per_migration=True,
        )
        with context.begin_transaction():
          context.run_migrations()
      return
    except Exception as exc:
      if not _is_lock_timeout(exc) or attempt >= MIGRATION_LOCK_RETRIES:
        raise
      print(
        f"extensions migration hit lock_timeout ({MIGRATION_LOCK_TIMEOUT}) on "
        f"attempt {attempt}/{MIGRATION_LOCK_RETRIES}; retrying in "
        f"{MIGRATION_LOCK_RETRY_PAUSE_SECONDS:.0f}s",
        file=sys.stderr,
      )
      time.sleep(MIGRATION_LOCK_RETRY_PAUSE_SECONDS)


if context.is_offline_mode():
  run_migrations_offline()
else:
  run_migrations_online()
