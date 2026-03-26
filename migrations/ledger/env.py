"""Alembic environment configuration for the roboledger OLTP database.

This is separate from the platform alembic/env.py because:
- Different database (roboledger vs robosystems)
- Different DeclarativeBase (LedgerBase vs Base)
- Schema-per-graph-id tenancy (public schema managed by Alembic,
  tenant schemas created at runtime by schema_provisioner)
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from robosystems.db.ledger import LedgerBase, get_ledger_database_url

# Import all models to register them on LedgerBase.metadata
from robosystems.models.ledger import *  # noqa: F403

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
  fileConfig(config.config_file_name)

# Set the database URL from environment variable
database_url = get_ledger_database_url()
if database_url:
  config.set_main_option("sqlalchemy.url", database_url)

# Target metadata for autogenerate
target_metadata = LedgerBase.metadata


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


def run_migrations_online() -> None:
  """Run migrations in 'online' mode."""
  connectable = engine_from_config(
    config.get_section(config.config_ini_section, {}),
    prefix="sqlalchemy.",
    poolclass=pool.NullPool,
  )

  with connectable.connect() as connection:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
      context.run_migrations()


if context.is_offline_mode():
  run_migrations_offline()
else:
  run_migrations_online()
