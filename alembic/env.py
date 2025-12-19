"""Alembic environment configuration."""

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# Import your models here
from robosystems.database import Model, get_database_url
from robosystems.models.billing.audit_log import BillingAuditLog  # noqa: F401

# Import all billing models
from robosystems.models.billing.customer import BillingCustomer  # noqa: F401
from robosystems.models.billing.invoice import (  # noqa: F401
  BillingInvoice,
  BillingInvoiceLineItem,
)
from robosystems.models.billing.subscription import BillingSubscription  # noqa: F401
from robosystems.models.iam.connection_credentials import (
  ConnectionCredentials,  # noqa: F401
)
from robosystems.models.iam.graph import Graph  # noqa: F401
from robosystems.models.iam.graph_backup import GraphBackup  # noqa: F401
from robosystems.models.iam.graph_credits import (  # noqa: F401
  GraphCredits,
  GraphCreditTransaction,
)
from robosystems.models.iam.graph_usage import GraphUsage  # noqa: F401
from robosystems.models.iam.graph_user import GraphUser  # noqa: F401
from robosystems.models.iam.org import Org  # noqa: F401
from robosystems.models.iam.org_limits import OrgLimits  # noqa: F401
from robosystems.models.iam.org_user import OrgUser  # noqa: F401

# Import all IAM models directly without going through models.__init__.py to avoid circular imports
from robosystems.models.iam.user import User  # noqa: F401
from robosystems.models.iam.user_api_key import UserAPIKey  # noqa: F401
from robosystems.models.iam.user_repository import (
  UserRepository,  # noqa: F401
)
from robosystems.models.iam.user_repository_credits import (  # noqa: F401
  UserRepositoryCredits,
  UserRepositoryCreditTransaction,
)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
  fileConfig(config.config_file_name)

# Set the database URL from environment variable with SSL configuration
database_url = get_database_url()
if database_url:
  config.set_main_option("sqlalchemy.url", database_url)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Model.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
  """Run migrations in 'offline' mode.

  This configures the context with just a URL
  and not an Engine, though an Engine is acceptable
  here as well.  By skipping the Engine creation
  we don't even need a DBAPI to be available.

  Calls to context.execute() here emit the given string to the
  script output.

  """
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
  """Run migrations in 'online' mode.

  In this scenario we need to create an Engine
  and associate a connection with the context.

  """
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
