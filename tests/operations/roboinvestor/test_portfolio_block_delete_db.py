"""Deleting a portfolio deletes its positions first, in the database.

Runs against the real extensions database with a throwaway tenant schema.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.investor import DeletePortfolioBlockOperation
from robosystems.models.extensions.roboinvestor import Portfolio, Position, Security
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  delete_portfolio_block,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd09"


@pytest.fixture(scope="module")
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def test_a_portfolio_with_positions_deletes(tenant):
  with extensions_session(GRAPH) as session:
    session.add(Portfolio(id="port_1", name="Fund I", created_by="u"))
    session.add(
      Security(
        id="sec_1",
        name="Series A Preferred",
        security_type="preferred_stock",
        created_by="u",
      )
    )
    session.flush()
    session.add(
      Position(
        id="pos_1",
        portfolio_id="port_1",
        security_id="sec_1",
        quantity=1000,
        created_by="u",
      )
    )
    session.commit()

  with extensions_session(GRAPH) as session:
    result = delete_portfolio_block(
      session,
      DeletePortfolioBlockOperation(
        portfolio_id="port_1", confirm_active_positions=True
      ),
    )
    session.commit()

  assert result.deleted and result.positions_deleted == 1
  with extensions_session(GRAPH) as session:
    assert session.get(Portfolio, "port_1") is None
    assert session.get(Position, "pos_1") is None
