"""Whether a QuickBooks load is a baseline, against a real tenant schema.

Books that hold no posted entry take the source's history as their own; one
posted entry, and the history is a change to books already set.
"""

from __future__ import annotations

from datetime import date

import pytest
from dagster import build_asset_context
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.adapters.quickbooks.pipeline.load import _is_baseline_import
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions.roboledger.entry import Entry
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgbbbbbbbbbbbbbbbb01"


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
      seed_parent_entity_on(conn.execution_options(schema_translate_map={None: GRAPH}))
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _entry(status: str) -> Entry:
  return Entry(
    entity_id=PARENT_ENTITY_ID,
    posting_date=date(2026, 3, 15),
    status=status,
    memo=status,
    created_by="usr_test",
  )


def test_empty_books_are_a_baseline(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
  assert _is_baseline_import(build_asset_context(), GRAPH) is True


def test_a_draft_does_not_set_the_books(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
    session.add(_entry("draft"))
  assert _is_baseline_import(build_asset_context(), GRAPH) is True


def test_one_posted_entry_ends_the_baseline(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
    session.add(_entry("posted"))
  assert _is_baseline_import(build_asset_context(), GRAPH) is False
