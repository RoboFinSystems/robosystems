"""Migration 0063 against a tenant schema: two own entities on one ticker,
case apart, stop the upgrade and are named; with each on its own the index
lands and then refuses the duplicate, while a ``linked`` row is not held to
it. Runs the migration's per-schema function on real Postgres."""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase

pytestmark = pytest.mark.unit

_VERSIONS = (
  Path(__file__).resolve().parents[2] / "migrations" / "extensions" / "versions"
)


def _load(name: str):
  spec = importlib.util.spec_from_file_location(name, _VERSIONS / f"{name}.py")
  assert spec is not None and spec.loader is not None
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


migration = _load("0063_unique_ticker_among_own_entities")


@pytest.fixture()
def tenant():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_m63_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    conn.execute(text(f'SET search_path TO "{schema}"'))
    ExtensionsBase.metadata.create_all(bind=conn)
    # The model carries the index; the migration is what brings it to a
    # tenant provisioned before it, so start that tenant without it.
    conn.execute(text(f'DROP INDEX IF EXISTS "{schema}".{migration._INDEX}'))
  try:
    yield engine, schema
  finally:
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _insert(conn, schema: str, ticker: str, source: str = "native") -> str:
  entity_id = f"ent_{uuid.uuid4().hex[:12]}"
  conn.execute(
    text(
      f'INSERT INTO "{schema}".entities '
      "(id, name, ticker, source, reporting_style_id, status, is_parent, "
      " consolidation_method, entity_role, metadata, version, created_by, "
      " created_at, updated_at) "
      "VALUES (:id, :name, :ticker, :source, 'style', 'active', false, "
      " 'none', 'operating', '{}'::jsonb, 1, 'test', now(), now())"
    ),
    {"id": entity_id, "name": ticker, "ticker": ticker, "source": source},
  )
  return entity_id


def test_a_shared_ticker_stops_the_upgrade_and_is_named(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    _insert(conn, schema, "ACME")
    _insert(conn, schema, "acme")
  with pytest.raises(RuntimeError, match="acme x2"), engine.begin() as conn:
    migration._expand(conn, schema)


def test_the_index_lands_and_holds_own_entities_only(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    _insert(conn, schema, "ACME")
    _insert(conn, schema, "acme", source="linked")
    migration._expand(conn, schema)
  with pytest.raises(IntegrityError, match=migration._INDEX), engine.begin() as conn:
    _insert(conn, schema, "Acme")
  with engine.begin() as conn:
    _insert(conn, schema, "ACME", source="linked")
    migration._contract(conn, schema)
    _insert(conn, schema, "Acme")
