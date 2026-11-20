"""The reused tenant schema hands every test the same starting state."""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

from tests import tenant_schema
from tests.ledger_entity import seed_parent_entity_on
from tests.tenant_schema import claim_tenant_schema, release_tenant_schema

pytestmark = pytest.mark.unit


@pytest.fixture()
def engine():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  engine = create_engine(database_url)
  yield engine
  engine.dispose()


def _name() -> str:
  return f"ext_pooltest_{uuid.uuid4().hex[:12]}"


def _parked(engine) -> list[str]:
  with engine.connect() as conn:
    key = (tenant_schema._database(conn), "bare")
  return [parked.name for parked in tenant_schema._idle.get(key, [])]


def test_a_released_schema_comes_back_empty_under_the_next_name(engine):
  first, second = _name(), _name()
  with engine.begin() as conn:
    claim_tenant_schema(conn, first)
    conn.execute(text(f'SET LOCAL search_path TO "{first}"'))
    seed_parent_entity_on(conn, name="Left behind")
  with engine.begin() as conn:
    release_tenant_schema(conn, first)
  parked = _parked(engine)[-1]

  with engine.begin() as conn:
    claim_tenant_schema(conn, second)
    rows = conn.execute(text(f'SELECT count(*) FROM "{second}".entities')).scalar()
    names = set(
      conn.execute(
        text("SELECT nspname FROM pg_namespace WHERE nspname = ANY(:names)"),
        {"names": [first, second, parked]},
      ).scalars()
    )
    release_tenant_schema(conn, second)

  assert rows == 0
  assert names == {second}  # the same schema, renamed: nothing was rebuilt


def test_a_schema_a_test_reshaped_is_dropped_not_reused(engine):
  schema = _name()
  with engine.begin() as conn:
    claim_tenant_schema(conn, schema)
    conn.execute(text(f'ALTER TABLE "{schema}".entities ADD COLUMN stray text'))
  before = _parked(engine)
  with engine.begin() as conn:
    release_tenant_schema(conn, schema)
    exists = conn.execute(
      text("SELECT 1 FROM pg_namespace WHERE nspname = :name"), {"name": schema}
    ).scalar()

  assert exists is None
  assert _parked(engine) == before


def test_a_schema_the_test_dropped_itself_is_forgotten(engine):
  schema = _name()
  with engine.begin() as conn:
    claim_tenant_schema(conn, schema)
    conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
  before = _parked(engine)
  with engine.begin() as conn:
    release_tenant_schema(conn, schema)

  assert _parked(engine) == before
