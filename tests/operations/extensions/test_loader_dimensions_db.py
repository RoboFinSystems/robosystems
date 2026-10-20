"""The QuickBooks dimension catalog across syncs, against a real database.

The catalog is unique on ``(dimension_type, value)``, so a loader that inserts
it fresh each sync fails every sync after the first. It is upserted instead,
and a member keeps its id, which the line junctions reference.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Dimension
from robosystems.operations.extensions.loader import _upsert_dimensions

pytestmark = pytest.mark.unit


@pytest.fixture()
def db():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_dims_{uuid.uuid4().hex[:12]}"
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))
  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _catalog(*rows: tuple[str, str, str]) -> list[dict]:
  return [
    {"dimension_type": t, "name": name, "value": value} for t, name, value in rows
  ]


def _held(session) -> dict[tuple[str, str], Dimension]:
  return {(d.dimension_type, d.value): d for d in session.query(Dimension).all()}


def test_second_sync_lands_on_the_same_rows(db):
  catalog = _catalog(("class", "Retail", "1"), ("department", "Sales", "7"))
  now = datetime.now(UTC)

  assert _upsert_dimensions(db, catalog, source="quickbooks", now=now) == 2
  first = {key: d.id for key, d in _held(db).items()}

  assert _upsert_dimensions(db, catalog, source="quickbooks", now=now) == 2
  second = {key: d.id for key, d in _held(db).items()}

  assert second == first


def test_a_rename_updates_the_member_in_place(db):
  now = datetime.now(UTC)
  _upsert_dimensions(
    db, _catalog(("class", "Retail", "1")), source="quickbooks", now=now
  )
  original = _held(db)[("class", "1")].id

  _upsert_dimensions(
    db, _catalog(("class", "Retail stores", "1")), source="quickbooks", now=now
  )
  db.expire_all()
  member = _held(db)[("class", "1")]

  assert member.id == original
  assert member.name == "Retail stores"


def test_a_retired_member_stays_retired(db):
  now = datetime.now(UTC)
  _upsert_dimensions(
    db, _catalog(("location", "Pier", "3")), source="quickbooks", now=now
  )
  db.query(Dimension).update({Dimension.is_active: False})
  db.flush()

  _upsert_dimensions(
    db, _catalog(("location", "Pier", "3")), source="quickbooks", now=now
  )
  db.expire_all()

  assert _held(db)[("location", "3")].is_active is False


def test_a_key_named_twice_in_one_batch_is_written_once(db):
  now = datetime.now(UTC)
  catalog = _catalog(("class", "Retail", "1"), ("class", "Retail (renamed)", "1"))

  assert _upsert_dimensions(db, catalog, source="quickbooks", now=now) == 1
  assert _held(db)[("class", "1")].name == "Retail (renamed)"


def test_an_empty_catalog_writes_nothing(db):
  assert _upsert_dimensions(db, [], source="quickbooks", now=datetime.now(UTC)) == 0
  assert _held(db) == {}
