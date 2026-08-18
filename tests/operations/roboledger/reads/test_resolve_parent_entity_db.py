"""``resolve_parent_entity`` picks the ledger's own entity by predicate — real
Postgres, because the failure is heap order.

A tenant that has received a shared report holds the sender's entity as a
``source='linked'``, ``is_parent=False`` row. ``entities`` has no ordering
guarantee, so ``SELECT ... LIMIT 1`` returns whatever heap order yields — after
the parent row is updated (new tuple) that is the counterparty. The QuickBooks
CompanyInfo overwrite, the CoA auto-link and ``link-entity-taxonomy`` all used
to pick "the entity" that way.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Entity
from robosystems.operations.roboledger.reads.entity import resolve_parent_entity

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_entity_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
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


def _entity(session, *, name, is_parent, source, created_at):
  row = Entity(
    name=name,
    is_parent=is_parent,
    source=source,
    created_by="usr_1",
    created_at=created_at,
  )
  session.add(row)
  session.flush()
  return row


def test_returns_the_parent_even_when_the_linked_row_comes_first(ext_session):
  t0 = datetime.now(UTC)
  parent = _entity(
    ext_session, name="Acme", is_parent=True, source="quickbooks", created_at=t0
  )
  linked = _entity(
    ext_session,
    name="Sender Co",
    is_parent=False,
    source="linked",
    created_at=t0 + timedelta(seconds=1),
  )
  # Rewrite the parent so its live tuple sits after the linked row in the heap
  # — the state an unordered LIMIT 1 mis-picks in.
  parent.name = "Acme Holdings"
  ext_session.commit()

  first_by_heap = ext_session.execute(select(Entity).limit(1)).scalar_one()
  assert first_by_heap.id == linked.id, "precondition: heap order is the trap"

  assert resolve_parent_entity(ext_session).id == parent.id


def test_none_when_only_linked_entities_exist(ext_session):
  _entity(
    ext_session,
    name="Sender Co",
    is_parent=False,
    source="linked",
    created_at=datetime.now(UTC),
  )
  ext_session.commit()
  assert resolve_parent_entity(ext_session) is None
