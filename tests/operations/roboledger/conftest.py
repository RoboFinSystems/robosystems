"""Real-Postgres fixtures for the multi-entity close path: one tenant schema
with a group parent, a native subsidiary, and a linked counterparty."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Entity


@pytest.fixture()
def tenant_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_me_{uuid.uuid4().hex[:12]}"
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


@dataclass
class TwoEntityTenant:
  session: Session
  parent: Entity
  sub: Entity
  linked: Entity


def _entity(session, *, name, is_parent, source, created_at, **kw) -> Entity:
  row = Entity(
    name=name,
    is_parent=is_parent,
    source=source,
    created_by="usr_1",
    created_at=created_at,
    **kw,
  )
  session.add(row)
  session.flush()
  return row


@pytest.fixture()
def two_entities(tenant_session) -> TwoEntityTenant:
  """A linked row created first (the heap-order trap), then the parent, then a
  native subsidiary under it."""
  t0 = datetime.now(UTC)
  linked = _entity(
    tenant_session,
    name="Sender Co",
    is_parent=False,
    source="linked",
    created_at=t0 - timedelta(seconds=5),
    metadata_={"source_graph_id": "kg_sender"},
  )
  parent = _entity(
    tenant_session,
    name="Harbor Holdings",
    is_parent=True,
    source="native",
    created_at=t0,
  )
  sub = _entity(
    tenant_session,
    name="Maple Court LLC",
    is_parent=False,
    source="native",
    created_at=t0 + timedelta(seconds=1),
    parent_entity_id=parent.id,
  )
  tenant_session.commit()
  return TwoEntityTenant(tenant_session, parent, sub, linked)
