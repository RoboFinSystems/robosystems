"""A chart authored with `create-taxonomy-block` keeps the books of the entity
it names, and never displaces that entity's primary chart: on a group it used
to land on the parent and demote the parent's own."""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  TaxonomyBlockElementRequest,
)
from robosystems.models.extensions import Entity, EntityTaxonomy
from robosystems.operations.taxonomy_block import chart_of_accounts
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

SUB = "ent_test_sub"


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_chart_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
  db.add(
    Entity(id=SUB, name="Sub Co", is_parent=False, source="native", created_by="t")
  )
  db.flush()
  try:
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _chart(session, name: str, entity_id: str | None = None) -> str:
  return chart_of_accounts.create(
    session,
    CreateTaxonomyBlockRequest(
      name=name,
      taxonomy_type="chart_of_accounts",
      entity_id=entity_id,
      elements=[
        TaxonomyBlockElementRequest(
          qname=f"coa:Cash{uuid.uuid4().hex[:6]}",
          code="1000",
          name="Cash",
          trait="asset",
        )
      ],
    ),
    "usr_1",
  )


def _links(session, entity_id: str) -> dict[str, bool]:
  return dict(
    session.execute(
      select(EntityTaxonomy.taxonomy_id, EntityTaxonomy.is_primary).where(
        EntityTaxonomy.entity_id == entity_id,
        EntityTaxonomy.basis == "chart_of_accounts",
      )
    ).all()
  )


def test_a_second_chart_never_displaces_the_primary(session):
  books = _chart(session, "Parent books")
  authored = _chart(session, "Authored over MCP")

  assert _links(session, PARENT_ENTITY_ID) == {books: True, authored: False}


def test_a_chart_named_for_a_subsidiary_keeps_its_books(session):
  parent_books = _chart(session, "Parent books")
  sub_books = _chart(session, "Sub books", SUB)

  assert _links(session, SUB) == {sub_books: True}
  assert _links(session, PARENT_ENTITY_ID) == {parent_books: True}


def test_entity_id_is_read_only_for_a_chart():
  with pytest.raises(ValueError, match="entity_id"):
    CreateTaxonomyBlockRequest(
      name="Notes", taxonomy_type="custom_ontology", entity_id=SUB
    )
