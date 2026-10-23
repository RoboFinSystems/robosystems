"""The group parent a test ledger's rows are anchored to.

Every ledger row belongs to an entity, so a fixture that builds a tenant
schema seeds one with `seed_parent_entity` and stamps the rows it inserts by
hand with `PARENT_ENTITY_ID`. An entity other than the parent posts only to
its own chart's accounts, which `entity_account` makes.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from robosystems.models.extensions import (
  Association,
  Element,
  Entity,
  EntityTaxonomy,
  Structure,
  Taxonomy,
)

PARENT_ENTITY_ID = "ent_test_parent"


def seed_parent_entity(
  session: Session, *, entity_id: str = PARENT_ENTITY_ID, name: str = "Test Co"
) -> str:
  """Insert the group parent and flush. Returns its id."""
  session.add(
    Entity(
      id=entity_id,
      name=name,
      is_parent=True,
      source="native",
      created_by="test",
    )
  )
  session.flush()
  return entity_id


def seed_parent_entity_on(
  conn: Connection, *, entity_id: str = PARENT_ENTITY_ID, name: str = "Test Co"
) -> str:
  """`seed_parent_entity` for a fixture that holds a connection rather than a
  session, in the schema that connection writes to."""
  conn.execute(
    Entity.__table__.insert().values(
      id=entity_id,
      name=name,
      is_parent=True,
      source="native",
      created_by="test",
    )
  )
  return entity_id


def entity_account(session: Session, entity_id: str, name: str) -> str:
  """An account in ``entity_id``'s own chart of accounts, which is made and
  linked to it on first use. Returns the element id."""
  chart_id = (
    session.execute(
      select(EntityTaxonomy.taxonomy_id).where(
        EntityTaxonomy.entity_id == entity_id,
        EntityTaxonomy.basis == "chart_of_accounts",
      )
    )
    .scalars()
    .first()
  )
  if chart_id is None:
    chart = Taxonomy(
      name=f"Chart of {entity_id}",
      taxonomy_type="chart_of_accounts",
      created_by="test",
    )
    session.add(chart)
    session.flush()
    chart_id = chart.id
    session.add(
      EntityTaxonomy(
        entity_id=entity_id,
        taxonomy_id=chart_id,
        basis="chart_of_accounts",
        is_primary=True,
      )
    )
  element = Element(name=name, code=name[:8], taxonomy_id=chart_id, created_by="test")
  session.add(element)
  session.flush()
  return str(element.id)


def map_to_the_books(session: Session, *element_ids: str) -> None:
  """A book mapping for the parent's accounts — a chart, the rs-gaap
  framework, one concept, the mapping between them and an arc per account —
  in the shape the close gate judges an account mapped by. A ledger whose
  accounts have postings and no mapping cannot close: its balances would be
  in no statement. Fixtures that close a month call this on the accounts
  they post to."""
  chart = Taxonomy(name="Chart", taxonomy_type="chart_of_accounts", created_by="test")
  framework = Taxonomy(
    name="rs-gaap",
    taxonomy_type="reporting_standard",
    standard="rs-gaap",
    created_by="test",
  )
  session.add_all([chart, framework])
  session.flush()
  concept = Element(
    qname="rs-gaap:Cash",
    name="Cash",
    taxonomy_id=framework.id,
    element_type="concept",
    is_abstract=False,
    created_by="test",
  )
  mapping = Taxonomy(
    name="mapping",
    taxonomy_type="mapping",
    source_taxonomy_id=chart.id,
    target_taxonomy_id=framework.id,
    created_by="test",
  )
  session.add_all([concept, mapping])
  session.flush()
  structure = Structure(
    name="CoA mapping", block_type="coa_mapping", taxonomy_id=mapping.id
  )
  session.add(structure)
  session.flush()
  for element_id in element_ids:
    session.add(
      Association(
        structure_id=structure.id,
        from_element_id=element_id,
        to_element_id=concept.id,
        association_type="mapping",
      )
    )
  session.flush()
