"""A mapping target must be a leaf the statement renders, never a subtotal.

A direct fact on a rolled-up concept overrides the sum of its children, so a
mapping to one breaks articulation. Runs against the real extensions database
with a throwaway tenant schema.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingTargetIsRollupError,
  create_mapping_association,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd07"
STYLE = "style_sr3"


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


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
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=_tenant_tables(),
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def ledger(tenant):
  """A CoA account, a subtotal and its leaf, and a mapping structure."""
  from robosystems.models.extensions import Element, Structure, Taxonomy

  with extensions_session(GRAPH) as session:
    for table in (
      "associations",
      "reporting_style_networks",
      "entities",
      "structures",
      "elements",
      "taxonomies",
    ):
      session.execute(text(f"DELETE FROM {table}"))
    session.add(
      Taxonomy(id="tax_sr3", name="Mapping", taxonomy_type="mapping", created_by="u")
    )
    session.flush()
    session.add_all(
      [
        Structure(
          id="map_sr3",
          name="CoA mapping",
          block_type="coa_mapping",
          taxonomy_id="tax_sr3",
          created_by="u",
        ),
        Structure(
          id="net_is",
          name="Income statement",
          block_type="income_statement",
          taxonomy_id="tax_sr3",
          created_by="library-seeder",
        ),
        Element(id="el_office", name="Office expense", code="6100", created_by="u"),
        Element(
          id="el_opex",
          name="Operating expenses",
          qname="rs-gaap:OperatingExpenses",
          created_by="library-seeder",
        ),
        Element(
          id="el_other_opex",
          name="Other operating expense",
          qname="rs-gaap:OtherCostAndExpenseOperating",
          created_by="library-seeder",
        ),
      ]
    )
    session.commit()


def _map(to_element_id: str):
  with extensions_session(GRAPH) as session:
    result = create_mapping_association(
      session,
      CreateMappingAssociationOperation(
        mapping_id="map_sr3",
        from_element_id="el_office",
        to_element_id=to_element_id,
        association_type="mapping",
      ),
      created_by="u",
    )
    session.commit()
    return result


def _seed_style() -> None:
  """An entity on a Style whose income statement sums OpEx from its leaf."""
  from robosystems.models.extensions import Association
  from robosystems.models.extensions.entity import Entity
  from robosystems.models.extensions.reporting_style_network import (
    ReportingStyleNetwork,
  )

  with extensions_session(GRAPH) as session:
    session.add(
      Entity(id="ent_sr3", name="Driftline", reporting_style_id=STYLE, created_by="u")
    )
    session.add(
      ReportingStyleNetwork(
        reporting_style_id=STYLE, statement_type="income_statement", network_id="net_is"
      )
    )
    session.add(
      Association(
        id="assoc_pres",
        structure_id="net_is",
        from_element_id="el_opex",
        to_element_id="el_other_opex",
        association_type="presentation",
        created_by="library-seeder",
      )
    )
    session.commit()


def test_a_subtotal_on_the_style_is_refused():
  _seed_style()
  with pytest.raises(MappingTargetIsRollupError):
    _map("el_opex")


def test_the_leaf_under_it_is_accepted():
  _seed_style()
  assert _map("el_other_opex").to_element_id == "el_other_opex"


def test_without_a_style_the_static_denylist_decides():
  with pytest.raises(MappingTargetIsRollupError):
    _map("el_opex")
  assert _map("el_other_opex").to_element_id == "el_other_opex"
