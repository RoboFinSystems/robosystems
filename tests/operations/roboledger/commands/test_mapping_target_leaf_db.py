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
  MappingTargetNotRenderedError,
  create_mapping_association,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd21"
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
    session.add_all(
      [
        Taxonomy(id="tax_sr3", name="Mapping", taxonomy_type="mapping", created_by="u"),
        Taxonomy(
          id="tax_calc",
          name="rs-gaap calculations",
          taxonomy_type="reporting",
          standard="rs-gaap-calculations",
          created_by="library-seeder",
        ),
      ]
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
        Structure(
          id="calc_is",
          name="Income statement calculation",
          block_type="income_statement",
          taxonomy_id="tax_calc",
          created_by="library-seeder",
        ),
        Element(
          id="el_office",
          name="Office expense",
          code="6100",
          qname="coa:6100",
          taxonomy_id="tax_sr3",
          created_by="u",
        ),
        Element(
          id="el_gross",
          name="Gross profit",
          qname="rs-gaap:GrossProfit",
          created_by="library-seeder",
        ),
        Element(
          id="el_net",
          name="Net income",
          qname="rs-gaap:NetIncomeLoss",
          created_by="library-seeder",
        ),
        Structure(
          id="net_equity",
          name="Changes in partners' capital",
          block_type="equity_statement",
          concept_arrangement="roll_forward",
          taxonomy_id="tax_sr3",
          created_by="library-seeder",
        ),
        Element(
          id="el_capital",
          name="Partners' capital",
          qname="rs-gaap:PartnersCapital",
          created_by="library-seeder",
        ),
        Element(
          id="el_contributions",
          name="Partner contributions",
          qname="rs-gaap:ProceedsFromPartnershipContribution",
          created_by="library-seeder",
        ),
        Element(
          id="el_sga",
          name="Selling, general and administrative",
          qname="rs-gaap:SellingGeneralAndAdministrativeExpense",
          created_by="library-seeder",
        ),
        Element(
          id="el_ppe_gross",
          name="PP&E, gross",
          qname="rs-gaap:PropertyPlantAndEquipmentGross",
          created_by="library-seeder",
        ),
        Element(
          id="el_extension",
          name="Customer deposits held",
          qname="drift:CustomerDepositsHeld",
          created_by="u",
        ),
        Element(
          id="el_costs",
          name="Costs and expenses",
          qname="rs-gaap:CostsAndExpenses",
          created_by="library-seeder",
        ),
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


def _seed_calculation() -> None:
  """GrossProfit is summed by a calculation arc, not by the Style's presentation."""
  from robosystems.models.extensions import Association

  with extensions_session(GRAPH) as session:
    session.add(
      Association(
        id="assoc_calc",
        structure_id="calc_is",
        from_element_id="el_gross",
        to_element_id="el_other_opex",
        association_type="calculation",
        weight=1.0,
        created_by="library-seeder",
      )
    )
    session.commit()


@pytest.mark.parametrize(
  "target",
  [
    pytest.param("el_opex", id="presentation parent"),
    pytest.param("el_gross", id="calculation parent"),
    pytest.param("el_net", id="net income"),
  ],
)
def test_a_subtotal_on_the_style_is_refused(target):
  _seed_style()
  _seed_calculation()
  with pytest.raises(MappingTargetIsRollupError):
    _map(target)


def test_the_leaf_under_it_is_accepted():
  _seed_style()
  assert _map("el_other_opex").to_element_id == "el_other_opex"


def test_without_a_style_the_static_denylist_decides():
  with pytest.raises(MappingTargetIsRollupError):
    _map("el_opex")
  assert _map("el_other_opex").to_element_id == "el_other_opex"


def test_with_no_entity_the_default_style_decides():
  """A denylisted concept that is a leaf on the default Style is mappable."""
  from robosystems.config.constants import ReportingStyleConstants
  from robosystems.models.extensions import Association
  from robosystems.models.extensions.reporting_style_network import (
    ReportingStyleNetwork,
  )

  with extensions_session(GRAPH) as session:
    session.add(
      ReportingStyleNetwork(
        reporting_style_id=ReportingStyleConstants.DEFAULT_STYLE_ID,
        statement_type="income_statement",
        network_id="net_is",
      )
    )
    session.add(
      Association(
        id="assoc_costs",
        structure_id="net_is",
        from_element_id="el_opex",
        to_element_id="el_costs",
        association_type="presentation",
        created_by="library-seeder",
      )
    )
    session.commit()

  assert _map("el_costs").to_element_id == "el_costs"
  with pytest.raises(MappingTargetIsRollupError):
    _map("el_opex")


def test_a_taxonomy_block_update_obeys_the_same_rule():
  from robosystems.models.api.taxonomy_block import (
    TaxonomyBlockAssociationRequest,
    UpdateTaxonomyBlockRequest,
  )
  from robosystems.models.extensions import Taxonomy
  from robosystems.operations.taxonomy_block.update_apply import (
    apply_associations_to_add,
  )

  _seed_style()
  payload = UpdateTaxonomyBlockRequest(
    taxonomy_id="tax_sr3",
    associations_to_add=[
      TaxonomyBlockAssociationRequest(
        structure_ref="CoA mapping",
        from_ref="coa:6100",
        to_ref="rs-gaap:OperatingExpenses",
        association_type="mapping",
      )
    ],
  )
  with extensions_session(GRAPH) as session:
    taxonomy = session.get(Taxonomy, "tax_sr3")
    with pytest.raises(MappingTargetIsRollupError):
      apply_associations_to_add(
        session,
        taxonomy,
        {},
        {},
        payload,
        "u",
        library_ref_lookup=lambda refs: {"rs-gaap:OperatingExpenses": "el_opex"},
      )


def test_a_roll_forward_balance_is_a_leaf():
  """A partnership's capital is the balance its flows move, not their sum."""
  from robosystems.models.extensions import Association
  from robosystems.models.extensions.reporting_style_network import (
    ReportingStyleNetwork,
  )

  _seed_style()
  with extensions_session(GRAPH) as session:
    session.add(
      ReportingStyleNetwork(
        reporting_style_id=STYLE,
        statement_type="equity_statement",
        network_id="net_equity",
      )
    )
    session.add(
      Association(
        id="assoc_rollforward",
        structure_id="net_equity",
        from_element_id="el_capital",
        to_element_id="el_contributions",
        association_type="presentation",
        created_by="library-seeder",
      )
    )
    session.commit()

  assert _map("el_capital").to_element_id == "el_capital"


def test_an_rs_gaap_concept_off_the_statements_is_refused():
  """Off the Style it would roll into the subtotal above it at render."""
  _seed_style()
  with pytest.raises(MappingTargetNotRenderedError):
    _map("el_sga")


@pytest.mark.parametrize(
  "target",
  [
    pytest.param("el_ppe_gross", id="synthesized detail"),
    pytest.param("el_extension", id="extension concept"),
  ],
)
def test_off_the_statements_but_rendered_another_way_is_accepted(target):
  _seed_style()
  assert _map(target).to_element_id == target
