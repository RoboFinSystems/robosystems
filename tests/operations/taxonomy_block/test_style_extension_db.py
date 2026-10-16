"""Style extensions on a real tenant: a reporting extension's rows layered onto
the entity's Reporting Style networks, anchored at authoring and read as one
statement by every reader.

Against the extensions database with a throwaway tenant provisioned from the
library, so the networks, calc DAG and tenant-exclude curation are the real
ones.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  TaxonomyBlockAssociationRequest,
  TaxonomyBlockElementRequest,
  TaxonomyBlockStructureRequest,
)
from robosystems.models.extensions import Element, Entity, Taxonomy
from robosystems.operations.roboledger.reads.taxonomies import (
  suggest_mapping_candidates,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  ReportFact,
  render_structure_view,
)
from robosystems.operations.roboledger.reports.network_picker import (
  get_render_network,
  load_entity_reporting_style,
  taggable_flow_qnames,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  _build_structure_mapping,
)
from robosystems.operations.taxonomy_block import reporting_extension
from robosystems.operations.taxonomy_block.style_extension import StyleExtensionError

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd47"
PARENT = "ent_style_ext_parent"
ROLES = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/"
FY = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 12, 31), label="FY2026")

# Curated out of tenants by tenant-exclude: the pull-in case.
AFS_PURCHASES = "rs-gaap:PaymentsToAcquireAvailableForSaleSecuritiesDebt"


@pytest.fixture()
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
  with engine.begin() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
  try:
    provision_tenant_schema(GRAPH)
    with extensions_session(GRAPH) as session:
      session.add(
        Entity(
          id=PARENT,
          name="Style Extension Corp",
          entity_type="corporation",
          is_parent=True,
          created_by="usr_1",
        )
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _library_id(session) -> str:
  return str(
    session.execute(
      select(Taxonomy.id).where(
        Taxonomy.standard == "rs-gaap", Taxonomy.taxonomy_type == "reporting_standard"
      )
    ).scalar_one()
  )


def _network_role(session, statement_type: str) -> str:
  network = get_render_network(
    session, load_entity_reporting_style(session, PARENT), statement_type
  )
  return str(
    session.execute(
      text("SELECT metadata ->> 'role_uri' FROM structures WHERE id = :id"),
      {"id": network.structure_id},
    ).scalar_one()
  )


def _extension(session, *, elements=(), structures=(), associations=()) -> str:
  return reporting_extension.create(
    session,
    CreateTaxonomyBlockRequest(
      name="Acme Reporting Extension",
      taxonomy_type="reporting_extension",
      parent_taxonomy_id=_library_id(session),
      standard="acme",
      namespace_uri="https://acme.example.com/acme",
      elements=list(elements),
      structures=list(structures),
      associations=list(associations),
    ),
    "usr_1",
  )


def _cash_flow_extension(session) -> str:
  role = _network_role(session, "cash_flow_statement")
  return _extension(
    session,
    structures=[
      TaxonomyBlockStructureRequest(
        name="Acme cash flow rows",
        block_type="cash_flow_statement",
        extends_network=role,
      )
    ],
    associations=[
      TaxonomyBlockAssociationRequest(
        structure_ref="Acme cash flow rows",
        from_ref="rs-gaap:NetCashProvidedByUsedInInvestingActivities",
        to_ref=AFS_PURCHASES,
        association_type="presentation",
        order_value=31950,
      ),
      TaxonomyBlockAssociationRequest(
        structure_ref="Acme cash flow rows",
        from_ref="rs-gaap:NetCashProvidedByUsedInInvestingActivities",
        to_ref=AFS_PURCHASES,
        association_type="calculation",
        weight=1.0,
      ),
    ],
  )


def _income_statement_extension(session) -> str:
  """A tenant concept refining rs-gaap's professional fees, as its own
  operating expense row."""
  role = _network_role(session, "income_statement")
  return _extension(
    session,
    elements=[
      TaxonomyBlockElementRequest(
        qname="acme:AuditFees",
        name="Audit Fees",
        balance_type="debit",
        period_type="duration",
        parent_ref="rs-gaap:ProfessionalFees",
      )
    ],
    structures=[
      TaxonomyBlockStructureRequest(
        name="Acme income statement rows",
        block_type="income_statement",
        extends_network=role,
      )
    ],
    associations=[
      TaxonomyBlockAssociationRequest(
        structure_ref="Acme income statement rows",
        from_ref="rs-gaap:OperatingExpenses",
        to_ref="acme:AuditFees",
        association_type="presentation",
        order_value=10413,
      ),
      TaxonomyBlockAssociationRequest(
        structure_ref="Acme income statement rows",
        from_ref="rs-gaap:OperatingExpenses",
        to_ref="acme:AuditFees",
        association_type="calculation",
        weight=1.0,
      ),
    ],
  )


def test_a_curated_out_library_flow_is_pulled_in_and_becomes_taggable(tenant):
  """The extension names an rs-gaap investing flow the tenant copy curated
  out: the library's row is pulled in (not a tenant copy), and the merged
  cash flow network makes it a row a line may be tagged with."""
  with extensions_session(GRAPH) as session:
    style = load_entity_reporting_style(session, PARENT)
    assert AFS_PURCHASES not in taggable_flow_qnames(session, style)
    _cash_flow_extension(session)
    pulled = session.execute(
      select(Element).where(Element.qname == AFS_PURCHASES)
    ).scalar_one()
    assert pulled.taxonomy_id == _library_id(session)
    assert pulled.created_by == "library-seeder"
    assert AFS_PURCHASES in taggable_flow_qnames(session, style)


def test_a_tenant_row_renders_persists_and_is_mappable(tenant):
  """The tenant concept inherits its rs-gaap parent's expense classification,
  so it is a mapping candidate; a fact on it renders under operating
  expenses, sums into them, and lands in the income statement's fact set."""
  with extensions_session(GRAPH) as session:
    _income_statement_extension(session)
    style = load_entity_reporting_style(session, PARENT)
    audit = session.execute(
      select(Element).where(Element.qname == "acme:AuditFees")
    ).scalar_one()

    candidates = {
      c.qname
      for c in suggest_mapping_candidates(session, "expense", reporting_style_id=style)
    }
    assert "acme:AuditFees" in candidates

    fact = ReportFact(
      element_id=str(audit.id),
      element_qname="acme:AuditFees",
      element_name="Audit Fees",
      classification="expense",
      balance_type="debit",
      value=1200.0,
      period_start=FY.start,
      period_end=FY.end,
      period_type="duration",
    )
    grid = render_structure_view(session, [fact], "income_statement", [FY], style)
    rows = {r.element_qname: r for r in grid.rows}
    assert rows["acme:AuditFees"].values == [1200.0]
    assert rows["rs-gaap:OperatingExpenses"].values == [1200.0]
    assert rows["acme:AuditFees"].depth == rows["rs-gaap:OperatingExpenses"].depth + 1

    element_to_structures, _ = _build_structure_mapping(session, style)
    income_network = get_render_network(session, style, "income_statement")
    assert element_to_structures[str(audit.id)] == [income_network.structure_id]


@pytest.mark.parametrize(
  "case, message",
  [
    ("parent_off_network", "is not on the network it extends"),
    ("moves_library_row", "an extension adds rows, it never moves one"),
    ("no_calculation_path", "no calculation path"),
    ("tenant_concept_without_parent", "must refine an rs-gaap concept"),
  ],
)
def test_an_extension_that_would_not_anchor_is_refused(tenant, case, message):
  with extensions_session(GRAPH) as session:
    role = _network_role(session, "income_statement")
    structure = TaxonomyBlockStructureRequest(
      name="Acme rows", block_type="income_statement", extends_network=role
    )
    elements: list[TaxonomyBlockElementRequest] = []
    parent, child, calc = (
      "rs-gaap:OperatingExpenses",
      "rs-gaap:LaborAndRelatedExpense",
      True,
    )
    if case == "parent_off_network":
      parent = "rs-gaap:AssetsCurrent"
    elif case == "moves_library_row":
      child = "rs-gaap:ResearchAndDevelopmentExpense"
    elif case == "no_calculation_path":
      calc = False
    elif case == "tenant_concept_without_parent":
      elements = [
        TaxonomyBlockElementRequest(
          qname="acme:Orphan",
          name="Orphan",
          balance_type="debit",
          period_type="duration",
        )
      ]
      child = "acme:Orphan"
    associations = [
      TaxonomyBlockAssociationRequest(
        structure_ref="Acme rows",
        from_ref=parent,
        to_ref=child,
        association_type="presentation",
      )
    ]
    if calc:
      associations.append(
        TaxonomyBlockAssociationRequest(
          structure_ref="Acme rows",
          from_ref=parent,
          to_ref=child,
          association_type="calculation",
          weight=1.0,
        )
      )
    with pytest.raises(StyleExtensionError, match=message):
      _extension(
        session, elements=elements, structures=[structure], associations=associations
      )
    session.rollback()


def test_extends_network_must_name_a_style_network_of_its_statement(tenant):
  with extensions_session(GRAPH) as session:
    role = _network_role(session, "income_statement")
    with pytest.raises(StyleExtensionError, match="must be block_type"):
      _extension(
        session,
        structures=[
          TaxonomyBlockStructureRequest(
            name="Wrong type", block_type="balance_sheet", extends_network=role
          )
        ],
      )
    session.rollback()
    with pytest.raises(StyleExtensionError, match="not a Reporting Style network"):
      _extension(
        session,
        structures=[
          TaxonomyBlockStructureRequest(
            name="Unknown",
            block_type="income_statement",
            extends_network=f"{ROLES}NoSuchNetwork",
          )
        ],
      )
    session.rollback()
