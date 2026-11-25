"""A contra-asset account is named the concept its gross sibling maps to.

On a provisioned tenant with a real chart: an Intangible Assets parent with a
gross account and an Accumulated Amortization contra under it, the shape a
QuickBooks company brings. The unmapped read offers the contra the gross
account's concept once the gross account is mapped, and only when the parent
has exactly one gross account.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.api.extensions.entity import CreateEntityRequest
from robosystems.models.extensions import Association, Element, ElementTrait, Trait
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.entity import create_entity
from robosystems.operations.roboledger.reads.taxonomies import list_unmapped_elements
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)
from tests.tenant_schema import claim_provisioned_schema, release_tenant_schema

pytestmark = pytest.mark.integration

GRAPH = "kgcccccccccccccccc83"
NET = "rs-gaap:IntangibleAssetsNetExcludingGoodwill"


@pytest.fixture()
def chart():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  db = create_engine(url)
  try:
    with db.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    db.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  try:
    claim_provisioned_schema(GRAPH)
    with extensions_session(GRAPH) as session:
      entity_id = create_entity(
        session, CreateEntityRequest(name="Harbor LLC", entity_type="llc"), "usr_1"
      ).id
      taxonomy_id = initialize_chart_of_accounts(
        session,
        InitializeChartOfAccountsRequest(template="services", entity_id=entity_id),
        "usr_1",
      ).taxonomy_id
      session.commit()
      yield {
        "session": session,
        "taxonomy": taxonomy_id,
        "mapping": find_entity_mapping(session, entity_id, BOOK_FRAMEWORK).id,
      }
  finally:
    with db.begin() as conn:
      release_tenant_schema(conn, GRAPH)
    db.dispose()


def _account(g, code: str, name: str, trait: str | None, parent=None) -> Element:
  session = g["session"]
  template = session.execute(
    select(Element).where(Element.taxonomy_id == g["taxonomy"]).limit(1)
  ).scalar_one()
  account = Element(
    code=code,
    name=name,
    taxonomy_id=g["taxonomy"],
    source=template.source,
    element_type="concept",
    balance_type="credit" if trait == "contraAsset" else "debit",
    period_type="instant",
    is_abstract=False,
    is_monetary=True,
    parent_id=parent.id if parent else None,
    created_by="usr_1",
  )
  session.add(account)
  session.flush()
  if trait:
    trait_id = session.execute(
      select(Trait.id).where(
        Trait.category == "elementsOfFinancialStatements", Trait.identifier == trait
      )
    ).scalar_one()
    session.add(ElementTrait(element_id=account.id, trait_id=trait_id))
    session.flush()
  return account


def _map(g, account: Element, qname: str) -> None:
  session = g["session"]
  concept = session.execute(
    select(Element.id).where(Element.qname == qname)
  ).scalar_one()
  session.add(
    Association(
      structure_id=g["mapping"],
      from_element_id=account.id,
      to_element_id=concept,
      association_type="mapping",
      created_by="usr_1",
    )
  )
  session.flush()


def _offer(g, account: Element):
  unmapped = list_unmapped_elements(g["session"], mapping_id=g["mapping"])
  return next(e for e in unmapped if e.id == account.id).nets_against


def test_the_contra_is_offered_its_mapped_gross_siblings_concept(chart) -> None:
  parent = _account(chart, "1400", "Intangible Assets", "asset")
  gross = _account(chart, "1410", "Gross Intangible Assets", "asset", parent)
  contra = _account(chart, "1420", "Accumulated Amortization", "contraAsset", parent)

  # Not yet: the sibling it nets against is unmapped.
  assert _offer(chart, contra) is None

  _map(chart, gross, NET)
  offer = _offer(chart, contra)
  assert offer is not None and offer.qname == NET


def test_two_gross_siblings_leave_the_contra_to_a_person(chart) -> None:
  parent = _account(chart, "1400", "Intangible Assets", "asset")
  software = _account(chart, "1410", "Software", "asset", parent)
  licenses = _account(chart, "1415", "Licenses", "asset", parent)
  contra = _account(chart, "1420", "Accumulated Amortization", "contraAsset", parent)
  _map(chart, software, NET)
  _map(chart, licenses, NET)

  assert _offer(chart, contra) is None


def test_a_contra_with_no_parent_is_left_to_a_person(chart) -> None:
  contra = _account(chart, "1420", "Accumulated Amortization", "contraAsset")

  assert _offer(chart, contra) is None
