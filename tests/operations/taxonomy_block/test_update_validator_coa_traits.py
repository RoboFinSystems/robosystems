"""Update-path projection must carry live EFS traits — real Postgres.

The projection synthesizes a virtual create request from live rows and
re-runs the create phases. The CoA type-specific phase rejects EVERY
element with ``trait=None`` — so a projection that drops existing
elements' classifications 422s every update on a chart_of_accounts
block, however small the delta. Traits live in the element_traits
junction, not on the Element row, so this needs a real session.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.taxonomy_block import (
  ElementUpdatePatch,
  TaxonomyBlockElementRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Element, ElementTrait, Taxonomy, Trait
from robosystems.operations.taxonomy_block.update_validator import (
  validate_update_envelope,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_coaupd_{uuid.uuid4().hex[:12]}"
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


@pytest.fixture()
def coa_taxonomy(ext_session):
  """A minimal chart_of_accounts block whose traits live in the junction."""
  taxonomy = Taxonomy(name="Tenant CoA", taxonomy_type="chart_of_accounts")
  ext_session.add(taxonomy)
  ext_session.flush()

  cash = Element(
    name="Cash",
    qname="rl:Cash",
    code="1000",
    balance_type="debit",
    period_type="instant",
    taxonomy_id=taxonomy.id,
  )
  rent = Element(
    name="Rent Expense",
    qname="rl:RentExpense",
    code="6000",
    balance_type="debit",
    period_type="duration",
    taxonomy_id=taxonomy.id,
  )
  ext_session.add_all([cash, rent])
  ext_session.flush()

  asset_trait = Trait(category="elementsOfFinancialStatements", identifier="asset")
  expense_trait = Trait(category="elementsOfFinancialStatements", identifier="expense")
  ext_session.add_all([asset_trait, expense_trait])
  ext_session.flush()

  ext_session.add_all(
    [
      ElementTrait(element_id=cash.id, trait_id=asset_trait.id, is_primary=True),
      ElementTrait(element_id=rent.id, trait_id=expense_trait.id, is_primary=True),
    ]
  )
  ext_session.flush()

  return taxonomy


class TestCoaUpdateProjectionTraits:
  def test_rename_produces_no_missing_classification(self, ext_session, coa_taxonomy):
    """Renaming one account must not flag every element as unclassified."""
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id=str(coa_taxonomy.id),
      elements_to_update=[
        ElementUpdatePatch(qname="rl:Cash", name="Cash and Equivalents")
      ],
    )

    issues = validate_update_envelope(ext_session, coa_taxonomy, payload)

    missing = [i for i in issues if i.code == "coa_missing_classification"]
    assert missing == [], (
      f"existing elements' live traits must reach the projection; got {missing}"
    )
    assert issues == []

  def test_new_element_without_trait_still_flagged(self, ext_session, coa_taxonomy):
    """The check must keep rejecting genuinely new trait-less elements."""
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id=str(coa_taxonomy.id),
      elements_to_add=[
        TaxonomyBlockElementRequest(
          qname="rl:MysteryAccount",
          name="Mystery Account",
          code="9999",
        )
      ],
    )

    issues = validate_update_envelope(ext_session, coa_taxonomy, payload)

    flagged = [i for i in issues if i.code == "coa_missing_classification"]
    assert len(flagged) == 1
    assert flagged[0].context["element_qname"] == "rl:MysteryAccount"
