"""Element listing, mapping detail, coverage and calc-DAG reachability.

Runs against the real test Postgres with a throwaway tenant schema: the
filters and joins under test are SQL, and a mocked session would only echo
the query back.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import (
  Association,
  Element,
  ElementTrait,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingStructureNotFoundError,
)
from robosystems.operations.roboledger.reads.taxonomies import (
  check_mapping_reachability,
  get_mapping_coverage,
  get_mapping_detail,
  is_target_reachable,
  list_elements,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_tax_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(
    bind=session.connection(),
    tables=[t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None],
  )
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
def ledger(ext_session):
  """A two-account CoA mapped onto a small rs-gaap calc DAG.

  Assets ← CurrentAssets ← Cash is a live branch; Orphan has no path to a
  root, so a mapping onto it never renders.
  """
  s = ext_session
  s.add_all(
    [
      Taxonomy(id="tax_coa", name="Chart", taxonomy_type="chart_of_accounts"),
      Taxonomy(id="tax_map", name="Mapping", taxonomy_type="mapping"),
      Taxonomy(id="tax_rs", name="rs-gaap", taxonomy_type="reporting"),
      Trait(
        id="trt_asset", category="elementsOfFinancialStatements", identifier="Asset"
      ),
      Trait(
        id="trt_exp", category="elementsOfFinancialStatements", identifier="Expense"
      ),
    ]
  )
  s.flush()
  s.add_all(
    [
      Element(id="el_cash_acct", name="Checking", code="1010", taxonomy_id="tax_coa"),
      Element(id="el_rent_acct", name="Rent", code="6100", taxonomy_id="tax_coa"),
      Element(id="el_ar_acct", name="Receivables", code="1200", taxonomy_id="tax_coa"),
      Element(
        id="el_group",
        name="Current assets group",
        code="1000",
        taxonomy_id="tax_coa",
        is_abstract=True,
      ),
      Element(
        id="el_assets",
        name="Assets",
        qname="rs-gaap:Assets",
        source="rs-gaap",
        taxonomy_id="tax_rs",
      ),
      Element(
        id="el_current",
        name="Current assets",
        qname="rs-gaap:AssetsCurrent",
        source="rs-gaap",
        taxonomy_id="tax_rs",
        depth=1,
      ),
      Element(
        id="el_cash",
        name="Cash",
        qname="rs-gaap:Cash",
        source="rs-gaap",
        taxonomy_id="tax_rs",
        depth=2,
      ),
      Element(
        id="el_orphan",
        name="Orphan",
        qname="rs-gaap:Orphan",
        source="rs-gaap",
        taxonomy_id="tax_rs",
        depth=2,
      ),
      Element(
        id="el_inactive",
        name="Retired",
        code="9999",
        taxonomy_id="tax_coa",
        is_active=False,
      ),
      Structure(
        id="map_1", name="CoA mapping", block_type="coa_mapping", taxonomy_id="tax_map"
      ),
      Structure(
        id="calc_bs",
        name="Balance sheet calc",
        block_type="balance_sheet",
        taxonomy_id="tax_rs",
      ),
    ]
  )
  s.flush()
  s.add_all(
    [
      ElementTrait(element_id="el_cash_acct", trait_id="trt_asset"),
      ElementTrait(element_id="el_ar_acct", trait_id="trt_asset"),
      ElementTrait(element_id="el_rent_acct", trait_id="trt_exp"),
      # Calculation arcs point parent → child.
      Association(
        structure_id="calc_bs",
        from_element_id="el_assets",
        to_element_id="el_current",
        association_type="calculation",
      ),
      Association(
        structure_id="calc_bs",
        from_element_id="el_current",
        to_element_id="el_cash",
        association_type="calculation",
      ),
      Association(
        structure_id="map_1",
        from_element_id="el_cash_acct",
        to_element_id="el_cash",
        association_type="mapping",
        order_value=1,
        confidence=0.95,
      ),
      Association(
        structure_id="map_1",
        from_element_id="el_ar_acct",
        to_element_id="el_current",
        association_type="mapping",
        order_value=2,
        confidence=0.8,
      ),
      Association(
        structure_id="map_1",
        from_element_id="el_rent_acct",
        to_element_id="el_orphan",
        association_type="mapping",
        order_value=3,
        confidence=0.5,
      ),
    ]
  )
  s.flush()
  return s


def _mappings(session) -> list[Association]:
  return list(
    session.query(Association)
    .filter_by(structure_id="map_1", association_type="mapping")
    .order_by(Association.order_value)
  )


class TestListElements:
  def test_lists_active_elements_only(self, ledger):
    result = list_elements(ledger)
    ids = {e.id for e in result.elements}
    assert "el_inactive" not in ids
    assert result.pagination.total == 8

  def test_filters_by_taxonomy_source_and_abstract(self, ledger):
    coa = list_elements(ledger, taxonomy_id="tax_coa", is_abstract=False)
    assert {e.code for e in coa.elements} == {"1010", "1200", "6100"}

    rs = list_elements(ledger, source="rs-gaap")
    assert {e.qname for e in rs.elements} == {
      "rs-gaap:Assets",
      "rs-gaap:AssetsCurrent",
      "rs-gaap:Cash",
      "rs-gaap:Orphan",
    }

  def test_filters_by_trait_and_reports_it(self, ledger):
    result = list_elements(ledger, trait="Asset")
    assert {(e.id, e.trait) for e in result.elements} == {
      ("el_cash_acct", "Asset"),
      ("el_ar_acct", "Asset"),
    }
    assert result.pagination.total == 2

  def test_pages_in_depth_then_code_order(self, ledger):
    first = list_elements(ledger, limit=2, offset=0)
    second = list_elements(ledger, limit=2, offset=2)
    assert [e.code for e in first.elements] == ["1000", "1010"]
    assert [e.code for e in second.elements] == ["1200", "6100"]
    assert first.pagination.total == second.pagination.total == 8


class TestMappingDetail:
  def test_returns_associations_with_both_ends_named(self, ledger):
    detail = get_mapping_detail(ledger, "map_1")
    assert detail is not None
    assert detail.total_associations == 3
    first = detail.associations[0]
    assert (first.from_element_name, first.to_element_qname) == (
      "Checking",
      "rs-gaap:Cash",
    )
    assert [a.order_value for a in detail.associations] == [1, 2, 3]

  def test_unknown_mapping_is_none(self, ledger):
    assert get_mapping_detail(ledger, "map_missing") is None


class TestMappingCoverage:
  def test_counts_confidence_bands_and_unreachable_targets(self, ledger):
    coverage = get_mapping_coverage(ledger, "map_1")
    # Abstract and inactive CoA elements are not mapping candidates.
    assert coverage.total_coa_elements == 3
    assert coverage.mapped_count == 3
    assert coverage.unmapped_count == 0
    assert coverage.coverage_percent == 100.0
    assert (coverage.high_confidence, coverage.medium_confidence) == (1, 1)
    assert coverage.low_confidence == 1
    assert coverage.unreachable_count == 1
    assert coverage.unreachable[0].coa_code == "6100"
    assert coverage.unreachable[0].target_qname == "rs-gaap:Orphan"

  def test_unknown_mapping_raises_rather_than_reporting_zero(self, ledger):
    with pytest.raises(MappingStructureNotFoundError):
      get_mapping_coverage(ledger, "map_missing")


class TestReachability:
  @pytest.mark.parametrize(
    ("target", "reachable"),
    [
      ("el_assets", True),  # a root is trivially reachable
      ("el_current", True),
      ("el_cash", True),  # two hops up
      ("el_orphan", False),
    ],
  )
  def test_walks_the_calc_dag_to_a_root(self, ledger, target, reachable):
    assert is_target_reachable(ledger, target) is reachable

  def test_a_cycle_without_a_root_terminates(self, ledger):
    ledger.add_all(
      [
        Association(
          structure_id="calc_bs",
          from_element_id="el_orphan",
          to_element_id="el_rent_acct",
          association_type="calculation",
        ),
        Association(
          structure_id="calc_bs",
          from_element_id="el_rent_acct",
          to_element_id="el_orphan",
          association_type="calculation",
        ),
      ]
    )
    ledger.flush()
    assert is_target_reachable(ledger, "el_orphan") is False

  def test_reports_only_the_dead_branch_mapping(self, ledger):
    unreachable = check_mapping_reachability(ledger, _mappings(ledger))
    assert [(u.coa_element_id, u.target_element_id) for u in unreachable] == [
      ("el_rent_acct", "el_orphan")
    ]
    assert unreachable[0].coa_name == "Rent"

  def test_no_mappings_is_nothing_to_check(self, ledger):
    assert check_mapping_reachability(ledger, []) == []
