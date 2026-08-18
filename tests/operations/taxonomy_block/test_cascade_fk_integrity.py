"""FK-integrity regression tests for taxonomy cascade deletion.

The disclosure substrate (#885/#889) attaches derived rows to tenant
taxonomy structures — VerificationResults FK the rules and structures,
FactSets FK the structures, AssociationClassifications FK the
associations, all with no ON DELETE. ``cascade_delete_taxonomy``
predates those tables, so deleting a taxonomy that had been used for a
disclosure note (bound text block or published report) died on the
first constraint with no API path to clear the blockers. Same hole in
``apply_structures_to_remove`` for update-driven structure removal.

These tests run against a real PostgreSQL schema (mocked sessions
can't violate constraints, which is how the bug survived the existing
delete tests).
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.taxonomy_block import UpdateTaxonomyBlockRequest
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  Classification,
  Element,
  FactSet,
  Rule,
  Structure,
  Taxonomy,
  VerificationResult,
)
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.operations.taxonomy_block.cascade import (
  cascade_delete_taxonomy,
  preflight_delete,
)
from robosystems.operations.taxonomy_block.update_apply import (
  apply_structures_to_remove,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test.

  FK constraints only exist (and only bite) on a real database; the schema
  name is unique so parallel workers can't collide.
  """
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_cascade_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session_factory = sessionmaker(bind=engine)
  session = session_factory()
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


def _build_disclosure_taxonomy(session: Session) -> dict:
  """A tenant taxonomy exercising every FK the cascade must clear."""
  taxonomy = Taxonomy(name="Notes", taxonomy_type="reporting_extension")
  session.add(taxonomy)
  session.flush()

  structure = Structure(name="Note 1", block_type="policy", taxonomy_id=taxonomy.id)
  session.add(structure)
  session.flush()

  parent = Element(name="NotePolicy", taxonomy_id=taxonomy.id)
  child = Element(name="NoteDetail", taxonomy_id=taxonomy.id)
  session.add_all([parent, child])
  session.flush()

  association = Association(
    structure_id=structure.id,
    from_element_id=parent.id,
    to_element_id=child.id,
  )
  session.add(association)
  session.flush()

  classification = Classification(
    category="concept_arrangement", identifier=f"cap-{uuid.uuid4().hex[:8]}"
  )
  session.add(classification)
  session.flush()
  session.add(
    AssociationClassification(
      association_id=association.id, classification_id=classification.id
    )
  )

  rule = Rule(
    taxonomy_id=taxonomy.id,
    rule_category="AutomatedAccountingAndReportingChecks",
    rule_pattern="RollUp",
    rule_expression="parent == sum(children)",
    target_kind="structure",
    target_structure_id=structure.id,
  )
  session.add(rule)
  session.flush()

  session.add(
    VerificationResult(rule_id=rule.id, structure_id=structure.id, status="pass")
  )

  fact_set = FactSet(
    structure_id=structure.id,
    period_end=date(2026, 6, 30),
    factset_type="disclosure",
    entity_id="ent_1",
  )
  fact_set.provenance = {"origin": "text_block", "document_id": "doc_1"}
  session.add(fact_set)
  session.flush()

  fact = Fact(
    element_id=child.id,
    string_value="Accounting policy text",
    fact_type="Nonnumeric",
    value_type="inline",
    period_end=date(2026, 6, 30),
    period_type="duration",
    entity_id="ent_1",
    structure_id=structure.id,
    fact_set_id=fact_set.id,
  )
  session.add(fact)
  session.commit()

  return {
    "taxonomy": taxonomy,
    "structure": structure,
    "elements": [parent, child],
    "fact_set": fact_set,
    "fact": fact,
  }


class TestCascadeDeleteTaxonomy:
  def test_deletes_taxonomy_with_full_disclosure_substrate(self, ext_session):
    built = _build_disclosure_taxonomy(ext_session)
    taxonomy_id = built["taxonomy"].id

    facts_deleted = cascade_delete_taxonomy(
      ext_session, taxonomy_id, cascade_facts=True
    )
    ext_session.commit()

    assert facts_deleted == 1
    assert ext_session.get(Taxonomy, taxonomy_id) is None
    assert ext_session.query(Structure).count() == 0
    assert ext_session.query(VerificationResult).count() == 0
    assert ext_session.query(FactSet).count() == 0
    assert ext_session.query(Fact).count() == 0
    assert ext_session.query(AssociationClassification).count() == 0
    assert ext_session.query(Rule).count() == 0

  def test_derived_rows_never_block_when_no_facts(self, ext_session):
    """VerificationResults and an empty standing FactSet are derived records;
    they cascade even with cascade_facts=False."""
    built = _build_disclosure_taxonomy(ext_session)
    ext_session.delete(built["fact"])
    ext_session.commit()

    facts_deleted = cascade_delete_taxonomy(
      ext_session, built["taxonomy"].id, cascade_facts=False
    )
    ext_session.commit()

    assert facts_deleted == 0
    assert ext_session.query(FactSet).count() == 0
    assert ext_session.query(VerificationResult).count() == 0

  def test_preflight_counts_snapshot_facts_of_foreign_elements(self, ext_session):
    """A report snapshot inside this taxonomy's set can copy another
    taxonomy's concepts; those facts die with the set, so preflight must
    count them toward the cascade_facts consent gate."""
    built = _build_disclosure_taxonomy(ext_session)

    other_tax = Taxonomy(name="Other", taxonomy_type="chart_of_accounts")
    ext_session.add(other_tax)
    ext_session.flush()
    foreign_element = Element(name="ForeignConcept", taxonomy_id=other_tax.id)
    ext_session.add(foreign_element)
    ext_session.flush()

    ext_session.add(
      Fact(
        element_id=foreign_element.id,
        value=100,
        period_end=date(2026, 6, 30),
        period_type="duration",
        entity_id="ent_1",
        fact_set_id=built["fact_set"].id,
      )
    )
    ext_session.commit()

    preflight = preflight_delete(ext_session, built["taxonomy"].id)
    assert preflight.fact_count == 2

    facts_deleted = cascade_delete_taxonomy(
      ext_session, built["taxonomy"].id, cascade_facts=True
    )
    ext_session.commit()

    assert facts_deleted == 2
    assert ext_session.get(Element, foreign_element.id) is not None
    assert ext_session.get(Taxonomy, other_tax.id) is not None


class TestApplyStructuresToRemove:
  def test_removes_structure_with_disclosure_substrate(self, ext_session):
    built = _build_disclosure_taxonomy(ext_session)
    structure_id = built["structure"].id

    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id=built["taxonomy"].id,
      structures_to_remove=[structure_id],
    )
    apply_structures_to_remove(ext_session, built["taxonomy"], payload)
    ext_session.commit()

    assert ext_session.get(Structure, structure_id) is None
    assert ext_session.query(FactSet).count() == 0
    assert ext_session.query(Fact).count() == 0
    assert ext_session.query(VerificationResult).count() == 0
    assert ext_session.query(AssociationClassification).count() == 0
    assert ext_session.get(Taxonomy, built["taxonomy"].id) is not None
    assert ext_session.get(Element, built["elements"][0].id) is not None


class TestCurationNeverReachesImmutableSets:
  """A filed report's snapshot and a closed month's canonical statement sets
  are immutable against curation: no cascade flag deletes them. Refusal happens
  before the first delete, so nothing is half-done."""

  @staticmethod
  def _report(session, taxonomy_id: str, filing_status: str):
    from robosystems.models.extensions import Report

    report = Report(
      name="FY26 Notes",
      taxonomy_id=taxonomy_id,
      filing_status=filing_status,
      created_by="usr_1",
    )
    session.add(report)
    session.flush()
    return report

  def test_delete_refuses_a_filed_reports_snapshot(self, ext_session):
    from robosystems.operations.taxonomy_block.immutability import (
      ProtectedFactsError,
    )

    built = _build_disclosure_taxonomy(ext_session)
    report = self._report(ext_session, built["taxonomy"].id, "filed")
    built["fact_set"].report_id = report.id
    built["fact_set"].factset_type = "report"
    ext_session.commit()

    with pytest.raises(ProtectedFactsError) as excinfo:
      cascade_delete_taxonomy(ext_session, built["taxonomy"].id, cascade_facts=True)
    assert excinfo.value.filed_report_count == 1
    ext_session.rollback()
    assert ext_session.get(Taxonomy, built["taxonomy"].id) is not None
    assert ext_session.query(Fact).count() == 1

  def test_delete_allows_a_draft_reports_snapshot(self, ext_session):
    built = _build_disclosure_taxonomy(ext_session)
    report = self._report(ext_session, built["taxonomy"].id, "draft")
    built["fact_set"].report_id = report.id
    built["fact_set"].factset_type = "report"
    ext_session.commit()

    assert (
      cascade_delete_taxonomy(ext_session, built["taxonomy"].id, cascade_facts=True)
      == 1
    )

  def test_delete_refuses_a_closed_periods_canonical_sets(self, ext_session):
    from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
    from robosystems.operations.taxonomy_block.immutability import (
      ProtectedFactsError,
    )

    built = _build_disclosure_taxonomy(ext_session)
    built["fact_set"].factset_type = "report"
    built["fact_set"].period_start = date(2026, 6, 1)
    ext_session.add(
      FiscalPeriod(
        graph_id="kg0123456789abcdef01",
        name="2026-06",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 30),
        period_type="month",
        status="closed",
      )
    )
    ext_session.commit()

    with pytest.raises(ProtectedFactsError) as excinfo:
      cascade_delete_taxonomy(ext_session, built["taxonomy"].id, cascade_facts=True)
    assert excinfo.value.closed_period_count == 1

  def test_delete_allows_an_open_periods_canonical_sets(self, ext_session):
    from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

    built = _build_disclosure_taxonomy(ext_session)
    built["fact_set"].factset_type = "report"
    built["fact_set"].period_start = date(2026, 6, 1)
    ext_session.add(
      FiscalPeriod(
        graph_id="kg0123456789abcdef01",
        name="2026-06",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 30),
        period_type="month",
        status="open",
      )
    )
    ext_session.commit()

    assert (
      cascade_delete_taxonomy(ext_session, built["taxonomy"].id, cascade_facts=True)
      == 1
    )

  def test_structure_removal_refuses_and_reports_through_the_validator(
    self, ext_session
  ):
    from robosystems.operations.taxonomy_block.immutability import (
      ProtectedFactsError,
    )
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_structures_to_remove,
    )

    built = _build_disclosure_taxonomy(ext_session)
    report = self._report(ext_session, built["taxonomy"].id, "filed")
    built["fact_set"].report_id = report.id
    built["fact_set"].factset_type = "report"
    ext_session.commit()

    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id=built["taxonomy"].id,
      structures_to_remove=[built["structure"].id],
    )
    issues = _validate_structures_to_remove(
      ext_session, built["taxonomy"], payload, {built["structure"].id}
    )
    assert [i.code for i in issues] == ["protected_facts"]

    with pytest.raises(ProtectedFactsError):
      apply_structures_to_remove(ext_session, built["taxonomy"], payload)
    ext_session.rollback()
    assert ext_session.get(Structure, built["structure"].id) is not None


class TestDependentsThatUsedToBlock:
  """Two dependents nothing swept: the parent entity's adoption row for a
  chart of accounts (``entity_taxonomies.taxonomy_id`` RESTRICT), and tenant
  rules targeting an element being removed through update."""

  def test_coa_with_an_adopting_entity_is_deletable(self, ext_session):
    from robosystems.models.extensions import Entity, EntityTaxonomy

    taxonomy = Taxonomy(name="CoA", taxonomy_type="chart_of_accounts")
    ext_session.add(taxonomy)
    ext_session.flush()
    entity = Entity(name="Acme", created_by="usr_1")
    ext_session.add(entity)
    ext_session.flush()
    ext_session.add(
      EntityTaxonomy(
        entity_id=entity.id,
        taxonomy_id=taxonomy.id,
        basis="chart_of_accounts",
        is_primary=True,
      )
    )
    ext_session.commit()

    cascade_delete_taxonomy(ext_session, taxonomy.id, cascade_facts=False)
    ext_session.commit()

    assert ext_session.get(Taxonomy, taxonomy.id) is None
    assert ext_session.query(EntityTaxonomy).count() == 0
    assert ext_session.get(Entity, entity.id) is not None

  def test_element_removal_sweeps_rules_targeting_it(self, ext_session):
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_elements_to_remove,
    )

    built = _build_disclosure_taxonomy(ext_session)
    child = built["elements"][1]
    child.qname = "x:NoteDetail"
    # A tenant rule targeting the element, with an evaluation on record.
    rule = Rule(
      taxonomy_id=built["taxonomy"].id,
      rule_category="AutomatedAccountingAndReportingChecks",
      rule_pattern="GreaterThanOrEqualToZero",
      rule_expression="value >= 0",
      target_kind="element",
      target_element_id=child.id,
    )
    ext_session.add(rule)
    ext_session.flush()
    ext_session.add(
      VerificationResult(
        rule_id=rule.id, structure_id=built["structure"].id, status="pass"
      )
    )
    # Remove the facts first: element removal is refused while facts cite it.
    ext_session.delete(built["fact"])
    ext_session.commit()

    child_id, rule_id = child.id, rule.id
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id=built["taxonomy"].id, elements_to_remove=["x:NoteDetail"]
    )
    apply_elements_to_remove(
      ext_session, built["taxonomy"], payload, {"x:NoteDetail": child_id}
    )
    ext_session.commit()
    ext_session.expunge_all()

    assert ext_session.get(Element, child_id) is None
    assert ext_session.get(Rule, rule_id) is None
    assert ext_session.query(AssociationClassification).count() == 0
