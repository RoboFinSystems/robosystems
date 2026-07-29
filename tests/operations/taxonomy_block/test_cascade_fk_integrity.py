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
