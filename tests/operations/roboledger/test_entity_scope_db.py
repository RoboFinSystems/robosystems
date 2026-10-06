"""The single entity resolver and the entity-scoped close stamp, against real
Postgres with a parent, a native subsidiary and a linked counterparty in one
tenant schema. Mocked sessions cannot show these: the failures are SQL scope
and heap order."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboledger.report import Report
from robosystems.operations.roboledger.entity_scope import (
  EntityNotInGraphError,
  NoEntityError,
  find_linked_entity_id,
  report_entity_id,
  resolve_entity_id,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.reads.reports import resolve_entity_name
from robosystems.operations.roboledger.reports.statement_sets import (
  has_canonical_statement_sets,
  retract_canonical_statement_sets,
)

pytestmark = pytest.mark.unit

PS = date(2026, 9, 1)
PE = date(2026, 9, 30)


def _canonical_set(session, entity_id: str, report_id: str | None = None) -> str:
  fact_set = create_fact_set(
    session,
    period_start=PS,
    period_end=PE,
    factset_type="report",
    entity_id=entity_id,
    report_id=report_id,
    provenance=AssertedProvenance(
      source_system="test", asserted_by="usr_1", basis_note="fixture"
    ),
    created_by="usr_1",
  )
  session.flush()
  return fact_set.id


class TestResolveEntityId:
  def test_defaults_to_the_parent_not_the_earlier_linked_row(self, two_entities):
    assert resolve_entity_id(two_entities.session) == two_entities.parent.id

  def test_a_named_sibling_resolves_to_itself(self, two_entities):
    t = two_entities
    assert resolve_entity_id(t.session, t.sub.id) == t.sub.id

  def test_a_linked_row_is_not_a_ledger_scope(self, two_entities):
    t = two_entities
    with pytest.raises(EntityNotInGraphError):
      resolve_entity_id(t.session, t.linked.id)

  def test_an_unknown_id_is_refused(self, two_entities):
    with pytest.raises(EntityNotInGraphError):
      resolve_entity_id(two_entities.session, "ent_nope")

  def test_no_entity_raises_an_error_every_old_caller_catches(self, tenant_session):
    with pytest.raises(NoEntityError) as exc:
      resolve_entity_id(tenant_session)
    assert isinstance(exc.value, LookupError)
    assert isinstance(exc.value, ValueError)


class TestRetractIsScopedToOneEntity:
  def test_retracting_the_parent_leaves_the_subsidiary(self, two_entities):
    t = two_entities
    parent_set = _canonical_set(t.session, t.parent.id)
    sub_set = _canonical_set(t.session, t.sub.id)

    retracted = retract_canonical_statement_sets(
      t.session, period_start=PS, period_end=PE
    )

    assert retracted == [parent_set]
    assert has_canonical_statement_sets(
      t.session, period_start=PS, period_end=PE, entity_id=t.sub.id
    )
    assert not has_canonical_statement_sets(t.session, period_start=PS, period_end=PE)
    assert sub_set

  def test_retracting_the_subsidiary_leaves_the_parent(self, two_entities):
    t = two_entities
    parent_set = _canonical_set(t.session, t.parent.id)
    sub_set = _canonical_set(t.session, t.sub.id)

    retracted = retract_canonical_statement_sets(
      t.session, period_start=PS, period_end=PE, entity_id=t.sub.id
    )

    assert retracted == [sub_set]
    assert has_canonical_statement_sets(t.session, period_start=PS, period_end=PE)
    assert parent_set

  def test_no_entity_retracts_nothing(self, tenant_session):
    assert (
      retract_canonical_statement_sets(tenant_session, period_start=PS, period_end=PE)
      == []
    )


class TestLinkedEntityKey:
  def _linked(self, session, source_entity_id: str | None) -> Entity:
    metadata = {"source_graph_id": "kg_group"}
    if source_entity_id:
      metadata["source_entity_id"] = source_entity_id
    row = Entity(
      name=f"Linked {source_entity_id}",
      is_parent=False,
      source="linked",
      created_by="usr_1",
      metadata_=metadata,
    )
    session.add(row)
    session.flush()
    return row

  def test_two_entities_of_one_sending_graph_stay_two_rows(self, tenant_session):
    a = self._linked(tenant_session, "ent_a")
    b = self._linked(tenant_session, "ent_b")
    assert find_linked_entity_id(tenant_session, "kg_group", "ent_a") == a.id
    assert find_linked_entity_id(tenant_session, "kg_group", "ent_b") == b.id

  def test_an_exact_match_wins_over_a_row_from_before_the_key(self, tenant_session):
    self._linked(tenant_session, None)
    exact = self._linked(tenant_session, "ent_a")
    assert find_linked_entity_id(tenant_session, "kg_group", "ent_a") == exact.id

  def test_a_row_from_before_the_key_still_matches(self, tenant_session):
    legacy = self._linked(tenant_session, None)
    assert find_linked_entity_id(tenant_session, "kg_group", "ent_a") == legacy.id

  def test_an_unkeyed_row_is_not_claimed_for_a_non_parent(self, tenant_session):
    self._linked(tenant_session, None)
    assert (
      find_linked_entity_id(tenant_session, "kg_group", "ent_b", match_unkeyed=False)
      is None
    )

  def test_another_graphs_rows_never_match(self, tenant_session):
    self._linked(tenant_session, "ent_a")
    assert find_linked_entity_id(tenant_session, "kg_other", "ent_a") is None


class TestAReportNamesItsOwnEntity:
  def _report(self, session) -> Report:
    report = Report(
      name="September",
      taxonomy_id="tax_1",
      created_by="usr_1",
      created_at=datetime.now(UTC),
    )
    session.add(report)
    session.flush()
    return report

  def test_a_subsidiary_report_is_labeled_as_the_subsidiary(self, two_entities):
    t = two_entities
    report = self._report(t.session)
    _canonical_set(t.session, t.sub.id, report_id=report.id)

    assert report_entity_id(t.session, report.id) == t.sub.id
    assert resolve_entity_name(t.session, report) == "Maple Court LLC"

  def test_the_earliest_fact_set_decides_a_mixed_report(self, two_entities):
    t = two_entities
    report = self._report(t.session)
    _canonical_set(t.session, t.sub.id, report_id=report.id)
    _canonical_set(t.session, t.parent.id, report_id=report.id)
    assert report_entity_id(t.session, report.id) == t.sub.id

  def test_a_report_without_facts_falls_back_to_the_parent(self, two_entities):
    t = two_entities
    report = self._report(t.session)
    assert resolve_entity_name(t.session, report) == "Harbor Holdings"
