"""The single entity resolver and the entity-scoped close stamp, against real
Postgres with a parent, a native subsidiary and a linked counterparty in one
tenant schema. Mocked sessions cannot show these: the failures are SQL scope
and heap order."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import text

from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.report import Report
from robosystems.operations.roboledger import entity_scope
from robosystems.operations.roboledger.commands._guards import closed_periods
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.entity_scope import (
  EntityNotInGraphError,
  NoEntityError,
  ensure_entity_id,
  find_entity_id,
  find_linked_entity_id,
  report_entity_id,
  resolve_entity_id,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.fiscal_calendar.service import (
  FiscalCalendarService,
)
from robosystems.operations.roboledger.reads.reports import resolve_entity_name
from robosystems.operations.roboledger.reports.statement_sets import (
  has_canonical_statement_sets,
  retract_canonical_statement_sets,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef08"
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


class TestAGraphCreatedWithoutItsEntity:
  """``create_entity=false`` at graph creation. Ledger writes worked on such
  a graph before rows carried an entity, so the first one gives it a parent."""

  def test_its_first_ledger_write_makes_the_group_parent(self, tenant_session):
    session = tenant_session
    accounts = [
      Element(name=name, code=name, created_by="usr_1") for name in ("Cash", "Rent")
    ]
    session.add_all(accounts)
    session.flush()

    created = create_journal_entry(
      session,
      CreateJournalEntryRequest(
        posting_date=date(2026, 9, 15),
        memo="Rent received",
        line_items=[
          JournalEntryLineItemInput(element_id=accounts[0].id, debit_amount=50_000),
          JournalEntryLineItemInput(element_id=accounts[1].id, credit_amount=50_000),
        ],
      ),
      "usr_1",
    )

    parent = session.query(Entity).one()
    schema = session.execute(text("SELECT current_schema()")).scalar_one()
    assert parent.id == f"entity_{schema}"
    assert parent.is_parent and parent.source == "native"
    # A test tenant has no platform record, so the name falls back to the id.
    assert parent.name == schema
    assert session.get(Entry, created.id).entity_id == parent.id
    assert resolve_entity_id(session) == parent.id

  def test_the_parent_carries_the_graphs_name(self, tenant_session, monkeypatch):
    monkeypatch.setattr(entity_scope, "graph_display_name", lambda _: "Harbinger Group")

    parent_id = ensure_entity_id(tenant_session)

    assert tenant_session.get(Entity, parent_id).name == "Harbinger Group"

  def test_a_second_write_finds_the_same_one(self, tenant_session):
    first = ensure_entity_id(tenant_session)

    assert ensure_entity_id(tenant_session) == first
    assert tenant_session.query(Entity).count() == 1

  def test_setting_up_its_calendar_makes_it_too(self, tenant_session):
    calendar = FiscalCalendarService().initialize(
      tenant_session, GRAPH_ID, closed_through="2026-06", actor_id="usr_1"
    )

    assert calendar.entity_id == resolve_entity_id(tenant_session)

  def test_a_read_makes_nothing(self, tenant_session):
    assert find_entity_id(tenant_session) is None
    assert closed_periods(tenant_session, [date(2026, 9, 15)], entity_id=None) == []
    assert FiscalCalendarService().get(tenant_session, GRAPH_ID) is None
    assert tenant_session.query(Entity).count() == 0

  def test_entities_with_no_parent_among_them_are_left_alone(self, tenant_session):
    tenant_session.add(
      Entity(name="Orphan LLC", is_parent=False, source="native", created_by="usr_1")
    )
    tenant_session.flush()

    with pytest.raises(NoEntityError):
      ensure_entity_id(tenant_session)
    assert tenant_session.query(Entity).count() == 1

  def test_a_named_entity_is_never_made(self, tenant_session):
    with pytest.raises(EntityNotInGraphError):
      ensure_entity_id(tenant_session, "ent_missing")
    assert tenant_session.query(Entity).count() == 0


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


class TestGraphDisplayName:
  def test_an_unknown_graph_falls_back_to_its_id(self):
    assert entity_scope.graph_display_name("kg_nowhere") == "kg_nowhere"

  def test_a_platform_read_that_fails_falls_back_to_its_id(self, monkeypatch):
    import robosystems.db.platform as platform

    def boom():
      raise RuntimeError("platform db down")

    monkeypatch.setattr(platform, "SessionFactory", boom)

    assert entity_scope.graph_display_name("kg_x") == "kg_x"
