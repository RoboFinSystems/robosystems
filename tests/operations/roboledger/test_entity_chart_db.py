"""Each entity keeps its books in its own chart of accounts, and its close
stamps its own statements from them. Runs against a tenant provisioned the
way a graph's is, with the taxonomy library copied in, so the stamp is the
real pivot through each entity's own mapping."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.extensions import Element, Entity, EntityTaxonomy
from robosystems.models.extensions.roboledger import Fact, FactSet
from robosystems.operations.roboledger.commands._guards import (
  AccountOutsideEntityChartError,
)
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  ChartAlreadyExistsError,
  active_chart_id,
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import close_period
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads.account_rollups import get_account_rollups
from robosystems.operations.roboledger.reads.accounts import list_accounts
from robosystems.operations.roboledger.reads.taxonomies import (
  list_unmapped_elements,
)
from robosystems.operations.taxonomy_block.coa_mappings import find_entity_mapping

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd38"
PARENT, SUB = "ent_harbor", "ent_maple"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
REVENUE = "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
CASH = "rs-gaap:CashAndCashEquivalentsAtCarryingValue"
JULY_END = date(2026, 7, 31)


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
          name="Harbor Holdings",
          entity_type="llc",
          is_parent=True,
          created_by="usr_1",
        )
      )
      session.flush()
      session.add(
        Entity(
          id=SUB,
          name="Maple Court LLC",
          ticker="MCL",
          entity_type="llc",
          is_parent=False,
          parent_entity_id=PARENT,
          created_by="usr_1",
        )
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture()
def charts(tenant):
  """A services chart for each entity. Returns ``{entity_id: chart_id}``."""
  body = InitializeChartOfAccountsRequest(template="services")
  with extensions_session(GRAPH) as session:
    parent = initialize_chart_of_accounts(session, body, "usr_1")
    # The request names the entity, the way the operation reaches it.
    sub = initialize_chart_of_accounts(
      session,
      InitializeChartOfAccountsRequest(template="services", entity_id=SUB),
      "usr_1",
    )
    return {PARENT: parent.taxonomy_id, SUB: sub.taxonomy_id}


def _account(session, chart_id: str, code: str) -> str:
  return str(
    session.execute(
      select(Element.id).where(Element.taxonomy_id == chart_id, Element.code == code)
    ).scalar_one()
  )


def _revenue(session, charts, entity_id: str, cents: int, *, into: str | None = None):
  """Draft cash received for services, in ``entity_id``'s books and on the
  accounts of ``into``'s chart (its own unless a test crosses them)."""
  chart_id = charts[into or entity_id]
  return create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=date(2026, 7, 15),
      memo="Consulting fees received",
      line_items=[
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, "1000"), debit_amount=cents
        ),
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, "4000"), credit_amount=cents
        ),
      ],
    ),
    "usr_1",
    entity_id=entity_id,
  )


class TestOneChartPerEntity:
  def test_each_entity_initializes_its_own_chart(self, charts):
    assert charts[PARENT] != charts[SUB]
    with extensions_session(GRAPH) as session:
      assert active_chart_id(session) == charts[PARENT]
      assert active_chart_id(session, SUB) == charts[SUB]
      owners = dict(
        session.execute(
          select(EntityTaxonomy.taxonomy_id, EntityTaxonomy.entity_id).where(
            EntityTaxonomy.basis == "chart_of_accounts"
          )
        ).all()
      )
    assert owners == {charts[PARENT]: PARENT, charts[SUB]: SUB}

  def test_the_same_template_twice_does_not_collide_on_account_names(self, charts):
    with extensions_session(GRAPH) as session:
      parent_cash = session.get(Element, _account(session, charts[PARENT], "1000"))
      sub_cash = session.get(Element, _account(session, charts[SUB], "1000"))
      assert parent_cash.qname == "coa:1000"
      assert sub_cash.qname == "coa-mcl:1000"

  def test_an_entity_still_has_only_one_chart(self, charts):
    body = InitializeChartOfAccountsRequest(template="saas")
    with extensions_session(GRAPH) as session:
      for entity_id in (None, SUB):
        with pytest.raises(ChartAlreadyExistsError):
          initialize_chart_of_accounts(session, body, "usr_1", entity_id=entity_id)

  def test_each_entity_maps_its_own_chart(self, charts):
    with extensions_session(GRAPH) as session:
      parent_mapping = find_entity_mapping(session, PARENT)
      sub_mapping = find_entity_mapping(session, SUB)
      assert parent_mapping is not None and sub_mapping is not None
      assert parent_mapping.id != sub_mapping.id

  def test_account_reads_are_one_entitys(self, charts):
    with extensions_session(GRAPH) as session:
      parent_accounts = list_accounts(session, limit=500).accounts
      sub_accounts = list_accounts(session, limit=500, entity_id=SUB).accounts
      assert parent_accounts and len(parent_accounts) == len(sub_accounts)
      assert {a.id for a in parent_accounts}.isdisjoint(a.id for a in sub_accounts)
      # The template maps every account, for each entity against its own mapping.
      assert list_unmapped_elements(session) == []
      assert list_unmapped_elements(session, entity_id=SUB) == []
      assert get_account_rollups(session, entity_id=SUB).total_unmapped == 0

  def test_an_entry_cannot_post_to_a_siblings_account(self, charts):
    with extensions_session(GRAPH) as session:
      with pytest.raises(AccountOutsideEntityChartError):
        _revenue(session, charts, PARENT, 10_000, into=SUB)
      with pytest.raises(AccountOutsideEntityChartError):
        _revenue(session, charts, SUB, 10_000, into=PARENT)
      assert _revenue(session, charts, SUB, 10_000).id


class TestEachEntityStampsItsOwnStatements:
  @pytest.fixture(autouse=True)
  def _no_platform(self):
    with (
      patch(f"{_COMMANDS}.exclusive_period_fence"),
      patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
      patch(
        "robosystems.operations.roboledger.reads.fiscal_calendar.qb_sync_state",
        return_value=(False, None),
      ),
    ):
      yield

  def _open_books(self, session, charts, service) -> None:
    for entity_id in (PARENT, SUB):
      service.initialize(
        session, GRAPH, closed_through="2026-06", actor_id="usr_1", entity_id=entity_id
      )
      service.ensure_fiscal_periods(
        session,
        GRAPH,
        start_period="2026-06",
        end_period="2026-07",
        closed_through="2026-06",
        entity_id=entity_id,
      )
    _revenue(session, charts, PARENT, 500_000)
    _revenue(session, charts, SUB, 70_000)
    session.commit()

  def _close(self, session, service, entity_id: str):
    return close_period(
      session,
      MagicMock(),
      GRAPH,
      "2026-07",
      actor_id="usr_1",
      allow_stale_sync=False,
      note=None,
      service=service,
      close_service=PeriodCloseService(service),
      entity_id=entity_id,
    )

  def _stamped(self, session, entity_id: str, qname: str) -> list[float]:
    """The values of ``qname`` in the entity's canonical July statements."""
    return [
      value
      for (value,) in session.execute(
        select(Fact.value)
        .join(FactSet, FactSet.id == Fact.fact_set_id)
        .join(Element, Element.id == Fact.element_id)
        .where(
          FactSet.entity_id == entity_id,
          FactSet.report_id.is_(None),
          FactSet.factset_type == "report",
          FactSet.period_end == JULY_END,
          Element.qname == qname,
        )
      )
    ]

  def test_closing_the_subsidiary_stamps_only_its_own_numbers(self, charts):
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)

      response = self._close(session, service, SUB)

      assert response.statements_stamped, response.statement_stamp_note
      assert response.entries_posted == 1
      sub_revenue = self._stamped(session, SUB, REVENUE)
      assert sub_revenue and set(sub_revenue) == {700.0}
      assert set(self._stamped(session, SUB, CASH)) == {700.0}
      assert self._stamped(session, PARENT, REVENUE) == []

  def test_both_entities_close_with_their_own_statements(self, charts):
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)

      self._close(session, service, SUB)
      parent_close = self._close(session, service, PARENT)

      assert parent_close.statements_stamped, parent_close.statement_stamp_note
      assert set(self._stamped(session, PARENT, REVENUE)) == {5000.0}
      assert set(self._stamped(session, PARENT, CASH)) == {5000.0}
      # Stamping the parent left the subsidiary's statements as they were.
      assert set(self._stamped(session, SUB, REVENUE)) == {700.0}
