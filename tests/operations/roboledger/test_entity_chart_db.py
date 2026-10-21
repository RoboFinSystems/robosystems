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
from robosystems.models.api.extensions.entity import (
  CreateEntityRequest,
  UpdateEntityRequest,
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
from robosystems.operations.roboledger.commands.entity import (
  create_entity,
  update_entity,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  close_period,
  reopen_period,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads.account_rollups import get_account_rollups
from robosystems.operations.roboledger.reads.accounts import list_accounts
from robosystems.operations.roboledger.reads.reports import (
  CombinedViewOnParentOnlyError,
  get_consolidation_worksheet,
  get_live_financial_statement,
)
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
          consolidation_method="full",
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


def _entry(
  session,
  charts,
  entity_id: str,
  debit_code: str,
  credit_code: str,
  cents: int,
  memo: str,
):
  """Draft one two-line entry in ``entity_id``'s books, on its own chart."""
  chart_id = charts[entity_id]
  return create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=date(2026, 7, 20),
      memo=memo,
      line_items=[
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, debit_code), debit_amount=cents
        ),
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, credit_code), credit_amount=cents
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

  def _stamped(
    self, session, entity_id: str, qname: str, *, retracted: bool = False
  ) -> list[float]:
    """The values of ``qname`` in the entity's canonical July statements —
    the current ones, or with ``retracted`` the ones a reopen took back."""
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
          FactSet.retracted_at.is_not(None)
          if retracted
          else FactSet.retracted_at.is_(None),
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

  def _live_cash(self, session, **kwargs) -> tuple[float | None, object]:
    response = get_live_financial_statement(
      session,
      graph_id=GRAPH,
      statement_type="balance_sheet",
      period_start=date(2026, 7, 1),
      period_end=JULY_END,
      **kwargs,
    )
    july = next(i for i, p in enumerate(response.periods) if p.end == JULY_END)
    cash = {fact.qname: fact.values[july] for fact in response.facts}.get(CASH)
    return cash, response

  def test_the_combined_statement_presents_a_subsidiarys_own_concepts(self, charts):
    """Two rows only the subsidiary carries: a bill payable, which the parent's
    Style has a row for but the parent's own sheet dropped as all-zero, and
    members' equity, which the corporate Style has no row for at all. Both
    take a row under their subtotal in the combined view, and the view foots
    instead of warning that two subtotals exceed their children."""
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)
      _entry(session, charts, SUB, "1000", "3000", 10_000, "Member contribution")
      _entry(session, charts, SUB, "6000", "2000", 2_500, "July rent, unpaid")
      session.commit()
      self._close(session, service, SUB)
      self._close(session, service, PARENT)

      _, response = self._live_cash(session, consolidated=True)

      july = next(i for i, p in enumerate(response.periods) if p.end == JULY_END)
      order = [fact.qname for fact in response.facts]
      by_qname = {fact.qname: fact for fact in response.facts}
      payable = by_qname["rs-gaap:AccountsPayableCurrent"]
      current = by_qname["rs-gaap:LiabilitiesCurrent"]
      members = by_qname["rs-gaap:MembersEquity"]
      equity = by_qname["rs-gaap:StockholdersEquity"]

      assert order.index(payable.qname) < order.index(current.qname)
      assert (payable.depth, payable.values[july]) == (current.depth + 1, 25.0)
      assert order.index(members.qname) < order.index(equity.qname)
      assert members.depth == equity.depth + 1
      assert members.values[july] == 100.0
      # The books start in July, so the prior column is empty; nothing else.
      assert [w for w in response.validation.warnings if "Prior" not in w] == []

  def test_the_parents_combined_statement_sums_the_group(self, charts):
    """The combined view is the one new computation of M1: each entity's
    statement under the parent's Style, summed at the rs-gaap concepts."""
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)
      self._close(session, service, SUB)
      self._close(session, service, PARENT)

      alone, _ = self._live_cash(session)
      combined, response = self._live_cash(session, consolidated=True)
      sub_alone, _ = self._live_cash(session, entity_id=SUB)

      assert (alone, sub_alone, combined) == (5000.0, 700.0, 5700.0)
      assert response.consolidated is True
      assert response.entity_id == PARENT
      assert response.combined_entity_ids == [PARENT, SUB]
      with pytest.raises(CombinedViewOnParentOnlyError):
        self._live_cash(session, entity_id=SUB, consolidated=True)

  def _open_member(self, session, charts, service, body) -> str:
    """One more entity with a services chart and July open, as the fixture's
    two have; returns its id."""
    created = create_entity(session, body, "usr_1")
    charts[created.id] = initialize_chart_of_accounts(
      session,
      InitializeChartOfAccountsRequest(template="services", entity_id=created.id),
      "usr_1",
    ).taxonomy_id
    service.initialize(
      session, GRAPH, closed_through="2026-06", actor_id="usr_1", entity_id=created.id
    )
    service.ensure_fiscal_periods(
      session,
      GRAPH,
      start_period="2026-06",
      end_period="2026-07",
      closed_through="2026-06",
      entity_id=created.id,
    )
    return created.id

  def test_combined_is_the_sum_before_the_consolidation_entity(self, charts):
    """``combined`` is the worksheet's first total; ``consolidated`` adds the
    consolidation entity, the adjustments column, and the older
    ``consolidated=True`` reads the same."""
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)
      adjustments = self._open_member(
        session,
        charts,
        service,
        CreateEntityRequest(
          name="Consolidation Adjustments", entity_role="consolidation"
        ),
      )
      _revenue(session, charts, adjustments, 30_000)
      session.commit()
      self._close(session, service, SUB)
      self._close(session, service, PARENT)
      self._close(session, service, adjustments)

      combined, combined_response = self._live_cash(session, scope="combined")
      consolidated, response = self._live_cash(session, scope="consolidated")
      older, _ = self._live_cash(session, consolidated=True)

      assert (combined, consolidated, older) == (5700.0, 6000.0, 6000.0)
      assert (combined_response.scope, combined_response.consolidated) == (
        "combined",
        True,
      )
      assert combined_response.combined_entity_ids == [PARENT, SUB]
      assert response.scope == "consolidated"
      assert response.combined_entity_ids == [PARENT, SUB, adjustments]

      # The group close stamps the consolidated statements on the
      # consolidation entity; the parent's own stamp keeps its own books.
      assert self._stamped(session, adjustments, CASH) == [6000.0]
      assert self._stamped(session, PARENT, CASH) == [5000.0]

      # Reopening the group's month takes its stamp back, and only its own.
      reopen_period(
        session,
        MagicMock(),
        GRAPH,
        "2026-07",
        actor_id="usr_1",
        reason="restate",
        note=None,
        service=service,
        entity_id=adjustments,
      )
      assert self._stamped(session, adjustments, CASH) == []
      assert self._stamped(session, PARENT, CASH) == [5000.0]
      # The stamp it took back is kept, as the month as it was closed.
      assert self._stamped(session, adjustments, CASH, retracted=True) == [6000.0]

  def test_an_equity_method_member_is_not_summed(self, charts):
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)
      self._close(session, service, SUB)
      self._close(session, service, PARENT)
      update_entity(
        session, UpdateEntityRequest(entity_id=SUB, ownership_pct=40), "usr_1"
      )

      cash, response = self._live_cash(session, scope="consolidated")

      assert cash == 5000.0
      assert response.combined_entity_ids == [PARENT]

  def test_a_member_is_summed_for_the_months_inside_its_interval(self, charts):
    """Any day of the month a member left still counts that month; a month
    after it adds nothing, and a member outside every column is not listed."""
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)
      self._close(session, service, SUB)
      self._close(session, service, PARENT)

      update_entity(
        session,
        UpdateEntityRequest(entity_id=SUB, left_on=date(2026, 7, 10)),
        "usr_1",
      )
      still, _ = self._live_cash(session, scope="combined")
      update_entity(
        session,
        UpdateEntityRequest(entity_id=SUB, left_on=date(2026, 6, 30)),
        "usr_1",
      )
      gone, response = self._live_cash(session, scope="combined")
      update_entity(
        session,
        UpdateEntityRequest(entity_id=SUB, left_on=date(2026, 5, 31)),
        "usr_1",
      )
      _, before = self._live_cash(session, scope="combined")

      assert (still, gone) == (5700.0, 5000.0)
      # June, the prior column, still counts it; from May it is in neither.
      assert response.combined_entity_ids == [PARENT, SUB]
      assert before.combined_entity_ids == [PARENT]

  def test_the_worksheet_lays_the_group_out_by_entity(self, charts):
    """Each member's own number, the combined sum, the adjustments, the
    consolidated total, and an equity-method member beside the sum: the
    totals are the two group scopes' numbers."""
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)
      adjustments = self._open_member(
        session,
        charts,
        service,
        CreateEntityRequest(
          name="Consolidation Adjustments", entity_role="consolidation"
        ),
      )
      kiosk = self._open_member(
        session,
        charts,
        service,
        CreateEntityRequest(name="Pier Market Kiosk LLC", ownership_pct=40),
      )
      _revenue(session, charts, adjustments, 30_000)
      _revenue(session, charts, kiosk, 20_000)
      # The adjustments column, posted: the group close waits on entries this
      # chart cannot post (it has no equity-method accounts), and this reads
      # the worksheet, not the close.
      session.execute(
        text("UPDATE entries SET status = 'posted' WHERE entity_id = :e"),
        {"e": adjustments},
      )
      session.commit()
      for entity_id in (SUB, kiosk, PARENT):
        self._close(session, service, entity_id)

      worksheet = get_consolidation_worksheet(
        session,
        graph_id=GRAPH,
        statement_type="balance_sheet",
        period_start=date(2026, 7, 1),
        period_end=JULY_END,
      )
      _, consolidated = self._live_cash(session, scope="consolidated")

      assert [(c.key, c.kind, c.in_sum) for c in worksheet.columns] == [
        (PARENT, "member", True),
        (SUB, "member", True),
        ("combined", "combined", True),
        (adjustments, "adjustments", True),
        ("consolidated", "consolidated", True),
        (kiosk, "not_summed", False),
      ]
      kiosk_column = worksheet.columns[-1]
      assert (kiosk_column.consolidation_method, kiosk_column.ownership_pct) == (
        "equity",
        40.0,
      )
      cash = next(r for r in worksheet.rows if r.qname == CASH)
      assert cash.values == [5000.0, 700.0, 5700.0, 300.0, 6000.0, 200.0]
      assert worksheet.entity_id == PARENT
      assert worksheet.validation is not None
      assert worksheet.validation.status == consolidated.validation.status

  def test_the_worksheet_leaves_out_who_is_not_there(self, charts):
    """A member that left before the window has no column, an entity with
    no mapping is named instead of failing the read, and a group with no
    consolidation entity has no adjustments column: consolidated is
    combined."""
    service = FiscalCalendarService()
    with extensions_session(GRAPH) as session:
      self._open_books(session, charts, service)
      unmapped = create_entity(
        session, CreateEntityRequest(name="Unmapped LLC", ownership_pct=100), "usr_1"
      ).id
      self._close(session, service, SUB)
      self._close(session, service, PARENT)
      update_entity(
        session,
        UpdateEntityRequest(entity_id=SUB, left_on=date(2026, 6, 30)),
        "usr_1",
      )

      worksheet = get_consolidation_worksheet(
        session,
        graph_id=GRAPH,
        statement_type="balance_sheet",
        period_start=date(2026, 7, 1),
        period_end=JULY_END,
      )

      assert [(c.key, c.kind) for c in worksheet.columns] == [
        (PARENT, "member"),
        ("combined", "combined"),
        ("consolidated", "consolidated"),
      ]
      assert worksheet.unmapped_entity_ids == [unmapped]
      cash = next(r for r in worksheet.rows if r.qname == CASH)
      assert cash.values == [5000.0, 5000.0, 5000.0]


class TestMembershipWindows:
  """A window wider than a month counts each member for its own months: a
  balance sheet holds the members of the month it is struck in, an income
  statement sums each member's months, and a cash flow or equity statement a
  member joined or left within is left out, on the live statement and the
  worksheet alike."""

  JUNE_JULY = (date(2026, 6, 1), JULY_END)

  @pytest.fixture(autouse=True)
  def _no_platform(self):
    with patch(
      "robosystems.operations.roboledger.reads.fiscal_calendar.qb_sync_state",
      return_value=(False, None),
    ):
      yield

  def _posted(self, session, charts, entity_id, on: date, cents: int) -> None:
    chart_id = charts[entity_id]
    create_journal_entry(
      session,
      CreateJournalEntryRequest(
        posting_date=on,
        memo="Consulting fees received",
        status="posted",
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

  def _june_and_july(self, session, charts) -> None:
    """The parent earns 1,000 in June and 2,000 in July; the member 300 and
    400, and leaves the group at June's end."""
    service = FiscalCalendarService()
    for entity_id in (PARENT, SUB):
      service.initialize(
        session, GRAPH, closed_through="2026-05", actor_id="usr_1", entity_id=entity_id
      )
      service.ensure_fiscal_periods(
        session,
        GRAPH,
        start_period="2026-05",
        end_period="2026-07",
        closed_through="2026-05",
        entity_id=entity_id,
      )
    self._posted(session, charts, PARENT, date(2026, 6, 10), 100_000)
    self._posted(session, charts, PARENT, date(2026, 7, 10), 200_000)
    self._posted(session, charts, SUB, date(2026, 6, 10), 30_000)
    self._posted(session, charts, SUB, date(2026, 7, 10), 40_000)
    session.commit()
    update_entity(
      session, UpdateEntityRequest(entity_id=SUB, left_on=date(2026, 6, 30)), "usr_1"
    )

  def _live(self, session, statement_type: str):
    start, end = self.JUNE_JULY
    return get_live_financial_statement(
      session,
      graph_id=GRAPH,
      statement_type=statement_type,
      period_start=start,
      period_end=end,
      scope="combined",
    )

  def _worksheet(self, session, statement_type: str):
    start, end = self.JUNE_JULY
    return get_consolidation_worksheet(
      session,
      graph_id=GRAPH,
      statement_type=statement_type,
      period_start=start,
      period_end=end,
    )

  @staticmethod
  def _current(response, qname: str) -> float:
    return {f.qname: f.values[0] for f in response.facts}[qname]

  def test_a_member_that_left_is_off_the_window_end_balance_sheet(self, charts):
    with extensions_session(GRAPH) as session:
      self._june_and_july(session, charts)
      assert self._current(self._live(session, "balance_sheet"), CASH) == 3000.0

  def test_the_income_statement_sums_only_the_members_own_months(self, charts):
    with extensions_session(GRAPH) as session:
      self._june_and_july(session, charts)
      live = self._live(session, "income_statement")
      worksheet = self._worksheet(session, "income_statement")

    assert self._current(live, REVENUE) == 3300.0
    revenue = next(r for r in worksheet.rows if r.qname == REVENUE)
    by_key = dict(zip([c.key for c in worksheet.columns], revenue.values, strict=True))
    assert (by_key[PARENT], by_key[SUB], by_key["combined"]) == (3000.0, 300.0, 3300.0)

  def test_a_cash_flow_the_member_left_within_is_left_out(self, charts):
    with extensions_session(GRAPH) as session:
      self._june_and_july(session, charts)
      live = self._live(session, "cash_flow_statement")
      worksheet = self._worksheet(session, "cash_flow_statement")

    assert [p.label for p in live.omitted_periods] == ["Current"]
    assert [p.label for p in live.periods] == ["Prior"]
    assert [p.label for p in worksheet.omitted_periods] == ["Current"]
    assert worksheet.rows == []
    assert worksheet.validation.status == "inconclusive"

  def _late_member(self, session, charts, *, joined_on: date | None) -> str:
    """A member whose books open on July 1 from an opening balance and that
    earns 100 in July."""
    from robosystems.models.api.extensions.journal_entries import (
      OpeningBalanceInput,
      RecordOpeningBalanceRequest,
    )
    from robosystems.operations.roboledger.commands.opening_balances import (
      record_opening_balance,
    )

    service = FiscalCalendarService()
    late = create_entity(
      session,
      CreateEntityRequest(name="Late LLC", ownership_pct=100, joined_on=joined_on),
      "usr_1",
    ).id
    charts[late] = initialize_chart_of_accounts(
      session,
      InitializeChartOfAccountsRequest(template="services", entity_id=late),
      "usr_1",
    ).taxonomy_id
    service.initialize(session, GRAPH, actor_id="usr_1", entity_id=late)
    service.ensure_fiscal_periods(
      session, GRAPH, start_period="2026-07", end_period="2026-07", entity_id=late
    )
    record_opening_balance(
      session,
      RecordOpeningBalanceRequest(
        entity_id=late,
        balances=[
          OpeningBalanceInput(
            element_id=_account(session, charts[late], "1000"), debit_amount=50_000
          ),
          OpeningBalanceInput(
            element_id=_account(session, charts[late], "3000"), credit_amount=50_000
          ),
        ],
      ),
      "usr_1",
    )
    self._posted(session, charts, late, date(2026, 7, 12), 10_000)
    session.commit()
    return late

  def test_a_member_acquired_with_an_opening_balance_adds_its_months(self, charts):
    """Joined July 1 with books from that day: the June-July income
    statement adds its July, instead of being left out for reaching back
    before its books."""
    with extensions_session(GRAPH) as session:
      self._june_and_july(session, charts)
      self._late_member(session, charts, joined_on=date(2026, 7, 1))
      live = self._live(session, "income_statement")
      worksheet = self._worksheet(session, "income_statement")

    assert live.omitted_periods == []
    assert self._current(live, REVENUE) == 3400.0
    assert worksheet.omitted_periods == []
    revenue = next(r for r in worksheet.rows if r.qname == REVENUE)
    combined = [c.key for c in worksheet.columns].index("combined")
    assert revenue.values[combined] == 3400.0

  def test_the_worksheet_leaves_out_a_window_before_a_members_books(self, charts):
    """A member whose books open on July 1 from an opening balance, a member
    since before then: the June-July income statement is left out of the
    worksheet, as the live statement leaves it out, instead of totalling the
    parent's months alone."""
    with extensions_session(GRAPH) as session:
      self._june_and_july(session, charts)
      self._late_member(session, charts, joined_on=None)
      live = self._live(session, "income_statement")
      worksheet = self._worksheet(session, "income_statement")

    assert [p.label for p in live.omitted_periods] == ["Current", "Prior"]
    assert worksheet.omitted_periods == live.omitted_periods
    assert worksheet.rows == []
