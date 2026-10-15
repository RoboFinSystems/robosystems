"""A journal entry's lines carry a cash flow row the way bank lines do.

Against the real extensions database with a throwaway tenant: a first-class
``flow_qname`` is checked against the entity's own cash flow statement and
reaches both lines of the entry; a posted entry's lines are re-tagged in place
through ``update-journal-entry(line_flows=...)`` with no period fence, and the
list read projects the tag.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

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
  UpdateJournalEntryRequest,
)
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  JournalEntryNotDraftError,
  create_journal_entry,
  update_journal_entry,
)
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.reads.journal_entries import (
  list_journal_entries,
)
from robosystems.operations.roboledger.reports.network_picker import (
  load_entity_reporting_style,
  taggable_flow_qnames,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd46"
PARENT = "ent_flow_parent"
_COMMANDS = "robosystems.operations.roboledger.commands.journal_entries"
_FISCAL = "robosystems.operations.roboledger.commands.fiscal_calendar"


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
          name="Flow Harbor LLC",
          entity_type="llc",
          is_parent=True,
          created_by="usr_1",
        )
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture()
def books(tenant):
  """A services chart and an open July. Returns the chart id."""
  with (
    patch(f"{_FISCAL}.exclusive_period_fence"),
    patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
  ):
    with extensions_session(GRAPH) as session:
      chart = initialize_chart_of_accounts(
        session, InitializeChartOfAccountsRequest(template="services"), "usr_1"
      )
      service = FiscalCalendarService()
      service.initialize(
        session, GRAPH, closed_through="2026-06", actor_id="usr_1", entity_id=PARENT
      )
      service.ensure_fiscal_periods(
        session,
        GRAPH,
        start_period="2026-06",
        end_period="2026-07",
        closed_through="2026-06",
        entity_id=PARENT,
      )
      session.commit()
      return chart.taxonomy_id


def _account(session, chart_id: str, code: str) -> str:
  return str(
    session.execute(
      select(Element.id).where(Element.taxonomy_id == chart_id, Element.code == code)
    ).scalar_one()
  )


def _flows(session) -> tuple[str, str, str]:
  """Two rows of this entity's cash flow statement, and one flow that is not."""
  rows = sorted(
    taggable_flow_qnames(session, load_entity_reporting_style(session, PARENT))
  )
  assert len(rows) >= 2, rows
  off = next(
    q
    for q in (
      "rs-gaap:ProceedsFromContributedCapital",
      "rs-gaap:PaymentsOfCapitalDistribution",
      "rs-gaap:ProceedsFromIssuanceOfCommonStock",
    )
    if q not in rows
  )
  return rows[0], rows[1], off


def _line_flows(session, entry_id: str) -> list[str | None]:
  return [
    (
      session.execute(
        text("SELECT qname FROM elements WHERE id = :i"), {"i": fid}
      ).scalar()
    )
    if fid
    else None
    for (fid,) in session.execute(
      select(LineItem.flow_element_id)
      .where(LineItem.entry_id == entry_id)
      .order_by(LineItem.line_order)
    )
  ]


def _note(session, chart_id: str, flow: str | None, *, status: str = "posted"):
  """Cash in against accrued liabilities, the credit line tagged."""
  return create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=date(2026, 7, 20),
      memo="Note drawn",
      status=status,
      line_items=[
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, "1000"), debit_amount=50_000
        ),
        JournalEntryLineItemInput(
          element_id=_account(session, chart_id, "2100"),
          credit_amount=50_000,
          flow_qname=flow,
        ),
      ],
    ),
    "usr_1",
    entity_id=PARENT,
  )


class TestFlowsOnJournalEntries:
  def test_a_flow_on_one_line_reaches_both_and_the_response_carries_it(self, books):
    with extensions_session(GRAPH) as session:
      flow_a, _flow_b, _off = _flows(session)

      response = _note(session, books, flow_a)

      assert [li.flow_qname for li in response.line_items] == [flow_a, flow_a]
      assert _line_flows(session, response.id) == [flow_a, flow_a]

  def test_a_flow_off_the_statement_is_refused_with_its_rows(self, books):
    with extensions_session(GRAPH) as session:
      _a, _b, off = _flows(session)

      with pytest.raises(HandlerMetadataValidationError, match="cash flow statement"):
        _note(session, books, off)

  def test_a_posted_entry_is_re_tagged_in_place_past_the_period_fence(self, books):
    """The closed-month fence would refuse any edit here; a flow is not an
    edit of the books, so it goes through, both lines follow, and the entry
    records the change."""
    with extensions_session(GRAPH) as session:
      flow_a, flow_b, _off = _flows(session)
      posted = _note(session, books, flow_a)
      credit_line = posted.line_items[1].id

      with patch(
        f"{_COMMANDS}.assert_period_not_closed",
        side_effect=ClosedPeriodError("2026-07", "closed"),
      ):
        retagged = update_journal_entry(
          session,
          UpdateJournalEntryRequest(
            entry_id=posted.id,
            line_flows=[{"line_id": credit_line, "flow_qname": flow_b}],
          ),
        )
        cleared = update_journal_entry(
          session,
          UpdateJournalEntryRequest(
            entry_id=posted.id,
            line_flows=[{"line_id": credit_line, "flow_qname": None}],
          ),
        )

      assert [li.flow_qname for li in retagged.line_items] == [flow_b, flow_b]
      assert [li.flow_qname for li in cleared.line_items] == [None, None]
      history = session.execute(
        text("SELECT metadata -> 'flow_retags' FROM entries WHERE id = :i"),
        {"i": posted.id},
      ).scalar()
      assert [(h["line_ids"], h["flows"]) for h in history] == [
        ([credit_line], [flow_b]),
        ([credit_line], []),
      ]

  def test_the_list_read_projects_the_tag(self, books):
    with extensions_session(GRAPH) as session:
      flow_a, _b, _off = _flows(session)
      posted = _note(session, books, flow_a)

      listed = list_journal_entries(session, entity_id=PARENT, status="posted")

      entry = next(e for e in listed.entries if e.id == posted.id)
      assert [li.flow_qname for li in entry.line_items] == [flow_a, flow_a]

  def test_anything_but_flows_on_a_posted_entry_is_still_refused(self, books):
    with extensions_session(GRAPH) as session:
      flow_a, _b, _off = _flows(session)
      posted = _note(session, books, flow_a)

      with pytest.raises(JournalEntryNotDraftError):
        update_journal_entry(
          session,
          UpdateJournalEntryRequest(
            entry_id=posted.id,
            memo="A new memo",
            line_flows=[{"line_id": posted.line_items[1].id, "flow_qname": None}],
          ),
        )
