"""A shared report lands in the recipient as the sender's rows, read from the
holon.

The round trip on two provisioned tenants: real books, a real report, its
holon, the share. The recipient's facts must equal the sender's apart from
their ids, on the same library statements, and a re-share or a revoke must
leave nothing of the earlier copy behind.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.api.extensions.entity import CreateEntityRequest
from robosystems.models.api.extensions.reports import CreateReportRequest, PeriodSpec
from robosystems.models.extensions import Element, Fact, Report, Structure
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.entity import create_entity
from robosystems.operations.roboledger.commands.reports import (
  _delete_shared_copy,
  _share_to_target,
  create_report,
)
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.reads.reports import get_report_package
from robosystems.operations.serialization import (
  build_report_bundle,
  serialize_to_holon_jsonld,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)
from tests.tenant_schema import claim_provisioned_schema, release_tenant_schema

pytestmark = pytest.mark.integration

SOURCE = "kgffffffffffffffff81"
TARGET = "kgffffffffffffffff82"
JUNE = (date(2026, 6, 1), date(2026, 6, 30))
_REPORTS = "robosystems.operations.roboledger.commands.reports"


@pytest.fixture()
def shared():
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
    claim_provisioned_schema(SOURCE)
    claim_provisioned_schema(TARGET)
    with (
      patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
      patch(f"{_REPORTS}._stamp_report_bundle"),
      patch(f"{_REPORTS}.S3Client"),
      patch(f"{_REPORTS}.delete_report_artifacts"),
      extensions_session(SOURCE) as session,
    ):
      yield _publish(session)
  finally:
    with db.begin() as conn:
      release_tenant_schema(conn, SOURCE)
      release_tenant_schema(conn, TARGET)
    db.dispose()


def _publish(session) -> dict:
  """June's books for one company, its report, and the report's holon."""
  entity_id = create_entity(
    session, CreateEntityRequest(name="Harbor Holdings", entity_type="llc"), "usr_1"
  ).id
  chart = initialize_chart_of_accounts(
    session,
    InitializeChartOfAccountsRequest(template="services", entity_id=entity_id),
    "usr_1",
  ).taxonomy_id
  service = FiscalCalendarService()
  service.initialize(
    session, SOURCE, closed_through="2026-05", actor_id="usr_1", entity_id=entity_id
  )
  service.ensure_fiscal_periods(
    session,
    SOURCE,
    start_period="2026-05",
    end_period="2026-07",
    closed_through="2026-05",
    entity_id=entity_id,
  )
  session.commit()
  _post(
    session,
    entity_id,
    chart,
    date(2026, 6, 10),
    [("1000", 100_000), ("4000", -100_000)],
  )
  mapping = find_entity_mapping(session, entity_id, BOOK_FRAMEWORK)
  report = create_report(
    session,
    CreateReportRequest(
      name="June",
      mapping_id=str(mapping.id),
      period_start=JUNE[0],
      period_end=JUNE[1],
      period_type="monthly",
      comparative=False,
      periods=[PeriodSpec(start=JUNE[0], end=JUNE[1], label="June")],
    ),
    graph_id=SOURCE,
    created_by="usr_1",
  )
  # The bundle reads the framework pin off the platform Graph row, which this
  # throwaway tenant has none of.
  platform = MagicMock()
  platform.__enter__.return_value.query.return_value.filter.return_value.first.return_value = MagicMock()
  with (
    patch("robosystems.database.platform_session", return_value=platform),
    patch("robosystems.taxonomy.pins.resolve_pin", return_value={"rs-gaap": "v1"}),
  ):
    holon = serialize_to_holon_jsonld(build_report_bundle(session, SOURCE, report.id))
  source = session.get(Report, report.id)
  return {
    "entity": entity_id,
    "report": report.id,
    "holon": holon,
    "snapshot": {
      "id": source.id,
      "name": source.name,
      "description": source.description,
      "taxonomy_id": source.taxonomy_id,
      "mapping_id": source.mapping_id,
      "period_type": source.period_type,
      "period_start": source.period_start,
      "period_end": source.period_end,
      "comparative": source.comparative,
      "periods": source.periods,
      "generation_count": 1,
      "filing_status": source.filing_status,
      "filed_at": source.filed_at,
    },
  }


def _post(session, entity_id, chart, on: date, lines: list[tuple[str, int]]) -> None:
  event = Event(
    entity_id=entity_id,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    occurred_at=datetime(on.year, on.month, on.day, tzinfo=UTC),
    status="fulfilled",
    source="manual",
    created_by="test",
  )
  session.add(event)
  session.flush()
  entry = Entry(
    entity_id=entity_id,
    status="posted",
    posting_date=on,
    triggered_by_event_id=event.id,
    created_by="test",
  )
  session.add(entry)
  session.flush()
  for order, (code, amount) in enumerate(lines, 1):
    account = session.execute(
      select(Element.id).where(Element.taxonomy_id == chart, Element.code == code)
    ).scalar_one()
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=account,
        debit_amount=max(amount, 0),
        credit_amount=max(-amount, 0),
        line_order=order,
      )
    )
  session.commit()


def _share(g):
  target = MagicMock()
  target.schema_extensions = ["roboledger"]
  platform = MagicMock()
  platform.__enter__.return_value.execute.return_value.scalar_one_or_none.return_value = target
  with patch("robosystems.db.platform.SessionFactory", return_value=platform):
    return _share_to_target(
      source_graph_id=SOURCE,
      report_snapshot=g["snapshot"],
      target_graph_id=TARGET,
      shared_by="usr_1",
      publication_artifacts={".holon.jsonld": g["holon"]},
      source_entity_id=g["entity"],
    )


def _rows(graph_id: str, report_id: str) -> Counter:
  """Each fact as the reader sees it: concept, value, period and statement.
  An instant is its date; the ledger also stamps the report's start on it."""
  with extensions_session(graph_id) as session:
    rows = session.execute(
      select(
        Element.qname,
        Fact.value,
        Fact.string_value,
        Fact.period_type,
        Fact.period_start,
        Fact.period_end,
        FactSet.structure_id,
      )
      .join(FactSet, FactSet.id == Fact.fact_set_id)
      .join(Element, Element.id == Fact.element_id)
      .where(FactSet.report_id == report_id)
    ).all()
  return Counter(
    (
      q,
      None if v is None else round(float(v), 2),
      s or None,
      None if kind == "instant" else start,
      end,
      sid,
    )
    for q, v, s, kind, start, end, sid in rows
  )


def _copy_id() -> str:
  with extensions_session(TARGET) as session:
    return session.execute(select(Report.id)).scalar_one()


def _received_structures() -> int:
  with extensions_session(TARGET) as session:
    return len(
      session.execute(
        select(Structure.id).where(Structure.metadata_["received"].astext == "true")
      ).all()
    )


def test_the_copy_holds_the_senders_rows_on_the_same_statements(shared) -> None:
  g = shared
  result = _share(g)
  assert result.status == "shared", result.error

  copy = _copy_id()
  source_rows = _rows(SOURCE, g["report"])
  assert source_rows
  assert _rows(TARGET, copy) == source_rows

  with extensions_session(SOURCE) as session:
    sent = get_report_package(session, g["report"])
  with extensions_session(TARGET) as session:
    received = get_report_package(session, copy)
  assert sent is not None and received is not None
  assert [i.structure_id for i in received.items] == [
    i.structure_id for i in sent.items
  ]
  # The sender's statements are library ones, so the copy brought none.
  assert _received_structures() == 0

  # The copy names where its facts came from.
  with extensions_session(TARGET) as session:
    provenance = {
      (p["origin"], p["source_system"], p["basis_note"])
      for p in session.execute(
        select(Fact.provenance)
        .join(FactSet, FactSet.id == Fact.fact_set_id)
        .where(FactSet.report_id == copy)
      ).scalars()
    }
  assert provenance == {
    (
      "asserted",
      "cross_graph_share",
      f"source_graph={SOURCE} source_report={g['report']}",
    )
  }


def test_a_reshare_replaces_the_copy(shared) -> None:
  g = shared
  assert _share(g).status == "shared"
  first = _copy_id()
  assert _share(g).status == "shared"
  second = _copy_id()

  assert second != first
  assert _rows(TARGET, second) == _rows(SOURCE, g["report"])


def test_a_revoke_leaves_nothing_of_the_copy(shared) -> None:
  g = shared
  assert _share(g).status == "shared"
  assert _delete_shared_copy(SOURCE, g["report"], TARGET, "usr_1") is True

  with extensions_session(TARGET) as session:
    assert session.execute(select(Report.id)).first() is None
    assert session.execute(select(FactSet.id)).first() is None
  assert _received_structures() == 0


def test_a_share_without_its_holon_sends_nothing(shared) -> None:
  result = _share_to_target(
    source_graph_id=SOURCE,
    report_snapshot=shared["snapshot"],
    target_graph_id=TARGET,
    shared_by="usr_1",
    publication_artifacts={},
    source_entity_id=shared["entity"],
  )
  assert result.status == "error"
  with extensions_session(TARGET) as session:
    assert session.execute(select(Report.id)).first() is None


def test_a_share_that_names_no_company_sends_nothing(shared) -> None:
  """Every fact needs the company it is about; a copy without one is refused
  rather than written with an empty entity."""
  target = MagicMock()
  target.schema_extensions = ["roboledger"]
  platform = MagicMock()
  platform.__enter__.return_value.execute.return_value.scalar_one_or_none.return_value = target
  with (
    patch("robosystems.db.platform.SessionFactory", return_value=platform),
    patch(f"{_REPORTS}._ensure_linked_entity", return_value=None),
  ):
    result = _share_to_target(
      source_graph_id=SOURCE,
      report_snapshot=shared["snapshot"],
      target_graph_id=TARGET,
      shared_by="usr_1",
      publication_artifacts={".holon.jsonld": shared["holon"]},
      source_entity_id=None,
    )
  assert result.status == "error"
  with extensions_session(TARGET) as session:
    assert session.execute(select(Report.id)).first() is None


def test_a_holon_with_nothing_to_load_sends_nothing(shared) -> None:
  from robosystems.operations.roboledger.commands import report_import

  with patch.object(report_import, "_loadable", return_value=[]):
    result = _share(shared)
  assert result.status == "error"
  with extensions_session(TARGET) as session:
    assert session.execute(select(Report.id)).first() is None
