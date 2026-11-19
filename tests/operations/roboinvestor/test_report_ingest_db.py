"""An issuer is created and a report is ingested onto it from rows, in the
database: the receiver's side of the importer, on a provisioned tenant."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from xbrlkit.check import check_report
from xbrlkit.deserialize import from_holon_report

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.api.extensions.investor import (
  CreateIssuerRequest,
  IngestReportRequest,
)
from robosystems.models.extensions import Element, Fact, Report
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.roboinvestor.commands.reports import (
  IngestRequestError,
  IssuerAlreadyExistsError,
  create_issuer,
  ingest_report,
)
from robosystems.operations.roboledger.commands.report_import import (
  ReportImportRefusedError,
)
from robosystems.operations.roboledger.reads.reports import list_reports

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd72"
_S3 = "robosystems.operations.roboledger.commands.report_import.S3Client"


@pytest.fixture()
def fund():
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
  with db.begin() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
  try:
    provision_tenant_schema(GRAPH)
    with patch(_S3) as s3, extensions_session(GRAPH) as session:
      s3.return_value.upload_string.return_value = True
      issuer = create_issuer(
        session,
        CreateIssuerRequest(name="Acme Co", tax_id="12-3456789"),
        created_by="usr_1",
      )
      yield {"session": session, "s3": s3.return_value, "issuer": issuer}
  finally:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    db.dispose()


def _rows(*, cash: float = 600) -> dict:
  def bs(concept: str, value: float, cell: str) -> dict:
    return {
      "concept": concept,
      "value": value,
      "end": "2026-10-31",
      "unit": "USD",
      "decimals": 0,
      "source": f"package.xlsx#sheet=BS&cell={cell}",
    }

  def money(qname: str, label: str, balance: str) -> dict:
    return {
      "qname": qname,
      "label": label,
      "period_type": "instant",
      "balance": balance,
    }

  return {
    "period_end": "2026-10-31",
    "kind": "monthly-package",
    "attributed_to": "package-reader",
    "namespaces": {"kpi": "https://example.com/taxonomy/kpi/v1/"},
    "concepts": [
      {"qname": "kpi:BalanceSheetHead", "label": "Balance Sheet", "abstract": True},
      money("rs-gaap:Assets", "Total assets", "debit"),
      money("rs-gaap:Cash", "Cash", "debit"),
      money("rs-gaap:AccountsReceivableNetCurrent", "Receivables", "debit"),
      money("rs-gaap:LiabilitiesAndStockholdersEquity", "Total L&E", "credit"),
      {"qname": "kpi:KpiHead", "label": "KPIs", "abstract": True},
      {
        "qname": "kpi:Headcount",
        "label": "Headcount",
        "kind": "integer",
        "period_type": "instant",
      },
    ],
    "sections": [
      {
        "name": "Balance Sheet",
        "block_type": "balance_sheet",
        "head": "kpi:BalanceSheetHead",
        "lines": [
          {"concept": "rs-gaap:Assets"},
          {"concept": "rs-gaap:Cash", "parent": "rs-gaap:Assets", "weight": 1},
          {
            "concept": "rs-gaap:AccountsReceivableNetCurrent",
            "parent": "rs-gaap:Assets",
            "weight": 1,
          },
          {"concept": "rs-gaap:LiabilitiesAndStockholdersEquity"},
        ],
      },
      {
        "name": "KPIs",
        "head": "kpi:KpiHead",
        "lines": [{"concept": "kpi:Headcount"}],
      },
    ],
    "facts": [
      bs("rs-gaap:Assets", 1000, "B10"),
      bs("rs-gaap:Cash", cash, "B4"),
      bs("rs-gaap:AccountsReceivableNetCurrent", 400, "B5"),
      bs("rs-gaap:LiabilitiesAndStockholdersEquity", 1000, "B20"),
      {
        "concept": "kpi:Headcount",
        "value": 42,
        "end": "2026-10-31",
        "unit": "pure",
        "decimals": 0,
        "source": "package.pdf#page=3",
      },
    ],
  }


def _ingest(fund, **body):
  return ingest_report(
    fund["session"],
    IngestReportRequest.model_validate({"entity_id": fund["issuer"].id, **body}),
    created_by="usr_1",
    graph_id=GRAPH,
  )


def test_an_issuer_is_a_counterparty_not_a_member_of_the_group(fund):
  entity = fund["session"].get(Entity, fund["issuer"].id)
  assert (entity.source, entity.is_parent, entity.consolidation_method) == (
    "linked",
    False,
    "none",
  )
  assert entity.tax_id == "12-3456789"


def test_a_second_issuer_with_the_same_identifier_is_refused(fund):
  with pytest.raises(IssuerAlreadyExistsError) as raised:
    create_issuer(
      fund["session"],
      CreateIssuerRequest(name="Acme Company", tax_id="12-3456789"),
      created_by="usr_1",
    )
  assert raised.value.entity_id == fund["issuer"].id


def test_rows_become_a_stored_holon_and_then_facts(fund):
  session = fund["session"]
  result = _ingest(fund, report=_rows(), require_source=True)

  assert result.dry_run is False and result.report_id
  assert (result.fact_count, result.section_count) == (5, 2)
  assert (result.calculations_checked, result.balance_checked) == (1, True)
  assert result.concepts_added == [
    "kpi:BalanceSheetHead",
    "kpi:Headcount",
    "kpi:KpiHead",
  ]

  report = session.get(Report, result.report_id)
  assert (report.origin, report.entity_id, report.period_end) == (
    "uploaded",
    fund["issuer"].id,
    date(2026, 10, 31),
  )

  # What was stored is a holon anyone can read back: the issuer by its own
  # identifier, the style, every value with its cell, and it still checks.
  stored = fund["s3"].upload_string.call_args.kwargs["content"]
  model, _ = from_holon_report(stored)
  assert model.filing.reporting_style == "authored-package"
  assert model.filing.report_date == date(2026, 10, 31)
  assert model.entity.name == "Acme Co"
  assert check_report(model, expected_facts=5, require_source=True).ok
  sources = {f.concept_qname: f.provenance.source for f in model.facts if f.provenance}
  assert sources["rs-gaap:Cash"] == "package.xlsx#sheet=BS&cell=B4"

  rows = session.execute(
    select(Element.qname, Fact.value, Fact.unit)
    .join(Fact, Fact.element_id == Element.id)
    .join(FactSet, FactSet.id == Fact.fact_set_id)
    .where(FactSet.report_id == result.report_id)
  ).all()
  assert {(q, v, u) for q, v, u in rows} >= {
    ("rs-gaap:Assets", 1000.0, "USD"),
    ("kpi:Headcount", 42.0, "pure"),
  }


def test_an_ingested_report_is_listed_for_the_fund_and_for_its_issuer(fund):
  """The fund's own list shows what it received about a company, as it shows
  a shared report; the issuer's list shows only that company's."""
  session = fund["session"]
  session.add(
    Entity(id="ent_fund", name="Meridian Fund I", is_parent=True, created_by="usr_1")
  )
  other = create_issuer(
    session, CreateIssuerRequest(name="Borealis Ltd"), created_by="usr_1"
  )
  session.flush()
  result = _ingest(fund, report=_rows())

  listed = list_reports(session).reports
  assert [(r.id, r.origin, r.entity_name) for r in listed] == [
    (result.report_id, "uploaded", "Acme Co")
  ]
  assert [r.id for r in list_reports(session, entity_id=fund["issuer"].id).reports] == [
    result.report_id
  ]
  assert list_reports(session, entity_id=other.id).reports == []


def test_a_dry_run_checks_and_writes_nothing(fund):
  session = fund["session"]
  result = _ingest(fund, report=_rows(), dry_run=True)

  assert result.dry_run is True and result.report_id is None
  assert (result.fact_count, result.section_count) == (5, 2)
  assert result.concepts_added == [
    "kpi:BalanceSheetHead",
    "kpi:Headcount",
    "kpi:KpiHead",
  ]
  assert session.execute(select(Report.id)).scalars().all() == []
  assert (
    session.execute(select(Element.id).where(Element.qname == "kpi:Headcount"))
    .scalars()
    .all()
    == []
  )
  fund["s3"].upload_string.assert_not_called()


def test_rows_that_do_not_foot_are_refused_with_the_numbers(fund):
  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest(fund, report=_rows(cash=500))
  [problem] = refused.value.problems
  assert problem["code"] == "does_not_foot"
  assert (problem["reported"], problem["computed"]) == (1000, 900)


def test_a_row_that_cannot_be_placed_is_refused_by_name(fund):
  rows = _rows()
  rows["facts"].append(
    {"concept": "rs-gaap:Goodwill", "value": 5, "end": "2026-10-31", "unit": "USD"}
  )
  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest(fund, report=rows)
  assert "undeclared concept" in refused.value.problems[0]["message"]


def test_the_request_names_exactly_one_report(fund):
  with pytest.raises(IngestRequestError):
    _ingest(fund)
  with pytest.raises(IngestRequestError):
    _ingest(fund, report=_rows(), holon="{}")
