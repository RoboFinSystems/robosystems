"""An issuer is created and a report is ingested onto it from rows, in the
database: the receiver's side of the importer, on a provisioned tenant."""

from __future__ import annotations

import copy
from datetime import date, timedelta
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from xbrlkit.check import check_report
from xbrlkit.deserialize import from_holon_report
from xbrlkit.serialize import to_tavi

from robosystems.config import env
from robosystems.db.extensions import extensions_session
from robosystems.models.api.extensions.investor import (
  MAX_REPORT_FACTS,
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
  ReportImportEntityNotFoundError,
  ReportImportRefusedError,
)
from robosystems.operations.roboledger.reads.reports import list_reports
from tests.tenant_schema import claim_provisioned_schema, release_tenant_schema

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
  try:
    claim_provisioned_schema(GRAPH)
    with (
      patch(_S3) as s3,
      patch(
        "robosystems.operations.roboledger.commands.reports.delete_report_artifacts"
      ),
      extensions_session(GRAPH) as session,
    ):
      s3.return_value.upload_string.return_value = True
      issuer = create_issuer(
        session,
        CreateIssuerRequest(name="Acme Co", tax_id="12-3456789"),
        created_by="usr_1",
      )
      yield {"session": session, "s3": s3.return_value, "issuer": issuer}
  finally:
    with db.begin() as conn:
      release_tenant_schema(conn, GRAPH)
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
  # The date and the lines counted toward the total come with it.
  assert problem["when"] == "2026-10-31"
  assert problem["terms"] == [
    {"concept": "rs-gaap:Cash", "weight": 1.0, "value": 500.0},
    {"concept": "rs-gaap:AccountsReceivableNetCurrent", "weight": 1.0, "value": 400.0},
  ]


def test_a_row_that_cannot_be_placed_is_refused_by_name(fund):
  rows = _rows()
  rows["facts"].append(
    {"concept": "kpi:Backlog", "value": 5, "end": "2026-10-31", "unit": "USD"}
  )
  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest(fund, report=rows)
  assert "undeclared concept" in refused.value.problems[0]["message"]


def test_a_library_concept_needs_no_declaration_and_an_unplaced_one_is_named(fund):
  rows = _rows()
  rows["facts"].append(
    {"concept": "rs-gaap:Goodwill", "value": 5, "end": "2026-10-31", "unit": "USD"}
  )
  result = _ingest(fund, report=rows, dry_run=True)
  assert result.fact_count == 6
  assert result.unplaced_concepts == ["rs-gaap:Goodwill"]


def test_rows_for_an_entity_outside_the_graph_are_refused_before_anything_is_built(
  fund,
):
  with pytest.raises(ReportImportEntityNotFoundError):
    ingest_report(
      fund["session"],
      IngestReportRequest.model_validate(
        {"entity_id": "ent_nobody", "report": _rows()}
      ),
      created_by="usr_1",
      graph_id=GRAPH,
    )


def test_a_report_is_not_loaded_onto_one_of_the_graphs_own_entities(fund):
  session = fund["session"]
  own = Entity(id="ent_fund", name="Meridian Fund I", is_parent=True, created_by="u")
  session.add(own)
  session.flush()

  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest({**fund, "issuer": own}, report=_rows(), dry_run=True)

  assert refused.value.problems[0]["code"] == "own_entity"
  assert "create-issuer" in refused.value.problems[0]["message"]


def test_an_issuer_the_fund_holds_no_position_in_still_takes_a_report(fund):
  # The fixture's issuer has no security and no position.
  assert _ingest(fund, report=_rows()).report_id


def test_the_request_names_exactly_one_report(fund):
  with pytest.raises(IngestRequestError):
    _ingest(fund)
  with pytest.raises(IngestRequestError):
    _ingest(fund, report=_rows(), holon="{}")


def _values_only(*, opex: float = 300, **extra_sections) -> dict:
  """A package that sends values and names its statements, nothing else."""

  def value(concept: str, amount: float, *, flow: bool = False) -> dict:
    row = {
      "concept": concept,
      "value": amount,
      "end": "2026-10-31",
      "unit": "USD",
      "decimals": 0,
    }
    if flow:
      row["start"] = "2026-10-01"
    return row

  return {
    "period_end": "2026-10-31",
    "kind": "monthly-package",
    "sections": [
      {"name": "Balance Sheet", "block_type": "balance_sheet"},
      {"name": "Income Statement", "block_type": "income_statement"},
      *extra_sections.get("sections", []),
    ],
    "facts": [
      value("rs-gaap:Assets", 1000),
      value("rs-gaap:AssetsCurrent", 700),
      value("rs-gaap:CashAndCashEquivalentsAtCarryingValue", 600),
      value("rs-gaap:AccountsReceivableNetCurrent", 100),
      value("rs-gaap:PropertyPlantAndEquipmentNet", 300),
      value("rs-gaap:LiabilitiesAndStockholdersEquity", 1000),
      value("rs-gaap:Liabilities", 400),
      value("rs-gaap:StockholdersEquity", 600),
      value("rs-gaap:Revenues", 900, flow=True),
      value("rs-gaap:CostOfRevenue", 400, flow=True),
      value("rs-gaap:GrossProfit", 500, flow=True),
      value("rs-gaap:OperatingExpenses", opex, flow=True),
      value("rs-gaap:InterestExpense", 50, flow=True),
      value("rs-gaap:NetIncomeLoss", 150, flow=True),
    ],
  }


def test_statements_sent_as_values_are_laid_out_from_the_library(fund):
  response = ingest_report(
    fund["session"],
    IngestReportRequest(entity_id=fund["issuer"].id, report=_values_only()),
    created_by="usr_1",
    graph_id=GRAPH,
  )

  assert response.fact_count == 14
  assert response.section_count == 2
  assert response.balance_checked
  # Assets, current assets, liabilities and equity, gross profit, net income.
  assert response.calculations_checked == 5
  assert response.unplaced_concepts == []

  holon = fund["s3"].upload_string.call_args.kwargs["content"]
  model, _ = from_holon_report(holon)
  arcs = {
    (arc.from_qname, arc.to_qname): arc.weight
    for network in model.networks
    if network.kind == "calculation"
    for arc in network.arcs
  }
  # Receivables hang from current assets, the nearest line with a value.
  assert arcs[("rs-gaap:AssetsCurrent", "rs-gaap:AccountsReceivableNetCurrent")] == 1
  # With no noncurrent subtotal sent, equipment adds straight into assets.
  assert arcs[("rs-gaap:Assets", "rs-gaap:PropertyPlantAndEquipmentNet")] == 1
  # Subtotals the package leaves out are stepped over, signs carried.
  assert arcs[("rs-gaap:NetIncomeLoss", "rs-gaap:GrossProfit")] == 1
  assert arcs[("rs-gaap:NetIncomeLoss", "rs-gaap:OperatingExpenses")] == -1
  assert arcs[("rs-gaap:NetIncomeLoss", "rs-gaap:InterestExpense")] == -1
  assert model.concepts["rs-gaap:Assets"].balance == "debit"


def test_a_statement_laid_out_from_the_library_must_foot(fund):
  with pytest.raises(ReportImportRefusedError) as refused:
    ingest_report(
      fund["session"],
      IngestReportRequest(entity_id=fund["issuer"].id, report=_values_only(opex=250)),
      created_by="usr_1",
      graph_id=GRAPH,
    )

  codes = {problem["code"] for problem in refused.value.problems}
  assert codes == {"does_not_foot"}


def test_a_section_the_library_does_not_lay_out_needs_its_lines(fund):
  with pytest.raises(ReportImportRefusedError) as refused:
    ingest_report(
      fund["session"],
      IngestReportRequest(
        entity_id=fund["issuer"].id,
        report=_values_only(
          sections=[{"name": "Cash Flow", "block_type": "cash_flow_statement"}]
        ),
      ),
      created_by="usr_1",
      graph_id=GRAPH,
    )

  assert "give this one its lines" in refused.value.problems[0]["message"]


def test_a_tavi_model_is_stored_as_a_holon_and_loaded(fund):
  # A Tavi model of a package: what the rows build, written out as Tavi.
  session = fund["session"]
  _ingest(fund, report=_rows())
  built, _ = from_holon_report(fund["s3"].upload_string.call_args.kwargs["content"])
  other = create_issuer(session, CreateIssuerRequest(name="Bolt Co"), created_by="u")

  result = _ingest({**fund, "issuer": other}, tavi=to_tavi(built), dry_run=False)

  assert (result.fact_count, result.section_count) == (5, 2)
  assert (result.calculations_checked, result.balance_checked) == (1, True)
  stored = fund["s3"].upload_string.call_args.kwargs["content"]
  model, _ = from_holon_report(stored)
  assert len(model.facts) == 5
  headcount = next(f for f in model.facts if f.concept_qname == "kpi:Headcount")
  assert headcount.numeric_value == 42


def test_a_tavi_model_past_the_size_limit_is_refused_before_it_is_built(fund):
  _ingest(fund, report=_rows())
  built, _ = from_holon_report(fund["s3"].upload_string.call_args.kwargs["content"])
  commands = "robosystems.operations.roboinvestor.commands.reports"

  with (
    patch(f"{commands}.MAX_REPORT_FACTS", 4),
    patch(f"{commands}.to_holon") as to_holon,
    pytest.raises(ReportImportRefusedError) as refused,
  ):
    _ingest(fund, tavi=to_tavi(built), dry_run=True)

  assert refused.value.problems[0]["code"] == "too_large"
  to_holon.assert_not_called()


def test_rows_past_the_size_limit_are_refused_by_the_request():
  rows = _rows()
  rows["facts"] = [rows["facts"][0]] * (MAX_REPORT_FACTS + 1)
  with pytest.raises(ValidationError) as refused:
    IngestReportRequest.model_validate({"entity_id": "ent_1", "report": rows})
  assert refused.value.errors()[0]["type"] == "too_long"


def _monthly(month: int) -> dict:
  end = (date(2020, 1, 31) + timedelta(days=31 * month)).replace(day=28).isoformat()
  rows = _rows()
  rows["period_end"] = end
  for fact in rows["facts"]:
    fact["end"] = end
  return rows


@pytest.mark.timeout(300)
def test_a_fund_keeps_receiving_reports_past_fifty(fund):
  # Every report's sections land in the one extension for received reports,
  # which an earlier build re-checked against a 100-section authoring cap.
  for month in range(50):
    _ingest(fund, report=_monthly(month))

  result = _ingest(fund, report=_monthly(50))

  assert result.report_id and result.section_count == 2
  held = (
    fund["session"].execute(select(Report.id).where(Report.origin == "uploaded")).all()
  )
  assert len(held) == 51


def test_a_value_stated_twice_is_loaded_once(fund):
  # A summary page restates cash in thousands. The check keeps the more
  # precise of the two; so must the load, or the statement adds them.
  rows = _rows()
  rounded = copy.deepcopy(
    next(f for f in rows["facts"] if f["concept"] == "rs-gaap:Cash")
  )
  rounded.update(value=1000, decimals=-3, source="package.pdf#page=1")
  rows["facts"].append(rounded)

  result = _ingest(fund, report=rows)

  cash = (
    fund["session"]
    .execute(
      select(Fact.value)
      .join(Element, Fact.element_id == Element.id)
      .join(FactSet, FactSet.id == Fact.fact_set_id)
      .where(FactSet.report_id == result.report_id, Element.qname == "rs-gaap:Cash")
    )
    .all()
  )
  assert [value for (value,) in cash] == [600.0]
  assert result.fact_count == 5


def test_text_that_is_not_a_tavi_model_is_refused(fund):
  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest(fund, tavi="[1, 2]")
  assert refused.value.problems[0]["code"] == "unreadable"


def test_two_forms_of_one_report_are_refused(fund):
  with pytest.raises(IngestRequestError):
    _ingest(fund, tavi="{}", report=_rows())


def test_a_refusal_names_the_values_no_statement_places(fund):
  rows = _values_only(opex=250)
  rows["facts"].append(
    {
      "concept": "rs-gaap:CostsAndExpenses",
      "value": 50,
      "start": "2026-10-01",
      "end": "2026-10-31",
      "unit": "USD",
      "decimals": 0,
    }
  )
  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest(fund, report=rows)

  unplaced = [p for p in refused.value.problems if p["code"] == "unplaced"]
  assert len(unplaced) == 1
  assert "rs-gaap:CostsAndExpenses" in unplaced[0]["message"]


def test_a_statement_laid_out_from_the_library_takes_the_firms_own_lines(fund):
  # Cash is split into two lines the library does not have; receivables are
  # moved from current assets to sit directly under total assets.
  rows = _values_only()
  rows["namespaces"] = {"acme": "https://example.com/taxonomy/acme/v1/"}
  rows["concepts"] = [
    {
      "qname": f"acme:{name}",
      "label": label,
      "period_type": "instant",
      "balance": "debit",
    }
    for name, label in (("Operating", "Cash - operating"), ("Sweep", "Cash - sweep"))
  ]
  rows["facts"] = [
    f
    for f in rows["facts"]
    if f["concept"] != "rs-gaap:CashAndCashEquivalentsAtCarryingValue"
  ]
  for name, amount in (("Operating", 450), ("Sweep", 150)):
    rows["facts"].append(
      {
        "concept": f"acme:{name}",
        "value": amount,
        "end": "2026-10-31",
        "unit": "USD",
        "decimals": 0,
      }
    )
  for fact in rows["facts"]:
    if fact["concept"] == "rs-gaap:AssetsCurrent":
      fact["value"] = 600
  rows["sections"][0] |= {
    "layout": "library",
    "lines": [
      {"concept": "acme:Operating", "parent": "rs-gaap:AssetsCurrent", "weight": 1},
      {"concept": "acme:Sweep", "parent": "rs-gaap:AssetsCurrent", "weight": 1},
      {
        "concept": "rs-gaap:AccountsReceivableNetCurrent",
        "parent": "rs-gaap:Assets",
        "weight": 1,
      },
    ],
  }

  result = _ingest(fund, report=rows)

  assert result.unplaced_concepts == []
  assert result.calculations_checked == 5
  model, _ = from_holon_report(fund["s3"].upload_string.call_args.kwargs["content"])
  arcs = {
    (arc.from_qname, arc.to_qname)
    for network in model.networks
    if network.kind == "calculation"
    for arc in network.arcs
  }
  assert ("rs-gaap:AssetsCurrent", "acme:Sweep") in arcs
  assert ("rs-gaap:Assets", "rs-gaap:AccountsReceivableNetCurrent") in arcs
  assert ("rs-gaap:AssetsCurrent", "rs-gaap:AccountsReceivableNetCurrent") not in arcs


def test_a_subtotal_given_its_own_line_keeps_the_lines_beneath_it(fund):
  rows = _values_only()
  rows["sections"][0] |= {
    "layout": "library",
    "lines": [
      {"concept": "rs-gaap:AssetsCurrent", "parent": "rs-gaap:Assets", "weight": 1}
    ],
  }

  result = _ingest(fund, report=rows)

  assert result.calculations_checked == 5
  model, _ = from_holon_report(fund["s3"].upload_string.call_args.kwargs["content"])
  arcs = {
    (arc.from_qname, arc.to_qname)
    for network in model.networks
    if network.kind == "calculation"
    for arc in network.arcs
  }
  assert ("rs-gaap:AssetsCurrent", "rs-gaap:AccountsReceivableNetCurrent") in arcs
  assert ("rs-gaap:Assets", "rs-gaap:AccountsReceivableNetCurrent") not in arcs


def test_a_library_line_reads_back_in_the_documents_wording(fund):
  rows = _values_only()
  rows["labels"] = {"rs-gaap:OperatingExpenses": "Total Operating Expense"}

  _ingest(fund, report=rows)

  model, _ = from_holon_report(fund["s3"].upload_string.call_args.kwargs["content"])

  def wording(qname: str) -> list[str | None]:
    return [label.value for label in model.concepts[qname].labels]

  assert wording("rs-gaap:OperatingExpenses") == ["Total Operating Expense"]
  assert wording("rs-gaap:Revenues") != ["Total Operating Expense"]


def _own(name: str, label: str, balance: str = "debit") -> dict:
  return {
    "qname": f"acme:{name}",
    "label": label,
    "period_type": "instant",
    "balance": balance,
  }


def _with_own_line(line: dict | None) -> dict:
  """The values-only package with cash replaced by a concept of the firm's
  own, given ``line`` on the balance sheet."""
  rows = _values_only()
  rows["namespaces"] = {"acme": "https://example.com/taxonomy/acme/v1/"}
  rows["concepts"] = [_own("Operating", "Cash - operating")]
  for fact in rows["facts"]:
    if fact["concept"] == "rs-gaap:CashAndCashEquivalentsAtCarryingValue":
      fact["concept"] = "acme:Operating"
  if line is not None:
    rows["sections"][0] |= {"layout": "library", "lines": [line]}
  return rows


def test_values_laid_out_by_the_library_conform_to_it(fund):
  result = _ingest(fund, report=_values_only())
  assert result.conforms_to_library is True
  assert result.departures == []


def test_a_firms_own_line_under_a_library_total_conforms(fund):
  line = {"concept": "acme:Operating", "parent": "rs-gaap:AssetsCurrent", "weight": 1}
  result = _ingest(fund, report=_with_own_line(line))
  assert result.conforms_to_library is True


@pytest.mark.parametrize(
  "line",
  [
    {"concept": "acme:Operating"},
    {"concept": "acme:Operating", "parent": "rs-gaap:AssetsCurrent"},
  ],
)
def test_a_firms_own_line_on_a_statement_must_add_into_a_library_total(fund, line):
  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest(fund, report=_with_own_line(line))
  problem = refused.value.problems[0]
  assert (problem["code"], problem["concept"]) == ("unanchored", "acme:Operating")


def test_a_firms_own_concept_with_a_value_and_no_line_is_refused(fund):
  with pytest.raises(ReportImportRefusedError) as refused:
    _ingest(fund, report=_with_own_line(None))
  assert refused.value.problems[0]["code"] == "unanchored"
  assert "acme:Operating" in refused.value.problems[0]["message"]


def test_a_moved_line_loads_and_is_recorded_as_a_departure(fund):
  rows = _values_only()
  rows["sections"][0] |= {
    "layout": "library",
    "lines": [
      {
        "concept": "rs-gaap:AccountsReceivableNetCurrent",
        "parent": "rs-gaap:Assets",
        "weight": 1,
      }
    ],
  }
  for fact in rows["facts"]:
    if fact["concept"] == "rs-gaap:AssetsCurrent":
      fact["value"] = 600

  result = _ingest(fund, report=rows)

  assert result.conforms_to_library is False
  [departure] = result.departures
  assert (departure.kind, departure.concept) == (
    "moved",
    "rs-gaap:AccountsReceivableNetCurrent",
  )
  assert (departure.library_parent, departure.parent) == (
    "rs-gaap:AssetsCurrent",
    "rs-gaap:Assets",
  )
  stored = fund["session"].get(Report, result.report_id).metadata_["layout"]
  assert stored["conforms_to_library"] is False
  assert stored["departures"][0]["kind"] == "moved"


def test_a_statement_sent_with_all_its_own_lines_is_a_departure(fund):
  result = _ingest(fund, report=_rows())
  assert [d.kind for d in result.departures] == ["own_layout"]


def test_a_holon_is_not_compared_with_the_library(fund):
  _ingest(fund, report=_rows())
  holon = fund["s3"].upload_string.call_args.kwargs["content"]
  other = create_issuer(
    fund["session"], CreateIssuerRequest(name="Bolt Co"), created_by="u"
  )
  result = _ingest({**fund, "issuer": other}, holon=holon)
  assert result.conforms_to_library is None
