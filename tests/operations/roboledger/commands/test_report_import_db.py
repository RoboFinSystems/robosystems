"""A report arrives as a holon and lands as rows, in the database.

Runs against the real extensions database with a provisioned throwaway
tenant, so the library, the reporting-extension command and every constraint
are the real ones. The holon is written by xbrlkit from rows, the way the
rows door will write it.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from xbrlkit.author import (
  ConceptRow,
  EntityRow,
  FactRow,
  LineRow,
  ReportRows,
  SectionRow,
  build_report,
)
from xbrlkit.serialize import to_holon

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.extensions import (
  Association,
  Element,
  Fact,
  Report,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.roboledger.commands.report_import import (
  ReportImportEntityNotFoundError,
  ReportImportRefusedError,
  import_report,
)
from robosystems.operations.roboledger.commands.reports import delete_report

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd71"
ISSUER = "ent_issuer_acme"
OCT = date(2026, 10, 31)
OCT_1 = date(2026, 10, 1)
_S3 = "robosystems.operations.roboledger.commands.report_import.S3Client"
_DELETE_ARTIFACTS = (
  "robosystems.operations.roboledger.commands.reports.delete_report_artifacts"
)


@pytest.fixture()
def tenant():
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
    with (
      patch(_S3) as s3,
      patch(_DELETE_ARTIFACTS) as delete_artifacts,
      extensions_session(GRAPH) as session,
    ):
      s3.return_value.upload_string.return_value = True
      session.add(
        Entity(
          id=ISSUER,
          name="Acme Co",
          source="linked",
          is_parent=False,
          status="active",
          created_by="usr_1",
        )
      )
      session.flush()
      yield {
        "session": session,
        "s3": s3.return_value,
        "delete_artifacts": delete_artifacts,
      }
  finally:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    db.dispose()


def _holon(*, cash: float = 600, headcount: float = 42, drop_expenses: bool = False):
  """October's package: a balance sheet, an income statement and one KPI the
  library does not have."""

  def money(qname: str, label: str, period: str, balance: str) -> ConceptRow:
    return ConceptRow.model_validate(
      {"qname": qname, "label": label, "period_type": period, "balance": balance}
    )

  def bs(concept: str, value: float, cell: str) -> FactRow:
    return FactRow(
      concept=concept,
      value=value,
      end=OCT,
      unit="USD",
      decimals=0,
      source=f"package.xlsx#sheet=BS&cell={cell}",
    )

  def pl(concept: str, value: float, cell: str) -> FactRow:
    return FactRow(
      concept=concept,
      value=value,
      start=OCT_1,
      end=OCT,
      unit="USD",
      decimals=0,
      source=f"package.xlsx#sheet=PL&cell={cell}",
    )

  facts = [
    bs("rs-gaap:Assets", 1000, "B10"),
    bs("rs-gaap:Cash", cash, "B4"),
    bs("rs-gaap:AccountsReceivableNetCurrent", 400, "B5"),
    bs("rs-gaap:LiabilitiesAndStockholdersEquity", 1000, "B20"),
    bs("rs-gaap:Liabilities", 300, "B14"),
    bs("rs-gaap:StockholdersEquity", 700, "B18"),
    pl("rs-gaap:NetIncomeLoss", 50, "B9"),
    pl("rs-gaap:Revenues", 250, "B3"),
    FactRow(
      concept="kpi:Headcount",
      value=headcount,
      end=OCT,
      unit="pure",
      decimals=0,
      source="package.pdf#page=3",
    ),
  ]
  if not drop_expenses:
    facts.append(pl("rs-gaap:CostsAndExpenses", 200, "B7"))
  model = build_report(
    ReportRows(
      report_id="pkg-acme-2026-10",
      entity=EntityRow(identifier="acme-co", name="Acme Co"),
      reporting_style="authored-package",
      period_end=OCT,
      form="monthly-package",
      attributed_to="package-reader",
      namespaces={"kpi": "https://example.com/taxonomy/kpi/v1/"},
      concepts=[
        ConceptRow(qname="kpi:BalanceSheetHead", label="Balance Sheet", abstract=True),
        money("rs-gaap:Assets", "Total assets", "instant", "debit"),
        money("rs-gaap:Cash", "Cash", "instant", "debit"),
        money(
          "rs-gaap:AccountsReceivableNetCurrent", "Receivables", "instant", "debit"
        ),
        money(
          "rs-gaap:LiabilitiesAndStockholdersEquity",
          "Total liabilities and equity",
          "instant",
          "credit",
        ),
        money("rs-gaap:Liabilities", "Liabilities", "instant", "credit"),
        money("rs-gaap:StockholdersEquity", "Equity", "instant", "credit"),
        money("rs-gaap:NetIncomeLoss", "Net income", "duration", "credit"),
        money("rs-gaap:Revenues", "Revenue", "duration", "credit"),
        money("rs-gaap:CostsAndExpenses", "Expenses", "duration", "debit"),
        ConceptRow(qname="kpi:KpiHead", label="KPIs", abstract=True),
        ConceptRow(
          qname="kpi:Headcount",
          label="Headcount",
          kind="integer",
          period_type="instant",
        ),
      ],
      sections=[
        SectionRow(
          name="Balance Sheet",
          block_type="balance_sheet",
          head="kpi:BalanceSheetHead",
          lines=[
            LineRow(concept="rs-gaap:Assets"),
            LineRow(concept="rs-gaap:Cash", parent="rs-gaap:Assets", weight=1),
            LineRow(
              concept="rs-gaap:AccountsReceivableNetCurrent",
              parent="rs-gaap:Assets",
              weight=1,
            ),
            LineRow(concept="rs-gaap:LiabilitiesAndStockholdersEquity"),
            LineRow(
              concept="rs-gaap:Liabilities",
              parent="rs-gaap:LiabilitiesAndStockholdersEquity",
              weight=1,
            ),
            LineRow(
              concept="rs-gaap:StockholdersEquity",
              parent="rs-gaap:LiabilitiesAndStockholdersEquity",
              weight=1,
            ),
          ],
        ),
        SectionRow(
          name="Income Statement",
          block_type="income_statement",
          lines=[
            LineRow(concept="rs-gaap:NetIncomeLoss"),
            LineRow(
              concept="rs-gaap:Revenues", parent="rs-gaap:NetIncomeLoss", weight=1
            ),
            LineRow(
              concept="rs-gaap:CostsAndExpenses",
              parent="rs-gaap:NetIncomeLoss",
              weight=-1,
            ),
          ],
        ),
        SectionRow(
          name="KPIs", head="kpi:KpiHead", lines=[LineRow(concept="kpi:Headcount")]
        ),
      ],
      facts=facts,
    )
  )
  return to_holon(model)


def _values(session, report_id: str) -> dict[str, float | None]:
  rows = session.execute(
    select(Element.qname, Fact.value)
    .join(Fact, Fact.element_id == Element.id)
    .join(FactSet, FactSet.id == Fact.fact_set_id)
    .where(FactSet.report_id == report_id)
  ).all()
  return {str(qname): value for qname, value in rows}


def test_a_package_lands_as_an_uploaded_report_on_its_entity(tenant):
  session = tenant["session"]
  holon = _holon()
  result = import_report(
    session,
    GRAPH,
    holon_text=holon,
    entity_id=ISSUER,
    created_by="usr_1",
    expected_facts=10,
    require_source=True,
  )

  report = session.get(Report, result.report_id)
  assert (report.origin, report.is_received, report.entity_id) == (
    "uploaded",
    True,
    ISSUER,
  )
  assert (report.generation_status, report.period_end, report.period_type) == (
    "published",
    OCT,
    "monthly",
  )
  assert report.source_graph_id is None

  # The file is kept exactly as it arrived, under the report's own key.
  upload = tenant["s3"].upload_string.call_args.kwargs
  assert upload["content"] == holon
  assert upload["key"] == f"report-bundles/{GRAPH}/{report.id}/g1.holon.jsonld"
  assert report.bundle_url.endswith(upload["key"])

  values = _values(session, result.report_id)
  assert result.fact_count == len(values) == 10
  assert values["rs-gaap:Assets"] == 1000
  assert values["rs-gaap:CostsAndExpenses"] == 200
  assert values["kpi:Headcount"] == 42

  # Three sections, each a structure of the received-reports extension with
  # its own fact set; every fact names the entity and its source cell.
  sets = (
    session.execute(select(FactSet).where(FactSet.report_id == result.report_id))
    .scalars()
    .all()
  )
  assert result.structure_count == len(sets) == 3
  assert all(s.structure_id and s.entity_id == ISSUER for s in sets)
  facts = (
    session.execute(select(Fact).where(Fact.fact_set_id.in_([s.id for s in sets])))
    .scalars()
    .all()
  )
  assert {f.entity_id for f in facts} == {ISSUER}
  cash = next(f for f in facts if f.value == 600)
  assert cash.provenance["origin"] == "asserted"
  assert cash.provenance["source_system"] == "report_upload"
  assert cash.provenance["basis_note"] == "package.xlsx#sheet=BS&cell=B4"

  # The library's own concept is used where it has one, pulled in when the
  # tenant's curated copy left it out; only the package's own are added.
  assert result.concepts_added == [
    "kpi:BalanceSheetHead",
    "kpi:Headcount",
    "kpi:KpiHead",
  ]
  # A received concept is not one of this graph's accounts.
  session.expire_all()
  assert set(
    session.execute(
      select(Element.source).where(Element.qname.in_(result.concepts_added))
    ).scalars()
  ) == {"linked"}
  library = session.execute(
    select(Taxonomy).where(Taxonomy.standard == "rs-gaap")
  ).scalar_one()
  expenses = session.execute(
    select(Element).where(Element.qname == "rs-gaap:CostsAndExpenses")
  ).scalar_one()
  assert expenses.taxonomy_id == library.id
  extension = session.get(Taxonomy, report.taxonomy_id)
  assert extension.taxonomy_type == "reporting_extension"
  assert extension.parent_taxonomy_id == library.id

  # The balance sheet's totals are calculation arcs, as the file stated them.
  balance_sheet = session.execute(
    select(Structure).where(
      Structure.taxonomy_id == extension.id, Structure.block_type == "balance_sheet"
    )
  ).scalar_one()
  calc = (
    session.execute(
      select(Association).where(
        Association.structure_id == balance_sheet.id,
        Association.association_type == "calculation",
      )
    )
    .scalars()
    .all()
  )
  assert len(calc) == 4 and {a.weight for a in calc} == {1.0}


def test_a_second_import_for_the_period_replaces_the_first(tenant):
  session = tenant["session"]
  first = import_report(
    session, GRAPH, holon_text=_holon(), entity_id=ISSUER, created_by="usr_1"
  )
  second = import_report(
    session,
    GRAPH,
    holon_text=_holon(headcount=45),
    entity_id=ISSUER,
    created_by="usr_1",
  )

  assert second.replaced_report_ids == [first.report_id]
  assert second.concepts_added == []
  reports = session.execute(select(Report.id)).scalars().all()
  assert reports == [second.report_id]
  assert _values(session, second.report_id)["kpi:Headcount"] == 45
  # The first report's sections went with it; one set of three remains.
  extension = session.get(Taxonomy, session.get(Report, second.report_id).taxonomy_id)
  structures = (
    session.execute(select(Structure.id).where(Structure.taxonomy_id == extension.id))
    .scalars()
    .all()
  )
  assert len(structures) == 3
  assert session.execute(select(FactSet.id)).scalars().all().__len__() == 3


def test_deleting_an_uploaded_report_takes_its_sections_and_keeps_its_concepts(tenant):
  session = tenant["session"]
  result = import_report(
    session, GRAPH, holon_text=_holon(), entity_id=ISSUER, created_by="usr_1"
  )
  extension_id = session.get(Report, result.report_id).taxonomy_id

  assert delete_report(session, result.report_id, acting_user_id="usr_1") is True

  assert session.execute(select(Report.id)).scalars().all() == []
  assert session.execute(select(Fact.id)).scalars().all() == []
  assert (
    session.execute(select(Structure.id).where(Structure.taxonomy_id == extension_id))
    .scalars()
    .all()
    == []
  )
  assert session.execute(
    select(Element.id).where(Element.qname == "kpi:Headcount")
  ).scalar_one()


def test_a_concept_the_graph_holds_in_another_vocabulary_is_linked_not_copied(
  tenant,
):
  """A firm's own extension, or one a share brought, already names the KPI:
  the report's section links to that element rather than adding a second."""
  session = tenant["session"]
  library = session.execute(
    select(Taxonomy).where(Taxonomy.standard == "rs-gaap")
  ).scalar_one()
  firm = Taxonomy(
    name="Firm KPIs",
    taxonomy_type="reporting_extension",
    parent_taxonomy_id=library.id,
    created_by="usr_1",
  )
  session.add(firm)
  session.flush()
  headcount = Element(
    qname="kpi:Headcount",
    name="Headcount",
    period_type="instant",
    is_monetary=False,
    taxonomy_id=firm.id,
    source="linked",
    created_by="usr_1",
  )
  session.add(headcount)
  session.flush()

  result = import_report(
    session, GRAPH, holon_text=_holon(), entity_id=ISSUER, created_by="usr_1"
  )

  assert result.concepts_added == ["kpi:BalanceSheetHead", "kpi:KpiHead"]
  assert _values(session, result.report_id)["kpi:Headcount"] == 42
  linked = (
    session.execute(
      select(Association).where(Association.to_element_id == headcount.id)
    )
    .scalars()
    .all()
  )
  assert [a.association_type for a in linked] == ["presentation"]


def test_a_line_shown_twice_under_one_parent_is_one_association(tenant):
  """A ledger's own roll-forward presents a balance at its start and its end
  under one parent; the pair is unique in the graph."""
  session = tenant["session"]
  document = json.loads(_holon())
  arcs = [
    node
    for graph in document["@graph"]
    for node in graph.get("@graph", [])
    if "rs:Association" in str(node.get("@type")) and "RollUp" not in str(node)
  ]
  twin = dict(arcs[0])
  twin["@id"] = twin["@id"] + "-again"
  next(graph for graph in document["@graph"] if arcs[0] in graph.get("@graph", []))[
    "@graph"
  ].append(twin)

  result = import_report(
    session,
    GRAPH,
    holon_text=json.dumps(document),
    entity_id=ISSUER,
    created_by="usr_1",
  )
  assert result.fact_count == 10 and result.structure_count == 3


def test_the_stored_files_follow_the_transaction(tenant):
  """A replaced report's file goes only once the replacement commits, and a
  report whose transaction is undone takes its own file with it."""
  session, deleted = tenant["session"], tenant["delete_artifacts"]

  def run(**kwargs):
    return import_report(
      session,
      GRAPH,
      holon_text=_holon(**kwargs),
      entity_id=ISSUER,
      created_by="usr_1",
    )

  first = run()
  session.commit()
  deleted.assert_not_called()

  second = run(headcount=45)
  deleted.assert_not_called()
  session.commit()
  deleted.assert_called_once_with(GRAPH, [first.report_id])

  deleted.reset_mock()
  third = run(headcount=50)
  session.rollback()
  deleted.assert_called_once_with(GRAPH, [third.report_id])
  assert session.execute(select(Report.id)).scalars().all() == [second.report_id]


def test_a_package_that_does_not_foot_is_refused_and_nothing_is_written(tenant):
  session = tenant["session"]
  with pytest.raises(ReportImportRefusedError) as refused:
    import_report(
      session,
      GRAPH,
      holon_text=_holon(cash=500),
      entity_id=ISSUER,
      created_by="usr_1",
    )
  assert [p["code"] for p in refused.value.problems] == ["does_not_foot"]
  assert refused.value.problems[0]["concept"] == "rs-gaap:Assets"
  assert session.execute(select(Report.id)).scalars().all() == []
  assert session.execute(select(Fact.id)).scalars().all() == []
  tenant["s3"].upload_string.assert_not_called()


def test_a_fact_the_sender_counted_and_the_file_lacks_is_refused(tenant):
  with pytest.raises(ReportImportRefusedError) as refused:
    import_report(
      tenant["session"],
      GRAPH,
      holon_text=_holon(drop_expenses=True),
      entity_id=ISSUER,
      created_by="usr_1",
      expected_facts=10,
    )
  assert "fact_count" in [p["code"] for p in refused.value.problems]


def test_a_file_that_is_not_a_holon_is_refused(tenant):
  with pytest.raises(ReportImportRefusedError) as refused:
    import_report(
      tenant["session"],
      GRAPH,
      holon_text='{"hello": "world"}',
      entity_id=ISSUER,
      created_by="usr_1",
    )
  assert refused.value.problems[0]["code"] == "unreadable"


def test_an_entity_outside_the_graph_is_named(tenant):
  with pytest.raises(ReportImportEntityNotFoundError):
    import_report(
      tenant["session"],
      GRAPH,
      holon_text=_holon(),
      entity_id="ent_nobody",
      created_by="usr_1",
    )
