"""PP&E Net on a real tenant schema when a chart mixes a direct Net mapping
with Gross + Accumulated Depreciation mappings: every account lands on the
balance sheet once, and the statement foots."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.operations.roboledger.reports.calc_dag import (
  PPE_ACCUMULATED_DEPRECIATION_QNAME,
  PPE_GROSS_QNAME,
  PPE_NET_QNAME,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  generate_report_facts,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd31"
SEP = PeriodSpec(start=date(2026, 9, 1), end=date(2026, 9, 30), label="Sep")

TARGETS = {
  "net": (PPE_NET_QNAME, "debit"),
  "gross": (PPE_GROSS_QNAME, "debit"),
  "ad": (PPE_ACCUMULATED_DEPRECIATION_QNAME, "credit"),
  "cash": ("rs-gaap:Cash", "debit"),
  "equity": ("rs-gaap:CommonStockValue", "credit"),
}
# source account -> the targets it maps to
CHART = {
  "equipment": ["gross"],
  "accum": ["ad"],
  "leasehold_net": ["net"],
  "vehicles": ["net", "gross"],
  "bank": ["cash"],
  "capital": ["equity"],
}


@pytest.fixture(scope="module")
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
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
      seed_parent_entity_on(conn.execution_options(schema_translate_map={None: GRAPH}))
    yield _seed()
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _seed() -> dict[str, str]:
  ids: dict[str, str] = {}
  with extensions_session(GRAPH) as session:
    taxonomy = Taxonomy(name="test", taxonomy_type="mapping")
    session.add(taxonomy)
    session.flush()
    mapping = Structure(
      name="CoA mapping", block_type="coa_mapping", taxonomy_id=taxonomy.id
    )
    session.add(mapping)
    session.flush()
    ids["mapping"] = mapping.id
    targets = {}
    for key, (qname, balance) in TARGETS.items():
      target = Element(
        qname=qname,
        name=qname.split(":")[1],
        balance_type=balance,
        period_type="instant",
        element_type="concept",
        is_abstract=False,
      )
      session.add(target)
      session.flush()
      targets[key] = target.id
    for account, mapped_to in CHART.items():
      balance = TARGETS[mapped_to[0]][1]
      source = Element(
        qname=f"coa:{account}",
        name=account,
        balance_type=balance,
        period_type="instant",
        element_type="concept",
        is_abstract=False,
      )
      session.add(source)
      session.flush()
      ids[account] = source.id
      for key in mapped_to:
        session.add(
          Association(
            structure_id=mapping.id,
            from_element_id=source.id,
            to_element_id=targets[key],
            association_type="mapping",
          )
        )
  return ids


def _post(ids, lines):
  with extensions_session(GRAPH) as session:
    entry = Entry(
      entity_id=PARENT_ENTITY_ID,
      type="standard",
      status="posted",
      posting_date=date(2026, 9, 15),
      memo="test",
      created_by="test",
    )
    session.add(entry)
    session.flush()
    for order, (account, debit, credit) in enumerate(lines, start=1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=ids[account],
          debit_amount=debit,
          credit_amount=credit,
          line_order=order,
        )
      )


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
  yield


def _net(ids) -> float:
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(session, "", ids["mapping"], [SEP]).facts
  return sum(
    f.value
    for f in facts
    if f.element_qname == PPE_NET_QNAME and not f.audit_only and f.period_end == SEP.end
  )


def test_a_direct_net_account_beside_gross_and_ad_keeps_both(tenant):
  _post(
    tenant,
    [
      ("bank", 10_000_000, 0),
      ("capital", 0, 10_000_000),
      ("equipment", 8_000_000, 0),
      ("bank", 0, 8_000_000),
      ("accum", 0, 447_130),
      ("capital", 447_130, 0),
      ("leasehold_net", 1_000_000, 0),
      ("bank", 0, 1_000_000),
    ],
  )
  # 10,000 direct Net + (80,000 Gross - 4,471.30 AD)
  assert _net(tenant) == pytest.approx(85_528.70)


def test_an_account_mapped_to_net_and_gross_counts_once(tenant):
  _post(
    tenant,
    [
      ("bank", 5_000_000, 0),
      ("capital", 0, 5_000_000),
      ("vehicles", 3_000_000, 0),
      ("bank", 0, 3_000_000),
      ("equipment", 1_000_000, 0),
      ("bank", 0, 1_000_000),
    ],
  )
  # vehicles 30,000 once (via Net) + equipment 10,000 (Gross only)
  assert _net(tenant) == pytest.approx(40_000.0)


def test_an_all_in_net_chart_is_unchanged(tenant):
  _post(tenant, [("leasehold_net", 2_500_000, 0), ("bank", 0, 2_500_000)])
  assert _net(tenant) == pytest.approx(25_000.0)


def test_assets_at_cost_on_net_with_depreciation_on_ad_net_down(tenant):
  _post(
    tenant,
    [
      ("leasehold_net", 5_000_000, 0),
      ("bank", 0, 5_000_000),
      ("accum", 0, 600_000),
      ("capital", 600_000, 0),
    ],
  )
  # 50,000 at cost on Net, less 6,000 accumulated depreciation
  assert _net(tenant) == pytest.approx(44_000.0)
