"""Books with a closed month, for tests of what curation may and may not do
to closed history: a throwaway extensions schema, the helpers that lay down a
closed period with a canonical statement set and landed history, and a chart
whose style composes a balance sheet and an income statement.

Imported by name into each test module (never as fixtures across modules),
so every module wraps ``ext_schema_session`` in its own fixture.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import (
  Association,
  Element,
  EntityTaxonomy,
  FactSet,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.reporting_style_network import (
  ReportingStyleNetwork,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.line_item import LineItem
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

GRAPH = "kg0123456789abcdef01"
JUNE = (date(2026, 6, 1), date(2026, 6, 30))
JULY = (date(2026, 7, 1), date(2026, 7, 31))
STYLE = "style_closed_books"


def ext_schema_session() -> Iterator[Session]:
  """Extensions schema in the test Postgres DB, one throwaway schema per test,
  with the group parent seeded."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_closed_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  seed_parent_entity(session)
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def fiscal_period(
  session: Session, name: str, window: tuple[date, date], status: str
) -> None:
  session.add(
    FiscalPeriod(
      entity_id=PARENT_ENTITY_ID,
      graph_id=GRAPH,
      name=name,
      start_date=window[0],
      end_date=window[1],
      period_type="month",
      status=status,
    )
  )


def canonical_set(
  session: Session, structure: Structure, window: tuple[date, date], target: Element
) -> FactSet:
  """A close-minted statement set: report_id NULL, scenario NULL, type report."""
  fact_set = FactSet(
    structure_id=structure.id,
    period_start=window[0],
    period_end=window[1],
    factset_type="report",
    entity_id=PARENT_ENTITY_ID,
  )
  fact_set.provenance = {
    "origin": "pivot",
    "mapping_id": "map_1",
    "period": f"{window[0]}/{window[1]}",
  }
  session.add(fact_set)
  session.flush()
  session.add(
    Fact(
      element_id=target.id,
      string_value="stamped",
      fact_type="Nonnumeric",
      value_type="inline",
      period_start=window[0],
      period_end=window[1],
      period_type="duration",
      entity_id=PARENT_ENTITY_ID,
      structure_id=structure.id,
      fact_set_id=fact_set.id,
    )
  )
  return fact_set


def landed_line(
  session: Session, account: Element, posting_date: date, status: str = "posted"
) -> None:
  entry = Entry(
    entity_id=PARENT_ENTITY_ID,
    posting_date=posting_date,
    status=status,
    created_by="usr_1",
  )
  session.add(entry)
  session.flush()
  session.add(
    LineItem(
      entry_id=entry.id, element_id=account.id, debit_amount=100, credit_amount=0
    )
  )


def _library(session: Session, qname: str, name: str) -> Element:
  element = Element(name=name, qname=qname, created_by="library-seeder")
  session.add(element)
  session.flush()
  return element


def _network(
  session: Session, taxonomy: Taxonomy, block_type: str, statement_type: str
) -> Structure:
  structure = Structure(
    name=statement_type, block_type=block_type, taxonomy_id=taxonomy.id
  )
  session.add(structure)
  session.flush()
  session.add(
    ReportingStyleNetwork(
      reporting_style_id=STYLE, statement_type=statement_type, network_id=structure.id
    )
  )
  return structure


def _presents(
  session: Session, network: Structure, parent: Element, child: Element
) -> None:
  session.add(
    Association(
      structure_id=network.id,
      from_element_id=parent.id,
      to_element_id=child.id,
      association_type="presentation",
      created_by="library-seeder",
    )
  )


def reclassification_books(s: Session) -> dict:
  """A chart on the group parent, whose style composes a balance sheet (cash,
  PP&E net, short-term debt, other current liabilities) and an income
  statement (rent); PP&E gross is synthesized detail under PP&E net by
  calculation arc; one concept is presented nowhere. The card account has
  landed history before closed June; the newer card has history in open
  July only."""
  entity = s.get(Entity, PARENT_ENTITY_ID)
  assert entity is not None
  entity.reporting_style_id = STYLE
  chart = Taxonomy(name="CoA", taxonomy_type="chart_of_accounts")
  library = Taxonomy(
    name="rs-gaap", taxonomy_type="reporting_standard", standard="rs-gaap"
  )
  calculations = Taxonomy(
    name="rs-gaap calculations",
    taxonomy_type="reporting",
    standard="rs-gaap-calculations",
    created_by="library-seeder",
  )
  s.add_all([chart, library, calculations])
  s.flush()
  s.add(
    EntityTaxonomy(
      entity_id=PARENT_ENTITY_ID,
      taxonomy_id=chart.id,
      basis="chart_of_accounts",
      is_primary=True,
    )
  )

  assets = _library(s, "rs-gaap:Assets", "Assets")
  liabilities = _library(s, "rs-gaap:Liabilities", "Liabilities")
  expenses = _library(s, "rs-gaap:CostsAndExpenses", "Costs and expenses")
  cash = _library(s, "rs-gaap:Cash", "Cash")
  debt = _library(s, "rs-gaap:DebtCurrent", "Short-term debt")
  other = _library(s, "rs-gaap:OtherLiabilitiesCurrent", "Other current liabilities")
  rent = _library(s, "rs-gaap:OperatingLeaseExpense", "Rent")
  ppe_net = _library(s, "rs-gaap:PropertyPlantAndEquipmentNet", "PP&E, net")
  ppe_gross = _library(s, "rs-gaap:PropertyPlantAndEquipmentGross", "PP&E, gross")
  orphan = _library(s, "rs-gaap:Orphan", "Presented nowhere")

  balance_sheet = _network(s, library, "balance_sheet", "balance_sheet")
  income = _network(s, library, "income_statement", "income_statement")
  for child in (cash, ppe_net):
    _presents(s, balance_sheet, assets, child)
  for child in (debt, other):
    _presents(s, balance_sheet, liabilities, child)
  _presents(s, income, expenses, rent)
  calc = Structure(
    name="BS calculation", block_type="balance_sheet", taxonomy_id=calculations.id
  )
  s.add(calc)
  s.flush()
  s.add(
    Association(
      structure_id=calc.id,
      from_element_id=ppe_net.id,
      to_element_id=ppe_gross.id,
      association_type="calculation",
      weight=1.0,
      created_by="library-seeder",
    )
  )

  card = Element(name="2110 Card", qname="coa:2110", taxonomy_id=chart.id)
  newer = Element(name="2111 Newer card", qname="coa:2111", taxonomy_id=chart.id)
  s.add_all([card, newer])
  s.flush()

  fiscal_period(s, "2026-06", JUNE, "closed")
  fiscal_period(s, "2026-07", JULY, "open")
  canonical_set(s, balance_sheet, JUNE, debt)
  canonical_set(s, balance_sheet, JULY, debt)
  landed_line(s, card, date(2026, 5, 15))
  landed_line(s, newer, date(2026, 7, 10))
  s.commit()
  return {
    "chart": chart,
    "balance_sheet": balance_sheet,
    "card": card,
    "newer": newer,
    "debt": debt,
    "other": other,
    "rent": rent,
    "ppe_gross": ppe_gross,
    "orphan": orphan,
  }
