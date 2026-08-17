"""Scenario facts must not reach a rule evaluation of the actual books.

The default-member rule says actuals are the undimensioned scenario and a
forecast carries an explicit member, so any reader honoring the contract
excludes forecasts by construction. On the graph that contract is
``has_dimensions = false``; in OLTP it is ``fact_sets.scenario_id IS NULL``.

The rule engine reads OLTP, and it has two unpinned paths — ``evaluate-rules``
called with nothing but a ``structure_id`` is a supported request, and the
period close evaluates its statement structures without pinning a FactSet.
Both used to scope by ``structure_id`` alone, and scenario month sets carry
the *statement* structure's id: the whole point of the design is that a
scenario column renders through the same structure as its actuals. So the
scope that reads as "this statement" is really "this statement, in every
scenario, including months that have not happened".

These tests exist because that is invisible on a graph with no forecasts,
which is every graph the engine was written against.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models)
from robosystems.db.extensions import ExtensionsBase

pytestmark = pytest.mark.unit

ACTUAL_START = date(2026, 6, 1)
ACTUAL_END = date(2026, 6, 30)
FORECAST_START = date(2026, 9, 1)
FORECAST_END = date(2026, 9, 30)


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_leak_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
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


class _Books:
  """A balance sheet structure, its elements, and whatever sets a test adds."""

  def __init__(self, session) -> None:
    from robosystems.models.extensions import Element, Structure, Taxonomy

    self.session = session
    taxonomy = Taxonomy(name="rs-gaap", taxonomy_type="reporting_extension")
    session.add(taxonomy)
    session.flush()
    self.taxonomy_id = taxonomy.id

    self.structure = Structure(
      name="Balance Sheet", block_type="balance_sheet", taxonomy_id=taxonomy.id
    )
    self.scenario = Structure(
      name="Plan", block_type="forecast", taxonomy_id=taxonomy.id
    )
    session.add_all([self.structure, self.scenario])
    session.flush()

    def _el(qname: str, balance_type: str):
      element = Element(
        name=qname.split(":")[-1],
        qname=qname,
        balance_type=balance_type,
        period_type="instant",
        taxonomy_id=taxonomy.id,
      )
      session.add(element)
      return element

    self.assets = _el("rs-gaap:Assets", "debit")
    self.cash = _el("rs-gaap:CashAndCashEquivalentsAtCarryingValue", "debit")
    self.ppe = _el("rs-gaap:PropertyPlantAndEquipmentNet", "debit")
    self.liabilities = _el("rs-gaap:Liabilities", "credit")
    self.equity = _el("rs-gaap:StockholdersEquity", "credit")
    session.flush()

  def add_calc_arcs(self) -> None:
    from robosystems.models.extensions import Association

    for order, child in enumerate((self.cash, self.ppe)):
      self.session.add(
        Association(
          structure_id=self.structure.id,
          from_element_id=self.assets.id,
          to_element_id=child.id,
          association_type="calculation",
          order_value=float(order),
          weight=1.0,
        )
      )
    self.session.flush()

  def add_rule(self, pattern: str, expression: str, variables: list[tuple[str, str]]):
    from robosystems.models.extensions import Rule

    rule = Rule(
      taxonomy_id=self.taxonomy_id,
      rule_category="FundamentalAccountingConceptRelation",
      rule_pattern=pattern,
      rule_expression=expression,
      target_kind="structure",
      target_structure_id=self.structure.id,
      rule_variables=[
        {"variable_name": name, "variable_qname": qname} for name, qname in variables
      ],
    )
    self.session.add(rule)
    self.session.flush()
    return rule

  def add_set(self, *, scenario: bool, period, values: dict) -> str:
    from robosystems.models.extensions.roboledger import Fact, FactSet

    period_start, period_end = period
    fact_set = FactSet(
      structure_id=self.structure.id,
      period_start=period_start,
      period_end=period_end,
      factset_type="report",
      entity_id="ent_1",
      scenario_id=self.scenario.id if scenario else None,
    )
    fact_set.provenance = (
      {
        "origin": "forecast",
        "scenario_structure_id": self.scenario.id,
        "base_period": "2026-06",
        "month_index": 3,
        "drivers": [],
      }
      if scenario
      else {"origin": "pivot", "mapping_id": "m", "period": "2026-06"}
    )
    self.session.add(fact_set)
    self.session.flush()

    for element, value in values.items():
      self.session.add(
        Fact(
          element_id=element.id,
          value=value,
          fact_type="Numeric",
          period_start=period_start,
          period_end=period_end,
          period_type="instant",
          entity_id="ent_1",
          structure_id=self.structure.id,
          fact_set_id=fact_set.id,
        )
      )
    self.session.flush()
    return fact_set.id


def _evaluate(session, structure_id: str):
  from robosystems.operations.information_block.rules.engine import (
    evaluate_rules_for_structure,
  )

  # Exactly the `evaluate-rules` shape: a structure, nothing else. The
  # request model documents all three narrowing arguments as optional.
  return evaluate_rules_for_structure(session, structure_id, global_calculations={})


class TestUnpinnedBindingIgnoresScenarios:
  """An unpinned check binds the newest fact per element — and the newest
  fact on a graph with a forecast is a month that has not happened yet."""

  def test_the_equation_is_checked_against_actuals(self, ext_session) -> None:
    books = _Books(ext_session)
    books.add_rule(
      "EqualTo",
      "$Assets = ($Liabilities + $Equity)",
      [
        ("Assets", books.assets.qname),
        ("Liabilities", books.liabilities.qname),
        ("Equity", books.equity.qname),
      ],
    )
    # Actuals balance.
    books.add_set(
      scenario=False,
      period=(ACTUAL_START, ACTUAL_END),
      values={books.assets: 100.0, books.liabilities: 40.0, books.equity: 60.0},
    )
    # A forecast three months out that does not — the scenario is allowed
    # to be mid-edit, unverified, or plain wrong; it is not the books.
    books.add_set(
      scenario=True,
      period=(FORECAST_START, FORECAST_END),
      values={books.assets: 999.0, books.liabilities: 1.0, books.equity: 1.0},
    )

    results = _evaluate(ext_session, books.structure.id)

    assert [r.status for r in results] == ["pass"], (
      "the actual books balance; a forecast month bound in their place is "
      f"what makes this fail — {[(r.status, r.message) for r in results]}"
    )

  def test_a_graph_without_forecasts_is_unaffected(self, ext_session) -> None:
    """The control: the filter must not change the answer on actuals alone."""
    books = _Books(ext_session)
    books.add_rule(
      "EqualTo",
      "$Assets = ($Liabilities + $Equity)",
      [
        ("Assets", books.assets.qname),
        ("Liabilities", books.liabilities.qname),
        ("Equity", books.equity.qname),
      ],
    )
    books.add_set(
      scenario=False,
      period=(ACTUAL_START, ACTUAL_END),
      values={books.assets: 100.0, books.liabilities: 40.0, books.equity: 55.0},
    )

    results = _evaluate(ext_session, books.structure.id)

    assert [r.status for r in results] == ["fail"]


class TestUnpinnedRollupIgnoresScenarios:
  """The rollup path pins to the newest ACTUAL FactSet — unless there isn't
  one, and then it used to fall back to summing everything on the structure.

  Reachable: the walk emits balance-sheet sets on the statement structure
  even when no actual balance sheet exists (the degraded, rule-driven
  working-capital branch), so a structure can carry scenario sets and no
  actual set at all.
  """

  def test_no_actual_set_means_nothing_to_foot(self, ext_session) -> None:
    books = _Books(ext_session)
    books.add_calc_arcs()
    books.add_rule(
      "RollUp",
      "$Assets = ($Cash + $PPE)",
      [
        ("Assets", books.assets.qname),
        ("Cash", books.cash.qname),
        ("PPE", books.ppe.qname),
      ],
    )
    # Only a scenario set exists, and it does not foot.
    books.add_set(
      scenario=True,
      period=(FORECAST_START, FORECAST_END),
      values={books.assets: 999.0, books.cash: 1.0, books.ppe: 1.0},
    )

    results = _evaluate(ext_session, books.structure.id)

    assert [r.status for r in results] == ["skipped"], (
      "with no actual FactSet there is nothing to check; reporting a "
      "verdict here reports one about a forecast, and stamps it on the "
      f"actual books — {[(r.status, r.message) for r in results]}"
    )

  def test_an_actual_set_still_foots(self, ext_session) -> None:
    """The control: the actual rollup still evaluates, and still fails when
    it should."""
    books = _Books(ext_session)
    books.add_calc_arcs()
    books.add_rule(
      "RollUp",
      "$Assets = ($Cash + $PPE)",
      [
        ("Assets", books.assets.qname),
        ("Cash", books.cash.qname),
        ("PPE", books.ppe.qname),
      ],
    )
    books.add_set(
      scenario=False,
      period=(ACTUAL_START, ACTUAL_END),
      values={books.assets: 200.0, books.cash: 100.0, books.ppe: 50.0},
    )

    results = _evaluate(ext_session, books.structure.id)

    assert [r.status for r in results] == ["fail"]
