"""Statement handler tests — dispatch_create + dispatch_build_envelope.

Four statement block types (balance_sheet, income_statement,
cash_flow_statement, equity_statement) share one handler body
parameterised on block_type via ``make_statement_handlers``. These
tests exercise both dispatch points with a mocked SQLAlchemy session.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from robosystems.operations.information_block import statement as statement_handlers


def _exec_result(
  *, scalars_all: list[Any] | None = None, scalar: Any = None
) -> MagicMock:
  """Build a MagicMock shaped like a SQLAlchemy Result object."""
  result = MagicMock()
  if scalars_all is not None:
    result.scalars.return_value.all.return_value = scalars_all
  if scalar is not None:
    result.scalar.return_value = scalar
  else:
    # Default: execute().scalar() returns None (no latest report).
    result.scalar.return_value = None
  return result


class TestCreate:
  def test_create_raises_not_implemented_with_create_report_pointer(self) -> None:
    session = MagicMock()
    with pytest.raises(NotImplementedError) as exc:
      statement_handlers._create_not_implemented(session, MagicMock(), "usr_test")
    assert "create-report" in str(exc.value)

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_every_statement_handler_binds_the_not_implemented_create(
    self, block_type: str
  ) -> None:
    """Every handler pair wires the same 501-producing ``create``."""
    handlers = statement_handlers.make_statement_handlers(block_type)
    assert handlers["create"] is statement_handlers._create_not_implemented


class TestUpdate:
  def test_update_raises_not_implemented(self) -> None:
    session = MagicMock()
    with pytest.raises(NotImplementedError) as exc:
      statement_handlers._update_not_implemented(session, MagicMock(), "usr_test")
    assert "library-seeded" in str(exc.value)

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_every_statement_handler_binds_not_implemented_update(
    self, block_type: str
  ) -> None:
    handlers = statement_handlers.make_statement_handlers(block_type)
    assert handlers["update"] is statement_handlers._update_not_implemented


class TestDelete:
  def test_delete_raises_not_implemented(self) -> None:
    session = MagicMock()
    with pytest.raises(NotImplementedError) as exc:
      statement_handlers._delete_not_implemented(session, MagicMock(), "usr_test")
    assert "library-seeded" in str(exc.value)

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_every_statement_handler_binds_not_implemented_delete(
    self, block_type: str
  ) -> None:
    handlers = statement_handlers.make_statement_handlers(block_type)
    assert handlers["delete"] is statement_handlers._delete_not_implemented


class TestBuildEnvelope:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    build = statement_handlers.make_statement_handlers("balance_sheet")[
      "build_envelope"
    ]
    assert build(session, "struct_missing") is None

  def test_returns_none_when_structure_is_wrong_block_type(self) -> None:
    """A ``schedule`` structure must not surface through the BS handler."""
    session = MagicMock()
    structure = MagicMock()
    structure.structure_type = "schedule"
    session.get.return_value = structure
    build = statement_handlers.make_statement_handlers("balance_sheet")[
      "build_envelope"
    ]
    assert build(session, "struct_other") is None

  def test_returns_envelope_with_empty_facts_when_no_reports_exist(self) -> None:
    """Library sentinel path: seeded atoms present, no tenant reports."""
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_balance_sheet"
    structure.structure_type = "balance_sheet"
    structure.name = "Balance Sheet"
    structure.description = "Assets + Liabilities + Equity"
    session.get.return_value = structure
    # No associations → no elements → element_ids short-circuits the
    # element + latest-report queries. Only associations runs.
    session.execute.side_effect = [
      _exec_result(scalars_all=[]),  # associations
    ]

    build = statement_handlers.make_statement_handlers("balance_sheet")[
      "build_envelope"
    ]
    envelope = build(session, "struct_balance_sheet")

    assert envelope is not None
    assert envelope.id == "struct_balance_sheet"
    assert envelope.block_type == "balance_sheet"
    assert envelope.name == "Balance Sheet"
    assert envelope.display_name == "Balance Sheet"
    assert envelope.category == "Reporting"
    assert envelope.information_model.concept_arrangement == "roll_up"
    assert envelope.information_model.member_arrangement == "aggregation"
    assert envelope.artifact.topic == "Assets + Liabilities + Equity"
    assert envelope.artifact.mechanics.kind == "statement_renderer"
    assert envelope.elements == []
    assert envelope.connections == []
    assert envelope.facts == []
    # Reserved-for-later-phase fields default-empty.
    assert envelope.rules == []
    assert envelope.dimensions == []
    assert envelope.fact_set is None
    assert envelope.verification_results == []

  def test_loads_elements_and_associations_from_library_seed(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_income_statement"
    structure.structure_type = "income_statement"
    structure.name = "Income Statement"
    structure.description = None
    session.get.return_value = structure

    association = MagicMock()
    association.id = "assoc_1"
    association.from_element_id = "elem_revenue"
    association.to_element_id = "elem_sales"
    association.association_type = "presentation"
    association.arcrole = "http://…/parent-child"
    association.order_value = 1.0
    association.weight = None

    element_revenue = MagicMock()
    element_revenue.id = "elem_revenue"
    element_revenue.qname = "us-gaap:Revenues"
    element_revenue.name = "Revenues"
    element_revenue.code = None
    element_revenue.element_type = "abstract"
    element_revenue.is_abstract = True
    element_revenue.is_monetary = True
    element_revenue.balance_type = "credit"
    element_revenue.period_type = "duration"

    element_sales = MagicMock()
    element_sales.id = "elem_sales"
    element_sales.qname = "us-gaap:SalesRevenueNet"
    element_sales.name = "Sales"
    element_sales.code = None
    element_sales.element_type = "concept"
    element_sales.is_abstract = False
    element_sales.is_monetary = True
    element_sales.balance_type = "credit"
    element_sales.period_type = "duration"

    session.execute.side_effect = [
      _exec_result(scalars_all=[association]),  # associations
      _exec_result(scalars_all=[element_revenue, element_sales]),  # elements
      _exec_result(scalar=None),  # no reports → facts=[]
    ]

    build = statement_handlers.make_statement_handlers("income_statement")[
      "build_envelope"
    ]
    envelope = build(session, "struct_income_statement")

    assert envelope is not None
    assert envelope.block_type == "income_statement"
    assert len(envelope.connections) == 1
    assert envelope.connections[0].from_element_id == "elem_revenue"
    assert len(envelope.elements) == 2
    assert {e.id for e in envelope.elements} == {"elem_revenue", "elem_sales"}
    assert envelope.facts == []

  def test_facts_populated_from_most_recent_report(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_balance_sheet"
    structure.structure_type = "balance_sheet"
    structure.name = "Balance Sheet"
    structure.description = None
    session.get.return_value = structure

    assoc = MagicMock()
    assoc.id = "assoc_1"
    assoc.from_element_id = "elem_assets"
    assoc.to_element_id = "elem_cash"
    assoc.association_type = "presentation"
    assoc.arcrole = None
    assoc.order_value = None
    assoc.weight = None

    element = MagicMock()
    element.id = "elem_cash"
    element.qname = "us-gaap:CashAndCashEquivalentsAtCarryingValue"
    element.name = "Cash"
    element.code = None
    element.element_type = "concept"
    element.is_abstract = False
    element.is_monetary = True
    element.balance_type = "debit"
    element.period_type = "instant"

    fact = MagicMock()
    fact.id = "fact_1"
    fact.element_id = "elem_cash"
    fact.value = 100_000.0
    fact.period_start = None
    fact.period_end = date(2026, 3, 31)
    fact.period_type = "instant"
    fact.unit = "USD"
    fact.fact_scope = "in_scope"
    fact.fact_set_id = None

    session.execute.side_effect = [
      _exec_result(scalars_all=[assoc]),  # associations
      _exec_result(scalars_all=[element]),  # elements
      _exec_result(scalar="rep_latest"),  # latest_report_id
      _exec_result(scalars_all=[fact]),  # facts
    ]

    build = statement_handlers.make_statement_handlers("balance_sheet")[
      "build_envelope"
    ]
    envelope = build(session, "struct_balance_sheet")

    assert envelope is not None
    assert len(envelope.facts) == 1
    assert envelope.facts[0].id == "fact_1"
    assert envelope.facts[0].element_id == "elem_cash"
    assert envelope.facts[0].value == 100_000.0
    assert envelope.facts[0].period_end == date(2026, 3, 31)

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_display_metadata_is_block_type_specific(self, block_type: str) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.id = f"struct_{block_type}"
    structure.structure_type = block_type
    structure.name = statement_handlers.STATEMENT_DISPLAY[block_type][0]
    structure.description = None
    session.get.return_value = structure
    # No associations → no elements → no report lookup.
    session.execute.side_effect = [
      _exec_result(scalars_all=[]),
    ]

    build = statement_handlers.make_statement_handlers(block_type)["build_envelope"]
    envelope = build(session, f"struct_{block_type}")

    assert envelope is not None
    assert envelope.block_type == block_type
    assert envelope.display_name == statement_handlers.STATEMENT_DISPLAY[block_type][0]
    assert envelope.category == "Reporting"
    assert envelope.artifact.mechanics.kind == "statement_renderer"
