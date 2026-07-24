"""Tests for element projection with documentation labels.

Exercises :func:`element_to_lite`, :func:`load_documentation_for_elements`,
and :func:`elements_to_lites` from
:mod:`robosystems.operations.information_block.envelope` with a mocked
SQLAlchemy session — no database required.

The documentation-role label carries the catalog's authoritative value
semantics (e.g. whether a percent driver is a growth rate or a
rate-on-base fraction). Surfacing it on the envelope is what lets a
code-blind MCP operator author lever values correctly.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.information_block.envelope import (
  element_to_lite,
  elements_to_lites,
  load_documentation_for_elements,
)


def _make_element(elem_id: str = "elem_1", qname: str | None = None) -> MagicMock:
  element = MagicMock()
  element.id = elem_id
  element.qname = qname
  element.name = "Cost of Revenue Rate"
  element.code = qname
  element.element_type = "concept"
  element.is_abstract = False
  element.is_monetary = False
  element.balance_type = "debit"
  element.period_type = "duration"
  element.item_type = "percent"
  return element


def _session_returning(rows: list[tuple[str, str]]) -> MagicMock:
  session = MagicMock()
  result = MagicMock()
  result.all.return_value = rows
  session.execute.return_value = result
  return session


class TestElementToLite:
  def test_documentation_defaults_to_none(self) -> None:
    lite = element_to_lite(_make_element())
    assert lite.documentation is None

  def test_documentation_attached_when_passed(self) -> None:
    lite = element_to_lite(
      _make_element(qname="rs-driver:CostOfRevenueRate"),
      documentation="Cost of revenue as a fraction of the same month's revenues.",
    )
    assert lite.documentation is not None
    assert "fraction" in lite.documentation


class TestLoadDocumentationForElements:
  def test_empty_ids_short_circuits_without_querying(self) -> None:
    session = MagicMock()
    assert load_documentation_for_elements(session, []) == {}
    session.execute.assert_not_called()

  def test_maps_element_id_to_label_text(self) -> None:
    session = _session_returning(
      [
        ("elem_a", "Month-over-month revenue growth (compounding lever)."),
        ("elem_b", "Rate-on-base lever: fraction of same-month revenues."),
      ]
    )
    docs = load_documentation_for_elements(session, ["elem_a", "elem_b", "elem_c"])
    assert docs == {
      "elem_a": "Month-over-month revenue growth (compounding lever).",
      "elem_b": "Rate-on-base lever: fraction of same-month revenues.",
    }
    session.execute.assert_called_once()


class TestElementsToLites:
  def test_attaches_documentation_by_id_with_one_query(self) -> None:
    elements = [
      _make_element("elem_growth", "rs-driver:RevenueGrowthRate"),
      _make_element("elem_cogs", "rs-driver:CostOfRevenueRate"),
      _make_element("elem_coa", None),
    ]
    session = _session_returning(
      [
        ("elem_growth", "Month-over-month revenue growth."),
        ("elem_cogs", "Fraction of the same month's revenues."),
      ]
    )
    lites = elements_to_lites(session, elements)
    assert [lite.documentation for lite in lites] == [
      "Month-over-month revenue growth.",
      "Fraction of the same month's revenues.",
      None,
    ]
    # Batched — one label query per envelope, never per element.
    session.execute.assert_called_once()

  def test_empty_element_list(self) -> None:
    session = MagicMock()
    assert elements_to_lites(session, []) == []
    session.execute.assert_not_called()
