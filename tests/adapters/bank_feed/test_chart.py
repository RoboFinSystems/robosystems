"""Resolving a hint against the tenant's chart."""

from __future__ import annotations

import pytest

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex, name_key


@pytest.mark.unit
class TestChartIndex:
  def test_resolve_by_name_normalizes(self):
    index = ChartIndex(by_name={name_key("Bank Fees"): "e1"})
    assert index.resolve("bank-fees") == "e1"
    assert index.resolve(None, "nothing") is None

  def test_resolve_gl_code_prefers_the_code(self):
    index = ChartIndex(
      by_name={name_key("Office Supplies"): "by_name"}, by_code={"500": "by_code"}
    )
    assert index.resolve_gl_code("500 - Office Supplies") == "by_code"
    assert index.resolve_gl_code("501 - Office Supplies") == "by_name"
    assert index.resolve_gl_code("Yachts") is None
    assert index.resolve_gl_code(None) is None


@pytest.mark.unit
def test_a_liability_account_is_credit():
  card = BankAccount(
    account_id="a1",
    name="Card",
    kind="credit card",
    trait="liability",
    balance_type="credit",
    institution="First Platypus Bank",
  )
  assert card.is_credit
