"""Plaid's personal-finance category as a hint: the tables are consistent."""

from __future__ import annotations

import pytest

from robosystems.adapters.bank_feed.hints import HINTS
from robosystems.adapters.plaid.pipeline.tier0 import (
  DETAILED,
  PRIMARY,
  category,
  hint_for_category,
)


@pytest.mark.unit
class TestTables:
  def test_every_category_points_at_a_listed_hint(self):
    for table in (PRIMARY, DETAILED):
      for code, key in table.items():
        assert key is None or key in HINTS, f"{code!r} -> unknown hint {key!r}"

  def test_every_detailed_code_belongs_to_a_listed_primary(self):
    for code in DETAILED:
      assert any(code.startswith(f"{primary}_") for primary in PRIMARY), code


@pytest.mark.unit
class TestHintForCategory:
  def test_detailed_overrides_its_primary(self):
    hint, known = hint_for_category("INCOME", "INCOME_INTEREST_EARNED")
    assert known and hint is HINTS["InterestIncome"]

  def test_primary_is_the_fallback(self):
    hint, known = hint_for_category("FOOD_AND_DRINK", "FOOD_AND_DRINK_COFFEE")
    assert known and hint is HINTS["BusinessMeals"]

  def test_a_movement_is_known_but_suggests_nothing(self):
    assert hint_for_category("TRANSFER_OUT", "TRANSFER_OUT_SAVINGS") == (None, True)
    assert hint_for_category("INCOME", "INCOME_SALARY") == (None, True)

  def test_an_unlisted_category_is_unknown(self):
    assert hint_for_category("SPACE_TRAVEL", "SPACE_TRAVEL_ORBIT") == (None, False)
    assert hint_for_category(None, None) == (None, False)

  def test_category_reads_the_v2_block(self):
    txn = {
      "personal_finance_category": {
        "primary": "TRAVEL",
        "detailed": "TRAVEL_FLIGHTS",
        "confidence_level": "VERY_HIGH",
        "version": "v2",
      }
    }
    assert category(txn) == ("TRAVEL", "TRAVEL_FLIGHTS", "VERY_HIGH")
    assert category({}) == (None, None, None)
