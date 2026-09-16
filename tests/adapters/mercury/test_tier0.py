"""Tier-0 hints: the map is consistent, and GL labels parse."""

from __future__ import annotations

import pytest

from robosystems.adapters.mercury.pipeline.tier0 import (
  CUSTOM_CATEGORIES,
  GL_CODES,
  HINTS,
  MERCURY_CATEGORIES,
  hint_for_custom_category,
  hint_for_gl_code,
  hint_for_mercury_category,
)


@pytest.mark.unit
class TestHintMap:
  def test_every_category_points_at_a_listed_hint(self):
    for table in (CUSTOM_CATEGORIES, MERCURY_CATEGORIES, GL_CODES):
      for name, key in table.items():
        assert key is None or key in HINTS, f"{name!r} -> unknown hint {key!r}"

  def test_hint_keys_match_their_dict_keys(self):
    assert all(hint.key == key for key, hint in HINTS.items())

  def test_custom_category_movement_is_known_but_suggests_nothing(self):
    hint, known = hint_for_custom_category("Transfer")
    assert known is True
    assert hint is None

  def test_custom_category_unknown_is_not_known(self):
    assert hint_for_custom_category("Yachts") == (None, False)
    assert hint_for_custom_category(None) == (None, False)

  def test_custom_category_resolves(self):
    hint, known = hint_for_custom_category("Bank Fees")
    assert known and hint is not None and hint.name == "Bank fees"

  def test_mercury_category_resolves_or_not(self):
    assert hint_for_mercury_category("Software") is HINTS["SoftwareSubscriptions"]
    assert hint_for_mercury_category("Yachts") is None
    assert hint_for_mercury_category(None) is None


@pytest.mark.unit
class TestGlCodes:
  def test_listed_gl_code_uses_its_hint(self):
    assert hint_for_gl_code("Credit card rewards") is HINTS["CreditCardRewards"]

  def test_unlisted_gl_code_derives_a_hint_from_the_name(self):
    hint = hint_for_gl_code("500 - Office Supplies")
    assert hint.name == "Office Supplies"
    assert hint.key == "gl_OfficeSupplies"
    assert (hint.trait, hint.balance_type) == ("expense", "debit")
