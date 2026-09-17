"""The shared hint vocabulary: GL labels parse, traits infer, names slug."""

from __future__ import annotations

import pytest

from robosystems.adapters.bank_feed.hints import (
  HINTS,
  infer_trait,
  parse_gl_code_name,
  slug,
)


@pytest.mark.unit
class TestHints:
  def test_hint_keys_match_their_dict_keys(self):
    assert all(hint.key == key for key, hint in HINTS.items())

  def test_every_hint_is_income_statement(self):
    assert {hint.trait for hint in HINTS.values()} == {"revenue", "expense"}

  @pytest.mark.parametrize(
    ("label", "expected"),
    [
      ("500 - Office Supplies", ("500", "Office Supplies")),
      ("6100 – Software", ("6100", "Software")),
      ("4000: Revenue", ("4000", "Revenue")),
      ("Office Supplies", (None, "Office Supplies")),
      ("", (None, "")),
    ],
  )
  def test_parse_gl_code_name(self, label, expected):
    assert parse_gl_code_name(label) == expected

  @pytest.mark.parametrize(
    ("name", "trait"),
    [
      ("Subscription revenue", ("revenue", "credit")),
      ("Accounts payable", ("liability", "credit")),
      ("Owner draw", ("equity", "credit")),
      ("Prepaid insurance", ("asset", "debit")),
      ("Meals", ("expense", "debit")),
    ],
  )
  def test_infer_trait(self, name, trait):
    assert infer_trait(name) == trait

  def test_slug(self):
    assert slug("General & Administrative:Technology") == (
      "GeneralAdministrativeTechnology"
    )
