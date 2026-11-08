"""A fact's number comes from its exact text, never a 0.0 the engine projected."""

import pytest

from robosystems.operations.roboledger.views.fact_value import fact_number


@pytest.mark.unit
class TestFactNumber:
  def test_the_text_wins_over_a_zero_the_engine_projected(self):
    assert fact_number("72795612000", 0.0) == 72795612000.0

  def test_a_decimal_text_keeps_its_value(self):
    assert fact_number("0.2630", 0.0) == 0.263

  def test_a_negative_text_keeps_its_sign(self):
    assert fact_number("-121983000000", 0.0) == -121983000000.0

  def test_a_real_zero_stays_zero(self):
    assert fact_number("0", 0.0) == 0.0

  def test_no_numeric_value_means_no_number(self):
    assert fact_number("some text", None) is None
    assert fact_number("12", None) is None

  def test_text_that_is_not_a_number_falls_back(self):
    assert fact_number("see note 4", 12.5) == 12.5
    assert fact_number(None, 12.5) == 12.5
    assert fact_number("", 12.5) == 12.5

  def test_a_non_finite_text_falls_back(self):
    assert fact_number("nan", 3.0) == 3.0
    assert fact_number("inf", 3.0) == 3.0
