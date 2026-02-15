"""Tests for Entity update helpers and NAType serialization fix.

Ensures pandas NA/NaN/NaT values from parquet files are properly
handled during entity comparison and JSON serialization.
"""

import numpy as np
import pandas as pd
import pytest

from robosystems.adapters.sec.processors.ingestion.materialization import (
  _is_empty_value,
  _to_native,
)


class TestIsEmptyValue:
  """Test _is_empty_value handles all pandas missing value types."""

  def test_none(self):
    assert _is_empty_value(None) is True

  def test_pd_na(self):
    assert _is_empty_value(pd.NA) is True

  def test_pd_nat(self):
    assert _is_empty_value(pd.NaT) is True

  def test_float_nan(self):
    assert _is_empty_value(float("nan")) is True

  def test_numpy_nan(self):
    assert _is_empty_value(np.nan) is True

  def test_normal_string(self):
    assert _is_empty_value("NVIDIA CORP") is False

  def test_empty_string(self):
    assert _is_empty_value("") is True

  def test_whitespace_only(self):
    assert _is_empty_value("   ") is True

  def test_zero(self):
    assert _is_empty_value(0) is False

  def test_normal_int(self):
    assert _is_empty_value(42) is False

  def test_numpy_int(self):
    assert _is_empty_value(np.int64(42)) is False


class TestToNative:
  """Test _to_native converts pandas/numpy types to JSON-safe Python types."""

  def test_none_passthrough(self):
    assert _to_native(None) is None

  def test_pd_na_to_none(self):
    assert _to_native(pd.NA) is None

  def test_pd_nat_to_none(self):
    assert _to_native(pd.NaT) is None

  def test_float_nan_to_none(self):
    assert _to_native(float("nan")) is None

  def test_numpy_nan_to_none(self):
    assert _to_native(np.nan) is None

  def test_string_passthrough(self):
    assert _to_native("NVIDIA CORP") == "NVIDIA CORP"

  def test_int_passthrough(self):
    assert _to_native(42) == 42

  def test_bool_passthrough(self):
    assert _to_native(True) is True

  def test_float_passthrough(self):
    result = _to_native(3.14)
    assert result == 3.14
    assert isinstance(result, float)

  def test_numpy_int64(self):
    result = _to_native(np.int64(7372))
    assert result == 7372
    assert isinstance(result, int)

  def test_numpy_float64(self):
    result = _to_native(np.float64(3.14))
    assert result == pytest.approx(3.14)
    assert isinstance(result, float)

  def test_numpy_str(self):
    result = _to_native(np.str_("hello"))
    assert result == "hello"
    assert isinstance(result, str)

  def test_strips_trailing_whitespace(self):
    result = _to_native("Wholesale-Electrical Apparatus ")
    assert result == "Wholesale-Electrical Apparatus"

  def test_strips_leading_whitespace(self):
    result = _to_native("  NVIDIA CORP")
    assert result == "NVIDIA CORP"

  def test_whitespace_only_becomes_none(self):
    assert _to_native("   ") is None

  def test_empty_string_becomes_none(self):
    assert _to_native("") is None

  def test_numpy_bool(self):
    result = _to_native(np.bool_(True))
    assert result is True
    assert isinstance(result, bool)


class TestEntityComparisonWithNAValues:
  """Integration test: simulate the entity comparison loop with NA values.

  This reproduces the exact failure from production where pd.NA values
  in parquet data caused 'Object of type NAType is not JSON serializable'.
  """

  def test_parquet_row_with_na_values_produces_json_safe_params(self):
    """Simulate a parquet row with mixed NA values, verify JSON-serializable output."""
    import json

    # Simulate a DataFrame row as it comes from parquet (with pd.NA for missing)
    df = pd.DataFrame(
      [
        {
          "identifier": "e859b2cc-22ef-51bf-9063-d7c321170af1",
          "name": "NVIDIA CORP",
          "legal_name": pd.NA,
          "ticker": "NVDA",
          "exchange": "Nasdaq",
          "category": pd.NA,
          "fiscal_year_end": "0128",
          "phone": pd.NA,
          "website": pd.NA,
          "status": pd.NA,
          "sic": np.int64(3674),
          "sic_description": "SEMICONDUCTORS & RELATED DEVICES",
        }
      ]
    )

    row = df.iloc[0]

    # Simulate existing entity from LadybugDB
    existing = {
      "identifier": "e859b2cc-22ef-51bf-9063-d7c321170af1",
      "name": "NVIDIA CORP",
      "legal_name": None,
      "ticker": "NVDA",
      "exchange": "Nasdaq",
      "category": None,
      "fiscal_year_end": "0128",
      "phone": None,
      "website": None,
      "status": None,
      "sic": "3674",
      "sic_description": "SEMICONDUCTORS & RELATED DEVICES",
    }

    updatable_fields = [
      "name",
      "legal_name",
      "ticker",
      "exchange",
      "category",
      "fiscal_year_end",
      "phone",
      "website",
      "status",
      "sic",
      "sic_description",
    ]

    # Run the same comparison logic as materialization.py
    update_data = {"identifier": row["identifier"]}

    for field in updatable_fields:
      new_value = row.get(field)
      old_value = existing.get(field)

      new_is_empty = _is_empty_value(new_value)
      old_is_empty = old_value is None or old_value == ""

      if new_is_empty and old_is_empty:
        continue

      new_str = "" if new_is_empty else str(new_value)
      old_str = "" if old_is_empty else str(old_value)

      if new_str != old_str:
        update_data[field] = _to_native(new_value) if not new_is_empty else None

    # Build params dict exactly as the MERGE query builder does
    params = {"identifier": update_data["identifier"]}
    for field, value in update_data.items():
      if field == "identifier":
        continue
      params[f"p_{field}"] = value

    # THIS is what failed in production - JSON serialization of params
    serialized = json.dumps(params)
    assert isinstance(serialized, str)

    # Verify no pandas types leaked through
    for key, value in params.items():
      assert not isinstance(value, (pd.core.arrays.masked.BaseMaskedDtype,)), (
        f"Pandas type leaked for {key}: {type(value)}"
      )
      if value is not None:
        assert isinstance(value, (str, int, float, bool)), (
          f"Non-native type for {key}: {type(value)}"
        )

  def test_entity_with_changed_na_field_not_flagged_as_changed(self):
    """pd.NA in parquet + None in graph = both empty = no change."""
    df = pd.DataFrame(
      [
        {
          "identifier": "test-id",
          "name": "Test Corp",
          "ticker": pd.NA,
        }
      ]
    )
    row = df.iloc[0]
    existing = {"identifier": "test-id", "name": "Test Corp", "ticker": None}

    new_is_empty = _is_empty_value(row.get("ticker"))
    old_is_empty = existing.get("ticker") is None

    # Both should be considered empty - no false change detection
    assert new_is_empty is True
    assert old_is_empty is True
