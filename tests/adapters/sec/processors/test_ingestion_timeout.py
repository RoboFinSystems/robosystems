"""Tests for staging timeout configuration in SEC ingestion processor."""

import pytest

from robosystems.adapters.sec.processors.ingestion import (
  DEFAULT_STAGING_TIMEOUT,
  LARGE_STAGING_TABLES,
  LARGE_TABLE_STAGING_TIMEOUT,
  STAGING_MAX_RETRIES,
  STAGING_RETRY_BACKOFF_BASE,
  _get_staging_timeout,
)


class TestStagingTimeoutConfiguration:
  """Tests for the staging timeout configuration constants and helper."""

  def test_timeout_constants_values(self):
    """Test that timeout constants have expected values."""
    assert DEFAULT_STAGING_TIMEOUT == 300  # 5 minutes
    assert LARGE_TABLE_STAGING_TIMEOUT == 1800  # 30 minutes

  def test_retry_constants_values(self):
    """Test that retry configuration constants have expected values."""
    assert STAGING_MAX_RETRIES == 3  # 1 initial + 2 retries
    assert STAGING_RETRY_BACKOFF_BASE == 30  # 30 seconds base backoff

  def test_retry_backoff_sequence(self):
    """Test that retry backoff sequence is reasonable."""
    # Backoff sequence: 30s, 60s, 90s (base * attempt)
    backoffs = [
      STAGING_RETRY_BACKOFF_BASE * (i + 1) for i in range(STAGING_MAX_RETRIES - 1)
    ]
    assert backoffs == [30, 60]  # Two retries with increasing backoff
    # Total max wait time before final failure: 90 seconds
    assert sum(backoffs) == 90

  def test_large_staging_tables_contents(self):
    """Test that large staging tables set contains expected tables."""
    expected_tables = {
      # Large node tables
      "Fact",
      "Label",
      "Element",
      "Dimension",
      "Association",
      "Structure",
      # Large relationship tables (fact-related)
      "REPORT_HAS_FACT",
      "FACT_HAS_ELEMENT",
      "FACT_HAS_ENTITY",
      "FACT_HAS_PERIOD",
      "FACT_HAS_UNIT",
      "FACT_HAS_DIMENSION",
      "FACT_REPORTS_ELEMENT",
      "FACT_SET_CONTAINS_FACT",
      "DIMENSION_HAS_MEMBER_ELEMENT",
      "DIMENSION_HAS_AXIS_ELEMENT",
      # Large relationship tables (shared reference)
      "ELEMENT_HAS_LABEL",
      "TAXONOMY_HAS_LABEL",
      # Large relationship tables (structure/association)
      "STRUCTURE_HAS_ASSOCIATION",
      "ASSOCIATION_HAS_FROM_ELEMENT",
      "ASSOCIATION_HAS_TO_ELEMENT",
    }
    assert expected_tables == LARGE_STAGING_TABLES

  def test_large_staging_tables_is_immutable(self):
    """Test that LARGE_STAGING_TABLES is a frozenset (immutable)."""
    assert isinstance(LARGE_STAGING_TABLES, frozenset)


class TestGetStagingTimeout:
  """Tests for _get_staging_timeout helper function."""

  @pytest.mark.parametrize(
    "table_name",
    [
      # Large node tables
      "Fact",
      "Label",
      "Element",
      "Dimension",
      "Association",
      "Structure",
      # Large relationship tables (fact-related)
      "REPORT_HAS_FACT",
      "FACT_HAS_ELEMENT",
      "FACT_HAS_ENTITY",
      "FACT_HAS_PERIOD",
      "FACT_HAS_UNIT",
      "FACT_HAS_DIMENSION",
      "FACT_REPORTS_ELEMENT",
      "FACT_SET_CONTAINS_FACT",
      "DIMENSION_HAS_MEMBER_ELEMENT",
      "DIMENSION_HAS_AXIS_ELEMENT",
      # Large relationship tables (shared reference)
      "ELEMENT_HAS_LABEL",
      "TAXONOMY_HAS_LABEL",
      # Large relationship tables (structure/association)
      "STRUCTURE_HAS_ASSOCIATION",
      "ASSOCIATION_HAS_FROM_ELEMENT",
      "ASSOCIATION_HAS_TO_ELEMENT",
    ],
  )
  def test_large_tables_get_extended_timeout(self, table_name: str):
    """Test that all large tables get the extended timeout."""
    assert _get_staging_timeout(table_name) == LARGE_TABLE_STAGING_TIMEOUT

  @pytest.mark.parametrize(
    "table_name",
    [
      "Entity",
      "Period",
      "Unit",
      "Report",
      "Taxonomy",
      "Reference",
      "FactSet",
      "ENTITY_HAS_REPORT",
    ],
  )
  def test_regular_tables_get_default_timeout(self, table_name: str):
    """Test that regular tables get the default timeout."""
    assert _get_staging_timeout(table_name) == DEFAULT_STAGING_TIMEOUT

  def test_unknown_table_gets_default_timeout(self):
    """Test that unknown table names get the default timeout."""
    assert _get_staging_timeout("UnknownTable") == DEFAULT_STAGING_TIMEOUT
    assert _get_staging_timeout("") == DEFAULT_STAGING_TIMEOUT
    assert _get_staging_timeout("random_table_name") == DEFAULT_STAGING_TIMEOUT

  def test_case_sensitivity(self):
    """Test that table name matching is case-sensitive."""
    # Exact case should match
    assert _get_staging_timeout("Fact") == LARGE_TABLE_STAGING_TIMEOUT

    # Different case should NOT match (gets default timeout)
    assert _get_staging_timeout("fact") == DEFAULT_STAGING_TIMEOUT
    assert _get_staging_timeout("FACT") == DEFAULT_STAGING_TIMEOUT
    assert _get_staging_timeout("fAcT") == DEFAULT_STAGING_TIMEOUT
