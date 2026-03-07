"""Tests for SEC pipeline configuration classes and constants."""

from datetime import UTC, datetime

import pytest

from robosystems.adapters.sec.pipeline.configs import (
  SEC_FORM_TYPE_BATCHES,
  SEC_HISTORICAL_END_YEAR,
  SEC_HISTORICAL_FORM_TYPES,
  SEC_PRIMARY_START_YEAR,
  SEC_QUARTERS,
  SEC_START_YEAR,
  SECDownloadConfig,
  SECEntityUpdateConfig,
  SECHistoricalStageConfig,
  SECIncrementalStageConfig,
  SECMaterializeConfig,
  SECProcessConfig,
  SECStageConfig,
  sec_quarter_partitions,
)
from robosystems.config.constants import SEC_PROCESS_BATCH_SIZE


@pytest.mark.unit
class TestSECConstants:
  """Tests for SEC pipeline constants."""

  def test_sec_start_year(self):
    """Test SEC start year is 2009 (when XBRL filings began)."""
    assert SEC_START_YEAR == 2009

  def test_sec_primary_start_year(self):
    """Test SEC primary graph starts at 2024."""
    assert SEC_PRIMARY_START_YEAR == 2024

  def test_sec_historical_end_year(self):
    """Test SEC historical graph ends at 2023."""
    assert SEC_HISTORICAL_END_YEAR == 2023

  def test_historical_form_types(self):
    """Test historical form types include annual reports only."""
    assert "10-K" in SEC_HISTORICAL_FORM_TYPES
    assert "20-F" in SEC_HISTORICAL_FORM_TYPES
    assert "40-F" in SEC_HISTORICAL_FORM_TYPES
    assert "10-Q" not in SEC_HISTORICAL_FORM_TYPES
    assert "DEF 14A" not in SEC_HISTORICAL_FORM_TYPES

  def test_form_type_batches_structure(self):
    """Test form type batches are structured as expected."""
    assert len(SEC_FORM_TYPE_BATCHES) == 2
    # Batch 1: Core financials
    assert "10-K" in SEC_FORM_TYPE_BATCHES[0]
    assert "10-Q" in SEC_FORM_TYPE_BATCHES[0]
    # Batch 2: Supplementary
    assert "DEF 14A" in SEC_FORM_TYPE_BATCHES[1]
    assert "S-1" in SEC_FORM_TYPE_BATCHES[1]

  def test_form_type_batches_no_overlap(self):
    """Test form type batches have no overlapping forms."""
    all_forms = []
    for batch in SEC_FORM_TYPE_BATCHES:
      all_forms.extend(batch)
    assert len(all_forms) == len(set(all_forms))

  def test_quarters_start_from_start_year(self):
    """Test quarters start from SEC_START_YEAR-Q1."""
    assert SEC_QUARTERS[0] == f"{SEC_START_YEAR}-Q1"

  def test_quarters_include_current_year(self):
    """Test quarters include the current year."""
    current_year = datetime.now(UTC).year
    assert f"{current_year}-Q1" in SEC_QUARTERS
    assert f"{current_year}-Q4" in SEC_QUARTERS

  def test_quarters_format(self):
    """Test all quarters follow the YYYY-QN format."""
    for q in SEC_QUARTERS:
      parts = q.split("-Q")
      assert len(parts) == 2
      year = int(parts[0])
      quarter = int(parts[1])
      assert SEC_START_YEAR <= year <= datetime.now(UTC).year + 1
      assert 1 <= quarter <= 4

  def test_quarters_are_sequential(self):
    """Test quarters are in sequential order."""
    for i in range(len(SEC_QUARTERS) - 1):
      year1, q1 = SEC_QUARTERS[i].split("-Q")
      year2, q2 = SEC_QUARTERS[i + 1].split("-Q")
      val1 = int(year1) * 4 + int(q1)
      val2 = int(year2) * 4 + int(q2)
      assert val2 == val1 + 1

  def test_quarter_partitions_is_static_partitions(self):
    """Test sec_quarter_partitions is a StaticPartitionsDefinition."""
    from dagster import StaticPartitionsDefinition

    assert isinstance(sec_quarter_partitions, StaticPartitionsDefinition)

  def test_quarter_partitions_keys_match_quarters(self):
    """Test partition keys match SEC_QUARTERS list."""
    partition_keys = sec_quarter_partitions.get_partition_keys()
    assert list(partition_keys) == SEC_QUARTERS


@pytest.mark.unit
class TestSECDownloadConfig:
  """Tests for SECDownloadConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECDownloadConfig()
    assert config.skip_existing is True
    assert config.skip_submissions is False
    assert config.max_filings == 0
    assert config.dry_run is False
    assert config.tickers == []
    assert config.ciks == []

  def test_default_form_types(self):
    """Test default form types include all expected types."""
    config = SECDownloadConfig()
    assert "10-K" in config.form_types
    assert "10-Q" in config.form_types
    assert "20-F" in config.form_types
    assert "40-F" in config.form_types
    assert "DEF 14A" in config.form_types
    assert "S-1" in config.form_types

  def test_default_concurrency_settings(self):
    """Test default concurrency settings."""
    config = SECDownloadConfig()
    assert config.submissions_rate == 8.0
    assert config.submissions_concurrency == 5
    assert config.download_rate == 5.0
    assert config.download_concurrency == 10

  def test_custom_values(self):
    """Test custom configuration values."""
    config = SECDownloadConfig(
      skip_existing=False,
      skip_submissions=True,
      form_types=["10-K"],
      tickers=["AAPL"],
      max_filings=100,
      dry_run=True,
      download_rate=2.0,
    )
    assert config.skip_existing is False
    assert config.skip_submissions is True
    assert config.form_types == ["10-K"]
    assert config.tickers == ["AAPL"]
    assert config.max_filings == 100
    assert config.dry_run is True
    assert config.download_rate == 2.0


@pytest.mark.unit
class TestSECProcessConfig:
  """Tests for SECProcessConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECProcessConfig()
    assert config.batch_size == SEC_PROCESS_BATCH_SIZE
    assert config.continue_on_error is True
    assert config.enable_cache is True
    assert config.form_types is None

  def test_custom_batch_size(self):
    """Test custom batch size."""
    config = SECProcessConfig(batch_size=100)
    assert config.batch_size == 100

  def test_form_type_filter(self):
    """Test form type filter."""
    config = SECProcessConfig(form_types=["10-K", "20-F"])
    assert config.form_types == ["10-K", "20-F"]

  def test_stop_on_error(self):
    """Test stop on error configuration."""
    config = SECProcessConfig(continue_on_error=False)
    assert config.continue_on_error is False


@pytest.mark.unit
class TestSECStageConfig:
  """Tests for SECStageConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECStageConfig()
    assert config.graph_id == "sec"
    assert config.year is None
    assert config.start_year == SEC_PRIMARY_START_YEAR
    assert config.end_year is None
    assert config.reset_staging is False
    assert config.skip_taxonomy_relationships is False

  def test_year_filter(self):
    """Test single year filter."""
    config = SECStageConfig(year=2024)
    assert config.year == 2024

  def test_year_range(self):
    """Test year range configuration."""
    config = SECStageConfig(start_year=2020, end_year=2023)
    assert config.start_year == 2020
    assert config.end_year == 2023

  def test_reset_staging(self):
    """Test reset staging flag."""
    config = SECStageConfig(reset_staging=True)
    assert config.reset_staging is True


@pytest.mark.unit
class TestSECHistoricalStageConfig:
  """Tests for SECHistoricalStageConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECHistoricalStageConfig()
    assert config.graph_id == "sec_historical"
    assert config.start_year == SEC_START_YEAR
    assert config.end_year == SEC_HISTORICAL_END_YEAR
    assert config.reset_staging is False
    assert config.skip_taxonomy_relationships is False

  def test_custom_year_range(self):
    """Test custom year range."""
    config = SECHistoricalStageConfig(start_year=2015, end_year=2020)
    assert config.start_year == 2015
    assert config.end_year == 2020


@pytest.mark.unit
class TestSECIncrementalStageConfig:
  """Tests for SECIncrementalStageConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECIncrementalStageConfig()
    assert config.graph_id == "sec"
    assert config.year is None
    assert config.quarter is None
    assert config.skip_taxonomy_relationships is False

  def test_specific_quarter(self):
    """Test specific quarter configuration."""
    config = SECIncrementalStageConfig(year=2024, quarter=2)
    assert config.year == 2024
    assert config.quarter == 2

  def test_quarter_validation_min(self):
    """Test quarter validation - minimum is 1."""
    with pytest.raises(Exception):
      SECIncrementalStageConfig(quarter=0)

  def test_quarter_validation_max(self):
    """Test quarter validation - maximum is 4."""
    with pytest.raises(Exception):
      SECIncrementalStageConfig(quarter=5)


@pytest.mark.unit
class TestSECMaterializeConfig:
  """Tests for SECMaterializeConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECMaterializeConfig()
    assert config.graph_id == "sec"
    assert config.rebuild_graph is True
    assert config.skip_taxonomy_relationships is False
    assert config.batch_materialization is True
    assert config.materialization_batch_size == 20_000_000

  def test_no_rebuild(self):
    """Test configuration without rebuild."""
    config = SECMaterializeConfig(rebuild_graph=False)
    assert config.rebuild_graph is False

  def test_custom_batch_size(self):
    """Test custom batch size."""
    config = SECMaterializeConfig(materialization_batch_size=5_000_000)
    assert config.materialization_batch_size == 5_000_000

  def test_batch_size_minimum(self):
    """Test batch size minimum validation (1,000,000)."""
    with pytest.raises(Exception):
      SECMaterializeConfig(materialization_batch_size=500_000)


@pytest.mark.unit
class TestSECEntityUpdateConfig:
  """Tests for SECEntityUpdateConfig."""

  def test_default_values(self):
    """Test default configuration values."""
    config = SECEntityUpdateConfig()
    assert config.graph_id == "sec"
    assert config.year is None
    assert config.quarter is None

  def test_specific_quarter(self):
    """Test specific quarter/year."""
    config = SECEntityUpdateConfig(year=2025, quarter=1)
    assert config.year == 2025
    assert config.quarter == 1
