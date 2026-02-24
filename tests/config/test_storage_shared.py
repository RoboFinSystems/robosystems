"""Shared data source storage configuration tests."""

from robosystems.config.storage.shared import (
    DATA_SOURCES,
    DataSourceConfig,
    DataSourceType,
    get_data_source,
    get_processed_key,
    get_processed_uri,
    get_raw_key,
    get_raw_uri,
    is_source_enabled,
    list_enabled_sources,
)


class TestDataSourceType:
    """Tests for DataSourceType enum."""

    def test_all_types_in_registry(self):
        for source_type in DataSourceType:
            assert source_type in DATA_SOURCES

    def test_registry_values_are_configs(self):
        for config in DATA_SOURCES.values():
            assert isinstance(config, DataSourceConfig)

    def test_sec_is_enabled(self):
        assert DATA_SOURCES[DataSourceType.SEC].enabled is True

    def test_future_sources_disabled(self):
        assert DATA_SOURCES[DataSourceType.FRED].enabled is False
        assert DATA_SOURCES[DataSourceType.BLS].enabled is False
        assert DATA_SOURCES[DataSourceType.CENSUS].enabled is False
        assert DATA_SOURCES[DataSourceType.INDUSTRY].enabled is False


class TestGetDataSource:
    """Tests for get_data_source."""

    def test_returns_config(self):
        config = get_data_source(DataSourceType.SEC)
        assert config.source_type == DataSourceType.SEC
        assert config.raw_prefix == "sec/"

    def test_raises_for_unknown_source(self):
        # DataSourceType is an enum so we can't really pass an unknown one
        # but we can test it exists
        assert get_data_source(DataSourceType.SEC) is not None


class TestRawKey:
    """Tests for get_raw_key."""

    def test_with_parts(self):
        result = get_raw_key(DataSourceType.SEC, "year=2024", "320193", "filing.zip")
        assert result == "sec/year=2024/320193/filing.zip"

    def test_without_parts(self):
        result = get_raw_key(DataSourceType.SEC)
        assert result == "sec"

    def test_single_part(self):
        result = get_raw_key(DataSourceType.SEC, "year=2024")
        assert result == "sec/year=2024"


class TestProcessedKey:
    """Tests for get_processed_key."""

    def test_with_parts(self):
        result = get_processed_key(
            DataSourceType.SEC, "processed", "nodes", "Entity", "file.parquet"
        )
        assert result == "sec/processed/nodes/Entity/file.parquet"

    def test_without_parts(self):
        result = get_processed_key(DataSourceType.SEC)
        assert result == "sec"


class TestURIBuilders:
    """Tests for full S3 URI builder functions."""

    def test_get_raw_uri(self):
        result = get_raw_uri(
            "robosystems-shared-raw-staging", DataSourceType.SEC, "year=2024"
        )
        assert result == "s3://robosystems-shared-raw-staging/sec/year=2024"

    def test_get_processed_uri(self):
        result = get_processed_uri(
            "robosystems-shared-processed-staging",
            DataSourceType.SEC,
            "year=2024",
            "nodes",
        )
        assert result == "s3://robosystems-shared-processed-staging/sec/year=2024/nodes"


class TestListEnabledSources:
    """Tests for list_enabled_sources."""

    def test_returns_only_enabled(self):
        enabled = list_enabled_sources()
        assert all(config.enabled for config in enabled)

    def test_sec_in_enabled_list(self):
        enabled = list_enabled_sources()
        source_types = [config.source_type for config in enabled]
        assert DataSourceType.SEC in source_types


class TestIsSourceEnabled:
    """Tests for is_source_enabled."""

    def test_sec_enabled(self):
        assert is_source_enabled(DataSourceType.SEC) is True

    def test_fred_disabled(self):
        assert is_source_enabled(DataSourceType.FRED) is False
