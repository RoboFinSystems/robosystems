"""LadybugDB metrics collector tests."""

from unittest.mock import patch

import pytest

from robosystems.graph_api.core.metrics_collector import (
  LadybugMetricsCollector,
  NoOpCounter,
  NoOpHistogram,
  NoOpMeter,
  NoOpMetrics,
)


class TestNoOpClasses:
  """Tests for no-op metric classes."""

  def test_noop_meter_create_gauge(self):
    meter = NoOpMeter()
    result = meter.create_observable_gauge("test", callbacks=[])
    assert result is None

  def test_noop_meter_create_histogram(self):
    meter = NoOpMeter()
    result = meter.create_histogram("test")
    assert result is None

  def test_noop_meter_create_counter(self):
    meter = NoOpMeter()
    result = meter.create_counter("test")
    assert result is None

  def test_noop_metrics_get_meter(self):
    metrics = NoOpMetrics()
    meter = metrics.get_meter("test", "1.0")
    assert isinstance(meter, NoOpMeter)

  def test_noop_counter_add(self):
    counter = NoOpCounter()
    counter.add(1, {"key": "value"})  # Should not raise

  def test_noop_histogram_record(self):
    histogram = NoOpHistogram()
    histogram.record(1.5, {"key": "value"})  # Should not raise


class TestLadybugMetricsCollector:
  """Tests for LadybugMetricsCollector."""

  @pytest.fixture
  def collector(self, tmp_path):
    """Create collector with temp directory."""
    with patch("robosystems.graph_api.core.metrics_collector.metrics", NoOpMetrics()):
      return LadybugMetricsCollector(base_path=str(tmp_path), node_type="test_writer")

  def test_init(self, collector):
    assert collector.node_type == "test_writer"

  def test_get_directory_size_empty(self, collector, tmp_path):
    size = collector._get_directory_size(tmp_path)
    assert size == 0

  def test_get_directory_size_with_files(self, collector, tmp_path):
    (tmp_path / "file1.txt").write_text("hello")
    (tmp_path / "file2.txt").write_text("world!")
    size = collector._get_directory_size(tmp_path)
    assert size > 0

  def test_get_directory_size_nested(self, collector, tmp_path):
    subdir = tmp_path / "sub"
    subdir.mkdir()
    (subdir / "file.txt").write_text("data")
    size = collector._get_directory_size(tmp_path)
    assert size > 0

  def test_get_database_sizes_empty(self, collector):
    sizes = collector._get_database_sizes()
    assert isinstance(sizes, dict)

  def test_get_database_sizes_with_dirs(self, collector, tmp_path):
    db_dir = tmp_path / "testdb.lbug"
    db_dir.mkdir()
    (db_dir / "data.bin").write_bytes(b"\x00" * 100)
    sizes = collector._get_database_sizes()
    assert isinstance(sizes, dict)

  def test_get_database_sizes_caching(self, collector, tmp_path):
    # First call fills cache
    sizes1 = collector._get_database_sizes()
    # Second call uses cache (within TTL)
    sizes2 = collector._get_database_sizes()
    assert sizes1 == sizes2

  def test_record_query(self, collector):
    collector.record_query("testdb", 50.0, success=True)
    metrics = collector.get_query_metrics()
    assert metrics["total_queries"] == 1
    assert "testdb" in metrics["queries_by_database"]
    assert metrics["queries_by_database"]["testdb"] == 1

  def test_record_query_multiple(self, collector):
    collector.record_query("db1", 10.0)
    collector.record_query("db1", 20.0)
    collector.record_query("db2", 30.0)
    metrics = collector.get_query_metrics()
    assert metrics["total_queries"] == 3
    assert metrics["queries_by_database"]["db1"] == 2
    assert metrics["queries_by_database"]["db2"] == 1

  def test_record_database_operation(self, collector):
    collector.record_database_operation("create", "testdb", success=True)
    # Should not raise

  def test_collect_database_metrics(self, collector):
    metrics = collector.collect_database_metrics()
    assert "count" in metrics
    assert "total_size_gb" in metrics
    assert "databases" in metrics
    assert isinstance(metrics["databases"], dict)

  def test_collect_database_metrics_cached(self, collector):
    metrics = collector.collect_database_metrics_cached()
    assert "count" in metrics
    assert metrics.get("cached") is True

  def test_get_query_metrics(self, collector):
    metrics = collector.get_query_metrics()
    assert "total_queries" in metrics
    assert "queries_by_database" in metrics
    assert "reset_time" in metrics

  @pytest.mark.asyncio
  async def test_get_database_metrics_for_usage(self, collector, tmp_path):
    db_dir = tmp_path / "testdb.lbug"
    db_dir.mkdir()
    (db_dir / "data.bin").write_bytes(b"\x00" * 100)
    # Clear cache to force refresh
    collector._db_sizes_cache = None
    collector._db_sizes_cache_time = 0

    metrics = await collector.get_database_metrics_for_usage()
    assert isinstance(metrics, list)

  @pytest.mark.asyncio
  async def test_collect_ingestion_metrics(self, collector):
    metrics = await collector.collect_ingestion_metrics()
    assert isinstance(metrics, dict)

  def test_collect_system_metrics(self, collector):
    metrics = collector.collect_system_metrics()
    assert "cpu" in metrics
    assert "memory" in metrics
    assert "disk" in metrics
    assert "usage_percent" in metrics["cpu"]
