"""Tests for the per-CIK SEC entity-sync pipeline.

`sec_entity_sync` is a live Dagster job -- `SECProvider` submits it when a
customer connects a SEC data source -- so the extract → transform → load
chain runs against customer graphs. Covers filing discovery, the
consolidation/dedup step, and the node-before-relationship load ordering
that foreign-key constraints depend on.
"""

import json
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dagster import build_asset_context

from robosystems.adapters.sec.pipeline.entity_sync.configs import SECEntitySyncConfig
from robosystems.adapters.sec.pipeline.entity_sync.extract import sec_entity_extract
from robosystems.adapters.sec.pipeline.entity_sync.load import (
  _count_parquet_rows,
  sec_entity_load,
)
from robosystems.adapters.sec.pipeline.entity_sync.transform import sec_entity_transform
from robosystems.adapters.sec.pipeline.entity_sync.utils import get_pipeline_work_dir

EXTRACT = "robosystems.adapters.sec.pipeline.entity_sync.extract"
TRANSFORM = "robosystems.adapters.sec.pipeline.entity_sync.transform"
LOAD = "robosystems.adapters.sec.pipeline.entity_sync.load"


def _config(**overrides):
  defaults = {
    "graph_id": "kg1a2b3c4d5e6f78",
    "connection_id": "conn_1",
    "user_id": "usr_1",
    "cik": "0000320193",
  }
  defaults.update(overrides)
  return SECEntitySyncConfig(**defaults)


@pytest.fixture
def work_dir(tmp_path):
  """Pin the shared pipeline work directory into tmp_path."""
  d = tmp_path / "work"
  d.mkdir()
  with (
    patch(f"{EXTRACT}.get_pipeline_work_dir", return_value=d),
    patch(f"{TRANSFORM}.get_pipeline_work_dir", return_value=d),
    patch(f"{LOAD}.get_pipeline_work_dir", return_value=d),
  ):
    yield d


@pytest.fixture
def pyarrow_local_io(tmp_path):
  """Skip if pyarrow's local-file scheme has been claimed by another library.

  Importing `networkit` (pulled in by the SEC knowledge package's PageRank
  path, which sorts earlier in a full-suite run) registers the ``file`` URI
  scheme that pyarrow also registers, after which every pyarrow *filesystem*
  operation raises ``ArrowKeyError``. In-memory ops are unaffected, so only
  the tests that reach `pq.write_table(..., path)` or `pq.read_metadata` need
  this -- see commit a5b7baad.

  Probes the capability rather than sniffing `sys.modules`: the collision is
  platform-specific (macOS hits it, Linux/CI does not), so CI keeps full
  coverage of these paths while a local run degrades to a skip instead of
  tripping `-x` and halting the suite.
  """
  try:
    pq.write_table(pa.table({"probe": [1]}), str(tmp_path / "_probe.parquet"))
  except pa.ArrowKeyError as exc:  # pragma: no cover - platform-dependent
    pytest.skip(f"pyarrow local-file I/O unavailable in this process: {exc}")


def _parquet_bytes(rows):
  """Serialise rows to parquet bytes as `process_single_filing_to_memory` does."""
  buf = BytesIO()
  pq.write_table(pa.Table.from_pylist(rows), buf)
  return buf.getvalue()


def _filing_result(tables=None, success=True, error=None, skipped_reason=None):
  result = MagicMock()
  result.success = success
  result.error = error
  result.skipped_reason = skipped_reason
  result.tables = tables if tables is not None else {}
  return result


@pytest.mark.unit
class TestWorkDir:
  def test_is_deterministic_per_graph_and_created(self):
    a = get_pipeline_work_dir("kg_alpha")
    b = get_pipeline_work_dir("kg_alpha")
    c = get_pipeline_work_dir("kg_beta")

    assert a == b, "all three assets must land on the same directory"
    assert a != c, "graphs must not share a work directory"
    assert a.exists()


@pytest.mark.unit
class TestExtract:
  def _s3_with(self, year_prefixes, objects_by_prefix, has_submissions=True):
    paginator = MagicMock()

    def paginate(**kwargs):
      prefix = kwargs["Prefix"]
      if kwargs.get("Delimiter") == "/":
        return [{"CommonPrefixes": [{"Prefix": p} for p in year_prefixes]}]
      return [{"Contents": objects_by_prefix.get(prefix, [])}]

    paginator.paginate.side_effect = paginate
    s3 = MagicMock()
    s3.get_paginator.return_value = paginator
    if not has_submissions:
      s3.head_object.side_effect = RuntimeError("404")
    return s3

  def test_discovers_zips_across_year_partitions(self, work_dir):
    cik = "0000320193"
    s3 = self._s3_with(
      year_prefixes=["sec/year=2023/", "sec/year=2024/"],
      objects_by_prefix={
        f"sec/year=2023/{cik}/": [
          {"Key": f"sec/year=2023/{cik}/acc-1.zip", "Size": 100}
        ],
        f"sec/year=2024/{cik}/": [
          {"Key": f"sec/year=2024/{cik}/acc-2.zip", "Size": 200}
        ],
      },
    )

    with patch("boto3.client", return_value=s3):
      result = sec_entity_extract(build_asset_context(), _config(cik=cik))

    assert result.metadata["filings_found"] == 2
    assert result.metadata["years"] == ["2023", "2024"]

    manifest = json.loads((work_dir / "filing_manifest.json").read_text())
    assert [f["accession"] for f in manifest["filings"]] == ["acc-1", "acc-2"]
    assert manifest["filings"][0]["year"] == "2023"
    assert manifest["filings"][0]["size_bytes"] == 100

  def test_ignores_non_zip_objects(self, work_dir):
    cik = "0000320193"
    s3 = self._s3_with(
      year_prefixes=["sec/year=2024/"],
      objects_by_prefix={
        f"sec/year=2024/{cik}/": [
          {"Key": f"sec/year=2024/{cik}/acc-1.zip", "Size": 100},
          {"Key": f"sec/year=2024/{cik}/notes.txt", "Size": 5},
          {"Key": f"sec/year=2024/{cik}/index.json", "Size": 5},
        ]
      },
    )

    with patch("boto3.client", return_value=s3):
      result = sec_entity_extract(build_asset_context(), _config(cik=cik))

    assert result.metadata["filings_found"] == 1

  def test_records_missing_submissions_metadata(self, work_dir):
    s3 = self._s3_with(year_prefixes=[], objects_by_prefix={}, has_submissions=False)

    with patch("boto3.client", return_value=s3):
      result = sec_entity_extract(build_asset_context(), _config())

    assert result.metadata["has_submissions"] is False
    manifest = json.loads((work_dir / "filing_manifest.json").read_text())
    assert manifest["submissions_key"] is None

  def test_writes_manifest_even_when_no_filings_found(self, work_dir):
    """Transform reads this manifest unconditionally; a missing file would
    turn an empty CIK into a hard pipeline failure."""
    s3 = self._s3_with(year_prefixes=[], objects_by_prefix={})

    with patch("boto3.client", return_value=s3):
      result = sec_entity_extract(build_asset_context(), _config())

    assert result.metadata["filings_found"] == 0
    manifest = json.loads((work_dir / "filing_manifest.json").read_text())
    assert manifest["filings"] == []
    assert manifest["form_types"] == ["10-K", "10-Q", "20-F", "40-F"]


@pytest.mark.unit
class TestTransform:
  def _write_manifest(self, work_dir, filings, form_types=None):
    (work_dir / "filing_manifest.json").write_text(
      json.dumps(
        {
          "cik": "0000320193",
          "filings": filings,
          "bucket": "raw-bucket",
          "form_types": form_types or ["10-K"],
        }
      )
    )

  def test_missing_manifest_is_a_clear_error(self, work_dir):
    with pytest.raises(ValueError, match="Run sec_entity_extract first"):
      sec_entity_transform(build_asset_context(), _config())

  def test_returns_zeroed_metadata_when_no_filings(self, work_dir):
    self._write_manifest(work_dir, [])

    result = sec_entity_transform(build_asset_context(), _config())

    assert result.metadata["filings_processed"] == 0
    assert result.metadata["tables_output"] == 0

  def test_consolidates_tables_across_filings(self, work_dir, pyarrow_local_io):
    self._write_manifest(
      work_dir,
      [
        {"storage_key": "k1", "accession": "acc-1", "year": "2023"},
        {"storage_key": "k2", "accession": "acc-2", "year": "2024"},
      ],
    )
    results = [
      _filing_result(
        {"nodes/Fact": _parquet_bytes([{"identifier": "f1", "value": 1}])}
      ),
      _filing_result(
        {"nodes/Fact": _parquet_bytes([{"identifier": "f2", "value": 2}])}
      ),
    ]

    with (
      patch("boto3.client"),
      patch("robosystems.adapters.sec.processors.metadata.SECMetadataLoader"),
      patch(
        "robosystems.adapters.sec.processors.processing.process_single_filing_to_memory",
        side_effect=results,
      ),
    ):
      result = sec_entity_transform(build_asset_context(), _config())

    assert result.metadata["filings_processed"] == 2
    assert result.metadata["tables_output"] == 1

    out = pq.read_table(str(work_dir / "output" / "sec_Fact.parquet"))
    assert out.num_rows == 2, "facts from both filings must survive"

  def test_dedups_shared_node_tables_on_identifier(self, work_dir, pyarrow_local_io):
    """Element/Label/etc. use content-derived ids, so the same element
    appearing in two filings must collapse to one row."""
    self._write_manifest(
      work_dir,
      [
        {"storage_key": "k1", "accession": "acc-1", "year": "2023"},
        {"storage_key": "k2", "accession": "acc-2", "year": "2024"},
      ],
    )
    shared = _parquet_bytes(
      [
        {"identifier": "el-1", "qname": "Revenue"},
        {"identifier": "el-2", "qname": "Cost"},
      ]
    )
    results = [
      _filing_result({"nodes/Element": shared}),
      _filing_result({"nodes/Element": shared}),
    ]

    with (
      patch("boto3.client"),
      patch("robosystems.adapters.sec.processors.metadata.SECMetadataLoader"),
      patch(
        "robosystems.adapters.sec.processors.processing.process_single_filing_to_memory",
        side_effect=results,
      ),
    ):
      sec_entity_transform(build_asset_context(), _config())

    out = pq.read_table(str(work_dir / "output" / "sec_Element.parquet"))
    assert out.num_rows == 2, "duplicate elements must be deduped to 2, not 4"
    assert sorted(out.column("identifier").to_pylist()) == ["el-1", "el-2"]

  def test_non_shared_tables_are_not_deduped(self, work_dir, pyarrow_local_io):
    """Facts legitimately repeat identifiers across periods -- dedup here
    would silently drop customer data."""
    self._write_manifest(
      work_dir,
      [
        {"storage_key": "k1", "accession": "acc-1", "year": "2023"},
        {"storage_key": "k2", "accession": "acc-2", "year": "2024"},
      ],
    )
    same = _parquet_bytes([{"identifier": "dup", "value": 1}])
    results = [
      _filing_result({"nodes/Fact": same}),
      _filing_result({"nodes/Fact": same}),
    ]

    with (
      patch("boto3.client"),
      patch("robosystems.adapters.sec.processors.metadata.SECMetadataLoader"),
      patch(
        "robosystems.adapters.sec.processors.processing.process_single_filing_to_memory",
        side_effect=results,
      ),
    ):
      sec_entity_transform(build_asset_context(), _config())

    out = pq.read_table(str(work_dir / "output" / "sec_Fact.parquet"))
    assert out.num_rows == 2

  def test_counts_failed_and_skipped_filings_without_aborting(
    self, work_dir, pyarrow_local_io
  ):
    self._write_manifest(
      work_dir,
      [
        {"storage_key": "k1", "accession": "ok", "year": "2023"},
        {"storage_key": "k2", "accession": "failed", "year": "2023"},
        {"storage_key": "k3", "accession": "skipped", "year": "2023"},
        {"storage_key": "k4", "accession": "empty", "year": "2023"},
      ],
    )
    results = [
      _filing_result({"nodes/Fact": _parquet_bytes([{"identifier": "f1"}])}),
      _filing_result(success=False, error="corrupt zip"),
      _filing_result(skipped_reason="form type not requested"),
      _filing_result(tables={}),
    ]

    with (
      patch("boto3.client"),
      patch("robosystems.adapters.sec.processors.metadata.SECMetadataLoader"),
      patch(
        "robosystems.adapters.sec.processors.processing.process_single_filing_to_memory",
        side_effect=results,
      ),
    ):
      result = sec_entity_transform(build_asset_context(), _config())

    assert result.metadata["filings_processed"] == 1
    assert result.metadata["filings_failed"] == 1
    assert result.metadata["filings_skipped"] == 2

  def test_raised_exception_counts_as_failure_not_crash(
    self, work_dir, pyarrow_local_io
  ):
    """One bad filing must not abandon the other 40 in a backfill."""
    self._write_manifest(
      work_dir,
      [
        {"storage_key": "k1", "accession": "boom", "year": "2023"},
        {"storage_key": "k2", "accession": "ok", "year": "2023"},
      ],
    )
    results = [
      RuntimeError("arelle exploded"),
      _filing_result({"nodes/Fact": _parquet_bytes([{"identifier": "f1"}])}),
    ]

    with (
      patch("boto3.client"),
      patch("robosystems.adapters.sec.processors.metadata.SECMetadataLoader"),
      patch(
        "robosystems.adapters.sec.processors.processing.process_single_filing_to_memory",
        side_effect=results,
      ),
    ):
      result = sec_entity_transform(build_asset_context(), _config())

    assert result.metadata["filings_failed"] == 1
    assert result.metadata["filings_processed"] == 1

  def test_writes_table_metadata_for_the_load_step(self, work_dir, pyarrow_local_io):
    self._write_manifest(
      work_dir, [{"storage_key": "k1", "accession": "acc-1", "year": "2023"}]
    )
    tables = {
      "nodes/Fact": _parquet_bytes([{"identifier": "f1"}]),
      "relationships/HAS_FACT": _parquet_bytes([{"from": "a", "to": "b"}]),
    }

    with (
      patch("boto3.client"),
      patch("robosystems.adapters.sec.processors.metadata.SECMetadataLoader"),
      patch(
        "robosystems.adapters.sec.processors.processing.process_single_filing_to_memory",
        return_value=_filing_result(tables),
      ),
    ):
      sec_entity_transform(build_asset_context(), _config())

    meta = json.loads((work_dir / "table_metadata.json").read_text())
    assert meta["Fact"]["entity_type"] == "nodes"
    assert meta["HAS_FACT"]["entity_type"] == "relationships"

  def test_enrichment_failure_is_non_fatal(self, work_dir):
    """Enrichment is optional; a broken enricher must not fail the sync."""
    self._write_manifest(work_dir, [])

    with (
      patch("boto3.client"),
      patch("robosystems.adapters.sec.processors.metadata.SECMetadataLoader"),
    ):
      result = sec_entity_transform(
        build_asset_context(), _config(skip_enrichment=False)
      )

    assert result.metadata["filings_processed"] == 0


@pytest.mark.unit
class TestParquetRowCount:
  def test_counts_rows(self, tmp_path, pyarrow_local_io):
    path = tmp_path / "t.parquet"
    pq.write_table(pa.Table.from_pylist([{"a": 1}, {"a": 2}, {"a": 3}]), str(path))

    assert _count_parquet_rows(path) == 3

  def test_unreadable_file_counts_as_zero(self, tmp_path, pyarrow_local_io):
    """Row count only feeds progress metadata -- it must never abort a load."""
    path = tmp_path / "broken.parquet"
    path.write_bytes(b"not a parquet file")

    assert _count_parquet_rows(path) == 0


@pytest.mark.unit
class TestLoad:
  def test_missing_table_metadata_is_a_clear_error(self, work_dir):
    with pytest.raises(ValueError, match="Run sec_entity_transform first"):
      sec_entity_load(build_asset_context(), _config())

  def test_stages_nodes_before_relationships(
    self, work_dir, tmp_path, pyarrow_local_io
  ):
    """Relationship rows reference node ids, so a relationship staged first
    would violate foreign-key constraints at materialization."""
    (work_dir / "table_metadata.json").write_text(
      json.dumps(
        {
          "HAS_FACT": {"entity_type": "relationships", "table_name": "HAS_FACT"},
          "Fact": {"entity_type": "nodes", "table_name": "Fact"},
          "Entity": {"entity_type": "nodes", "table_name": "Entity"},
        }
      )
    )
    output = work_dir / "output"
    output.mkdir()
    for name in ("HAS_FACT", "Fact", "Entity"):
      pq.write_table(
        pa.Table.from_pylist([{"identifier": "x"}]), str(output / f"sec_{name}.parquet")
      )

    staged_order = []
    client = MagicMock()

    async def create_table(**kwargs):
      staged_order.append(kwargs["table_name"])

    client.create_table = create_table
    client.materialize_table = AsyncMock(return_value={"rows_ingested": 1})

    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)

    graph_file = MagicMock(id="gf_1")
    graph_table = MagicMock(id="gt_1", row_count=0, file_count=0, total_size_bytes=0)

    with (
      patch("robosystems.database.SessionFactory", return_value=session),
      patch("robosystems.operations.aws.s3.S3Client"),
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        AsyncMock(return_value=client),
      ),
      patch(
        "robosystems.models.core.graph.graph_table.GraphTable.get_by_name",
        return_value=graph_table,
      ),
      patch(
        "robosystems.models.core.graph.graph_file.GraphFile.create",
        return_value=graph_file,
      ),
      patch(
        "robosystems.models.core.graph.graph_file.GraphFile.get_all_for_table",
        return_value=[],
      ),
      patch(
        "robosystems.operations.connection_service.ConnectionService.update_last_sync",
        AsyncMock(),
      ),
    ):
      result = sec_entity_load(build_asset_context(), _config())

    assert staged_order[-1] == "HAS_FACT", "relationships must stage last"
    assert set(staged_order[:2]) == {"Fact", "Entity"}
    assert result.metadata["tables_staged"] == 3
    assert result.metadata["tables_materialized"] == 3

  def test_skips_tables_with_no_parquet_on_disk(self, work_dir):
    (work_dir / "table_metadata.json").write_text(
      json.dumps({"Ghost": {"entity_type": "nodes", "table_name": "Ghost"}})
    )
    (work_dir / "output").mkdir()

    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)

    with (
      patch("robosystems.database.SessionFactory", return_value=session),
      patch("robosystems.operations.aws.s3.S3Client"),
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        AsyncMock(return_value=MagicMock()),
      ),
      patch(
        "robosystems.operations.connection_service.ConnectionService.update_last_sync",
        AsyncMock(),
      ),
    ):
      result = sec_entity_load(build_asset_context(), _config())

    assert result.metadata["tables_staged"] == 0
    assert result.metadata["tables_materialized"] == 0


@pytest.mark.unit
class TestJobRegistration:
  def test_job_is_named_for_the_provider_submission(self):
    """`SECProvider` submits this job by name; a rename would break the
    connect flow silently."""
    from robosystems.adapters.sec.pipeline.entity_sync.jobs import (
      sec_entity_sync_job,
    )

    assert sec_entity_sync_job.name == "sec_entity_sync"

  def test_pipeline_exports_the_three_assets(self):
    from robosystems.adapters.sec.pipeline import entity_sync

    for name in ("sec_entity_extract", "sec_entity_transform", "sec_entity_load"):
      assert hasattr(entity_sync, name), f"{name} must stay importable"
