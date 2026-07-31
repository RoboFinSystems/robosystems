"""Tests for the materialization helpers.

These build the SQL that moves staged customer data from DuckDB into
LadybugDB, so a mistake here is a silent data-correctness bug rather than a
crash: a dropped column, a truncated DECIMAL, or a keyset snapshot written
outside the staging directory.
"""

from pathlib import Path as FilePath
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graph_api.routers.databases.tables.materialize import (
  CHECKPOINT_MAX_RETRIES,
  _build_reconciled_select,
  _build_type_safe_select,
  _copy_result_rows,
  _export_incremental_keyset,
  _get_target_columns,
  _incr_keyset_path,
  _lbug_type_to_duck,
  checkpoint_with_retry,
)

MODULE = "robosystems.graph_api.routers.databases.tables.materialize"


def _arrow_result(message):
  arrow = MagicMock()
  arrow.num_rows = 1
  arrow.num_columns = 1
  cell = MagicMock()
  cell.as_py.return_value = message
  arrow.column.return_value = [cell]
  result = MagicMock()
  result.get_as_arrow.return_value = arrow
  return result


@pytest.mark.unit
class TestCopyResultRows:
  def test_parses_tuple_count_from_copy_message(self):
    assert _copy_result_rows(_arrow_result("1500 tuples have been copied"), 0) == 1500

  def test_parses_singular_tuple(self):
    assert _copy_result_rows(_arrow_result("1 tuple has been copied"), 0) == 1

  def test_falls_back_when_message_has_no_count(self):
    assert _copy_result_rows(_arrow_result("copy finished"), 42) == 42

  def test_falls_back_on_none_result(self):
    assert _copy_result_rows(None, 42) == 42

  def test_falls_back_when_arrow_access_raises(self):
    """A count is only reported metadata -- it must never fail the load."""
    result = MagicMock()
    result.get_as_arrow.side_effect = RuntimeError("arrow unavailable")

    assert _copy_result_rows(result, 42) == 42

  def test_falls_back_on_empty_arrow_table(self):
    arrow = MagicMock(num_rows=0, num_columns=0)
    result = MagicMock()
    result.get_as_arrow.return_value = arrow

    assert _copy_result_rows(result, 7) == 7


@pytest.mark.unit
class TestKeysetPathSafety:
  """`graph_id` and `table_name` arrive as URL path params and are used to
  build a filesystem path -- they must not be able to escape the staging dir."""

  def test_builds_path_under_staging(self):
    with patch(f"{MODULE}.env") as mock_env:
      mock_env.DUCKDB_STAGING_PATH = "/data/staging"
      path = _incr_keyset_path("kg1a2b3c", "Fact")

    assert path == FilePath("/data/staging/incr_keys/kg1a2b3c/Fact.parquet")

  @pytest.mark.parametrize(
    "graph_id",
    ["../../etc", "kg/../..", "kg1;rm -rf /", "kg 1", "kg\x00", "", "kg/sub"],
  )
  def test_rejects_unsafe_graph_id(self, graph_id):
    with pytest.raises(ValueError, match="Unsafe graph_id"):
      _incr_keyset_path(graph_id, "Fact")

  @pytest.mark.parametrize(
    "table_name", ["../secrets", "Fact/../..", "Fact;DROP", "", "a b"]
  )
  def test_rejects_unsafe_table_name(self, table_name):
    with pytest.raises(ValueError, match="Unsafe table_name"):
      _incr_keyset_path("kg1a2b3c", table_name)

  def test_allows_hyphens_and_underscores(self):
    with patch(f"{MODULE}.env") as mock_env:
      mock_env.DUCKDB_STAGING_PATH = "/data/staging"
      path = _incr_keyset_path("kg_1a-2b", "HAS_FACT")

    assert path.name == "HAS_FACT.parquet"


@pytest.mark.unit
class TestTypeMapping:
  @pytest.mark.parametrize(
    "lbug,duck",
    [
      ("STRING", "VARCHAR"),
      ("INT64", "BIGINT"),
      ("INT32", "INT"),
      ("DOUBLE", "DOUBLE"),
      ("BOOL", "BOOLEAN"),
      ("DATE", "DATE"),
      ("TIMESTAMP", "TIMESTAMP"),
    ],
  )
  def test_maps_known_types(self, lbug, duck):
    assert _lbug_type_to_duck(lbug) == duck

  def test_is_case_insensitive(self):
    assert _lbug_type_to_duck("string") == "VARCHAR"
    assert _lbug_type_to_duck("Int64") == "BIGINT"

  def test_passes_through_parameterized_types(self):
    """Embedding columns like FLOAT[384] are DuckDB-native -- flattening them
    to VARCHAR would corrupt vector search."""
    assert _lbug_type_to_duck("FLOAT[384]") == "FLOAT[384]"
    assert _lbug_type_to_duck("DOUBLE[3]") == "DOUBLE[3]"

  def test_unknown_scalar_defaults_to_varchar(self):
    assert _lbug_type_to_duck("SOMETHING_NEW") == "VARCHAR"


@pytest.mark.unit
class TestTypeSafeSelect:
  def test_preserves_source_column_order(self):
    select = _build_type_safe_select(
      [("identifier", "VARCHAR"), ("value", "BIGINT"), ("name", "VARCHAR")]
    )

    assert select == '"identifier", "value", "name"'

  def test_casts_decimal_to_double(self):
    """LadybugDB has no DECIMAL; postgres_scan stages NUMERIC as DECIMAL, so
    the cast has to happen at the source or Arrow types disagree."""
    select = _build_type_safe_select([("amount", "DECIMAL(18,2)")])

    assert select == 'CAST("amount" AS DOUBLE) AS "amount"'

  def test_excluded_columns_are_dropped(self):
    select = _build_type_safe_select(
      [("identifier", "VARCHAR"), ("secret", "VARCHAR")], exclude_cols={"secret"}
    )

    assert select == '"identifier"'

  def test_nullified_columns_keep_their_position_and_type(self):
    """COPY is positional -- a nullified column must still occupy its slot."""
    select = _build_type_safe_select(
      [("identifier", "VARCHAR"), ("embedding", "FLOAT[384]")],
      null_cols={"embedding"},
    )

    assert select == '"identifier", NULL::FLOAT[384] AS "embedding"'

  def test_exclusion_wins_over_nullification(self):
    select = _build_type_safe_select(
      [("a", "VARCHAR"), ("b", "VARCHAR")],
      exclude_cols={"b"},
      null_cols={"b"},
    )

    assert select == '"a"'


@pytest.mark.unit
class TestReconciledSelect:
  def test_casts_present_columns_to_target_type(self):
    select = _build_reconciled_select(
      target_columns=[("identifier", "STRING"), ("value", "INT64")],
      source_column_names=["identifier", "value"],
      source_table="staged",
    )

    assert "TRY_CAST(identifier AS VARCHAR) AS identifier" in select
    assert "TRY_CAST(value AS BIGINT) AS value" in select

  def test_missing_source_columns_become_typed_nulls(self):
    """DuckDB infers all-NULL staged columns as INT32; without the typed NULL
    the COPY would fail on a type mismatch against the target."""
    select = _build_reconciled_select(
      target_columns=[("identifier", "STRING"), ("added_later", "DOUBLE")],
      source_column_names=["identifier"],
      source_table="staged",
    )

    assert "NULL::DOUBLE AS added_later" in select

  def test_relationship_key_columns_are_passed_through_first(self):
    """TABLE_INFO omits from/to for rel tables, but COPY requires them."""
    select = _build_reconciled_select(
      target_columns=[("weight", "DOUBLE")],
      source_column_names=["from", "to", "weight"],
      source_table="staged",
    )

    assert select.startswith('"from", "to",')

  def test_duckdb_src_dst_variants_are_passed_through(self):
    select = _build_reconciled_select(
      target_columns=[("weight", "DOUBLE")],
      source_column_names=["src", "dst", "weight"],
      source_table="staged",
    )

    assert select.startswith('"src", "dst",')

  def test_implicit_columns_not_duplicated_when_target_declares_them(self):
    select = _build_reconciled_select(
      target_columns=[("from", "STRING"), ("weight", "DOUBLE")],
      source_column_names=["from", "weight"],
      source_table="staged",
    )

    columns = [part.strip() for part in select.split(",")]
    from_columns = [c for c in columns if c.endswith("from") or c == '"from"']
    assert from_columns == ["TRY_CAST(from AS VARCHAR) AS from"], (
      "the implicit pass-through must be skipped when TABLE_INFO already "
      f"declares the column, else COPY gets it twice: {select}"
    )

  def test_excluded_target_columns_are_dropped(self):
    select = _build_reconciled_select(
      target_columns=[("identifier", "STRING"), ("internal", "STRING")],
      source_column_names=["identifier", "internal"],
      source_table="staged",
      exclude_cols={"internal"},
    )

    assert "internal" not in select

  def test_nullified_target_columns_keep_their_slot(self):
    select = _build_reconciled_select(
      target_columns=[("identifier", "STRING"), ("embedding", "FLOAT[384]")],
      source_column_names=["identifier", "embedding"],
      source_table="staged",
      null_cols={"embedding"},
    )

    assert "NULL::FLOAT[384] AS embedding" in select
    assert "TRY_CAST(embedding" not in select


@pytest.mark.unit
class TestGetTargetColumns:
  def _service_returning(self, rows):
    conn = MagicMock()
    result = MagicMock()
    result.get_as_list.return_value = rows
    conn.execute.return_value = result
    service = MagicMock()
    service.db_manager.connection_pool.get_connection.return_value.__enter__.return_value = conn
    return service

  def test_parses_table_info_rows(self):
    service = self._service_returning(
      [[0, "identifier", "STRING", None, True], [1, "value", "INT64", None, False]]
    )

    assert _get_target_columns(service, "kg1", "Fact") == [
      ("identifier", "STRING"),
      ("value", "INT64"),
    ]

  def test_returns_none_when_no_columns(self):
    assert _get_target_columns(self._service_returning([]), "kg1", "Fact") is None

  def test_returns_none_when_table_missing(self):
    """A table that doesn't exist yet is normal on first materialization --
    the caller falls back to the type-safe select."""
    service = MagicMock()
    service.db_manager.connection_pool.get_connection.side_effect = RuntimeError(
      "Table Fact does not exist"
    )

    assert _get_target_columns(service, "kg1", "Fact") is None

  def test_skips_malformed_rows(self):
    service = self._service_returning([[0, "identifier", "STRING"], [1, "short"]])

    assert _get_target_columns(service, "kg1", "Fact") == [("identifier", "STRING")]


@pytest.mark.unit
class TestExportIncrementalKeyset:
  def _service(self):
    conn = MagicMock()
    service = MagicMock()
    service.db_manager.connection_pool.get_connection.return_value.__enter__.return_value = conn
    return service, conn

  def test_node_keyset_selects_identifier(self, tmp_path):
    service, conn = self._service()
    path = tmp_path / "keys" / "Fact.parquet"

    _export_incremental_keyset(service, "kg1", "Fact", is_rel=False, snapshot_path=path)

    copy_sql = [c.args[0] for c in conn.execute.call_args_list if "COPY" in c.args[0]]
    assert "MATCH (n:Fact) RETURN n.identifier AS identifier" in copy_sql[0]
    assert path.parent.exists(), "snapshot directory must be created"

  def test_relationship_keyset_selects_src_and_dst(self, tmp_path):
    service, conn = self._service()

    _export_incremental_keyset(
      service, "kg1", "HAS_FACT", is_rel=True, snapshot_path=tmp_path / "k.parquet"
    )

    copy_sql = [c.args[0] for c in conn.execute.call_args_list if "COPY" in c.args[0]]
    assert "MATCH (a)-[:HAS_FACT]->(b)" in copy_sql[0]
    assert "a.identifier AS src" in copy_sql[0]
    assert "b.identifier AS dst" in copy_sql[0]

  def test_raises_timeout_for_the_traversal_then_resets_it(self, tmp_path):
    """A 200M-edge traversal needs the long timeout, but leaving it raised
    would let ordinary queries hang for an hour."""
    service, conn = self._service()

    _export_incremental_keyset(
      service, "kg1", "Fact", is_rel=False, snapshot_path=tmp_path / "k.parquet"
    )

    calls = [c.args[0] for c in conn.execute.call_args_list]
    assert calls[0] == "CALL timeout=3600000"
    assert calls[-1] == "CALL timeout=120000"

  def test_timeout_is_reset_even_when_copy_fails(self, tmp_path):
    service, conn = self._service()
    conn.execute.side_effect = lambda sql: (
      (_ for _ in ()).throw(RuntimeError("copy failed"))
      if sql.startswith("COPY")
      else MagicMock()
    )

    with pytest.raises(RuntimeError):
      _export_incremental_keyset(
        service, "kg1", "Fact", is_rel=False, snapshot_path=tmp_path / "k.parquet"
      )

    assert conn.execute.call_args_list[-1].args[0] == "CALL timeout=120000"

  def test_escapes_single_quotes_in_snapshot_path(self, tmp_path):
    service, conn = self._service()
    path = tmp_path / "o'brien.parquet"

    _export_incremental_keyset(service, "kg1", "Fact", False, path)

    copy_sql = [c.args[0] for c in conn.execute.call_args_list if "COPY" in c.args[0]]
    assert "o''brien.parquet" in copy_sql[0]


@pytest.mark.unit
class TestCheckpointWithRetry:
  def test_returns_on_first_success(self):
    conn = MagicMock()

    checkpoint_with_retry(conn, "kg1")

    conn.execute.assert_called_once_with("CHECKPOINT")

  def test_retries_then_succeeds(self):
    conn = MagicMock()
    conn.execute.side_effect = [RuntimeError("locked"), None]

    with patch("time.sleep"):
      checkpoint_with_retry(conn, "kg1")

    assert conn.execute.call_count == 2

  def test_raises_500_after_exhausting_retries(self):
    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("still locked")

    with patch("time.sleep"):
      with pytest.raises(HTTPException) as exc:
        checkpoint_with_retry(conn, "kg1", context="parent DuckDB")

    assert exc.value.status_code == 500
    assert "parent DuckDB" in exc.value.detail
    assert conn.execute.call_count == CHECKPOINT_MAX_RETRIES
