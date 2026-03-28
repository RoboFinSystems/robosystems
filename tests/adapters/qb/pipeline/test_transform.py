"""Tests for QuickBooks transform Dagster asset."""

from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context


def _make_config(
  graph_id="kg_test123",
  connection_id="conn_abc",
  user_id="user_xyz",
  realm_id="123456789",
  full_rebuild=False,
  lookback_days=60,
):
  """Create a QBSyncConfig for tests."""
  from robosystems.adapters.quickbooks.pipeline.configs import QBSyncConfig

  return QBSyncConfig(
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=user_id,
    realm_id=realm_id,
    full_rebuild=full_rebuild,
    lookback_days=lookback_days,
  )


def _make_duckdb_mock(table_counts: dict[str, int]):
  """Create a DuckDB connection mock for row counting."""
  mock_con = MagicMock()
  table_names = list(table_counts.keys())

  def execute_side_effect(query):
    result = MagicMock()
    if query == "SHOW TABLES":
      result.fetchall.return_value = [(t,) for t in table_names]
    else:
      for t, count in table_counts.items():
        if f"count(*) FROM {t}" in query:
          result.fetchone.return_value = (count,)
          break
      else:
        result.fetchone.return_value = (0,)
    return result

  mock_con.execute.side_effect = execute_side_effect
  return mock_con


@pytest.mark.unit
class TestQbTransformSuccess:
  """Test successful execution paths for qb_transform."""

  def test_transform_returns_materialize_result(self, tmp_path):
    """Test that a successful dbt run returns MaterializeResult with metadata."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout="Running with dbt=1.8\nCompleted successfully.",
      stderr="",
    )

    mock_con = _make_duckdb_mock({"elements": 5, "transactions": 12, "entries": 3})

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch("duckdb.connect", return_value=mock_con),
    ):
      context = build_asset_context()
      result = qb_transform(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["graph_id"] == config.graph_id
    assert result.metadata["tables"] == 3
    assert result.metadata["total_rows"] == 20
    assert result.metadata["rows_elements"] == 5
    assert result.metadata["rows_transactions"] == 12
    assert result.metadata["rows_entries"] == 3

  def test_transform_result_includes_duckdb_path(self, tmp_path):
    """Test duckdb_path is set in the result metadata."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config(graph_id="kg_output_test")

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout="",
      stderr="",
    )

    mock_con = _make_duckdb_mock({"elements": 10})

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch("duckdb.connect", return_value=mock_con),
    ):
      context = build_asset_context()
      result = qb_transform(context, config)

    assert isinstance(result, MaterializeResult)
    assert "quickbooks.duckdb" in result.metadata["duckdb_path"]

  def test_transform_dbt_called_with_correct_vars(self, tmp_path):
    """Test that dbt build is called with realm_id and qb_extract_path vars."""
    import json

    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config(realm_id="realm_xyz_789")

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout="",
      stderr="",
    )

    mock_con = _make_duckdb_mock({})

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ) as mock_run,
      patch("duckdb.connect", return_value=mock_con),
    ):
      context = build_asset_context()
      qb_transform(context, config)

    call_args = mock_run.call_args
    cmd = call_args[0][0]

    # Find --vars argument in the command
    vars_idx = cmd.index("--vars")
    vars_json = cmd[vars_idx + 1]
    vars_dict = json.loads(vars_json)

    assert vars_dict["realm_id"] == "realm_xyz_789"
    assert "qb_extract_path" in vars_dict
    assert vars_dict["use_seeds"] is False

  def test_transform_dbt_target_path_uses_work_dir(self, tmp_path):
    """Test that dbt --target-path points inside the work_dir."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout="",
      stderr="",
    )

    mock_con = _make_duckdb_mock({})

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ) as mock_run,
      patch("duckdb.connect", return_value=mock_con),
    ):
      context = build_asset_context()
      qb_transform(context, config)

    cmd = mock_run.call_args[0][0]
    target_idx = cmd.index("--target-path")
    target_path = cmd[target_idx + 1]

    assert str(work_dir) in target_path

  def test_transform_dbt_env_includes_duckdb_path(self, tmp_path):
    """Test that DBT_DUCKDB_PATH env var is set in subprocess call."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout="",
      stderr="",
    )

    mock_con = _make_duckdb_mock({})

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ) as mock_run,
      patch("duckdb.connect", return_value=mock_con),
    ):
      context = build_asset_context()
      qb_transform(context, config)

    call_kwargs = mock_run.call_args[1]
    env = call_kwargs["env"]
    assert "DBT_DUCKDB_PATH" in env
    assert "quickbooks.duckdb" in env["DBT_DUCKDB_PATH"]

  def test_transform_empty_tables_returns_zero_rows(self, tmp_path):
    """Test that no tables produces total_rows=0."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout="",
      stderr="",
    )

    mock_con = _make_duckdb_mock({})

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch("duckdb.connect", return_value=mock_con),
    ):
      context = build_asset_context()
      result = qb_transform(context, config)

    assert result.metadata["tables"] == 0
    assert result.metadata["total_rows"] == 0

  def test_transform_dbt_stdout_is_logged(self, tmp_path):
    """Test that dbt stdout output is passed to context log."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    stdout_lines = "\n".join(f"dbt line {i}" for i in range(5))
    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout=stdout_lines,
      stderr="",
    )

    mock_con = _make_duckdb_mock({})

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch("duckdb.connect", return_value=mock_con),
    ):
      context = build_asset_context()
      qb_transform(context, config)


@pytest.mark.unit
class TestQbTransformFailure:
  """Test error handling in qb_transform."""

  def test_transform_raises_runtime_error_on_dbt_failure(self, tmp_path):
    """Test RuntimeError is raised when dbt exits with non-zero return code."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    failed_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=1,
      stdout="Some dbt output",
      stderr="Error: model compilation failed",
    )

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=failed_proc,
      ),
    ):
      context = build_asset_context()
      with pytest.raises(RuntimeError, match="dbt build failed"):
        qb_transform(context, config)

  def test_transform_error_message_includes_exit_code(self, tmp_path):
    """Test that the RuntimeError message includes the exit code."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    failed_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=5,
      stdout="",
      stderr="",
    )

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=failed_proc,
      ),
    ):
      context = build_asset_context()
      with pytest.raises(RuntimeError, match="5"):
        qb_transform(context, config)

  def test_transform_duckdb_not_opened_on_dbt_failure(self, tmp_path):
    """Test that DuckDB is not opened when dbt fails."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config()

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    failed_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=1,
      stdout="",
      stderr="Error",
    )

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=failed_proc,
      ),
      patch("duckdb.connect") as mock_duckdb,
    ):
      context = build_asset_context()
      with pytest.raises(RuntimeError):
        qb_transform(context, config)

    mock_duckdb.assert_not_called()
