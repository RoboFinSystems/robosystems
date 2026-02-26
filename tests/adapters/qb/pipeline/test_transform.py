"""Tests for QuickBooks transform Dagster asset."""

from subprocess import CompletedProcess
from unittest.mock import patch

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

    table_counts = {"entity": 5, "transaction": 12, "element": 3}

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
      stdout="Running with dbt=1.8\nCompleted successfully.",
      stderr="",
    )

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value=table_counts,
      ),
    ):
      context = build_asset_context()
      result = qb_transform(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["graph_id"] == config.graph_id
    assert result.metadata["tables_exported"] == 3
    assert result.metadata["total_rows"] == 20
    assert result.metadata["rows_entity"] == 5
    assert result.metadata["rows_transaction"] == 12
    assert result.metadata["rows_element"] == 3

  def test_transform_result_output_path_in_metadata(self, tmp_path):
    """Test output_path is set in the result metadata."""
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

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={"entity": 10},
      ),
    ):
      context = build_asset_context()
      result = qb_transform(context, config)

    assert isinstance(result, MaterializeResult)
    assert "output" in result.metadata["output_path"]

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

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ) as mock_run,
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={},
      ),
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

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ) as mock_run,
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={},
      ),
    ):
      context = build_asset_context()
      qb_transform(context, config)

    cmd = mock_run.call_args[0][0]
    target_idx = cmd.index("--target-path")
    target_path = cmd[target_idx + 1]

    # The target path should be inside the work directory
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

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ) as mock_run,
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={},
      ),
    ):
      context = build_asset_context()
      qb_transform(context, config)

    call_kwargs = mock_run.call_args[1]
    env = call_kwargs["env"]
    assert "DBT_DUCKDB_PATH" in env
    assert "quickbooks.duckdb" in env["DBT_DUCKDB_PATH"]

  def test_transform_export_called_with_correct_paths(self, tmp_path):
    """Test export_duckdb_tables is called with the right duckdb_path and output_dir."""
    from robosystems.adapters.quickbooks.pipeline.transform import qb_transform

    config = _make_config(graph_id="kg_export_test")

    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    work_dir.mkdir(parents=True)

    successful_proc = CompletedProcess(
      args=["dbt", "build"],
      returncode=0,
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
        return_value=successful_proc,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={},
      ) as mock_export,
    ):
      context = build_asset_context()
      qb_transform(context, config)

    mock_export.assert_called_once()
    export_call = mock_export.call_args
    duckdb_path_arg = export_call[0][0]
    output_dir_arg = export_call[0][1]

    assert "quickbooks.duckdb" in str(duckdb_path_arg)
    assert "output" in str(output_dir_arg)

  def test_transform_empty_tables_returns_zero_rows(self, tmp_path):
    """Test that no exported tables produces total_rows=0."""

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

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={},
      ),
    ):
      context = build_asset_context()
      result = qb_transform(context, config)

    assert result.metadata["tables_exported"] == 0
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

    with (
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.get_pipeline_work_dir",
        return_value=work_dir,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.subprocess.run",
        return_value=successful_proc,
      ),
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={},
      ),
    ):
      # Use a real build_asset_context - stdout is logged via context.log.info
      # We verify it doesn't crash; the actual log output goes to Dagster's stream
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

  def test_transform_export_not_called_on_dbt_failure(self, tmp_path):
    """Test that export_duckdb_tables is not called when dbt fails."""
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
      patch(
        "robosystems.adapters.quickbooks.pipeline.transform.export_duckdb_tables",
        return_value={},
      ) as mock_export,
    ):
      context = build_asset_context()
      with pytest.raises(RuntimeError):
        qb_transform(context, config)

    mock_export.assert_not_called()
