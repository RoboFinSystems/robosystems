"""Tests for graph_api main module."""

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from robosystems.middleware.graph.types import NodeType, RepositoryType


class TestMain:
  """Test cases for main entry point."""

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_basic_configuration(
    self, mock_init_cluster, mock_create_app, mock_uvicorn
  ):
    """Test main with basic configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--port",
        "8002",
        "--host",
        "127.0.0.1",
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      # Mock env import inside main
      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = None  # No tier configured
        mock_env.get_lbug_tier_config.side_effect = Exception("No tier config")

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify init_cluster_service was called with correct args
      mock_init_cluster.assert_called_once_with(
        base_path=str(Path(tmpdir).resolve()),
        max_databases=200,
        read_only=False,
        node_type=NodeType.WRITER,
        repository_type=RepositoryType.ENTITY,
      )

      # Verify create_app was called
      mock_create_app.assert_called_once()

      # Verify uvicorn.run was called with correct args
      mock_uvicorn.assert_called_once_with(
        mock_app,
        host="127.0.0.1",
        port=8002,
        workers=1,
        log_level="info",
        access_log=True,
      )

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_read_only_mode(self, mock_init_cluster, mock_create_app, mock_uvicorn):
    """Test main with read-only mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--read-only",
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      # Mock env to avoid tier config loading
      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = None  # No tier configured

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify read_only flag was passed
      mock_init_cluster.assert_called_once()
      call_args = mock_init_cluster.call_args[1]
      assert call_args["read_only"] is True

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_shared_master_node(
    self, mock_init_cluster, mock_create_app, mock_uvicorn
  ):
    """Test main with shared master node configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--node-type",
        "shared_master",
        "--repository-type",
        "shared",
        "--max-databases",
        "50",
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      # Mock env to avoid tier config loading
      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = None  # No tier configured

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify correct node and repository types
      mock_init_cluster.assert_called_once_with(
        base_path=str(Path(tmpdir).resolve()),
        max_databases=50,
        read_only=False,
        node_type=NodeType.SHARED_MASTER,
        repository_type=RepositoryType.SHARED,
      )

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_with_tier_config(
    self, mock_init_cluster, mock_create_app, mock_uvicorn
  ):
    """Test main with tier configuration from environment."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--max-databases",
        "100",  # This should be overridden by tier config
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      # Mock tier configuration
      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = "standard"
        mock_env.get_lbug_tier_config.return_value = {
          "databases_per_instance": 10,
          "memory_per_database_mb": 2048,
        }

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify tier config was used
      mock_init_cluster.assert_called_once_with(
        base_path=str(Path(tmpdir).resolve()),
        max_databases=10,  # From tier config, not CLI arg
        read_only=False,
        node_type=NodeType.WRITER,
        repository_type=RepositoryType.ENTITY,
      )

  def test_main_invalid_node_repository_combination(self):
    """Test main with invalid node/repository type combination."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--node-type",
        "writer",
        "--repository-type",
        "shared",  # Invalid: writer nodes must use entity type
      ]

      with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
          from robosystems.graph_api.main import main

          main()

  def test_main_invalid_shared_node_repository_combination(self):
    """Test main with invalid shared node/repository type combination."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--node-type",
        "shared_master",
        "--repository-type",
        "entity",  # Invalid: shared nodes must use shared type
      ]

      with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
          from robosystems.graph_api.main import main

          main()

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_with_workers(self, mock_init_cluster, mock_create_app, mock_uvicorn):
    """Test main with multiple workers."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--workers",
        "4",
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = None

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify workers parameter was passed
      mock_uvicorn.assert_called_once()
      call_args = mock_uvicorn.call_args[1]
      assert call_args["workers"] == 4

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_with_log_level(self, mock_init_cluster, mock_create_app, mock_uvicorn):
    """Test main with custom log level."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--log-level",
        "debug",
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = None

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify log level was passed
      mock_uvicorn.assert_called_once()
      call_args = mock_uvicorn.call_args[1]
      assert call_args["log_level"] == "debug"

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_base_path_creation(
    self, mock_init_cluster, mock_create_app, mock_uvicorn
  ):
    """Test that main creates base path if it doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
      base_path = Path(tmpdir) / "nested" / "path" / "to" / "databases"
      test_args = [
        "main.py",
        "--base-path",
        str(base_path),
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = None

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify base path was created
      assert base_path.exists()
      assert base_path.is_dir()

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_tier_config_exception_handling(
    self, mock_init_cluster, mock_create_app, mock_uvicorn
  ):
    """Test main handles tier config loading exceptions gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--max-databases",
        "150",
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      # Mock tier configuration to raise exception
      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = "standard"
        mock_env.get_lbug_tier_config.side_effect = Exception("Config error")

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Should use CLI argument when tier config fails
      mock_init_cluster.assert_called_once()
      call_args = mock_init_cluster.call_args[1]
      assert call_args["max_databases"] == 150  # Falls back to CLI arg

  @patch("robosystems.graph_api.main.uvicorn.run")
  @patch("robosystems.graph_api.main.create_app")
  @patch("robosystems.graph_api.main.init_cluster_service")
  def test_main_shared_replica_node(
    self, mock_init_cluster, mock_create_app, mock_uvicorn
  ):
    """Test main with shared replica node configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
      test_args = [
        "main.py",
        "--base-path",
        tmpdir,
        "--node-type",
        "shared_replica",
        "--repository-type",
        "shared",
        "--read-only",
      ]

      mock_app = MagicMock()
      mock_app.version = "1.0.0"
      mock_create_app.return_value = mock_app

      with patch("robosystems.config.env") as mock_env:
        mock_env.CLUSTER_TIER = None

        with patch.object(sys, "argv", test_args):
          from robosystems.graph_api.main import main

          main()

      # Verify correct configuration for shared replica
      mock_init_cluster.assert_called_once_with(
        base_path=str(Path(tmpdir).resolve()),
        max_databases=200,
        read_only=True,
        node_type=NodeType.SHARED_REPLICA,
        repository_type=RepositoryType.SHARED,
      )
