"""Graph identity utility tests."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.graph.utils.identity import (
    get_access_pattern,
    get_graph_cluster_type,
    get_migration_status,
    log_cluster_operation,
)


class TestGetAccessPattern:
    """Tests for get_access_pattern."""

    def test_returns_enum(self):
        result = get_access_pattern()
        # Should return a ConnectionPattern enum value
        assert result is not None

    @patch("robosystems.middleware.graph.utils.identity.env")
    def test_invalid_pattern_returns_default(self, mock_env):
        mock_env.LBUG_ACCESS_PATTERN = "INVALID_PATTERN"
        result = get_access_pattern()
        # Should fallback to API_AUTO
        assert result is not None


class TestGetGraphClusterType:
    """Tests for get_graph_cluster_type."""

    @patch("robosystems.middleware.graph.utils.identity.get_graph_identity")
    def test_shared_repo_returns_shared_writer(self, mock_identity):
        identity = MagicMock()
        identity.is_shared_repository = True
        identity.is_user_graph = False
        mock_identity.return_value = identity
        assert get_graph_cluster_type("sec") == "shared_writer"

    @patch("robosystems.middleware.graph.utils.identity.get_graph_identity")
    def test_user_graph_returns_user_writer(self, mock_identity):
        identity = MagicMock()
        identity.is_shared_repository = False
        identity.is_user_graph = True
        mock_identity.return_value = identity
        assert get_graph_cluster_type("kg123") == "user_writer"

    @patch("robosystems.middleware.graph.utils.identity.get_graph_identity")
    def test_other_returns_system(self, mock_identity):
        identity = MagicMock()
        identity.is_shared_repository = False
        identity.is_user_graph = False
        mock_identity.return_value = identity
        assert get_graph_cluster_type("system_graph") == "system"


class TestLogClusterOperation:
    """Tests for log_cluster_operation."""

    def test_logs_without_error(self):
        # Should not raise
        log_cluster_operation(
            operation="test",
            cluster_id="cluster1",
            graph_id="kg123",
            extra_key="extra_value",
        )


class TestGetMigrationStatus:
    """Tests for get_migration_status."""

    def test_returns_dict(self):
        status = get_migration_status()
        assert isinstance(status, dict)
        assert "access_pattern" in status
        assert "environment" in status
        assert "shared_repositories" in status
        assert "max_databases_per_node" in status

    def test_shared_repositories_is_dict(self):
        status = get_migration_status()
        assert isinstance(status["shared_repositories"], dict)
