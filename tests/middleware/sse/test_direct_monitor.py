"""Tests for provisioning service (moved from direct_monitor).

Tests for graph/repository provisioning and Dagster reporting helpers.
ProgressEmitter and run_subgraph_creation tests removed — subgraph
creation now handled by worker/tasks/subgraph_creation.py.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.dagster.reporting import (
  DAGSTER_REPORT_TIMEOUT,
  report_asset_materialization,
  report_asset_materialization_sync,
)
from robosystems.operations.graph.provisioning_service import (
  run_graph_provisioning,
  run_user_repository_provisioning,
)


class TestRunGraphProvisioning:
  """Test run_graph_provisioning function."""

  @pytest.mark.asyncio
  async def test_successful_graph_provisioning(self):
    """Test successful graph provisioning after payment."""
    mock_result = MagicMock()
    mock_result.graph_id = "kg123456789"
    mock_result.to_dict.return_value = {"graph_id": "kg123456789", "status": "created"}

    with patch(
      "robosystems.operations.graph.provisioning_service.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.subscription_metadata = {
          "graph_type": "generic",
          "graph_name": "Test Graph",
          "schema_extensions": ["roboledger"],
        }
        mock_subscription.stripe_subscription_id = "stripe_123"
        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.operations.graph.graph_creation_service.GraphCreationService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create.return_value = mock_result
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.operations.graph.provisioning_service.report_asset_materialization"
          ) as mock_report:
            mock_report.return_value = None

            result = await run_graph_provisioning(
              operation_id="op123",
              subscription_id="sub123",
              user_id="user456",
              tier="ladybug-standard",
            )

            assert result["status"] == "activated"
            assert result["subscription_id"] == "sub123"
            mock_subscription.activate.assert_called_once()
            mock_report.assert_called_once()

  @pytest.mark.asyncio
  async def test_graph_provisioning_without_operation_id(self):
    """Test graph provisioning without SSE operation (webhook-triggered)."""
    mock_result = MagicMock()
    mock_result.graph_id = "kg123"
    mock_result.to_dict.return_value = {"graph_id": "kg123", "status": "created"}

    with patch(
      "robosystems.operations.graph.provisioning_service.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.subscription_metadata = {"graph_type": "generic"}
        mock_subscription.stripe_subscription_id = "stripe_123"
        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.operations.graph.graph_creation_service.GraphCreationService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create.return_value = mock_result
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.operations.graph.provisioning_service.report_asset_materialization"
          ):
            result = await run_graph_provisioning(
              operation_id=None,
              subscription_id="sub123",
              user_id="user456",
              tier="ladybug-standard",
            )

            assert result["graph_id"] == "kg123"
            mock_manager.emit_progress.assert_not_called()
            mock_manager.complete_operation.assert_not_called()

  @pytest.mark.asyncio
  async def test_graph_provisioning_failure_fails_the_sse_operation(self):
    """A failed attempt fails the SSE operation and nothing more.

    Disposition of the subscription row is the caller's, not this function's —
    pinned against real Postgres in
    ``tests/operations/graph/test_provisioning_failure_disposition.py``.
    """
    with patch(
      "robosystems.operations.graph.provisioning_service.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.subscription_metadata = {}
        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.operations.graph.graph_creation_service.GraphCreationService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create.side_effect = Exception("Database allocation failed")
          mock_service_class.return_value = mock_service

          with pytest.raises(Exception, match="Database allocation failed"):
            await run_graph_provisioning(
              operation_id="op123",
              subscription_id="sub123",
              user_id="user456",
              tier="ladybug-standard",
            )

          mock_manager.fail_operation.assert_called_once()


class TestRunUserRepositoryProvisioning:
  """Tests for run_user_repository_provisioning function."""

  @pytest.mark.asyncio
  async def test_successful_user_repository_provisioning(self):
    """Test successful user repository provisioning."""
    with patch(
      "robosystems.operations.graph.provisioning_service.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.plan_name = "sec-starter"
        mock_subscription.current_period_start = MagicMock()
        mock_subscription.current_period_start.isoformat.return_value = "2026-01-01"
        mock_subscription.current_period_end = MagicMock()
        mock_subscription.current_period_end.isoformat.return_value = "2026-02-01"

        mock_customer = MagicMock()
        mock_customer.org_id = "org123"

        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.models.core.billing.BillingCustomer"
        ) as mock_billing_customer:
          mock_billing_customer.get_by_user_id.return_value = mock_customer

          with patch(
            "robosystems.operations.graph.repository_subscription_service.RepositorySubscriptionService"
          ) as mock_repo_service_class:
            mock_repo_service = MagicMock()
            mock_repo_service.grant_access.return_value = True
            mock_repo_service.allocate_credits.return_value = 10000
            mock_repo_service_class.return_value = mock_repo_service

            with patch("robosystems.models.core.billing.BillingAuditLog"):
              with patch(
                "robosystems.operations.graph.subscription_service.generate_subscription_invoice"
              ):
                with patch(
                  "robosystems.operations.graph.provisioning_service.report_asset_materialization"
                ):
                  result = await run_user_repository_provisioning(
                    operation_id="op123",
                    subscription_id="sub123",
                    user_id="user456",
                    repository_name="sec",
                  )

                  assert result["status"] == "activated"
                  assert result["repository_name"] == "sec"
                  assert result["access_granted"] is True
                  assert result["credits_allocated"] == 10000
                  mock_subscription.activate.assert_called_once()

  @pytest.mark.asyncio
  async def test_user_repository_provisioning_invalid_type(self):
    """Test user repository provisioning with invalid repository type."""
    with patch(
      "robosystems.operations.graph.provisioning_service.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.plan_name = "invalid-plan"
        mock_subscription.subscription_metadata = {}

        mock_customer = MagicMock()
        mock_customer.org_id = "org123"

        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.models.core.billing.BillingCustomer"
        ) as mock_billing_customer:
          mock_billing_customer.get_by_user_id.return_value = mock_customer

          with pytest.raises(ValueError, match="Invalid repository type"):
            await run_user_repository_provisioning(
              operation_id="op123",
              subscription_id="sub123",
              user_id="user456",
              repository_name="invalid_repo",
            )


class TestDagsterMaterialization:
  """Test Dagster materialization reporting."""

  @pytest.mark.asyncio
  async def test_async_materialization_success(self):
    """Test successful async materialization reporting."""
    with patch(
      "robosystems.dagster.reporting.report_asset_materialization_sync"
    ) as mock_sync:
      await report_asset_materialization(
        asset_key="user_graph_creation",
        description="Test creation",
        metadata={"graph_id": "kg123"},
      )

      mock_sync.assert_called_once_with(
        "user_graph_creation",
        "Test creation",
        {"graph_id": "kg123"},
      )

  @pytest.mark.asyncio
  async def test_async_materialization_timeout(self):
    """Test materialization reporting timeout."""

    async def slow_operation(*args, **kwargs):
      await asyncio.sleep(10)

    with patch("asyncio.to_thread", return_value=slow_operation()):
      with patch("robosystems.dagster.reporting.DAGSTER_REPORT_TIMEOUT", 0.01):
        await report_asset_materialization(
          asset_key="user_graph_creation",
          description="Test",
          metadata={},
        )

  @pytest.mark.asyncio
  async def test_async_materialization_failure(self):
    """Test materialization reporting failure handling."""
    with patch("asyncio.to_thread", side_effect=Exception("Dagster unavailable")):
      await report_asset_materialization(
        asset_key="user_graph_creation",
        description="Test",
        metadata={},
      )

  def test_sync_materialization_skips_in_test_env(self):
    """Test that sync materialization skips in test environment."""
    report_asset_materialization_sync(
      asset_key="user_graph_creation",
      description="Test",
      metadata={"graph_id": "kg123"},
    )


class TestConstants:
  """Test module constants."""

  def test_dagster_report_timeout(self):
    """Test Dagster report timeout is a positive float."""
    assert isinstance(DAGSTER_REPORT_TIMEOUT, float)
    assert DAGSTER_REPORT_TIMEOUT > 0
