"""Tests for direct execution monitor module."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.graph.allocation_manager import CapacityScalingTriggered
from robosystems.middleware.sse.direct_monitor import (
  DAGSTER_REPORT_TIMEOUT,
  ProgressEmitter,
  _report_graph_materialization_async,
  _report_graph_materialization_sync,
  _submit_wait_and_create_job,
  run_entity_graph_creation,
  run_graph_creation,
  run_graph_provisioning,
  run_subgraph_creation,
  run_user_repository_provisioning,
)


class TestProgressEmitter:
  """Test ProgressEmitter class."""

  def test_initialization(self):
    """Test emitter initialization."""
    emitter = ProgressEmitter("op123")
    assert emitter.operation_id == "op123"
    assert emitter.manager is not None

  @pytest.mark.asyncio
  async def test_call_emits_progress(self):
    """Test that calling emitter emits SSE progress."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      emitter = ProgressEmitter("op123")

      # Call the emitter (sync callback)
      emitter("Creating nodes...", 50.0)

      # Give the async task time to run
      await asyncio.sleep(0.1)

      # Verify emit_progress was called
      mock_manager.emit_progress.assert_called_with(
        "op123",
        message="Creating nodes...",
        progress_percent=50.0,
      )

  def test_call_without_event_loop(self):
    """Test calling emitter when no event loop is running."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = MagicMock()
      mock_get_manager.return_value = mock_manager

      emitter = ProgressEmitter("op123")

      # This should not raise even without an event loop
      # (it will just skip the emit)
      with patch("asyncio.get_running_loop", side_effect=RuntimeError):
        emitter("Test message", 25.0)
        # Should not raise


class TestRunGraphCreation:
  """Test run_graph_creation function."""

  @pytest.mark.asyncio
  async def test_successful_creation(self):
    """Test successful graph creation."""
    mock_result = {
      "graph_id": "kg123456789",
      "status": "created",
    }

    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch(
        "robosystems.operations.graph.generic_graph_service.GenericGraphService"
      ) as mock_service_class:
        mock_service = AsyncMock()
        mock_service.create_graph.return_value = mock_result
        mock_service_class.return_value = mock_service

        with patch(
          "robosystems.middleware.sse.direct_monitor._report_graph_materialization_async"
        ) as mock_report:
          mock_report.return_value = None

          result = await run_graph_creation(
            operation_id="op123",
            user_id="user456",
            graph_name="Test Graph",
            tier="ladybug-standard",
            schema_extensions=["roboledger"],
            description="Test description",
            tags=["test"],
          )

          assert result == mock_result
          mock_manager.emit_progress.assert_called()
          mock_manager.complete_operation.assert_called_once()
          mock_report.assert_called_once()

  @pytest.mark.asyncio
  async def test_creation_failure(self):
    """Test graph creation failure handling."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch(
        "robosystems.operations.graph.generic_graph_service.GenericGraphService"
      ) as mock_service_class:
        mock_service = AsyncMock()
        mock_service.create_graph.side_effect = Exception("Database error")
        mock_service_class.return_value = mock_service

        with pytest.raises(Exception, match="Database error"):
          await run_graph_creation(
            operation_id="op123",
            user_id="user456",
            graph_name="Test Graph",
            tier="ladybug-standard",
            schema_extensions=[],
          )

        mock_manager.fail_operation.assert_called_once()
        call_args = mock_manager.fail_operation.call_args
        # Check operation_id and error message
        assert call_args.args[0] == "op123"
        assert "Database error" in call_args.kwargs.get(
          "error", call_args.args[1] if len(call_args.args) > 1 else ""
        )

  @pytest.mark.asyncio
  async def test_provisioning_handoff_to_dagster(self):
    """Test that CapacityScalingTriggered hands off to Dagster instead of failing."""
    provisioning_error = CapacityScalingTriggered(
      "No ladybug-standard capacity currently available. "
      "A new instance is being provisioned and should be ready in 3-5 minutes. "
      "Please retry shortly."
    )

    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch(
        "robosystems.operations.graph.generic_graph_service.GenericGraphService"
      ) as mock_service_class:
        mock_service = AsyncMock()
        mock_service.create_graph.side_effect = provisioning_error
        mock_service_class.return_value = mock_service

        with patch(
          "robosystems.middleware.sse.direct_monitor._submit_wait_and_create_job",
          new_callable=AsyncMock,
        ) as mock_submit:
          result = await run_graph_creation(
            operation_id="op123",
            user_id="user456",
            graph_name="Test Graph",
            tier="ladybug-standard",
            schema_extensions=["roboledger"],
            description="Test description",
            tags=["test"],
          )

          # Should NOT have failed the operation
          mock_manager.fail_operation.assert_not_called()

          # Should have submitted the Dagster job
          mock_submit.assert_called_once_with(
            operation_id="op123",
            user_id="user456",
            graph_name="Test Graph",
            tier="ladybug-standard",
            schema_extensions=["roboledger"],
            description="Test description",
            tags=["test"],
            custom_schema=None,
          )

          # Should return provisioning status
          assert result["status"] == "provisioning"
          assert result["operation_id"] == "op123"

  @pytest.mark.asyncio
  async def test_non_provisioning_error_still_fails(self):
    """Test that non-provisioning errors still fail normally."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch(
        "robosystems.operations.graph.generic_graph_service.GenericGraphService"
      ) as mock_service_class:
        mock_service = AsyncMock()
        mock_service.create_graph.side_effect = Exception(
          "No ladybug-standard capacity currently available. "
          "Please contact support or try again later."
        )
        mock_service_class.return_value = mock_service

        with pytest.raises(Exception, match="No ladybug-standard capacity"):
          await run_graph_creation(
            operation_id="op123",
            user_id="user456",
            graph_name="Test Graph",
            tier="ladybug-standard",
            schema_extensions=[],
          )

        mock_manager.fail_operation.assert_called_once()


class TestSubmitWaitAndCreateJob:
  """Test _submit_wait_and_create_job helper."""

  @pytest.mark.asyncio
  async def test_submits_dagster_job_with_correct_config(self):
    """Test that Dagster job is submitted with correct run config."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch(
        "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor"
      ) as MockMonitor:
        mock_monitor = MagicMock()
        mock_monitor.submit_job.return_value = "run-abc-123"
        MockMonitor.return_value = mock_monitor

        await _submit_wait_and_create_job(
          operation_id="op123",
          user_id="user456",
          graph_name="Test Graph",
          tier="ladybug-standard",
          schema_extensions=["roboledger"],
          description="Test desc",
          tags=["tag1", "tag2"],
          custom_schema=None,
        )

        # Verify SSE progress was emitted
        mock_manager.emit_progress.assert_called_once()

        # Verify Dagster job was submitted
        mock_monitor.submit_job.assert_called_once()
        call_args = mock_monitor.submit_job.call_args
        assert call_args.args[0] == "wait_and_create_graph_job"
        assert call_args.kwargs["tags"]["operation_id"] == "op123"

  @pytest.mark.asyncio
  async def test_submit_failure_emits_sse_error(self):
    """Test that Dagster submission failure emits SSE error."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch(
        "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor"
      ) as MockMonitor:
        mock_monitor = MagicMock()
        mock_monitor.submit_job.side_effect = Exception("Dagster unreachable")
        MockMonitor.return_value = mock_monitor

        with pytest.raises(Exception, match="Dagster unreachable"):
          await _submit_wait_and_create_job(
            operation_id="op123",
            user_id="user456",
            graph_name="Test",
            tier="ladybug-standard",
            schema_extensions=[],
          )

        mock_manager.fail_operation.assert_called_once()


class TestRunEntityGraphCreation:
  """Test run_entity_graph_creation function."""

  @pytest.mark.asyncio
  async def test_successful_entity_creation(self):
    """Test successful entity graph creation."""
    mock_result = {
      "graph_id": "kg987654321",
      "entity_id": "entity123",
      "status": "created",
    }

    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        with patch(
          "robosystems.operations.graph.entity_graph_service.EntityGraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_entity_with_new_graph.return_value = mock_result
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.middleware.sse.direct_monitor._report_graph_materialization_async"
          ) as mock_report:
            mock_report.return_value = None

            result = await run_entity_graph_creation(
              operation_id="op123",
              user_id="user456",
              entity_name="Acme Corp",
              tier="ladybug-large",
              schema_extensions=["roboledger"],
              entity_identifier="12-3456789",
              entity_identifier_type="ein",
              description="Test company",
              tags=["test"],
              create_entity=True,
            )

            assert result == mock_result
            mock_manager.complete_operation.assert_called_once()
            mock_report.assert_called_once()

  @pytest.mark.asyncio
  async def test_entity_creation_failure(self):
    """Test entity graph creation failure handling."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        with patch(
          "robosystems.operations.graph.entity_graph_service.EntityGraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_entity_with_new_graph.side_effect = Exception(
            "Allocation failed"
          )
          mock_service_class.return_value = mock_service

          with pytest.raises(Exception, match="Allocation failed"):
            await run_entity_graph_creation(
              operation_id="op123",
              user_id="user456",
              entity_name="Acme Corp",
              tier="ladybug-standard",
            )

          mock_manager.fail_operation.assert_called_once()


class TestRunSubgraphCreation:
  """Test run_subgraph_creation function."""

  @pytest.mark.asyncio
  async def test_successful_subgraph_creation(self):
    """Test successful subgraph creation."""
    mock_result = {
      "graph_id": "kg123_dev",
      "status": "created",
    }

    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        mock_parent = MagicMock()
        mock_user = MagicMock()
        # Set up db.query to return different results for Graph and User
        mock_db.query.return_value.filter.return_value.first.side_effect = [
          mock_parent,
          mock_user,
        ]

        with patch(
          "robosystems.operations.graph.subgraph_service.SubgraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_subgraph.return_value = mock_result
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.middleware.sse.direct_monitor._report_graph_materialization_async"
          ) as mock_report:
            mock_report.return_value = None

            result = await run_subgraph_creation(
              operation_id="op123",
              user_id="user456",
              parent_graph_id="kg123",
              subgraph_name="dev",
              description="Development subgraph",
              fork_data=True,
            )

            assert result == mock_result
            mock_manager.complete_operation.assert_called_once()


class TestRunGraphProvisioning:
  """Test run_graph_provisioning function."""

  @pytest.mark.asyncio
  async def test_successful_graph_provisioning(self):
    """Test successful graph provisioning after payment."""
    mock_result = {
      "graph_id": "kg123456789",
      "status": "created",
    }

    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        # Mock subscription
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
          "robosystems.operations.graph.generic_graph_service.GenericGraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_graph.return_value = mock_result
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.middleware.sse.direct_monitor._report_graph_materialization_async"
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
    mock_result = {"graph_id": "kg123", "status": "created"}

    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
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
          "robosystems.operations.graph.generic_graph_service.GenericGraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_graph.return_value = mock_result
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.middleware.sse.direct_monitor._report_graph_materialization_async"
          ):
            result = await run_graph_provisioning(
              operation_id=None,  # No SSE tracking
              subscription_id="sub123",
              user_id="user456",
              tier="ladybug-standard",
            )

            assert result["graph_id"] == "kg123"
            # Should not emit progress without operation_id
            mock_manager.emit_progress.assert_not_called()
            mock_manager.complete_operation.assert_not_called()

  @pytest.mark.asyncio
  async def test_graph_provisioning_failure_marks_subscription_failed(self):
    """Test that provisioning failure marks subscription as failed."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        # First call for main logic, second for error cleanup
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
          "robosystems.operations.graph.generic_graph_service.GenericGraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_graph.side_effect = Exception(
            "Database allocation failed"
          )
          mock_service_class.return_value = mock_service

          with pytest.raises(Exception, match="Database allocation failed"):
            await run_graph_provisioning(
              operation_id="op123",
              subscription_id="sub123",
              user_id="user456",
              tier="ladybug-standard",
            )

          mock_manager.fail_operation.assert_called_once()

  @pytest.mark.asyncio
  async def test_graph_provisioning_failure_cancels_stripe_subscription(self):
    """Test that provisioning failure cancels the Stripe subscription."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        # Two calls to get_db_session: one for main logic, one for error cleanup
        mock_get_db.side_effect = [iter([mock_db]), iter([mock_db])]

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.subscription_metadata = {}
        mock_subscription.stripe_subscription_id = "stripe_sub_abc"
        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.operations.graph.generic_graph_service.GenericGraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_graph.side_effect = Exception("Provisioning failed")
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.operations.providers.payment_provider.get_payment_provider"
          ) as mock_get_provider:
            mock_provider = MagicMock()
            mock_get_provider.return_value = mock_provider

            with pytest.raises(Exception, match="Provisioning failed"):
              await run_graph_provisioning(
                operation_id="op123",
                subscription_id="sub123",
                user_id="user456",
                tier="ladybug-standard",
              )

            mock_provider.cancel_subscription.assert_called_once_with("stripe_sub_abc")
            assert mock_subscription.status == "failed"

  @pytest.mark.asyncio
  async def test_graph_provisioning_failure_skips_cancel_without_stripe(self):
    """Test that failure without Stripe subscription doesn't attempt cancel."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.side_effect = [iter([mock_db]), iter([mock_db])]

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.subscription_metadata = {}
        mock_subscription.stripe_subscription_id = None
        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.operations.graph.generic_graph_service.GenericGraphService"
        ) as mock_service_class:
          mock_service = AsyncMock()
          mock_service.create_graph.side_effect = Exception("Failed")
          mock_service_class.return_value = mock_service

          with patch(
            "robosystems.operations.providers.payment_provider.get_payment_provider"
          ) as mock_get_provider:
            with pytest.raises(Exception, match="Failed"):
              await run_graph_provisioning(
                operation_id=None,
                subscription_id="sub123",
                user_id="user456",
                tier="ladybug-standard",
              )

            mock_get_provider.assert_not_called()


class TestRunUserRepositoryProvisioning:
  """Tests for run_user_repository_provisioning function."""

  @pytest.mark.asyncio
  async def test_successful_user_repository_provisioning(self):
    """Test successful user repository provisioning."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.return_value = iter([mock_db])

        # Mock subscription
        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.plan_name = "sec-starter"
        mock_subscription.current_period_start = MagicMock()
        mock_subscription.current_period_start.isoformat.return_value = "2026-01-01"
        mock_subscription.current_period_end = MagicMock()
        mock_subscription.current_period_end.isoformat.return_value = "2026-02-01"

        # Mock customer
        mock_customer = MagicMock()
        mock_customer.org_id = "org123"

        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.models.billing.BillingCustomer"
        ) as mock_billing_customer:
          mock_billing_customer.get_by_user_id.return_value = mock_customer

          with patch(
            "robosystems.operations.graph.repository_subscription_service.RepositorySubscriptionService"
          ) as mock_repo_service_class:
            mock_repo_service = MagicMock()
            mock_repo_service.grant_access.return_value = True
            mock_repo_service.allocate_credits.return_value = 10000
            mock_repo_service_class.return_value = mock_repo_service

            with patch("robosystems.models.billing.BillingAuditLog"):
              with patch(
                "robosystems.operations.graph.subscription_service.generate_subscription_invoice"
              ):
                with patch(
                  "robosystems.middleware.sse.direct_monitor._report_graph_materialization_async"
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
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
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
          "robosystems.models.billing.BillingCustomer"
        ) as mock_billing_customer:
          mock_billing_customer.get_by_user_id.return_value = mock_customer

          with pytest.raises(ValueError, match="Invalid repository type"):
            await run_user_repository_provisioning(
              operation_id="op123",
              subscription_id="sub123",
              user_id="user456",
              repository_name="invalid_repo",
            )

  @pytest.mark.asyncio
  async def test_repository_provisioning_failure_cancels_stripe_subscription(self):
    """Test that repository provisioning failure cancels the Stripe subscription."""
    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch("robosystems.database.get_db_session") as mock_get_db:
        mock_db = MagicMock()
        mock_get_db.side_effect = [iter([mock_db]), iter([mock_db])]

        mock_subscription = MagicMock()
        mock_subscription.id = "sub123"
        mock_subscription.status = "provisioning"
        mock_subscription.plan_name = "sec-starter"
        mock_subscription.subscription_metadata = {}
        mock_subscription.stripe_subscription_id = "stripe_sub_xyz"

        mock_customer = MagicMock()
        mock_customer.org_id = "org123"

        mock_db.query.return_value.filter.return_value.first.return_value = (
          mock_subscription
        )

        with patch(
          "robosystems.models.billing.BillingCustomer"
        ) as mock_billing_customer:
          mock_billing_customer.get_by_user_id.return_value = mock_customer

          with patch(
            "robosystems.operations.graph.repository_subscription_service.RepositorySubscriptionService"
          ) as mock_repo_service_class:
            mock_repo_service = MagicMock()
            mock_repo_service.grant_access.side_effect = Exception("Access denied")
            mock_repo_service_class.return_value = mock_repo_service

            with patch(
              "robosystems.operations.providers.payment_provider.get_payment_provider"
            ) as mock_get_provider:
              mock_provider = MagicMock()
              mock_get_provider.return_value = mock_provider

              with pytest.raises(Exception, match="Access denied"):
                await run_user_repository_provisioning(
                  operation_id="op123",
                  subscription_id="sub123",
                  user_id="user456",
                  repository_name="sec",
                )

              mock_provider.cancel_subscription.assert_called_once_with(
                "stripe_sub_xyz"
              )
              assert mock_subscription.status == "failed"


class TestDagsterMaterialization:
  """Test Dagster materialization reporting."""

  @pytest.mark.asyncio
  async def test_async_materialization_success(self):
    """Test successful async materialization reporting."""
    with patch(
      "robosystems.middleware.sse.direct_monitor._report_graph_materialization_sync"
    ) as mock_sync:
      await _report_graph_materialization_async(
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
      await asyncio.sleep(10)  # Longer than DAGSTER_REPORT_TIMEOUT

    with patch("asyncio.to_thread", return_value=slow_operation()):
      with patch(
        "robosystems.middleware.sse.direct_monitor.DAGSTER_REPORT_TIMEOUT", 0.01
      ):
        # Should not raise, just log warning
        await _report_graph_materialization_async(
          asset_key="user_graph_creation",
          description="Test",
          metadata={},
        )

  @pytest.mark.asyncio
  async def test_async_materialization_failure(self):
    """Test materialization reporting failure handling."""
    with patch("asyncio.to_thread", side_effect=Exception("Dagster unavailable")):
      # Should not raise, just log warning
      await _report_graph_materialization_async(
        asset_key="user_graph_creation",
        description="Test",
        metadata={},
      )

  def test_sync_materialization_skips_in_test_env(self):
    """Test that sync materialization skips in test environment.

    Note: The actual test runs in 'test' environment, so the function
    should return early without calling Dagster.
    """
    # This should not raise - it should just log a debug message and return
    _report_graph_materialization_sync(
      asset_key="user_graph_creation",
      description="Test",
      metadata={"graph_id": "kg123"},
    )


class TestFeatureFlagIntegration:
  """Test feature flag integration scenarios."""

  @pytest.mark.asyncio
  async def test_direct_execution_enabled(self):
    """Test that direct execution runs when flag is enabled."""
    mock_result = {"graph_id": "kg123", "status": "created"}

    with patch(
      "robosystems.middleware.sse.direct_monitor.get_operation_manager"
    ) as mock_get_manager:
      mock_manager = AsyncMock()
      mock_get_manager.return_value = mock_manager

      with patch(
        "robosystems.operations.graph.generic_graph_service.GenericGraphService"
      ) as mock_service_class:
        mock_service = AsyncMock()
        mock_service.create_graph.return_value = mock_result
        mock_service_class.return_value = mock_service

        with patch(
          "robosystems.middleware.sse.direct_monitor._report_graph_materialization_async"
        ):
          result = await run_graph_creation(
            operation_id="op123",
            user_id="user456",
            graph_name="Test",
            tier="ladybug-standard",
            schema_extensions=[],
          )

          assert result["graph_id"] == "kg123"
          mock_service.create_graph.assert_called_once()


class TestConstants:
  """Test module constants."""

  def test_dagster_report_timeout(self):
    """Test Dagster report timeout value (default 15s, configurable via env)."""
    assert DAGSTER_REPORT_TIMEOUT == 15.0
    assert isinstance(DAGSTER_REPORT_TIMEOUT, float)
