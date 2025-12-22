"""Tests for Dagster provisioning sensors and jobs.

Tests the subscription provisioning sensors that watch for pending subscriptions
and trigger graph/repository provisioning jobs.
"""

from unittest.mock import MagicMock, patch

import pytest
from dagster import build_sensor_context

from robosystems.dagster.jobs.provisioning import (
  provision_graph_job,
  provision_repository_job,
)
from robosystems.dagster.sensors.provisioning import (
  pending_repository_sensor,
  pending_subscription_sensor,
)


class TestPendingSubscriptionSensor:
  """Tests for the pending subscription sensor."""

  @pytest.mark.unit
  def test_sensor_skips_in_dev_environment(self):
    """Test sensor skips execution in dev environment."""
    context = build_sensor_context()

    with patch("robosystems.dagster.sensors.provisioning.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"

      result = list(pending_subscription_sensor(context))

      # Should yield a SkipReason
      assert len(result) == 1
      assert "Skipped in dev environment" in str(result[0])

  @pytest.mark.unit
  def test_sensor_no_pending_subscriptions(self):
    """Test sensor returns nothing when no pending subscriptions."""
    context = build_sensor_context()

    with patch("robosystems.dagster.sensors.provisioning.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      with patch("robosystems.database.session") as mock_session_factory:
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = []
        mock_session_factory.return_value = mock_session

        result = list(pending_subscription_sensor(context))

        assert len(result) == 0
        mock_session.close.assert_called_once()

  @pytest.mark.unit
  def test_sensor_yields_run_request_for_pending_subscription(self, mock_subscription):
    """Test sensor yields RunRequest for pending subscription."""
    context = build_sensor_context()

    with patch("robosystems.dagster.sensors.provisioning.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      with patch("robosystems.database.session") as mock_session_factory:
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = [
          mock_subscription
        ]
        mock_session_factory.return_value = mock_session

        result = list(pending_subscription_sensor(context))

        assert len(result) == 1
        run_request = result[0]
        assert run_request.run_key == f"provision-graph-{mock_subscription.id}"
        mock_session.close.assert_called_once()

  @pytest.mark.unit
  def test_sensor_handles_multiple_pending_subscriptions(self):
    """Test sensor handles multiple pending subscriptions."""
    context = build_sensor_context()

    # Create multiple mock subscriptions
    subscriptions = []
    for i in range(3):
      sub = MagicMock()
      sub.id = f"sub_{i}"
      sub.org_id = f"org_{i}"
      sub.status = "provisioning"
      sub.resource_type = "graph"
      sub.plan_name = "ladybug-standard"
      subscriptions.append(sub)

    with patch("robosystems.dagster.sensors.provisioning.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      with patch("robosystems.database.session") as mock_session_factory:
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = (
          subscriptions
        )
        mock_session_factory.return_value = mock_session

        result = list(pending_subscription_sensor(context))

        assert len(result) == 3
        for i, run_request in enumerate(result):
          assert run_request.run_key == f"provision-graph-sub_{i}"

  @pytest.mark.unit
  def test_sensor_reraises_exceptions(self):
    """Test sensor re-raises exceptions for Dagster tracking."""
    context = build_sensor_context()

    with patch("robosystems.dagster.sensors.provisioning.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      with patch("robosystems.database.session") as mock_session_factory:
        mock_session = MagicMock()
        mock_session.query.side_effect = Exception("Database error")
        mock_session_factory.return_value = mock_session

        with pytest.raises(Exception, match="Database error"):
          list(pending_subscription_sensor(context))

        # Session should still be closed
        mock_session.close.assert_called_once()


class TestPendingRepositorySensor:
  """Tests for the pending repository sensor."""

  @pytest.mark.unit
  def test_sensor_skips_in_dev_environment(self):
    """Test sensor skips execution in dev environment."""
    context = build_sensor_context()

    with patch("robosystems.dagster.sensors.provisioning.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"

      result = list(pending_repository_sensor(context))

      assert len(result) == 1
      assert "Skipped in dev environment" in str(result[0])

  @pytest.mark.unit
  def test_sensor_yields_run_request_for_repository(self, mock_repository_subscription):
    """Test sensor yields RunRequest for repository subscription."""
    context = build_sensor_context()

    with patch("robosystems.dagster.sensors.provisioning.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"

      with patch("robosystems.database.session") as mock_session_factory:
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = [
          mock_repository_subscription
        ]
        mock_session_factory.return_value = mock_session

        result = list(pending_repository_sensor(context))

        assert len(result) == 1
        run_request = result[0]
        assert (
          run_request.run_key == f"provision-repo-{mock_repository_subscription.id}"
        )
        # Verify repository name is in config
        config = run_request.run_config["ops"]["get_repository_subscription"]["config"]
        assert config["repository_name"] == "sec"


class TestProvisioningJobGraphs:
  """Tests for provisioning job graph construction."""

  @pytest.mark.unit
  def test_provision_graph_job_graph(self):
    """Test provision graph job graph is valid."""
    job_def = provision_graph_job

    assert job_def.name == "provision_graph_job"
    # Should have multiple ops for the provisioning flow
    assert len(job_def.all_node_defs) >= 2

  @pytest.mark.unit
  def test_provision_repository_job_graph(self):
    """Test provision repository job graph is valid."""
    job_def = provision_repository_job

    assert job_def.name == "provision_repository_job"
    assert len(job_def.all_node_defs) >= 2
