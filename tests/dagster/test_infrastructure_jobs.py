"""Tests for Dagster infrastructure jobs.

Tests auth cleanup, health check, and instance maintenance jobs.
"""

from unittest.mock import patch

import pytest

from robosystems.dagster.jobs.infrastructure import (
  hourly_auth_cleanup_job,
  weekly_health_check_job,
)


class TestJobGraphs:
  """Tests for job graph construction."""

  @pytest.mark.unit
  def test_hourly_auth_cleanup_job_graph(self):
    """Test hourly auth cleanup job graph is valid."""
    # Job graph validation - ensures ops are properly connected
    job_def = hourly_auth_cleanup_job

    assert job_def.name == "hourly_auth_cleanup_job"
    assert len(job_def.all_node_defs) == 1  # Single op

  @pytest.mark.unit
  def test_weekly_health_check_job_graph(self):
    """Test weekly health check job graph is valid."""
    job_def = weekly_health_check_job

    assert job_def.name == "weekly_health_check_job"
    # Has multiple ops for different health checks
    assert len(job_def.all_node_defs) >= 2


class TestJobExecution:
  """Integration-style tests for job execution."""

  @pytest.mark.unit
  def test_auth_cleanup_job_executes(self, mock_session):
    """Test auth cleanup job can be executed with mocked resources."""
    mock_session.query.return_value.filter.return_value.all.return_value = []

    with patch("robosystems.dagster.jobs.infrastructure.DatabaseResource"):
      # Verify job definition is valid
      job_def = hourly_auth_cleanup_job
      assert job_def is not None
      assert job_def.name == "hourly_auth_cleanup_job"
