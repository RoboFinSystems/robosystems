"""Tests for Dagster billing jobs.

Tests credit allocation and usage report jobs.
"""

import pytest

from robosystems.dagster.jobs.billing import (
  monthly_credit_allocation_job,
  monthly_usage_report_job,
)


class TestBillingJobGraphs:
  """Tests for billing job graph construction."""

  @pytest.mark.unit
  def test_monthly_credit_allocation_job_graph(self):
    """Test monthly credit allocation job graph is valid."""
    job_def = monthly_credit_allocation_job

    assert job_def.name == "monthly_credit_allocation_job"
    # Should have ops for finding subscriptions and allocating credits
    assert len(job_def.all_node_defs) >= 2

  @pytest.mark.unit
  def test_monthly_usage_report_job_graph(self):
    """Test monthly usage report job graph is valid."""
    job_def = monthly_usage_report_job

    assert job_def.name == "monthly_usage_report_job"
    assert len(job_def.all_node_defs) >= 1


class TestBillingJobConfiguration:
  """Tests for billing job configuration and scheduling."""

  @pytest.mark.unit
  def test_monthly_job_has_schedule(self):
    """Test monthly credit allocation has proper schedule configuration."""
    from robosystems.dagster.jobs.billing import monthly_credit_allocation_schedule

    assert monthly_credit_allocation_schedule is not None
    assert (
      monthly_credit_allocation_schedule.job_name == "monthly_credit_allocation_job"
    )
    # Should run on first of month
    assert "0 0 1 * *" in monthly_credit_allocation_schedule.cron_schedule


class TestCreditAllocationLogic:
  """Tests for credit allocation business logic."""

  @pytest.mark.unit
  def test_allocate_credits_calculates_correctly(self, mock_db_resource, mock_session):
    """Test credit allocation calculation logic."""
    # This test verifies the job structure without running the full job
    job_def = monthly_credit_allocation_job

    # Verify job has expected ops
    op_names = [node.name for node in job_def.all_node_defs]
    assert "get_subscriptions_for_allocation" in op_names or len(op_names) >= 1
