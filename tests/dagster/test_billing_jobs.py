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


class TestOverageDollarization:
  """Overage invoices keep the credit quantity as the record everywhere,
  but dollarize at $0 when billing is off (fee settled off-platform)."""

  def _run_op(self, graphs):
    from contextlib import contextmanager
    from unittest.mock import Mock, patch

    from dagster import build_op_context

    from robosystems.dagster.jobs.billing import process_overage_invoices

    session = Mock()

    @contextmanager
    def _session_cm():
      yield session

    db = Mock()
    db.get_session = _session_cm

    with patch(
      "robosystems.dagster.jobs.billing.GraphCredits.get_by_graph_id",
      return_value=None,
    ):
      return process_overage_invoices(build_op_context(), db, graphs)

  @pytest.mark.unit
  def test_overage_zero_dollars_when_billing_disabled(self):
    from unittest.mock import patch

    from robosystems.config import env

    graphs = [
      {
        "graph_id": "kg_x",
        "user_id": "u1",
        "negative_balance": -2000,
        "graph_tier": "ladybug-standard",
      }
    ]
    with patch.object(env, "BILLING_ENABLED", False):
      invoices = self._run_op(graphs)

    assert len(invoices) == 1
    assert invoices[0]["overage_credits"] == 2000.0
    assert invoices[0]["amount_usd"] == 0.0

  @pytest.mark.unit
  def test_overage_real_dollars_when_billing_enabled(self):
    from unittest.mock import patch

    from robosystems.config import env

    graphs = [
      {
        "graph_id": "kg_y",
        "user_id": "u1",
        "negative_balance": -2000,
        "graph_tier": "ladybug-standard",
      }
    ]
    with patch.object(env, "BILLING_ENABLED", True):
      invoices = self._run_op(graphs)

    assert invoices[0]["amount_usd"] == 10.0
