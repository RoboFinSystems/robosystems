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
  def test_monthly_job_wires_exactly_the_allocation_ops(self):
    """The monthly job is allocation + repo allocation + both retention ops.

    Asserted as an exact set rather than a lower bound: the previous version
    (`"get_subscriptions_for_allocation" in op_names or len(op_names) >= 1`)
    named an op that has never existed and was disjunctively true regardless,
    so it stayed green through any rewiring of the job.
    """
    op_names = {node.name for node in monthly_credit_allocation_job.all_node_defs}

    assert op_names == {
      "allocate_monthly_credits",
      "allocate_user_repository_credits",
      "cleanup_old_credit_transactions",
      "cleanup_old_usage_records",
    }

  @pytest.mark.unit
  def test_usage_retention_is_wired_and_matches_the_credit_ledger_window(self):
    """`graph_usage` has a reaper, and it keeps the same window as its sibling.

    The table is the higher-volume of the two and had no retention at all while
    `graph_credit_transactions` was pruned monthly. Divergent windows would make
    a period rollup reproducible from one table and not the other.
    """
    from contextlib import contextmanager
    from unittest.mock import MagicMock, patch

    from dagster import build_op_context

    from robosystems.dagster.jobs.billing import cleanup_old_usage_records

    session = MagicMock()

    @contextmanager
    def _session_cm():
      yield session

    db = MagicMock()
    db.get_session = _session_cm

    with patch(
      "robosystems.models.core.graph.graph_usage.GraphUsage.cleanup_old_records",
      return_value={
        "deleted_records": 3,
        "preserved_summaries": 7,
        "total_processed": 10,
      },
    ) as reaper:
      result = cleanup_old_usage_records(build_op_context(), db, {"prior": "value"})

    reaper.assert_called_once()
    assert reaper.call_args.kwargs["older_than_days"] == 365
    assert result["usage_cleanup"]["deleted_records"] == 3
    # The prior op's result is threaded through — it is the job's sequencing edge.
    assert result["credit_cleanup"] == {"prior": "value"}
