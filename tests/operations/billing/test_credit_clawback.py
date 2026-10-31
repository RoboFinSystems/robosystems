"""The consumption rule behind the unused-time credit.

A period buys time and an AI-credit allocation; the credit refunds the
smaller unused share. These are the pure cases over a mocked credit pool;
the snapshot path and the invoice-lane reduction run against Postgres in
`test_invoice_credits.py`.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.billing.invoice_credits import (
  CREDIT_CONSUMPTION_SNAPSHOT,
  plan_credit_clawback,
  snapshot_credit_consumption,
)

PERIOD_START = datetime(2026, 10, 25, 6, 13, 22, tzinfo=UTC)
PERIOD_END = datetime(2026, 11, 25, 6, 13, 22, tzinfo=UTC)
DAY_ONE = datetime(2026, 10, 26, 6, 13, 22, tzinfo=UTC)
HALFWAY = datetime(2026, 11, 9, 18, 13, 22, tzinfo=UTC)


def _subscription(**overrides) -> MagicMock:
  sub = MagicMock()
  sub.resource_type = "graph"
  sub.resource_id = "kg_test"
  sub.base_price_cents = 9900
  sub.current_period_start = PERIOD_START
  sub.current_period_end = PERIOD_END
  sub.subscription_metadata = {}
  for key, value in overrides.items():
    setattr(sub, key, value)
  return sub


def _pool(allocation: int, balance: int) -> MagicMock:
  pool = MagicMock()
  pool.monthly_allocation = Decimal(allocation)
  pool.current_balance = Decimal(balance)
  return pool


@pytest.mark.unit
class TestPlanCreditClawback:
  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_credits_used_in_step_with_time_claw_nothing_back(self, get_pool):
    get_pool.return_value = _pool(8000, 4000)

    consumption = plan_credit_clawback(_subscription(), MagicMock(), now=HALFWAY)

    assert consumption is not None
    assert consumption.consumed == 4000
    assert consumption.clawback_cents == 0

  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_a_burned_allocation_on_day_one_claws_back_the_unused_time(self, get_pool):
    get_pool.return_value = _pool(8000, 0)

    consumption = plan_credit_clawback(_subscription(), MagicMock(), now=DAY_ONE)

    assert consumption is not None
    assert consumption.consumed_fraction == 1.0
    assert consumption.elapsed_fraction == pytest.approx(1 / 31)
    # price * (1 - 1/31): the whole credit Stripe would otherwise prorate.
    assert consumption.clawback_cents == round(9900 * (1 - 1 / 31))

  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_partial_use_ahead_of_time_claws_back_the_excess(self, get_pool):
    get_pool.return_value = _pool(8000, 2000)  # 75 % used, ~3 % elapsed

    consumption = plan_credit_clawback(_subscription(), MagicMock(), now=DAY_ONE)

    assert consumption is not None
    assert consumption.clawback_cents == round(9900 * (0.75 - 1 / 31))
    assert "6,000 of 8,000 credits" in consumption.description

  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_bonus_credits_never_count_as_negative_use(self, get_pool):
    get_pool.return_value = _pool(8000, 9000)

    consumption = plan_credit_clawback(_subscription(), MagicMock(), now=HALFWAY)

    assert consumption is not None
    assert consumption.consumed == 0
    assert consumption.clawback_cents == 0

  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_no_pool_and_no_snapshot_means_unknown(self, get_pool):
    get_pool.return_value = None

    assert plan_credit_clawback(_subscription(), MagicMock(), now=DAY_ONE) is None

  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_an_unreadable_pool_is_unknown_not_an_error(self, get_pool):
    get_pool.side_effect = RuntimeError("db hiccup")

    assert plan_credit_clawback(_subscription(), MagicMock(), now=DAY_ONE) is None

  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_the_snapshot_stands_in_for_a_deleted_pool(self, get_pool):
    # The delete keeps what it saw; a later pass (the backfill) reaches the
    # same clawback with the pool gone, at the elapsed share of the delete.
    get_pool.return_value = _pool(8000, 0)
    sub = _subscription()
    seen = plan_credit_clawback(sub, MagicMock(), now=DAY_ONE)
    assert seen is not None
    snapshot_credit_consumption(sub, seen, now=DAY_ONE)
    assert sub.subscription_metadata[CREDIT_CONSUMPTION_SNAPSHOT]["consumed"] == 8000

    get_pool.return_value = None
    later = plan_credit_clawback(sub, MagicMock(), now=HALFWAY)

    assert later is not None
    assert later.clawback_cents == seen.clawback_cents
    assert later.elapsed_fraction == seen.elapsed_fraction

  def test_only_graph_subscriptions_have_a_pool(self):
    sub = _subscription(resource_type="repository")

    assert plan_credit_clawback(sub, MagicMock(), now=DAY_ONE) is None

  @patch("robosystems.operations.billing.invoice_credits.GraphCredits.get_by_graph_id")
  def test_without_period_bounds_nothing_can_be_prorated(self, get_pool):
    get_pool.return_value = _pool(8000, 0)
    sub = _subscription(current_period_end=None)

    assert plan_credit_clawback(sub, MagicMock(), now=DAY_ONE) is None
    get_pool.assert_not_called()
