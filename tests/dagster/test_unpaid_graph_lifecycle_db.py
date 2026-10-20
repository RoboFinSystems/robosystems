"""A graph whose Stripe subscription lapses reaches teardown on its own:
``unpaid`` starts the retention clock, a payment stops it, and a cancel that
began at Stripe waits out retention instead of tearing down at once."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from dagster import DagsterInstance, build_op_context, build_sensor_context

from robosystems.config import env
from robosystems.config.deprovisioning import get_deprovisioning_config
from robosystems.dagster.jobs.billing import (
  _handle_subscription_deleted,
  _handle_subscription_updated,
)
from robosystems.dagster.jobs.graph_lifecycle import (
  DeprovisionGraphsConfig,
  SuspendGraphsConfig,
  _end_unpaid_subscription,
  deprovision_suspended_graphs,
  suspend_expired_graphs,
)
from robosystems.dagster.sensors.graph_lifecycle import (
  expired_graph_subscription_sensor,
  suspended_graph_deprovisioning_sensor,
)
from robosystems.models.core import Graph, Org, OrgType
from robosystems.models.core.billing import BillingSubscription
from robosystems.models.core.billing.subscription import CancellationType
from robosystems.models.core.graph import GraphStatus


def _graph_with_subscription(
  session, sub_status: str, graph_status: str = "active", **fields
):
  org = Org.create(
    name=f"Lapse {uuid.uuid4().hex[:6]}", org_type=OrgType.PERSONAL, session=session
  )
  graph = Graph.create(
    graph_id=f"kg{uuid.uuid4().hex[:16]}",
    org_id=org.id,
    graph_name="Lapse",
    graph_type="generic",
    session=session,
  )
  graph.status = graph_status
  sub = BillingSubscription(
    org_id=org.id,
    resource_type="graph",
    resource_id=graph.graph_id,
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status=sub_status,
    stripe_subscription_id=f"sub_test_{uuid.uuid4().hex[:10]}",
    subscription_metadata={},
    **fields,
  )
  session.add(sub)
  session.commit()
  return graph, sub


def _stripe_event(sub: BillingSubscription, status: str) -> dict:
  now = int(datetime.now(UTC).timestamp())
  return {
    "id": sub.stripe_subscription_id,
    "object": "subscription",
    "status": status,
    "cancel_at_period_end": False,
    "metadata": {},
    "items": {
      "object": "list",
      "data": [
        {
          "id": "si_test_1",
          "current_period_start": now,
          "current_period_end": now + 30 * 86400,
        }
      ],
    },
  }


def _selected(sensor, session) -> list[str]:
  factory = MagicMock(return_value=session)
  with patch("robosystems.database.session", factory):
    runs = list(sensor(build_sensor_context(instance=DagsterInstance.ephemeral())))
  if not runs:
    return []
  op_config = next(iter(runs[0].run_config["ops"].values()))
  return op_config["config"]["graph_ids"]


@pytest.mark.unit
class TestUnpaidStartsTheClock:
  @pytest.mark.asyncio
  async def test_unpaid_stamps_ends_at_without_an_immediate_bypass(self, test_db):
    _, sub = _graph_with_subscription(test_db, "past_due")

    await _handle_subscription_updated(
      _stripe_event(sub, "unpaid"), test_db, MagicMock()
    )

    test_db.expire_all()
    row = test_db.get(BillingSubscription, sub.id)
    assert row.status == "unpaid"
    assert row.ends_at is not None
    assert row.cancellation_type is None

  @pytest.mark.asyncio
  async def test_a_payment_stops_the_clock_and_restores_the_graph(self, test_db):
    graph, sub = _graph_with_subscription(
      test_db,
      "unpaid",
      graph_status=GraphStatus.SUSPENDED.value,
      ends_at=datetime.now(UTC) - timedelta(days=2),
    )

    await _handle_subscription_updated(
      _stripe_event(sub, "active"), test_db, MagicMock()
    )

    test_db.expire_all()
    row = test_db.get(BillingSubscription, sub.id)
    assert row.status == "active"
    assert row.ends_at is None
    assert test_db.get(Graph, graph.graph_id).status == GraphStatus.ACTIVE.value


@pytest.mark.unit
class TestStripeOriginatedCancelsKeepRetention:
  @pytest.mark.asyncio
  async def test_a_canceled_status_from_stripe_is_lapsed_not_immediate(self, test_db):
    _, sub = _graph_with_subscription(test_db, "past_due")

    await _handle_subscription_updated(
      _stripe_event(sub, "canceled"), test_db, MagicMock()
    )

    test_db.expire_all()
    row = test_db.get(BillingSubscription, sub.id)
    assert row.status == "canceled"
    assert row.cancellation_type == CancellationType.LAPSED.value

  @pytest.mark.asyncio
  async def test_a_stripe_deletion_is_lapsed_not_immediate(self, test_db):
    _, sub = _graph_with_subscription(test_db, "unpaid", ends_at=datetime.now(UTC))

    await _handle_subscription_deleted(
      _stripe_event(sub, "canceled"), test_db, MagicMock()
    )

    test_db.expire_all()
    row = test_db.get(BillingSubscription, sub.id)
    assert row.status == "canceled"
    assert row.cancellation_type == CancellationType.LAPSED.value


@pytest.mark.unit
class TestTheSensorsSeeALapsedGraph:
  def test_an_unpaid_graph_past_ends_at_is_suspended(self, test_db):
    graph, _ = _graph_with_subscription(
      test_db, "unpaid", ends_at=datetime.now(UTC) - timedelta(minutes=1)
    )

    assert graph.graph_id in _selected(expired_graph_subscription_sensor, test_db)

  def test_a_past_due_graph_is_never_suspended(self, test_db):
    graph, _ = _graph_with_subscription(
      test_db, "past_due", ends_at=datetime.now(UTC) - timedelta(days=30)
    )

    assert graph.graph_id not in _selected(expired_graph_subscription_sensor, test_db)

  def test_a_lapsed_graph_waits_out_retention_before_deprovisioning(self, test_db):
    retention = get_deprovisioning_config().retention_days
    within, _ = _graph_with_subscription(
      test_db,
      "unpaid",
      graph_status=GraphStatus.SUSPENDED.value,
      ends_at=datetime.now(UTC) - timedelta(days=retention - 1),
    )
    past, _ = _graph_with_subscription(
      test_db,
      "canceled",
      graph_status=GraphStatus.SUSPENDED.value,
      ends_at=datetime.now(UTC) - timedelta(days=retention + 1),
      cancellation_type=CancellationType.LAPSED.value,
    )

    selected = _selected(suspended_graph_deprovisioning_sensor, test_db)

    assert within.graph_id not in selected
    assert past.graph_id in selected


@pytest.mark.unit
class TestTeardownEndsTheStripeSubscriptionFirst:
  def test_an_unpaid_subscription_is_canceled_at_stripe_then_lapsed(self, test_db):
    graph, sub = _graph_with_subscription(
      test_db, "unpaid", graph_status=GraphStatus.SUSPENDED.value
    )
    provider = MagicMock()

    with (
      patch.object(env, "BILLING_ENABLED", True),
      patch(
        "robosystems.operations.providers.payment_provider.get_payment_provider",
        return_value=provider,
      ),
    ):
      assert _end_unpaid_subscription(graph.graph_id, test_db, MagicMock())

    provider.cancel_subscription.assert_called_once_with(sub.stripe_subscription_id)
    test_db.expire_all()
    row = test_db.get(BillingSubscription, sub.id)
    assert row.status == "canceled"
    assert row.cancellation_type == CancellationType.LAPSED.value

  def test_a_failed_stripe_cancel_skips_the_teardown(self, test_db):
    graph, sub = _graph_with_subscription(
      test_db, "unpaid", graph_status=GraphStatus.SUSPENDED.value
    )
    provider = MagicMock()
    provider.cancel_subscription.side_effect = RuntimeError("stripe down")

    with (
      patch.object(env, "BILLING_ENABLED", True),
      patch(
        "robosystems.operations.providers.payment_provider.get_payment_provider",
        return_value=provider,
      ),
    ):
      assert not _end_unpaid_subscription(graph.graph_id, test_db, MagicMock())

    test_db.expire_all()
    assert test_db.get(BillingSubscription, sub.id).status == "unpaid"


def _run_op(op, config, session) -> dict:
  db = MagicMock()
  db.get_session.return_value.__enter__ = lambda *_: session
  db.get_session.return_value.__exit__ = lambda *_: False
  return op(build_op_context(), db, config)


def _deprovision(graph_ids: list[str], session) -> list[str]:
  """Run the deprovision op with the service stubbed; the graphs it reached."""
  torn_down: list[str] = []
  ok = MagicMock()
  ok.status = "success"
  ok.errors = []

  async def _deprovision_graph(graph_id, _session, create_backup=True):
    torn_down.append(graph_id)
    return ok

  with (
    patch(
      "robosystems.operations.graph.deprovision_service.GraphDeprovisionService"
    ) as svc_cls,
    patch(
      "robosystems.operations.providers.payment_provider.get_payment_provider",
      return_value=MagicMock(),
    ),
  ):
    svc_cls.return_value.deprovision_graph = _deprovision_graph
    _run_op(
      deprovision_suspended_graphs,
      DeprovisionGraphsConfig(graph_ids=graph_ids),
      session,
    )
  return torn_down


def _past_retention() -> datetime:
  return datetime.now(UTC) - timedelta(
    days=get_deprovisioning_config().retention_days + 1
  )


@pytest.mark.unit
class TestTheOpsRecheckWhatTheSensorSelected:
  """A payment can land between the sensor's tick and the run. The ops act
  on the subscription and the graph as they stand, not on the selection."""

  def test_suspend_skips_a_graph_whose_payment_landed_after_the_tick(self, test_db):
    graph, sub = _graph_with_subscription(
      test_db, "unpaid", ends_at=datetime.now(UTC) - timedelta(days=1)
    )
    assert _selected(expired_graph_subscription_sensor, test_db) == [graph.graph_id]
    sub.status = "active"
    sub.ends_at = None
    test_db.commit()

    result = _run_op(
      suspend_expired_graphs, SuspendGraphsConfig(graph_ids=[graph.graph_id]), test_db
    )

    test_db.expire_all()
    assert result["suspended_count"] == 0
    assert test_db.get(Graph, graph.graph_id).status == GraphStatus.ACTIVE.value

  def test_suspend_still_suspends_a_lapsed_graph(self, test_db):
    graph, _ = _graph_with_subscription(
      test_db, "unpaid", ends_at=datetime.now(UTC) - timedelta(days=1)
    )

    result = _run_op(
      suspend_expired_graphs, SuspendGraphsConfig(graph_ids=[graph.graph_id]), test_db
    )

    test_db.expire_all()
    assert result["suspended_count"] == 1
    assert test_db.get(Graph, graph.graph_id).status == GraphStatus.SUSPENDED.value

  def test_deprovision_skips_a_graph_restored_before_the_run(self, test_db):
    graph, sub = _graph_with_subscription(
      test_db,
      "unpaid",
      graph_status=GraphStatus.SUSPENDED.value,
      ends_at=_past_retention(),
    )
    assert graph.graph_id in _selected(suspended_graph_deprovisioning_sensor, test_db)
    # The payment's webhook: the subscription revives and the graph is restored.
    sub.status = "active"
    sub.ends_at = None
    graph.status = GraphStatus.ACTIVE.value
    test_db.commit()

    assert _deprovision([graph.graph_id], test_db) == []

    test_db.expire_all()
    assert test_db.get(Graph, graph.graph_id).status == GraphStatus.ACTIVE.value
    assert test_db.get(BillingSubscription, sub.id).status == "active"

  def test_deprovision_skips_a_suspended_graph_whose_subscription_revived(
    self, test_db
  ):
    """The webhook revived the subscription first; the graph's restore is
    still to come. Nothing is torn down and the subscription is not
    canceled under it."""
    graph, sub = _graph_with_subscription(
      test_db,
      "unpaid",
      graph_status=GraphStatus.SUSPENDED.value,
      ends_at=_past_retention(),
    )
    sub.status = "active"
    sub.ends_at = None
    test_db.commit()

    assert _deprovision([graph.graph_id], test_db) == []

    test_db.expire_all()
    assert test_db.get(Graph, graph.graph_id).status == GraphStatus.SUSPENDED.value
    assert test_db.get(BillingSubscription, sub.id).status == "active"

  def test_deprovision_tears_down_a_lapsed_suspended_graph(self, test_db):
    graph, _ = _graph_with_subscription(
      test_db,
      "unpaid",
      graph_status=GraphStatus.SUSPENDED.value,
      ends_at=_past_retention(),
    )

    assert _deprovision([graph.graph_id], test_db) == [graph.graph_id]

  def test_deprovision_still_retries_a_stranded_teardown(self, test_db):
    """A teardown that stamped deleted_at and died is retried regardless of
    what the subscription says now."""
    graph, sub = _graph_with_subscription(
      test_db,
      "canceled",
      graph_status=GraphStatus.SUSPENDED.value,
      ends_at=_past_retention(),
    )
    graph.deleted_at = datetime.now(UTC) - timedelta(hours=2)
    sub.status = "active"
    sub.ends_at = None
    test_db.commit()

    assert _deprovision([graph.graph_id], test_db) == [graph.graph_id]
