"""Tests for graph lifecycle Dagster jobs and sensors."""

from unittest.mock import MagicMock, patch

import pytest
from dagster import DagsterInstance, DagsterRunStatus, SkipReason, build_sensor_context
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.utils import make_new_run_id

from robosystems.dagster.sensors.graph_lifecycle import (
  expired_graph_subscription_sensor,
  stalled_provisioning_sensor,
  suspended_graph_deprovisioning_sensor,
)


class TestSuspendedGraphDeprovisioningSensor:
  """Tests for the suspended_graph_deprovisioning_sensor."""

  def test_finds_ready_graphs(self):
    """Detects suspended graphs past the retention period."""
    mock_sub = MagicMock()
    mock_sub.resource_id = "kg_ready"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub
      ]

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["deprovision_suspended_graphs"]["config"]
      assert config["graph_ids"] == ["kg_ready"]

  def test_skips_when_no_matches(self):
    """Returns nothing when no graphs are ready."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 0

  def test_re_selects_graphs_stranded_mid_teardown(self):
    """A graph whose teardown started (deleted_at set) but never reached
    DEPROVISIONED is excluded by the retention query (deleted_at IS NULL); the
    sensor's second query re-selects it so it is retried rather than stranded
    forever with its .lbug / registry entry / tenant rows intact."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      # No subscription-driven graphs...
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []
      # ...but one stranded graph (query(Graph.graph_id).filter(...).all()).
      mock_session.query.return_value.filter.return_value.all.return_value = [
        ("kg_stranded",)
      ]

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["deprovision_suspended_graphs"]["config"]
      assert config["graph_ids"] == ["kg_stranded"]

  def test_dedups_a_graph_that_is_both_ready_and_stranded(self):
    mock_sub = MagicMock()
    mock_sub.resource_id = "kg_both"
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub
      ]
      mock_session.query.return_value.filter.return_value.all.return_value = [
        ("kg_both",)
      ]

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["deprovision_suspended_graphs"]["config"]
      assert config["graph_ids"] == ["kg_both"]

  def test_session_cleanup(self):
    """Database session is always cleaned up."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      list(suspended_graph_deprovisioning_sensor(context))

      mock_db.remove.assert_called_once()

  def test_multiple_ready_graphs(self):
    """Handles multiple graphs ready for deprovisioning."""
    mock_sub1 = MagicMock()
    mock_sub1.resource_id = "kg_ready1"
    mock_sub2 = MagicMock()
    mock_sub2.resource_id = "kg_ready2"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub1,
        mock_sub2,
      ]

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["deprovision_suspended_graphs"]["config"]
      assert config["graph_ids"] == ["kg_ready1", "kg_ready2"]

  def test_query_includes_immediate_cancellation_clause(self):
    """The deprovision sensor query must OR `cancellation_type == 'immediate'`
    against the retention-window clause, so user-initiated immediate
    cancellations bypass the 7-day wait."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      list(suspended_graph_deprovisioning_sensor(context))

      filter_call = mock_session.query.return_value.join.return_value.filter
      assert filter_call.called, "filter() was never invoked on the query"

      # Compile each clause expression to its SQL string and look for the
      # cancellation_type OR branch. We don't bind a dialect — Compile.__str__
      # returns the parameterized SQL which is enough to confirm the clause
      # is present.
      compiled_clauses = " ".join(str(arg) for arg in filter_call.call_args.args)
      assert "cancellation_type" in compiled_clauses, (
        "Expected the deprovision query to filter on cancellation_type; "
        f"got: {compiled_clauses}"
      )
      assert "ends_at" in compiled_clauses, (
        "Expected the deprovision query to still include the ends_at "
        "retention-window clause"
      )


class TestExpiredGraphSubscriptionSensor:
  """Tests for the expired_graph_subscription_sensor (the suspend stage)."""

  def test_finds_expired_subs_on_active_graphs(self):
    """Yields one suspend run listing active graphs with terminal/expired subs."""
    mock_sub = MagicMock()
    mock_sub.resource_id = "kg_expired"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub
      ]

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(expired_graph_subscription_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["suspend_expired_graphs"]["config"]
      assert config["graph_ids"] == ["kg_expired"]

  def test_skips_when_no_matches(self):
    """Returns nothing when no subscriptions are expired."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(expired_graph_subscription_sensor(context))

      assert len(runs) == 0

  def test_session_cleanup(self):
    """Database session is always cleaned up."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      list(expired_graph_subscription_sensor(context))

      mock_db.remove.assert_called_once()

  def test_query_includes_immediate_cancellation_bypass(self):
    """Defense-in-depth: the suspend query must OR
    `cancellation_type == 'immediate'` against `ends_at < now`, so an immediate
    cancel suspends even if a downstream event (e.g. the Stripe
    subscription.deleted handler) pushed ends_at into the future. Mirrors the
    deprovision sensor's immediate-bypass."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      list(expired_graph_subscription_sensor(context))

      filter_call = mock_session.query.return_value.join.return_value.filter
      assert filter_call.called, "filter() was never invoked on the query"

      compiled_clauses = " ".join(str(arg) for arg in filter_call.call_args.args)
      assert "cancellation_type" in compiled_clauses, (
        "Expected the suspend query to OR on cancellation_type (immediate "
        f"bypass); got: {compiled_clauses}"
      )
      assert "ends_at" in compiled_clauses, (
        "Expected the suspend query to still include the ends_at clause"
      )


class TestStalledProvisioningSensor:
  """Tests for the stalled_provisioning_sensor.

  `provisioning` is the one subscription state nothing else revisits: it is
  neither active nor terminal, so both lifecycle sensors skip it, and a
  customer who paid can sit in it indefinitely. These pin that this sensor is
  the thing that ends it.
  """

  def test_finds_stalled_subscriptions(self):
    from robosystems.dagster.sensors.graph_lifecycle import (
      stalled_provisioning_sensor,
    )

    mock_sub = MagicMock()
    mock_sub.id = "bsub_stalled"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = [mock_sub]

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      runs = list(stalled_provisioning_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["reap_stalled_provisioning"]["config"]
      assert config["subscription_ids"] == ["bsub_stalled"]

  def test_skips_when_none_stalled(self):
    from robosystems.dagster.sensors.graph_lifecycle import (
      stalled_provisioning_sensor,
    )

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      assert list(stalled_provisioning_sensor(context)) == []

  def test_session_cleanup(self):
    from robosystems.dagster.sensors.graph_lifecycle import (
      stalled_provisioning_sensor,
    )

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context(instance=DagsterInstance.ephemeral())
      list(stalled_provisioning_sensor(context))

      mock_db.remove.assert_called_once()


class TestReapStalledProvisioning:
  """Tests for the reap_stalled_provisioning op."""

  def _run(self, subscription, subscription_ids):
    from contextlib import contextmanager

    from dagster import build_op_context

    from robosystems.dagster.jobs.graph_lifecycle import (
      ReapStalledProvisioningConfig,
      reap_stalled_provisioning,
    )

    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = subscription

    @contextmanager
    def get_session():
      yield session

    db = MagicMock()
    db.get_session = get_session

    return reap_stalled_provisioning(
      build_op_context(),
      db,
      ReapStalledProvisioningConfig(subscription_ids=subscription_ids),
    )

  def test_delegates_the_write_off_to_the_model(self):
    """The op's job is wiring; the disposition lives on the model.

    ``write_off_stalled_provisioning`` carries the staleness predicate and the
    ``failed`` + ``ends_at`` write — its behavior is pinned against real
    Postgres in ``tests/models/core/billing/test_provisioning_claim.py``.
    """
    subscription = MagicMock()
    subscription.id = "bsub_stalled"
    subscription.status = "provisioning"
    subscription.write_off_stalled_provisioning = MagicMock(return_value=True)

    result = self._run(subscription, ["bsub_stalled"])

    assert result["reaped_count"] == 1
    subscription.write_off_stalled_provisioning.assert_called_once()

  def test_skips_a_subscription_that_completed_since_the_sensor_read(self):
    """The sensor's read and this write are minutes apart.

    A redelivery can succeed in between, and writing the row off then would
    tear down a graph that is working and paid for.
    """
    subscription = MagicMock()
    subscription.id = "bsub_recovered"
    subscription.status = "active"
    subscription.write_off_stalled_provisioning = MagicMock(return_value=True)

    result = self._run(subscription, ["bsub_recovered"])

    assert result["reaped_count"] == 0
    subscription.write_off_stalled_provisioning.assert_not_called()

  def test_skips_a_subscription_reclaimed_since_the_sensor_read(self):
    """Still `provisioning`, but the write-off refuses: a redelivery re-claimed
    the row and stamped a fresh heartbeat, so the run is live, not stalled."""
    subscription = MagicMock()
    subscription.id = "bsub_reclaimed"
    subscription.status = "provisioning"
    subscription.write_off_stalled_provisioning = MagicMock(return_value=False)

    result = self._run(subscription, ["bsub_reclaimed"])

    assert result["reaped_count"] == 0
    subscription.write_off_stalled_provisioning.assert_called_once()

  def test_missing_subscription_is_skipped(self):
    result = self._run(None, ["bsub_gone"])

    assert result["reaped_count"] == 0
    assert result["total_requested"] == 1


@pytest.mark.parametrize(
  "sensor_fn",
  [
    expired_graph_subscription_sensor,
    suspended_graph_deprovisioning_sensor,
    stalled_provisioning_sensor,
  ],
)
class TestSkipWhileARunIsUnfinished:
  """Each tick's run key is new, so a sensor that requested regardless would
  queue a duplicate every tick behind one that has not finished."""

  def _context_with_run(self, sensor_fn, status):
    instance = DagsterInstance.ephemeral()
    instance.add_run(
      DagsterRun(job_name=sensor_fn.job_name, run_id=make_new_run_id(), status=status)
    )
    return build_sensor_context(instance=instance)

  @pytest.mark.parametrize(
    "status", [DagsterRunStatus.NOT_STARTED, DagsterRunStatus.STARTED]
  )
  def test_skips_without_reading_the_database(self, sensor_fn, status):
    with patch("robosystems.database.session") as mock_db:
      results = list(sensor_fn(self._context_with_run(sensor_fn, status)))

    assert len(results) == 1 and isinstance(results[0], SkipReason)
    mock_db.assert_not_called()

  def test_a_finished_run_does_not_hold_the_next(self, sensor_fn):
    context = self._context_with_run(sensor_fn, DagsterRunStatus.SUCCESS)
    with patch("robosystems.database.session") as mock_db:
      list(sensor_fn(context))

    mock_db.assert_called_once()


@pytest.mark.parametrize(
  "job_name",
  [
    "suspend_expired_graphs_job",
    "deprovision_suspended_graphs_job",
    "reap_stalled_provisioning_job",
  ],
)
def test_a_lifecycle_run_cannot_hang_forever(job_name):
  """Each sensor holds off while its job has a run unfinished, and prod run
  monitoring sets no default limit: without its own, one hung run would stop
  that lifecycle for good."""
  from robosystems.dagster.jobs import graph_lifecycle

  job = getattr(graph_lifecycle, job_name)
  assert int(job.tags["dagster/max_runtime"]) > 0
