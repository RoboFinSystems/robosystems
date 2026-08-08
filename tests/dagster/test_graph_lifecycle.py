"""Tests for graph lifecycle Dagster jobs and sensors."""

from unittest.mock import MagicMock, patch

from dagster import build_sensor_context

from robosystems.dagster.sensors.graph_lifecycle import (
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

      context = build_sensor_context()
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

      context = build_sensor_context()
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 0

  def test_session_cleanup(self):
    """Database session is always cleaned up."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context()
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

      context = build_sensor_context()
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

      context = build_sensor_context()
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

      context = build_sensor_context()
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

      context = build_sensor_context()
      assert list(stalled_provisioning_sensor(context)) == []

  def test_session_cleanup(self):
    from robosystems.dagster.sensors.graph_lifecycle import (
      stalled_provisioning_sensor,
    )

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context()
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

  def test_marks_stalled_subscription_failed_and_starts_retention(self):
    """`failed` + `ends_at` is what makes the lifecycle sensors act on it."""
    subscription = MagicMock()
    subscription.id = "bsub_stalled"
    subscription.status = "provisioning"
    subscription.ends_at = None
    subscription.subscription_metadata = {}

    result = self._run(subscription, ["bsub_stalled"])

    assert result["reaped_count"] == 1
    assert subscription.status == "failed"
    assert subscription.ends_at is not None
    assert subscription.subscription_metadata["error"]
    assert subscription.subscription_metadata["failed_at"]

  def test_skips_a_subscription_that_completed_since_the_sensor_read(self):
    """The sensor's read and this write are minutes apart.

    A redelivery can succeed in between, and writing the row off then would
    tear down a graph that is working and paid for.
    """
    subscription = MagicMock()
    subscription.id = "bsub_recovered"
    subscription.status = "active"
    subscription.ends_at = None

    result = self._run(subscription, ["bsub_recovered"])

    assert result["reaped_count"] == 0
    assert subscription.status == "active"
    assert subscription.ends_at is None

  def test_missing_subscription_is_skipped(self):
    result = self._run(None, ["bsub_gone"])

    assert result["reaped_count"] == 0
    assert result["total_requested"] == 1
