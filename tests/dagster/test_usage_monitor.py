"""Tests for graph usage monitor sensor."""

from unittest.mock import AsyncMock, MagicMock, patch

from dagster import build_sensor_context

from robosystems.dagster.sensors.usage_monitor import (
  _ALERT_DEDUP_TTL_SECONDS,
  _ALERTS_ENABLED_FLAG,
  USAGE_MONITOR_PRINCIPAL,
  _capacity_alert_recipients,
  graph_usage_monitor_sensor,
)

RECIPIENTS = "robosystems.dagster.sensors.usage_monitor._capacity_alert_recipients"


def _admin(user_id, email):
  user = MagicMock()
  user.id = user_id
  user.email = email
  user.name = email.split("@")[0]
  return user


def _make_graph(graph_id="kg_test123", tier="ladybug-standard"):
  """Create a mock Graph object."""
  graph = MagicMock()
  graph.graph_id = graph_id
  graph.graph_tier = tier
  graph.is_repository = False
  graph.parent_graph_id = None
  graph.status = "active"
  return graph


class TestSensorEnablement:
  """The sensor must be on, and must stay switchable off without a deploy.

  It shipped STOPPED on 2026-04-04 next to a soft cap, the cap went hard on
  2026-05-13, and nobody restarted the sensor — so the hard 413 lost the 80%
  warning meant to precede it. Pinned here so that cannot recur silently.
  """

  def test_sensor_default_status_is_running(self):
    from dagster import DefaultSensorStatus

    from robosystems.dagster.sensors.usage_monitor import _SENSOR_STATUS

    assert _SENSOR_STATUS is DefaultSensorStatus.RUNNING

  @patch("robosystems.config.parameter_store.get_parameter_value")
  @patch("robosystems.database.session")
  def test_ssm_flag_false_skips_before_touching_the_database(
    self, mock_session_factory, mock_get_param
  ):
    """The kill switch short-circuits before any DB or Graph API work."""
    mock_get_param.return_value = "false"
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db

    result = graph_usage_monitor_sensor(build_sensor_context())

    assert result is not None
    assert _ALERTS_ENABLED_FLAG in str(result)
    mock_db.query.assert_not_called()

  @patch("robosystems.config.parameter_store.get_parameter_value")
  @patch("robosystems.database.session")
  def test_defaults_to_enabled_when_flag_unset(
    self, mock_session_factory, mock_get_param
  ):
    """Absent config, alerting runs — it should fail toward telling you."""
    mock_get_param.side_effect = lambda _key, default="": default
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = []

    graph_usage_monitor_sensor(build_sensor_context())

    mock_db.query.assert_called_once()


class TestGraphUsageMonitorSensor:
  """Tests for graph usage monitor sensor."""

  @patch("robosystems.database.session")
  def test_skips_when_no_graphs(self, mock_session_factory):
    """Test sensor skips when no active user graphs exist."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = []

    context = build_sensor_context()
    result = graph_usage_monitor_sensor(context)

    assert result is not None  # Returns SkipReason
    mock_db.close.assert_called_once()

  @patch("robosystems.operations.aws.ses.ses_service")
  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_sends_alert_on_approaching(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
    mock_ses,
  ):
    """Test sensor sends email when storage is approaching limit."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db

    graph = _make_graph()
    mock_db.query.return_value.filter.return_value.all.return_value = [graph]

    # Two admins resolve for the graph (one explicit, one implicit via org role)
    admin_a, admin_b = (
      _admin("usr_a", "a@example.com"),
      _admin("usr_b", "b@example.com"),
    )

    # Storage check returns approaching
    mock_check_storage.return_value = {
      "total_storage_gb": 17.0,
      "limit_gb": 20.0,
      "usage_percentage": 85.0,
      "status": "approaching",
      "databases": [{"graph_id": "kg_test123", "is_parent": True, "size_mb": 17000}],
    }

    # Valkey: no existing alert
    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis
    mock_redis.exists.return_value = False

    # SES: success
    mock_ses.send_capacity_warning_email = AsyncMock(return_value=True)

    context = build_sensor_context()
    with patch(RECIPIENTS, return_value=[admin_a, admin_b]):
      graph_usage_monitor_sensor(context)

    # Every admin is emailed, and a dedup key is set per delivered recipient
    sent_to = [
      c.kwargs["user_email"]
      for c in mock_ses.send_capacity_warning_email.call_args_list
    ]
    assert sent_to == ["a@example.com", "b@example.com"]
    dedup_keys = [c[0][0] for c in mock_redis.setex.call_args_list]
    assert dedup_keys == [
      "usage_alert:kg_test123:approaching:usr_a",
      "usage_alert:kg_test123:approaching:usr_b",
    ]
    assert all(
      c[0][1] == _ALERT_DEDUP_TTL_SECONDS for c in mock_redis.setex.call_args_list
    )

    # The snapshot is attributed to the monitor, not to a member
    added = [
      a.args[0] for a in mock_db.add.call_args_list if hasattr(a.args[0], "user_id")
    ]
    assert added and all(r.user_id == USAGE_MONITOR_PRINCIPAL for r in added)

    mock_db.close.assert_called_once()

  @patch("robosystems.operations.aws.ses.ses_service")
  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_no_recipient_records_snapshot_but_sends_nothing(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
    mock_ses,
  ):
    """A graph with no reachable admin still gets its snapshot row (the usage
    surfaces read it), but no email and no dedup key — the next tick retries."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]
    mock_check_storage.return_value = {
      "total_storage_gb": 17.0,
      "limit_gb": 20.0,
      "usage_percentage": 85.0,
      "status": "approaching",
      "databases": [],
    }
    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis
    mock_redis.exists.return_value = False
    mock_ses.send_capacity_warning_email = AsyncMock(return_value=True)

    with patch(RECIPIENTS, return_value=[]):
      graph_usage_monitor_sensor(build_sensor_context())

    mock_ses.send_capacity_warning_email.assert_not_called()
    mock_redis.setex.assert_not_called()
    assert mock_db.add.called

  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_skips_already_alerted(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
  ):
    """Test sensor skips graphs that were already alerted."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]

    mock_check_storage.return_value = {
      "total_storage_gb": 17.0,
      "limit_gb": 20.0,
      "usage_percentage": 85.0,
      "status": "approaching",
      "databases": [],
    }

    # Valkey: already alerted (for the one admin who exists)
    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis
    mock_redis.exists.return_value = True

    context = build_sensor_context()
    with (
      patch(RECIPIENTS, return_value=[_admin("usr_a", "a@example.com")]),
      patch("robosystems.operations.aws.ses.ses_service") as mock_ses,
    ):
      mock_ses.send_capacity_warning_email = AsyncMock(return_value=True)
      graph_usage_monitor_sensor(context)

    # No email sent — this admin's dedup key exists
    mock_ses.send_capacity_warning_email.assert_not_called()
    mock_redis.setex.assert_not_called()
    mock_db.close.assert_called_once()

  @patch("robosystems.operations.aws.ses.ses_service")
  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_partial_delivery_retries_only_the_failed_admin(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
    mock_ses,
  ):
    """One admin's send fails: only the delivered admin gets a dedup key, so
    the next tick skips them and retries the failed one — instead of a
    single graph-level key silencing the failed admin for the whole TTL."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]
    mock_check_storage.return_value = {
      "total_storage_gb": 17.0,
      "limit_gb": 20.0,
      "usage_percentage": 85.0,
      "status": "approaching",
      "databases": [],
    }
    admin_a, admin_b = (
      _admin("usr_a", "a@example.com"),
      _admin("usr_b", "b@example.com"),
    )

    # Tick 1: nobody alerted yet; delivery to B fails.
    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis
    mock_redis.exists.return_value = False
    mock_ses.send_capacity_warning_email = AsyncMock(side_effect=[True, False])

    with patch(RECIPIENTS, return_value=[admin_a, admin_b]):
      graph_usage_monitor_sensor(build_sensor_context())

    assert [c[0][0] for c in mock_redis.setex.call_args_list] == [
      "usage_alert:kg_test123:approaching:usr_a"
    ]

    # Tick 2: A's key exists, B's does not — only B is emailed.
    mock_redis.reset_mock()
    mock_redis.exists.side_effect = lambda key: key.endswith(":usr_a")
    mock_ses.send_capacity_warning_email = AsyncMock(return_value=True)

    with patch(RECIPIENTS, return_value=[admin_a, admin_b]):
      graph_usage_monitor_sensor(build_sensor_context())

    sent_to = [
      c.kwargs["user_email"]
      for c in mock_ses.send_capacity_warning_email.call_args_list
    ]
    assert sent_to == ["b@example.com"]
    assert [c[0][0] for c in mock_redis.setex.call_args_list] == [
      "usage_alert:kg_test123:approaching:usr_b"
    ]

  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_no_alert_when_healthy(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
  ):
    """Test sensor does nothing for healthy graphs."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]

    mock_check_storage.return_value = {
      "total_storage_gb": 5.0,
      "limit_gb": 20.0,
      "usage_percentage": 25.0,
      "status": "healthy",
      "databases": [],
    }

    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis

    context = build_sensor_context()
    graph_usage_monitor_sensor(context)

    # No dedup check needed for healthy graphs
    mock_redis.exists.assert_not_called()
    mock_db.close.assert_called_once()

  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_handles_storage_check_failure(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
  ):
    """Test sensor handles storage check failures gracefully."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]

    # Storage check fails
    mock_check_storage.side_effect = Exception("Connection refused")

    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis

    context = build_sensor_context()
    # Should not raise
    graph_usage_monitor_sensor(context)

    mock_db.close.assert_called_once()


class TestCapacityAlertRecipients:
  """The recipient set is every graph admin — explicit rows AND the owning
  org's owners/admins, who hold implicit graph admin with no GraphUser row.
  The old lookup took one arbitrary explicit admin, so an org-owned graph with
  only implicit admins got no email at all."""

  @staticmethod
  def _user(session, email, active=True):
    from robosystems.models.core import User

    u = User(email=email, name=email.split("@")[0], password_hash="x", is_active=active)
    session.add(u)
    session.commit()
    session.refresh(u)
    return u

  def test_implicit_org_admins_are_recipients_without_a_graph_user_row(self, test_db):
    from uuid import uuid4

    from robosystems.models.core import (
      Graph,
      GraphRole,
      GraphUser,
      Org,
      OrgRole,
      OrgType,
      OrgUser,
    )

    org = Org.create(name="Cap Org", org_type=OrgType.TEAM, session=test_db)
    owner = self._user(test_db, f"owner+{uuid4().hex[:6]}@example.com")
    org_admin = self._user(test_db, f"orgadmin+{uuid4().hex[:6]}@example.com")
    member = self._user(test_db, f"member+{uuid4().hex[:6]}@example.com")
    explicit = self._user(test_db, f"explicit+{uuid4().hex[:6]}@example.com")
    viewer = self._user(test_db, f"viewer+{uuid4().hex[:6]}@example.com")
    for u, r in (
      (owner, OrgRole.OWNER),
      (org_admin, OrgRole.ADMIN),
      (member, OrgRole.MEMBER),
      (explicit, OrgRole.MEMBER),
      (viewer, OrgRole.MEMBER),
    ):
      OrgUser.create(org_id=org.id, user_id=u.id, role=r, session=test_db)
    graph = Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=org.id,
      graph_name="Cap Graph",
      graph_type="generic",
      session=test_db,
    )
    GraphUser.create(
      user_id=explicit.id,
      graph_id=graph.graph_id,
      role=GraphRole.ADMIN,
      session=test_db,
    )
    GraphUser.create(
      user_id=viewer.id, graph_id=graph.graph_id, role=GraphRole.VIEWER, session=test_db
    )
    # The owner also holds an explicit admin row: must not be emailed twice
    GraphUser.create(
      user_id=owner.id, graph_id=graph.graph_id, role=GraphRole.ADMIN, session=test_db
    )

    recipients = _capacity_alert_recipients(test_db, graph)

    emails = sorted(u.email for u in recipients)
    assert emails == sorted([owner.email, org_admin.email, explicit.email])
    assert len(recipients) == 3

  def test_inactive_and_emailless_admins_are_skipped(self, test_db):
    from uuid import uuid4

    from robosystems.models.core import Graph, Org, OrgRole, OrgType, OrgUser

    org = Org.create(name="Cap Org 2", org_type=OrgType.TEAM, session=test_db)
    inactive = self._user(test_db, f"gone+{uuid4().hex[:6]}@example.com", active=False)
    OrgUser.create(
      org_id=org.id, user_id=inactive.id, role=OrgRole.OWNER, session=test_db
    )
    graph = Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=org.id,
      graph_name="Cap Graph 2",
      graph_type="generic",
      session=test_db,
    )

    assert _capacity_alert_recipients(test_db, graph) == []
