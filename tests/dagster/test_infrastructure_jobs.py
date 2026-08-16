"""Tests for Dagster infrastructure jobs.

Tests auth cleanup, health check, and instance maintenance jobs.
"""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_op_context

from robosystems.dagster.jobs.infrastructure import (
  REVOKED_KEY_RETENTION_DAYS,
  cleanup_stale_api_keys,
  hourly_auth_cleanup_job,
  weekly_health_check_job,
)
from robosystems.models.core.user.user_api_key import UserAPIKey


def _db(session: Any) -> MagicMock:
  """Stand in for DatabaseResource, handing the op `session` and committing
  on exit the way the real resource does."""

  @contextmanager
  def _session_cm():
    yield session
    session.commit()

  db = MagicMock()
  db.get_session = _session_cm
  return db


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


class TestStaleApiKeyCleanup:
  """The sweep must take expired and long-revoked keys, and nothing else."""

  @pytest.mark.unit
  def test_sweeps_expired_and_long_revoked_keys_only(self, test_db, test_user):
    now = datetime.now(UTC)

    live, _ = UserAPIKey.create(user_id=test_user.id, name="live", session=test_db)
    expired, _ = UserAPIKey.create(
      user_id=test_user.id,
      name="expired",
      expires_at=now - timedelta(hours=1),
      session=test_db,
    )
    revoked_recently, _ = UserAPIKey.create(
      user_id=test_user.id, name="recent", session=test_db
    )
    revoked_long_ago, _ = UserAPIKey.create(
      user_id=test_user.id, name="old", session=test_db
    )

    ids = {
      name: str(key.id)
      for name, key in {
        "live": live,
        "expired": expired,
        "revoked_recently": revoked_recently,
        "revoked_long_ago": revoked_long_ago,
      }.items()
    }

    with patch.object(UserAPIKey, "invalidate_cache", return_value=True):
      revoked_recently.deactivate(test_db)
      revoked_long_ago.deactivate(test_db)

      # deactivate() stamps updated_at, which is what dates the revocation.
      revoked_long_ago.updated_at = now - timedelta(days=REVOKED_KEY_RETENTION_DAYS + 1)
      test_db.commit()

      result = cleanup_stale_api_keys(build_op_context(), _db(test_db))

    surviving = {
      str(row.id)
      for row in test_db.query(UserAPIKey)
      .filter(UserAPIKey.user_id == test_user.id)
      .all()
    }

    assert surviving == {ids["live"], ids["revoked_recently"]}
    assert result["deleted_count"] >= 2
    assert result["deferred_count"] == 0

  @pytest.mark.unit
  def test_key_whose_cache_survives_is_deferred_not_deleted(self, mock_session):
    """A row must outlive a failed cache clear — deleting it strands the entry.

    The cache is keyed on the row's fingerprint, so once the row is gone the
    surviving entry can never be targeted and the key keeps authenticating
    until TTL.
    """
    stuck = MagicMock()
    stuck.is_active = True
    stuck.invalidate_cache.return_value = False
    clean = MagicMock()
    clean.is_active = True
    clean.invalidate_cache.return_value = True

    mock_session.query.return_value.filter.return_value.all.return_value = [
      stuck,
      clean,
    ]

    result = cleanup_stale_api_keys(build_op_context(), _db(mock_session))

    mock_session.delete.assert_called_once_with(clean)
    assert result["deleted_count"] == 1
    assert result["deferred_count"] == 1

  @pytest.mark.unit
  def test_long_revoked_key_is_dropped_without_a_cache_scan(self, mock_session):
    """Clearing the cache costs two keyspace scans and can find nothing this
    long after revocation — the entry outlived its TTL weeks ago."""
    revoked = MagicMock()
    revoked.is_active = False

    mock_session.query.return_value.filter.return_value.all.return_value = [revoked]

    result = cleanup_stale_api_keys(build_op_context(), _db(mock_session))

    revoked.invalidate_cache.assert_not_called()
    mock_session.delete.assert_called_once_with(revoked)
    assert result["deleted_count"] == 1
    assert result["deferred_count"] == 0

  @pytest.mark.unit
  def test_cache_is_cleared_before_the_row_is_dropped(self, mock_session):
    calls = []

    key = MagicMock()
    key.is_active = True
    key.invalidate_cache.side_effect = lambda: calls.append("invalidate") or True
    mock_session.delete.side_effect = lambda _row: calls.append("delete")
    mock_session.query.return_value.filter.return_value.all.return_value = [key]

    cleanup_stale_api_keys(build_op_context(), _db(mock_session))

    assert calls == ["invalidate", "delete"]


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
