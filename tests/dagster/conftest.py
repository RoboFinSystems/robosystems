"""Dagster test fixtures.

Provides mock resources and test utilities for Dagster job and sensor tests.
"""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from dagster import build_op_context, build_sensor_context

from robosystems.dagster.resources import DatabaseResource


class MockDatabaseResource(DatabaseResource):
  """Mock database resource for testing.

  Provides a mock session that can be configured with test data
  without requiring a real database connection.
  """

  def __init__(self, mock_session: MagicMock | None = None):
    super().__init__(database_url="postgresql://test:test@localhost/test")
    self._mock_session = mock_session or MagicMock()

  def setup_for_execution(self, context: Any) -> None:
    """Skip actual database setup in tests."""
    pass

  @contextmanager
  def get_session(self) -> Generator[MagicMock]:
    """Return mock session for testing."""
    yield self._mock_session


@pytest.fixture
def mock_session():
  """Create a mock SQLAlchemy session."""
  session = MagicMock()
  session.query.return_value.filter.return_value.all.return_value = []
  session.query.return_value.filter.return_value.count.return_value = 0
  session.query.return_value.filter.return_value.first.return_value = None
  return session


@pytest.fixture
def mock_db_resource(mock_session):
  """Create a mock database resource."""
  return MockDatabaseResource(mock_session=mock_session)


@pytest.fixture
def op_context():
  """Create a Dagster op execution context for testing."""
  return build_op_context()


@pytest.fixture
def sensor_context():
  """Create a Dagster sensor context for testing."""
  return build_sensor_context()


@pytest.fixture
def mock_expired_api_key():
  """Create a mock expired API key."""
  key = MagicMock()
  key.id = "test_key_123"
  key.user_id = "user_123"
  key.expires_at = datetime.now(UTC) - timedelta(days=1)
  key.is_revoked = False
  return key


@pytest.fixture
def mock_valid_api_key():
  """Create a mock valid API key."""
  key = MagicMock()
  key.id = "test_key_456"
  key.user_id = "user_456"
  key.expires_at = datetime.now(UTC) + timedelta(days=30)
  key.is_revoked = False
  return key


@pytest.fixture
def mock_subscription():
  """Create a mock billing subscription."""
  sub = MagicMock()
  sub.id = "sub_123"
  sub.org_id = "org_123"
  sub.status = "provisioning"
  sub.resource_type = "graph"
  sub.plan_name = "ladybug-standard"
  sub.subscription_metadata = {}
  return sub


@pytest.fixture
def mock_repository_subscription():
  """Create a mock repository subscription."""
  sub = MagicMock()
  sub.id = "sub_456"
  sub.org_id = "org_456"
  sub.status = "provisioning"
  sub.resource_type = "repository"
  sub.plan_name = "starter"
  sub.subscription_metadata = {"repository_name": "sec"}
  return sub


@pytest.fixture
def mock_graph():
  """Create a mock graph database record."""
  graph = MagicMock()
  graph.graph_id = "kg123456789abcdef"
  graph.org_id = "org_123"
  graph.graph_name = "Test Graph"
  graph.graph_type = "entity"
  graph.graph_tier = "ladybug-standard"
  graph.is_active = True
  return graph


@pytest.fixture
def mock_user_graph():
  """Create a mock user graph record."""
  ug = MagicMock()
  ug.id = "ug_123"
  ug.user_id = "user_123"
  ug.graph_id = "kg123456789abcdef"
  ug.access_level = "admin"
  ug.is_active = True
  return ug


@pytest.fixture
def mock_user_credits():
  """Create a mock user credits record."""
  credits = MagicMock()
  credits.id = "credits_123"
  credits.user_id = "user_123"
  credits.graph_id = "kg123456789abcdef"
  credits.current_balance = 100000
  credits.monthly_allocation = 100000
  return credits
