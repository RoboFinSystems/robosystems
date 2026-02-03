"""
Pytest configuration for config module tests.

Config module tests are pure unit tests that don't need database fixtures.
This conftest prevents the session-scoped test_db fixture from being created
when only running config tests.
"""

import pytest


@pytest.fixture(scope="session")
def test_db():
  """
  Override the session-scoped test_db fixture from the parent conftest.

  Config tests don't need database access, so we provide a no-op fixture
  that returns None. This prevents PostgreSQL connection errors when
  running config tests without Docker services.
  """
  return None


@pytest.fixture(autouse=True)
def setup_database(test_db):
  """
  Override the autouse setup_database fixture from the parent conftest.

  Config tests don't use the database, so this is a no-op that just yields.
  """
  yield
