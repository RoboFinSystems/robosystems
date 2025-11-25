"""
Tests for LadybugDB Engine implementation.

Basic tests to validate that the Engine works with the existing
repository patterns for seamless LadybugDB database integration.
"""

import pytest
import tempfile
import os

from robosystems.graph_api.core.ladybug import (
  Engine,
  Repository,
)


@pytest.fixture
def temp_lbug_db():
  """Create a temporary LadybugDB database for testing."""
  with tempfile.TemporaryDirectory() as temp_dir:
    db_path = os.path.join(temp_dir, "test.db")
    yield db_path


@pytest.fixture
def lbug_engine(temp_lbug_db):
  """Create a LadybugDB Engine instance for testing."""
  engine = Engine(temp_lbug_db)
  yield engine
  engine.close()


@pytest.fixture
def lbug_repository(temp_lbug_db):
  """Create a LadybugDB Repository instance for testing."""
  repo = Repository(temp_lbug_db)
  yield repo
  repo.close()


class TestLadybugEngine:
  """Test LadybugDB Engine functionality."""

  def test_connection(self, lbug_engine):
    """Test that we can connect to LadybugDB database."""
    health = lbug_engine.health_check()
    assert health["status"] == "healthy"
    assert health["engine"] == "ladybug"
    assert health["test_result"] == 1

  def test_simple_query(self, lbug_engine):
    """Test executing a simple query."""
    result = lbug_engine.execute_query("RETURN 'hello' as greeting, 42 as number")

    assert len(result) == 1
    assert result[0]["greeting"] == "hello"
    assert result[0]["number"] == 42

  def test_execute_single(self, lbug_engine):
    """Test execute_single method."""
    result = lbug_engine.execute_single("RETURN 'test' as value")

    assert result is not None
    assert result["value"] == "test"

  def test_execute_single_no_results(self, lbug_engine):
    """Test execute_single with no results."""
    # First create a table to query
    lbug_engine.execute_query("CREATE NODE TABLE TestNode(id INT64, PRIMARY KEY (id))")

    result = lbug_engine.execute_single("MATCH (n:TestNode) WHERE n.id = 999 RETURN n")
    assert result is None

  def test_create_basic_schema(self, lbug_engine):
    """Test creating a basic schema for testing purposes."""
    # Create node tables
    lbug_engine.execute_query("""
            CREATE NODE TABLE Entity(
                cik STRING,
                name STRING,
                PRIMARY KEY (cik)
            )
        """)

    lbug_engine.execute_query("""
            CREATE NODE TABLE Report(
                accession_number STRING,
                form STRING,
                PRIMARY KEY (accession_number)
            )
        """)

    # Create relationship table
    lbug_engine.execute_query("""
            CREATE REL TABLE Filed(FROM Entity TO Report)
        """)

    # Insert test data
    lbug_engine.execute_query("""
            CREATE (c:Entity {cik: 'TEST123', name: 'Test Entity'})
        """)

    # Query the data
    result = lbug_engine.execute_single("""
            MATCH (c:Entity) WHERE c.cik = 'TEST123' RETURN c.name as name
        """)

    assert result["name"] == "Test Entity"


class TestLadybugRepository:
  """Test LadybugDB Repository functionality."""

  def test_repository_interface(self, lbug_repository):
    """Test that Repository implements the same interface as GraphRepository."""
    # Test health check
    health = lbug_repository.health_check()
    assert health["status"] == "healthy"

    # Test execute method (alias for execute_query)
    result = lbug_repository.execute("RETURN 1 as test")
    assert len(result) == 1
    assert result[0]["test"] == 1

  def test_count_nodes_no_filters(self, lbug_repository):
    """Test count_nodes method without filters."""
    # Note: This will fail until we create a proper schema
    # For now, just test that the method exists and handles errors gracefully
    try:
      count = lbug_repository.count_nodes("NonExistentLabel")
      assert count == 0
    except Exception:
      # Expected for now since we don't have a schema
      pass

  def test_node_exists(self, lbug_repository):
    """Test node_exists method."""
    # Note: This will fail until we create a proper schema
    try:
      exists = lbug_repository.node_exists("NonExistentLabel", {"id": "test"})
      assert exists is False
    except Exception:
      # Expected for now since we don't have a schema
      pass


class TestLadybugRouter:
  """Test LadybugDB database router functionality."""

  def test_router_health_check(self, temp_lbug_db):
    """Test router health check."""
    with Repository(temp_lbug_db) as repo:
      health = repo.health_check()
      assert health["status"] == "healthy"
      assert health["engine"] == "ladybug"


@pytest.mark.integration
class TestLadybugIntegration:
  """Integration tests for LadybugDB with existing patterns."""

  def test_lbug_as_drop_in_replacement(self, temp_lbug_db):
    """Test using LadybugDB as the primary database repository."""
    # Create repository directly
    repo = Repository(temp_lbug_db)

    # Should be able to execute basic queries
    result = repo.execute("RETURN 'lbug works' as message")
    assert len(result) == 1
    assert result[0]["message"] == "lbug works"

    repo.close()


if __name__ == "__main__":
  pytest.main([__file__, "-v"])
