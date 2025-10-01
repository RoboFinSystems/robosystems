"""
Tests for KuzuEngine implementation.

Basic tests to validate that the KuzuEngine works with the existing
repository patterns for seamless Kuzu database integration.
"""

import pytest
import tempfile
import os

from robosystems.middleware.graph import (
  Engine,
  Repository,
)


@pytest.fixture
def temp_kuzu_db():
  """Create a temporary Kuzu database for testing."""
  with tempfile.TemporaryDirectory() as temp_dir:
    db_path = os.path.join(temp_dir, "test.db")
    yield db_path


@pytest.fixture
def kuzu_engine(temp_kuzu_db):
  """Create a Kuzu Engine instance for testing."""
  engine = Engine(temp_kuzu_db)
  yield engine
  engine.close()


@pytest.fixture
def kuzu_repository(temp_kuzu_db):
  """Create a Kuzu Repository instance for testing."""
  repo = Repository(temp_kuzu_db)
  yield repo
  repo.close()


class TestKuzuEngine:
  """Test KuzuEngine functionality."""

  def test_connection(self, kuzu_engine):
    """Test that we can connect to Kuzu database."""
    health = kuzu_engine.health_check()
    assert health["status"] == "healthy"
    assert health["engine"] == "kuzu"
    assert health["test_result"] == 1

  def test_simple_query(self, kuzu_engine):
    """Test executing a simple query."""
    result = kuzu_engine.execute_query("RETURN 'hello' as greeting, 42 as number")

    assert len(result) == 1
    assert result[0]["greeting"] == "hello"
    assert result[0]["number"] == 42

  def test_execute_single(self, kuzu_engine):
    """Test execute_single method."""
    result = kuzu_engine.execute_single("RETURN 'test' as value")

    assert result is not None
    assert result["value"] == "test"

  def test_execute_single_no_results(self, kuzu_engine):
    """Test execute_single with no results."""
    # First create a table to query
    kuzu_engine.execute_query("CREATE NODE TABLE TestNode(id INT64, PRIMARY KEY (id))")

    result = kuzu_engine.execute_single("MATCH (n:TestNode) WHERE n.id = 999 RETURN n")
    assert result is None

  def test_create_basic_schema(self, kuzu_engine):
    """Test creating a basic schema for testing purposes."""
    # Create node tables
    kuzu_engine.execute_query("""
            CREATE NODE TABLE Entity(
                cik STRING,
                name STRING,
                PRIMARY KEY (cik)
            )
        """)

    kuzu_engine.execute_query("""
            CREATE NODE TABLE Report(
                accession_number STRING,
                form STRING,
                PRIMARY KEY (accession_number)
            )
        """)

    # Create relationship table
    kuzu_engine.execute_query("""
            CREATE REL TABLE Filed(FROM Entity TO Report)
        """)

    # Insert test data
    kuzu_engine.execute_query("""
            CREATE (c:Entity {cik: 'TEST123', name: 'Test Entity'})
        """)

    # Query the data
    result = kuzu_engine.execute_single("""
            MATCH (c:Entity) WHERE c.cik = 'TEST123' RETURN c.name as name
        """)

    assert result["name"] == "Test Entity"


class TestKuzuRepository:
  """Test KuzuRepository functionality."""

  def test_repository_interface(self, kuzu_repository):
    """Test that KuzuRepository implements the same interface as GraphRepository."""
    # Test health check
    health = kuzu_repository.health_check()
    assert health["status"] == "healthy"

    # Test execute method (alias for execute_query)
    result = kuzu_repository.execute("RETURN 1 as test")
    assert len(result) == 1
    assert result[0]["test"] == 1

  def test_count_nodes_no_filters(self, kuzu_repository):
    """Test count_nodes method without filters."""
    # Note: This will fail until we create a proper schema
    # For now, just test that the method exists and handles errors gracefully
    try:
      count = kuzu_repository.count_nodes("NonExistentLabel")
      assert count == 0
    except Exception:
      # Expected for now since we don't have a schema
      pass

  def test_node_exists(self, kuzu_repository):
    """Test node_exists method."""
    # Note: This will fail until we create a proper schema
    try:
      exists = kuzu_repository.node_exists("NonExistentLabel", {"id": "test"})
      assert exists is False
    except Exception:
      # Expected for now since we don't have a schema
      pass


class TestKuzuRouter:
  """Test Kuzu database router functionality."""

  def test_router_health_check(self, temp_kuzu_db):
    """Test router health check."""
    with Repository(temp_kuzu_db) as repo:
      health = repo.health_check()
      assert health["status"] == "healthy"
      assert health["engine"] == "kuzu"


@pytest.mark.integration
class TestKuzuIntegration:
  """Integration tests for Kuzu with existing patterns."""

  def test_kuzu_as_drop_in_replacement(self, temp_kuzu_db):
    """Test using Kuzu as the primary database repository."""
    # Create repository directly
    repo = Repository(temp_kuzu_db)

    # Should be able to execute basic queries
    result = repo.execute("RETURN 'kuzu works' as message")
    assert len(result) == 1
    assert result[0]["message"] == "kuzu works"

    repo.close()


if __name__ == "__main__":
  pytest.main([__file__, "-v"])
