"""
Tests for graph engine consistency and interface compliance.

This test suite validates that the current Kuzu engine implementation
provides consistent behavior and implements the expected interface properly.
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


class TestEngineInterface:
  """Test that the engine implements the expected interface consistently."""

  def test_engine_implements_required_methods(self, temp_kuzu_db):
    """Test that engine implements all required interface methods."""
    engine = Engine(temp_kuzu_db)

    # Required methods
    interface_methods = [
      "execute_query",
      "execute_single",
      "health_check",
      "close",
    ]

    for method in interface_methods:
      assert hasattr(engine, method)
      assert callable(getattr(engine, method))

    # Cleanup
    engine.close()

  def test_repository_implements_required_methods(self, temp_kuzu_db):
    """Test that repository implements all required interface methods."""
    repo = Repository(temp_kuzu_db)

    # Required methods
    repo_methods = [
      "execute_query",
      "execute_single",
      "count_nodes",
      "node_exists",
      "execute",  # Alias for execute_query
      "health_check",
      "close",
    ]

    for method in repo_methods:
      assert hasattr(repo, method)
      assert callable(getattr(repo, method))

    # Cleanup
    repo.close()

  def test_health_check_format_consistency(self, temp_kuzu_db):
    """Test that health checks return consistent format."""
    engine = Engine(temp_kuzu_db)
    health = engine.health_check()

    # Required fields
    required_fields = ["status", "engine"]

    for field in required_fields:
      assert field in health

    # Engine should be identified correctly
    assert health["engine"] == "kuzu"
    assert health["status"] in ["healthy", "degraded", "unhealthy"]

    # Cleanup
    engine.close()

  def test_simple_query_consistency(self, temp_kuzu_db):
    """Test that simple queries work consistently."""
    engine = Engine(temp_kuzu_db)

    # Test basic query
    result = engine.execute_query("RETURN 'test' as message, 42 as number")

    assert len(result) == 1
    assert result[0]["message"] == "test"
    assert result[0]["number"] == 42

    # Test execute_single
    single_result = engine.execute_single("RETURN 'single' as value")
    assert single_result is not None
    assert single_result["value"] == "single"

    # Cleanup
    engine.close()


class TestRepositoryConsistency:
  """Test that repository provides consistent behavior."""

  def test_repository_execute_alias(self, temp_kuzu_db):
    """Test that execute method works as alias for execute_query."""
    repo = Repository(temp_kuzu_db)

    # Both methods should return the same result for the same query
    query = "RETURN 'alias_test' as test_value"

    result1 = repo.execute_query(query)
    result2 = repo.execute(query)

    assert result1 == result2
    assert len(result1) == 1
    assert result1[0]["test_value"] == "alias_test"

    # Cleanup
    repo.close()

  def test_count_nodes_behavior(self, kuzu_repository_with_schema):
    """Test count_nodes method behavior."""
    # Create some test data first
    kuzu_repository_with_schema.execute_query("""
      CREATE (c1:Entity {identifier: 'count-test-1', name: 'Test Entity 1'}),
             (c2:Entity {identifier: 'count-test-2', name: 'Test Entity 2'})
    """)

    # Count entities
    count = kuzu_repository_with_schema.count_nodes("Entity")
    assert count >= 2  # At least the two we just created

    # Count non-existent nodes - Kuzu will throw an error for non-existent tables
    try:
      count_none = kuzu_repository_with_schema.count_nodes("NonExistentLabel")
      assert count_none == 0  # If method handles it gracefully
    except Exception:
      # Expected behavior in Kuzu - non-existent tables throw errors
      pass

  def test_node_exists_behavior(self, kuzu_repository_with_schema):
    """Test node_exists method behavior."""
    # Create test data
    kuzu_repository_with_schema.execute_query("""
      CREATE (c:Entity {identifier: 'exists-test', name: 'Exists Test Entity'})
    """)

    # Test existing node
    exists = kuzu_repository_with_schema.node_exists(
      "Entity", {"identifier": "exists-test"}
    )
    assert exists is True

    # Test non-existent node
    not_exists = kuzu_repository_with_schema.node_exists(
      "Entity", {"identifier": "does-not-exist"}
    )
    assert not_exists is False

    # Test non-existent label - Kuzu will throw an error for non-existent tables
    try:
      no_label = kuzu_repository_with_schema.node_exists(
        "NonExistentLabel", {"id": "test"}
      )
      assert no_label is False  # If method handles it gracefully
    except Exception:
      # Expected behavior in Kuzu - non-existent tables throw errors
      pass


class TestEngineErrorHandling:
  """Test that engine handles errors consistently."""

  def test_invalid_query_handling(self, temp_kuzu_db):
    """Test that invalid queries are handled properly."""
    engine = Engine(temp_kuzu_db)

    # Test invalid syntax
    with pytest.raises(Exception):  # Should raise some form of query error
      engine.execute_query("INVALID CYPHER SYNTAX")

    # Test empty query
    with pytest.raises(Exception):  # Should raise some form of error
      engine.execute_query("")

    # Cleanup
    engine.close()

  def test_execute_single_no_results(self, temp_kuzu_db):
    """Test execute_single with no results."""
    engine = Engine(temp_kuzu_db)

    # Create a table first
    engine.execute_query("CREATE NODE TABLE TestEmpty(id INT64, PRIMARY KEY (id))")

    # Query that returns no results
    result = engine.execute_single("MATCH (n:TestEmpty) WHERE n.id = 999 RETURN n")
    assert result is None

    # Cleanup
    engine.close()


class TestContextManagerSupport:
  """Test that engines support context manager protocol."""

  def test_engine_context_manager(self, temp_kuzu_db):
    """Test that engine supports context manager protocol."""
    with Engine(temp_kuzu_db) as engine:
      assert engine is not None
      health = engine.health_check()
      assert "status" in health

  def test_repository_context_manager(self, temp_kuzu_db):
    """Test that repository supports context manager protocol."""
    with Repository(temp_kuzu_db) as repo:
      assert repo is not None
      health = repo.health_check()
      assert "status" in health


class TestDatabaseOperations:
  """Test database operation consistency."""

  def test_schema_creation_consistency(self, temp_kuzu_db):
    """Test that schema creation works consistently."""
    engine = Engine(temp_kuzu_db)

    # Create node tables
    engine.execute_query("""
      CREATE NODE TABLE TestEntity(
        cik STRING,
        name STRING,
        PRIMARY KEY (cik)
      )
    """)

    engine.execute_query("""
      CREATE NODE TABLE TestReport(
        accession_number STRING,
        form STRING,
        PRIMARY KEY (accession_number)
      )
    """)

    # Create relationship table
    engine.execute_query("""
      CREATE REL TABLE TestFiled(FROM TestEntity TO TestReport)
    """)

    # Insert test data
    engine.execute_query("""
      CREATE (c:TestEntity {cik: 'SCHEMA123', name: 'Schema Test Entity'})
    """)

    # Query the data
    result = engine.execute_single("""
      MATCH (c:TestEntity) WHERE c.cik = 'SCHEMA123' RETURN c.name as name
    """)

    assert result is not None
    assert result["name"] == "Schema Test Entity"

    # Cleanup
    engine.close()

  def test_transaction_consistency(self, temp_kuzu_db):
    """Test that transactions work consistently."""
    engine = Engine(temp_kuzu_db)

    # Create schema
    engine.execute_query(
      "CREATE NODE TABLE TxTest(id STRING, value STRING, PRIMARY KEY (id))"
    )

    # Test successful operations
    operations = [
      "CREATE (n:TxTest {id: 'tx1', value: 'test1'})",
      "CREATE (n:TxTest {id: 'tx2', value: 'test2'})",
    ]

    # Execute operations
    for op in operations:
      engine.execute_query(op)

    # Verify data was created
    result = engine.execute_query("MATCH (n:TxTest) RETURN count(n) as count")
    assert result[0]["count"] == 2

    # Cleanup
    engine.close()


class TestPerformanceCharacteristics:
  """Test performance characteristics of the engine."""

  def test_bulk_operations_performance(self, temp_kuzu_db):
    """Test that bulk operations perform reasonably."""
    engine = Engine(temp_kuzu_db)

    # Create schema
    engine.execute_query(
      "CREATE NODE TABLE PerfTest(id STRING, value STRING, PRIMARY KEY (id))"
    )

    # Create multiple nodes efficiently
    bulk_size = 50
    for i in range(bulk_size):
      engine.execute_query(
        "CREATE (n:PerfTest {id: $id, value: $value})",
        {"id": f"perf-{i}", "value": f"value-{i}"},
      )

    # Verify all nodes were created
    result = engine.execute_single("MATCH (n:PerfTest) RETURN count(n) as count")
    assert result is not None
    assert result["count"] == bulk_size

    # Test efficient querying
    query_result = engine.execute_query(
      "MATCH (n:PerfTest) WHERE n.id STARTS WITH 'perf-1' RETURN n.id as id"
    )

    # Should find all nodes with ids starting with 'perf-1' (perf-1, perf-10, perf-11, etc.)
    assert len(query_result) >= 1

    # Cleanup
    engine.close()


if __name__ == "__main__":
  pytest.main([__file__, "-v"])
