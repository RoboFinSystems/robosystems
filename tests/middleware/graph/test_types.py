"""Tests for GraphTypeRegistry and graph type identification."""

import pytest

from robosystems.config.graph_tier import GraphTier
from robosystems.middleware.graph.types import (
  AccessPattern,
  GraphCategory,
  GraphTypeRegistry,
)
from robosystems.models.iam import Graph, Org
from robosystems.models.iam.org import OrgType


class TestGraphTypeRegistry:
  """Test suite for GraphTypeRegistry."""

  @pytest.fixture(autouse=True)
  def setup(self):
    """Set up unique identifiers for this test class."""
    import uuid

    self.unique_id = str(uuid.uuid4())[:8]

  @pytest.fixture
  def test_org(self, db_session):
    """Create a test organization."""
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    org = Org(
      id=f"test_org_{unique_id}",
      name=f"Test Org {unique_id}",
      org_type=OrgType.PERSONAL,
    )
    db_session.add(org)
    db_session.commit()
    return org

  def test_identify_graph_with_database_lookup_repository(self, db_session):
    """Test identify_graph uses database lookup for repository graphs."""
    repo = Graph.find_or_create_repository(
      graph_id=f"sec_{self.unique_id}",
      graph_name="SEC Public Filings",
      repository_type="sec",
      session=db_session,
      graph_tier=GraphTier.LADYBUG_SHARED,
    )

    identity = GraphTypeRegistry.identify_graph(repo.graph_id, session=db_session)

    assert identity.graph_id == repo.graph_id
    assert identity.category == GraphCategory.SHARED
    assert identity.graph_type == "sec"
    assert identity.graph_tier == GraphTier.LADYBUG_SHARED
    assert identity.access_pattern == AccessPattern.READ_ONLY

  def test_identify_graph_with_database_lookup_user_graph(self, db_session, test_org):
    """Test identify_graph uses database lookup for user graphs."""
    user_graph = Graph.create(
      graph_id=f"kg_{self.unique_id}",
      graph_name="Test User Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      schema_extensions=["roboledger"],
      graph_tier=GraphTier.LADYBUG_STANDARD,
    )

    identity = GraphTypeRegistry.identify_graph(user_graph.graph_id, session=db_session)

    assert identity.graph_id == user_graph.graph_id
    assert identity.category == GraphCategory.USER
    assert identity.graph_type == "entity"
    assert identity.graph_tier == GraphTier.LADYBUG_STANDARD

  def test_identify_graph_named_parameter_usage(self):
    """
    Regression test: ensure graph_tier uses named parameter.

    This is the critical test that would have caught the bug where
    graph_tier was passed as second positional argument (where session
    is expected), causing AttributeError: 'GraphTier' object has no attribute 'query'.
    """
    identity = GraphTypeRegistry.identify_graph(
      "kg123abc", graph_tier=GraphTier.LADYBUG_STANDARD
    )

    assert identity.graph_tier == GraphTier.LADYBUG_STANDARD
    assert identity.graph_id == "kg123abc"

  def test_identify_graph_fallback_to_pattern_matching(self):
    """Test identify_graph falls back to pattern matching when not in database."""
    identity = GraphTypeRegistry.identify_graph("sec")

    assert identity.graph_id == "sec"
    assert identity.category == GraphCategory.SHARED
    assert identity.graph_type == "sec"

  def test_identify_graph_uses_database_tier(self, db_session):
    """Test that database tier is used when graph is in database."""
    repo = Graph.find_or_create_repository(
      graph_id=f"test_{self.unique_id}",
      graph_name="Test",
      repository_type="test",
      session=db_session,
      graph_tier=GraphTier.LADYBUG_SHARED,
    )

    identity = GraphTypeRegistry.identify_graph(
      repo.graph_id,
      session=db_session,
    )

    assert identity.graph_tier == GraphTier.LADYBUG_SHARED

  def test_identify_graph_without_session_uses_patterns(self):
    """Test that without session, identify_graph uses pattern matching."""
    identity = GraphTypeRegistry.identify_graph("kg_user123")

    assert identity.graph_id == "kg_user123"
    assert identity.category == GraphCategory.USER

  def test_identify_graph_session_none_no_error(self):
    """Test that passing session=None doesn't cause errors."""
    identity = GraphTypeRegistry.identify_graph("kg_test", session=None)

    assert identity.graph_id == "kg_test"
    assert identity.category == GraphCategory.USER
