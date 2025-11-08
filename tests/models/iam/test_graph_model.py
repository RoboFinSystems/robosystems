"""Comprehensive tests for the Graph model."""

import pytest
from unittest.mock import MagicMock
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.models.iam import Graph, User
from robosystems.config.graph_tier import GraphTier


@pytest.fixture
def test_user(db_session: Session):
  """Create a test user with org."""
  from robosystems.models.iam import Org, OrgUser, OrgRole, OrgType
  import uuid

  unique_id = str(uuid.uuid4())[:8]

  org = Org(
    id=f"test_org_{unique_id}",
    name=f"Test Org {unique_id}",
    org_type=OrgType.PERSONAL,
  )
  db_session.add(org)
  db_session.flush()

  user = User(
    id=f"test_user_{unique_id}",
    email=f"test+{unique_id}@example.com",
    name="Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.flush()

  org_user = OrgUser(
    org_id=org.id,
    user_id=user.id,
    role=OrgRole.OWNER,
  )
  db_session.add(org_user)
  db_session.commit()
  return user


@pytest.fixture
def test_org(test_user, db_session: Session):
  """Get org for test user."""
  from robosystems.models.iam import OrgUser

  org_users = OrgUser.get_user_orgs(test_user.id, db_session)
  return org_users[0].org


class TestGraphModel:
  """Test suite for the Graph model."""

  @pytest.fixture(autouse=True)
  def setup(self):
    """Set up unique identifiers for this test class."""
    import uuid

    self.unique_id = str(uuid.uuid4())[:8]

  def test_graph_initialization(self):
    """Test Graph model can be instantiated with required fields."""
    graph = Graph(
      graph_id="kg1a2b3c4d5",
      graph_name="Test Graph",
      graph_type="entity",
    )

    assert graph.graph_id == "kg1a2b3c4d5"
    assert graph.graph_name == "Test Graph"
    assert graph.graph_type == "entity"
    assert graph.base_schema is None
    # Default values are not set until the object is added to session
    assert graph.schema_extensions is None or graph.schema_extensions == []
    assert graph.graph_instance_id is None or graph.graph_instance_id == "default"
    assert graph.graph_tier is None or graph.graph_tier == GraphTier.KUZU_STANDARD.value
    assert graph.is_subgraph is None or graph.is_subgraph is False
    assert graph.parent_graph_id is None

  def test_graph_repr_main_graph(self):
    """Test Graph string representation for main graphs."""
    graph = Graph(
      graph_id="kg1a2b3c4d5",
      graph_name="Test Graph",
      graph_type="entity",
      schema_extensions=["roboledger", "roboinvestor"],
    )

    assert (
      repr(graph)
      == "<Graph kg1a2b3c4d5 type=entity extensions=['roboledger', 'roboinvestor']>"
    )

  def test_graph_repr_subgraph(self):
    """Test Graph string representation for subgraphs."""
    graph = Graph(
      graph_id="kg1a2b3c4d5_sub1",
      graph_name="Subgraph 1",
      graph_type="entity",
      is_subgraph=True,
      parent_graph_id="kg1a2b3c4d5",
    )

    assert (
      repr(graph) == "<Graph kg1a2b3c4d5_sub1 (subgraph of kg1a2b3c4d5) type=entity>"
    )

  def test_has_extension_property(self):
    """Test has_extension property."""
    # Graph with extensions
    graph_with = Graph(
      graph_id="kg1",
      graph_name="With Extensions",
      graph_type="entity",
      schema_extensions=["roboledger"],
    )
    assert graph_with.has_extension is True

    # Graph without extensions
    graph_without = Graph(
      graph_id="kg2",
      graph_name="Without Extensions",
      graph_type="generic",
      schema_extensions=[],
    )
    assert graph_without.has_extension is False

    # Graph with None extensions
    graph_none = Graph(
      graph_id="kg3",
      graph_name="None Extensions",
      graph_type="generic",
    )
    graph_none.schema_extensions = None
    assert graph_none.has_extension is False

  def test_database_name_property_main_graph(self):
    """Test database_name property for main graphs."""
    graph = Graph(
      graph_id="kg1a2b3c4d5",
      graph_name="Main Graph",
      graph_type="entity",
    )

    assert graph.database_name == "kg1a2b3c4d5"

  def test_database_name_property_subgraph(self):
    """Test database_name property for subgraphs."""
    graph = Graph(
      graph_id="kg_sub",
      graph_name="Subgraph",
      graph_type="entity",
      is_subgraph=True,
      parent_graph_id="kg1a2b3c4d5",
      subgraph_name="analysis2024",
    )

    assert graph.database_name == "kg1a2b3c4d5_analysis2024"

  def test_can_have_subgraphs_property(self):
    """Test can_have_subgraphs property for different tiers."""
    # Standard tier cannot have subgraphs
    standard_graph = Graph(
      graph_id="kg1",
      graph_name="Standard",
      graph_type="entity",
      graph_tier=GraphTier.KUZU_STANDARD.value,
    )
    assert standard_graph.can_have_subgraphs is False

    # Enterprise tier can have subgraphs
    enterprise_graph = Graph(
      graph_id="kg2",
      graph_name="Enterprise",
      graph_type="entity",
      graph_tier=GraphTier.KUZU_LARGE.value,
    )
    assert enterprise_graph.can_have_subgraphs is True

    # Premium tier can have subgraphs
    premium_graph = Graph(
      graph_id="kg3",
      graph_name="Premium",
      graph_type="entity",
      graph_tier=GraphTier.KUZU_XLARGE.value,
    )
    assert premium_graph.can_have_subgraphs is True

  def test_has_specific_extension(self):
    """Test has_specific_extension method."""
    graph = Graph(
      graph_id="kg1",
      graph_name="Test Graph",
      graph_type="entity",
      schema_extensions=["roboledger", "roboinvestor"],
    )

    assert graph.has_specific_extension("roboledger") is True
    assert graph.has_specific_extension("roboinvestor") is True
    assert graph.has_specific_extension("nonexistent") is False

    # Test with empty extensions
    graph.schema_extensions = []
    assert graph.has_specific_extension("roboledger") is False

    # Test with None extensions
    graph.schema_extensions = None
    assert graph.has_specific_extension("roboledger") is False

  def test_create_entity_graph(self, test_org, db_session):
    """Test creating an entity graph."""
    graph = Graph.create(
      graph_id=f"kg_entity_test_{self.unique_id}",
      graph_name="Entity Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      base_schema="base",
      schema_extensions=["roboledger", "roboinvestor"],
      graph_instance_id="cluster1",
      graph_cluster_region="us-east-1",
      graph_tier=GraphTier.KUZU_LARGE,
      graph_metadata={"test": "metadata"},
    )

    assert graph.graph_id == f"kg_entity_test_{self.unique_id}"
    assert graph.graph_name == "Entity Graph"
    assert graph.graph_type == "entity"
    assert graph.base_schema == "base"
    assert graph.schema_extensions == ["roboledger", "roboinvestor"]
    assert graph.graph_instance_id == "cluster1"
    assert graph.graph_cluster_region == "us-east-1"
    assert graph.graph_tier == GraphTier.KUZU_LARGE.value
    assert graph.graph_metadata == {"test": "metadata"}
    assert graph.is_subgraph is False
    assert graph.created_at is not None
    assert graph.updated_at is not None

    # Verify in database
    db_graph = (
      db_session.query(Graph)
      .filter_by(graph_id=f"kg_entity_test_{self.unique_id}")
      .first()
    )
    assert db_graph is not None
    assert db_graph.graph_name == "Entity Graph"

  def test_create_generic_graph(self, test_org, db_session):
    """Test creating a generic graph."""
    graph = Graph.create(
      graph_id="generic_123",
      graph_name="Generic Graph",
      graph_type="generic",
      org_id=test_org.id,
      session=db_session,
    )

    assert graph.graph_id == "generic_123"
    assert graph.graph_name == "Generic Graph"
    assert graph.graph_type == "generic"
    assert graph.base_schema is None
    assert graph.schema_extensions == []
    assert graph.graph_tier == GraphTier.KUZU_STANDARD.value

  def test_create_entity_graph_auto_base_schema(self, test_org, db_session):
    """Test that entity graphs automatically get base_schema if not provided."""
    graph = Graph.create(
      graph_id="kg_auto_base",
      graph_name="Auto Base",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    assert graph.base_schema == "base"

  def test_create_subgraph(self, test_org, db_session):
    """Test creating a subgraph."""
    # First create parent graph
    Graph.create(
      graph_id="kg_parent",
      graph_name="Parent Graph",
      graph_type="entity",
      graph_tier=GraphTier.KUZU_LARGE,
      org_id=test_org.id,
      session=db_session,
    )

    # Create subgraph
    subgraph = Graph.create(
      graph_id="kg_sub1",
      graph_name="Subgraph 1",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      parent_graph_id="kg_parent",
      subgraph_index=1,
      subgraph_name="analysis2024",
      is_subgraph=True,
      subgraph_metadata={"purpose": "year-end analysis"},
    )

    assert subgraph.graph_id == "kg_sub1"
    assert subgraph.parent_graph_id == "kg_parent"
    assert subgraph.subgraph_index == 1
    assert subgraph.subgraph_name == "analysis2024"
    assert subgraph.is_subgraph is True
    assert subgraph.subgraph_metadata == {"purpose": "year-end analysis"}

  def test_create_subgraph_missing_params(self, test_org, db_session):
    """Test that creating subgraph without required params fails."""
    with pytest.raises(ValueError, match="Subgraphs require"):
      Graph.create(
        graph_id="kg_invalid_sub",
        graph_name="Invalid Subgraph",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
        is_subgraph=True,
        # Missing parent_graph_id, subgraph_index, subgraph_name
      )

  def test_create_subgraph_invalid_name(self, test_org, db_session):
    """Test that subgraph name validation works."""
    # Invalid characters
    with pytest.raises(ValueError, match="alphanumeric"):
      Graph.create(
        graph_id="kg_sub_bad",
        graph_name="Bad Subgraph",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
        parent_graph_id="kg_parent",
        subgraph_index=1,
        subgraph_name="analysis-2024",  # Has hyphen
        is_subgraph=True,
      )

    # Too long
    with pytest.raises(ValueError, match="alphanumeric"):
      Graph.create(
        graph_id="kg_sub_long",
        graph_name="Long Subgraph",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
        parent_graph_id="kg_parent",
        subgraph_index=1,
        subgraph_name="a" * 21,  # 21 characters
        is_subgraph=True,
      )

  def test_create_invalid_graph_type(self, test_org, db_session):
    """Test that invalid graph_type raises error."""
    with pytest.raises(ValueError, match="graph_type must be"):
      Graph.create(
        graph_id="kg_invalid_type",
        graph_name="Invalid Type",
        graph_type="invalid",
        org_id=test_org.id,
        session=db_session,
      )

  def test_create_graph_rollback_on_error(self):
    """Test that create rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
      Graph.create(
        graph_id="kg_error",
        graph_name="Error Graph",
        graph_type="entity",
        org_id="test-org-id",
        session=mock_session,
      )

    mock_session.rollback.assert_called_once()

  def test_get_by_id(self, test_org, db_session):
    """Test getting graph by ID."""
    # Create a graph
    Graph.create(
      graph_id="kg_find_me",
      graph_name="Find Me",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    # Find it
    found = Graph.get_by_id("kg_find_me", db_session)
    assert found is not None
    assert found.graph_id == "kg_find_me"
    assert found.graph_name == "Find Me"

    # Not found
    not_found = Graph.get_by_id("nonexistent", db_session)
    assert not_found is None

  def test_get_by_extension(self, test_org, db_session):
    """Test getting graphs by schema extension."""
    # Clean up any existing graphs to ensure test isolation
    # Delete in correct order due to foreign key constraints
    from robosystems.models.iam import GraphUser, GraphCredits
    from robosystems.models.iam.graph_credits import GraphCreditTransaction

    # Delete in dependency order
    db_session.query(GraphCreditTransaction).delete()
    db_session.query(GraphCredits).delete()
    db_session.query(GraphUser).delete()
    db_session.query(Graph).delete()
    db_session.commit()

    # Create graphs with different extensions using unique IDs
    Graph.create(
      graph_id=f"kg_robo1_{self.unique_id}",
      graph_name="RoboLedger 1",
      graph_type="entity",
      schema_extensions=["roboledger"],
      org_id=test_org.id,
      session=db_session,
    )

    Graph.create(
      graph_id=f"kg_robo2_{self.unique_id}",
      graph_name="RoboLedger 2",
      graph_type="entity",
      schema_extensions=["roboledger", "roboinvestor"],
      org_id=test_org.id,
      session=db_session,
    )

    Graph.create(
      graph_id=f"kg_investor_{self.unique_id}",
      graph_name="RoboInvestor Only",
      graph_type="entity",
      schema_extensions=["roboinvestor"],
      org_id=test_org.id,
      session=db_session,
    )

    Graph.create(
      graph_id=f"kg_none_{self.unique_id}",
      graph_name="No Extensions",
      graph_type="generic",
      schema_extensions=[],
      org_id=test_org.id,
      session=db_session,
    )

    # Find by extension
    roboledger_graphs = Graph.get_by_extension("roboledger", db_session)
    assert len(roboledger_graphs) == 2
    ids = [g.graph_id for g in roboledger_graphs]
    assert f"kg_robo1_{self.unique_id}" in ids
    assert f"kg_robo2_{self.unique_id}" in ids

    roboinvestor_graphs = Graph.get_by_extension("roboinvestor", db_session)
    assert len(roboinvestor_graphs) == 2
    ids = [g.graph_id for g in roboinvestor_graphs]
    assert f"kg_robo2_{self.unique_id}" in ids
    assert f"kg_investor_{self.unique_id}" in ids

    # Extension that doesn't exist
    none_graphs = Graph.get_by_extension("nonexistent", db_session)
    assert len(none_graphs) == 0

  def test_get_by_type(self, test_org, db_session):
    """Test getting graphs by type."""
    # Clean up any existing graphs to ensure test isolation
    # Delete in correct order due to foreign key constraints
    from robosystems.models.iam import GraphUser, GraphCredits
    from robosystems.models.iam.graph_credits import GraphCreditTransaction

    # Delete in dependency order
    db_session.query(GraphCreditTransaction).delete()
    db_session.query(GraphCredits).delete()
    db_session.query(GraphUser).delete()
    db_session.query(Graph).delete()
    db_session.commit()

    # Create graphs of different types with unique IDs
    Graph.create(
      graph_id=f"kg_entity1_{self.unique_id}",
      graph_name="Entity 1",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    Graph.create(
      graph_id=f"kg_entity2_{self.unique_id}",
      graph_name="Entity 2",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    Graph.create(
      graph_id=f"generic1_{self.unique_id}",
      graph_name="Generic 1",
      graph_type="generic",
      org_id=test_org.id,
      session=db_session,
    )

    # Find by type
    entity_graphs = Graph.get_by_type("entity", db_session)
    assert len(entity_graphs) == 2
    ids = [g.graph_id for g in entity_graphs]
    assert f"kg_entity1_{self.unique_id}" in ids
    assert f"kg_entity2_{self.unique_id}" in ids

    generic_graphs = Graph.get_by_type("generic", db_session)
    assert len(generic_graphs) == 1
    assert generic_graphs[0].graph_id == f"generic1_{self.unique_id}"

  def test_update_extensions(self, test_org, db_session):
    """Test updating graph extensions."""
    # Create a graph
    graph = Graph.create(
      graph_id="kg_update_ext",
      graph_name="Update Extensions",
      graph_type="entity",
      schema_extensions=["roboledger"],
      org_id=test_org.id,
      session=db_session,
    )

    original_updated_at = graph.updated_at

    # Update extensions
    graph.update_extensions(["roboledger", "roboinvestor", "custom"], db_session)

    assert graph.schema_extensions == ["roboledger", "roboinvestor", "custom"]
    assert graph.updated_at > original_updated_at

    # Verify in database
    db_graph = db_session.query(Graph).filter_by(graph_id="kg_update_ext").first()
    assert db_graph.schema_extensions == ["roboledger", "roboinvestor", "custom"]

  def test_update_extensions_rollback_on_error(self):
    """Test that update_extensions rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    graph = Graph(
      graph_id="kg_error",
      graph_name="Error Graph",
      graph_type="entity",
    )

    with pytest.raises(SQLAlchemyError):
      graph.update_extensions(["new"], mock_session)

    mock_session.rollback.assert_called_once()

  def test_delete_graph(self, test_org, db_session):
    """Test deleting a graph."""
    # Create a graph
    graph = Graph.create(
      graph_id="kg_delete_me",
      graph_name="Delete Me",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    # Delete it
    graph.delete(db_session)

    # Verify deletion
    db_graph = db_session.query(Graph).filter_by(graph_id="kg_delete_me").first()
    assert db_graph is None

  def test_delete_rollback_on_error(self):
    """Test that delete rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    graph = Graph(
      graph_id="kg_error",
      graph_name="Error Graph",
      graph_type="entity",
    )

    with pytest.raises(SQLAlchemyError):
      graph.delete(mock_session)

    mock_session.rollback.assert_called_once()

  def test_get_subgraphs(self, test_org, db_session):
    """Test getting all subgraphs for a parent graph."""
    # Create parent graph
    Graph.create(
      graph_id="kg_parent",
      graph_name="Parent",
      graph_type="entity",
      graph_tier=GraphTier.KUZU_LARGE,
      org_id=test_org.id,
      session=db_session,
    )

    # Create subgraphs
    Graph.create(
      graph_id="kg_sub1",
      graph_name="Sub 1",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      parent_graph_id="kg_parent",
      subgraph_index=1,
      subgraph_name="first",
      is_subgraph=True,
    )

    Graph.create(
      graph_id="kg_sub2",
      graph_name="Sub 2",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      parent_graph_id="kg_parent",
      subgraph_index=2,
      subgraph_name="second",
      is_subgraph=True,
    )

    # Create another parent's subgraph
    Graph.create(
      graph_id="kg_other_sub",
      graph_name="Other Sub",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      parent_graph_id="kg_other_parent",
      subgraph_index=1,
      subgraph_name="other",
      is_subgraph=True,
    )

    # Get subgraphs
    subgraphs = Graph.get_subgraphs("kg_parent", db_session)
    assert len(subgraphs) == 2
    assert subgraphs[0].graph_id == "kg_sub1"  # Ordered by index
    assert subgraphs[1].graph_id == "kg_sub2"

  def test_get_next_subgraph_index(self, test_org, db_session):
    """Test getting the next available subgraph index."""
    # Clean up any existing graphs to ensure test isolation
    # Delete in correct order due to foreign key constraints
    from robosystems.models.iam import GraphUser, GraphCredits
    from robosystems.models.iam.graph_credits import GraphCreditTransaction

    # Delete in dependency order
    db_session.query(GraphCreditTransaction).delete()
    db_session.query(GraphCredits).delete()
    db_session.query(GraphUser).delete()
    db_session.query(Graph).delete()
    db_session.commit()

    # No subgraphs yet
    next_index = Graph.get_next_subgraph_index(
      f"kg_new_parent_{self.unique_id}", db_session
    )
    assert next_index == 1

    # Create parent and subgraphs with unique IDs
    Graph.create(
      graph_id=f"kg_parent_{self.unique_id}",
      graph_name="Parent",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    Graph.create(
      graph_id=f"kg_sub1_{self.unique_id}",
      graph_name="Sub 1",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      parent_graph_id=f"kg_parent_{self.unique_id}",
      subgraph_index=1,
      subgraph_name="first",
      is_subgraph=True,
    )

    Graph.create(
      graph_id=f"kg_sub2_{self.unique_id}",
      graph_name="Sub 2",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      parent_graph_id=f"kg_parent_{self.unique_id}",
      subgraph_index=3,  # Skip 2
      subgraph_name="third",
      is_subgraph=True,
    )

    # Get next index
    next_index = Graph.get_next_subgraph_index(
      f"kg_parent_{self.unique_id}", db_session
    )
    assert next_index == 4  # After 3

  def test_validate_subgraph_name(self):
    """Test subgraph name validation."""
    # Valid names
    assert Graph.validate_subgraph_name("analysis2024") is True
    assert Graph.validate_subgraph_name("Q1") is True
    assert Graph.validate_subgraph_name("test123") is True
    assert Graph.validate_subgraph_name("a") is True
    assert Graph.validate_subgraph_name("a" * 20) is True

    # Invalid names
    assert Graph.validate_subgraph_name("") is False
    assert Graph.validate_subgraph_name("analysis-2024") is False  # Has hyphen
    assert Graph.validate_subgraph_name("analysis 2024") is False  # Has space
    assert Graph.validate_subgraph_name("analysis_2024") is False  # Has underscore
    assert Graph.validate_subgraph_name("a" * 21) is False  # Too long
    assert Graph.validate_subgraph_name("!@#$") is False  # Special chars

  def test_graph_relationships(self):
    """Test that Graph model has correct relationship definitions."""
    graph = Graph(
      graph_id="kg_test",
      graph_name="Test",
      graph_type="entity",
    )

    # Check relationship attributes exist
    assert hasattr(graph, "graph_users")

  def test_graph_tier_enum_handling(self, test_org, db_session):
    """Test that GraphTier enum is properly handled."""
    # Create with enum
    graph1 = Graph.create(
      graph_id="kg_enum",
      graph_name="Enum Test",
      graph_type="entity",
      graph_tier=GraphTier.KUZU_LARGE,
      org_id=test_org.id,
      session=db_session,
    )
    assert graph1.graph_tier == "kuzu-large"

    # Create with string
    graph2 = Graph.create(
      graph_id="kg_string",
      graph_name="String Test",
      graph_type="entity",
      graph_tier="kuzu-xlarge",
      org_id=test_org.id,
      session=db_session,
    )
    assert graph2.graph_tier == "kuzu-xlarge"

  def test_graph_constraints(self, test_org, db_session):
    """Test database constraints are enforced."""
    # Create a graph with subgraph
    Graph.create(
      graph_id="kg_parent",
      graph_name="Parent",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    Graph.create(
      graph_id="kg_sub1",
      graph_name="Sub 1",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
      parent_graph_id="kg_parent",
      subgraph_index=1,
      subgraph_name="first",
      is_subgraph=True,
    )

    # Try to create another subgraph with same index - should fail
    with pytest.raises(SQLAlchemyError):
      Graph.create(
        graph_id="kg_sub2",
        graph_name="Sub 2",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
        parent_graph_id="kg_parent",
        subgraph_index=1,  # Same index as sub1
        subgraph_name="second",
        is_subgraph=True,
      )


class TestGraphRepositoryFeatures:
  """Test suite for Graph repository features (shared repositories)."""

  @pytest.fixture(autouse=True)
  def setup(self):
    """Set up unique identifiers for this test class."""
    import uuid

    self.unique_id = str(uuid.uuid4())[:8]

  def test_find_or_create_repository_new(self, test_org, db_session):
    """Test creating a new repository when it doesn't exist."""
    result = Graph.find_or_create_repository(
      graph_id=f"sec_{self.unique_id}",
      graph_name="SEC Public Filings",
      repository_type="sec",
      session=db_session,
      data_source_type="sec_edgar",
      data_source_url="https://www.sec.gov/cgi-bin/browse-edgar",
      sync_frequency="daily",
      graph_tier=GraphTier.KUZU_SHARED,
    )

    assert result.graph_id == f"sec_{self.unique_id}"
    assert result.is_repository is True
    assert result.graph_type == "repository"
    assert result.repository_type == "sec"
    assert result.data_source_type == "sec_edgar"
    assert result.data_source_url == "https://www.sec.gov/cgi-bin/browse-edgar"
    assert result.sync_frequency == "daily"
    assert result.sync_status == "active"
    assert result.graph_tier == GraphTier.KUZU_SHARED.value

  def test_find_or_create_repository_existing(self, test_org, db_session):
    """Test that existing repository is returned without creating new one."""
    repo_id = f"industry_{self.unique_id}"

    # Create first time
    first = Graph.find_or_create_repository(
      graph_id=repo_id,
      graph_name="Industry Benchmarks",
      repository_type="industry",
      session=db_session,
    )

    # Call again - should return existing
    second = Graph.find_or_create_repository(
      graph_id=repo_id,
      graph_name="Industry Benchmarks",
      repository_type="industry",
      session=db_session,
    )

    assert first.graph_id == second.graph_id
    assert first.created_at == second.created_at

    # Verify only one exists in database
    all_repos = db_session.query(Graph).filter(Graph.graph_id == repo_id).all()
    assert len(all_repos) == 1

  def test_repository_needs_sync_no_last_sync(self, test_org, db_session):
    """Test needs_sync when last_sync_at is None."""
    repo = Graph.find_or_create_repository(
      graph_id=f"economic_{self.unique_id}",
      graph_name="Economic Indicators",
      repository_type="economic",
      session=db_session,
      sync_frequency="weekly",
    )

    # Clear last_sync_at
    repo.last_sync_at = None
    db_session.commit()

    assert repo.needs_sync is True

  def test_repository_needs_sync_recent(self, test_org, db_session):
    """Test needs_sync when sync is recent."""
    from datetime import datetime, timezone

    repo = Graph.find_or_create_repository(
      graph_id=f"sec_recent_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
      sync_frequency="daily",
    )

    # Set recent sync
    repo.last_sync_at = datetime.now(timezone.utc)
    repo.sync_status = "active"
    db_session.commit()

    assert repo.needs_sync is False

  def test_repository_needs_sync_stale(self, test_org, db_session):
    """Test needs_sync when sync is stale."""
    from datetime import datetime, timezone, timedelta

    repo = Graph.find_or_create_repository(
      graph_id=f"sec_stale_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
      sync_frequency="daily",
    )

    # Set old sync (2 days ago)
    repo.last_sync_at = datetime.now(timezone.utc) - timedelta(days=2)
    db_session.commit()

    assert repo.needs_sync is True

  def test_repository_needs_sync_error_status(self, test_org, db_session):
    """Test needs_sync when sync_status is error."""
    from datetime import datetime, timezone

    repo = Graph.find_or_create_repository(
      graph_id=f"sec_error_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
      sync_frequency="daily",
    )

    repo.sync_status = "error"
    repo.last_sync_at = datetime.now(timezone.utc)
    db_session.commit()

    assert repo.needs_sync is True

  def test_get_all_repositories(self, test_org, db_session):
    """Test getting all shared repositories."""
    # Clean up existing repositories
    from robosystems.models.iam import UserRepository

    db_session.query(UserRepository).delete()
    db_session.query(Graph).filter(Graph.is_repository.is_(True)).delete()
    db_session.commit()

    # Create repositories
    Graph.find_or_create_repository(
      graph_id=f"sec_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
    )

    Graph.find_or_create_repository(
      graph_id=f"industry_{self.unique_id}",
      graph_name="Industry",
      repository_type="industry",
      session=db_session,
    )

    # Create a regular graph (not a repository)
    Graph.create(
      graph_id=f"kg_user_{self.unique_id}",
      graph_name="User Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    repos = Graph.get_all_repositories(db_session)
    assert len(repos) == 2
    repo_types = [r.repository_type for r in repos]
    assert "sec" in repo_types
    assert "industry" in repo_types

  def test_get_repository_by_type(self, test_org, db_session):
    """Test getting a repository by its type."""
    Graph.find_or_create_repository(
      graph_id=f"sec_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
    )

    repo = Graph.get_repository_by_type("sec", db_session)
    assert repo is not None
    assert repo.repository_type == "sec"
    assert repo.is_repository is True

    # Non-existent type
    not_found = Graph.get_repository_by_type("nonexistent", db_session)
    assert not_found is None

  def test_update_sync_status_success(self, test_org, db_session):
    """Test updating sync status to active."""
    repo = Graph.find_or_create_repository(
      graph_id=f"sec_sync_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
    )

    repo.sync_status = "syncing"
    repo.sync_error_message = "Old error"
    db_session.commit()

    repo.update_sync_status("active", session=db_session)

    assert repo.sync_status == "active"
    assert repo.last_sync_at is not None
    assert repo.sync_error_message is None

  def test_update_sync_status_error(self, test_org, db_session):
    """Test updating sync status to error."""
    repo = Graph.find_or_create_repository(
      graph_id=f"sec_error_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
    )

    repo.update_sync_status(
      "error", error_message="Connection timeout", session=db_session
    )

    assert repo.sync_status == "error"
    assert repo.sync_error_message == "Connection timeout"

  def test_update_sync_status_non_repository_fails(self, test_org, db_session):
    """Test that update_sync_status fails for non-repository graphs."""
    graph = Graph.create(
      graph_id=f"kg_user_{self.unique_id}",
      graph_name="User Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    with pytest.raises(
      ValueError, match="Can only update sync status for repository graphs"
    ):
      graph.update_sync_status("active", session=db_session)

  def test_is_user_graph_property(self, test_org, db_session):
    """Test is_user_graph property."""
    # User graph
    user_graph = Graph.create(
      graph_id=f"kg_user_{self.unique_id}",
      graph_name="User Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )
    assert user_graph.is_user_graph is True

    # Repository
    repo = Graph.find_or_create_repository(
      graph_id=f"sec_{self.unique_id}",
      graph_name="SEC",
      repository_type="sec",
      session=db_session,
    )
    assert repo.is_user_graph is False

  def test_repository_type_validation(self, test_org, db_session):
    """Test that repository graph type is set correctly."""
    repo = Graph.find_or_create_repository(
      graph_id=f"test_repo_{self.unique_id}",
      graph_name="Test Repo",
      repository_type="test",
      session=db_session,
    )

    assert repo.graph_type == "repository"
    assert repo.is_repository is True
    assert repo.repository_type == "test"
