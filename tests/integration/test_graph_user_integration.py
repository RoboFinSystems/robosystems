"""Integration tests for Graph and GraphUser models working together."""

import pytest
from sqlalchemy.exc import IntegrityError

from robosystems.models.iam import Graph, GraphUser, User, GraphCredits
from robosystems.config.graph_tier import GraphTier


class TestGraphUserIntegration:
  """Test Graph and GraphUser models working together."""

  def test_create_graph_with_user_access(self, test_db, test_user, test_org):
    """Test creating a graph and granting user access in one flow."""
    # Create graph
    graph = Graph.create(
      graph_id="entity_integration_test",
      graph_name="Integration Test Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
      base_schema="base",
      schema_extensions=["roboledger"],
      graph_tier=GraphTier.LADYBUG_STANDARD,
      graph_metadata={
        "created_by": test_user.id,
        "purpose": "integration testing",
      },
    )

    # Grant user access
    user_graph = GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role="admin",
      is_selected=True,
      session=test_db,
    )

    # Verify the relationship works both ways
    assert user_graph.graph.graph_id == graph.graph_id
    assert user_graph.graph.graph_name == "Integration Test Entity"
    assert user_graph.graph.has_specific_extension("roboledger")

    # Verify we can query through relationships
    graph_users = GraphUser.get_by_user_id(test_user.id, test_db)
    assert len(graph_users) == 1
    assert graph_users[0].graph.graph_tier == GraphTier.LADYBUG_STANDARD.value

  def test_graph_deletion_cascades(self, test_db, test_user, test_org):
    """Test that deleting a graph removes all GraphUser relationships."""
    # Create graph
    graph = Graph.create(
      graph_id="cascade_test",
      graph_name="Cascade Test",
      graph_type="generic",
      org_id=test_org.id,
      session=test_db,
    )

    # Create multiple user relationships
    user1 = test_user
    # Create password hash for the second user
    import bcrypt

    salt = bcrypt.gensalt()
    password_hash = bcrypt.hashpw("password123".encode("utf-8"), salt).decode("utf-8")

    user2 = User.create(
      email="user2@test.com",
      name="User 2",
      password_hash=password_hash,
      session=test_db,
    )

    GraphUser.create(
      user_id=user1.id, graph_id=graph.graph_id, role="admin", session=test_db
    )
    GraphUser.create(
      user_id=user2.id, graph_id=graph.graph_id, role="member", session=test_db
    )

    # Verify relationships exist
    assert len(GraphUser.get_by_graph_id(graph.graph_id, test_db)) == 2

    # Delete the graph
    graph.delete(test_db)

    # Verify cascade deletion
    assert Graph.get_by_id("cascade_test", test_db) is None
    assert len(GraphUser.get_by_graph_id("cascade_test", test_db)) == 0

  def test_user_cannot_access_deleted_graph(self, test_db, test_user, test_org):
    """Test that users cannot create relationships to non-existent graphs."""
    # This should fail due to foreign key constraint
    with pytest.raises(IntegrityError):
      GraphUser.create(
        user_id=test_user.id,
        graph_id="non_existent_graph",
        role="admin",
        session=test_db,
      )

  def test_multi_tenant_access_control(self, test_db, test_org):
    """Test multi-tenant access control scenarios."""
    # Create two users
    import bcrypt

    salt = bcrypt.gensalt()
    password_hash = bcrypt.hashpw("password123".encode("utf-8"), salt).decode("utf-8")

    user1 = User(
      id="tenant1",
      email="tenant1@test.com",
      name="Tenant 1",
      password_hash=password_hash,
    )
    user2 = User(
      id="tenant2",
      email="tenant2@test.com",
      name="Tenant 2",
      password_hash=password_hash,
    )
    test_db.add_all([user1, user2])
    test_db.commit()

    # Create graphs for each tenant
    graph1 = Graph.create(
      graph_id="tenant1_entity",
      graph_name="Tenant 1 Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
    )

    graph2 = Graph.create(
      graph_id="tenant2_entity",
      graph_name="Tenant 2 Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
    )

    # Grant access
    GraphUser.create(
      user_id=user1.id, graph_id=graph1.graph_id, role="admin", session=test_db
    )
    GraphUser.create(
      user_id=user2.id, graph_id=graph2.graph_id, role="admin", session=test_db
    )

    # Verify isolation
    assert GraphUser.user_has_access(user1.id, graph1.graph_id, test_db) is True
    assert GraphUser.user_has_access(user1.id, graph2.graph_id, test_db) is False
    assert GraphUser.user_has_access(user2.id, graph1.graph_id, test_db) is False
    assert GraphUser.user_has_access(user2.id, graph2.graph_id, test_db) is True

  def test_shared_graph_multiple_users(self, test_db, test_org):
    """Test multiple users sharing access to the same graph."""
    # Create a graph
    graph = Graph.create(
      graph_id="shared_graph",
      graph_name="Shared Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
      schema_extensions=["roboledger"],
    )

    # Create multiple users
    import bcrypt

    salt = bcrypt.gensalt()
    password_hash = bcrypt.hashpw("password123".encode("utf-8"), salt).decode("utf-8")

    users = []
    roles = ["admin", "member", "member", "viewer"]
    for i, role in enumerate(roles):
      user = User(
        id=f"shared_user_{i}",
        email=f"shared_user{i}@test.com",
        name=f"Shared User {i}",
        password_hash=password_hash,
      )
      test_db.add(user)
      users.append(user)
    test_db.commit()

    # Grant different levels of access
    for user, role in zip(users, roles):
      GraphUser.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role=role,
        session=test_db,
      )

    # Verify access levels
    graph_users = GraphUser.get_by_graph_id(graph.graph_id, test_db)
    assert len(graph_users) == 4

    admin_count = sum(1 for ug in graph_users if ug.role == "admin")
    member_count = sum(1 for ug in graph_users if ug.role == "member")
    viewer_count = sum(1 for ug in graph_users if ug.role == "viewer")

    assert admin_count == 1
    assert member_count == 2
    assert viewer_count == 1

  def test_graph_selection_across_multiple_graphs(self, test_db, test_user, test_org):
    """Test graph selection when user has access to multiple graphs."""
    # Create multiple graphs
    graphs = []
    for i in range(3):
      graph = Graph.create(
        graph_id=f"multi_graph_{i}",
        graph_name=f"Graph {i}",
        graph_type="entity" if i < 2 else "generic",
        org_id=test_org.id,
        session=test_db,
      )
      graphs.append(graph)

      # Grant access
      GraphUser.create(
        user_id=test_user.id,
        graph_id=graph.graph_id,
        role="admin",
        is_selected=(i == 0),  # First one selected
        session=test_db,
      )

    # Verify initial selection
    selected = GraphUser.get_selected_graph(test_user.id, test_db)
    assert selected.graph_id == "multi_graph_0"

    # Switch selection
    GraphUser.set_selected_graph(test_user.id, "multi_graph_2", test_db)

    # Verify new selection
    selected = GraphUser.get_selected_graph(test_user.id, test_db)
    assert selected.graph_id == "multi_graph_2"
    assert selected.graph.graph_type == "generic"

  def test_graph_with_credits_integration(self, test_db, test_user, test_org):
    """Test that graph tier affects credit calculations correctly."""
    # Create graphs with different tiers
    standard_graph = Graph.create(
      graph_id="standard_tier_graph",
      graph_name="Standard Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
      graph_tier=GraphTier.LADYBUG_STANDARD,
    )

    premium_graph = Graph.create(
      graph_id="premium_tier_graph",
      graph_name="Premium Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
      graph_tier=GraphTier.LADYBUG_XLARGE,
    )

    # Grant access
    GraphUser.create(
      user_id=test_user.id,
      graph_id=standard_graph.graph_id,
      role="admin",
      session=test_db,
    )

    GraphUser.create(
      user_id=test_user.id,
      graph_id=premium_graph.graph_id,
      role="admin",
      session=test_db,
    )

    # Create credits for each graph
    from decimal import Decimal
    from datetime import datetime, timezone

    standard_credits = GraphCredits(
      id="sc_001",
      graph_id=standard_graph.graph_id,
      user_id=test_user.id,
      billing_admin_id=test_user.id,
      current_balance=Decimal("1000.0"),
      monthly_allocation=Decimal("1000.0"),
      last_allocation_date=datetime.now(timezone.utc),
    )

    premium_credits = GraphCredits(
      id="pc_001",
      graph_id=premium_graph.graph_id,
      user_id=test_user.id,
      billing_admin_id=test_user.id,
      current_balance=Decimal("1000.0"),
      monthly_allocation=Decimal("1000.0"),
      last_allocation_date=datetime.now(timezone.utc),
    )

    test_db.add_all([standard_credits, premium_credits])
    test_db.commit()

    # Verify both credits were created successfully
    # Credits are used only for AI operations with per-token pricing
    assert standard_credits.current_balance == Decimal("1000.0")
    assert premium_credits.current_balance == Decimal("1000.0")

  def test_graph_metadata_access_through_user_graph(self, test_db, test_user, test_org):
    """Test accessing graph metadata through GraphUser relationship."""
    # Create a graph with rich metadata
    graph = Graph.create(
      graph_id="metadata_rich_graph",
      graph_name="Metadata Test Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
      schema_extensions=["roboledger", "roboinvestor"],
      graph_metadata={
        "industry": "Technology",
        "employees": 150,
        "founded": 2020,
        "tags": ["startup", "fintech"],
      },
    )

    # Grant user access
    user_graph = GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role="viewer",
      session=test_db,
    )

    # Access metadata through relationship
    assert user_graph.graph.graph_metadata["industry"] == "Technology"
    assert user_graph.graph.graph_metadata["employees"] == 150
    assert "fintech" in user_graph.graph.graph_metadata["tags"]
    assert user_graph.graph.has_specific_extension("roboinvestor")

  def test_role_based_operations(self, test_db, test_org):
    """Test operations that depend on user roles."""
    # Create graph
    graph = Graph.create(
      graph_id="role_test_graph",
      graph_name="Role Test Entity",
      graph_type="entity",
      org_id=test_org.id,
      session=test_db,
    )

    # Create users with different roles
    import bcrypt

    salt = bcrypt.gensalt()
    password_hash = bcrypt.hashpw("password123".encode("utf-8"), salt).decode("utf-8")

    admin_user = User(
      id="admin", email="admin@test.com", name="Admin", password_hash=password_hash
    )
    member_user = User(
      id="member", email="member@test.com", name="Member", password_hash=password_hash
    )
    viewer_user = User(
      id="viewer", email="viewer@test.com", name="Viewer", password_hash=password_hash
    )
    test_db.add_all([admin_user, member_user, viewer_user])
    test_db.commit()

    # Grant different access levels
    GraphUser.create(
      user_id=admin_user.id, graph_id=graph.graph_id, role="admin", session=test_db
    )
    GraphUser.create(
      user_id=member_user.id, graph_id=graph.graph_id, role="member", session=test_db
    )
    GraphUser.create(
      user_id=viewer_user.id, graph_id=graph.graph_id, role="viewer", session=test_db
    )

    # Test role-based checks
    assert (
      GraphUser.user_has_admin_access(admin_user.id, graph.graph_id, test_db) is True
    )
    assert (
      GraphUser.user_has_admin_access(member_user.id, graph.graph_id, test_db) is False
    )
    assert (
      GraphUser.user_has_admin_access(viewer_user.id, graph.graph_id, test_db) is False
    )

    # All have basic access
    assert GraphUser.user_has_access(admin_user.id, graph.graph_id, test_db) is True
    assert GraphUser.user_has_access(member_user.id, graph.graph_id, test_db) is True
    assert GraphUser.user_has_access(viewer_user.id, graph.graph_id, test_db) is True
