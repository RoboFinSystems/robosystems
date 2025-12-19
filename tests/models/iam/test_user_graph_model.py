"""Comprehensive tests for the GraphUser model."""

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import Graph, GraphUser, User


class TestGraphUserModel:
  """Test suite for the GraphUser model."""

  def test_graph_user_initialization(self):
    """Test GraphUser model can be instantiated with required fields."""
    graph_user = GraphUser(
      user_id="user_test123",
      graph_id="kg1a2b3c4d5",
      role="member",
    )

    assert graph_user.user_id == "user_test123"
    assert graph_user.graph_id == "kg1a2b3c4d5"
    assert graph_user.role == "member"
    # Default values are set by SQLAlchemy when the object is added to session
    assert graph_user.is_selected is None or graph_user.is_selected is False
    assert graph_user.id is None  # ID is generated on commit

  def test_graph_user_id_generation(self):
    """Test that GraphUser ID is generated with proper format."""
    GraphUser(
      user_id="user_test123",
      graph_id="kg1a2b3c4d5",
    )

    # Call the default lambda to generate ID
    generated_id = GraphUser.id.default.arg(None)
    assert generated_id.startswith("gu_")
    assert len(generated_id) > 3  # gu_ + token

  def test_graph_user_repr(self):
    """Test GraphUser string representation."""
    graph_user = GraphUser(
      user_id="user_test123",
      graph_id="kg1a2b3c4d5",
      role="admin",
    )
    graph_user.id = "gu_test123"

    assert (
      repr(graph_user)
      == "<GraphUser gu_test123 graph=kg1a2b3c4d5 user=user_test123 role=admin>"
    )

  def test_create_graph_user(self, test_org, db_session):
    """Test creating a new user-graph relationship."""
    # Create test user and graph first
    user = User.create(
      email="ug@example.com",
      name="UG User",
      password_hash="hashed_password",
      session=db_session,
    )

    graph = Graph.create(
      graph_id="kg_test",
      graph_name="Test Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    # Create user-graph relationship
    graph_user = GraphUser.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="admin",
      is_selected=True,
      session=db_session,
    )

    assert graph_user.id is not None
    assert graph_user.id.startswith("gu_")
    assert graph_user.user_id == user.id
    assert graph_user.graph_id == graph.graph_id
    assert graph_user.role == "admin"
    assert graph_user.is_selected is True
    assert graph_user.created_at is not None
    assert graph_user.updated_at is not None

    # Verify in database
    db_graph_user = db_session.query(GraphUser).filter_by(id=graph_user.id).first()
    assert db_graph_user is not None
    assert db_graph_user.user_id == user.id

  def test_create_graph_user_without_session(self):
    """Test that creating user-graph without session raises error."""
    with pytest.raises(ValueError, match="Session is required"):
      GraphUser.create(
        user_id="user_test",
        graph_id="kg_test",
        session=None,
      )

  def test_create_graph_user_rollback_on_error(self):
    """Test that create rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
      GraphUser.create(
        user_id="user_test",
        graph_id="kg_test",
        session=mock_session,
      )

    mock_session.rollback.assert_called_once()

  def test_create_duplicate_graph_user(self, test_org, db_session):
    """Test that creating duplicate user-graph relationship fails."""
    # Create test user and graph
    user = User.create(
      email="dup@example.com",
      name="Dup User",
      password_hash="hashed_password",
      session=db_session,
    )

    graph = Graph.create(
      graph_id="kg_dup",
      graph_name="Dup Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    # Create first relationship
    GraphUser.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="member",
      session=db_session,
    )

    # Try to create duplicate - should fail due to unique constraint
    with pytest.raises(SQLAlchemyError):
      GraphUser.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="admin",
        session=db_session,
      )

  def test_get_by_user_id(self, test_org, db_session):
    """Test getting all graph relationships for a user."""
    # Create test user and multiple graphs
    user = User.create(
      email="graph_users@example.com",
      name="User Graphs",
      password_hash="hashed_password",
      session=db_session,
    )

    graphs = []
    for i in range(3):
      graph = Graph.create(
        graph_id=f"kg_user_{i}",
        graph_name=f"Graph {i}",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
      )
      graphs.append(graph)

      GraphUser.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="member" if i < 2 else "admin",
        session=db_session,
      )

    # Get all relationships for user
    graph_users = GraphUser.get_by_user_id(user.id, db_session)
    assert len(graph_users) == 3

    graph_ids = [ug.graph_id for ug in graph_users]
    assert "kg_user_0" in graph_ids
    assert "kg_user_1" in graph_ids
    assert "kg_user_2" in graph_ids

  def test_get_by_graph_id(self, test_org, db_session):
    """Test getting all user relationships for a graph."""
    # Create test graph and multiple users
    graph = Graph.create(
      graph_id="kg_shared",
      graph_name="Shared Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    users = []
    for i in range(3):
      user = User.create(
        email=f"graph_user_{i}@example.com",
        name=f"Graph User {i}",
        password_hash="hashed_password",
        session=db_session,
      )
      users.append(user)

      GraphUser.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="admin" if i == 0 else "member",
        session=db_session,
      )

    # Get all relationships for graph
    graph_users = GraphUser.get_by_graph_id(graph.graph_id, db_session)
    assert len(graph_users) == 3

    # Check roles
    admin_count = sum(1 for gu in graph_users if gu.role == "admin")
    member_count = sum(1 for gu in graph_users if gu.role == "member")
    assert admin_count == 1
    assert member_count == 2

  def test_get_by_user_and_graph(self, test_org, db_session):
    """Test getting a specific user-graph relationship."""
    # Create test user and graph
    user = User.create(
      email="specific@example.com",
      name="Specific User",
      password_hash="hashed_password",
      session=db_session,
    )

    graph = Graph.create(
      graph_id="kg_specific",
      graph_name="Specific Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    GraphUser.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="viewer",
      session=db_session,
    )

    # Get specific relationship
    graph_user = GraphUser.get_by_user_and_graph(user.id, graph.graph_id, db_session)
    assert graph_user is not None
    assert graph_user.user_id == user.id
    assert graph_user.graph_id == graph.graph_id
    assert graph_user.role == "viewer"

    # Try with non-existent combination
    not_found = GraphUser.get_by_user_and_graph(
      user.id, "nonexistent_graph", db_session
    )
    assert not_found is None

  def test_get_selected_graph(self, test_org, db_session):
    """Test getting the currently selected graph for a user."""
    # Create test user and multiple graphs
    user = User.create(
      email="selected@example.com",
      name="Selected User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Create multiple graphs
    for i in range(3):
      graph = Graph.create(
        graph_id=f"kg_sel_{i}",
        graph_name=f"Selectable {i}",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
      )

      GraphUser.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="member",
        is_selected=(i == 1),  # Second one is selected
        session=db_session,
      )

    # Get selected graph
    selected = GraphUser.get_selected_graph(user.id, db_session)
    assert selected is not None
    assert selected.graph_id == "kg_sel_1"
    assert selected.is_selected is True

  def test_set_selected_graph(self, test_org, db_session):
    """Test setting a graph as the selected one for a user."""
    # Create test user and multiple graphs
    user = User.create(
      email="setselected@example.com",
      name="Set Selected User",
      password_hash="hashed_password",
      session=db_session,
    )

    graphs = []
    graph_users = []
    for i in range(3):
      graph = Graph.create(
        graph_id=f"kg_setsel_{i}",
        graph_name=f"Set Selectable {i}",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
      )
      graphs.append(graph)

      ug = GraphUser.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="member",
        is_selected=(i == 0),  # First one initially selected
        session=db_session,
      )
      graph_users.append(ug)

    # Verify initial state
    initial_selected = GraphUser.get_selected_graph(user.id, db_session)
    assert initial_selected.graph_id == "kg_setsel_0"

    # Set different graph as selected
    success = GraphUser.set_selected_graph(user.id, "kg_setsel_2", db_session)
    assert success is True

    # Verify new selection
    new_selected = GraphUser.get_selected_graph(user.id, db_session)
    assert new_selected.graph_id == "kg_setsel_2"

    # Verify old one is deselected
    old_ug = GraphUser.get_by_user_and_graph(user.id, "kg_setsel_0", db_session)
    assert old_ug.is_selected is False

    # Verify only one is selected
    all_ugs = GraphUser.get_by_user_id(user.id, db_session)
    selected_count = sum(1 for ug in all_ugs if ug.is_selected)
    assert selected_count == 1

  def test_set_selected_graph_nonexistent(self, test_org, db_session):
    """Test setting a nonexistent graph as selected returns False."""
    user = User.create(
      email="nonex@example.com",
      name="Nonexistent",
      password_hash="hashed_password",
      session=db_session,
    )

    success = GraphUser.set_selected_graph(user.id, "nonexistent_graph", db_session)
    assert success is False

  def test_set_selected_graph_rollback_on_error(self):
    """Test that set_selected_graph rolls back on database error."""
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.first.return_value = GraphUser(user_id="user_test", graph_id="kg_test")
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
      GraphUser.set_selected_graph("user_test", "kg_test", mock_session)

    mock_session.rollback.assert_called_once()

  def test_user_has_access(self, test_org, db_session):
    """Test checking if a user has access to a specific graph."""
    # Create test user and graph
    user = User.create(
      email="access@example.com",
      name="Access User",
      password_hash="hashed_password",
      session=db_session,
    )

    graph_with_access = Graph.create(
      graph_id="kg_with_access",
      graph_name="With Access",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    graph_without_access = Graph.create(
      graph_id="kg_without_access",
      graph_name="Without Access",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    # Create access to one graph
    GraphUser.create(
      user_id=user.id,
      graph_id=graph_with_access.graph_id,
      role="member",
      session=db_session,
    )

    # Test access
    has_access = GraphUser.user_has_access(
      user.id, graph_with_access.graph_id, db_session
    )
    assert has_access is True

    no_access = GraphUser.user_has_access(
      user.id, graph_without_access.graph_id, db_session
    )
    assert no_access is False

  def test_user_has_admin_access(self, test_org, db_session):
    """Test checking if a user has admin access to a specific graph."""
    # Create test user and graphs
    user = User.create(
      email="adminaccess@example.com",
      name="Admin Access User",
      password_hash="hashed_password",
      session=db_session,
    )

    graph_admin = Graph.create(
      graph_id="kg_admin",
      graph_name="Admin Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    graph_member = Graph.create(
      graph_id="kg_member",
      graph_name="Member Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    # Create admin access to one graph
    GraphUser.create(
      user_id=user.id,
      graph_id=graph_admin.graph_id,
      role="admin",
      session=db_session,
    )

    # Create member access to another
    GraphUser.create(
      user_id=user.id,
      graph_id=graph_member.graph_id,
      role="member",
      session=db_session,
    )

    # Test admin access
    has_admin = GraphUser.user_has_admin_access(
      user.id, graph_admin.graph_id, db_session
    )
    assert has_admin is True

    no_admin = GraphUser.user_has_admin_access(
      user.id, graph_member.graph_id, db_session
    )
    assert no_admin is False

    # Test with non-existent relationship
    no_access = GraphUser.user_has_admin_access(
      user.id, "nonexistent_graph", db_session
    )
    assert no_access is False

  def test_update_role(self, test_org, db_session):
    """Test updating the user's role for a graph."""
    # Create test user and graph
    user = User.create(
      email="updaterole@example.com",
      name="Update Role User",
      password_hash="hashed_password",
      session=db_session,
    )

    graph = Graph.create(
      graph_id="kg_role",
      graph_name="Role Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    graph_user = GraphUser.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="member",
      session=db_session,
    )

    original_updated_at = graph_user.updated_at

    # Update role
    graph_user.update_role("admin", db_session)

    assert graph_user.role == "admin"
    assert graph_user.updated_at > original_updated_at

    # Verify in database
    db_ug = db_session.query(GraphUser).filter_by(id=graph_user.id).first()
    assert db_ug.role == "admin"

  def test_update_role_rollback_on_error(self):
    """Test that update_role rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    graph_user = GraphUser(
      id="gu_test",
      user_id="user_test",
      graph_id="kg_test",
      role="member",
    )

    with pytest.raises(SQLAlchemyError):
      graph_user.update_role("admin", mock_session)

    mock_session.rollback.assert_called_once()

  def test_delete_graph_user(self, test_org, db_session):
    """Test deleting a user-graph relationship."""
    # Create test user and graph
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    user = User.create(
      email=f"delete_{unique_id}@example.com",
      name="Delete User",
      password_hash="hashed_password",
      session=db_session,
    )

    graph = Graph.create(
      graph_id="kg_delete",
      graph_name="Delete Graph",
      graph_type="entity",
      org_id=test_org.id,
      session=db_session,
    )

    graph_user = GraphUser.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="member",
      session=db_session,
    )

    ug_id = graph_user.id

    # Delete the relationship
    graph_user.delete(db_session)

    # Verify deletion
    db_ug = db_session.query(GraphUser).filter_by(id=ug_id).first()
    assert db_ug is None

  def test_delete_rollback_on_error(self):
    """Test that delete rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    graph_user = GraphUser(
      id="gu_test",
      user_id="user_test",
      graph_id="kg_test",
    )

    with pytest.raises(SQLAlchemyError):
      graph_user.delete(mock_session)

    mock_session.rollback.assert_called_once()

  def test_graph_user_relationships(self):
    """Test that GraphUser model has correct relationship definitions."""
    graph_user = GraphUser(
      user_id="user_test",
      graph_id="kg_test",
    )

    # Check relationship attributes exist
    assert hasattr(graph_user, "user")
    assert hasattr(graph_user, "graph")

  def test_multiple_roles_different_graphs(self, test_org, db_session):
    """Test that a user can have different roles in different graphs."""
    # Create test user
    user = User.create(
      email="multirole@example.com",
      name="Multi Role User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Create graphs with different roles
    roles = ["admin", "member", "viewer"]
    for i, role in enumerate(roles):
      graph = Graph.create(
        graph_id=f"kg_role_{role}",
        graph_name=f"{role.title()} Graph",
        graph_type="entity",
        org_id=test_org.id,
        session=db_session,
      )

      GraphUser.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role=role,
        session=db_session,
      )

    # Verify each role
    for role in roles:
      ug = GraphUser.get_by_user_and_graph(user.id, f"kg_role_{role}", db_session)
      assert ug is not None
      assert ug.role == role
