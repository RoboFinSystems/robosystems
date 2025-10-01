"""Comprehensive tests for the UserGraph model."""

import pytest
from unittest.mock import MagicMock
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import UserGraph, User, Graph


class TestUserGraphModel:
  """Test suite for the UserGraph model."""

  def test_user_graph_initialization(self):
    """Test UserGraph model can be instantiated with required fields."""
    user_graph = UserGraph(
      user_id="user_test123",
      graph_id="kg1a2b3c4d5",
      role="member",
    )

    assert user_graph.user_id == "user_test123"
    assert user_graph.graph_id == "kg1a2b3c4d5"
    assert user_graph.role == "member"
    # Default values are set by SQLAlchemy when the object is added to session
    assert user_graph.is_selected is None or user_graph.is_selected is False
    assert user_graph.id is None  # ID is generated on commit

  def test_user_graph_id_generation(self):
    """Test that UserGraph ID is generated with proper format."""
    UserGraph(
      user_id="user_test123",
      graph_id="kg1a2b3c4d5",
    )

    # Call the default lambda to generate ID
    generated_id = UserGraph.id.default.arg(None)
    assert generated_id.startswith("ug_")
    assert len(generated_id) > 3  # ug_ + token

  def test_user_graph_repr(self):
    """Test UserGraph string representation."""
    user_graph = UserGraph(
      user_id="user_test123",
      graph_id="kg1a2b3c4d5",
      role="admin",
    )
    user_graph.id = "ug_test123"

    assert (
      repr(user_graph)
      == "<UserGraph ug_test123 user=user_test123 graph=kg1a2b3c4d5 role=admin>"
    )

  def test_create_user_graph(self, db_session):
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
      session=db_session,
    )

    # Create user-graph relationship
    user_graph = UserGraph.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="admin",
      is_selected=True,
      session=db_session,
    )

    assert user_graph.id is not None
    assert user_graph.id.startswith("ug_")
    assert user_graph.user_id == user.id
    assert user_graph.graph_id == graph.graph_id
    assert user_graph.role == "admin"
    assert user_graph.is_selected is True
    assert user_graph.created_at is not None
    assert user_graph.updated_at is not None

    # Verify in database
    db_user_graph = db_session.query(UserGraph).filter_by(id=user_graph.id).first()
    assert db_user_graph is not None
    assert db_user_graph.user_id == user.id

  def test_create_user_graph_without_session(self):
    """Test that creating user-graph without session raises error."""
    with pytest.raises(ValueError, match="Session is required"):
      UserGraph.create(
        user_id="user_test",
        graph_id="kg_test",
        session=None,
      )

  def test_create_user_graph_rollback_on_error(self):
    """Test that create rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
      UserGraph.create(
        user_id="user_test",
        graph_id="kg_test",
        session=mock_session,
      )

    mock_session.rollback.assert_called_once()

  def test_create_duplicate_user_graph(self, db_session):
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
      session=db_session,
    )

    # Create first relationship
    UserGraph.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="member",
      session=db_session,
    )

    # Try to create duplicate - should fail due to unique constraint
    with pytest.raises(SQLAlchemyError):
      UserGraph.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="admin",
        session=db_session,
      )

  def test_get_by_user_id(self, db_session):
    """Test getting all graph relationships for a user."""
    # Create test user and multiple graphs
    user = User.create(
      email="user_graphs@example.com",
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
        session=db_session,
      )
      graphs.append(graph)

      UserGraph.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="member" if i < 2 else "admin",
        session=db_session,
      )

    # Get all relationships for user
    user_graphs = UserGraph.get_by_user_id(user.id, db_session)
    assert len(user_graphs) == 3

    graph_ids = [ug.graph_id for ug in user_graphs]
    assert "kg_user_0" in graph_ids
    assert "kg_user_1" in graph_ids
    assert "kg_user_2" in graph_ids

  def test_get_by_graph_id(self, db_session):
    """Test getting all user relationships for a graph."""
    # Create test graph and multiple users
    graph = Graph.create(
      graph_id="kg_shared",
      graph_name="Shared Graph",
      graph_type="entity",
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

      UserGraph.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="admin" if i == 0 else "member",
        session=db_session,
      )

    # Get all relationships for graph
    graph_users = UserGraph.get_by_graph_id(graph.graph_id, db_session)
    assert len(graph_users) == 3

    # Check roles
    admin_count = sum(1 for gu in graph_users if gu.role == "admin")
    member_count = sum(1 for gu in graph_users if gu.role == "member")
    assert admin_count == 1
    assert member_count == 2

  def test_get_by_user_and_graph(self, db_session):
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
      session=db_session,
    )

    UserGraph.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="viewer",
      session=db_session,
    )

    # Get specific relationship
    user_graph = UserGraph.get_by_user_and_graph(user.id, graph.graph_id, db_session)
    assert user_graph is not None
    assert user_graph.user_id == user.id
    assert user_graph.graph_id == graph.graph_id
    assert user_graph.role == "viewer"

    # Try with non-existent combination
    not_found = UserGraph.get_by_user_and_graph(
      user.id, "nonexistent_graph", db_session
    )
    assert not_found is None

  def test_get_selected_graph(self, db_session):
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
        session=db_session,
      )

      UserGraph.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="member",
        is_selected=(i == 1),  # Second one is selected
        session=db_session,
      )

    # Get selected graph
    selected = UserGraph.get_selected_graph(user.id, db_session)
    assert selected is not None
    assert selected.graph_id == "kg_sel_1"
    assert selected.is_selected is True

  def test_set_selected_graph(self, db_session):
    """Test setting a graph as the selected one for a user."""
    # Create test user and multiple graphs
    user = User.create(
      email="setselected@example.com",
      name="Set Selected User",
      password_hash="hashed_password",
      session=db_session,
    )

    graphs = []
    user_graphs = []
    for i in range(3):
      graph = Graph.create(
        graph_id=f"kg_setsel_{i}",
        graph_name=f"Set Selectable {i}",
        graph_type="entity",
        session=db_session,
      )
      graphs.append(graph)

      ug = UserGraph.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role="member",
        is_selected=(i == 0),  # First one initially selected
        session=db_session,
      )
      user_graphs.append(ug)

    # Verify initial state
    initial_selected = UserGraph.get_selected_graph(user.id, db_session)
    assert initial_selected.graph_id == "kg_setsel_0"

    # Set different graph as selected
    success = UserGraph.set_selected_graph(user.id, "kg_setsel_2", db_session)
    assert success is True

    # Verify new selection
    new_selected = UserGraph.get_selected_graph(user.id, db_session)
    assert new_selected.graph_id == "kg_setsel_2"

    # Verify old one is deselected
    old_ug = UserGraph.get_by_user_and_graph(user.id, "kg_setsel_0", db_session)
    assert old_ug.is_selected is False

    # Verify only one is selected
    all_ugs = UserGraph.get_by_user_id(user.id, db_session)
    selected_count = sum(1 for ug in all_ugs if ug.is_selected)
    assert selected_count == 1

  def test_set_selected_graph_nonexistent(self, db_session):
    """Test setting a nonexistent graph as selected returns False."""
    user = User.create(
      email="nonex@example.com",
      name="Nonexistent",
      password_hash="hashed_password",
      session=db_session,
    )

    success = UserGraph.set_selected_graph(user.id, "nonexistent_graph", db_session)
    assert success is False

  def test_set_selected_graph_rollback_on_error(self):
    """Test that set_selected_graph rolls back on database error."""
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.first.return_value = UserGraph(user_id="user_test", graph_id="kg_test")
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    with pytest.raises(SQLAlchemyError):
      UserGraph.set_selected_graph("user_test", "kg_test", mock_session)

    mock_session.rollback.assert_called_once()

  def test_user_has_access(self, db_session):
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
      session=db_session,
    )

    graph_without_access = Graph.create(
      graph_id="kg_without_access",
      graph_name="Without Access",
      graph_type="entity",
      session=db_session,
    )

    # Create access to one graph
    UserGraph.create(
      user_id=user.id,
      graph_id=graph_with_access.graph_id,
      role="member",
      session=db_session,
    )

    # Test access
    has_access = UserGraph.user_has_access(
      user.id, graph_with_access.graph_id, db_session
    )
    assert has_access is True

    no_access = UserGraph.user_has_access(
      user.id, graph_without_access.graph_id, db_session
    )
    assert no_access is False

  def test_user_has_admin_access(self, db_session):
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
      session=db_session,
    )

    graph_member = Graph.create(
      graph_id="kg_member",
      graph_name="Member Graph",
      graph_type="entity",
      session=db_session,
    )

    # Create admin access to one graph
    UserGraph.create(
      user_id=user.id,
      graph_id=graph_admin.graph_id,
      role="admin",
      session=db_session,
    )

    # Create member access to another
    UserGraph.create(
      user_id=user.id,
      graph_id=graph_member.graph_id,
      role="member",
      session=db_session,
    )

    # Test admin access
    has_admin = UserGraph.user_has_admin_access(
      user.id, graph_admin.graph_id, db_session
    )
    assert has_admin is True

    no_admin = UserGraph.user_has_admin_access(
      user.id, graph_member.graph_id, db_session
    )
    assert no_admin is False

    # Test with non-existent relationship
    no_access = UserGraph.user_has_admin_access(
      user.id, "nonexistent_graph", db_session
    )
    assert no_access is False

  def test_update_role(self, db_session):
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
      session=db_session,
    )

    user_graph = UserGraph.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="member",
      session=db_session,
    )

    original_updated_at = user_graph.updated_at

    # Update role
    user_graph.update_role("admin", db_session)

    assert user_graph.role == "admin"
    assert user_graph.updated_at > original_updated_at

    # Verify in database
    db_ug = db_session.query(UserGraph).filter_by(id=user_graph.id).first()
    assert db_ug.role == "admin"

  def test_update_role_rollback_on_error(self):
    """Test that update_role rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    user_graph = UserGraph(
      id="ug_test",
      user_id="user_test",
      graph_id="kg_test",
      role="member",
    )

    with pytest.raises(SQLAlchemyError):
      user_graph.update_role("admin", mock_session)

    mock_session.rollback.assert_called_once()

  def test_delete_user_graph(self, db_session):
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
      session=db_session,
    )

    user_graph = UserGraph.create(
      user_id=user.id,
      graph_id=graph.graph_id,
      role="member",
      session=db_session,
    )

    ug_id = user_graph.id

    # Delete the relationship
    user_graph.delete(db_session)

    # Verify deletion
    db_ug = db_session.query(UserGraph).filter_by(id=ug_id).first()
    assert db_ug is None

  def test_delete_rollback_on_error(self):
    """Test that delete rolls back on database error."""
    mock_session = MagicMock()
    mock_session.commit.side_effect = SQLAlchemyError("Database error")

    user_graph = UserGraph(
      id="ug_test",
      user_id="user_test",
      graph_id="kg_test",
    )

    with pytest.raises(SQLAlchemyError):
      user_graph.delete(mock_session)

    mock_session.rollback.assert_called_once()

  def test_user_graph_relationships(self):
    """Test that UserGraph model has correct relationship definitions."""
    user_graph = UserGraph(
      user_id="user_test",
      graph_id="kg_test",
    )

    # Check relationship attributes exist
    assert hasattr(user_graph, "user")
    assert hasattr(user_graph, "graph")

  def test_multiple_roles_different_graphs(self, db_session):
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
        session=db_session,
      )

      UserGraph.create(
        user_id=user.id,
        graph_id=graph.graph_id,
        role=role,
        session=db_session,
      )

    # Verify each role
    for role in roles:
      ug = UserGraph.get_by_user_and_graph(user.id, f"kg_role_{role}", db_session)
      assert ug is not None
      assert ug.role == role
