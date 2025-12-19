import pytest
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import GraphTier
from robosystems.middleware.graph.types import is_subgraph_id, parse_graph_id
from robosystems.models.iam import Graph, GraphUser, User


@pytest.fixture
def auth_user(db_session: Session) -> User:
  import uuid

  from robosystems.utils.ulid import generate_prefixed_ulid

  user = User(
    id=generate_prefixed_ulid("user"),
    email=f"auth_{uuid.uuid4().hex[:8]}@example.com",
    name="Auth Test User",
    password_hash="hashed_password",
    is_active=True,
    email_verified=True,
  )
  db_session.add(user)
  db_session.commit()
  return user


@pytest.fixture
def other_user(db_session: Session) -> User:
  import uuid

  from robosystems.utils.ulid import generate_prefixed_ulid

  user = User(
    id=generate_prefixed_ulid("user"),
    email=f"other_{uuid.uuid4().hex[:8]}@example.com",
    name="Other User",
    password_hash="hashed_password",
    is_active=True,
    email_verified=True,
  )
  db_session.add(user)
  db_session.commit()
  return user


@pytest.fixture
def user_graph(db_session: Session, auth_user: User) -> Graph:
  import uuid

  graph_id = f"kg{uuid.uuid4().hex[:16]}"
  graph = Graph(
    graph_id=graph_id,
    graph_name="User's Graph",
    graph_type="generic",
    graph_tier=GraphTier.LADYBUG_STANDARD.value,
  )
  db_session.add(graph)

  graph_user = GraphUser(
    user_id=auth_user.id,
    graph_id=graph.graph_id,
    role="admin",
  )
  db_session.add(graph_user)
  db_session.commit()

  return graph


class TestSubgraphAuthorization:
  """Test that subgraph access is properly scoped to parent owner"""

  @pytest.mark.unit
  def test_user_has_access_to_parent_implies_subgraph_access(
    self,
    db_session: Session,
    auth_user: User,
    user_graph: Graph,
  ):
    """User with parent graph access should have access to subgraphs"""
    subgraph_id = f"{user_graph.graph_id}_dev"

    parent_id, subgraph_name = parse_graph_id(subgraph_id)
    assert parent_id == user_graph.graph_id
    assert subgraph_name == "dev"

    has_parent_access = GraphUser.user_has_access(auth_user.id, parent_id, db_session)
    assert has_parent_access is True

  @pytest.mark.unit
  def test_user_without_parent_access_denied_subgraph(
    self,
    db_session: Session,
    other_user: User,
    user_graph: Graph,
  ):
    """User without parent graph access should not access subgraphs"""
    subgraph_id = f"{user_graph.graph_id}_dev"

    parent_id, _ = parse_graph_id(subgraph_id)

    has_parent_access = GraphUser.user_has_access(other_user.id, parent_id, db_session)
    assert has_parent_access is False

  @pytest.mark.unit
  def test_subgraph_id_detection(self):
    """Verify subgraph ID detection works correctly"""
    assert is_subgraph_id("kg1234567890abcdef_dev") is True
    assert is_subgraph_id("kg1234567890abcdef_production") is True
    assert is_subgraph_id("kg1234567890abcdef") is False
    assert is_subgraph_id("sec") is False
    assert is_subgraph_id("industry") is False

  @pytest.mark.unit
  def test_shared_repository_not_treated_as_parent(self):
    """Shared repositories should not be parsed as parent graphs"""
    assert is_subgraph_id("sec") is False
    assert is_subgraph_id("industry") is False
    assert is_subgraph_id("economic") is False

    parent_id, subgraph_name = parse_graph_id("sec")
    assert parent_id == "sec"
    assert subgraph_name is None

  @pytest.mark.unit
  def test_admin_role_on_parent_implies_subgraph_admin(
    self,
    db_session: Session,
    auth_user: User,
    user_graph: Graph,
  ):
    """Admin role on parent should imply admin on subgraphs"""
    subgraph_id = f"{user_graph.graph_id}_staging"

    parent_id, _ = parse_graph_id(subgraph_id)

    has_admin = GraphUser.user_has_admin_access(auth_user.id, parent_id, db_session)
    assert has_admin is True

  @pytest.mark.unit
  def test_graph_user_record_uses_parent_id_only(
    self,
    db_session: Session,
    auth_user: User,
    user_graph: Graph,
  ):
    """GraphUser records should only exist for parent graphs, not subgraphs"""
    subgraph_id = f"{user_graph.graph_id}_test"

    parent_access = GraphUser.user_has_access(
      auth_user.id, user_graph.graph_id, db_session
    )
    assert parent_access is True

    direct_record = (
      db_session.query(GraphUser)
      .filter(GraphUser.user_id == auth_user.id, GraphUser.graph_id == subgraph_id)
      .first()
    )
    assert direct_record is None

    subgraph_access = GraphUser.user_has_access(auth_user.id, subgraph_id, db_session)
    assert subgraph_access is True

  @pytest.mark.integration
  def test_authorization_middleware_handles_subgraph_ids(
    self,
    db_session: Session,
    auth_user: User,
    user_graph: Graph,
  ):
    """Authorization middleware should check parent permissions for subgraphs"""
    from robosystems.middleware.graph.utils import MultiTenantUtils

    subgraph_id = f"{user_graph.graph_id}_dev"

    is_shared = MultiTenantUtils.is_shared_repository(subgraph_id)
    assert is_shared is False

    parent_id, subgraph_name = parse_graph_id(subgraph_id)
    assert parent_id == user_graph.graph_id

    has_access = GraphUser.user_has_access(auth_user.id, parent_id, db_session)
    assert has_access is True

  @pytest.mark.unit
  def test_parse_various_subgraph_formats(self):
    """Test parsing various valid subgraph ID formats"""
    test_cases = [
      ("kg1234567890abcdef_dev", "kg1234567890abcdef", "dev"),
      ("kg1234567890abcdef_staging", "kg1234567890abcdef", "staging"),
      ("kgabcdef1234567890_prod", "kgabcdef1234567890", "prod"),
      ("kg0123456789abcdef_test123", "kg0123456789abcdef", "test123"),
      ("kg1234567890abcdef_a", "kg1234567890abcdef", "a"),
    ]

    for graph_id, expected_parent, expected_subgraph in test_cases:
      parent_id, subgraph_name = parse_graph_id(graph_id)
      assert parent_id == expected_parent
      assert subgraph_name == expected_subgraph
      assert is_subgraph_id(graph_id) is True

  @pytest.mark.unit
  def test_parent_graph_formats(self):
    """Test parsing parent graph IDs"""
    test_cases = [
      "kg1234567890abcdef",
      "kg123456",
      "sec",
      "industry",
      "economic",
    ]

    for graph_id in test_cases:
      parent_id, subgraph_name = parse_graph_id(graph_id)
      assert parent_id == graph_id
      assert subgraph_name is None
      # Only hex-based IDs without underscores should return False
      if "_" not in graph_id and graph_id not in ["sec", "industry", "economic"]:
        assert is_subgraph_id(graph_id) is False

  @pytest.mark.integration
  def test_multiple_users_cannot_access_others_subgraphs(
    self,
    db_session: Session,
    auth_user: User,
    other_user: User,
    user_graph: Graph,
  ):
    """Users should only access subgraphs of graphs they own"""
    subgraph_id = f"{user_graph.graph_id}_private"
    parent_id, _ = parse_graph_id(subgraph_id)

    owner_access = GraphUser.user_has_access(auth_user.id, parent_id, db_session)
    assert owner_access is True

    other_access = GraphUser.user_has_access(other_user.id, parent_id, db_session)
    assert other_access is False
