"""Tests for GraphRole ordering and effective-role resolution.

Pins the two access sources: explicit GraphUser rows and the implicit graph
admin held by org OWNER/ADMIN on org-owned graphs.
"""

from uuid import uuid4

import pytest

from robosystems.models.core import (
  Graph,
  GraphRole,
  GraphUser,
  OrgRole,
  OrgUser,
  User,
)


def _kg_id() -> str:
  return f"kg{uuid4().hex[:16]}"


def _create_user(session, password_hash: str) -> User:
  suffix = uuid4().hex[:8]
  user = User(
    email=f"grantee+{suffix}@example.com",
    name=f"Grantee {suffix}",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


def _create_org_graph(session, org_id: str) -> Graph:
  return Graph.create(
    graph_id=_kg_id(),
    org_id=org_id,
    graph_name="Access Graph",
    graph_type="generic",
    session=session,
  )


class TestGraphRoleOrdering:
  def test_role_ordering(self):
    assert GraphRole.ADMIN.at_least(GraphRole.MEMBER)
    assert GraphRole.MEMBER.at_least(GraphRole.VIEWER)
    assert not GraphRole.VIEWER.at_least(GraphRole.MEMBER)
    assert not GraphRole.MEMBER.at_least(GraphRole.ADMIN)

  def test_coerce_rejects_unknown_role(self):
    with pytest.raises(ValueError):
      GraphRole.coerce("reader")

  def test_create_rejects_unknown_role(self, test_db, test_user, test_org):
    graph = _create_org_graph(test_db, test_org.id)
    with pytest.raises(ValueError):
      GraphUser.create(
        user_id=test_user.id, graph_id=graph.graph_id, role="reader", session=test_db
      )


class TestEffectiveRole:
  def test_org_owner_holds_implicit_admin(self, test_db, test_user, test_org):
    """Org OWNER has admin on org graphs without any GraphUser row."""
    graph = _create_org_graph(test_db, test_org.id)

    role, implicit = GraphUser.get_effective_role(test_user.id, graph.graph_id, test_db)
    assert role == GraphRole.ADMIN
    assert implicit is True
    assert GraphUser.user_has_access(test_user.id, graph.graph_id, test_db)
    assert GraphUser.user_has_write_access(test_user.id, graph.graph_id, test_db)
    assert GraphUser.user_has_admin_access(test_user.id, graph.graph_id, test_db)

  def test_org_admin_holds_implicit_admin(self, test_db, test_user, test_org):
    graph = _create_org_graph(test_db, test_org.id)
    org_admin = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=test_org.id, user_id=org_admin.id, role=OrgRole.ADMIN, session=test_db
    )

    assert GraphUser.user_has_admin_access(org_admin.id, graph.graph_id, test_db)

  def test_org_member_has_no_implicit_access(self, test_db, test_user, test_org):
    graph = _create_org_graph(test_db, test_org.id)
    org_member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=test_org.id, user_id=org_member.id, role=OrgRole.MEMBER, session=test_db
    )

    role, _ = GraphUser.get_effective_role(org_member.id, graph.graph_id, test_db)
    assert role is None
    assert not GraphUser.user_has_access(org_member.id, graph.graph_id, test_db)

  def test_explicit_viewer_is_read_only(self, test_db, test_user, test_org):
    graph = _create_org_graph(test_db, test_org.id)
    viewer = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=test_org.id, user_id=viewer.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=viewer.id,
      graph_id=graph.graph_id,
      role=GraphRole.VIEWER,
      session=test_db,
    )

    role, implicit = GraphUser.get_effective_role(viewer.id, graph.graph_id, test_db)
    assert role == GraphRole.VIEWER
    assert implicit is False
    assert GraphUser.user_has_access(viewer.id, graph.graph_id, test_db)
    assert not GraphUser.user_has_write_access(viewer.id, graph.graph_id, test_db)
    assert not GraphUser.user_has_admin_access(viewer.id, graph.graph_id, test_db)

  def test_implicit_admin_wins_over_explicit_lower_role(
    self, test_db, test_user, test_org
  ):
    """An org owner's implicit admin is not narrowed by an explicit row."""
    graph = _create_org_graph(test_db, test_org.id)
    GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role=GraphRole.VIEWER,
      session=test_db,
    )

    role, implicit = GraphUser.get_effective_role(test_user.id, graph.graph_id, test_db)
    assert role == GraphRole.ADMIN
    assert implicit is True

  def test_subgraph_resolves_to_parent_for_implicit_access(
    self, test_db, test_user, test_org
  ):
    """Org owner's implicit admin covers subgraph ids via parent resolution."""
    graph = _create_org_graph(test_db, test_org.id)
    subgraph_id = f"{graph.graph_id}_dev"

    assert GraphUser.user_has_admin_access(test_user.id, subgraph_id, test_db)

  def test_unowned_graph_grants_nothing_implicitly(self, test_db, test_user):
    """A graph with no org linkage confers no implicit access."""
    graph = Graph.create(
      graph_id=_kg_id(),
      org_id=None,
      graph_name="Orphan Graph",
      graph_type="generic",
      session=test_db,
    )

    role, _ = GraphUser.get_effective_role(test_user.id, graph.graph_id, test_db)
    assert role is None


class TestOrphanedGraphRecovery:
  def test_org_owner_retains_access_after_creator_removed(
    self, test_db, test_user, test_org
  ):
    """The decision-3 scenario: creator leaves, org owner can still manage."""
    creator = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=test_org.id, user_id=creator.id, role=OrgRole.MEMBER, session=test_db
    )
    graph = _create_org_graph(test_db, test_org.id)
    row = GraphUser.create(
      user_id=creator.id,
      graph_id=graph.graph_id,
      role=GraphRole.ADMIN,
      session=test_db,
    )

    row.delete(test_db)

    assert not GraphUser.user_has_access(creator.id, graph.graph_id, test_db)
    assert GraphUser.user_has_admin_access(test_user.id, graph.graph_id, test_db)
