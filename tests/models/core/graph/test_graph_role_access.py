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


class TestGoneGraphsGrantNoRole:
  """Nobody holds a role on a graph that is gone.

  Teardown stamps ``deleted_at`` first and flips ``status`` last; org
  OWNER/ADMIN hold implicit admin through ``Graph.org_id`` with no GraphUser
  row to delete. Both paths used to outlive the graph — the resolver is the
  one place every surface authorizes through, so it denies here.
  """

  def test_deprovisioned_graph_denies_explicit_admin(
    self, test_db, test_user, test_org
  ):
    from robosystems.models.core import GraphStatus

    graph = _create_org_graph(test_db, test_org.id)
    grantee = _create_user(test_db, test_user.password_hash)
    GraphUser.create(
      user_id=grantee.id, graph_id=graph.graph_id, role=GraphRole.ADMIN, session=test_db
    )
    assert GraphUser.user_has_admin_access(grantee.id, graph.graph_id, test_db)

    graph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    role, _ = GraphUser.get_effective_role(grantee.id, graph.graph_id, test_db)
    assert role is None
    assert not GraphUser.user_has_access(grantee.id, graph.graph_id, test_db)

  def test_deleted_at_stamp_denies_org_owner(self, test_db, test_user, test_org):
    """The implicit grant does not survive the soft-delete stamp — even while
    ``status`` still reads suspended/active mid-teardown."""
    from datetime import UTC, datetime

    graph = _create_org_graph(test_db, test_org.id)
    assert GraphUser.user_has_admin_access(test_user.id, graph.graph_id, test_db)

    graph.deleted_at = datetime.now(UTC)
    test_db.commit()

    role, implicit = GraphUser.get_effective_role(test_user.id, graph.graph_id, test_db)
    assert role is None
    assert implicit is False
    assert not GraphUser.user_has_admin_access(test_user.id, graph.graph_id, test_db)

  def test_subgraph_of_gone_parent_denies(self, test_db, test_user, test_org):
    from robosystems.models.core import GraphStatus

    graph = _create_org_graph(test_db, test_org.id)
    subgraph_id = f"{graph.graph_id}_dev"
    assert GraphUser.user_has_admin_access(test_user.id, subgraph_id, test_db)

    graph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    assert not GraphUser.user_has_access(test_user.id, subgraph_id, test_db)

  def test_gone_subgraph_row_denies_while_parent_lives(
    self, test_db, test_user, test_org
  ):
    """A subgraph row that exists is held to the same standard as its parent."""
    from robosystems.models.core import GraphStatus

    graph = _create_org_graph(test_db, test_org.id)
    subgraph = Graph.create(
      graph_id=f"{graph.graph_id}_dev",
      org_id=test_org.id,
      graph_name="dev",
      graph_type="generic",
      session=test_db,
      is_subgraph=True,
      parent_graph_id=graph.graph_id,
      subgraph_index=1,
      subgraph_name="dev",
    )
    assert GraphUser.user_has_admin_access(test_user.id, subgraph.graph_id, test_db)

    subgraph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    assert not GraphUser.user_has_access(test_user.id, subgraph.graph_id, test_db)
    # The parent itself is untouched.
    assert GraphUser.user_has_admin_access(test_user.id, graph.graph_id, test_db)

  def test_missing_graph_row_denies(self, test_db, test_user):
    role, _ = GraphUser.get_effective_role(test_user.id, _kg_id(), test_db)
    assert role is None


class TestExportGracePeriod:
  """``allow_deprovisioned=True`` — the one sanctioned relaxation of the
  gone-graph denial, for the backup export path only. Org OWNER/ADMIN keep
  implicit admin so a departing customer can export during the grace period;
  everyone else, and every other caller, still resolves to no role.
  """

  def test_org_owner_keeps_implicit_admin_on_a_deprovisioned_graph(
    self, test_db, test_user, test_org
  ):
    from robosystems.models.core import GraphStatus

    graph = _create_org_graph(test_db, test_org.id)
    graph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    # Default: denied.
    assert GraphUser.get_effective_role(test_user.id, graph.graph_id, test_db) == (
      None,
      False,
    )
    # Grace period: implicit admin restored.
    role, implicit = GraphUser.get_effective_role(
      test_user.id, graph.graph_id, test_db, allow_deprovisioned=True
    )
    assert role == GraphRole.ADMIN
    assert implicit is True
    assert GraphUser.user_has_admin_access(
      test_user.id, graph.graph_id, test_db, allow_deprovisioned=True
    )

  def test_grace_period_survives_the_deleted_at_stamp(
    self, test_db, test_user, test_org
  ):
    from datetime import UTC, datetime

    graph = _create_org_graph(test_db, test_org.id)
    graph.deleted_at = datetime.now(UTC)
    test_db.commit()

    assert not GraphUser.user_has_access(test_user.id, graph.graph_id, test_db)
    assert GraphUser.user_has_admin_access(
      test_user.id, graph.graph_id, test_db, allow_deprovisioned=True
    )

  def test_a_non_member_stranger_still_gets_nothing(self, test_db, test_user, test_org):
    """The relaxation is the org's implicit grant, not open access — a user
    with no role and no org membership resolves to None even with the flag."""
    from robosystems.models.core import GraphStatus

    graph = _create_org_graph(test_db, test_org.id)
    stranger = _create_user(test_db, test_user.password_hash)
    graph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    role, _ = GraphUser.get_effective_role(
      stranger.id, graph.graph_id, test_db, allow_deprovisioned=True
    )
    assert role is None

  def test_a_missing_graph_denies_even_with_the_flag(self, test_db, test_user):
    """The flag relaxes gone, not absent — a graph that never existed still
    resolves to no role."""
    role, _ = GraphUser.get_effective_role(
      test_user.id, _kg_id(), test_db, allow_deprovisioned=True
    )
    assert role is None
