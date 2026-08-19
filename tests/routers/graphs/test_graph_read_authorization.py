"""Reads that only separate at the second org member.

With one member per org, org membership and graph access are the same set, so
a read that authorizes on the former looks correct. The second member is the
first user for whom they differ: a plain org member without a grant on a graph.
Each test here puts that member at a graph-scoped read and asserts the graph
role, not the org roster, decides — and that a subgraph's visibility follows
the parent grant rather than a row of its own.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from robosystems.models.core import (
  BillingSubscription,
  Graph,
  GraphRole,
  GraphUser,
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  User,
)
from robosystems.models.core.billing import BillingCustomer

pytestmark = pytest.mark.asyncio


def _create_user(session, password_hash: str) -> User:
  suffix = uuid4().hex[:8]
  user = User(
    email=f"second-member+{suffix}@example.com",
    name=f"Second Member {suffix}",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


class _NoClose:
  """The service closes the session it pulls from ``get_db_session``; the test
  needs the fixture session to survive that."""

  def __init__(self, session):
    self._session = session

  def __getattr__(self, name):
    return getattr(self._session, name)

  def close(self) -> None:
    return None


def _team_org_with_graph(session, owner_id: str) -> tuple[Org, Graph]:
  org = Org.create(
    name=f"Team {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=session
  )
  OrgUser.create(org_id=org.id, user_id=owner_id, role=OrgRole.OWNER, session=session)
  graph = Graph.create(
    graph_id=f"kg{uuid4().hex[:16]}",
    org_id=org.id,
    graph_name="Team Graph",
    graph_type="generic",
    session=session,
  )
  return org, graph


class TestGraphSubscriptionRead:
  """``GET /v1/graphs/{graph_id}/subscriptions`` — the graph's tier and
  billing period. Reads on graph access, not org membership."""

  def _subscribed_graph_as_plain_member(self, test_db, test_user, test_org):
    """A team org where ``test_user`` is a plain member, owning a graph with a
    subscription; ``test_user`` holds no grant on the graph."""
    owner = _create_user(test_db, test_user.password_hash)
    org, graph = _team_org_with_graph(test_db, owner.id)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    # The endpoint resolves the caller's billing customer through their first
    # org membership, whichever that is.
    BillingCustomer.get_or_create(test_org.id, test_db)
    BillingCustomer.get_or_create(org.id, test_db)
    BillingSubscription.create_subscription(
      org_id=org.id,
      resource_type="graph",
      resource_id=graph.graph_id,
      plan_name="ladybug-standard",
      base_price_cents=4900,
      session=test_db,
    )
    return org, graph

  async def test_org_member_without_grant_is_refused(
    self, async_client, test_db, test_user, test_org
  ):
    _org, graph = self._subscribed_graph_as_plain_member(test_db, test_user, test_org)

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/subscriptions")

    assert response.status_code == 403

  async def test_explicit_grant_admits(
    self, async_client, test_db, test_user, test_org
  ):
    _org, graph = self._subscribed_graph_as_plain_member(test_db, test_user, test_org)
    GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role=GraphRole.VIEWER,
      session=test_db,
    )

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/subscriptions")

    assert response.status_code == 200
    assert response.json()["resource_id"] == graph.graph_id

  async def test_org_admin_is_admitted_implicitly(
    self, async_client, test_db, test_user, test_org
  ):
    org, graph = self._subscribed_graph_as_plain_member(test_db, test_user, test_org)
    membership = OrgUser.get_by_org_and_user(org.id, test_user.id, test_db)
    assert membership is not None
    membership.role = OrgRole.ADMIN
    test_db.commit()

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/subscriptions")

    assert response.status_code == 200


class TestSubgraphAccessFollowsTheParentGrant:
  """A subgraph has no ``GraphUser`` row of its own: creation writes none, the
  graph list derives subgraphs from the parent grant, and removing a member
  from the parent therefore leaves nothing behind on the subgraph."""

  async def test_creation_writes_no_subgraph_grant_row(
    self, test_db, test_user, test_org
  ):
    from robosystems.operations.graph.subgraph_service import SubgraphService

    parent = Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=test_org.id,
      graph_name="Parent",
      graph_type="generic",
      session=test_db,
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=parent.graph_id,
      role=GraphRole.ADMIN,
      session=test_db,
    )

    service = SubgraphService()
    with (
      patch.object(
        service,
        "create_subgraph_database",
        new=AsyncMock(return_value={"status": "created", "instance_id": "i-test"}),
      ),
      patch(
        "robosystems.database.get_db_session",
        return_value=iter([_NoClose(test_db)]),
      ),
    ):
      result = await service.create_subgraph(
        parent_graph=parent, user=test_user, name="dev", subgraph_type="empty"
      )

    subgraph_id = result["graph_id"]
    assert Graph.get_by_id(subgraph_id, test_db) is not None
    assert (
      test_db.query(GraphUser).filter(GraphUser.graph_id == subgraph_id).count() == 0
    )
    # ...and the creator still reaches it, at the parent's role.
    assert GraphUser.user_has_admin_access(test_user.id, subgraph_id, test_db)

  async def test_graph_list_shows_subgraphs_of_every_granted_parent(
    self, async_client, test_db, test_user
  ):
    """Not only the creator: any holder of a parent grant sees the subgraph in
    the list, at the parent's role — the same answer authorization gives."""
    owner = _create_user(test_db, test_user.password_hash)
    org, parent = _team_org_with_graph(test_db, owner.id)
    subgraph = Graph.create(
      graph_id=f"{parent.graph_id}_dev",
      org_id=org.id,
      graph_name="dev",
      graph_type="generic",
      session=test_db,
      parent_graph_id=parent.graph_id,
      is_subgraph=True,
      subgraph_name="dev",
      subgraph_index=1,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=parent.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )

    with patch("robosystems.routers.graphs.main.session", test_db):
      response = await async_client.get("/v1/graphs")

    assert response.status_code == 200
    by_id = {g["graphId"]: g for g in response.json()["graphs"]}
    assert by_id[parent.graph_id]["role"] == "member"
    assert by_id[subgraph.graph_id]["isSubgraph"] is True
    assert by_id[subgraph.graph_id]["parentGraphId"] == parent.graph_id
    assert by_id[subgraph.graph_id]["role"] == "member"

  async def test_graph_list_ignores_a_legacy_subgraph_row_without_duplicating(
    self, async_client, test_db, test_user, test_org
  ):
    """A pre-existing row on the subgraph id (from before creation stopped
    writing them) neither adds a second entry nor grants a different role."""
    parent = Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=test_org.id,
      graph_name="Parent",
      graph_type="generic",
      session=test_db,
    )
    subgraph = Graph.create(
      graph_id=f"{parent.graph_id}_dev",
      org_id=test_org.id,
      graph_name="dev",
      graph_type="generic",
      session=test_db,
      parent_graph_id=parent.graph_id,
      is_subgraph=True,
      subgraph_name="dev",
      subgraph_index=1,
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=parent.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=subgraph.graph_id,
      role=GraphRole.ADMIN,
      session=test_db,
    )

    with patch("robosystems.routers.graphs.main.session", test_db):
      response = await async_client.get("/v1/graphs")

    assert response.status_code == 200
    ids = [g["graphId"] for g in response.json()["graphs"]]
    assert ids.count(subgraph.graph_id) == 1
    entry = next(
      g for g in response.json()["graphs"] if g["graphId"] == subgraph.graph_id
    )
    assert entry["role"] == "member"

  async def test_org_graph_listing_shows_subgraphs_of_granted_parents(
    self, async_client, test_db, test_user
  ):
    owner = _create_user(test_db, test_user.password_hash)
    org, parent = _team_org_with_graph(test_db, owner.id)
    Graph.create(
      graph_id=f"{parent.graph_id}_dev",
      org_id=org.id,
      graph_name="dev",
      graph_type="generic",
      session=test_db,
      parent_graph_id=parent.graph_id,
      is_subgraph=True,
      subgraph_name="dev",
      subgraph_index=1,
    )
    hidden = Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=org.id,
      graph_name="Not Granted",
      graph_type="generic",
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=parent.graph_id,
      role=GraphRole.VIEWER,
      session=test_db,
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/graphs")

    assert response.status_code == 200
    ids = {g["graph_id"] for g in response.json()}
    assert ids == {parent.graph_id, f"{parent.graph_id}_dev"}
    assert hidden.graph_id not in ids


class TestBackupDownloadRequiresAdmin:
  """A dedicated graph's backup is the whole database; minting a download URL
  for it is an admin action, like creating or restoring one."""

  def _graph_with_grant(self, test_db, test_user, role: GraphRole) -> Graph:
    owner = _create_user(test_db, test_user.password_hash)
    org, graph = _team_org_with_graph(test_db, owner.id)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=test_user.id, graph_id=graph.graph_id, role=role, session=test_db
    )
    return graph

  async def test_viewer_cannot_mint_a_download_url(
    self, async_client, test_db, test_user
  ):
    graph = self._graph_with_grant(test_db, test_user, GraphRole.VIEWER)

    response = await async_client.get(
      f"/v1/graphs/{graph.graph_id}/backups/bk_x/download"
    )

    assert response.status_code == 403
    assert "Admin access required" in response.json()["detail"]

  async def test_admin_passes_the_role_gate(self, async_client, test_db, test_user):
    """The gate is the only thing under test: past it, the request reaches
    the backup lookup, which is stubbed to report the backup missing."""
    graph = self._graph_with_grant(test_db, test_user, GraphRole.ADMIN)
    manager = MagicMock()
    manager.get_backup_download_url = AsyncMock(return_value=None)

    with (
      patch(
        "robosystems.routers.graphs.backups.download.get_backup_manager",
        return_value=manager,
      ),
      patch(
        "robosystems.routers.graphs.backups.download.DownloadRateLimiter"
        ".check_graph_download_limit",
        new=AsyncMock(return_value=(True, 1, datetime.now(UTC))),
      ),
    ):
      response = await async_client.get(
        f"/v1/graphs/{graph.graph_id}/backups/bk_x/download"
      )

    assert response.status_code == 404
    manager.get_backup_download_url.assert_awaited_once()

  async def test_admin_can_download_from_a_deprovisioned_graph(
    self, async_client, test_db, test_user
  ):
    """The export grace period: after teardown, an org admin can still reach a
    torn-down graph's backups. The real verify_admin_access(allow_deprovisioned)
    runs here — the dependency override does not cover it."""
    from robosystems.models.core import GraphStatus

    graph = self._graph_with_grant(test_db, test_user, GraphRole.ADMIN)
    graph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    manager = MagicMock()
    manager.get_backup_download_url = AsyncMock(return_value=None)
    with (
      patch(
        "robosystems.routers.graphs.backups.download.get_backup_manager",
        return_value=manager,
      ),
      patch(
        "robosystems.routers.graphs.backups.download.DownloadRateLimiter"
        ".check_graph_download_limit",
        new=AsyncMock(return_value=(True, 1, datetime.now(UTC))),
      ),
    ):
      response = await async_client.get(
        f"/v1/graphs/{graph.graph_id}/backups/bk_x/download"
      )

    # Past the gate (reaches the stubbed-missing backup), not 403'd out.
    assert response.status_code == 404
    manager.get_backup_download_url.assert_awaited_once()

  async def test_viewer_still_denied_on_a_deprovisioned_graph(
    self, async_client, test_db, test_user
  ):
    """The grace period is admin-only: allow_deprovisioned relaxes the gone-graph
    denial, it does not grant a viewer admin."""
    from robosystems.models.core import GraphStatus

    graph = self._graph_with_grant(test_db, test_user, GraphRole.VIEWER)
    graph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    response = await async_client.get(
      f"/v1/graphs/{graph.graph_id}/backups/bk_x/download"
    )

    assert response.status_code == 403
    assert "Admin access required" in response.json()["detail"]
