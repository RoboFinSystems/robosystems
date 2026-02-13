"""Tests for GraphDeprovisionService."""

import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.graph_tier import GraphTier
from robosystems.models.iam import Graph, Org, OrgType, OrgUser, User
from robosystems.models.iam.graph import GraphStatus
from robosystems.operations.graph.deprovision_service import (
  GraphDeprovisionService,
)

SERVICE_MODULE = "robosystems.operations.graph.deprovision_service"


@pytest.fixture
def service():
  return GraphDeprovisionService(environment="test")


@pytest.fixture
def test_user(db_session):
  uid = str(uuid.uuid4())[:8]
  user = User(
    id=f"test_user_{uid}",
    email=f"test+{uid}@example.com",
    name="Test User",
    password_hash="hash",
  )
  db_session.add(user)
  db_session.commit()
  db_session.refresh(user)
  return user


@pytest.fixture
def test_org(db_session, test_user):
  org = Org.create(name="Test Org", org_type=OrgType.PERSONAL, session=db_session)
  OrgUser.create(org_id=org.id, user_id=test_user.id, role="OWNER", session=db_session)
  return org


@pytest.fixture
def test_graph(db_session, test_org):
  uid = str(uuid.uuid4())[:8]
  graph = Graph.create(
    graph_id=f"kg_{uid}",
    graph_name="Test Graph",
    graph_type="entity",
    org_id=test_org.id,
    session=db_session,
    graph_tier=GraphTier.LADYBUG_STANDARD,
  )
  return graph


def _patch_infra():
  """Context manager that patches all infrastructure dependencies."""
  return (
    patch(
      f"{SERVICE_MODULE}.get_graph_client",
      new_callable=AsyncMock,
    ),
    patch(
      f"{SERVICE_MODULE}.LadybugAllocationManager",
    ),
    patch(
      f"{SERVICE_MODULE}.BackupManager",
    ),
    patch(
      f"{SERVICE_MODULE}.SubgraphService",
    ),
  )


class TestDeprovisionService:
  """Tests for GraphDeprovisionService.deprovision_graph."""

  @pytest.mark.asyncio
  async def test_deprovision_success(self, service, db_session, test_graph):
    """Full happy path: all steps succeed."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
      patch(
        "robosystems.operations.lbug.backup_manager.BackupManager"
      ) as mock_backup_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      mock_backup = AsyncMock()
      mock_backup_metadata = MagicMock()
      mock_backup_metadata.s3_key = "backups/test/final.dump"
      mock_backup.create_backup.return_value = mock_backup_metadata
      mock_backup_cls.return_value = mock_backup

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=True
      )

      assert result.status in ("success", "partial")
      assert result.previous_status == "active"
      assert result.backup_created is True
      assert result.backup_path == "backups/test/final.dump"
      assert result.database_deleted is True

      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value
      assert test_graph.deleted_at is not None

  @pytest.mark.asyncio
  async def test_deprovision_not_found(self, service, db_session):
    """Returns not_found for nonexistent graph."""
    result = await service.deprovision_graph("kg_nonexistent", db_session)

    assert result.status == "not_found"
    assert result.graph_id == "kg_nonexistent"

  @pytest.mark.asyncio
  async def test_deprovision_already_deprovisioned(
    self, service, db_session, test_graph
  ):
    """Returns early for already-deprovisioned graph."""
    test_graph.transition_status(GraphStatus.DEPROVISIONED, db_session)

    result = await service.deprovision_graph(test_graph.graph_id, db_session)

    assert result.status == "already_deprovisioned"

  @pytest.mark.asyncio
  async def test_deprovision_rejects_shared_repository(
    self, service, db_session, test_graph
  ):
    """Shared repositories cannot be deprovisioned."""
    test_graph.is_repository = True
    test_graph.repository_type = "sec"
    db_session.commit()

    result = await service.deprovision_graph(test_graph.graph_id, db_session)

    assert result.status == "rejected"
    assert "shared repository" in result.errors[0]
    # Graph should NOT be modified
    db_session.refresh(test_graph)
    assert test_graph.status != "deprovisioned"
    assert test_graph.deleted_at is None

  @pytest.mark.asyncio
  async def test_deprovision_skip_backup(self, service, db_session, test_graph):
    """Backup is not created when create_backup=False."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      assert result.backup_created is False
      assert result.backup_path is None
      assert result.database_deleted is True

      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value

  @pytest.mark.asyncio
  async def test_deprovision_backup_failure_continues(
    self, service, db_session, test_graph
  ):
    """Backup failure does not block deprovisioning."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
      patch(
        "robosystems.operations.lbug.backup_manager.BackupManager"
      ) as mock_backup_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      mock_backup = AsyncMock()
      mock_backup.create_backup.side_effect = Exception("S3 unreachable")
      mock_backup_cls.return_value = mock_backup

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=True
      )

      assert result.status == "partial"
      assert result.backup_created is False
      assert result.database_deleted is True
      assert any("Backup creation failed" in e for e in result.errors)

      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value

  @pytest.mark.asyncio
  async def test_deprovision_database_deletion_failure_continues(
    self, service, db_session, test_graph
  ):
    """Database deletion failure does not block deprovisioning."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_client.delete_database.side_effect = Exception("Connection refused")
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      assert result.status == "partial"
      assert result.database_deleted is False
      assert any("Database deletion failed" in e for e in result.errors)

      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value

  @pytest.mark.asyncio
  async def test_deprovision_registry_deallocation_failure_continues(
    self, service, db_session, test_graph
  ):
    """Registry deallocation failure does not block deprovisioning."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc.deallocate_database.side_effect = Exception("DynamoDB error")
      mock_alloc_cls.return_value = mock_alloc

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      assert result.status == "partial"
      assert result.registry_deallocated is False
      assert any("Registry deallocation failed" in e for e in result.errors)

      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value

  @pytest.mark.asyncio
  async def test_deprovision_with_subgraphs(self, service, db_session, test_graph):
    """Subgraphs are deleted before parent."""
    # Create subgraph records
    sub1 = Graph(
      graph_id=f"{test_graph.graph_id}_dev",
      graph_name="Dev Subgraph",
      graph_type="entity",
      org_id=test_graph.org_id,
      graph_tier=test_graph.graph_tier,
      parent_graph_id=test_graph.graph_id,
      is_subgraph=True,
      subgraph_index=0,
      subgraph_name="dev",
      status=GraphStatus.ACTIVE.value,
    )
    sub2 = Graph(
      graph_id=f"{test_graph.graph_id}_staging",
      graph_name="Staging Subgraph",
      graph_type="entity",
      org_id=test_graph.org_id,
      graph_tier=test_graph.graph_tier,
      parent_graph_id=test_graph.graph_id,
      is_subgraph=True,
      subgraph_index=1,
      subgraph_name="staging",
      status=GraphStatus.ACTIVE.value,
    )
    db_session.add_all([sub1, sub2])
    db_session.commit()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
      patch("robosystems.operations.graph.subgraph_service.LadybugAllocationManager"),
      patch(
        "robosystems.operations.graph.subgraph_service.SubgraphService"
      ) as mock_sub_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      mock_sub_service = AsyncMock()
      mock_sub_cls.return_value = mock_sub_service

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      assert result.subgraphs_deleted == 2
      assert mock_sub_service.delete_subgraph_database.call_count == 2

      # Subgraphs should also be deprovisioned
      db_session.refresh(sub1)
      db_session.refresh(sub2)
      assert sub1.status == GraphStatus.DEPROVISIONED.value
      assert sub2.status == GraphStatus.DEPROVISIONED.value
      assert sub1.deleted_at is not None
      assert sub2.deleted_at is not None

  @pytest.mark.asyncio
  async def test_deprovision_cleans_pg_records(
    self, service, db_session, test_graph, test_user
  ):
    """Verify PG records are cleaned up."""
    from robosystems.models.iam.graph_credits import GraphCredits
    from robosystems.models.iam.graph_user import GraphUser

    # Create associated records
    graph_user = GraphUser(
      graph_id=test_graph.graph_id,
      user_id=test_user.id,
      role="owner",
    )
    db_session.add(graph_user)

    credits = GraphCredits(
      graph_id=test_graph.graph_id,
      user_id=test_user.id,
      billing_admin_id=test_user.id,
    )
    db_session.add(credits)
    db_session.commit()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      assert result.records_cleaned is True

      # Records should be deleted
      remaining_users = (
        db_session.query(GraphUser)
        .filter(GraphUser.graph_id == test_graph.graph_id)
        .count()
      )
      assert remaining_users == 0

      remaining_credits = (
        db_session.query(GraphCredits)
        .filter(GraphCredits.graph_id == test_graph.graph_id)
        .count()
      )
      assert remaining_credits == 0

  @pytest.mark.asyncio
  async def test_deprovision_updates_subscription_metadata(
    self, service, db_session, test_graph, test_org
  ):
    """Verify subscription metadata is updated with deprovisioning info."""
    from robosystems.models.billing.subscription import BillingSubscription

    sub = BillingSubscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id=test_graph.graph_id,
      plan_name="ladybug-standard",
      base_price_cents=4900,
      status="canceled",
      subscription_metadata={},
    )
    db_session.add(sub)
    db_session.commit()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      db_session.refresh(sub)
      metadata = sub.subscription_metadata
      assert "deprovisioned_at" in metadata
      assert "backup_hosting_expires_at" in metadata

  @pytest.mark.asyncio
  async def test_deprovision_sets_deleted_at(self, service, db_session, test_graph):
    """Verify Graph.deleted_at is set."""
    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      db_session.refresh(test_graph)
      assert test_graph.deleted_at is not None
      assert isinstance(test_graph.deleted_at, datetime)
