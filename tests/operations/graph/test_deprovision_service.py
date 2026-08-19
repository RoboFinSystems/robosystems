"""Tests for GraphDeprovisionService."""

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.graph_tier import GraphTier
from robosystems.models.core import Graph, Org, OrgType, OrgUser, User
from robosystems.models.core.graph import GraphStatus
from robosystems.operations.graph.deprovision_service import (
  GraphDeprovisionService,
)

SERVICE_MODULE = "robosystems.operations.graph.deprovision_service"


def _final_backup_metadata(s3_key: str = "backups/test/final.lbug.zip"):
  """A BackupMetadata stand-in with values a DB column will actually accept.

  A bare MagicMock passes every attribute access and then fails at flush, which
  the best-effort backup step swallows into result.errors — so the registration
  would look tested while never running.
  """
  metadata = MagicMock()
  metadata.s3_key = s3_key
  metadata.s3_metadata_key = f"{s3_key}.metadata.json"
  metadata.original_size = 2048
  metadata.compressed_size = 512
  metadata.compression_ratio = 0.75
  metadata.node_count = 12
  metadata.relationship_count = 7
  metadata.database_version = "1.0"
  metadata.backup_duration_seconds = 1.5
  metadata.checksum = "abc123"
  metadata.backup_format = "full_dump"
  metadata.timestamp = datetime(2026, 8, 15, 12, 0, 0)
  metadata.memory = None
  metadata.payload_delta = None
  return metadata


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


@pytest.fixture(autouse=True)
def _stub_search_purge():
  """Deprovision runs a best-effort OpenSearch purge, and SEMANTIC_SEARCH_ENABLED
  is true under test — stub the client so these tests never reach a real cluster."""
  with patch("robosystems.operations.search.client.OpenSearchClient"):
    yield


@pytest.fixture(autouse=True)
def stub_bundle_purge():
  """Deprovision purges the graph's report artifacts from object storage — stub
  the S3 client so these tests never reach a real bucket. Yields the class mock
  so the tests that care about the purge can drive it."""
  with patch("robosystems.operations.aws.s3.S3Client") as cls:
    client = MagicMock()
    client.iter_object_keys.return_value = []
    client.delete_object.return_value = True
    cls.return_value = client
    yield client


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
        "robosystems.operations.graph.engine.backup_manager.BackupManager"
      ) as mock_backup_cls,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      mock_backup = AsyncMock()
      mock_backup.s3_adapter.bucket_name = "test-backup-bucket"
      mock_backup_metadata = _final_backup_metadata("backups/test/final.dump")
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
  async def test_final_backup_is_skipped_when_this_teardown_already_made_one(
    self, service, db_session, test_graph
  ):
    """On a stranded-graph retry the sensor re-selects the same graph every
    cycle; the final backup must not be re-dumped each time. A completed FULL
    backup created since deleted_at (this teardown's own) is reused."""
    from datetime import UTC, datetime, timedelta

    from robosystems.models.core.graph.graph_backup import (
      BackupStatus,
      BackupType,
      GraphBackup,
    )
    from robosystems.operations.graph.deprovision_service import DeprovisionResult

    test_graph.deleted_at = datetime.now(UTC) - timedelta(minutes=10)
    db_session.commit()

    prior = GraphBackup.create(
      graph_id=test_graph.graph_id,
      database_name=test_graph.graph_id,
      backup_type=BackupType.FULL.value,
      s3_bucket="test-bucket",
      s3_key="backups/prior-final.lbug.zip",
      session=db_session,
    )
    prior.status = BackupStatus.COMPLETED.value
    prior.created_at = datetime.now(UTC) - timedelta(minutes=5)  # after deleted_at
    db_session.commit()

    result = DeprovisionResult(graph_id=test_graph.graph_id, status="success")

    with patch(
      "robosystems.operations.graph.engine.backup_manager.BackupManager"
    ) as mock_backup_cls:
      mock_backup = AsyncMock()
      mock_backup_cls.return_value = mock_backup

      await service._create_final_backup(test_graph, db_session, result)

    # No fresh S3 dump; the existing final backup is reused.
    mock_backup.create_backup.assert_not_called()
    assert result.backup_created is True
    assert result.backup_path == "backups/prior-final.lbug.zip"

  @pytest.mark.asyncio
  async def test_final_backup_ignores_a_backup_older_than_this_teardown(
    self, service, db_session, test_graph
  ):
    """A nightly/on-demand backup from before deleted_at is NOT the final
    snapshot — the first teardown still takes a fresh dump."""
    from datetime import UTC, datetime, timedelta

    from robosystems.models.core.graph.graph_backup import (
      BackupStatus,
      BackupType,
      GraphBackup,
    )
    from robosystems.operations.graph.deprovision_service import DeprovisionResult

    test_graph.deleted_at = datetime.now(UTC)
    db_session.commit()

    nightly = GraphBackup.create(
      graph_id=test_graph.graph_id,
      database_name=test_graph.graph_id,
      backup_type=BackupType.FULL.value,
      s3_bucket="test-bucket",
      s3_key="backups/nightly.lbug.zip",
      session=db_session,
    )
    nightly.status = BackupStatus.COMPLETED.value
    nightly.created_at = datetime.now(UTC) - timedelta(days=1)  # before deleted_at
    db_session.commit()

    result = DeprovisionResult(graph_id=test_graph.graph_id, status="success")

    with patch(
      "robosystems.operations.graph.engine.backup_manager.BackupManager"
    ) as mock_backup_cls:
      mock_backup = AsyncMock()
      mock_backup.s3_adapter.bucket_name = "test-bucket"
      mock_backup.create_backup.return_value = _final_backup_metadata()
      mock_backup_cls.return_value = mock_backup

      await service._create_final_backup(test_graph, db_session, result)

    mock_backup.create_backup.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_final_backup_is_registered_for_retrieval(
    self, service, db_session, test_graph
  ):
    """The final backup must be reachable, not just present in S3.

    Both the listing and the download URL resolve through GraphBackup, so an
    unregistered final backup is invisible to the customer whose export grace
    period it exists to serve.
    """
    from robosystems.models.core.graph.graph_backup import (
      BackupInitiator,
      BackupStatus,
      GraphBackup,
    )

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
      patch(
        "robosystems.operations.graph.engine.backup_manager.BackupManager"
      ) as mock_backup_cls,
    ):
      mock_get_client.return_value = AsyncMock()
      mock_alloc_cls.return_value = AsyncMock()

      mock_backup = AsyncMock()
      mock_backup.s3_adapter.bucket_name = "test-backup-bucket"
      mock_backup.create_backup.return_value = _final_backup_metadata()
      mock_backup_cls.return_value = mock_backup

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=True
      )

      assert result.backup_created is True
      assert result.backup_registered is True, result.errors

      row = (
        db_session.query(GraphBackup)
        .filter(GraphBackup.graph_id == test_graph.graph_id)
        .one()
      )
      assert row.status == BackupStatus.COMPLETED.value
      assert row.s3_key == "backups/test/final.lbug.zip"
      assert row.s3_bucket == "test-backup-bucket"

      # Not SYSTEM: that initiator is filtered out of the customer listing,
      # which would leave the export invisible for a second reason.
      assert row.initiated_by == BackupInitiator.FINAL.value
      assert row.initiated_by != BackupInitiator.SYSTEM.value

      # Expiry tracks the tier's backup hosting window, not the shorter export
      # window — a 30-day expiry here would delete the archive early.
      assert row.expires_at is not None
      # The column is naive; the value written was UTC. Compare like for like.
      expires_at = row.expires_at
      if expires_at.tzinfo is not None:
        expires_at = expires_at.astimezone(UTC).replace(tzinfo=None)
      days = (expires_at - datetime.now(UTC).replace(tzinfo=None)).days
      assert 88 <= days <= 90, days

  @pytest.mark.asyncio
  async def test_failed_backup_registration_does_not_wedge_the_teardown(
    self, service, db_session, test_graph, test_user
  ):
    """A failed registration must not take the rest of the teardown with it.

    Registering the backup is the first database write in deprovisioning, and
    on PostgreSQL a failed statement aborts the entire transaction — so
    without a SAVEPOINT every later step would fail against a dead session,
    and in the batch job every graph after this one in the same run would too.
    Here the write fails on a NOT NULL bucket; steps 2-7 must still complete.
    """
    from robosystems.models.core.document import Document
    from robosystems.models.core.graph.graph_backup import GraphBackup

    Document.create(
      graph_id=test_graph.graph_id,
      user_id=test_user.id,
      title="Survives the failed registration",
      content="confidential",
      session=db_session,
    )

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
      patch(
        "robosystems.operations.graph.engine.backup_manager.BackupManager"
      ) as mock_backup_cls,
    ):
      mock_get_client.return_value = AsyncMock()
      mock_alloc_cls.return_value = AsyncMock()

      mock_backup = AsyncMock()
      # s3_bucket is NOT NULL, so the insert fails inside the savepoint.
      mock_backup.s3_adapter.bucket_name = None
      mock_backup.create_backup.return_value = _final_backup_metadata()
      mock_backup_cls.return_value = mock_backup

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=True
      )

      assert result.backup_registered is False
      assert result.errors

      # The teardown continued: PG cleanup ran and the graph reached its
      # terminal state. Both would fail on a transaction poisoned at step 1.
      assert result.records_cleaned is True
      assert result.documents_deleted >= 1
      assert (
        db_session.query(Document)
        .filter(Document.graph_id == test_graph.graph_id)
        .count()
        == 0
      )
      assert (
        db_session.query(GraphBackup)
        .filter(GraphBackup.graph_id == test_graph.graph_id)
        .count()
        == 0
      )

      db_session.refresh(test_graph)
      assert test_graph.status == GraphStatus.DEPROVISIONED.value

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
    """An already-deprovisioned graph re-runs only the idempotent data-disposal
    steps — a partial teardown used to leave a ghost tenant schema behind with
    no way to retry — and touches nothing else."""
    from unittest.mock import patch

    test_graph.transition_status(GraphStatus.DEPROVISIONED, db_session)

    with (
      patch(
        "robosystems.db.extensions.drop_tenant_schema", return_value=True
      ) as drop_schema,
      patch.object(service, "_purge_search_index") as purge_search,
      patch.object(service, "_purge_report_bundles") as purge_bundles,
      patch.object(service, "_delete_database") as delete_db,
      patch.object(service, "_deallocate_registry") as dealloc,
    ):
      result = await service.deprovision_graph(test_graph.graph_id, db_session)

    assert result.status == "already_deprovisioned"
    assert result.extensions_schema_dropped is True
    assert "residual data disposal re-run" in result.message
    drop_schema.assert_called_once_with(test_graph.graph_id)
    purge_search.assert_called_once()
    purge_bundles.assert_called_once()
    delete_db.assert_not_called()
    dealloc.assert_not_called()
    assert test_graph.status == GraphStatus.DEPROVISIONED.value

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
        "robosystems.operations.graph.engine.backup_manager.BackupManager"
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
  async def test_deprovision_database_deletion_failure_leaves_graph_for_retry(
    self, service, db_session, test_graph
  ):
    """A database-delete failure must NOT free the registry or flip the status.

    The `.lbug` is still on the instance; freeing the registry slot would
    strand it (the allocator reads the instance as empty while the graph_api
    counts the on-disk file against max_databases). So the registry entry and
    the status are left intact — deleted_at is already stamped, so the graph is
    closed to callers — and the teardown sensor re-selects the stranded graph
    to retry the whole sequence.
    """
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
      assert result.registry_deallocated is False
      assert any("Database deletion failed" in e for e in result.errors)

      # Registry was NOT freed, so the .lbug is not stranded on an "empty"
      # instance.
      mock_alloc.deallocate_database.assert_not_called()

      # Status left un-deprovisioned, deleted_at still set → sensor retries it.
      db_session.refresh(test_graph)
      assert test_graph.status != GraphStatus.DEPROVISIONED.value
      assert test_graph.deleted_at is not None

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
    from robosystems.models.core.connection.connection import Connection
    from robosystems.models.core.connection.connection_credentials import (
      ConnectionCredentials,
    )
    from robosystems.models.core.document import Document
    from robosystems.models.core.graph.graph_credits import GraphCredits
    from robosystems.models.core.graph.graph_user import GraphUser

    # Create associated records
    graph_user = GraphUser(
      graph_id=test_graph.graph_id,
      user_id=test_user.id,
      role="admin",
    )
    db_session.add(graph_user)

    credits = GraphCredits(
      graph_id=test_graph.graph_id,
      user_id=test_user.id,
      billing_admin_id=test_user.id,
    )
    db_session.add(credits)
    db_session.commit()

    Document.create(
      graph_id=test_graph.graph_id,
      user_id=test_user.id,
      title="A departed tenant's memo",
      content="confidential",
      session=db_session,
    )

    # An org ADMIN with no GraphUser row: implicit graph admin through the
    # org, so no membership row is deleted for them — their cached decision
    # has to be dropped by name.
    org_admin = User(
      id=f"org_admin_{uuid.uuid4().hex[:8]}",
      email=f"admin+{uuid.uuid4().hex[:8]}@example.com",
      name="Org Admin",
      password_hash="hash",
    )
    db_session.add(org_admin)
    db_session.commit()
    OrgUser.create(
      org_id=test_graph.org_id, user_id=org_admin.id, role="ADMIN", session=db_session
    )

    connection = Connection.create(
      graph_id=test_graph.graph_id,
      user_id=test_user.id,
      provider="quickbooks",
      session=db_session,
      realm_id="realm-departed",
    )
    # Built directly rather than through create(): that path encrypts, which
    # needs a real Fernet key, and what is under test is that the row is
    # removed — not how its contents were sealed.
    db_session.add(
      ConnectionCredentials(
        connection_id=connection.id,
        provider="quickbooks",
        user_id=test_user.id,
        encrypted_credentials="ciphertext-standing-in-for-a-refresh-token",
      )
    )
    db_session.commit()
    connection_id = connection.id

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ) as mock_get_client,
      patch(
        "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
      ) as mock_alloc_cls,
      patch("robosystems.middleware.auth.cache.api_key_cache") as cache,
    ):
      mock_client = AsyncMock()
      mock_get_client.return_value = mock_client

      mock_alloc = AsyncMock()
      mock_alloc_cls.return_value = mock_alloc

      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

      assert result.records_cleaned is True

      # Members' cached access decisions go with their rows, so a warm entry
      # cannot carry a request onto the half-dropped graph.
      cache.invalidate_user_jwt_graph_access.assert_any_call(
        test_user.id, test_graph.graph_id
      )
      # ...and so do the org OWNER/ADMIN's, who hold implicit admin with no
      # row of their own to delete.
      cache.invalidate_user_jwt_graph_access.assert_any_call(
        org_admin.id, test_graph.graph_id
      )
      cache.invalidate_user_graph_access.assert_called_once_with(
        "*", test_graph.graph_id
      )

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

      # The departed tenant's documents must not survive teardown.
      remaining_documents = (
        db_session.query(Document)
        .filter(Document.graph_id == test_graph.graph_id)
        .count()
      )
      assert remaining_documents == 0
      assert result.documents_deleted >= 1

      # Neither the connection nor — critically — the encrypted OAuth token
      # behind it may outlive the graph. connection_credentials carries no FK,
      # so nothing else in the tree would ever reach it.
      remaining_connections = (
        db_session.query(Connection)
        .filter(Connection.graph_id == test_graph.graph_id)
        .count()
      )
      assert remaining_connections == 0
      assert result.connections_deleted >= 1

      remaining_credentials = (
        db_session.query(ConnectionCredentials)
        .filter(ConnectionCredentials.connection_id == connection_id)
        .count()
      )
      assert remaining_credentials == 0

  @pytest.mark.asyncio
  async def test_deprovision_updates_subscription_metadata(
    self, service, db_session, test_graph, test_org
  ):
    """Verify subscription metadata is updated with deprovisioning info."""
    from robosystems.models.core.billing.subscription import BillingSubscription

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


class TestReportBundlePurge:
  """Published report artifacts must not outlive the tenant.

  Report bundles were the last customer data store teardown did not reach.
  Their prefix carries no lifecycle rule by design — a clock there would destroy
  a live report's publication — so teardown is the only thing that can remove
  them, and these tests are what keep that true.
  """

  @pytest.mark.asyncio
  async def test_bundles_are_deleted_under_the_graph_prefix(
    self, service, db_session, test_graph, stub_bundle_purge
  ):
    """Every artifact under report-bundles/{graph_id}/ goes, and the count is
    reported so the operator log states what actually happened."""
    keys = [
      f"report-bundles/{test_graph.graph_id}/rpt_a/g1.jsonld",
      f"report-bundles/{test_graph.graph_id}/rpt_a/g1.holon.jsonld",
      f"report-bundles/{test_graph.graph_id}/rpt_b/g2.zip",
    ]
    stub_bundle_purge.iter_object_keys.return_value = keys

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ),
      patch("robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"),
    ):
      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

    # Scoped to this graph's prefix — never the whole bucket.
    _, kwargs = stub_bundle_purge.iter_object_keys.call_args
    assert kwargs["prefix"] == f"report-bundles/{test_graph.graph_id}/"

    assert stub_bundle_purge.delete_object.call_count == 3
    assert result.report_bundles_deleted == 3
    assert not [e for e in result.errors if "Report bundle" in e]

  @pytest.mark.asyncio
  async def test_an_object_that_will_not_delete_is_recorded_as_an_error(
    self, service, db_session, test_graph, stub_bundle_purge
  ):
    """Incomplete disposal is the one outcome this step exists to prevent, so a
    surviving object degrades the teardown to `partial` rather than passing
    quietly. This is deliberately stricter than the sibling purges."""
    stub_bundle_purge.iter_object_keys.return_value = [
      f"report-bundles/{test_graph.graph_id}/rpt_a/g1.jsonld",
      f"report-bundles/{test_graph.graph_id}/rpt_b/g1.jsonld",
    ]
    stub_bundle_purge.delete_object.side_effect = [True, False]

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ),
      patch("robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"),
    ):
      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

    assert result.report_bundles_deleted == 1
    assert any("Report bundle purge incomplete" in e for e in result.errors)
    assert result.status == "partial"

  @pytest.mark.asyncio
  async def test_storage_failure_does_not_strand_the_teardown(
    self, service, db_session, test_graph, stub_bundle_purge
  ):
    """Best-effort like its siblings: object storage being unreachable must not
    hold the graph in a half-torn-down state or block the capacity release."""
    stub_bundle_purge.iter_object_keys.side_effect = RuntimeError("s3 unreachable")

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        new_callable=AsyncMock,
      ),
      patch("robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"),
    ):
      result = await service.deprovision_graph(
        test_graph.graph_id, db_session, create_backup=False
      )

    assert any("Report bundle purge failed" in e for e in result.errors)

    # The graph is still torn down and the row still soft-deleted.
    db_session.refresh(test_graph)
    assert test_graph.status == GraphStatus.DEPROVISIONED.value
    assert test_graph.deleted_at is not None


class TestOrphanTenantSchemas:
  """A partial teardown, or a crash after the drop step failed, leaves a
  tenant schema no graph owns; every per-tenant migration keeps touching it."""

  def test_orphans_are_schemas_without_a_live_graph(self, db_session, test_graph):
    from unittest.mock import patch

    from robosystems.operations.graph.deprovision_service import (
      find_orphan_tenant_schemas,
    )

    ghost = "kgdeadbeefdeadbeef01"
    with patch(
      "robosystems.db.extensions.list_tenant_schemas",
      return_value=[test_graph.graph_id, ghost],
    ):
      assert find_orphan_tenant_schemas(db_session) == [ghost]

      # A deprovisioned graph's schema is an orphan too.
      test_graph.transition_status(GraphStatus.DEPROVISIONED, db_session)
      db_session.flush()
      assert find_orphan_tenant_schemas(db_session) == [test_graph.graph_id, ghost]

  def test_purge_drops_each_orphan(self, db_session, test_graph):
    from unittest.mock import patch

    from robosystems.operations.graph.deprovision_service import (
      purge_orphan_tenant_schemas,
    )

    ghost = "kgdeadbeefdeadbeef01"
    with (
      patch(
        "robosystems.db.extensions.list_tenant_schemas",
        return_value=[test_graph.graph_id, ghost],
      ),
      patch("robosystems.db.extensions.drop_tenant_schema", return_value=True) as drop,
    ):
      assert purge_orphan_tenant_schemas(db_session) == [ghost]
    drop.assert_called_once_with(ghost)
