"""Graph deprovisioning service.

Shared service for tearing down graph infrastructure. Called by both
the admin endpoint and the Dagster automation job.

Steps are best-effort: individual failures are captured as warnings
and do not block the overall deprovisioning flow.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from ...config.deprovisioning import get_deprovisioning_config
from ...logger import get_logger

logger = get_logger(__name__)


@dataclass
class DeprovisionResult:
  """Result of a graph deprovisioning operation."""

  status: str  # "success" | "partial" | "already_deprovisioned" | "not_found"
  graph_id: str
  previous_status: str = ""
  backup_created: bool = False
  backup_registered: bool = False
  backup_path: str | None = None
  subgraphs_deleted: int = 0
  database_deleted: bool = False
  extensions_schema_dropped: bool = False
  registry_deallocated: bool = False
  records_cleaned: bool = False
  documents_deleted: int = 0
  connections_deleted: int = 0
  search_purged: bool = False
  errors: list[str] = field(default_factory=list)

  @property
  def message(self) -> str:
    """Build a human-readable summary message."""
    if self.status == "not_found":
      return f"Graph {self.graph_id} not found"
    if self.status == "already_deprovisioned":
      return f"Graph {self.graph_id} is already deprovisioned"
    if self.status == "rejected":
      return (
        self.errors[0]
        if self.errors
        else f"Graph {self.graph_id} cannot be deprovisioned"
      )

    parts = [f"Graph {self.graph_id} deprovisioned"]
    if self.backup_created:
      parts.append("(backup created)")
    if self.database_deleted:
      parts.append("(database deleted)")
    else:
      parts.append("(database not found or already removed)")
    if self.subgraphs_deleted > 0:
      parts.append(f"({self.subgraphs_deleted} subgraphs removed)")
    return " ".join(parts)


class GraphDeprovisionService:
  """Tear down a graph's infrastructure, in dependency order.

  Backup first (optional), then subgraph databases, the parent database, the
  DynamoDB routing entry, the PostgreSQL records, the subscription metadata,
  and finally the soft-delete of the graph row. Later steps assume earlier ones
  ran, so the order is not arbitrary.
  """

  def __init__(self, environment: str):
    self.environment = environment

  async def deprovision_graph(
    self,
    graph_id: str,
    session: Session,
    create_backup: bool = True,
    skip_backup_check: bool = False,
  ) -> DeprovisionResult:
    """Tear down a graph. Destructive and not reversible from here.

    The caller owns ``session``. ``skip_backup_check`` bypasses the
    configuration's backup requirement, so a graph can be torn down with no
    recoverable copy — use it only when the data is known to be disposable.

    Individual steps are best-effort: a failure is recorded in the result's
    ``errors`` and downgrades the status to ``partial`` rather than aborting.
    """
    from ...models.core.graph import Graph, GraphStatus

    result = DeprovisionResult(graph_id=graph_id, status="success")

    # --- Validate ---
    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if not graph:
      result.status = "not_found"
      return result

    if graph.status == GraphStatus.DEPROVISIONED.value:
      result.status = "already_deprovisioned"
      result.previous_status = graph.status
      return result

    # Shared repositories (SEC, etc.) are platform-managed and must never
    # be deprovisioned through the normal lifecycle.  Their subgraphs
    # (e.g. sec_historical) would be destroyed, and the instance/volume
    # freed for reuse which could leak data.
    if graph.is_repository:
      result.status = "rejected"
      result.previous_status = graph.status or "active"
      result.errors.append(
        f"Graph {graph_id} is a shared repository and cannot be deprovisioned"
      )
      return result

    result.previous_status = graph.status or "active"

    # --- 1. Create final backup ---
    if create_backup and not skip_backup_check:
      await self._create_final_backup(graph, session, result)

    # --- 2. Delete subgraphs ---
    await self._delete_subgraphs(graph_id, session, result)

    # --- 3. Delete parent database ---
    await self._delete_database(graph_id, result)

    # --- 3b. Drop extensions OLTP schema (financial-data removal) ---
    self._drop_extensions_schema(graph_id, result)

    # --- 3c. Purge documents from the shared search index ---
    self._purge_search_index(graph_id, result)

    # --- 4. Deallocate DynamoDB registry ---
    await self._deallocate_registry(graph_id, result)

    # --- 5. Clean PostgreSQL records ---
    self._clean_pg_records(graph_id, session, result)

    # --- 6. Update subscription metadata ---
    self._update_subscription_metadata(graph_id, session, result)

    # --- 7. Soft-delete and transition status ---
    graph.deleted_at = datetime.now(UTC)
    graph.transition_status(GraphStatus.DEPROVISIONED, session)

    if result.errors:
      result.status = "partial"

    logger.info(
      f"Deprovisioned graph {graph_id}",
      extra={
        "graph_id": graph_id,
        "status": result.status,
        "backup_created": result.backup_created,
        "subgraphs_deleted": result.subgraphs_deleted,
        "database_deleted": result.database_deleted,
        "documents_deleted": result.documents_deleted,
        "connections_deleted": result.connections_deleted,
        "search_purged": result.search_purged,
        "errors": result.errors,
      },
    )

    return result

  async def _create_final_backup(
    self, graph, session: Session, result: DeprovisionResult
  ) -> None:
    """Create a final backup before teardown, and register it for retrieval."""
    try:
      from .engine.backup_manager import (
        BackupFormat,
        BackupJob,
        BackupManager,
      )

      config = get_deprovisioning_config()
      retention_days = config.get_backup_hosting_days(graph.graph_tier)

      backup_manager = BackupManager()
      backup_job = BackupJob(
        graph_id=graph.graph_id,
        backup_format=BackupFormat.FULL_DUMP,
        retention_days=retention_days,
        compression=True,
      )
      metadata = await backup_manager.create_backup(backup_job)
      result.backup_created = True
      result.backup_path = metadata.s3_key if metadata else None
      if metadata:
        self._register_final_backup(
          graph,
          session,
          metadata,
          backup_manager.s3_adapter.bucket_name,
          retention_days,
          result,
        )
      logger.info(f"Final backup created for graph {graph.graph_id}")
    except Exception as e:
      error_msg = f"Backup creation failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph.graph_id})

  def _register_final_backup(
    self,
    graph,
    session: Session,
    metadata,
    s3_bucket: str,
    retention_days: int,
    result: DeprovisionResult,
  ) -> None:
    """Record the final backup as a GraphBackup row.

    Without a row the object is unreachable: both the listing and the download
    URL resolve through GraphBackup, so a final backup that exists only in S3
    is invisible to the customer it was taken for — which is the export grace
    period the privacy policy publishes. Only the user-initiated path created
    rows, and deprovisioning does not go through it.

    ``expires_at`` matches the tier's backup hosting window rather than the
    shorter export window, so the retention sweep keeps treating this object
    exactly as long as it did while untracked. Shortening it here would delete
    the archive early, which is a different promise from the export one.

    Built and flushed rather than going through ``GraphBackup.create`` /
    ``complete_backup``: those commit, and this runs inside a best-effort step
    of a caller-owned transaction, where an early commit would persist a
    partial teardown if a later step failed.

    The write goes inside a SAVEPOINT because it is the *first* database
    statement of the teardown, and on PostgreSQL a failed statement aborts the
    whole transaction — every later step would then fail against a dead
    session, turning one best-effort miss into total failure and breaking this
    module's contract that steps do not block each other.

    A plain ``session.rollback()`` would be worse than the problem. The batch
    job (``dagster/jobs/graph_lifecycle.py``) runs every graph through one
    session that commits only after the loop, so rolling back here would
    discard the completed teardowns of every graph already processed in that
    run — graphs whose databases, extensions schemas and registry slots are
    already gone, leaving their records looking live. The savepoint contains
    the failure to this row alone.
    """
    from ...models.core.graph.graph_backup import (
      BackupInitiator,
      BackupStatus,
      BackupType,
      GraphBackup,
    )

    now = datetime.now(UTC)
    backup_metadata: dict = {
      "backup_format": metadata.backup_format,
      "compression_ratio": metadata.compression_ratio,
    }
    if metadata.memory:
      backup_metadata["memory"] = metadata.memory
    if metadata.payload_delta:
      backup_metadata["payload_delta"] = metadata.payload_delta

    with session.begin_nested():
      session.add(
        GraphBackup(
          graph_id=graph.graph_id,
          database_name=graph.graph_id,
          backup_type=BackupType.FULL.value,
          initiated_by=BackupInitiator.FINAL.value,
          status=BackupStatus.COMPLETED.value,
          s3_bucket=s3_bucket,
          s3_key=metadata.s3_key or "",
          s3_metadata_key=metadata.s3_metadata_key,
          original_size_bytes=metadata.original_size,
          compressed_size_bytes=metadata.compressed_size,
          compression_ratio=metadata.compression_ratio,
          node_count=metadata.node_count,
          relationship_count=metadata.relationship_count,
          database_version=metadata.database_version,
          backup_duration_seconds=metadata.backup_duration_seconds,
          checksum=metadata.checksum,
          compression_enabled=True,
          backup_metadata=backup_metadata,
          started_at=metadata.timestamp,
          completed_at=now,
          expires_at=now + timedelta(days=retention_days),
        )
      )
    result.backup_registered = True

  async def _delete_subgraphs(
    self, graph_id: str, session: Session, result: DeprovisionResult
  ) -> None:
    """Delete all subgraph databases and mark their records."""
    from ...models.core.graph import Graph, GraphStatus

    subgraphs = Graph.get_subgraphs(graph_id, session)
    if not subgraphs:
      return

    from .subgraph_service import SubgraphService

    subgraph_service = SubgraphService()

    for subgraph in subgraphs:
      try:
        await subgraph_service.delete_subgraph_database(subgraph.graph_id, force=True)
        result.subgraphs_deleted += 1
      except Exception as e:
        error_msg = f"Subgraph {subgraph.graph_id} deletion failed: {e}"
        result.errors.append(error_msg)
        logger.warning(error_msg)

      # Clean subgraph PG records (schemas, files) before marking deprovisioned
      self._clean_pg_records(subgraph.graph_id, session, result)
      self._purge_search_index(subgraph.graph_id, result)

      # Mark subgraph as deprovisioned regardless of DB deletion outcome
      try:
        subgraph.deleted_at = datetime.now(UTC)
        subgraph.transition_status(GraphStatus.DEPROVISIONED, session)
      except Exception as e:
        error_msg = f"Subgraph {subgraph.graph_id} status transition failed: {e}"
        result.errors.append(error_msg)
        logger.warning(error_msg)

  async def _delete_database(self, graph_id: str, result: DeprovisionResult) -> None:
    """Delete the parent graph database."""
    try:
      from ...graph_api.client.factory import get_graph_client

      graph_client = await get_graph_client(graph_id=graph_id, operation_type="write")
      try:
        await graph_client.delete_database(graph_id)
        result.database_deleted = True
        logger.info(f"Deleted database for graph {graph_id}")
      finally:
        await graph_client.close()
    except Exception as e:
      error_msg = f"Database deletion failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})

  def _drop_extensions_schema(self, graph_id: str, result: DeprovisionResult) -> None:
    """Drop the tenant's extensions OLTP schema (financial-data removal).

    Counterpart to ``provision_tenant_schema``. Without this, a deprovisioned
    tenant's transactions/entries/facts persist in the extensions database.
    No-op for subgraphs and extensions-disabled deployments (drop returns
    False). Best-effort: a failure is recorded but does not block teardown.
    """
    try:
      from ...db.extensions import drop_tenant_schema

      result.extensions_schema_dropped = drop_tenant_schema(graph_id)
      if result.extensions_schema_dropped:
        logger.info(f"Dropped extensions OLTP schema for graph {graph_id}")
    except Exception as e:
      error_msg = f"Extensions schema drop failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})

  def _purge_search_index(self, graph_id: str, result: DeprovisionResult) -> None:
    """Purge the tenant's documents from the shared OpenSearch index.

    The document index is shared across all tenants (an application-level
    graph_id filter is the only boundary), so a departed tenant's content
    persisting in it is real over-retention. Guarded on SEMANTIC_SEARCH_ENABLED
    so a search-disabled deployment is a clean no-op, and best-effort so an
    index failure records a warning without stranding the teardown.
    """
    from ...config import env

    if not env.SEMANTIC_SEARCH_ENABLED:
      return
    try:
      from ...operations.search.client import OpenSearchClient

      client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
      client.delete_by_graph_id(graph_id)
      result.search_purged = True
      logger.info(f"Purged search index for graph {graph_id}")
    except Exception as e:
      error_msg = f"Search index purge failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})

  async def _deallocate_registry(
    self, graph_id: str, result: DeprovisionResult
  ) -> None:
    """Deallocate from DynamoDB routing registry."""
    try:
      from ...middleware.graph.allocation_manager import LadybugAllocationManager

      allocation_manager = LadybugAllocationManager(environment=self.environment)
      await allocation_manager.deallocate_database(graph_id)
      result.registry_deallocated = True
      logger.info(f"Deallocated routing entry for graph {graph_id}")
    except Exception as e:
      error_msg = f"Registry deallocation failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})

  def _clean_pg_records(
    self, graph_id: str, session: Session, result: DeprovisionResult
  ) -> None:
    """Delete associated PostgreSQL records (credits, users, schemas, files).

    GraphBackup records are intentionally kept for post-deprovisioning hosting.
    """
    try:
      from ...models.core.connection.connection import Connection
      from ...models.core.connection.connection_credentials import (
        ConnectionCredentials,
      )
      from ...models.core.document import Document
      from ...models.core.graph.graph_credits import (
        GraphCredits,
        GraphCreditTransaction,
      )
      from ...models.core.graph.graph_file import GraphFile
      from ...models.core.graph.graph_schema import GraphSchema
      from ...models.core.graph.graph_user import GraphUser

      session.query(GraphCreditTransaction).filter(
        GraphCreditTransaction.graph_id == graph_id
      ).delete(synchronize_session=False)

      session.query(GraphCredits).filter(GraphCredits.graph_id == graph_id).delete(
        synchronize_session=False
      )

      session.query(GraphUser).filter(GraphUser.graph_id == graph_id).delete(
        synchronize_session=False
      )

      session.query(GraphSchema).filter(GraphSchema.graph_id == graph_id).delete(
        synchronize_session=False
      )

      session.query(GraphFile).filter(GraphFile.graph_id == graph_id).delete(
        synchronize_session=False
      )

      # documents.graph_id cascades on the graphs row, but step 7 is a soft
      # delete so the cascade never fires — delete explicitly here. The shared
      # OpenSearch content is purged separately in _purge_search_index.
      documents_deleted = (
        session.query(Document)
        .filter(Document.graph_id == graph_id)
        .delete(synchronize_session=False)
      )
      result.documents_deleted += documents_deleted

      # Connections are a graph asset, not the creating member's: graph_id is
      # the scoping FK and the delete endpoint gates on graph admin rather than
      # the creator. So the graph's teardown is what removes them — nothing
      # else does. Credentials go first and by id, because
      # connection_credentials.connection_id carries no foreign key, so no
      # cascade can ever reach it; dropping the connection rows first would
      # strand a live encrypted OAuth token with no row left to find it by.
      # Soft-deleted connections are included — deleted_at is unset here on
      # purpose, since the graph is going away either way.
      connection_ids = [
        row.id
        for row in session.query(Connection.id).filter(Connection.graph_id == graph_id)
      ]
      if connection_ids:
        session.query(ConnectionCredentials).filter(
          ConnectionCredentials.connection_id.in_(connection_ids)
        ).delete(synchronize_session=False)

      connections_deleted = (
        session.query(Connection)
        .filter(Connection.graph_id == graph_id)
        .delete(synchronize_session=False)
      )
      result.connections_deleted += connections_deleted

      session.flush()
      result.records_cleaned = True
      logger.info(f"Cleaned PG records for graph {graph_id}")
    except Exception as e:
      error_msg = f"PG record cleanup failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})

  def _update_subscription_metadata(
    self, graph_id: str, session: Session, result: DeprovisionResult
  ) -> None:
    """Update the billing subscription with deprovisioning metadata."""
    try:
      from ...models.core.billing.subscription import BillingSubscription

      config = get_deprovisioning_config()
      sub = BillingSubscription.get_by_resource("graph", graph_id, session)
      if not sub:
        return

      # Get tier for hosting duration
      from ...models.core.graph import Graph

      graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()
      tier = graph.graph_tier if graph else "ladybug-standard"
      hosting_days = config.get_backup_hosting_days(tier)

      now = datetime.now(UTC)
      metadata = dict(sub.subscription_metadata or {})
      metadata["deprovisioned_at"] = now.isoformat()
      if result.backup_path:
        metadata["final_backup_s3_path"] = result.backup_path
      from datetime import timedelta

      metadata["backup_hosting_expires_at"] = (
        now + timedelta(days=hosting_days)
      ).isoformat()

      sub.subscription_metadata = metadata
      session.flush()
      logger.info(f"Updated subscription metadata for graph {graph_id}")
    except Exception as e:
      error_msg = f"Subscription metadata update failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})
