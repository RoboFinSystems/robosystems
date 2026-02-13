"""Graph deprovisioning service.

Shared service for tearing down graph infrastructure. Called by both
the admin endpoint and the Dagster automation job.

Steps are best-effort: individual failures are captured as warnings
and do not block the overall deprovisioning flow.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime

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
  backup_path: str | None = None
  subgraphs_deleted: int = 0
  database_deleted: bool = False
  registry_deallocated: bool = False
  records_cleaned: bool = False
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
  """Service for deprovisioning graph infrastructure.

  Handles the full teardown lifecycle:
  1. Create final backup (optional)
  2. Delete subgraph databases
  3. Delete parent database
  4. Deallocate DynamoDB routing registry
  5. Clean PostgreSQL records
  6. Update subscription metadata
  7. Soft-delete the graph record
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
    """Deprovision a graph, tearing down all infrastructure.

    Args:
        graph_id: The graph to deprovision.
        session: Active SQLAlchemy session (caller manages lifecycle).
        create_backup: Whether to create a final backup before teardown.
        skip_backup_check: If True, skip backup even when config requires it.

    Returns:
        DeprovisionResult with status and details of each step.
    """
    from ...models.iam.graph import Graph, GraphStatus

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
      await self._create_final_backup(graph, result)

    # --- 2. Delete subgraphs ---
    await self._delete_subgraphs(graph_id, session, result)

    # --- 3. Delete parent database ---
    await self._delete_database(graph_id, result)

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
        "errors": result.errors,
      },
    )

    return result

  async def _create_final_backup(self, graph, result: DeprovisionResult) -> None:
    """Create a final backup before teardown."""
    try:
      from ...operations.lbug.backup_manager import (
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
        encryption=False,
        allow_export=True,
      )
      metadata = await backup_manager.create_backup(backup_job)
      result.backup_created = True
      result.backup_path = metadata.s3_key if metadata else None
      logger.info(f"Final backup created for graph {graph.graph_id}")
    except Exception as e:
      error_msg = f"Backup creation failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph.graph_id})

  async def _delete_subgraphs(
    self, graph_id: str, session: Session, result: DeprovisionResult
  ) -> None:
    """Delete all subgraph databases and mark their records."""
    from ...models.iam.graph import Graph, GraphStatus

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
      from ...models.iam.graph_credits import GraphCredits, GraphCreditTransaction
      from ...models.iam.graph_file import GraphFile
      from ...models.iam.graph_schema import GraphSchema
      from ...models.iam.graph_user import GraphUser

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
      from ...models.billing.subscription import BillingSubscription

      config = get_deprovisioning_config()
      sub = BillingSubscription.get_by_resource("graph", graph_id, session)
      if not sub:
        return

      # Get tier for hosting duration
      from ...models.iam.graph import Graph

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
