"""Graph teardown, shared by the admin endpoint and the Dagster lifecycle job.

Steps are best-effort: a failure is recorded and does not block later steps.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from ...config.deprovisioning import get_deprovisioning_config
from ...logger import get_logger

logger = get_logger(__name__)


# graph_metadata key: a deprovisioned graph whose data disposal must be retried.
RESIDUAL_PENDING_KEY = "residual_pending"


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
  connections_revoked: int = 0
  connections_deleted: int = 0
  search_purged: bool = False
  report_bundles_deleted: int = 0
  staged_objects_deleted: int = 0
  errors: list[str] = field(default_factory=list)

  @property
  def message(self) -> str:
    if self.status == "not_found":
      return f"Graph {self.graph_id} not found"
    if self.status == "already_deprovisioned":
      redone = []
      if self.extensions_schema_dropped:
        redone.append("extensions schema dropped")
      if self.search_purged:
        redone.append("search index purged")
      if self.report_bundles_deleted:
        redone.append(f"{self.report_bundles_deleted} report bundle(s) deleted")
      suffix = (
        f"; residual data disposal re-run ({', '.join(redone)})" if redone else ""
      )
      return f"Graph {self.graph_id} is already deprovisioned{suffix}"
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


def find_orphan_tenant_schemas(session: Session) -> list[str]:
  """Tenant schemas in the extensions database that no live graph owns.

  An orphan has no platform ``Graph`` row, or one that is deprovisioned or
  soft-deleted. Read-only; ``purge_orphan_tenant_schemas`` drops them.
  """
  from ...db.extensions import list_tenant_schemas
  from ...models.core.graph import Graph, GraphStatus

  schemas = list_tenant_schemas()
  if not schemas:
    return []
  live = {
    graph_id
    for (graph_id,) in session.query(Graph.graph_id)
    .filter(
      Graph.graph_id.in_(schemas),
      Graph.status != GraphStatus.DEPROVISIONED.value,
      Graph.deleted_at.is_(None),
    )
    .all()
  }
  return [schema for schema in schemas if schema not in live]


def purge_orphan_tenant_schemas(session: Session) -> list[str]:
  """Drop every orphan tenant schema. Returns the ids that were dropped."""
  from ...db.extensions import drop_tenant_schema

  dropped: list[str] = []
  for graph_id in find_orphan_tenant_schemas(session):
    if drop_tenant_schema(graph_id):
      logger.info(f"Dropped orphan extensions schema {graph_id}")
      dropped.append(graph_id)
  return dropped


class GraphDeprovisionService:
  """Tear down a graph's infrastructure. Step order is load-bearing."""

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

    graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if not graph:
      result.status = "not_found"
      return result

    if graph.status == GraphStatus.DEPROVISIONED.value:
      # The status flips even when a disposal step fails, so re-run the
      # idempotent disposal steps (and nothing else) to finish the job.
      result.status = "already_deprovisioned"
      result.previous_status = graph.status
      self._dispose_residual_data(graph_id, result)
      self._mark_residual_pending(graph, session, bool(result.errors))
      return result

    # Shared repositories are platform-managed and never deprovisioned here.
    if graph.is_repository:
      result.status = "rejected"
      result.previous_status = graph.status or "active"
      result.errors.append(
        f"Graph {graph_id} is a shared repository and cannot be deprovisioned"
      )
      return result

    result.previous_status = graph.status or "active"

    # Stamp and commit `deleted_at` before anything is dropped: the schema
    # provisioner refuses such a graph, so an in-flight sync cannot recreate
    # the tenant schema mid-teardown. Status is left until the end, so a
    # partial run still reads as unfinished.
    graph.deleted_at = datetime.now(UTC)
    session.commit()

    if create_backup and not skip_backup_check:
      await self._create_final_backup(graph, session, result)

    await self._delete_subgraphs(graph_id, session, result)
    await self._delete_database(graph_id, result)
    errors_before_disposal = len(result.errors)
    self._dispose_residual_data(graph_id, result)
    residual_failed = len(result.errors) > errors_before_disposal

    # The .lbug is still on the instance: freeing the registry slot would hand
    # the next tenant a volume carrying this one's file. Stop here; the
    # teardown sensor retries graphs with deleted_at set and status not yet
    # deprovisioned, and every step above is idempotent.
    if not result.database_deleted:
      result.status = "partial"
      logger.warning(
        f"Graph {graph_id} database delete failed; leaving registry, PG records "
        "and status intact for the teardown sensor to retry (avoids stranding "
        "the .lbug on a freed instance)",
        extra={"graph_id": graph_id, "errors": result.errors},
      )
      return result

    await self._deallocate_registry(graph_id, result)

    # Before _clean_pg_records: the GraphUser rows are the only way to
    # enumerate staged uploads.
    self._purge_staged_uploads(graph_id, session, result)

    # Before _clean_pg_records: revocation reads the stored credential.
    await self._revoke_provider_grants(graph_id, session, result)

    self._clean_pg_records(graph_id, session, result)
    self._update_subscription_metadata(graph_id, session, result)
    graph.transition_status(GraphStatus.DEPROVISIONED, session)
    # The status is final either way; a failed disposal step is retried by the
    # teardown sensor, which selects graphs carrying this mark.
    self._mark_residual_pending(graph, session, residual_failed)

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
        "connections_revoked": result.connections_revoked,
        "connections_deleted": result.connections_deleted,
        "search_purged": result.search_purged,
        "report_bundles_deleted": result.report_bundles_deleted,
        "errors": result.errors,
      },
    )

    return result

  async def _create_final_backup(
    self, graph, session: Session, result: DeprovisionResult
  ) -> None:
    """Create a final backup before teardown, and register it for retrieval."""
    try:
      from ...models.core.graph.graph_backup import (
        BackupStatus,
        BackupType,
        GraphBackup,
      )

      # On a sensor retry, reuse a completed full backup taken since
      # `deleted_at` was stamped (i.e. by this teardown) instead of re-dumping.
      if graph.deleted_at is not None:
        deleted_at = graph.deleted_at
        if deleted_at.tzinfo is None:
          deleted_at = deleted_at.replace(tzinfo=UTC)
        for prior in GraphBackup.get_by_graph_id(
          graph.graph_id,
          session=session,
          backup_type=BackupType.FULL.value,
          status=BackupStatus.COMPLETED.value,
        ):
          created = prior.created_at
          if created is not None and created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
          if created is not None and created >= deleted_at:
            result.backup_created = True
            result.backup_path = prior.s3_key
            logger.info(
              f"Final backup from this teardown already exists for "
              f"{graph.graph_id}; skipping re-dump on retry",
              extra={"graph_id": graph.graph_id},
            )
            return

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
    """Record the final backup as a GraphBackup row, so the customer can list
    and download it during the export grace period.

    ``expires_at`` follows the tier's backup hosting window, not the shorter
    export window. Not ``GraphBackup.create``, which commits. The SAVEPOINT
    keeps a failed insert from aborting the transaction every later step runs
    in.
    """
    from ...models.core.graph.graph_backup import BackupInitiator, GraphBackup

    with session.begin_nested():
      session.add(
        GraphBackup.from_completed_export(
          graph_id=graph.graph_id,
          database_name=graph.graph_id,
          metadata=metadata,
          s3_bucket=s3_bucket,
          retention_days=retention_days,
          initiated_by=BackupInitiator.FINAL.value,
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

      await self._release_subgraph(subgraph.graph_id, session, result)

      # Regardless of the database deletion outcome.
      try:
        subgraph.deleted_at = datetime.now(UTC)
        subgraph.transition_status(GraphStatus.DEPROVISIONED, session)
      except Exception as e:
        error_msg = f"Subgraph {subgraph.graph_id} status transition failed: {e}"
        result.errors.append(error_msg)
        logger.warning(error_msg)

  async def release_subgraph_records(
    self, subgraph_id: str, session: Session
  ) -> DeprovisionResult:
    """Remove what a subgraph holds outside its database, before its row goes.

    ``delete-subgraph`` deletes the Graph row, and a connection (even a
    soft-deleted one) still references it by FK, so the same release the
    parent's teardown runs has to come first. Flushes; the caller commits.
    """
    result = DeprovisionResult(status="success", graph_id=subgraph_id)
    await self._release_subgraph(subgraph_id, session, result)
    if result.errors:
      result.status = "partial"
      logger.warning(
        f"Subgraph {subgraph_id} release incomplete",
        extra={"graph_id": subgraph_id, "errors": result.errors},
      )
    return result

  async def _release_subgraph(
    self, subgraph_id: str, session: Session, result: DeprovisionResult
  ) -> None:
    # Same ordering as the parent path; a connection may be scoped directly
    # to a subgraph.
    self._purge_staged_uploads(subgraph_id, session, result)
    await self._revoke_provider_grants(subgraph_id, session, result)
    self._clean_pg_records(subgraph_id, session, result)
    self._purge_search_index(subgraph_id, result)

  async def _delete_database(self, graph_id: str, result: DeprovisionResult) -> None:
    """Delete the parent graph database.

    Success means the file is not on the instance, not that this run removed
    it, so retries converge. A missing .lbug still has its side stores
    disposed (``existed=False``); a 404 from an older node also counts as
    success, leaving any residue to storage reclaim.
    """
    from ...graph_api.client.exceptions import GraphAPIError

    try:
      from ...graph_api.client.factory import get_graph_client

      graph_client = await get_graph_client(graph_id=graph_id, operation_type="write")
      try:
        outcome = await graph_client.delete_database(graph_id)
        result.database_deleted = True
        if isinstance(outcome, dict) and outcome.get("existed") is False:
          logger.info(
            f"Database for graph {graph_id} was already absent; the node "
            f"disposed its side stores ({len(outcome.get('removed') or [])} paths)",
            extra={"graph_id": graph_id, "removed": outcome.get("removed")},
          )
        else:
          logger.info(f"Deleted database for graph {graph_id}")
      finally:
        await graph_client.close()
    except GraphAPIError as e:
      if getattr(e, "status_code", None) == 404:
        result.database_deleted = True
        logger.info(
          f"Database for graph {graph_id} already absent on a node that does "
          "not dispose side stores for a missing file; treating delete as "
          "complete so teardown can converge",
          extra={"graph_id": graph_id},
        )
      else:
        error_msg = f"Database deletion failed: {e}"
        result.errors.append(error_msg)
        logger.warning(error_msg, extra={"graph_id": graph_id})
    except Exception as e:
      error_msg = f"Database deletion failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})

  @staticmethod
  def _mark_residual_pending(graph, session: Session, pending: bool) -> None:
    metadata = dict(graph.graph_metadata or {})
    if bool(metadata.get(RESIDUAL_PENDING_KEY)) == pending:
      return
    if pending:
      metadata[RESIDUAL_PENDING_KEY] = True
    else:
      metadata.pop(RESIDUAL_PENDING_KEY, None)
    graph.graph_metadata = metadata
    session.commit()

  def _dispose_residual_data(self, graph_id: str, result: DeprovisionResult) -> None:
    """The three data-disposal steps that are safe to repeat."""
    self._drop_extensions_schema(graph_id, result)
    self._purge_search_index(graph_id, result)
    self._purge_report_bundles(graph_id, result)

  def _drop_extensions_schema(self, graph_id: str, result: DeprovisionResult) -> None:
    """Drop the tenant's extensions OLTP schema; a no-op for subgraphs and
    extensions-disabled deployments."""
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
    """Purge the tenant's documents from the shared, cross-tenant OpenSearch index."""
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

  def _purge_report_bundles(self, graph_id: str, result: DeprovisionResult) -> None:
    """Delete the tenant's whole ``report-bundles/{graph_id}/`` prefix.

    The prefix has no lifecycle rule (it would expire live reports), so
    teardown is the only thing that removes it. An object that fails to delete
    is recorded as an error.
    """
    try:
      from ...config import env
      from ...config.storage.graph import get_report_bundle_prefix
      from ..aws.s3 import S3Client

      bucket = env.USER_DATA_BUCKET
      prefix = get_report_bundle_prefix(graph_id)
      s3 = S3Client()

      deleted = 0
      failed = 0
      # Not ``list_objects``: it reads an S3 error or a truncated listing as
      # an empty prefix. ``iter_object_keys`` paginates and raises.
      for key in s3.iter_object_keys(bucket, prefix=prefix):
        if s3.delete_object(bucket, key):
          deleted += 1
        else:
          failed += 1
          logger.warning(
            f"Failed to delete report artifact s3://{bucket}/{key}",
            extra={"graph_id": graph_id},
          )

      result.report_bundles_deleted = deleted
      if failed:
        result.errors.append(
          f"Report bundle purge incomplete: {failed} object(s) not deleted"
        )
      elif deleted:
        logger.info(
          f"Purged {deleted} report artifact(s) for graph {graph_id}",
          extra={"graph_id": graph_id},
        )
    except Exception as e:
      error_msg = f"Report bundle purge failed: {e}"
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

  @staticmethod
  def _invalidate_member_access(graph_id: str, member_ids: list[str]) -> None:
    from ...middleware.auth.cache import api_key_cache

    for user_id in member_ids:
      api_key_cache.invalidate_user_jwt_graph_access(user_id, graph_id)
    api_key_cache.invalidate_user_graph_access("*", graph_id)

  def _purge_staged_uploads(
    self, graph_id: str, session: Session, result: DeprovisionResult
  ) -> None:
    """Delete ``user-staging/{user_id}/{graph_id}/`` for every member.

    Must run before ``_clean_pg_records`` drops the GraphUser rows that
    enumerate the members. The bucket lifecycle rule is the backstop.
    """
    try:
      from ...config import env
      from ...config.storage.graph import get_staging_prefix
      from ...middleware.graph.types import parse_graph_id
      from ...models.core.graph.graph_user import GraphUser
      from ...operations.aws.s3 import S3Client

      bucket = env.USER_DATA_BUCKET
      if not bucket:
        return

      # Membership lives on the parent graph, but the prefix is keyed on the
      # graph_id given: a subgraph's uploads sit under its own id.
      parent_id, _ = parse_graph_id(graph_id)
      user_ids = [
        row[0]
        for row in session.query(GraphUser.user_id)
        .filter(GraphUser.graph_id == parent_id)
        .distinct()
        .all()
      ]
      if not user_ids:
        return

      s3 = S3Client()
      deleted = 0
      for user_id in user_ids:
        prefix = get_staging_prefix(user_id, graph_id)
        for key in s3.iter_object_keys(bucket, prefix):
          if s3.delete_object(bucket, key):
            deleted += 1
      result.staged_objects_deleted += deleted
      if deleted:
        logger.info(f"Purged {deleted} staged upload object(s) for graph {graph_id}")
    except Exception as e:
      error_msg = f"Staged upload purge failed: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})

  async def _revoke_provider_grants(
    self, graph_id: str, session: Session, result: DeprovisionResult
  ) -> None:
    """Revoke each connection's grant at the provider, as disconnect does.

    Deleting our token does not end the authorization upstream. Best-effort
    per connection: a failure is recorded and the credential is deleted anyway.
    """
    try:
      from ...models.core.connection.connection import Connection
      from ...operations.providers.registry import provider_registry

      rows = (
        session.query(Connection.id, Connection.provider, Connection.entity_name)
        .filter(Connection.graph_id == graph_id)
        .all()
      )
    except Exception as e:
      error_msg = f"Provider grant revocation could not list connections: {e}"
      result.errors.append(error_msg)
      logger.warning(error_msg, extra={"graph_id": graph_id})
      return

    for connection_id, provider, entity_name in rows:
      provider_type = (provider or "").lower()
      try:
        await provider_registry.cleanup_connection(
          provider_type,
          {
            "id": connection_id,
            "connection_id": connection_id,
            "provider": provider_type,
            "entity_name": entity_name,
            # The disconnect endpoint's payload carries entity_id = graph_id;
            # the provider cleanup logs it.
            "entity_id": graph_id,
            "graph_id": graph_id,
          },
          graph_id,
        )
        result.connections_revoked += 1
        logger.info(
          f"Revoked provider grant for connection {connection_id} ({provider_type}) "
          f"during teardown of {graph_id}",
          extra={"graph_id": graph_id, "connection_id": connection_id},
        )
      except ValueError:
        # Provider disabled by feature flag in this deployment: no client to
        # call. Mirrors the disconnect endpoint, which also skips cleanup here.
        logger.warning(
          f"Provider {provider_type} disabled; skipping grant revocation for "
          f"connection {connection_id} during teardown of {graph_id}",
          extra={"graph_id": graph_id, "connection_id": connection_id},
        )
      except Exception as e:
        error_msg = (
          f"Provider grant revocation failed for connection {connection_id} "
          f"({provider_type}): {e}"
        )
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

      # Drop cached access decisions with the rows, or a warm entry admits a
      # member for up to the cache TTL.
      member_ids = [
        row[0]
        for row in session.query(GraphUser.user_id)
        .filter(GraphUser.graph_id == graph_id)
        .all()
      ]
      # Org OWNER/ADMIN hold implicit graph admin without a GraphUser row, so
      # their cached decisions have to be dropped by name as well.
      from ...models.core.graph.graph import Graph
      from ...models.core.org.org_user import OrgRole, OrgUser

      org_id = session.query(Graph.org_id).filter(Graph.graph_id == graph_id).scalar()
      if org_id is not None:
        member_ids += [
          row[0]
          for row in session.query(OrgUser.user_id)
          .filter(
            OrgUser.org_id == org_id,
            OrgUser.role.in_([OrgRole.OWNER, OrgRole.ADMIN]),
          )
          .all()
        ]
      self._invalidate_member_access(graph_id, list(dict.fromkeys(member_ids)))

      session.query(GraphUser).filter(GraphUser.graph_id == graph_id).delete(
        synchronize_session=False
      )

      session.query(GraphSchema).filter(GraphSchema.graph_id == graph_id).delete(
        synchronize_session=False
      )

      session.query(GraphFile).filter(GraphFile.graph_id == graph_id).delete(
        synchronize_session=False
      )

      # The FK cascade never fires: the graph row is only soft-deleted.
      documents_deleted = (
        session.query(Document)
        .filter(Document.graph_id == graph_id)
        .delete(synchronize_session=False)
      )
      result.documents_deleted += documents_deleted

      # Connections belong to the graph, so teardown removes them, soft-deleted
      # ones included. Credentials first: connection_credentials.connection_id
      # has no FK, so the connection rows are the only way to find them.
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
