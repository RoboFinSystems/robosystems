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
  report_bundles_deleted: int = 0
  staged_objects_deleted: int = 0
  errors: list[str] = field(default_factory=list)

  @property
  def message(self) -> str:
    """Build a human-readable summary message."""
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

  A schema is an orphan when its graph id has no platform ``Graph`` row, or
  the row is deprovisioned / soft-deleted. Every ``for_each_tenant_schema``
  migration keeps operating on such a schema, and the data in it belongs to
  a tenant the platform no longer serves. Read-only; ``purge_orphan_tenant_schemas``
  drops them.
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
      # The status flipped even when a data-disposal step failed (`partial`),
      # and there was no way back in: a ghost tenant schema, index documents,
      # or report bundles stayed behind for good, and every per-tenant
      # migration kept operating on the ghost. Re-running the disposal steps
      # is idempotent — DROP SCHEMA IF EXISTS, delete-by-graph, prefix
      # delete — so an already-deprovisioned graph re-runs exactly those and
      # nothing else (no backup, no database, no registry, no status change).
      result.status = "already_deprovisioned"
      result.previous_status = graph.status
      self._dispose_residual_data(graph_id, result)
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

    # --- 0. Mark the graph as leaving, and make it visible now ---
    # `deleted_at` is stamped and committed before anything is dropped, so
    # a QuickBooks sync already in flight cannot re-provision the tenant
    # schema between step 3b (drop) and step 7 (status flip): the schema
    # provisioner refuses a graph with `deleted_at` set. Status stays as it
    # was until teardown ends, so a partial run still reads as unfinished.
    graph.deleted_at = datetime.now(UTC)
    session.commit()

    # --- 1. Create final backup ---
    if create_backup and not skip_backup_check:
      await self._create_final_backup(graph, session, result)

    # --- 2. Delete subgraphs ---
    await self._delete_subgraphs(graph_id, session, result)

    # --- 3. Delete parent database ---
    await self._delete_database(graph_id, result)

    # --- 3b-3d. Dispose of the tenant's data outside the graph database:
    # the extensions OLTP schema, the shared search index, and the published
    # report artifacts. Idempotent, so a re-run on an already-deprovisioned
    # graph can finish what a partial teardown left behind.
    self._dispose_residual_data(graph_id, result)

    # If the graph database itself could not be deleted, the `.lbug` is still
    # on the instance. Freeing the registry slot now would strand it: the
    # instance/volume would read as empty to the allocator while carrying the
    # departed tenant's file, poisoning the next tenant that lands there (see
    # `graph_api` on-disk vs registry counting). So leave the registry entry
    # and the status alone — deleted_at is already stamped, so the graph is
    # closed to callers, and the teardown sensor re-selects a stranded graph
    # (deleted_at set, status not yet deprovisioned) to retry the whole
    # sequence. Every step above is idempotent on re-run.
    if not result.database_deleted:
      result.status = "partial"
      logger.warning(
        f"Graph {graph_id} database delete failed; leaving registry, PG records "
        "and status intact for the teardown sensor to retry (avoids stranding "
        "the .lbug on a freed instance)",
        extra={"graph_id": graph_id, "errors": result.errors},
      )
      return result

    # --- 4. Deallocate DynamoDB registry ---
    await self._deallocate_registry(graph_id, result)

    # --- 5. Purge staged uploads BEFORE the PG records that index them ---
    # user-staging/{user_id}/{graph_id}/ is the last customer-data store, and
    # the GraphUser rows are the only record of which users hold objects there.
    # _clean_pg_records drops those rows, after which the platform can no longer
    # enumerate what it still holds — so this must run first.
    self._purge_staged_uploads(graph_id, session, result)

    # --- 6. Clean PostgreSQL records ---
    self._clean_pg_records(graph_id, session, result)

    # --- 7. Update subscription metadata ---
    self._update_subscription_metadata(graph_id, session, result)

    # --- 8. Transition status (soft-delete stamp landed in step 0) ---
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

      # Idempotency for the stranded-graph retry: the sensor re-selects a graph
      # whose database delete keeps failing every cycle, and a fresh full S3
      # dump on each pass buys nothing. If THIS teardown already produced a
      # completed final backup — one created since `deleted_at` was stamped at
      # step 0 (an older backup would be a nightly/on-demand one, not the final
      # snapshot) — reuse it and skip the re-dump.
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

      # Purge staged uploads before the PG records that index them (see the
      # parent path), then clean subgraph PG records.
      self._purge_staged_uploads(subgraph.graph_id, session, result)
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
    """Delete the parent graph database.

    A 404 (the .lbug is already absent) counts as success. The invariant that
    gates the rest of teardown is *the file is not on the instance*, not *we
    were the one who removed it*: run #1 can delete the file and the Dagster
    daemon can restart before the status flip, and then every retry would get
    the same 404 and strand the graph forever. Delete is idempotent on the
    outcome that matters.
    """
    from ...graph_api.client.exceptions import GraphAPIError

    try:
      from ...graph_api.client.factory import get_graph_client

      graph_client = await get_graph_client(graph_id=graph_id, operation_type="write")
      try:
        await graph_client.delete_database(graph_id)
        result.database_deleted = True
        logger.info(f"Deleted database for graph {graph_id}")
      finally:
        await graph_client.close()
    except GraphAPIError as e:
      if getattr(e, "status_code", None) == 404:
        result.database_deleted = True
        logger.info(
          f"Database for graph {graph_id} already absent; treating delete as "
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

  def _dispose_residual_data(self, graph_id: str, result: DeprovisionResult) -> None:
    """The three data-disposal steps that are safe to repeat."""
    self._drop_extensions_schema(graph_id, result)
    self._purge_search_index(graph_id, result)
    self._purge_report_bundles(graph_id, result)

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

  def _purge_report_bundles(self, graph_id: str, result: DeprovisionResult) -> None:
    """Delete the tenant's published report artifacts from object storage.

    Report bundles are the serialized publications of a customer's reports —
    financial statements in JSON-LD and XBRL — and they were the last customer
    data store teardown never reached. Their prefix deliberately carries no
    lifecycle rule: a clock there would destroy the artifact of a live report
    whose row still exists, which is why `delete_report_artifacts` deletes them
    on withdrawal instead. Absence of a clock was correct; absence of a teardown
    delete was not, and it left a departed tenant's statements in the bucket
    indefinitely.

    Deletes the whole `report-bundles/{graph_id}/` prefix, so every report,
    generation and flavor goes together. The final backup lives under a
    different prefix and is untouched.

    Best-effort like its siblings — a storage failure records a warning rather
    than stranding the capacity release — but an object that fails to delete is
    recorded as an error, because incomplete disposal is the one outcome this
    step exists to prevent.
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
      # ``iter_object_keys`` paginates and raises on list failure. The
      # single-page ``list_objects`` treats both an S3 error and a
      # truncated listing as "prefix empty", which is the one outcome
      # this step exists to prevent.
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
    """Delete the graph's staged uploads from the user-data bucket.

    The prefix is ``user-staging/{user_id}/{graph_id}/`` — raw source files a
    customer uploaded before ingestion, held only by a bucket lifecycle rule.
    Must run before ``_clean_pg_records`` drops the GraphUser rows: those rows
    are the only record of which users have objects under this graph, so once
    they are gone the objects can no longer be enumerated. Best-effort — a
    failure records a warning without stranding teardown, and the lifecycle
    rule remains the backstop.
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

      # Members are recorded on the PARENT graph — a subgraph carries no
      # GraphUser row of its own (access to a subgraph is the parent's grant).
      # So resolve the parent id for the membership lookup, but key the S3
      # prefix on the graph_id we were handed: a file uploaded directly against
      # a subgraph lives under user-staging/{user_id}/{subgraph_id}/.
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

      # Drop the members' cached access decisions with their rows: a warm
      # positive entry would otherwise let a member's request past the auth
      # dependency for up to the cache TTL, onto a graph whose schema and
      # database are being dropped underneath it. Best-effort — the cache
      # helpers log and swallow their own failures.
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
