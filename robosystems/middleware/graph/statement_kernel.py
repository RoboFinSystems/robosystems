"""Transport-independent authorization for graph statements.

REST, MCP and Operators share one gauntlet: write detection, the write policy
(main graph and shared repository read-only, subgraph read+write), per-engine
validation, and role-based write access. Rate limiting, execution and
streaming stay with the callers.
"""

import logging
from dataclasses import dataclass
from enum import Enum

from fastapi import HTTPException
from sqlalchemy.orm import Session
from starlette import status as http_status

from robosystems.middleware.graph.utils import MultiTenantUtils, parse_subgraph_id
from robosystems.models.core import User
from robosystems.security.cypher_analyzer import (
  find_guarded_string_match,
  has_opaque_statement_call,
  is_admin_operation,
  is_bulk_operation,
  is_schema_ddl,
  is_write_operation,
)

logger = logging.getLogger(__name__)


class SharedRepositoryReadRefused(HTTPException):
  """A read refused by a shared repository's declared query limits."""

  telemetry_signal = "string_match_refused"


def shared_repository_read_refusal(graph_id: str, statement: str) -> str | None:
  """Why a shared repository refuses this read (per its manifest's
  ``guarded_string_properties``), or None. Also used by read-graph-cypher,
  whose paths can bypass the kernel."""
  from robosystems.config.shared_repositories import (
    get_manifest,
    is_shared_repository_or_subgraph,
    resolve_shared_repository_parent,
  )

  if not is_shared_repository_or_subgraph(graph_id.lower()):
    return None
  manifest = get_manifest(resolve_shared_repository_parent(graph_id.lower()))
  if manifest is None or not manifest.guarded_string_properties:
    return None

  match = find_guarded_string_match(statement, manifest.guarded_string_properties)
  if match is None:
    return None

  label, _, prop = match.guarded_property.partition(".")
  reason = (
    f"String matching (CONTAINS, STARTS WITH, ENDS WITH, =~) on "
    f"`{match.guarded_property}` is not available through Cypher on the "
    f"'{manifest.id}' repository."
  )
  if not match.label_resolved:
    reason += (
      f" The node carrying `{prop}` has no label in this statement; if it is "
      f"not a {label}, state its label in the MATCH pattern."
    )
  if manifest.guarded_string_guidance:
    reason += f" {manifest.guarded_string_guidance}"
  return reason


class StatementEngine(str, Enum):
  """Query language a statement is written in."""

  CYPHER = "cypher"
  SQL = "sql"


@dataclass(frozen=True)
class StatementAuthorization:
  """Result of authorizing a statement against a graph.

  ``access_type`` is the string the repository layer expects ("read"/"write").
  """

  is_write: bool
  access_type: str
  is_subgraph: bool


class StatementKernel:
  """Authorizes a statement for a graph, independent of transport/engine."""

  def authorize(
    self,
    *,
    engine: StatementEngine,
    graph_id: str,
    statement: str,
    user: User,
    session: Session,
  ) -> StatementAuthorization:
    """Raise ``HTTPException`` (403/400) on a violation, else the write disposition."""
    if engine is StatementEngine.CYPHER:
      return self._authorize_cypher(graph_id, statement, user, session)
    if engine is StatementEngine.SQL:
      return self._authorize_sql(graph_id, user)
    raise NotImplementedError(f"StatementKernel: engine {engine!r} not wired yet")

  def _authorize_cypher(
    self, graph_id: str, statement: str, user: User, session: Session
  ) -> StatementAuthorization:
    # A string payload (CALL GQL('...')) is opaque to the analyzer, which masks
    # literals, so it is refused on every graph before any other check.
    if has_opaque_statement_call(statement):
      logger.warning(
        f"User {user.id} attempted an opaque statement call on {graph_id}: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Procedures that execute a statement passed as a string "
        "(e.g. CALL GQL(...)), and CALLs whose procedure name is quoted, are "
        "not allowed through the query endpoints. Submit the statement "
        "directly, with an unquoted procedure name, so it can be authorized.",
      )

    is_write = is_write_operation(statement)
    access_type = "write" if is_write else "read"

    is_subgraph = parse_subgraph_id(graph_id) is not None

    if is_write and not is_subgraph:
      logger.warning(
        f"User {user.id} attempted write operation through query endpoint on main graph: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Write operations (CREATE, MERGE, SET, DELETE) are not allowed on main graphs. "
        "The query endpoint is read-only for main graphs. Use the staging pipeline to load data:\n"
        "1. Create file upload: POST /v1/graphs/{graph_id}/tables/{table_name}/files\n"
        "2. Ingest to graph: POST /v1/graphs/{graph_id}/tables/ingest\n"
        "This ensures data integrity and enables pipeline benefits (audit, rollback, validation).\n"
        "Note: Subgraphs support write operations for scratch/workspace use (e.g. agent memory).",
      )

    if is_write and is_subgraph:
      logger.info(
        f"User {user.id} executing write operation on subgraph {graph_id}: {statement[:100]}"
      )

    if is_bulk_operation(statement):
      logger.warning(
        f"User {user.id} attempted bulk operation through query endpoint: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        detail="Bulk operations (COPY, LOAD, IMPORT) are not allowed through the query endpoint. "
        "Please use the staging pipeline for data ingestion.",
      )

    if is_admin_operation(statement):
      logger.warning(
        f"User {user.id} attempted admin operation through query endpoint: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Administrative operations (EXPORT, IMPORT DATABASE, INSTALL, ATTACH, etc.) require admin privileges.",
      )

    if is_schema_ddl(statement):
      logger.warning(
        f"User {user.id} attempted schema DDL through query endpoint: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Schema DDL operations (CREATE/DROP/ALTER TABLE, etc.) are not allowed. "
        "Graph schemas are immutable after creation to ensure consistency with staging tables.",
      )

    if is_write and MultiTenantUtils.is_shared_repository_or_subgraph(graph_id.lower()):
      logger.warning(f"User {user.id} attempted write on shared repository {graph_id}")
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail=f"Write operations not allowed on shared repository '{graph_id}'",
      )

    refusal = shared_repository_read_refusal(graph_id, statement)
    if refusal:
      logger.warning(f"User {user.id} sent a refused read on {graph_id}: {refusal}")
      raise SharedRepositoryReadRefused(
        status_code=http_status.HTTP_400_BAD_REQUEST, detail=refusal
      )

    # A write here targets a non-shared subgraph; viewers are read-only.
    if is_write:
      from robosystems.models.core.graph.graph_user import GraphUser

      if not GraphUser.user_has_write_access(user.id, graph_id, session):
        logger.warning(
          f"User {user.id} with read-only role attempted write on {graph_id}"
        )
        raise HTTPException(
          status_code=http_status.HTTP_403_FORBIDDEN,
          detail="Write operations require the 'member' or 'admin' role for this "
          "graph. Your access is read-only (viewer).",
        )

    return StatementAuthorization(
      is_write=is_write, access_type=access_type, is_subgraph=is_subgraph
    )

  def _authorize_sql(self, graph_id: str, user: User) -> StatementAuthorization:
    # SQL is read-only until a sandboxed DuckDB write connection exists.
    # Shared repositories have no user columnar tables, so SQL is refused there.
    if MultiTenantUtils.is_shared_repository_or_subgraph(graph_id.lower()):
      logger.warning(
        f"User {user.id} attempted SQL query on shared repository {graph_id}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Shared repositories do not allow direct SQL queries. "
        "Use POST /query/cypher to access shared repository data through the "
        "structured graph interface.",
      )
    is_subgraph = parse_subgraph_id(graph_id) is not None
    return StatementAuthorization(
      is_write=False, access_type="read", is_subgraph=is_subgraph
    )


statement_kernel = StatementKernel()
