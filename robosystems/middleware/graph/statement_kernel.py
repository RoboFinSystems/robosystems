"""Transport-independent authorization kernel for graph statements.

Extracted from the Cypher ``/query`` handler so REST (``/query/cypher``,
``/query/sql``), MCP, and Operators share **one** policy path — write
detection, the three-tier write policy (main-graph reads-only / subgraph
read+write / shared-repo reads-only), per-engine validation, and role-based
write access — instead of each surface re-implementing it inline and drifting.

See the content-ops cutover plan, Phase B. This module owns the authorization
gauntlet only; circuit-breaker checks, dual-layer rate limiting, repository
acquisition, execution-strategy selection, and streaming stay in the callers
(they are transport/engine-specific and position-sensitive in the hot path).
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
  is_admin_operation,
  is_bulk_operation,
  is_schema_ddl,
  is_write_operation,
)

logger = logging.getLogger(__name__)


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
    """Run the write-policy + validation + role gauntlet for a statement.

    Raises ``HTTPException`` (403/400) on any policy or validation violation,
    exactly as the Cypher handler did inline. Returns the write disposition
    for the caller to thread into logging and repository acquisition.
    """
    if engine is StatementEngine.CYPHER:
      return self._authorize_cypher(graph_id, statement, user, session)
    # SQL wired in Phase B2 (read-only, shared-repos blocked).
    raise NotImplementedError(f"StatementKernel: engine {engine!r} not wired yet")

  def _authorize_cypher(
    self, graph_id: str, statement: str, user: User, session: Session
  ) -> StatementAuthorization:
    # Analyze query
    is_write = is_write_operation(statement)
    access_type = "write" if is_write else "read"

    # Check if this is a subgraph (allows writes) or main graph (read-only)
    is_subgraph = parse_subgraph_id(graph_id) is not None

    # Block write operations for main graphs only - subgraphs allow writes
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

    # Log write operations on subgraphs for audit
    if is_write and is_subgraph:
      logger.info(
        f"User {user.id} executing write operation on subgraph {graph_id}: {statement[:100]}"
      )

    # Check for bulk operations (COPY, LOAD, IMPORT) - should never reach here due to write check above
    if is_bulk_operation(statement):
      logger.warning(
        f"User {user.id} attempted bulk operation through query endpoint: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        detail="Bulk operations (COPY, LOAD, IMPORT) are not allowed through the query endpoint. "
        "Please use the staging pipeline for data ingestion.",
      )

    # Check for admin operations (EXPORT, INSTALL, ATTACH, etc.)
    if is_admin_operation(statement):
      logger.warning(
        f"User {user.id} attempted admin operation through query endpoint: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Administrative operations (EXPORT, IMPORT DATABASE, INSTALL, ATTACH, etc.) require admin privileges.",
      )

    # Check for schema DDL operations (CREATE/DROP/ALTER TABLE, etc.)
    if is_schema_ddl(statement):
      logger.warning(
        f"User {user.id} attempted schema DDL through query endpoint: {statement[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Schema DDL operations (CREATE/DROP/ALTER TABLE, etc.) are not allowed. "
        "Graph schemas are immutable after creation to ensure consistency with staging tables.",
      )

    # Block writes on shared repositories
    if is_write and MultiTenantUtils.is_shared_repository_or_subgraph(graph_id):
      logger.warning(f"User {user.id} attempted write on shared repository {graph_id}")
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail=f"Write operations not allowed on shared repository '{graph_id}'",
      )

    # Enforce role-based write access on user subgraphs (viewer is read-only).
    # At this point a write is confirmed to target a non-shared subgraph.
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


statement_kernel = StatementKernel()
