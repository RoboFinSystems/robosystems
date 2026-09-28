"""Graph mutation audit: every change made to a graph, and through which surface."""

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import general_api_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.audit import MutationAuditListResponse
from robosystems.models.core import GraphUser, User
from robosystems.operations.graph.mutation_audit import (
  InvalidCursorError,
  list_mutation_audit,
)
from robosystems.security import SecurityAuditLogger

router = APIRouter(prefix="/audit", tags=["Graph Audit"])


@router.get(
  "/mutations",
  response_model=MutationAuditListResponse,
  summary="List Graph Mutations",
  description=(
    "Every call that changed this graph, newest first: REST operations, "
    "external MCP clients, and in-app AI operator runs. Each entry names the "
    "operation, the outcome, who made it and with which credential, and the "
    "objects it touched. Arguments are not stored, only their SHA-256 "
    "fingerprint. Requires graph admin."
  ),
  operation_id="listGraphMutations",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_graph_mutations(
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  surface: Literal["api", "mcp", "operator"] | None = Query(
    None, description="Only calls from this surface"
  ),
  operation_name: str | None = Query(
    None, description="Only this operation or MCP tool"
  ),
  user_id: str | None = Query(None, description="Only calls made as this user"),
  operation_id: str | None = Query(
    None, description="Only calls from this REST operation or operator run"
  ),
  since: datetime | None = Query(None, description="Only calls at or after this time"),
  until: datetime | None = Query(None, description="Only calls before this time"),
  cursor: str | None = Query(
    None, description="The `next_cursor` of the previous page"
  ),
  limit: int = Query(50, ge=1, le=200, description="Entries per page"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> MutationAuditListResponse:
  if not GraphUser.user_has_admin_access(current_user.id, graph_id, db):
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"user_graph:{graph_id}",
      action="read_mutation_audit",
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access to the graph is required to read its mutation audit",
    )
  try:
    return list_mutation_audit(
      db,
      graph_id,
      surface=surface,
      operation_name=operation_name,
      user_id=user_id,
      operation_id=operation_id,
      since=since,
      until=until,
      cursor=cursor,
      limit=limit,
    )
  except InvalidCursorError as exc:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
