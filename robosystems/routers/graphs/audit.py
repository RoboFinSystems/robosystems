"""Graph audit: every change made to a graph, and through which surface; and
the in-app AI operator runs behind the changes marked `operator`."""

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import general_api_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.audit import (
  MutationAuditListResponse,
  OperatorRunDetail,
  OperatorRunListResponse,
  OperatorThreadListResponse,
)
from robosystems.models.core import GraphUser, User
from robosystems.operations.graph.mutation_audit import (
  InvalidCursorError,
  list_mutation_audit,
)
from robosystems.operations.graph.operator_runs import (
  get_operator_run,
  list_operator_runs,
  list_operator_threads,
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


def _runs_scope(
  graph_id: str, requested_user_id: str | None, current_user: User, db: Session
) -> str | None:
  """The user filter a reader may apply: an admin reads every run on the
  graph; any other member reads their own. Refuses a non-member, and a
  member asking for someone else's."""
  if GraphUser.user_has_admin_access(current_user.id, graph_id, db):
    return requested_user_id
  if not GraphUser.user_has_access(current_user.id, graph_id, db):
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"user_graph:{graph_id}",
      action="read_operator_runs",
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Access to the graph is required to read its operator runs",
    )
  if requested_user_id and requested_user_id != str(current_user.id):
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"user_graph:{graph_id}",
      action="read_operator_runs",
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access to the graph is required to read other users' runs",
    )
  return str(current_user.id)


@router.get(
  "/threads",
  response_model=OperatorThreadListResponse,
  summary="List Operator Threads",
  description=(
    "The conversations held with this graph's AI operators, most recently "
    "active first: every run made under a `thread_id` grouped by it, with the "
    "first question as the title. A graph admin sees every user's threads; "
    "any other member sees their own. Open a thread's runs at `/audit/runs`."
  ),
  operation_id="listOperatorThreads",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_graph_operator_threads(
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  user_id: str | None = Query(
    None, description="Only this user's threads (admins; members see their own)"
  ),
  cursor: str | None = Query(
    None, description="The `next_cursor` of the previous page"
  ),
  limit: int = Query(50, ge=1, le=200, description="Threads per page"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OperatorThreadListResponse:
  scope = _runs_scope(graph_id, user_id, current_user, db)
  try:
    return list_operator_threads(
      db, graph_id, user_id=scope, cursor=cursor, limit=limit
    )
  except InvalidCursorError as exc:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get(
  "/runs",
  response_model=OperatorRunListResponse,
  summary="List Operator Runs",
  description=(
    "The in-app AI operator runs made on this graph, newest first: each run's "
    "question, answer, outcome, cost and the tools it called. This is the "
    "record behind a mutation audit entry's `operation_id`, kept after the "
    "operation itself has expired. A graph admin sees every user's runs; any "
    "other member sees their own. Filter by `thread_id` for one conversation."
  ),
  operation_id="listOperatorRuns",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_graph_operator_runs(
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  thread_id: str | None = Query(None, description="Only the runs of this thread"),
  user_id: str | None = Query(
    None, description="Only runs made as this user (admins; members see their own)"
  ),
  operator_type: str | None = Query(None, description="Only runs of this operator"),
  status_filter: Literal["completed", "failed", "cancelled"] | None = Query(
    None, alias="status", description="Only runs that ended this way"
  ),
  since: datetime | None = Query(
    None, description="Only runs started at or after this time"
  ),
  until: datetime | None = Query(
    None, description="Only runs started before this time"
  ),
  cursor: str | None = Query(
    None, description="The `next_cursor` of the previous page"
  ),
  limit: int = Query(50, ge=1, le=200, description="Runs per page"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OperatorRunListResponse:
  scope = _runs_scope(graph_id, user_id, current_user, db)
  try:
    return list_operator_runs(
      db,
      graph_id,
      thread_id=thread_id,
      user_id=scope,
      operator_type=operator_type,
      status=status_filter,
      since=since,
      until=until,
      cursor=cursor,
      limit=limit,
    )
  except InvalidCursorError as exc:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


@router.get(
  "/runs/{operation_id}",
  response_model=OperatorRunDetail,
  summary="Get Operator Run",
  description=(
    "One operator run with its full paper trail: the history it was seeded "
    "with, every model call with the tool calls it made and the results it "
    "read, the answer, and the response metadata. Readable by a graph admin "
    "or by the user who made the run."
  ),
  operation_id="getOperatorRun",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_graph_operator_run(
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  operation_id: str = Path(..., description="The run's operation id"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OperatorRunDetail:
  scope = _runs_scope(graph_id, None, current_user, db)
  run = get_operator_run(db, graph_id, operation_id)
  if run is None or (scope is not None and run.user_id != scope):
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
  return run
