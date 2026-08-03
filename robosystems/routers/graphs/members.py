"""Graph member management endpoints.

Grants are restricted to members of the graph's owning organization and attach
to parent graphs only (subgraphs inherit the parent's permissions). Org
owners/admins hold implicit graph admin and appear in listings as
source="org_role"; their access is managed through org roles, not here.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import get_logger
from robosystems.middleware.auth.cache import api_key_cache
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import general_api_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.members import (
  AddGraphMemberRequest,
  GraphMemberListResponse,
  GraphMemberResponse,
  UpdateGraphMemberRoleRequest,
)
from robosystems.models.core import Graph, GraphRole, GraphUser, OrgRole, OrgUser, User
from robosystems.security import SecurityAuditLogger

logger = get_logger(__name__)

router = APIRouter(prefix="/members", tags=["Graph Members"])


def _load_managed_graph(graph_id: str, db: Session) -> Graph:
  graph = Graph.get_by_id(graph_id, db)
  if graph is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )
  if graph.is_repository:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories are subscription-based and have no member management",
    )
  if graph.is_subgraph:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Subgraphs inherit the parent graph's permissions. Manage members on the parent graph.",
    )
  return graph


def _require_graph_admin(current_user: User, graph_id: str, db: Session) -> None:
  if not GraphUser.user_has_admin_access(current_user.id, graph_id, db):
    SecurityAuditLogger.log_authorization_denied(
      user_id=str(current_user.id),
      resource=f"user_graph:{graph_id}",
      action="manage_members",
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access to the graph is required to manage members",
    )


def _invalidate_target_caches(user_id: str) -> None:
  api_key_cache.invalidate_user_jwt_graph_access(user_id)
  api_key_cache.invalidate_user_data(user_id)


def _explicit_member_response(row: GraphUser, user: User) -> GraphMemberResponse:
  return GraphMemberResponse(
    user_id=user.id,
    name=user.name,
    email=user.email,
    role=GraphRole.coerce(row.role),
    source="explicit",
    granted_at=row.created_at,
  )


@router.get(
  "",
  response_model=GraphMemberListResponse,
  summary="List Graph Members",
  description="All users with access to the graph: explicit grants plus org owners/admins who hold implicit admin. Requires graph admin.",
  operation_id="listGraphMembers",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_graph_members(
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> GraphMemberListResponse:
  graph = _load_managed_graph(graph_id, db)
  _require_graph_admin(current_user, graph_id, db)

  members: list[GraphMemberResponse] = []
  explicit_user_ids: set[str] = set()

  for row in GraphUser.get_by_graph_id(graph_id, db):
    explicit_user_ids.add(row.user_id)
    members.append(_explicit_member_response(row, row.user))

  if graph.org_id:
    for org_user in OrgUser.get_org_users(graph.org_id, db):
      if org_user.role not in (OrgRole.OWNER, OrgRole.ADMIN):
        continue
      if org_user.user_id in explicit_user_ids:
        continue
      members.append(
        GraphMemberResponse(
          user_id=org_user.user_id,
          name=org_user.user.name,
          email=org_user.user.email,
          role=GraphRole.ADMIN,
          source="org_role",
          granted_at=None,
        )
      )

  return GraphMemberListResponse(members=members, total=len(members), graph_id=graph_id)


@router.post(
  "",
  response_model=GraphMemberResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Add Graph Member",
  description="Grant a member of the graph's organization access at a chosen role. Requires graph admin.",
  operation_id="addGraphMember",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def add_graph_member(
  request: AddGraphMemberRequest,
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> GraphMemberResponse:
  graph = _load_managed_graph(graph_id, db)
  _require_graph_admin(current_user, graph_id, db)

  target = User.get_by_id(request.user_id, db)
  if target is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="User not found",
    )

  if not graph.org_id or not OrgUser.get_by_org_and_user(graph.org_id, target.id, db):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Only members of the graph's organization can be granted access",
    )

  if GraphUser.get_by_user_and_graph(target.id, graph_id, db):
    raise HTTPException(
      status_code=status.HTTP_409_CONFLICT,
      detail="User already has access to this graph",
    )

  row = GraphUser.create(
    user_id=target.id,
    graph_id=graph_id,
    role=request.role,
    session=db,
  )
  _invalidate_target_caches(target.id)

  logger.info(
    f"User {current_user.id} granted {target.id} role {request.role.value} on graph {graph_id}"
  )
  return _explicit_member_response(row, target)


@router.put(
  "/{user_id}",
  response_model=GraphMemberResponse,
  summary="Update Graph Member Role",
  description="Change an explicit member's graph role. Requires graph admin. Implicit org owner/admin access is managed through org roles.",
  operation_id="updateGraphMemberRole",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def update_graph_member_role(
  user_id: str,
  request: UpdateGraphMemberRoleRequest,
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> GraphMemberResponse:
  _load_managed_graph(graph_id, db)
  _require_graph_admin(current_user, graph_id, db)

  row = GraphUser.get_by_user_and_graph(user_id, graph_id, db)
  if row is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=(
        "User has no explicit grant on this graph. "
        "Org owner/admin access is managed through organization roles."
      ),
    )

  row.update_role(request.role, db)
  _invalidate_target_caches(user_id)

  logger.info(
    f"User {current_user.id} set {user_id} role to {request.role.value} on graph {graph_id}"
  )
  return _explicit_member_response(row, row.user)


@router.delete(
  "/{user_id}",
  status_code=status.HTTP_204_NO_CONTENT,
  summary="Remove Graph Member",
  description="Revoke an explicit member's access to the graph. Requires graph admin.",
  operation_id="removeGraphMember",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def remove_graph_member(
  user_id: str,
  graph_id: str = Path(
    ..., description="Graph identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> None:
  _load_managed_graph(graph_id, db)
  _require_graph_admin(current_user, graph_id, db)

  row = GraphUser.get_by_user_and_graph(user_id, graph_id, db)
  if row is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="User has no explicit grant on this graph",
    )

  row.delete(db)
  _invalidate_target_caches(user_id)

  logger.info(f"User {current_user.id} revoked {user_id} access to graph {graph_id}")
