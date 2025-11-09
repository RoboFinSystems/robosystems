"""Admin API for user management."""

from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Request, HTTPException, status, Query
from sqlalchemy import func

from ...database import get_db_session
from ...models.iam import User, Graph, GraphUser, OrgUser
from ...models.iam.user_api_key import UserAPIKey
from ...models.iam.user_repository import UserRepository
from ...models.iam.graph_credits import GraphCreditTransaction, CreditTransactionType
from ...models.iam.graph_usage import GraphUsage, UsageEventType
from ...models.api.admin import (
  UserResponse,
  UserGraphAccessResponse,
  UserRepositoryAccessResponse,
  UserAPIKeyResponse,
  UserActivityResponse,
)
from ...middleware.auth.admin import require_admin
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/users", tags=["admin-users"])


@router.get("", response_model=List[UserResponse])
@require_admin(permissions=["users:read"])
async def list_users(
  request: Request,
  email: Optional[str] = Query(None, description="Filter by email (partial match)"),
  verified_only: bool = Query(False, description="Only show verified users"),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all users with optional filters."""
  session = next(get_db_session())
  try:
    query = session.query(User).join(OrgUser, User.id == OrgUser.user_id)

    if email:
      query = query.filter(User.email.ilike(f"%{email}%"))

    if verified_only:
      query = query.filter(User.email_verified)

    total = query.count()
    users = query.offset(offset).limit(limit).all()

    results = []
    for user in users:
      org_user = session.query(OrgUser).filter(OrgUser.user_id == user.id).first()

      results.append(
        UserResponse(
          id=user.id,
          email=user.email,
          name=user.name,
          email_verified=user.email_verified,
          org_id=org_user.org_id if org_user else "",
          org_role=org_user.role if org_user else "MEMBER",
          created_at=user.created_at,
          updated_at=user.updated_at,
          last_login_at=None,
        )
      )

    logger.info(
      f"Admin listed {len(results)} users",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
        "filters": {
          "email": email,
          "verified_only": verified_only,
        },
      },
    )

    return results
  finally:
    session.close()


@router.get("/{user_id}", response_model=UserResponse)
@require_admin(permissions=["users:read"])
async def get_user(request: Request, user_id: str):
  """Get detailed information about a specific user."""
  session = next(get_db_session())
  try:
    user = session.query(User).filter(User.id == user_id).first()

    if not user:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User {user_id} not found",
      )

    org_user = session.query(OrgUser).filter(OrgUser.user_id == user.id).first()

    logger.info(
      f"Admin retrieved user {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
      },
    )

    return UserResponse(
      id=user.id,
      email=user.email,
      name=user.name,
      email_verified=user.email_verified,
      org_id=org_user.org_id if org_user else "",
      org_role=org_user.role if org_user else "MEMBER",
      created_at=user.created_at,
      updated_at=user.updated_at,
      last_login_at=None,
    )
  finally:
    session.close()


@router.get("/{user_id}/graphs", response_model=List[UserGraphAccessResponse])
@require_admin(permissions=["users:read"])
async def get_user_graphs(request: Request, user_id: str):
  """Get all graphs accessible by a user."""
  session = next(get_db_session())
  try:
    user = session.query(User).filter(User.id == user_id).first()

    if not user:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User {user_id} not found",
      )

    graph_accesses = session.query(GraphUser).filter(GraphUser.user_id == user_id).all()

    results = []
    for access in graph_accesses:
      graph = session.query(Graph).filter(Graph.graph_id == access.graph_id).first()
      if graph:
        latest_usage = (
          session.query(GraphUsage)
          .filter(
            GraphUsage.graph_id == graph.graph_id,
            GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
          )
          .order_by(GraphUsage.recorded_at.desc())
          .first()
        )

        results.append(
          UserGraphAccessResponse(
            graph_id=graph.graph_id,
            graph_name=graph.graph_name,
            role=access.role,
            graph_tier=graph.graph_tier,
            storage_gb=float(latest_usage.storage_gb)
            if latest_usage and latest_usage.storage_gb
            else None,
            created_at=access.created_at,
          )
        )

    logger.info(
      f"Admin retrieved graphs for user {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "graph_count": len(results),
      },
    )

    return results
  finally:
    session.close()


@router.get(
  "/{user_id}/repositories", response_model=List[UserRepositoryAccessResponse]
)
@require_admin(permissions=["users:read"])
async def get_user_repositories(request: Request, user_id: str):
  """Get all shared repositories accessible by a user."""
  session = next(get_db_session())
  try:
    user = session.query(User).filter(User.id == user_id).first()

    if not user:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User {user_id} not found",
      )

    repo_accesses = (
      session.query(UserRepository).filter(UserRepository.user_id == user_id).all()
    )

    results = []
    for access in repo_accesses:
      results.append(
        UserRepositoryAccessResponse(
          repository_name=access.repository_name,
          access_level=access.access_level,
          granted_at=access.created_at,
          expires_at=access.expires_at,
        )
      )

    logger.info(
      f"Admin retrieved repositories for user {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "repository_count": len(results),
      },
    )

    return results
  finally:
    session.close()


@router.get("/{user_id}/api-keys", response_model=List[UserAPIKeyResponse])
@require_admin(permissions=["users:read"])
async def get_user_api_keys(request: Request, user_id: str):
  """Get metadata for user's API keys (not the actual key values)."""
  session = next(get_db_session())
  try:
    user = session.query(User).filter(User.id == user_id).first()

    if not user:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User {user_id} not found",
      )

    api_keys = (
      session.query(UserAPIKey)
      .filter(
        UserAPIKey.user_id == user_id,
        ~UserAPIKey.revoked,
      )
      .all()
    )

    results = []
    for key in api_keys:
      results.append(
        UserAPIKeyResponse(
          key_id=key.id,
          name=key.name,
          prefix=key.key_prefix,
          scopes=key.scopes or [],
          last_used_at=key.last_used_at,
          created_at=key.created_at,
          expires_at=key.expires_at,
        )
      )

    logger.info(
      f"Admin retrieved API keys for user {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "key_count": len(results),
      },
    )

    return results
  finally:
    session.close()


@router.get("/{user_id}/activity", response_model=UserActivityResponse)
@require_admin(permissions=["users:read"])
async def get_user_activity(request: Request, user_id: str):
  """Get user's recent activity summary."""
  session = next(get_db_session())
  try:
    user = session.query(User).filter(User.id == user_id).first()

    if not user:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User {user_id} not found",
      )

    start_of_month = datetime.now(timezone.utc).replace(
      day=1, hour=0, minute=0, second=0, microsecond=0
    )

    user_graphs = [
      g.graph_id
      for g in session.query(GraphUser).filter(GraphUser.user_id == user_id).all()
    ]

    credit_usage_query = (
      session.query(func.sum(GraphCreditTransaction.amount))
      .filter(
        GraphCreditTransaction.user_id == user_id,
        GraphCreditTransaction.transaction_type
        == CreditTransactionType.CONSUMPTION.value,
        GraphCreditTransaction.created_at >= start_of_month,
      )
      .scalar()
    )
    credit_usage_month = abs(float(credit_usage_query or 0))

    storage_usage = 0.0
    for graph_id in user_graphs:
      latest_usage = (
        session.query(GraphUsage)
        .filter(
          GraphUsage.graph_id == graph_id,
          GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
        )
        .order_by(GraphUsage.recorded_at.desc())
        .first()
      )
      if latest_usage and latest_usage.storage_gb:
        storage_usage += float(latest_usage.storage_gb)

    repositories_accessed = [
      r.repository_name
      for r in session.query(UserRepository)
      .filter(UserRepository.user_id == user_id)
      .all()
    ]

    recent_logins = []

    logger.info(
      f"Admin retrieved activity for user {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "credit_usage_month": credit_usage_month,
      },
    )

    return UserActivityResponse(
      user_id=user_id,
      recent_logins=recent_logins,
      recent_api_calls=0,
      graphs_accessed=user_graphs,
      repositories_accessed=repositories_accessed,
      credit_usage_month=credit_usage_month,
      storage_usage_gb=storage_usage,
    )
  finally:
    session.close()
