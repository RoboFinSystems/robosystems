"""Admin API for user management."""

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query, Request, status
from sqlalchemy import func

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.admin import require_admin
from ...models.api.admin import (
  UserActivityResponse,
  UserAPIKeyResponse,
  UserDeletionBlockerResponse,
  UserDeletionResponse,
  UserGraphAccessResponse,
  UserRepositoryAccessResponse,
  UserResponse,
  UserStatusResponse,
)
from ...models.core import Graph, GraphUser, OrgUser, User
from ...models.core.graph.graph_credits import (
  CreditTransactionType,
  GraphCreditTransaction,
)
from ...models.core.graph.graph_usage import GraphUsage, UsageEventType
from ...models.core.user.user_api_key import UserAPIKey
from ...models.core.user.user_repository import UserRepository
from ...operations.admin import (
  UserDeletionBlocked,
  UserNotFound,
  execute_user_deletion,
  plan_user_deletion,
  set_user_active,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/users", tags=["admin-users"])


@router.get("", response_model=list[UserResponse])
@require_admin(permissions=["users:read"])
async def list_users(
  request: Request,
  email: str | None = Query(None, description="Filter by email (partial match)"),
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


@router.get("/{user_id}/graphs", response_model=list[UserGraphAccessResponse])
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
  "/{user_id}/repositories", response_model=list[UserRepositoryAccessResponse]
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


@router.get("/{user_id}/api-keys", response_model=list[UserAPIKeyResponse])
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

    start_of_month = datetime.now(UTC).replace(
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


async def _set_active(
  request: Request, user_id: str, active: bool
) -> UserStatusResponse:
  session = next(get_db_session())
  try:
    try:
      change = set_user_active(
        user_id,
        active,
        session,
        actor=f"admin:{request.state.admin.get('name', 'unknown')}",
      )
    except UserNotFound:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User {user_id} not found",
      )

    logger.info(
      f"Admin {'activated' if active else 'deactivated'} user {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "changed": change.changed,
        "api_keys_revoked": change.api_keys_revoked,
        "api_keys_failed": change.api_keys_failed,
      },
    )

    return UserStatusResponse(
      user_id=change.user_id,
      email=change.email,
      is_active=change.is_active,
      changed=change.changed,
      api_keys_revoked=change.api_keys_revoked,
      api_keys_failed=change.api_keys_failed,
    )
  finally:
    session.close()


@router.post("/{user_id}/deactivate", response_model=UserStatusResponse)
@require_admin(permissions=["users:write"])
async def deactivate_user(request: Request, user_id: str):
  """Deactivate a user, cutting off both interactive and programmatic access.

  Invalidates every session and revokes every API key. This is the escalation
  above a password change — that rotates a credential and invalidates sessions,
  but deliberately leaves keys alone so routine rotation doesn't break
  integrations. Use this when an account is compromised or must be suspended.

  Safe to re-run: key revocation is best-effort per key, so a repeat call
  finishes anything a partial failure left behind.
  """
  return await _set_active(request, user_id, active=False)


@router.post("/{user_id}/activate", response_model=UserStatusResponse)
@require_admin(permissions=["users:write"])
async def activate_user(request: Request, user_id: str):
  """Reactivate a deactivated user.

  Restores sign-in only. Revoked API keys are **not** reinstated — the user
  must issue new ones.
  """
  return await _set_active(request, user_id, active=True)


@router.delete("/{user_id}", response_model=UserDeletionResponse)
@require_admin(permissions=["users:write"])
async def delete_user(
  request: Request,
  user_id: str,
  dry_run: bool = Query(False, description="Assess the deletion without performing it"),
):
  """Delete a user account, freeing its email address.

  Refuses while the user's organization still holds live graphs, subscriptions
  in force, or active repository access — those must be resolved first. Billing
  and audit history is retained with the actor de-referenced.
  """
  session = next(get_db_session())
  try:
    try:
      plan = (
        plan_user_deletion(user_id, session)
        if dry_run
        else execute_user_deletion(
          user_id,
          session,
          actor=f"admin:{request.state.admin.get('name', 'unknown')}",
        )
      )
    except UserNotFound:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User {user_id} not found",
      )
    except UserDeletionBlocked as blocked:
      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
          "message": "User cannot be deleted yet",
          "blockers": [{"code": b.code, "detail": b.detail} for b in blocked.blockers],
        },
      )

    deleted = not dry_run and plan.can_delete

    logger.info(
      f"Admin {'assessed' if dry_run else 'deleted'} user {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "dry_run": dry_run,
        "blockers": [b.code for b in plan.blockers],
      },
    )

    return UserDeletionResponse(
      user_id=plan.user_id,
      email=plan.email,
      deleted=deleted,
      dry_run=dry_run,
      blockers=[
        UserDeletionBlockerResponse(code=b.code, detail=b.detail) for b in plan.blockers
      ],
      removals=plan.removals,
      orgs_deleted=plan.orgs_to_delete,
      orgs_retained=plan.orgs_retained,
    )
  finally:
    session.close()
