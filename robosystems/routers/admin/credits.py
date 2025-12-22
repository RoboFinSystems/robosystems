"""Admin API for credit management."""

from datetime import UTC, datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query, Request, status
from sqlalchemy import func

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.admin import require_admin
from ...models.api.admin import (
  BonusCreditsRequest,
  CreditAnalyticsResponse,
  CreditHealthResponse,
  CreditPoolResponse,
  RepositoryCreditPoolResponse,
)
from ...models.iam import User
from ...models.iam.graph_credits import (
  CreditTransactionType,
  GraphCredits,
  GraphCreditTransaction,
)
from ...models.iam.user_repository import UserRepository
from ...models.iam.user_repository_credits import (
  UserRepositoryCredits,
  UserRepositoryCreditTransaction,
  UserRepositoryCreditTransactionType,
)
from ...operations.graph.credit_service import CreditService

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/credits", tags=["admin-credits"])


@router.get("/graphs", response_model=list[CreditPoolResponse])
@require_admin(permissions=["credits:read"])
async def list_graph_credit_pools(
  request: Request,
  user_email: str | None = Query(None, description="Filter by user email"),
  tier: str | None = Query(None, description="Filter by graph tier"),
  low_balance_only: bool = Query(
    False, description="Only show pools with balance < 10% of allocation"
  ),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all graph credit pools with optional filters."""
  session = next(get_db_session())
  try:
    query = session.query(GraphCredits)

    if tier:
      query = query.filter(GraphCredits.graph_tier == tier)

    if user_email:
      query = query.join(User, GraphCredits.user_id == User.id)
      query = query.filter(User.email.ilike(f"%{user_email}%"))

    if low_balance_only:
      query = query.filter(
        GraphCredits.current_balance < (GraphCredits.monthly_allocation * 0.1)
      )

    total = query.count()
    pools = query.offset(offset).limit(limit).all()

    results = []
    for pool in pools:
      results.append(
        CreditPoolResponse(
          graph_id=pool.graph_id,
          user_id=pool.user_id,
          graph_tier=pool.graph_tier,
          current_balance=float(pool.current_balance),
          monthly_allocation=float(pool.monthly_allocation),
          credit_multiplier=1.0,
          storage_limit_override_gb=float(pool.storage_override_gb)
          if pool.storage_override_gb
          else None,
          created_at=pool.created_at,
          updated_at=pool.updated_at,
        )
      )

    logger.info(
      f"Admin listed {len(results)} credit pools",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
        "filters": {
          "user_email": user_email,
          "tier": tier,
          "low_balance_only": low_balance_only,
        },
      },
    )

    return results
  finally:
    session.close()


@router.get("/graphs/{graph_id}", response_model=CreditPoolResponse)
@require_admin(permissions=["credits:read"])
async def get_graph_credit_pool(request: Request, graph_id: str):
  """Get detailed information about a specific graph credit pool."""
  session = next(get_db_session())
  try:
    pool = GraphCredits.get_by_graph_id(graph_id, session)

    if not pool:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Credit pool not found for graph {graph_id}",
      )

    logger.info(
      f"Admin retrieved credit pool for graph {graph_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "graph_id": graph_id,
      },
    )

    return CreditPoolResponse(
      graph_id=pool.graph_id,
      user_id=pool.user_id,
      graph_tier=pool.graph_tier,
      current_balance=float(pool.current_balance),
      monthly_allocation=float(pool.monthly_allocation),
      credit_multiplier=1.0,
      storage_limit_override_gb=float(pool.storage_override_gb)
      if pool.storage_override_gb
      else None,
      created_at=pool.created_at,
      updated_at=pool.updated_at,
    )
  finally:
    session.close()


@router.post("/graphs/{graph_id}/bonus", response_model=CreditPoolResponse)
@require_admin(permissions=["credits:write"])
async def add_bonus_credits_to_graph(
  request: Request, graph_id: str, data: BonusCreditsRequest
):
  """Add bonus credits to a graph credit pool."""
  session = next(get_db_session())
  try:
    pool = GraphCredits.get_by_graph_id(graph_id, session)

    if not pool:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Credit pool not found for graph {graph_id}",
      )

    credit_service = CreditService(session)

    metadata = data.metadata or {}
    metadata.update(
      {
        "source": "admin_api",
        "admin_key_id": request.state.admin_key_id,
        "admin_name": request.state.admin.get("name"),
      }
    )

    credit_service.add_bonus_credits(
      graph_id=graph_id,
      amount=Decimal(str(data.amount)),
      description=data.description,
      metadata=metadata,
    )

    session.refresh(pool)

    logger.info(
      f"Admin added {data.amount} bonus credits to graph {graph_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "graph_id": graph_id,
        "amount": data.amount,
        "description": data.description,
      },
    )

    return CreditPoolResponse(
      graph_id=pool.graph_id,
      user_id=pool.user_id,
      graph_tier=pool.graph_tier,
      current_balance=float(pool.current_balance),
      monthly_allocation=float(pool.monthly_allocation),
      credit_multiplier=1.0,
      storage_limit_override_gb=float(pool.storage_override_gb)
      if pool.storage_override_gb
      else None,
      created_at=pool.created_at,
      updated_at=pool.updated_at,
    )
  finally:
    session.close()


@router.get("/repositories", response_model=list[RepositoryCreditPoolResponse])
@require_admin(permissions=["credits:read"])
async def list_repository_credit_pools(
  request: Request,
  user_email: str | None = Query(None, description="Filter by user email"),
  repository_type: str | None = Query(None, description="Filter by repository type"),
  low_balance_only: bool = Query(
    False, description="Only show pools with balance < 10% of allocation"
  ),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all repository credit pools with optional filters."""
  session = next(get_db_session())
  try:
    query = session.query(UserRepositoryCredits).join(
      UserRepository, UserRepositoryCredits.user_repository_id == UserRepository.id
    )

    if repository_type:
      query = query.filter(UserRepository.repository_type == repository_type)

    if user_email:
      query = query.join(User, UserRepository.user_id == User.id)
      query = query.filter(User.email.ilike(f"%{user_email}%"))

    if low_balance_only:
      query = query.filter(
        UserRepositoryCredits.current_balance
        < (UserRepositoryCredits.monthly_allocation * Decimal("0.1"))
      )

    total = query.count()
    pools = query.offset(offset).limit(limit).all()

    results = []
    for pool in pools:
      results.append(
        RepositoryCreditPoolResponse(
          user_repository_id=pool.user_repository_id,
          user_id=pool.user_repository.user_id,
          repository_type=pool.user_repository.repository_type.value,
          repository_plan=pool.user_repository.repository_plan.value,
          current_balance=float(pool.current_balance),
          monthly_allocation=float(pool.monthly_allocation),
          consumed_this_month=float(pool.credits_consumed_this_month),
          allows_rollover=pool.allows_rollover,
          rollover_credits=float(pool.rollover_credits),
          is_active=pool.is_active,
          last_allocation_date=pool.last_allocation_date,
          next_allocation_date=pool.next_allocation_date,
          created_at=pool.created_at,
          updated_at=pool.updated_at,
        )
      )

    logger.info(
      f"Admin listed {len(results)} repository credit pools",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
        "filters": {
          "user_email": user_email,
          "repository_type": repository_type,
          "low_balance_only": low_balance_only,
        },
      },
    )

    return results
  finally:
    session.close()


@router.get(
  "/repositories/{user_repository_id}", response_model=RepositoryCreditPoolResponse
)
@require_admin(permissions=["credits:read"])
async def get_repository_credit_pool(request: Request, user_repository_id: str):
  """Get detailed information about a specific repository credit pool."""
  session = next(get_db_session())
  try:
    pool = (
      session.query(UserRepositoryCredits)
      .filter(UserRepositoryCredits.user_repository_id == user_repository_id)
      .first()
    )

    if not pool:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Credit pool not found for user_repository {user_repository_id}",
      )

    logger.info(
      f"Admin retrieved repository credit pool for user_repository {user_repository_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_repository_id": user_repository_id,
      },
    )

    return RepositoryCreditPoolResponse(
      user_repository_id=pool.user_repository_id,
      user_id=pool.user_repository.user_id,
      repository_type=pool.user_repository.repository_type.value,
      repository_plan=pool.user_repository.repository_plan.value,
      current_balance=float(pool.current_balance),
      monthly_allocation=float(pool.monthly_allocation),
      consumed_this_month=float(pool.credits_consumed_this_month),
      allows_rollover=pool.allows_rollover,
      rollover_credits=float(pool.rollover_credits),
      is_active=pool.is_active,
      last_allocation_date=pool.last_allocation_date,
      next_allocation_date=pool.next_allocation_date,
      created_at=pool.created_at,
      updated_at=pool.updated_at,
    )
  finally:
    session.close()


@router.post(
  "/repositories/{user_repository_id}/bonus",
  response_model=RepositoryCreditPoolResponse,
)
@require_admin(permissions=["credits:write"])
async def add_bonus_credits_to_repository(
  request: Request, user_repository_id: str, data: BonusCreditsRequest
):
  """Add bonus credits to a repository credit pool."""
  session = next(get_db_session())
  try:
    pool = (
      session.query(UserRepositoryCredits)
      .filter(UserRepositoryCredits.user_repository_id == user_repository_id)
      .first()
    )

    if not pool:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Credit pool not found for user_repository {user_repository_id}",
      )

    metadata = data.metadata or {}
    metadata.update(
      {
        "source": "admin_api",
        "admin_key_id": request.state.admin_key_id,
        "admin_name": request.state.admin.get("name"),
      }
    )

    pool.current_balance += Decimal(str(data.amount))
    pool.updated_at = datetime.now(UTC)

    UserRepositoryCreditTransaction.create_transaction(
      credit_pool_id=pool.id,
      transaction_type=UserRepositoryCreditTransactionType.BONUS,
      amount=Decimal(str(data.amount)),
      description=data.description,
      metadata=metadata,
      session=session,
    )

    session.refresh(pool)

    logger.info(
      f"Admin added {data.amount} bonus credits to user_repository {user_repository_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_repository_id": user_repository_id,
        "amount": data.amount,
        "description": data.description,
      },
    )

    return RepositoryCreditPoolResponse(
      user_repository_id=pool.user_repository_id,
      user_id=pool.user_repository.user_id,
      repository_type=pool.user_repository.repository_type.value,
      repository_plan=pool.user_repository.repository_plan.value,
      current_balance=float(pool.current_balance),
      monthly_allocation=float(pool.monthly_allocation),
      consumed_this_month=float(pool.credits_consumed_this_month),
      allows_rollover=pool.allows_rollover,
      rollover_credits=float(pool.rollover_credits),
      is_active=pool.is_active,
      last_allocation_date=pool.last_allocation_date,
      next_allocation_date=pool.next_allocation_date,
      created_at=pool.created_at,
      updated_at=pool.updated_at,
    )
  finally:
    session.close()


@router.get("/analytics", response_model=CreditAnalyticsResponse)
@require_admin(permissions=["credits:read"])
async def get_credit_analytics(
  request: Request,
  tier: str | None = Query(None, description="Filter by tier"),
):
  """Get system-wide credit analytics for both graph and repository credits."""
  session = next(get_db_session())
  try:
    start_of_month = datetime.now(UTC).replace(
      day=1, hour=0, minute=0, second=0, microsecond=0
    )

    graph_query = session.query(GraphCredits)
    if tier:
      graph_query = graph_query.filter(GraphCredits.graph_tier == tier)
    graph_pools = graph_query.all()

    graph_total_pools = len(graph_pools)
    graph_total_allocated = sum(float(p.monthly_allocation) for p in graph_pools)
    graph_total_balance = sum(float(p.current_balance) for p in graph_pools)

    graph_consumed_query = (
      session.query(func.sum(GraphCreditTransaction.amount))
      .filter(
        GraphCreditTransaction.transaction_type
        == CreditTransactionType.CONSUMPTION.value,
        GraphCreditTransaction.created_at >= start_of_month,
      )
      .scalar()
    )
    graph_consumed_month = abs(float(graph_consumed_query or 0))

    graph_top_consumers_query = (
      session.query(
        GraphCreditTransaction.graph_id,
        func.sum(GraphCreditTransaction.amount).label("total_consumed"),
      )
      .filter(
        GraphCreditTransaction.transaction_type
        == CreditTransactionType.CONSUMPTION.value,
        GraphCreditTransaction.created_at >= start_of_month,
      )
      .group_by(GraphCreditTransaction.graph_id)
      .order_by(func.sum(GraphCreditTransaction.amount))
      .limit(10)
      .all()
    )

    graph_top_consumers = []
    for graph_id, consumed in graph_top_consumers_query:
      pool = GraphCredits.get_by_graph_id(graph_id, session)
      if pool:
        graph_top_consumers.append(
          {
            "graph_id": graph_id,
            "user_id": pool.user_id,
            "tier": pool.graph_tier,
            "consumed": abs(float(consumed)),
          }
        )

    graph_by_tier = {}
    for pool in graph_pools:
      tier_name = pool.graph_tier
      if tier_name not in graph_by_tier:
        graph_by_tier[tier_name] = {
          "pool_count": 0,
          "total_monthly_allocation": 0.0,
          "total_current_balance": 0.0,
        }
      graph_by_tier[tier_name]["pool_count"] += 1
      graph_by_tier[tier_name]["total_monthly_allocation"] += float(
        pool.monthly_allocation
      )
      graph_by_tier[tier_name]["total_current_balance"] += float(pool.current_balance)

    repo_pools = session.query(UserRepositoryCredits).all()
    repo_total_pools = len(repo_pools)
    repo_total_allocated = sum(float(p.monthly_allocation) for p in repo_pools)
    repo_total_balance = sum(float(p.current_balance) for p in repo_pools)

    repo_consumed_query = (
      session.query(func.sum(UserRepositoryCreditTransaction.amount))
      .filter(
        UserRepositoryCreditTransaction.transaction_type
        == UserRepositoryCreditTransactionType.CONSUMPTION.value,
        UserRepositoryCreditTransaction.created_at >= start_of_month,
      )
      .scalar()
    )
    repo_consumed_month = abs(float(repo_consumed_query or 0))

    repo_by_type = {}
    for pool in repo_pools:
      repo_type_str = pool.user_repository.repository_type.value
      if repo_type_str not in repo_by_type:
        repo_by_type[repo_type_str] = {
          "pool_count": 0,
          "total_monthly_allocation": 0.0,
          "total_current_balance": 0.0,
        }
      repo_by_type[repo_type_str]["pool_count"] += 1
      repo_by_type[repo_type_str]["total_monthly_allocation"] += float(
        pool.monthly_allocation
      )
      repo_by_type[repo_type_str]["total_current_balance"] += float(
        pool.current_balance
      )

    total_pools = graph_total_pools + repo_total_pools
    total_allocated = graph_total_allocated + repo_total_allocated
    total_balance = graph_total_balance + repo_total_balance
    total_consumed = graph_consumed_month + repo_consumed_month

    logger.info(
      "Admin retrieved credit analytics",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total_pools": total_pools,
        "graph_pools": graph_total_pools,
        "repo_pools": repo_total_pools,
      },
    )

    return CreditAnalyticsResponse(
      graph_credits={
        "total_pools": graph_total_pools,
        "total_allocated_monthly": graph_total_allocated,
        "total_current_balance": graph_total_balance,
        "total_consumed_month": graph_consumed_month,
        "top_consumers": graph_top_consumers,
        "by_tier": graph_by_tier,
      },
      repository_credits={
        "total_pools": repo_total_pools,
        "total_allocated_monthly": repo_total_allocated,
        "total_current_balance": repo_total_balance,
        "total_consumed_month": repo_consumed_month,
        "by_type": repo_by_type,
      },
      total_pools=total_pools,
      total_allocated_monthly=total_allocated,
      total_current_balance=total_balance,
      total_consumed_month=total_consumed,
    )
  finally:
    session.close()


@router.get("/health", response_model=CreditHealthResponse)
@require_admin(permissions=["credits:read"])
async def check_credit_health(request: Request):
  """Check health of both graph and repository credit systems."""
  session = next(get_db_session())
  try:
    graph_pools = session.query(GraphCredits).all()
    repo_pools = session.query(UserRepositoryCredits).all()

    graph_low_balance = []
    graph_negative_balance = []
    graph_allocation_issues = []

    for pool in graph_pools:
      balance = float(pool.current_balance)
      allocation = float(pool.monthly_allocation)

      if balance < 0:
        graph_negative_balance.append(
          {
            "graph_id": pool.graph_id,
            "user_id": pool.user_id,
            "balance": balance,
            "tier": pool.graph_tier,
          }
        )
      elif balance < allocation * 0.1:
        graph_low_balance.append(
          {
            "graph_id": pool.graph_id,
            "user_id": pool.user_id,
            "balance": balance,
            "allocation": allocation,
            "tier": pool.graph_tier,
          }
        )

      if allocation <= 0:
        graph_allocation_issues.append(
          {
            "graph_id": pool.graph_id,
            "user_id": pool.user_id,
            "allocation": allocation,
            "issue": "Zero or negative allocation",
          }
        )

    repo_low_balance = []
    repo_negative_balance = []
    repo_allocation_issues = []
    repo_inactive_pools = []

    for pool in repo_pools:
      balance = float(pool.current_balance)
      allocation = float(pool.monthly_allocation)

      if not pool.is_active:
        repo_inactive_pools.append(
          {
            "user_repository_id": pool.user_repository_id,
            "user_id": pool.user_repository.user_id,
            "repository_type": pool.user_repository.repository_type.value,
            "balance": balance,
          }
        )

      if balance < 0:
        repo_negative_balance.append(
          {
            "user_repository_id": pool.user_repository_id,
            "user_id": pool.user_repository.user_id,
            "repository_type": pool.user_repository.repository_type.value,
            "balance": balance,
          }
        )
      elif balance < allocation * 0.1:
        repo_low_balance.append(
          {
            "user_repository_id": pool.user_repository_id,
            "user_id": pool.user_repository.user_id,
            "repository_type": pool.user_repository.repository_type.value,
            "balance": balance,
            "allocation": allocation,
          }
        )

      if allocation <= 0:
        repo_allocation_issues.append(
          {
            "user_repository_id": pool.user_repository_id,
            "user_id": pool.user_repository.user_id,
            "allocation": allocation,
            "issue": "Zero or negative allocation",
          }
        )

    graph_issues = (
      len(graph_low_balance)
      + len(graph_negative_balance)
      + len(graph_allocation_issues)
    )
    repo_issues = (
      len(repo_low_balance)
      + len(repo_negative_balance)
      + len(repo_allocation_issues)
      + len(repo_inactive_pools)
    )

    total_issues = graph_issues + repo_issues
    total_pools = len(graph_pools) + len(repo_pools)

    status_value = "healthy"
    if graph_negative_balance or repo_negative_balance:
      status_value = "critical"
    elif total_issues > total_pools * 0.1:
      status_value = "warning"

    logger.info(
      "Admin checked credit health",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "status": status_value,
        "total_issues": total_issues,
        "graph_issues": graph_issues,
        "repo_issues": repo_issues,
      },
    )

    return CreditHealthResponse(
      status=status_value,
      graph_health={
        "total_pools": len(graph_pools),
        "pools_with_issues": graph_issues,
        "low_balance_pools": graph_low_balance,
        "negative_balance_pools": graph_negative_balance,
        "allocation_issues": graph_allocation_issues,
      },
      repository_health={
        "total_pools": len(repo_pools),
        "pools_with_issues": repo_issues,
        "low_balance_pools": repo_low_balance,
        "negative_balance_pools": repo_negative_balance,
        "allocation_issues": repo_allocation_issues,
        "inactive_pools": repo_inactive_pools,
      },
      total_pools=total_pools,
      pools_with_issues=total_issues,
      last_checked=datetime.now(UTC),
    )
  finally:
    session.close()
