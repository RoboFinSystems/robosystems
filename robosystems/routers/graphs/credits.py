"""
Graph credit management API endpoints.

Provides endpoints for:
- Viewing credit balances and usage
- Managing credit allocations
- Viewing transaction history
- Checking credit requirements
"""

import logging
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.iam import User, GraphUser
from robosystems.models.iam.graph_credits import CreditTransactionType
from robosystems.operations.graph.credit_service import (
  CreditService,
  get_operation_cost,
)
from robosystems.models.api.common import (
  ErrorResponse,
  ErrorCode,
  create_error_response,
)
from robosystems.models.api.billing.credits import (
  CreditSummaryResponse,
  StorageLimitResponse,
  EnhancedCreditTransactionResponse,
  TransactionSummaryResponse,
  DetailedTransactionsResponse,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN

logger = logging.getLogger(__name__)


def get_graph_access(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
) -> GraphUser:
  """Get user's access to a graph with proper authorization validation."""
  from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
  from robosystems.models.iam.user_repository import UserRepository

  # Determine graph type and validate access accordingly
  identity = MultiTenantUtils.get_graph_identity(graph_id)

  if identity.is_shared_repository:
    # Check shared repository access
    if not UserRepository.user_has_access(str(current_user.id), graph_id, db):
      logger.warning(
        f"User {current_user.id} attempted access to shared repository {graph_id} without permission"
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Access denied to shared repository {graph_id}",
      )

    # For shared repositories, create a synthetic GraphUser object
    # since credits system expects GraphUser interface
    user_graph = GraphUser()
    user_graph.user_id = str(current_user.id)
    user_graph.graph_id = graph_id
    user_graph.role = "reader"  # Default role for shared repositories
    return user_graph

  elif identity.is_user_graph:
    # Check user graph access
    if not GraphUser.user_has_access(str(current_user.id), graph_id, db):
      logger.warning(
        f"User {current_user.id} attempted access to user graph {graph_id} without permission"
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Access denied to user graph {graph_id}",
      )

    # Get the actual user graph relationship
    user_graph = (
      db.query(GraphUser)
      .filter(GraphUser.user_id == str(current_user.id), GraphUser.graph_id == graph_id)
      .first()
    )
    if not user_graph:
      # This should not happen if user_has_access returned True, but safety check
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Access validation failed",
      )

    return user_graph

  else:
    # Unknown graph type
    logger.error(f"Unknown graph type for graph_id: {graph_id}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Invalid graph identifier: {graph_id}",
    )


router = APIRouter(
  prefix="/credits",
  tags=["Credits"],
)

# Credit API models moved to robosystems.models.api.credits


@router.get(
  "/summary",
  response_model=CreditSummaryResponse,
  summary="Get Credit Summary",
  description="""Retrieve comprehensive credit usage summary for the specified graph.

This endpoint provides:
- Current credit balance and monthly allocation
- Credit consumption metrics for the current month
- Graph tier and credit multiplier information
- Usage percentage to help monitor credit consumption

No credits are consumed for checking credit status.""",
  operation_id="getCreditSummary",
  responses={
    200: {
      "description": "Credit summary retrieved successfully",
      "model": CreditSummaryResponse,
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Credit pool not found for graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve credit summary", "model": ErrorResponse},
  },
)
async def get_credit_summary(
  graph_id: str = Path(
    ...,
    description="Graph database identifier (e.g., 'kg1a2b3c' or 'sec')",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  user_graph: GraphUser = Depends(get_graph_access),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> CreditSummaryResponse:
  """
  Get comprehensive credit summary for a graph.

  This endpoint retrieves detailed credit usage information including
  current balance, monthly allocation, and consumption metrics.

  Args:
      graph_id: The graph to get credit summary for
      current_user: The authenticated user
      user_graph: User's access to the graph
      db: Database session

  Returns:
      CreditSummaryResponse: Detailed credit usage information

  Raises:
      HTTPException: If credit pool not found or access denied
  """
  try:
    credit_service = CreditService(db)
    summary = credit_service.get_credit_summary(graph_id, user_id=str(current_user.id))

    if "error" in summary:
      raise create_error_response(
        status_code=404, detail=summary["error"], code=ErrorCode.NOT_FOUND
      )

    return CreditSummaryResponse(**summary)

  except HTTPException:
    # Re-raise HTTP exceptions (like our 404)
    raise
  except Exception as e:
    logger.error(f"Failed to get credit summary for graph {graph_id}: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to retrieve credit summary",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "/transactions",
  response_model=DetailedTransactionsResponse,
  summary="List Credit Transactions",
  description="""Retrieve detailed credit transaction history for the specified graph.

This enhanced endpoint provides:
- Detailed transaction records with idempotency information
- Summary by operation type to identify high-consumption operations
- Date range filtering for analysis
- Metadata search capabilities

Transaction types include:
- ALLOCATION: Monthly credit allocations
- CONSUMPTION: Credit usage for operations
- BONUS: Bonus credits added by admins
- REFUND: Credit refunds

No credits are consumed for viewing transaction history.""",
  operation_id="listCreditTransactions",
  responses={
    200: {
      "description": "Transaction history retrieved successfully",
      "model": DetailedTransactionsResponse,
    },
    400: {"description": "Invalid transaction type filter", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve transactions", "model": ErrorResponse},
  },
)
async def get_credit_transactions(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  transaction_type: Optional[str] = Query(
    None,
    description="Filter by transaction type (allocation, consumption, bonus, refund)",
    example="consumption",
  ),
  operation_type: Optional[str] = Query(
    None,
    description="Filter by operation type (e.g., entity_lookup, cypher_query)",
  ),
  start_date: Optional[str] = Query(
    None,
    description="Start date for filtering (ISO format: YYYY-MM-DD)",
  ),
  end_date: Optional[str] = Query(
    None,
    description="End date for filtering (ISO format: YYYY-MM-DD)",
  ),
  limit: int = Query(
    100,
    ge=1,
    le=1000,
    description="Maximum number of transactions to return",
  ),
  offset: int = Query(
    0,
    ge=0,
    description="Number of transactions to skip",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  user_graph: GraphUser = Depends(get_graph_access),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> DetailedTransactionsResponse:
  """
  Get detailed credit transaction history for a graph or repository.

  Retrieves a comprehensive list of credit transactions with filtering
  and summary capabilities to help analyze credit consumption patterns.

  Works for both user graphs and shared repositories.
  """
  from datetime import datetime
  from sqlalchemy import func
  from ...models.iam.graph_credits import GraphCreditTransaction
  from ...models.iam.user_repository_credits import (
    UserRepositoryCreditTransaction,
    UserRepositoryCredits,
  )
  from ...middleware.graph.multitenant_utils import MultiTenantUtils

  try:
    # Determine if this is a repository or user graph
    identity = MultiTenantUtils.get_graph_identity(graph_id)
    user_repo_credits = None

    if identity.is_shared_repository:
      # Query repository credit transactions
      # Find the user's repository credit pool
      user_repo_credits = UserRepositoryCredits.get_user_repository_credits(
        str(current_user.id), graph_id, db
      )

      if not user_repo_credits:
        # No credit pool found for this user/repository
        return DetailedTransactionsResponse(
          transactions=[],
          summary={},
          total_count=0,
          filtered_count=0,
          date_range={"start": start_date or "all", "end": end_date or "all"},
        )

      # Build query for repository transactions
      query = db.query(UserRepositoryCreditTransaction).filter(
        UserRepositoryCreditTransaction.credit_pool_id == user_repo_credits.id
      )
    else:
      # Build query for user graph transactions
      query = db.query(GraphCreditTransaction).filter(
        GraphCreditTransaction.graph_id == graph_id
      )

    # Apply filters
    start_dt = None
    end_dt = None

    # Get the transaction model class for filtering
    TransactionModel = (
      UserRepositoryCreditTransaction
      if identity.is_shared_repository
      else GraphCreditTransaction
    )

    if transaction_type:
      query = query.filter(TransactionModel.transaction_type == transaction_type)

    if start_date:
      start_dt = datetime.fromisoformat(start_date)
      query = query.filter(TransactionModel.created_at >= start_dt)

    if end_date:
      end_dt = datetime.fromisoformat(end_date)
      query = query.filter(TransactionModel.created_at <= end_dt)

    # Filter by operation type if specified
    if operation_type:
      from sqlalchemy import cast
      from sqlalchemy.dialects.postgresql import JSONB

      query = query.filter(
        cast(TransactionModel.transaction_metadata, JSONB)["operation_type"].astext
        == operation_type
      )

    # Get total count before pagination
    total_count = query.count()

    # Apply pagination and ordering
    transactions = (
      query.order_by(TransactionModel.created_at.desc())
      .offset(offset)
      .limit(limit)
      .all()
    )

    # Get summary by operation type
    from sqlalchemy import cast
    from sqlalchemy.dialects.postgresql import JSONB

    if identity.is_shared_repository:
      # For repositories, use repository credit transactions
      from ...models.iam.user_repository_credits import (
        UserRepositoryCreditTransactionType,
      )

      assert user_repo_credits is not None

      operation_type_expr = cast(
        UserRepositoryCreditTransaction.transaction_metadata, JSONB
      )["operation_type"].astext

      summary_query = db.query(
        operation_type_expr.label("operation_type"),
        func.sum(UserRepositoryCreditTransaction.amount).label("total_amount"),
        func.count(UserRepositoryCreditTransaction.id).label("transaction_count"),
        func.avg(UserRepositoryCreditTransaction.amount).label("average_amount"),
        func.min(UserRepositoryCreditTransaction.created_at).label("first_transaction"),
        func.max(UserRepositoryCreditTransaction.created_at).label("last_transaction"),
      ).filter(
        UserRepositoryCreditTransaction.credit_pool_id == user_repo_credits.id,
        UserRepositoryCreditTransaction.transaction_type
        == UserRepositoryCreditTransactionType.CONSUMPTION.value,
      )

      # Apply same date filters to summary
      if start_dt is not None:
        summary_query = summary_query.filter(
          UserRepositoryCreditTransaction.created_at >= start_dt
        )
      if end_dt is not None:
        summary_query = summary_query.filter(
          UserRepositoryCreditTransaction.created_at <= end_dt
        )

      summary_results = summary_query.group_by(operation_type_expr).all()
    else:
      # For user graphs, use graph credit transactions
      operation_type_expr = cast(GraphCreditTransaction.transaction_metadata, JSONB)[
        "operation_type"
      ].astext

      summary_query = db.query(
        operation_type_expr.label("operation_type"),
        func.sum(GraphCreditTransaction.amount).label("total_amount"),
        func.count(GraphCreditTransaction.id).label("transaction_count"),
        func.avg(GraphCreditTransaction.amount).label("average_amount"),
        func.min(GraphCreditTransaction.created_at).label("first_transaction"),
        func.max(GraphCreditTransaction.created_at).label("last_transaction"),
      ).filter(
        GraphCreditTransaction.graph_id == graph_id,
        GraphCreditTransaction.transaction_type
        == CreditTransactionType.CONSUMPTION.value,
      )

      # Apply same date filters to summary
      if start_dt is not None:
        summary_query = summary_query.filter(
          GraphCreditTransaction.created_at >= start_dt
        )
      if end_dt is not None:
        summary_query = summary_query.filter(
          GraphCreditTransaction.created_at <= end_dt
        )

      summary_results = summary_query.group_by(operation_type_expr).all()

    # Build response
    transaction_list = []
    for txn in transactions:
      metadata = txn.get_metadata()

      # Repository transactions don't have these fields, use None as default
      transaction_list.append(
        EnhancedCreditTransactionResponse(
          id=txn.id,
          type=txn.transaction_type,
          amount=float(txn.amount),
          description=txn.description,
          metadata=metadata,
          created_at=txn.created_at.isoformat(),
          operation_id=getattr(txn, "operation_id", None),
          idempotency_key=getattr(txn, "idempotency_key", None),
          request_id=getattr(txn, "request_id", None),
          user_id=getattr(txn, "user_id", None),
        )
      )

    # Build summary
    summary = {}
    for row in summary_results:
      if row.operation_type:  # Skip null operation types
        summary[row.operation_type] = TransactionSummaryResponse(
          operation_type=row.operation_type,
          total_amount=abs(float(row.total_amount or 0)),
          transaction_count=row.transaction_count,
          average_amount=abs(float(row.average_amount or 0)),
          first_transaction=row.first_transaction.isoformat()
          if row.first_transaction
          else None,
          last_transaction=row.last_transaction.isoformat()
          if row.last_transaction
          else None,
        )

    # Determine date range
    date_range = {"start": start_date or "all", "end": end_date or "all"}

    return DetailedTransactionsResponse(
      transactions=transaction_list,
      summary=summary,
      total_count=total_count,
      filtered_count=len(transactions),
      date_range=date_range,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get credit transactions for graph {graph_id}: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to retrieve credit transactions",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "/balance/check",
  summary="Check Credit Balance",
  description="""Check if the graph has sufficient credits for a planned operation.

This endpoint allows you to verify credit availability before performing
an operation, helping prevent failed operations due to insufficient credits.

The check considers:
- Base operation cost
- Graph tier multiplier
- Current credit balance

No credits are consumed for checking availability.""",
  operation_id="checkCreditBalance",
  responses={
    200: {
      "description": "Credit check completed",
      "content": {
        "application/json": {
          "example": {
            "has_sufficient_credits": True,
            "required_credits": 1.5,
            "available_credits": 1000.0,
            "base_cost": 1.0,
            "multiplier": 1.5,
            "cached": False,
          }
        }
      },
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Credit pool not found", "model": ErrorResponse},
    500: {"description": "Credit check failed", "model": ErrorResponse},
  },
)
async def check_credit_balance(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  operation_type: str = Query(..., description="Type of operation to check"),
  base_cost: Optional[Decimal] = Query(
    None, description="Custom base cost (uses default if not provided)"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  user_graph: GraphUser = Depends(get_graph_access),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict:
  """
  Check if graph has sufficient credits for an operation.

  This endpoint performs a pre-flight check to verify credit availability
  without consuming any credits.

  Args:
      request: Operation details to check credits for
      graph_id: The graph to check credits for
      current_user: The authenticated user
      user_graph: User's access to the graph
      db: Database session

  Returns:
      Dict with credit availability information

  Raises:
      HTTPException: If credit pool not found or access denied
  """
  try:
    credit_service = CreditService(db)

    # Determine the cost
    if base_cost is None:
      base_cost = get_operation_cost(operation_type)

    result = credit_service.check_credit_balance(
      graph_id=graph_id, required_credits=base_cost
    )

    if "error" in result:
      raise create_error_response(
        status_code=404, detail=result["error"], code=ErrorCode.NOT_FOUND
      )

    return result

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to check credit balance for graph {graph_id}: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to check credit balance",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "/storage/usage",
  summary="Get Storage Usage",
  description="""Get storage usage history for a graph.

Returns detailed storage usage information including:
- Daily average storage consumption
- Storage trends over time
- Credit costs for storage
- Current storage billing information

This endpoint helps users understand their storage patterns
and associated credit costs.""",
  operation_id="getStorageUsage",
  responses={
    200: {
      "description": "Storage usage retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg1a2b3c",
            "graph_tier": "kuzu-large",
            "storage_multiplier": 1.0,
            "current_period": {
              "start_date": "2024-01-01",
              "end_date": "2024-01-31",
              "total_storage_credits": 45.0,
              "average_daily_storage_gb": 18.0,
              "storage_days": 31,
            },
            "recent_usage": [
              {
                "date": "2024-01-30",
                "average_storage_gb": 18.5,
                "credits_charged": 0.925,
                "flat_pricing": True,
              }
            ],
          }
        }
      },
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve storage usage", "model": ErrorResponse},
  },
)
async def get_storage_usage(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  days: int = Query(
    30, ge=1, le=365, description="Number of days of history to return"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  user_graph: GraphUser = Depends(get_graph_access),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict:
  """
  Get storage usage history for a graph.

  Args:
      graph_id: The graph to get storage usage for
      days: Number of days of history to return
      current_user: The authenticated user
      user_graph: User's access to the graph
      db: Database session

  Returns:
      Dict with storage usage details

  Raises:
      HTTPException: If access denied or retrieval fails
  """
  try:
    from datetime import datetime, timedelta
    from sqlalchemy import func
    from ...models.iam.graph_usage import GraphUsage, UsageEventType

    # Get storage usage records for the period
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    usage_records = (
      db.query(
        func.date(GraphUsage.recorded_at).label("usage_date"),
        func.avg(GraphUsage.storage_gb).label("avg_storage_gb"),
        func.count(GraphUsage.id).label("measurement_count"),
      )
      .filter(
        GraphUsage.graph_id == graph_id,
        GraphUsage.event_type == UsageEventType.STORAGE_SNAPSHOT.value,
        GraphUsage.recorded_at >= start_date,
        GraphUsage.recorded_at <= end_date,
      )
      .group_by(func.date(GraphUsage.recorded_at))
      .order_by(func.date(GraphUsage.recorded_at).desc())
      .all()
    )

    # Calculate credit costs
    from robosystems.operations.graph.credit_service import get_operation_cost
    from robosystems.models.iam import GraphCredits

    credits = GraphCredits.get_by_graph_id(graph_id, db)
    base_storage_cost = float(get_operation_cost("storage_daily"))
    # Storage uses flat pricing (no tier multiplier)
    storage_multiplier = 1.0

    # Process usage records
    recent_usage = []
    total_storage_credits = 0.0

    for record in usage_records:
      if record.avg_storage_gb:
        credits_charged = base_storage_cost * record.avg_storage_gb
        total_storage_credits += credits_charged

        recent_usage.append(
          {
            "date": record.usage_date.isoformat(),
            "average_storage_gb": round(record.avg_storage_gb, 2),
            "measurement_count": record.measurement_count,
            "credits_charged": round(credits_charged, 4),
            "flat_pricing": True,
          }
        )

    # Calculate summary stats
    avg_daily_storage = (
      sum(r["average_storage_gb"] for r in recent_usage) / len(recent_usage)
      if recent_usage
      else 0
    )

    return {
      "graph_id": graph_id,
      "graph_tier": credits.graph_tier if credits else "unknown",
      "storage_multiplier": storage_multiplier,  # Always 1.0 for flat pricing
      "base_storage_cost_per_gb": base_storage_cost,
      "period": {
        "start_date": start_date.date().isoformat(),
        "end_date": end_date.date().isoformat(),
        "days_requested": days,
        "days_with_data": len(recent_usage),
      },
      "summary": {
        "total_storage_credits": round(total_storage_credits, 2),
        "average_daily_storage_gb": round(avg_daily_storage, 2),
        "estimated_monthly_storage_credits": round(
          avg_daily_storage * base_storage_cost * 30, 2
        ),
      },
      "recent_usage": recent_usage,
    }

  except Exception as e:
    logger.error(f"Failed to get storage usage for graph {graph_id}: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to retrieve storage usage",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "/storage/limits",
  response_model=StorageLimitResponse,
  summary="Check Storage Limits",
  description="""Check storage limits and usage for a graph.

Returns comprehensive storage limit information including:
- Current storage usage
- Effective limit (override or default)
- Usage percentage and warnings
- Recommendations for limit management

This endpoint helps users monitor storage usage and plan for potential
limit increases. No credits are consumed for checking storage limits.""",
  operation_id="checkStorageLimits",
  responses={
    200: {
      "description": "Storage limit information retrieved successfully",
      "model": StorageLimitResponse,
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg1a2b3c",
            "current_storage_gb": 450.5,
            "effective_limit_gb": 500.0,
            "usage_percentage": 90.1,
            "within_limit": True,
            "approaching_limit": True,
            "needs_warning": True,
            "has_override": False,
            "recommendations": [
              "Monitor storage usage closely",
              "Plan for potential storage limit increase",
              "Review current data retention policies",
            ],
          }
        }
      },
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "No credit pool found for graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve storage limits", "model": ErrorResponse},
  },
)
async def check_storage_limits(
  graph_id: str = Path(
    ...,
    description="Graph database identifier (e.g., 'kg1a2b3c' or 'sec')",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  user_graph: GraphUser = Depends(get_graph_access),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> StorageLimitResponse:
  """
  Check storage limits and usage for a graph.

  This endpoint provides comprehensive storage limit information including
  current usage, effective limits, and usage warnings.

  Args:
      graph_id: The graph to check storage limits for
      current_user: The authenticated user
      user_graph: User's access to the graph
      db: Database session

  Returns:
      StorageLimitResponse: Storage limit information and recommendations

  Raises:
      HTTPException: If credit pool not found or access denied
  """
  try:
    credit_service = CreditService(db)
    limit_info = credit_service.check_storage_limit(graph_id)

    if "error" in limit_info:
      raise create_error_response(
        status_code=404, detail=limit_info["error"], code=ErrorCode.NOT_FOUND
      )

    return StorageLimitResponse(**limit_info)

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to check storage limits for graph {graph_id}: {e}")
    raise create_error_response(
      status_code=500,
      detail="Failed to check storage limits",
      code=ErrorCode.INTERNAL_ERROR,
    )
