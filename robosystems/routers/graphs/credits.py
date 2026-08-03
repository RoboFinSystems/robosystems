"""Graph credit management endpoints."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.api.billing.credits import (
  CreditSummaryResponse,
  DetailedTransactionsResponse,
  EnhancedCreditTransactionResponse,
  TransactionSummaryResponse,
)
from robosystems.models.api.common import (
  RESOURCE_ERROR_RESPONSES,
  ErrorCode,
  create_error_response,
)
from robosystems.models.core import GraphUser, User
from robosystems.models.core.graph.graph_credits import CreditTransactionType
from robosystems.operations.graph.credit_service import CreditService

logger = logging.getLogger(__name__)


def get_graph_access(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
) -> GraphUser:
  from robosystems.middleware.graph.utils import MultiTenantUtils
  from robosystems.models.core.user.user_repository import UserRepository

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
    role, implicit = GraphUser.get_effective_role(str(current_user.id), graph_id, db)
    if role is None:
      logger.warning(
        f"User {current_user.id} attempted access to user graph {graph_id} without permission"
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Access denied to user graph {graph_id}",
      )

    user_graph = (
      db.query(GraphUser)
      .filter(GraphUser.user_id == str(current_user.id), GraphUser.graph_id == graph_id)
      .first()
    )
    if user_graph:
      return user_graph

    # Implicit access (org owner/admin, or parent-graph grant on a subgraph):
    # synthesize the GraphUser interface the credits system expects.
    user_graph = GraphUser()
    user_graph.user_id = str(current_user.id)
    user_graph.graph_id = graph_id
    user_graph.role = role.value
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


@router.get(
  "",
  response_model=CreditSummaryResponse,
  summary="Get Credit Summary",
  operation_id="getCreditSummary",
  responses={**RESOURCE_ERROR_RESPONSES},
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
  operation_id="listCreditTransactions",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_credit_transactions(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  transaction_type: str | None = Query(
    None,
    description="Filter by transaction type (allocation, consumption, bonus, refund)",
    example="consumption",
  ),
  operation_type: str | None = Query(
    None,
    description="Filter by operation type (e.g., entity_lookup, cypher_query)",
  ),
  start_date: str | None = Query(
    None,
    description="Start date for filtering (ISO format: YYYY-MM-DD)",
  ),
  end_date: str | None = Query(
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
  from datetime import datetime

  from sqlalchemy import func

  from ...middleware.graph.utils import MultiTenantUtils
  from ...models.core.graph.graph_credits import GraphCreditTransaction
  from ...models.core.user.user_repository_credits import (
    UserRepositoryCredits,
    UserRepositoryCreditTransaction,
  )

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
      from ...models.core.user.user_repository_credits import (
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
