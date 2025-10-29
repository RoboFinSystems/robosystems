"""
Graph-scoped billing API endpoints.

Provides endpoints for:
- Credit-based billing reporting
- Storage charges (GB/month)
- Historical billing per database
- Subscription management
- Usage analytics and reporting

Note: These endpoints DO NOT consume credits as they are for billing transparency
and administrative purposes.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from robosystems.security import (
  handle_exception_securely,
  raise_secure_error,
  ErrorType,
)

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.iam import (
  User,
  GraphUsageTracking,
)
from robosystems.operations.graph.pricing_service import GraphPricingService
from robosystems.models.api.common import ErrorResponse

logger = logging.getLogger(__name__)

router = APIRouter(
  tags=["Graph Billing"],
  dependencies=[Depends(get_current_user_with_graph)],
)


@router.get(
  "/current",
  summary="Get Current Bill",
  description="""Get current month's billing details for the graph.

Returns comprehensive billing information including:
- **Credit Usage**: Consumed vs. allocated credits
- **Storage Charges**: Current storage usage and costs
- **Subscription Tier**: Current plan and features
- **Pro-rated Charges**: If plan changed mid-month
- **Estimated Total**: Current charges to date

Billing calculations are updated hourly. Storage is measured in GB-months.

ℹ️ No credits are consumed for viewing billing information.""",
  operation_id="getCurrentGraphBill",
  responses={
    200: {
      "description": "Current bill retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg1a2b3c",
            "period": "2024-01",
            "subscription_tier": "enterprise",
            "credit_allocation": 1500,
            "credits_consumed": 850,
            "storage_gb": 25.5,
            "storage_charge": 12.75,
            "total_charge": 62.75,
          }
        }
      },
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
    500: {"description": "Failed to calculate bill", "model": ErrorResponse},
  },
)
async def get_current_graph_bill(
  graph_id: str = Path(..., description="Graph database identifier"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict:
  """
  Get current month's bill for a specific graph database.

  Provides real-time billing information for the current billing period.

  Args:
      graph_id: The graph to get billing for
      current_user: The authenticated user
      db: Database session

  Returns:
      Dict with current billing details

  Raises:
      HTTPException: If graph not found or calculation fails
  """
  try:
    pricing_service = GraphPricingService(db)
    now = datetime.now()
    bill = pricing_service.calculate_graph_monthly_bill(
      user_id=current_user.id,
      graph_id=graph_id,
      year=now.year,
      month=now.month,
    )
    return bill
  except ValueError as e:
    raise_secure_error(
      ErrorType.NOT_FOUND_ERROR,
      original_error=e,
      additional_context={"operation": "get_current_bill"},
    )
  except Exception as e:
    handle_exception_securely(e, additional_context={"operation": "get_current_bill"})


@router.get(
  "/usage",
  summary="Get Usage Details",
  description="""Get detailed usage metrics for the graph.

Provides granular usage information including:
- **Daily Credit Consumption**: Track credit usage patterns
- **Storage Growth**: Monitor database size over time
- **Operation Breakdown**: Credits by operation type
- **Peak Usage Times**: Identify high-activity periods
- **API Call Volumes**: Request counts and patterns

Useful for:
- Optimizing credit consumption
- Capacity planning
- Usage trend analysis
- Cost optimization

ℹ️ No credits are consumed for viewing usage details.""",
  operation_id="getGraphUsageDetails",
  responses={
    200: {
      "description": "Usage details retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg1a2b3c",
            "period": "2024-01",
            "total_credits_consumed": 850,
            "daily_usage": [
              {"date": "2024-01-01", "credits": 25},
              {"date": "2024-01-02", "credits": 30},
            ],
            "operation_breakdown": {"query": 450, "mcp_call": 300, "import": 100},
            "storage_gb_days": 765.0,
          }
        }
      },
    },
    400: {"description": "Invalid year or month", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
    500: {"description": "Failed to retrieve usage", "model": ErrorResponse},
  },
)
async def get_graph_usage_details(
  graph_id: str = Path(..., description="Graph database identifier"),
  year: Optional[int] = Query(
    None, description="Year (defaults to current)", ge=2024, le=2030
  ),
  month: Optional[int] = Query(
    None, description="Month (defaults to current)", ge=1, le=12
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict:
  """
  Get detailed usage information for a specific graph database.

  Provides comprehensive usage metrics for analysis and optimization.

  Args:
      graph_id: The graph to get usage for
      year: Year to retrieve (optional)
      month: Month to retrieve (optional)
      current_user: The authenticated user
      db: Database session

  Returns:
      Dict with detailed usage metrics

  Raises:
      HTTPException: If invalid date or graph not found
  """
  # Default to current month if not specified
  if year is None or month is None:
    now = datetime.now()
    year = year or now.year
    month = month or now.month

  # Validate year/month
  if year < 2024 or year > 2030:
    raise HTTPException(status_code=400, detail="Invalid year")
  if month < 1 or month > 12:
    raise HTTPException(status_code=400, detail="Invalid month")

  try:
    # Get usage records for this specific graph
    usage_records = (
      db.query(GraphUsageTracking)
      .filter(
        GraphUsageTracking.user_id == current_user.id,
        GraphUsageTracking.graph_id == graph_id,
        GraphUsageTracking.billing_year == year,
        GraphUsageTracking.billing_month == month,
      )
      .order_by(GraphUsageTracking.recorded_at)
      .all()
    )

    if not usage_records:
      return {
        "graph_id": graph_id,
        "period": {"year": year, "month": month},
        "total_gb_hours": 0,
        "avg_size_gb": 0,
        "max_size_gb": 0,
        "total_queries": 0,
        "measurement_count": 0,
        "hourly_usage": [],
      }

    # Calculate metrics
    total_gb_hours = sum(record.size_gb for record in usage_records)
    total_queries = sum(record.query_count for record in usage_records)
    max_size_gb = max(record.size_gb for record in usage_records)
    avg_size_gb = total_gb_hours / len(usage_records) if usage_records else 0

    # Get hourly breakdown
    hourly_usage = [
      {
        "timestamp": record.recorded_at.isoformat(),
        "size_gb": record.size_gb,
        "query_count": record.query_count,
        "hour_of_day": record.billing_hour,
      }
      for record in usage_records[-168:]  # Last 7 days of hourly data
    ]

    return {
      "graph_id": graph_id,
      "period": {"year": year, "month": month},
      "total_gb_hours": total_gb_hours,
      "avg_size_gb": avg_size_gb,
      "max_size_gb": max_size_gb,
      "total_queries": total_queries,
      "measurement_count": len(usage_records),
      "hourly_usage": hourly_usage,
    }

  except Exception as e:
    raise HTTPException(
      status_code=500, detail=f"Failed to get usage details: {str(e)}"
    )


@router.get(
  "/history",
  summary="Get Billing History",
  description="""Get billing history for the graph.

Returns a chronological list of monthly bills, perfect for:
- Tracking spending trends over time
- Identifying usage patterns
- Budget forecasting
- Financial reporting

Each month includes:
- Credit usage and overages
- Storage charges
- Total charges
- Usage metrics

ℹ️ No credits are consumed for viewing billing history.""",
  operation_id="getGraphBillingHistory",
  responses={
    200: {
      "description": "Billing history retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg1a2b3c",
            "months_returned": 6,
            "history": [
              {"period": "2024-01", "total_charge": 62.75, "credits_consumed": 850},
              {"period": "2023-12", "total_charge": 57.50, "credits_consumed": 1200},
            ],
          }
        }
      },
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
    500: {"description": "Failed to retrieve history", "model": ErrorResponse},
  },
)
async def get_graph_billing_history(
  graph_id: str = Path(..., description="Graph database identifier"),
  months: int = Query(
    6, ge=1, le=24, description="Number of months to retrieve (1-24)"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict:
  """
  Get billing history for a specific graph database.

  Retrieves up to 24 months of billing history.

  Args:
      graph_id: The graph to get history for
      months: Number of months to retrieve
      current_user: The authenticated user
      db: Database session

  Returns:
      Dict with billing history

  Raises:
      HTTPException: If graph not found
  """
  now = datetime.now()
  bills = []
  pricing_service = GraphPricingService(db)

  for i in range(months):
    target_date = now - timedelta(days=i * 30)  # Approximate month
    year = target_date.year
    month = target_date.month

    try:
      bill = pricing_service.calculate_graph_monthly_bill(
        user_id=current_user.id,
        graph_id=graph_id,
        year=year,
        month=month,
      )
      bills.append(bill)
    except Exception:
      # No data for this month, skip
      continue

  return {
    "graph_id": graph_id,
    "months_requested": months,
    "months_available": len(bills),
    "billing_history": bills,
  }


@router.get(
  "/history/{year}/{month}",
  summary="Get Monthly Bill",
  description="""Get billing details for a specific month.

Retrieve historical billing information for any previous month.
Useful for:
- Reconciling past charges
- Tracking usage trends
- Expense reporting
- Budget analysis

Returns the same detailed breakdown as the current bill endpoint.

ℹ️ No credits are consumed for viewing billing history.""",
  operation_id="getGraphMonthlyBill",
  responses={
    200: {
      "description": "Monthly bill retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg1a2b3c",
            "period": "2023-12",
            "subscription_tier": "standard",
            "credit_allocation": 1000,
            "credits_consumed": 1200,
            "overage_credits": 200,
            "storage_gb": 15.0,
            "storage_charge": 7.50,
            "total_charge": 57.50,
          }
        }
      },
    },
    400: {"description": "Invalid year or month", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {
      "description": "Graph not found or no data for period",
      "model": ErrorResponse,
    },
    500: {"description": "Failed to calculate bill", "model": ErrorResponse},
  },
)
async def get_graph_monthly_bill(
  year: int = Path(..., description="Year (2024-2030)", ge=2024, le=2030),
  month: int = Path(..., description="Month (1-12)", ge=1, le=12),
  graph_id: str = Path(..., description="Graph database identifier"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict:
  """
  Get bill for a specific month for a specific graph database.

  Retrieves complete billing information for any past month.

  Args:
      year: The year to retrieve
      month: The month (1-12)
      graph_id: The graph to get billing for
      current_user: The authenticated user
      db: Database session

  Returns:
      Dict with monthly billing details

  Raises:
      HTTPException: If invalid date or graph not found
  """
  # Validate year/month
  if year < 2024 or year > 2030:
    raise HTTPException(status_code=400, detail="Invalid year")
  if month < 1 or month > 12:
    raise HTTPException(status_code=400, detail="Invalid month")

  try:
    pricing_service = GraphPricingService(db)
    bill = pricing_service.calculate_graph_monthly_bill(
      user_id=current_user.id,
      graph_id=graph_id,
      year=year,
      month=month,
    )
    return bill
  except ValueError as e:
    raise_secure_error(
      ErrorType.NOT_FOUND_ERROR,
      original_error=e,
      additional_context={
        "operation": "get_monthly_bill",
        "year": year,
        "month": month,
      },
    )
  except Exception as e:
    handle_exception_securely(
      e,
      additional_context={
        "operation": "get_monthly_bill",
        "year": year,
        "month": month,
      },
    )
