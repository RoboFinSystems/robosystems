"""
Unified subscription management endpoints for graphs and repositories.

This endpoint handles subscriptions for both:
- User Graphs: Per-graph billing (resource_type="graph", resource_id=graph_id)
- Shared Repositories: Per-user billing (resource_type="repository", resource_id=repo_name)

The same endpoint structure works for both, with automatic detection of the resource type.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import subscription_aware_rate_limit_dependency
from ...models.iam import User
from ...models.billing import BillingCustomer, BillingSubscription
from ...models.api.billing.subscription import (
  GraphSubscriptionResponse,
  CreateRepositorySubscriptionRequest,
  UpgradeSubscriptionRequest,
  CancellationResponse,
)
from ...config import BillingConfig

logger = logging.getLogger(__name__)

router = APIRouter(
  tags=["Graph Subscriptions"],
  dependencies=[Depends(get_current_user)],
)

SHARED_REPOSITORIES = {"sec", "industry", "economic"}


def is_shared_repository(graph_id: str) -> bool:
  """Check if a graph_id refers to a shared repository."""
  return graph_id in SHARED_REPOSITORIES


def subscription_to_response(
  subscription: BillingSubscription,
) -> GraphSubscriptionResponse:
  """Convert subscription model to API response."""
  return GraphSubscriptionResponse(
    id=subscription.id,
    resource_type=subscription.resource_type,
    resource_id=subscription.resource_id,
    plan_name=subscription.plan_name,
    billing_interval=subscription.billing_interval,
    status=subscription.status,
    base_price_cents=subscription.base_price_cents,
    current_period_start=(
      subscription.current_period_start.isoformat()
      if subscription.current_period_start
      else None
    ),
    current_period_end=(
      subscription.current_period_end.isoformat()
      if subscription.current_period_end
      else None
    ),
    started_at=subscription.started_at.isoformat() if subscription.started_at else None,
    canceled_at=(
      subscription.canceled_at.isoformat() if subscription.canceled_at else None
    ),
    created_at=subscription.created_at.isoformat(),
  )


@router.get(
  "",
  response_model=GraphSubscriptionResponse,
  summary="Get Subscription",
  description="""Get subscription details for a graph or shared repository.

For user graphs (kg*): Returns the graph's subscription (owned by graph creator)
For shared repositories (sec, industry, etc.): Returns user's personal subscription to that repository

This unified endpoint automatically detects the resource type and returns the appropriate subscription.""",
  operation_id="getGraphSubscription",
  responses={
    200: {
      "description": "Subscription retrieved successfully",
      "content": {
        "application/json": {
          "examples": {
            "user_graph": {
              "summary": "User Graph Subscription",
              "value": {
                "id": "bsub_abc123",
                "resource_type": "graph",
                "resource_id": "kg1a2b3c",
                "plan_name": "kuzu-standard",
                "billing_interval": "monthly",
                "status": "active",
                "base_price_cents": 4999,
                "started_at": "2024-01-15T10:30:00Z",
              },
            },
            "repository": {
              "summary": "Repository Subscription",
              "value": {
                "id": "bsub_xyz789",
                "resource_type": "repository",
                "resource_id": "sec",
                "plan_name": "sec-professional",
                "billing_interval": "monthly",
                "status": "active",
                "base_price_cents": 9999,
                "started_at": "2024-01-15T10:30:00Z",
              },
            },
          }
        }
      },
    },
    404: {"description": "No subscription found"},
  },
)
async def get_subscription(
  graph_id: str = Path(..., description="Graph ID or repository name"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphSubscriptionResponse:
  """Get subscription for a graph or repository."""
  try:
    if is_shared_repository(graph_id):
      subscription = BillingSubscription.get_by_resource_and_user(
        resource_type="repository",
        resource_id=graph_id,
        user_id=current_user.id,
        session=db,
      )
    else:
      subscription = BillingSubscription.get_by_resource(
        resource_type="graph", resource_id=graph_id, session=db
      )

      if subscription and subscription.billing_customer_user_id != current_user.id:
        raise HTTPException(
          status_code=403,
          detail="You do not have access to this graph subscription",
        )

    if not subscription:
      raise HTTPException(
        status_code=404,
        detail=f"No subscription found for {graph_id}",
      )

    return subscription_to_response(subscription)

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get subscription for {graph_id}: {e}")
    raise HTTPException(status_code=500, detail="Failed to retrieve subscription")


@router.post(
  "",
  response_model=GraphSubscriptionResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Create Repository Subscription",
  description="""Create a new subscription to a shared repository.

This endpoint is ONLY for shared repositories (sec, industry, economic).
User graph subscriptions are created automatically when the graph is provisioned.

The subscription will be created in ACTIVE status immediately and credits will be allocated.""",
  operation_id="createRepositorySubscription",
  responses={
    201: {"description": "Repository subscription created successfully"},
    400: {
      "description": "Invalid request - cannot create subscription for user graphs"
    },
    409: {"description": "User already has a subscription to this repository"},
  },
)
async def create_repository_subscription(
  graph_id: str = Path(..., description="Repository name (e.g., 'sec', 'industry')"),
  request: CreateRepositorySubscriptionRequest = ...,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphSubscriptionResponse:
  """Create a new subscription to a shared repository."""
  try:
    if not is_shared_repository(graph_id):
      raise HTTPException(
        status_code=400,
        detail=(
          "Cannot manually create subscription for user graphs. "
          "Graph subscriptions are created automatically when the graph is provisioned."
        ),
      )

    existing = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id=graph_id,
      user_id=current_user.id,
      session=db,
    )

    if existing:
      raise HTTPException(
        status_code=409,
        detail=f"You already have an active subscription to the {graph_id} repository",
      )

    plan_config = BillingConfig.get_repository_plan(graph_id, request.plan_name)
    if not plan_config:
      raise HTTPException(
        status_code=400,
        detail=f"Invalid plan '{request.plan_name}' for repository '{graph_id}'",
      )

    BillingCustomer.get_or_create(current_user.id, db)

    subscription = BillingSubscription.create_subscription(
      user_id=current_user.id,
      resource_type="repository",
      resource_id=graph_id,
      plan_name=request.plan_name,
      base_price_cents=plan_config["price_cents"],
      session=db,
      billing_interval="monthly",
    )

    subscription.activate(db)

    logger.info(
      f"Created repository subscription for user {current_user.id} to {graph_id}",
      extra={
        "user_id": current_user.id,
        "repository": graph_id,
        "plan_name": request.plan_name,
        "subscription_id": subscription.id,
      },
    )

    return subscription_to_response(subscription)

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to create repository subscription: {e}")
    raise HTTPException(
      status_code=500, detail="Failed to create repository subscription"
    )


@router.put(
  "/upgrade",
  response_model=GraphSubscriptionResponse,
  summary="Upgrade Subscription",
  description="""Upgrade a subscription to a different plan.

Works for both user graphs and shared repositories.
The subscription will be immediately updated to the new plan and pricing.""",
  operation_id="upgradeSubscription",
  responses={
    200: {"description": "Subscription upgraded successfully"},
    404: {"description": "No subscription found"},
  },
)
async def upgrade_subscription(
  graph_id: str = Path(..., description="Graph ID or repository name"),
  request: UpgradeSubscriptionRequest = ...,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphSubscriptionResponse:
  """Upgrade subscription to a different plan."""
  try:
    if is_shared_repository(graph_id):
      subscription = BillingSubscription.get_by_resource_and_user(
        resource_type="repository",
        resource_id=graph_id,
        user_id=current_user.id,
        session=db,
      )
      plan_config = BillingConfig.get_repository_plan(graph_id, request.new_plan_name)
    else:
      subscription = BillingSubscription.get_by_resource(
        resource_type="graph", resource_id=graph_id, session=db
      )

      if subscription and subscription.billing_customer_user_id != current_user.id:
        raise HTTPException(
          status_code=403,
          detail="You do not have access to this graph subscription",
        )

      plan_config = BillingConfig.get_subscription_plan(request.new_plan_name)

    if not subscription:
      raise HTTPException(
        status_code=404,
        detail=f"No subscription found for {graph_id}",
      )

    if not plan_config:
      raise HTTPException(
        status_code=400,
        detail=f"Invalid plan '{request.new_plan_name}'",
      )

    subscription.update_plan(
      new_plan_name=request.new_plan_name,
      new_price_cents=plan_config.get(
        "price_cents", plan_config.get("base_price_cents", 0)
      ),
      session=db,
    )

    logger.info(
      f"Upgraded subscription {subscription.id} to {request.new_plan_name}",
      extra={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "new_plan": request.new_plan_name,
      },
    )

    return subscription_to_response(subscription)

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to upgrade subscription: {e}")
    raise HTTPException(status_code=500, detail="Failed to upgrade subscription")


@router.delete(
  "",
  response_model=CancellationResponse,
  summary="Cancel Subscription",
  description="""Cancel a subscription.

For shared repositories: Cancels the user's personal subscription
For user graphs: Not allowed - delete the graph instead

The subscription will be marked as canceled and will end at the current period end date.""",
  operation_id="cancelSubscription",
  responses={
    200: {"description": "Subscription canceled successfully"},
    400: {"description": "Cannot cancel graph subscriptions directly"},
    404: {"description": "No subscription found"},
  },
)
async def cancel_subscription(
  graph_id: str = Path(..., description="Graph ID or repository name"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> CancellationResponse:
  """Cancel a subscription."""
  try:
    if not is_shared_repository(graph_id):
      raise HTTPException(
        status_code=400,
        detail=(
          "Cannot cancel graph subscriptions directly. "
          "To cancel a graph subscription, delete the graph instead."
        ),
      )

    subscription = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id=graph_id,
      user_id=current_user.id,
      session=db,
    )

    if not subscription:
      raise HTTPException(
        status_code=404,
        detail=f"No subscription found for {graph_id}",
      )

    subscription.cancel(db, immediate=False)

    logger.info(
      f"Canceled subscription {subscription.id} for user {current_user.id}",
      extra={
        "user_id": current_user.id,
        "repository": graph_id,
        "subscription_id": subscription.id,
      },
    )

    return CancellationResponse(
      message=f"Successfully canceled subscription to {graph_id}",
      subscription_id=subscription.id,
      cancelled_at=datetime.now(timezone.utc).isoformat(),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to cancel subscription: {e}")
    raise HTTPException(status_code=500, detail="Failed to cancel subscription")
