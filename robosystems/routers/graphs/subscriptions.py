"""
Unified subscription management endpoints for graphs and repositories.

This endpoint handles subscriptions for both:
- User Graphs: Per-graph billing (resource_type="graph", resource_id=graph_id)
- Shared Repositories: Per-user billing (resource_type="repository", resource_id=repo_name)

The same endpoint structure works for both, with automatic detection of the resource type.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from ...config import BillingConfig, env
from ...config.shared_repositories import is_shared_repository as _is_shared_repo
from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from ...middleware.rate_limits import subscription_aware_rate_limit_dependency
from ...models.api.billing.subscription import (
  CreateRepositorySubscriptionRequest,
  GraphSubscriptionResponse,
  UpgradeSubscriptionRequest,
)
from ...models.billing import BillingAuditLog, BillingCustomer, BillingSubscription
from ...models.billing.audit_log import BillingEventType
from ...models.iam import User

logger = logging.getLogger(__name__)

router = APIRouter(
  tags=["Subscriptions"],
  dependencies=[Depends(get_current_user)],
)


def is_shared_repository(graph_id: str) -> bool:
  """Check if a graph_id refers to a shared repository."""
  return _is_shared_repo(graph_id)


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
    ends_at=subscription.ends_at.isoformat() if subscription.ends_at else None,
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
                "plan_name": "ladybug-standard",
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
  graph_id: str = Path(
    ..., description="Graph ID or repository name", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphSubscriptionResponse:
  """Get subscription for a graph or repository."""
  try:
    from ...models.iam import OrgUser

    customer = BillingCustomer.get_by_user_id(current_user.id, db)
    if not customer:
      raise HTTPException(
        status_code=404,
        detail="No billing customer found for user",
      )

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

      if subscription:
        membership = OrgUser.get_by_org_and_user(
          org_id=subscription.org_id,
          user_id=current_user.id,
          session=db,
        )
        if not membership:
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
  graph_id: str = Path(
    ...,
    description="Repository name (e.g., 'sec', 'industry')",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
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

    from ...models.iam import OrgUser

    user_orgs = OrgUser.get_user_orgs(current_user.id, db)
    if not user_orgs:
      raise HTTPException(
        status_code=500,
        detail="User organization not found. Please contact support.",
      )

    org_id = user_orgs[0].org_id

    customer = BillingCustomer.get_or_create(org_id, db)

    can_provision, error_message = customer.can_provision_resources(
      environment=env.ENVIRONMENT, billing_enabled=env.BILLING_ENABLED
    )

    if not can_provision:
      raise HTTPException(
        status_code=402,
        detail=error_message
        or "Valid payment method required to subscribe to repositories.",
      )

    # Create subscription in "provisioning" state
    subscription = BillingSubscription.create_subscription(
      org_id=org_id,
      resource_type="repository",
      resource_id=graph_id,
      plan_name=request.plan_name,
      base_price_cents=plan_config["price_cents"],
      session=db,
      billing_interval="monthly",
    )

    BillingAuditLog.log_event(
      session=db,
      event_type=BillingEventType.SUBSCRIPTION_CREATED,
      org_id=org_id,
      subscription_id=subscription.id,
      description=f"Created {request.plan_name} subscription for {graph_id} repository",
      actor_type="user",
      actor_user_id=current_user.id,
      event_data={
        "resource_type": "repository",
        "resource_id": graph_id,
        "plan_name": request.plan_name,
        "base_price_cents": plan_config["price_cents"],
      },
    )

    # Create Stripe subscription if billing is enabled and customer has payment method
    if (
      env.BILLING_ENABLED
      and customer.has_payment_method
      and customer.stripe_customer_id
    ):
      try:
        from ...operations.providers.payment_provider import get_payment_provider

        provider = get_payment_provider("stripe")
        stripe_price_id = provider.get_or_create_price(
          plan_name=request.plan_name,
          resource_type="repository",
          repository_id=graph_id,
        )

        stripe_subscription_id = provider.create_subscription(
          customer_id=customer.stripe_customer_id,
          price_id=stripe_price_id,
          metadata={
            "subscription_id": str(subscription.id),
            "user_id": current_user.id,
            "resource_type": "repository",
            "resource_id": graph_id,
          },
        )

        subscription.stripe_subscription_id = stripe_subscription_id
        subscription.provider_subscription_id = stripe_subscription_id
        subscription.provider_customer_id = customer.stripe_customer_id
        subscription.payment_provider = "stripe"

        logger.info(
          f"Created Stripe subscription for repository {graph_id}",
          extra={
            "user_id": current_user.id,
            "subscription_id": subscription.id,
            "stripe_subscription_id": stripe_subscription_id,
          },
        )

      except Exception as e:
        logger.error(
          f"Failed to create Stripe subscription for repository {graph_id}: {e}",
          extra={
            "user_id": current_user.id,
            "subscription_id": subscription.id,
            "plan_name": request.plan_name,
          },
          exc_info=True,
        )
        subscription.status = "failed"
        db.commit()
        raise HTTPException(
          status_code=402,
          detail="Failed to create payment subscription. Please verify your payment method.",
        )

    # Store IDs before commit detaches the objects
    subscription_id = subscription.id
    user_id = current_user.id

    # Commit so run_user_repository_provisioning can see the subscription
    db.commit()

    # Delegate to run_user_repository_provisioning for:
    # - Grant repository access
    # - Allocate credits
    # - Activate subscription
    # - Generate invoice
    # - Report to Dagster
    from ...middleware.sse.direct_monitor import run_user_repository_provisioning

    try:
      result = await run_user_repository_provisioning(
        operation_id=None,  # No SSE tracking for sync API calls
        subscription_id=subscription_id,
        user_id=user_id,
        repository_name=graph_id,
      )
      logger.info(
        f"Repository provisioning completed for user {user_id} to {graph_id}",
        extra={
          "user_id": user_id,
          "repository": graph_id,
          "plan_name": request.plan_name,
          "subscription_id": subscription_id,
          "credits_allocated": result.get("credits_allocated", 0),
        },
      )
    except Exception as e:
      logger.error(f"Repository provisioning failed: {e}")
      # Subscription was created but provisioning failed
      # The subscription will be in a bad state - mark it as failed
      failed_sub = db.query(BillingSubscription).filter_by(id=subscription_id).first()
      if failed_sub:
        failed_sub.status = "failed"
        db.commit()
      raise HTTPException(
        status_code=500,
        detail=f"Repository provisioning failed: {e}",
      )

    # Re-fetch subscription to get updated state from provisioning
    updated_subscription = (
      db.query(BillingSubscription).filter_by(id=subscription_id).first()
    )
    if not updated_subscription:
      raise HTTPException(
        status_code=500,
        detail="Subscription not found after provisioning",
      )
    return subscription_to_response(updated_subscription)

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
  graph_id: str = Path(
    ..., description="Graph ID or repository name", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  request: UpgradeSubscriptionRequest = ...,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphSubscriptionResponse:
  """Upgrade subscription to a different plan."""
  try:
    from ...models.iam import OrgUser

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

      if subscription:
        membership = OrgUser.get_by_org_and_user(
          org_id=subscription.org_id,
          user_id=current_user.id,
          session=db,
        )
        if not membership:
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
