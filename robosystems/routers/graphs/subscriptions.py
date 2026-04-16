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
from ...config.shared_repositories import (
  is_shared_repository_or_subgraph as _is_shared_repo_or_sub,
)
from ...config.shared_repositories import (
  resolve_shared_repository_parent,
)
from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from ...middleware.rate_limits import subscription_aware_rate_limit_dependency
from ...models.api.billing.subscription import (
  CreateRepositorySubscriptionRequest,
  GraphSubscriptionResponse,
  UpgradeSubscriptionRequest,
)
from ...models.core import User
from ...models.core.billing import BillingAuditLog, BillingCustomer, BillingSubscription
from ...models.core.billing.audit_log import BillingEventType

logger = logging.getLogger(__name__)

router = APIRouter(
  tags=["Subscriptions"],
  dependencies=[Depends(get_current_user)],
)


def is_shared_repository(graph_id: str) -> bool:
  """Check if a graph_id refers to a shared repository or subgraph of one."""
  return _is_shared_repo_or_sub(graph_id)


def _get_plan_display_name(plan_name: str, resource_type: str, resource_id: str) -> str:
  """Resolve a plan's internal name to its human-readable display name."""
  if resource_type == "graph":
    plan = BillingConfig.get_subscription_plan(plan_name)
    if plan:
      return plan.get("display_name", plan_name)
  elif resource_type == "repository":
    plan = BillingConfig.get_repository_plan(resource_id, plan_name)
    if plan:
      return plan.get("display_name", plan_name)
  return plan_name


def subscription_to_response(
  subscription: BillingSubscription,
  operation_id: str | None = None,
) -> GraphSubscriptionResponse:
  """Convert subscription model to API response."""
  return GraphSubscriptionResponse(
    id=subscription.id,
    resource_type=subscription.resource_type,
    resource_id=subscription.resource_id,
    plan_name=subscription.plan_name,
    plan_display_name=_get_plan_display_name(
      subscription.plan_name, subscription.resource_type, subscription.resource_id
    ),
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
    operation_id=operation_id,
  )


@router.get(
  "",
  response_model=GraphSubscriptionResponse,
  summary="Get Graph and Shared Repository Subscription",
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
                "plan_name": "sec-advanced",
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
    from ...models.core import OrgUser

    customer = BillingCustomer.get_by_user_id(current_user.id, db)
    if not customer:
      raise HTTPException(
        status_code=404,
        detail="No billing customer found for user",
      )

    if is_shared_repository(graph_id):
      # Subscriptions are on the parent repo, not subgraphs
      parent_repo_id = resolve_shared_repository_parent(graph_id)
      subscription = BillingSubscription.get_by_resource_and_user(
        resource_type="repository",
        resource_id=parent_repo_id,
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

    parent_repo_id = resolve_shared_repository_parent(graph_id)
    existing = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id=parent_repo_id,
      user_id=current_user.id,
      session=db,
    )

    if existing:
      raise HTTPException(
        status_code=409,
        detail=f"You already have an active subscription to the {graph_id} repository",
      )

    plan_config = BillingConfig.get_repository_plan(parent_repo_id, request.plan_name)
    if not plan_config:
      raise HTTPException(
        status_code=400,
        detail=f"Invalid plan '{request.plan_name}' for repository '{graph_id}'",
      )

    from ...models.core import OrgUser

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
      resource_id=parent_repo_id,
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
    from robosystems.operations.graph.provisioning_service import (
      run_user_repository_provisioning,
    )

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
      logger.error(f"Repository provisioning failed: {e}", exc_info=True)
      # Subscription was created but provisioning failed
      # The subscription will be in a bad state - mark it as failed
      failed_sub = db.query(BillingSubscription).filter_by(id=subscription_id).first()
      if failed_sub:
        failed_sub.status = "failed"
        # Cancel Stripe subscription so the customer isn't charged
        if failed_sub.stripe_subscription_id:
          try:
            from ...operations.providers.payment_provider import get_payment_provider

            provider = get_payment_provider("stripe")
            provider.cancel_subscription(failed_sub.stripe_subscription_id)
          except Exception as cancel_error:
            logger.error(
              f"Failed to cancel Stripe subscription {failed_sub.stripe_subscription_id}: "
              f"{cancel_error}"
            )
        db.commit()
      raise HTTPException(
        status_code=500,
        detail="Repository provisioning failed.",
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


@router.patch(
  "",
  response_model=GraphSubscriptionResponse,
  summary="Change Subscription Plan",
  description="""Change the plan on an existing subscription.

**For shared repositories** (sec, industry, etc.): Changes access tier (e.g., starter -> advanced).
Synchronous — takes effect immediately.

**For user graphs** (kg*): Changes infrastructure tier (e.g., ladybug-standard -> ladybug-large).
Asynchronous — returns an `operation_id` for tracking the EBS volume migration via SSE.

Stripe handles proration automatically for both types.

**Requirements:**
- User must be an OWNER of their organization
- Subscription must be active
- New plan must be valid for the resource type

**Downgrade restrictions (graphs only):**
- Subgraph count must fit the target tier's limit
- Storage usage must fit the target tier's limit""",
  operation_id="changeSubscriptionPlan",
  responses={
    200: {"description": "Plan changed successfully"},
    400: {"description": "Invalid plan, validation failure, or status conflict"},
    404: {"description": "No subscription found"},
  },
)
async def change_plan(
  graph_id: str = Path(
    ...,
    description="Graph ID or repository name",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  request: UpgradeSubscriptionRequest = ...,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphSubscriptionResponse:
  """Change plan on a repository subscription.

  For graph tier changes, use ``POST /v1/graphs/{graph_id}/operations/upgrade-tier``.
  """
  try:
    if is_shared_repository(graph_id):
      return await _change_repository_plan(graph_id, request, current_user, db)
    raise HTTPException(
      status_code=400,
      detail="Graph tier changes use POST /v1/graphs/{graph_id}/operations/upgrade-tier. "
      "This endpoint is for shared repository plan changes only.",
    )
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to change subscription plan: {e}")
    raise HTTPException(status_code=500, detail="Failed to change subscription plan")


async def _change_repository_plan(
  graph_id: str,
  request: UpgradeSubscriptionRequest,
  current_user: User,
  db: Session,
) -> GraphSubscriptionResponse:
  """Change plan on a shared repository subscription (sync)."""
  from ...models.core import OrgRole, OrgUser
  from ...models.core.user.user_repository import UserRepository
  from ...operations.providers.payment_provider import get_payment_provider

  # Verify user is an org owner
  user_orgs = OrgUser.get_user_orgs(current_user.id, db)
  if not user_orgs:
    raise HTTPException(
      status_code=403, detail="You are not a member of any organization"
    )
  if user_orgs[0].role != OrgRole.OWNER:
    raise HTTPException(
      status_code=403,
      detail="Only organization owners can change subscription plans",
    )

  # Find existing subscription (subscriptions are on the parent repo)
  parent_repo_id = resolve_shared_repository_parent(graph_id)
  subscription = BillingSubscription.get_by_resource_and_user(
    resource_type="repository",
    resource_id=parent_repo_id,
    user_id=current_user.id,
    session=db,
  )
  if not subscription:
    raise HTTPException(
      status_code=404,
      detail=f"No subscription found for {graph_id}",
    )

  if subscription.status != "active":
    raise HTTPException(
      status_code=400,
      detail=f"Cannot change plan on a {subscription.status} subscription",
    )

  new_plan_name = request.new_plan_name

  # Validate the new plan exists
  plan_config = BillingConfig.get_repository_plan(parent_repo_id, new_plan_name)
  if not plan_config:
    raise HTTPException(
      status_code=400,
      detail=f"Invalid plan '{new_plan_name}' for repository '{graph_id}'",
    )

  new_plan_tier = plan_config["name"]
  if new_plan_tier == subscription.plan_name:
    raise HTTPException(status_code=400, detail="Already on this plan")

  new_price_cents = plan_config["price_cents"]
  new_credits = plan_config.get("monthly_credits", 0)
  old_plan = subscription.plan_name
  is_upgrade = new_price_cents > subscription.base_price_cents

  # Update DB first — if this fails, Stripe stays consistent
  subscription.update_plan(new_plan_tier, new_price_cents, db)

  # Update UserRepository access (credits, rate limits)
  user_repo = UserRepository.get_by_user_and_repository(current_user.id, graph_id, db)
  if not user_repo:
    logger.error(
      f"No UserRepository record for user {current_user.id} on {graph_id} "
      f"during plan change — credits and rate limits were NOT updated",
      extra={"user_id": current_user.id, "repository_id": graph_id},
    )
    raise HTTPException(
      status_code=500,
      detail="Repository access record not found. Please contact support.",
    )

  user_repo.upgrade_tier(
    new_plan=new_plan_tier,
    session=db,
    new_price_cents=new_price_cents,
    new_credits=new_credits,
  )

  # Update Stripe subscription if linked
  stripe_sub_id = subscription.stripe_subscription_id
  if stripe_sub_id:
    provider = get_payment_provider("stripe")
    new_stripe_price_id = provider.get_or_create_price(
      plan_name=new_plan_name,
      resource_type="repository",
      repository_id=graph_id,
    )
    provider.change_subscription_price(
      subscription_id=stripe_sub_id,
      new_price_id=new_stripe_price_id,
    )

  # Audit log
  org_id = user_orgs[0].org_id
  BillingAuditLog.log_event(
    session=db,
    event_type=(
      BillingEventType.PLAN_UPGRADED if is_upgrade else BillingEventType.PLAN_DOWNGRADED
    ),
    org_id=org_id,
    subscription_id=subscription.id,
    description=(
      f"Changed {graph_id} plan from {old_plan} to {new_plan_tier} "
      f"(${subscription.base_price_cents / 100:.0f}/mo)"
    ),
    actor_type="user",
    actor_user_id=current_user.id,
    event_data={
      "resource_type": "repository",
      "resource_id": graph_id,
      "old_plan": old_plan,
      "new_plan": new_plan_tier,
      "old_price_cents": subscription.base_price_cents,
      "new_price_cents": new_price_cents,
      "new_credits": new_credits,
    },
  )

  logger.info(
    f"Changed repository {graph_id} plan: {old_plan} -> {new_plan_tier}",
    extra={
      "user_id": current_user.id,
      "repository_id": graph_id,
      "old_plan": old_plan,
      "new_plan": new_plan_tier,
    },
  )

  return subscription_to_response(subscription)
