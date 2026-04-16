"""Graph tier change command.

Extracted from ``routers/graphs/subscriptions.py`` so both the subscriptions
router and the graph operations router can call the same business logic
without either depending on the other.
"""

from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session

from robosystems.config import BillingConfig, env
from robosystems.models.core import User
from robosystems.models.core.billing import BillingAuditLog, BillingSubscription
from robosystems.models.core.billing.audit_log import BillingEventType


async def change_graph_tier_cmd(
  graph_id: str,
  new_tier: str,
  current_user: User,
  db: Session,
) -> str:
  """Change infrastructure tier on a graph subscription (async).

  Validates authorization, updates billing in PostgreSQL, syncs Stripe,
  and enqueues an EBS volume migration worker task.

  Returns the ``operation_id`` for SSE tracking.
  Raises ``HTTPException`` on validation or payment failures.
  """
  from datetime import UTC, datetime

  from robosystems.config.billing import get_tier_credit_allocation
  from robosystems.logger import logger
  from robosystems.middleware.billing.enforcement import invalidate_subscription_cache
  from robosystems.models.core import OrgRole, OrgUser
  from robosystems.models.core.graph import Graph
  from robosystems.models.core.graph.graph_credits import GraphCredits
  from robosystems.operations.graph.tier_validation import (
    validate_storage_capacity,
    validate_subgraph_count,
  )
  from robosystems.operations.providers.payment_provider import get_payment_provider
  from robosystems.worker.client import enqueue_task

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

  # Look up graph and subscription
  graph = Graph.get_by_id(graph_id, db)
  if not graph:
    raise HTTPException(status_code=404, detail=f"Graph {graph_id} not found")

  subscription = BillingSubscription.get_by_resource(
    resource_type="graph", resource_id=graph_id, session=db
  )
  if not subscription:
    raise HTTPException(status_code=404, detail=f"No subscription found for {graph_id}")

  # Verify subscription belongs to user's org
  org_id = user_orgs[0].org_id
  if subscription.org_id != org_id:
    raise HTTPException(
      status_code=403,
      detail="You do not have access to this graph subscription",
    )

  if subscription.status != "active":
    raise HTTPException(
      status_code=400,
      detail=f"Cannot change tier on a {subscription.status} subscription",
    )

  # Validate new tier exists in billing plans
  plan_config = BillingConfig.get_subscription_plan(new_tier)
  if not plan_config:
    raise HTTPException(
      status_code=400,
      detail=f"Invalid tier '{new_tier}'. Valid tiers: ladybug-standard, ladybug-large, ladybug-xlarge",
    )

  old_tier = graph.graph_tier
  if new_tier == old_tier:
    raise HTTPException(status_code=400, detail="Already on this tier")

  new_price_cents = plan_config["base_price_cents"]
  old_price_cents = subscription.base_price_cents
  is_upgrade = new_price_cents > old_price_cents

  # Downgrade validation
  if not is_upgrade:
    validate_subgraph_count(graph_id, new_tier, db)
    await validate_storage_capacity(graph_id, old_tier, new_tier, db)

  # Update PG atomically — billing committed before Stripe
  now = datetime.now(UTC)
  old_plan_name = subscription.plan_name
  subscription.plan_name = new_tier
  subscription.base_price_cents = new_price_cents
  subscription.status = "upgrading"
  subscription.updated_at = now
  graph.graph_tier = new_tier
  graph.updated_at = now

  new_credits = get_tier_credit_allocation(new_tier)
  graph_credits = GraphCredits.get_by_graph_id(graph_id, db)
  if graph_credits:
    graph_credits.monthly_allocation = new_credits

  db.commit()

  # Invalidate the subscription cache so require_graph_access() sees "upgrading"
  invalidate_subscription_cache(graph_id)

  # Update Stripe — if this fails, roll back PG
  stripe_sub_id = subscription.stripe_subscription_id
  if stripe_sub_id and env.BILLING_ENABLED:
    try:
      provider = get_payment_provider("stripe")
      new_stripe_price_id = provider.get_or_create_price(
        plan_name=new_tier,
        resource_type="graph",
      )
      provider.change_subscription_price(
        subscription_id=stripe_sub_id,
        new_price_id=new_stripe_price_id,
      )
    except Exception as stripe_error:
      logger.error(
        f"Stripe price change failed for {graph_id}, rolling back: {stripe_error}",
        exc_info=True,
      )
      subscription.plan_name = old_plan_name
      subscription.base_price_cents = old_price_cents
      subscription.status = "active"
      subscription.updated_at = datetime.now(UTC)
      graph.graph_tier = old_tier
      graph.updated_at = datetime.now(UTC)
      if graph_credits:
        graph_credits.monthly_allocation = get_tier_credit_allocation(old_tier)
      db.commit()
      raise HTTPException(
        status_code=500,
        detail="Failed to update payment. Tier change has been rolled back.",
      )

  # Enqueue async worker task for infrastructure migration
  try:
    result = await enqueue_task(
      task_type="graph_tier_upgrade",
      graph_id=graph_id,
      user_id=str(current_user.id),
      params={
        "old_tier": old_tier,
        "new_tier": new_tier,
        "subscription_id": subscription.id,
      },
    )
  except Exception as enqueue_error:
    logger.error(
      f"Failed to enqueue tier upgrade for {graph_id}, rolling back: {enqueue_error}",
      exc_info=True,
    )
    subscription.plan_name = old_plan_name
    subscription.base_price_cents = old_price_cents
    subscription.status = "active"
    subscription.updated_at = datetime.now(UTC)
    graph.graph_tier = old_tier
    graph.updated_at = datetime.now(UTC)
    if graph_credits:
      graph_credits.monthly_allocation = get_tier_credit_allocation(old_tier)
    db.commit()
    invalidate_subscription_cache(graph_id)
    # Revert Stripe pricing
    if stripe_sub_id and env.BILLING_ENABLED:
      try:
        provider = get_payment_provider("stripe")
        old_stripe_price_id = provider.get_or_create_price(
          plan_name=old_tier, resource_type="graph"
        )
        provider.change_subscription_price(
          subscription_id=stripe_sub_id, new_price_id=old_stripe_price_id
        )
      except Exception as stripe_revert_error:
        logger.error(
          f"Failed to revert Stripe pricing for {graph_id}: {stripe_revert_error}. "
          f"Manual Stripe intervention required.",
        )
    raise HTTPException(
      status_code=500,
      detail="Failed to start tier migration. Changes have been rolled back.",
    )

  operation_id: str = result["operation_id"]

  # Audit log
  BillingAuditLog.log_event(
    session=db,
    event_type=(
      BillingEventType.PLAN_UPGRADED if is_upgrade else BillingEventType.PLAN_DOWNGRADED
    ),
    org_id=org_id,
    subscription_id=subscription.id,
    description=(
      f"Tier change: {old_tier} -> {new_tier} for {graph_id} "
      f"(${new_price_cents / 100:.0f}/mo)"
    ),
    actor_type="user",
    actor_user_id=current_user.id,
    event_data={
      "resource_type": "graph",
      "resource_id": graph_id,
      "old_tier": old_tier,
      "new_tier": new_tier,
      "old_price_cents": old_price_cents,
      "new_price_cents": new_price_cents,
      "operation_id": operation_id,
    },
  )

  logger.info(
    f"Graph {graph_id} tier change initiated: {old_tier} -> {new_tier}",
    extra={
      "user_id": current_user.id,
      "graph_id": graph_id,
      "old_tier": old_tier,
      "new_tier": new_tier,
      "operation_id": operation_id,
    },
  )

  return operation_id
