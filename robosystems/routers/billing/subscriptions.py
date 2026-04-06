"""Billing subscriptions endpoints for managing user subscriptions."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import billing_rate_limit_dependency
from ...models.api.billing.subscription import GraphSubscriptionResponse
from ...models.core import User
from ...models.core.billing import BillingSubscription
from ...operations.providers.payment_provider import get_payment_provider

logger = get_logger(__name__)

router = APIRouter(prefix="/billing/subscriptions", tags=["Billing"])


def _get_plan_display_name(plan_name: str, resource_type: str, resource_id: str) -> str:
  """Resolve a plan's internal name to its human-readable display name.

  For graphs: "ladybug-standard" -> "LadybugDB Standard"
  For repos:  "sec-advanced"     -> "SEC EDGAR Filings - Pro"
  """
  from ...config.billing import BillingConfig

  if resource_type == "graph":
    plan = BillingConfig.get_subscription_plan(plan_name)
    if plan:
      return plan.get("display_name", plan_name)
  elif resource_type == "repository":
    plan = BillingConfig.get_repository_plan(resource_id, plan_name)
    if plan:
      return plan.get("display_name", plan_name)

  return plan_name


@router.get(
  "/{org_id}",
  response_model=list[GraphSubscriptionResponse],
  summary="List Organization Subscriptions",
  description="""List all active and past subscriptions for an organization.

Includes both graph and repository subscriptions with their status, pricing, and billing information.

**Requirements:**
- User must be a member of the organization""",
  operation_id="listOrgSubscriptions",
)
async def list_subscriptions(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  """List all subscriptions for the organization."""
  try:
    from ...models.core import OrgUser

    # Verify user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    subscriptions = (
      db.query(BillingSubscription)
      .filter(
        BillingSubscription.org_id == org_id,
        # Exclude orphaned subscriptions that never completed provisioning
        ~(
          BillingSubscription.status.in_(["canceled", "failed"])
          & BillingSubscription.resource_id.is_(None)
        ),
      )
      .order_by(BillingSubscription.created_at.desc())
      .all()
    )

    result = []
    for sub in subscriptions:
      result.append(
        GraphSubscriptionResponse(
          id=str(sub.id),
          resource_type=sub.resource_type,
          resource_id=sub.resource_id or "",
          plan_name=sub.plan_name,
          plan_display_name=_get_plan_display_name(
            sub.plan_name, sub.resource_type, sub.resource_id or ""
          ),
          billing_interval=sub.billing_interval,
          status=sub.status,
          base_price_cents=sub.base_price_cents,
          current_period_start=(
            sub.current_period_start.isoformat() if sub.current_period_start else None
          ),
          current_period_end=(
            sub.current_period_end.isoformat() if sub.current_period_end else None
          ),
          started_at=sub.started_at.isoformat() if sub.started_at else None,
          canceled_at=(sub.canceled_at.isoformat() if sub.canceled_at else None),
          ends_at=sub.ends_at.isoformat() if sub.ends_at else None,
          created_at=sub.created_at.isoformat(),
        )
      )

    logger.info(
      f"Listed {len(result)} subscriptions for user {current_user.id}",
      extra={"user_id": current_user.id, "count": len(result)},
    )

    return result

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to list subscriptions: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve subscriptions")


@router.get(
  "/{org_id}/subscription/{subscription_id}",
  response_model=GraphSubscriptionResponse,
  summary="Get Organization Subscription Details",
  description="""Get detailed information about a specific subscription.

**Requirements:**
- User must be a member of the organization""",
  operation_id="getOrgSubscription",
)
async def get_subscription(
  org_id: str,
  subscription_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  """Get subscription details for organization."""
  try:
    from ...models.core import OrgUser

    # Verify user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    subscription = (
      db.query(BillingSubscription)
      .filter(
        BillingSubscription.id == subscription_id,
        BillingSubscription.org_id == org_id,
      )
      .first()
    )

    if not subscription:
      raise HTTPException(status_code=404, detail="Subscription not found")

    return GraphSubscriptionResponse(
      id=str(subscription.id),
      resource_type=subscription.resource_type,
      resource_id=subscription.resource_id or "",
      plan_name=subscription.plan_name,
      plan_display_name=_get_plan_display_name(
        subscription.plan_name,
        subscription.resource_type,
        subscription.resource_id or "",
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
      started_at=(
        subscription.started_at.isoformat() if subscription.started_at else None
      ),
      canceled_at=(
        subscription.canceled_at.isoformat() if subscription.canceled_at else None
      ),
      ends_at=subscription.ends_at.isoformat() if subscription.ends_at else None,
      created_at=subscription.created_at.isoformat(),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get subscription: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve subscription")


@router.post(
  "/{org_id}/subscription/{subscription_id}/cancel",
  response_model=GraphSubscriptionResponse,
  summary="Cancel Organization Subscription",
  description="""Cancel an organization subscription.

The subscription will remain active until the end of the current billing period.

**Requirements:**
- User must be an OWNER of the organization""",
  operation_id="cancelOrgSubscription",
)
async def cancel_subscription(
  org_id: str,
  subscription_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  """Cancel an organization subscription."""
  try:
    from ...models.core import OrgRole, OrgUser

    # Verify user is an owner of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    if membership.role != OrgRole.OWNER:
      raise HTTPException(
        status_code=403,
        detail="Only organization owners can cancel subscriptions",
      )

    subscription = (
      db.query(BillingSubscription)
      .filter(
        BillingSubscription.id == subscription_id,
        BillingSubscription.org_id == org_id,
      )
      .first()
    )

    if not subscription:
      raise HTTPException(status_code=404, detail="Subscription not found")

    if subscription.status in ["canceled", "canceling"]:
      raise HTTPException(status_code=400, detail="Subscription is already canceled")

    if subscription.status == "upgrading":
      raise HTTPException(
        status_code=400,
        detail="Cannot cancel during tier upgrade. Please wait for upgrade to complete.",
      )

    # Cancel in Stripe if there's a linked Stripe subscription
    if subscription.stripe_subscription_id:
      try:
        provider = get_payment_provider("stripe")
        provider.stripe.Subscription.modify(
          subscription.stripe_subscription_id,
          cancel_at_period_end=True,
        )
        logger.info(
          f"Canceled Stripe subscription {subscription.stripe_subscription_id}",
          extra={"subscription_id": subscription_id},
        )
      except Exception as e:
        logger.error(
          f"Failed to cancel Stripe subscription: {e}",
          extra={"subscription_id": subscription_id},
          exc_info=True,
        )

    subscription.cancel(db, immediate=False)

    logger.info(
      f"Canceled subscription {subscription_id} for org {org_id}",
      extra={
        "org_id": org_id,
        "user_id": current_user.id,
        "subscription_id": subscription_id,
      },
    )

    return GraphSubscriptionResponse(
      id=str(subscription.id),
      resource_type=subscription.resource_type,
      resource_id=subscription.resource_id or "",
      plan_name=subscription.plan_name,
      plan_display_name=_get_plan_display_name(
        subscription.plan_name,
        subscription.resource_type,
        subscription.resource_id or "",
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
      started_at=(
        subscription.started_at.isoformat() if subscription.started_at else None
      ),
      canceled_at=(
        subscription.canceled_at.isoformat() if subscription.canceled_at else None
      ),
      ends_at=subscription.ends_at.isoformat() if subscription.ends_at else None,
      created_at=subscription.created_at.isoformat(),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to cancel subscription: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to cancel subscription")
