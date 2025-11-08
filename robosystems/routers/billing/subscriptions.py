"""Billing subscriptions endpoints for managing user subscriptions."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.iam import User
from ...models.billing import BillingSubscription
from ...models.api.billing.subscription import GraphSubscriptionResponse
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/billing/subscriptions", tags=["Billing"])


@router.get(
  "",
  response_model=list[GraphSubscriptionResponse],
  summary="List All Subscriptions",
  description="""List all active and past subscriptions for the user.

Includes both graph and repository subscriptions with their status, pricing, and billing information.""",
  operation_id="listSubscriptions",
)
async def list_subscriptions(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """List all subscriptions for the current user."""
  try:
    subscriptions = (
      db.query(BillingSubscription)
      .join(BillingSubscription.billing_customer)
      .filter(BillingSubscription.billing_customer.has(user_id=current_user.id))
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
          created_at=sub.created_at.isoformat(),
        )
      )

    logger.info(
      f"Listed {len(result)} subscriptions for user {current_user.id}",
      extra={"user_id": current_user.id, "count": len(result)},
    )

    return result

  except Exception as e:
    logger.error(f"Failed to list subscriptions: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve subscriptions")


@router.get(
  "/{subscription_id}",
  response_model=GraphSubscriptionResponse,
  summary="Get Subscription Details",
  description="""Get detailed information about a specific subscription.""",
  operation_id="getSubscription",
)
async def get_subscription(
  subscription_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Get subscription details."""
  try:
    subscription = (
      db.query(BillingSubscription)
      .join(BillingSubscription.billing_customer)
      .filter(
        BillingSubscription.id == subscription_id,
        BillingSubscription.billing_customer.has(user_id=current_user.id),
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
      created_at=subscription.created_at.isoformat(),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get subscription: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve subscription")


@router.post(
  "/{subscription_id}/cancel",
  response_model=GraphSubscriptionResponse,
  summary="Cancel Subscription",
  description="""Cancel a subscription.

The subscription will remain active until the end of the current billing period.""",
  operation_id="cancelSubscription",
)
async def cancel_subscription(
  subscription_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Cancel a subscription."""
  try:
    subscription = (
      db.query(BillingSubscription)
      .join(BillingSubscription.billing_customer)
      .filter(
        BillingSubscription.id == subscription_id,
        BillingSubscription.billing_customer.has(user_id=current_user.id),
      )
      .first()
    )

    if not subscription:
      raise HTTPException(status_code=404, detail="Subscription not found")

    if subscription.status in ["canceled", "canceling"]:
      raise HTTPException(status_code=400, detail="Subscription is already canceled")

    subscription.cancel(db, immediate=False)

    logger.info(
      f"Canceled subscription {subscription_id} for user {current_user.id}",
      extra={"user_id": current_user.id, "subscription_id": subscription_id},
    )

    return GraphSubscriptionResponse(
      id=str(subscription.id),
      resource_type=subscription.resource_type,
      resource_id=subscription.resource_id or "",
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
      started_at=(
        subscription.started_at.isoformat() if subscription.started_at else None
      ),
      canceled_at=(
        subscription.canceled_at.isoformat() if subscription.canceled_at else None
      ),
      created_at=subscription.created_at.isoformat(),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to cancel subscription: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to cancel subscription")
