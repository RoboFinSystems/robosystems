"""Billing subscriptions endpoints for managing user subscriptions."""

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import billing_rate_limit_dependency
from ...models.api.billing.subscription import (
  CancelSubscriptionRequest,
  GraphSubscriptionResponse,
)
from ...models.api.common import RESOURCE_ERROR_RESPONSES
from ...models.core import User
from ...models.core.billing import (
  BillingAuditLog,
  BillingEventType,
  BillingSubscription,
)
from ...operations.providers.payment_provider import get_payment_provider

logger = get_logger(__name__)

router = APIRouter(prefix="/billing/subscriptions", tags=["Billing"])


def _get_plan_display_name(plan_name: str, resource_type: str, resource_id: str) -> str:
  """Backwards-compat thin wrapper — prefer BillingConfig.get_plan_display_name."""
  from ...config.billing import BillingConfig

  return BillingConfig.get_plan_display_name(plan_name, resource_type, resource_id)


@router.get(
  "/{org_id}",
  response_model=list[GraphSubscriptionResponse],
  summary="List Organization Subscriptions",
  operation_id="listOrgSubscriptions",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_subscriptions(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  try:
    from ...models.core import OrgUser

    # Verify user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    filters = [
      BillingSubscription.org_id == org_id,
      # Exclude orphaned subscriptions that never completed provisioning
      ~(
        BillingSubscription.status.in_(["canceled", "failed"])
        & BillingSubscription.resource_id.is_(None)
      ),
    ]

    # Billing is org-scoped but repository access is per-user, so an org-wide
    # list hands a plain member every other member's subscriptions. Owners and
    # admins manage billing and need the whole org; everyone else sees only the
    # rows attributed to them. `user_id` is set on repository subscriptions and
    # NULL on graph subscriptions, which are org-level resources rather than
    # per-person ones — so a plain member sees neither other people's seats nor
    # the org's graph inventory.
    if not membership.can_manage_billing():
      filters.append(BillingSubscription.user_id == current_user.id)

    subscriptions = (
      db.query(BillingSubscription)
      .filter(*filters)
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
          user_id=sub.user_id,
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
  operation_id="getOrgSubscription",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_subscription(
  org_id: str,
  subscription_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
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
      user_id=subscription.user_id,
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
  description=(
    "Cancels a subscription. Default behavior cancels at period end "
    "(subscription stays active through the current cycle). Pass "
    "`immediate=true` with `confirm=<resource_id>` to terminate now and "
    "trigger fast-path deprovisioning of the underlying resource. Requires "
    "org owner role."
  ),
  operation_id="cancelOrgSubscription",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def cancel_subscription(
  org_id: str,
  subscription_id: str,
  body: CancelSubscriptionRequest = Body(default_factory=CancelSubscriptionRequest),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
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

    # Resource-scoped cancellation lives where the resource lives:
    #   - User graphs  → POST /v1/graphs/{g}/operations/delete-graph
    #   - Repositories → POST /v1/graphs/{repo_id}/subscription/cancel
    # That keeps "one canonical cancel path per resource type" and avoids
    # the two-paths drift we cleaned up in the graph case. The billing
    # cancel endpoint is reserved for future non-resource-scoped subs
    # (e.g. platform-level add-ons not tied to a graph_id).
    if subscription.resource_type == "graph":
      raise HTTPException(
        status_code=400,
        detail=(
          "Cancellation of a graph subscription must go through "
          f"POST /v1/graphs/{subscription.resource_id}/operations/delete-graph "
          "(supports both immediate and `at_period_end=true` modes)."
        ),
      )

    if subscription.resource_type == "repository":
      raise HTTPException(
        status_code=400,
        detail=(
          "Cancellation of a repository subscription must go through "
          f"POST /v1/graphs/{subscription.resource_id}/subscriptions/cancel "
          "(supports both period-end and `immediate=true` modes)."
        ),
      )

    if body.immediate:
      if not body.confirm or body.confirm != (subscription.resource_id or ""):
        raise HTTPException(
          status_code=400,
          detail=(
            "Immediate cancellation requires `confirm` to match the "
            "subscription's resource_id."
          ),
        )

    # Cancel in Stripe if there's a linked Stripe subscription. For period-end
    # we mark the Stripe sub to cancel at period end; for immediate we cancel
    # outright. Stripe.Subscription.cancel() does NOT prorate or refund by
    # default — credit-on-account is handled separately.
    if subscription.stripe_subscription_id:
      try:
        provider = get_payment_provider("stripe")
        if body.immediate:
          provider.cancel_subscription(subscription.stripe_subscription_id)
          logger.info(
            f"Immediately canceled Stripe subscription "
            f"{subscription.stripe_subscription_id}",
            extra={"subscription_id": subscription_id},
          )
        else:
          provider.stripe.Subscription.modify(
            subscription.stripe_subscription_id,
            cancel_at_period_end=True,
          )
          logger.info(
            f"Canceled Stripe subscription "
            f"{subscription.stripe_subscription_id} at period end",
            extra={"subscription_id": subscription_id},
          )
      except Exception as e:
        logger.error(
          f"Failed to cancel Stripe subscription: {e}",
          extra={"subscription_id": subscription_id},
          exc_info=True,
        )

    subscription.cancel(db, immediate=body.immediate)

    BillingAuditLog.log_event(
      session=db,
      event_type=BillingEventType.SUBSCRIPTION_CANCELED,
      description=(
        f"Subscription {subscription_id} canceled "
        f"({'immediate' if body.immediate else 'period_end'})"
      ),
      actor_type="user",
      actor_user_id=current_user.id,
      org_id=org_id,
      subscription_id=subscription_id,
      event_data={
        "immediate": body.immediate,
        "resource_type": subscription.resource_type,
        "resource_id": subscription.resource_id,
      },
    )

    logger.info(
      f"Canceled subscription {subscription_id} for org {org_id} "
      f"(immediate={body.immediate})",
      extra={
        "org_id": org_id,
        "user_id": current_user.id,
        "subscription_id": subscription_id,
        "immediate": body.immediate,
      },
    )

    return GraphSubscriptionResponse(
      id=str(subscription.id),
      resource_type=subscription.resource_type,
      resource_id=subscription.resource_id or "",
      user_id=subscription.user_id,
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
