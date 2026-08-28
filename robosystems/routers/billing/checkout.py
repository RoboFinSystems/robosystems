"""Billing checkout endpoints for payment collection."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...config import BillingConfig, env
from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import billing_rate_limit_dependency
from ...models.api.billing.checkout import (
  CheckoutResponse,
  CheckoutStatusResponse,
  CreateCheckoutRequest,
)
from ...models.api.common import AUTHENTICATED_ERROR_RESPONSES, RESOURCE_ERROR_RESPONSES
from ...models.core import User
from ...models.core.billing import BillingCustomer, BillingSubscription
from ...operations.graph.capacity import (
  tier_capacity_status as _tier_capacity_status,
)
from ...operations.providers.payment_provider import get_payment_provider

logger = get_logger(__name__)

router = APIRouter(prefix="/billing", tags=["Billing"])


@router.post(
  "/checkout",
  response_model=CheckoutResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Create Payment Checkout Session",
  description="Initiates a Stripe checkout session for payment collection. Creates a pending subscription; the webhook activates it and provisions the resource after payment completes. Returns billing_disabled=true with no URL when billing is off. Requires org owner role.",
  operation_id="createCheckoutSession",
  responses={
    **AUTHENTICATED_ERROR_RESPONSES,
    402: {"description": "Payment required"},
  },
)
async def create_checkout_session(
  request: CreateCheckoutRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  if not env.BILLING_ENABLED:
    return CheckoutResponse(
      checkout_url=None,
      session_id=None,
      subscription_id=None,
      requires_checkout=False,
      billing_disabled=True,
    )

  try:
    from ...models.core import OrgLimits, OrgRole, OrgUser

    # Get user's org - they must be an OWNER
    user_orgs = OrgUser.get_user_orgs(current_user.id, db)
    if not user_orgs:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of any organization",
      )

    membership = user_orgs[0]
    org_id = membership.org_id

    if membership.role != OrgRole.OWNER:
      raise HTTPException(
        status_code=403,
        detail="Only organization owners can manage billing",
      )

    # Gate a graph checkout on the org's graph limit before collecting payment.
    # The graph-creation route enforces the same limit up front, but in the
    # checkout lane provisioning happens post-payment via the webhook, so
    # without this an org at its limit could pay and then be refused the graph
    # it just bought. Refuse the sale here instead, matching the graph route.
    if request.resource_type == "graph":
      org_limits = OrgLimits.get_or_create_for_org(org_id, db)
      can_create, reason = org_limits.can_create_graph(db)
      if not can_create:
        raise HTTPException(status_code=403, detail=reason)

    customer = BillingCustomer.get_or_create(org_id, db)
    logger.info(f"Using billing customer for org {org_id}")

    # Enterprise customers don't need checkout
    if customer.invoice_billing_enabled:
      raise HTTPException(
        status_code=400,
        detail="Checkout not required for enterprise customers with invoice billing",
      )

    # If they already have a payment method, they shouldn't be here
    if customer.has_payment_method:
      raise HTTPException(
        status_code=400,
        detail="Payment method already on file. Create resource directly.",
      )

    # Validate plan exists
    plan_config = None
    repo_name = None
    if request.resource_type == "graph":
      plan_config = BillingConfig.get_subscription_plan(request.plan_name)
    elif request.resource_type == "repository":
      # NOTE: repository_name contains the graph_id (e.g., "sec"), not display name
      repo_name = request.resource_config.get("repository_name")
      if repo_name:
        plan_config = BillingConfig.get_repository_plan(repo_name, request.plan_name)

    if not plan_config:
      raise HTTPException(
        status_code=400,
        detail=f"Invalid plan '{request.plan_name}' for {request.resource_type}",
      )

    # Gate a graph checkout on writer capacity for the tier — the same
    # refuse-the-sale rule as the org graph limit above, one level down.
    # Provisioning runs post-payment; if no writer has a free slot the sale
    # must not complete. Fails closed: if capacity cannot be determined, the
    # sale is refused rather than collected.
    if request.resource_type == "graph":
      capacity_status = await _tier_capacity_status(request.plan_name)
      if capacity_status != "ready":
        raise HTTPException(
          status_code=409,
          detail=(
            f"No capacity is currently available for the '{request.plan_name}' "
            "tier. Please contact support or try again later."
          ),
        )

    base_price_cents = plan_config.get(
      "base_price_cents", plan_config.get("price_cents", 0)
    )

    provider = get_payment_provider("stripe")

    # Retire any earlier checkout for this same resource before opening a new
    # one. Cancelling the local row alone is not enough: the hosted session it
    # points at stays payable at the provider for up to 24 hours, and a payment
    # against it would bind a live provider subscription to a row already
    # marked canceled — money that never becomes a resource. Expire the session
    # first; if it turns out to have been paid, that checkout won and this one
    # must not open a second one.
    already_paid = HTTPException(
      status_code=409,
      detail=(
        "A previous checkout for this resource has already been paid and is "
        "being provisioned. Check your subscriptions before starting another."
      ),
    )
    stale_pending = (
      db.query(BillingSubscription)
      .filter(
        BillingSubscription.org_id == org_id,
        BillingSubscription.status == "pending_payment",
        BillingSubscription.resource_type == request.resource_type,
      )
      .all()
    )
    for stale_sub in stale_pending:
      # Same resource only. Repository checkouts are per repository: an
      # in-flight checkout for a different repository under the same org is
      # not superseded by this one, and must not be expired — or, if it was
      # already paid, mistaken for this resource's payment.
      if request.resource_type == "repository":
        stale_config = (stale_sub.subscription_metadata or {}).get(
          "resource_config"
        ) or {}
        stale_repo = stale_config.get("repository_name")
        if stale_repo and stale_repo != repo_name:
          continue

      stale_session_id = stale_sub.provider_subscription_id
      if stale_session_id and not stale_session_id.startswith("cs_"):
        # `checkout.session.completed` has already replaced the session id
        # with the provider's subscription id: the row is paid and waiting
        # for the provisioning claim. It is not a stale checkout.
        raise already_paid
      if stale_session_id:
        outcome = provider.expire_checkout_session(stale_session_id)
        if outcome == "complete":
          raise already_paid
      now = datetime.now(UTC)
      stale_sub.status = "canceled"
      stale_sub.canceled_at = now
      stale_sub.ends_at = now
      logger.info(
        f"Canceled superseded pending_payment subscription {stale_sub.id}",
        extra={
          "subscription_id": stale_sub.id,
          "org_id": org_id,
          "checkout_session_id": stale_session_id,
        },
      )
    if stale_pending:
      db.commit()

    # Create subscription in PENDING_PAYMENT status
    subscription = BillingSubscription.create_subscription(
      org_id=org_id,
      resource_type=request.resource_type,
      resource_id=None,  # Will be set after provisioning
      plan_name=request.plan_name,
      base_price_cents=base_price_cents,
      session=db,
      billing_interval="monthly",
      user_id=current_user.id,
    )

    # Resource configuration for the post-payment provisioning step. The
    # subscriber also stays in metadata for older webhook payloads; the
    # user_id column is now the authoritative copy.
    subscription.subscription_metadata = {
      "resource_config": request.resource_config,
      "user_id": current_user.id,
    }
    subscription.status = "pending_payment"
    subscription.payment_provider = "stripe"
    db.commit()
    db.refresh(subscription)

    # Get or create Stripe customer ID
    if not customer.stripe_customer_id:
      stripe_customer_id = provider.create_customer(current_user.id, current_user.email)
      customer.stripe_customer_id = stripe_customer_id
      db.commit()

    # Get/create the Stripe price from billing config
    try:
      stripe_price_id = provider.get_or_create_price(
        plan_name=request.plan_name,
        resource_type=request.resource_type,
        repository_id=repo_name if request.resource_type == "repository" else None,
      )
    except ValueError as e:
      logger.error(f"Failed to get Stripe price: {e}", exc_info=True)
      raise HTTPException(
        status_code=500,
        detail="Payment configuration error.",
      )

    # Create Stripe checkout session
    checkout = provider.create_checkout_session(
      customer_id=customer.stripe_customer_id,
      price_id=stripe_price_id,
      metadata={
        "subscription_id": str(subscription.id),
        "user_id": str(current_user.id),
        "resource_type": request.resource_type,
      },
    )

    # Link checkout session to subscription
    subscription.provider_subscription_id = checkout["session_id"]
    subscription.provider_customer_id = customer.stripe_customer_id
    db.commit()

    logger.info(
      f"Created checkout session for user {current_user.id}",
      extra={
        "user_id": current_user.id,
        "subscription_id": subscription.id,
        "session_id": checkout["session_id"],
        "plan_name": request.plan_name,
        "resource_type": request.resource_type,
      },
    )

    return CheckoutResponse(
      checkout_url=checkout["checkout_url"],
      session_id=checkout["session_id"],
      subscription_id=str(subscription.id),
      requires_checkout=True,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to create checkout session: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to create checkout session")


@router.get(
  "/checkout/{session_id}/status",
  response_model=CheckoutStatusResponse,
  summary="Get Checkout Session Status",
  description=(
    "Poll after returning from Stripe Checkout. Status progresses: "
    "pending_payment → provisioning → active. When active, resource_id is "
    "populated. `operation_id` is always null for webhook-driven provisioning "
    "and cannot be used to follow progress — poll this endpoint instead."
  ),
  operation_id="getCheckoutStatus",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_checkout_status(
  session_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  try:
    from ...models.core import OrgUser

    subscription = BillingSubscription.get_by_provider_subscription_id(session_id, db)

    # Fallback: after webhook processing, provider_subscription_id is updated
    # from the checkout session ID to the Stripe subscription ID, so look up
    # by the preserved checkout_session_id in metadata.
    if not subscription:
      subscription = (
        db.query(BillingSubscription)
        .filter(
          BillingSubscription.subscription_metadata["checkout_session_id"].astext
          == session_id
        )
        .first()
      )

    if not subscription:
      raise HTTPException(status_code=404, detail="Checkout session not found")

    # Security check: ensure user belongs to subscription's org
    membership = OrgUser.get_by_org_and_user(
      org_id=subscription.org_id,
      user_id=current_user.id,
      session=db,
    )
    if not membership:
      raise HTTPException(
        status_code=403, detail="Not authorized to access this checkout session"
      )

    return CheckoutStatusResponse(
      status=subscription.status,
      subscription_id=str(subscription.id),
      resource_id=subscription.resource_id,
      operation_id=subscription.subscription_metadata.get("operation_id"),
      error=subscription.subscription_metadata.get("error"),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get checkout status: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve checkout status")
