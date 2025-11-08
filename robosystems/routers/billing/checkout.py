"""Billing checkout endpoints for payment collection."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.iam import User
from ...models.billing import BillingCustomer, BillingSubscription
from ...models.api.billing.checkout import (
  CreateCheckoutRequest,
  CheckoutResponse,
  CheckoutStatusResponse,
)
from ...operations.providers.payment_provider import get_payment_provider
from ...config import BillingConfig, env
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/billing", tags=["Billing"])


@router.post(
  "/checkout",
  response_model=CheckoutResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Create Payment Checkout Session",
  description="""Create a Stripe checkout session for collecting payment method.

This endpoint is used when an organization owner needs to add a payment method before
provisioning resources. It creates a pending subscription and redirects
to Stripe Checkout to collect payment details.

**Flow:**
1. Owner tries to create a graph but org has no payment method
2. Frontend calls this endpoint with graph configuration
3. Backend creates a subscription in PENDING_PAYMENT status for the user's org
4. Returns Stripe Checkout URL
5. User completes payment on Stripe
6. Webhook activates subscription and provisions resource

**Requirements:**
- User must be an OWNER of their organization
- Enterprise customers (with invoice_billing_enabled) should not call this endpoint.""",
  operation_id="createCheckoutSession",
)
async def create_checkout_session(
  request: CreateCheckoutRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Create Stripe checkout session for payment collection."""
  if not env.BILLING_ENABLED:
    return CheckoutResponse(
      checkout_url=None,
      session_id=None,
      subscription_id=None,
      requires_checkout=False,
      billing_disabled=True,
    )

  try:
    from ...models.iam import OrgUser, OrgRole

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
    if request.resource_type == "graph":
      plan_config = BillingConfig.get_subscription_plan(request.plan_name)
    elif request.resource_type == "repository":
      repo_name = request.resource_config.get("repository_name")
      if repo_name:
        plan_config = BillingConfig.get_repository_plan(repo_name, request.plan_name)

    if not plan_config:
      raise HTTPException(
        status_code=400,
        detail=f"Invalid plan '{request.plan_name}' for {request.resource_type}",
      )

    base_price_cents = plan_config.get(
      "base_price_cents", plan_config.get("price_cents", 0)
    )

    # Create subscription in PENDING_PAYMENT status
    subscription = BillingSubscription.create_subscription(
      org_id=org_id,
      resource_type=request.resource_type,
      resource_id=None,  # Will be set after provisioning
      plan_name=request.plan_name,
      base_price_cents=base_price_cents,
      session=db,
      billing_interval="monthly",
    )

    # Store resource configuration and user_id in metadata
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
      provider = get_payment_provider("stripe")
      stripe_customer_id = provider.create_customer(current_user.id, current_user.email)
      customer.stripe_customer_id = stripe_customer_id
      db.commit()

    # Get payment provider and get/create Stripe price from billing config
    provider = get_payment_provider("stripe")
    try:
      stripe_price_id = provider.get_or_create_price(
        plan_name=request.plan_name, resource_type=request.resource_type
      )
    except ValueError as e:
      logger.error(f"Failed to get Stripe price: {e}")
      raise HTTPException(
        status_code=500,
        detail=f"Payment configuration error: {str(e)}",
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
  description="""Poll the status of a checkout session.

Frontend should poll this endpoint after user returns from Stripe Checkout
to determine when the resource is ready.

**Status Values:**
- `pending_payment`: Waiting for payment to complete
- `provisioning`: Payment confirmed, resource being created
- `completed`: Resource is ready (resource_id will be set)
- `failed`: Something went wrong (error field will be set)

**When status is 'completed':**
- For graphs: `resource_id` will be the graph_id, and `operation_id` can be used to monitor SSE progress
- For repositories: `resource_id` will be the repository name and access is immediately available""",
  operation_id="getCheckoutStatus",
)
async def get_checkout_status(
  session_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Get status of a checkout session."""
  try:
    from ...models.iam import OrgUser

    subscription = BillingSubscription.get_by_provider_subscription_id(session_id, db)

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
