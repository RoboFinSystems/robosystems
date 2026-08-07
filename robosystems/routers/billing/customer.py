"""Billing customer endpoints for managing payment methods and customer info."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import billing_rate_limit_dependency
from ...models.api.billing.customer import (
  BillingCustomer,
  PaymentMethod,
  PortalSessionResponse,
)
from ...models.api.common import RESOURCE_ERROR_RESPONSES
from ...models.core import User
from ...models.core.billing import BillingCustomer as BillingCustomerModel
from ...operations.providers.payment_provider import get_payment_provider

logger = get_logger(__name__)

router = APIRouter(prefix="/billing/customer", tags=["Billing"])


@router.get(
  "/{org_id}",
  response_model=BillingCustomer,
  summary="Get Organization Customer Info",
  description="Payment method details are only visible to org owners.",
  operation_id="getOrgBillingCustomer",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_customer(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  try:
    from ...models.core import OrgRole, OrgUser

    # Verify user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    customer = BillingCustomerModel.get_or_create(org_id, db)

    # Only owners can see full payment details
    is_owner = membership.role == OrgRole.OWNER

    payment_methods = []
    if is_owner and customer.has_payment_method and customer.stripe_customer_id:
      try:
        provider = get_payment_provider("stripe")
        stripe_payment_methods = provider.list_payment_methods(
          customer.stripe_customer_id
        )

        payment_methods = [
          PaymentMethod(
            id=pm["id"],
            type=pm["type"],
            brand=pm.get("card", {}).get("brand"),
            last4=pm.get("card", {}).get("last4"),
            exp_month=pm.get("card", {}).get("exp_month"),
            exp_year=pm.get("card", {}).get("exp_year"),
            is_default=pm.get("is_default", False),
          )
          for pm in stripe_payment_methods
        ]
      except Exception as e:
        logger.error(f"Failed to fetch payment methods: {e}", exc_info=True)

    return BillingCustomer(
      org_id=str(customer.org_id),
      has_payment_method=customer.has_payment_method,
      invoice_billing_enabled=customer.invoice_billing_enabled,
      payment_methods=payment_methods,
      stripe_customer_id=customer.stripe_customer_id if is_owner else None,
      created_at=customer.created_at.isoformat(),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get customer info: {e}", exc_info=True)
    raise HTTPException(
      status_code=500, detail="Failed to retrieve customer information"
    )


@router.post(
  "/{org_id}/portal",
  response_model=PortalSessionResponse,
  summary="Create Customer Portal Session",
  description="Returns a Stripe Customer Portal URL for managing payment methods and billing history. Requires org owner role.",
  operation_id="createPortalSession",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def create_portal_session(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(billing_rate_limit_dependency),
):
  try:
    from ...config import env
    from ...models.core import OrgRole, OrgUser

    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    if membership.role != OrgRole.OWNER:
      raise HTTPException(
        status_code=403,
        detail="Only organization owners can manage billing",
      )

    customer = BillingCustomerModel.get_or_create(org_id, db)

    provider = get_payment_provider("stripe")

    if not customer.stripe_customer_id:
      stripe_customer_id = provider.create_customer(current_user.id, current_user.email)
      customer.stripe_customer_id = stripe_customer_id
      db.commit()
      logger.info(
        f"Created Stripe customer for org {org_id} during portal session",
        extra={"org_id": org_id, "user_id": current_user.id},
      )
    # Billing lives on the org page's Billing tab; /billing still redirects here
    # for older sessions, but returning directly avoids a redirect hop.
    return_url = f"{env.ROBOSYSTEMS_URL}/organization?tab=billing"
    portal_url = provider.create_portal_session(customer.stripe_customer_id, return_url)

    logger.info(
      f"Created portal session for org {org_id}",
      extra={
        "org_id": org_id,
        "user_id": current_user.id,
      },
    )

    return PortalSessionResponse(portal_url=portal_url)

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to create portal session: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to create portal session")
