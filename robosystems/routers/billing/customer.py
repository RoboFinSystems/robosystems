"""Billing customer endpoints for managing payment methods and customer info."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.iam import User
from ...models.billing import BillingCustomer as BillingCustomerModel
from ...models.api.billing.customer import (
  BillingCustomer,
  PaymentMethod,
  UpdatePaymentMethodRequest,
  UpdatePaymentMethodResponse,
)
from ...operations.providers.payment_provider import get_payment_provider
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/billing/customer", tags=["Billing"])


@router.get(
  "/{org_id}",
  response_model=BillingCustomer,
  summary="Get Organization Customer Info",
  description="""Get billing customer information for an organization including payment methods on file.

Returns customer details, payment methods, and whether invoice billing is enabled.

**Requirements:**
- User must be a member of the organization
- Sensitive payment details are only visible to owners""",
  operation_id="getOrgBillingCustomer",
)
async def get_customer(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Get billing customer information for an organization."""
  try:
    from ...models.iam import OrgUser, OrgRole

    # Verify user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    # Get or create customer for the org
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

  except Exception as e:
    logger.error(f"Failed to get customer info: {e}", exc_info=True)
    raise HTTPException(
      status_code=500, detail="Failed to retrieve customer information"
    )


@router.post(
  "/{org_id}/payment-method",
  response_model=UpdatePaymentMethodResponse,
  summary="Update Organization Default Payment Method",
  description="""Update the default payment method for the organization.

This changes which payment method will be used for future subscription charges.

**Requirements:**
- User must be an OWNER of the organization""",
  operation_id="updateOrgPaymentMethod",
)
async def update_payment_method(
  org_id: str,
  request: UpdatePaymentMethodRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Update default payment method for organization."""
  try:
    from ...models.iam import OrgUser, OrgRole

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
        detail="Only organization owners can update payment methods",
      )

    customer = BillingCustomerModel.get_or_create(org_id, db)

    if not customer.stripe_customer_id:
      raise HTTPException(status_code=400, detail="No Stripe customer ID found")

    provider = get_payment_provider("stripe")
    updated_pm = provider.update_default_payment_method(
      customer.stripe_customer_id, request.payment_method_id
    )

    logger.info(
      f"Updated default payment method for org {org_id}",
      extra={
        "org_id": org_id,
        "user_id": current_user.id,
        "payment_method_id": request.payment_method_id,
      },
    )

    return UpdatePaymentMethodResponse(
      message="Default payment method updated successfully",
      payment_method=PaymentMethod(
        id=updated_pm["id"],
        type=updated_pm["type"],
        brand=updated_pm.get("card", {}).get("brand"),
        last4=updated_pm.get("card", {}).get("last4"),
        exp_month=updated_pm.get("card", {}).get("exp_month"),
        exp_year=updated_pm.get("card", {}).get("exp_year"),
        is_default=True,
      ),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to update payment method: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to update payment method")
