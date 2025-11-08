"""Admin API for customer management."""

from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Request, Query

from ...database import get_db_session
from ...models.billing import BillingCustomer, BillingAuditLog
from ...models.iam import User
from ...models.api.admin import CustomerResponse
from ...middleware.auth.admin import require_admin
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/customers", tags=["admin"])


@router.get("", response_model=List[CustomerResponse])
@require_admin(permissions=["subscription:read"])
async def list_customers(
  request: Request,
  has_payment_method: Optional[bool] = None,
  invoice_billing_enabled: Optional[bool] = None,
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all billing customers."""
  session = next(get_db_session())
  try:
    query = session.query(BillingCustomer).join(
      User, BillingCustomer.user_id == User.id
    )

    if has_payment_method is not None:
      query = query.filter(BillingCustomer.has_payment_method == has_payment_method)

    if invoice_billing_enabled is not None:
      query = query.filter(
        BillingCustomer.invoice_billing_enabled == invoice_billing_enabled
      )

    total = query.count()
    customers = query.offset(offset).limit(limit).all()

    results = []
    for customer in customers:
      user = session.query(User).filter(User.id == customer.user_id).first()
      results.append(
        CustomerResponse(
          user_id=customer.user_id,
          user_email=user.email if user else None,
          user_name=user.name if user else None,
          stripe_customer_id=customer.stripe_customer_id,
          has_payment_method=customer.has_payment_method,
          default_payment_method_id=customer.default_payment_method_id,
          invoice_billing_enabled=customer.invoice_billing_enabled,
          billing_email=customer.billing_email,
          billing_contact_name=customer.billing_contact_name,
          payment_terms=customer.payment_terms,
          created_at=customer.created_at,
          updated_at=customer.updated_at,
        )
      )

    logger.info(
      f"Admin listed {len(results)} customers",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
      },
    )

    return results
  finally:
    session.close()


@router.patch("/{user_id}")
@require_admin(permissions=["subscription:write"])
async def update_customer(
  request: Request,
  user_id: str,
  invoice_billing_enabled: Optional[bool] = None,
  billing_email: Optional[str] = None,
  billing_contact_name: Optional[str] = None,
  payment_terms: Optional[str] = None,
):
  """Update billing customer settings."""
  session = next(get_db_session())
  try:
    customer = (
      session.query(BillingCustomer).filter(BillingCustomer.user_id == user_id).first()
    )

    if not customer:
      customer = BillingCustomer.get_or_create(user_id, session)

    old_values = {}
    new_values = {}

    if (
      invoice_billing_enabled is not None
      and invoice_billing_enabled != customer.invoice_billing_enabled
    ):
      old_values["invoice_billing_enabled"] = customer.invoice_billing_enabled
      new_values["invoice_billing_enabled"] = invoice_billing_enabled
      customer.invoice_billing_enabled = invoice_billing_enabled

    if billing_email is not None and billing_email != customer.billing_email:
      old_values["billing_email"] = customer.billing_email
      new_values["billing_email"] = billing_email
      customer.billing_email = billing_email

    if (
      billing_contact_name is not None
      and billing_contact_name != customer.billing_contact_name
    ):
      old_values["billing_contact_name"] = customer.billing_contact_name
      new_values["billing_contact_name"] = billing_contact_name
      customer.billing_contact_name = billing_contact_name

    if payment_terms is not None and payment_terms != customer.payment_terms:
      old_values["payment_terms"] = customer.payment_terms
      new_values["payment_terms"] = payment_terms
      customer.payment_terms = payment_terms

    customer.updated_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(customer)

    if old_values:
      sanitized_old_values = {
        k: v for k, v in old_values.items() if k != "payment_terms"
      }
      sanitized_new_values = {
        k: v for k, v in new_values.items() if k != "payment_terms"
      }

      if "payment_terms" in old_values:
        sanitized_old_values["payment_terms"] = "[REDACTED]"
        sanitized_new_values["payment_terms"] = "[REDACTED]"

      BillingAuditLog.log_event(
        session=session,
        event_type="customer.updated",
        billing_customer_user_id=user_id,
        actor_type="admin",
        description=f"Customer updated by admin {request.state.admin.get('name', 'unknown')}",
        event_data={
          "admin_key_id": request.state.admin_key_id,
          "admin_name": request.state.admin.get("name"),
          "old_values": sanitized_old_values,
          "new_values": sanitized_new_values,
        },
      )

    user = session.query(User).filter(User.id == user_id).first()

    log_changes = {k: v for k, v in new_values.items() if k != "payment_terms"}
    if "payment_terms" in new_values:
      log_changes["payment_terms"] = "[REDACTED]"

    logger.info(
      f"Admin updated customer {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "changes": log_changes,
      },
    )

    return CustomerResponse(
      user_id=customer.user_id,
      user_email=user.email if user else None,
      user_name=user.name if user else None,
      stripe_customer_id=customer.stripe_customer_id,
      has_payment_method=customer.has_payment_method,
      default_payment_method_id=customer.default_payment_method_id,
      invoice_billing_enabled=customer.invoice_billing_enabled,
      billing_email=customer.billing_email,
      billing_contact_name=customer.billing_contact_name,
      payment_terms=customer.payment_terms,
      created_at=customer.created_at,
      updated_at=customer.updated_at,
    )
  finally:
    session.close()
