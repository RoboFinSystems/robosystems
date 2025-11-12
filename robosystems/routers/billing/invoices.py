"""Billing invoices endpoints for payment history."""

from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.iam import User
from ...models.billing import BillingCustomer, BillingInvoice
from ...models.api.billing.invoice import (
  Invoice,
  InvoiceLineItem,
  InvoicesResponse,
  UpcomingInvoice,
)
from ...operations.providers.payment_provider import get_payment_provider
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/billing/invoices", tags=["Billing"])


@router.get(
  "/{org_id}",
  response_model=InvoicesResponse,
  summary="List Organization Invoices",
  description="""List payment history and invoices for an organization.

Returns past invoices with payment status, amounts, and line items.

**Requirements:**
- User must be a member of the organization
- Full invoice details are only visible to owners and admins""",
  operation_id="listOrgInvoices",
)
async def list_invoices(
  org_id: str,
  limit: int = Query(10, ge=1, le=100, description="Number of invoices to return"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """List invoices for the organization."""
  try:
    from ...models.iam import OrgUser, OrgRole

    # Verify user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    # Only owners and admins can see invoices
    if membership.role not in [OrgRole.OWNER, OrgRole.ADMIN]:
      raise HTTPException(
        status_code=403,
        detail="Only owners and admins can view invoices",
      )

    customer = BillingCustomer.get_or_create(org_id, db)

    if not customer.stripe_customer_id:
      local_invoices = BillingInvoice.get_by_org_id(org_id, db)
      invoices = []
      for inv in local_invoices[:limit]:
        invoices.append(
          Invoice(
            id=inv.id,
            number=inv.invoice_number,
            status=inv.status,
            amount_due=inv.total_cents,
            amount_paid=inv.total_cents if inv.status == "paid" else 0,
            currency="usd",
            created=inv.created_at.isoformat(),
            due_date=inv.due_date.isoformat() if inv.due_date else None,
            paid_at=inv.paid_at.isoformat() if inv.paid_at else None,
            invoice_pdf=None,
            hosted_invoice_url=None,
            line_items=[
              InvoiceLineItem(
                description=line.description,
                amount=line.amount_cents,
                quantity=line.quantity,
                period_start=line.period_start.isoformat()
                if line.period_start
                else None,
                period_end=line.period_end.isoformat() if line.period_end else None,
              )
              for line in inv.line_items
            ],
            subscription_id=None,
          )
        )
      return InvoicesResponse(
        invoices=invoices,
        total_count=len(local_invoices),
        has_more=len(local_invoices) > limit,
      )

    provider = get_payment_provider("stripe")
    result = provider.list_invoices(customer.stripe_customer_id, limit=limit)

    invoices = []
    for inv in result["invoices"]:
      invoices.append(
        Invoice(
          id=inv["id"],
          number=inv["number"],
          status=inv["status"],
          amount_due=inv["amount_due"],
          amount_paid=inv["amount_paid"],
          currency=inv["currency"],
          created=datetime.fromtimestamp(inv["created"]).isoformat(),
          due_date=(
            datetime.fromtimestamp(inv["due_date"]).isoformat()
            if inv["due_date"]
            else None
          ),
          paid_at=(
            datetime.fromtimestamp(inv["paid_at"]).isoformat()
            if inv["paid_at"]
            else None
          ),
          invoice_pdf=inv["invoice_pdf"],
          hosted_invoice_url=inv["hosted_invoice_url"],
          line_items=[
            InvoiceLineItem(
              description=line["description"],
              amount=line["amount"],
              quantity=line["quantity"],
              period_start=(
                datetime.fromtimestamp(line["period_start"]).isoformat()
                if line["period_start"]
                else None
              ),
              period_end=(
                datetime.fromtimestamp(line["period_end"]).isoformat()
                if line["period_end"]
                else None
              ),
            )
            for line in inv["lines"]
          ],
          subscription_id=inv["subscription"],
        )
      )

    return InvoicesResponse(
      invoices=invoices,
      total_count=len(invoices),
      has_more=result["has_more"],
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to list invoices: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve invoices")


@router.get(
  "/{org_id}/upcoming",
  response_model=UpcomingInvoice | None,
  summary="Get Organization Upcoming Invoice",
  description="""Get preview of the next invoice for an organization.

Returns estimated charges for the next billing period.

**Requirements:**
- User must be a member of the organization
- Full invoice details are only visible to owners and admins""",
  operation_id="getOrgUpcomingInvoice",
)
async def get_upcoming_invoice(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Get upcoming invoice preview for organization."""
  try:
    from ...models.iam import OrgUser, OrgRole

    # Verify user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=403,
        detail="You are not a member of this organization",
      )

    # Only owners and admins can see upcoming invoices
    if membership.role not in [OrgRole.OWNER, OrgRole.ADMIN]:
      raise HTTPException(
        status_code=403,
        detail="Only owners and admins can view upcoming invoices",
      )

    customer = BillingCustomer.get_or_create(org_id, db)

    if not customer.stripe_customer_id:
      return None

    provider = get_payment_provider("stripe")
    upcoming = provider.get_upcoming_invoice(customer.stripe_customer_id)

    if not upcoming:
      return None

    return UpcomingInvoice(
      amount_due=upcoming["amount_due"],
      currency=upcoming["currency"],
      period_start=datetime.fromtimestamp(upcoming["period_start"]).isoformat(),
      period_end=datetime.fromtimestamp(upcoming["period_end"]).isoformat(),
      line_items=[
        InvoiceLineItem(
          description=line["description"],
          amount=line["amount"],
          quantity=line["quantity"],
          period_start=(
            datetime.fromtimestamp(line["period_start"]).isoformat()
            if line["period_start"]
            else None
          ),
          period_end=(
            datetime.fromtimestamp(line["period_end"]).isoformat()
            if line["period_end"]
            else None
          ),
        )
        for line in upcoming["lines"]
      ],
      subscription_id=upcoming["subscription"],
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get upcoming invoice: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve upcoming invoice")
