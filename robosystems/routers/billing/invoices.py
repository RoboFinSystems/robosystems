"""Billing invoices endpoints for payment history."""

from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.iam import User
from ...models.billing import BillingCustomer
from ...models.api.billing.invoice import (
  Invoice,
  InvoiceLineItem,
  InvoicesResponse,
  UpcomingInvoice,
)
from ...operations.billing.payment_provider import get_payment_provider
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/billing/invoices", tags=["Billing"])


@router.get(
  "",
  response_model=InvoicesResponse,
  summary="List Invoices",
  description="""List payment history and invoices.

Returns past invoices with payment status, amounts, and line items.""",
  operation_id="listInvoices",
)
async def list_invoices(
  limit: int = Query(10, ge=1, le=100, description="Number of invoices to return"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """List invoices for the current user."""
  try:
    customer = BillingCustomer.get_or_create(current_user.id, db)

    if not customer.stripe_customer_id:
      return InvoicesResponse(
        invoices=[],
        total_count=0,
        has_more=False,
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

  except Exception as e:
    logger.error(f"Failed to list invoices: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve invoices")


@router.get(
  "/upcoming",
  response_model=UpcomingInvoice | None,
  summary="Get Upcoming Invoice",
  description="""Get preview of the next invoice.

Returns estimated charges for the next billing period.""",
  operation_id="getUpcomingInvoice",
)
async def get_upcoming_invoice(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Get upcoming invoice preview."""
  try:
    customer = BillingCustomer.get_or_create(current_user.id, db)

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

  except Exception as e:
    logger.error(f"Failed to get upcoming invoice: {e}", exc_info=True)
    raise HTTPException(status_code=500, detail="Failed to retrieve upcoming invoice")
