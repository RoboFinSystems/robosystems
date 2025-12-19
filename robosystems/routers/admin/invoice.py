"""Admin API for invoice management."""


from fastapi import APIRouter, HTTPException, Query, Request

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.admin import require_admin
from ...models.api.admin import InvoiceLineItemResponse, InvoiceResponse
from ...models.billing import BillingInvoice, BillingInvoiceLineItem
from ...models.iam import User

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/invoices", tags=["admin"])


@router.get("", response_model=list[InvoiceResponse])
@require_admin(permissions=["subscription:read"])
async def list_invoices(
  request: Request,
  status: str | None = None,
  user_id: str | None = None,
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all invoices with optional filtering."""
  session = next(get_db_session())
  try:
    query = session.query(BillingInvoice)

    if status:
      query = query.filter(BillingInvoice.status == status)

    if user_id:
      from ...models.iam import OrgUser

      query = query.join(OrgUser, BillingInvoice.org_id == OrgUser.org_id)
      query = query.filter(OrgUser.user_id == user_id)

    total = query.count()
    invoices = (
      query.order_by(BillingInvoice.created_at.desc()).offset(offset).limit(limit).all()
    )

    results = []
    for invoice in invoices:
      from ...models.iam import OrgUser

      owner = (
        session.query(OrgUser)
        .join(User, OrgUser.user_id == User.id)
        .filter(
          OrgUser.org_id == invoice.org_id,
          OrgUser.role == "OWNER",
        )
        .first()
      )

      user = owner.user if owner else None

      line_items = (
        session.query(BillingInvoiceLineItem)
        .filter(BillingInvoiceLineItem.invoice_id == invoice.id)
        .all()
      )

      results.append(
        InvoiceResponse(
          id=invoice.id,
          invoice_number=invoice.invoice_number,
          billing_customer_user_id=user.id if user else invoice.org_id,
          user_email=user.email if user else None,
          user_name=user.name if user else None,
          status=invoice.status,
          subtotal_cents=invoice.subtotal_cents,
          tax_cents=invoice.tax_cents,
          discount_cents=invoice.discount_cents,
          total_cents=invoice.total_cents,
          period_start=invoice.period_start,
          period_end=invoice.period_end,
          due_date=invoice.due_date,
          payment_terms=invoice.payment_terms,
          payment_method=invoice.payment_method,
          payment_reference=invoice.payment_reference,
          sent_at=invoice.sent_at,
          paid_at=invoice.paid_at,
          voided_at=invoice.voided_at,
          created_at=invoice.created_at,
          line_items=[
            InvoiceLineItemResponse(
              id=item.id,
              subscription_id=item.subscription_id,
              resource_type=item.resource_type,
              resource_id=item.resource_id,
              description=item.description,
              quantity=item.quantity,
              unit_price_cents=item.unit_price_cents,
              amount_cents=item.amount_cents,
              line_metadata=item.line_metadata,
            )
            for item in line_items
          ],
        )
      )

    logger.info(
      f"Admin listed {len(results)} invoices",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
        "filters": {"status": status, "user_id": user_id},
      },
    )

    return results
  finally:
    session.close()


@router.get("/{invoice_id}", response_model=InvoiceResponse)
@require_admin(permissions=["subscription:read"])
async def get_invoice(
  request: Request,
  invoice_id: str,
):
  """Get details of a specific invoice."""
  session = next(get_db_session())
  try:
    invoice = (
      session.query(BillingInvoice).filter(BillingInvoice.id == invoice_id).first()
    )

    if not invoice:
      raise HTTPException(status_code=404, detail="Invoice not found")

    from ...models.iam import OrgUser

    owner = (
      session.query(OrgUser)
      .join(User, OrgUser.user_id == User.id)
      .filter(
        OrgUser.org_id == invoice.org_id,
        OrgUser.role == "OWNER",
      )
      .first()
    )

    user = owner.user if owner else None

    line_items = (
      session.query(BillingInvoiceLineItem)
      .filter(BillingInvoiceLineItem.invoice_id == invoice.id)
      .all()
    )

    logger.info(
      f"Admin retrieved invoice {invoice_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "invoice_id": invoice_id,
      },
    )

    return InvoiceResponse(
      id=invoice.id,
      invoice_number=invoice.invoice_number,
      billing_customer_user_id=user.id if user else invoice.org_id,
      user_email=user.email if user else None,
      user_name=user.name if user else None,
      status=invoice.status,
      subtotal_cents=invoice.subtotal_cents,
      tax_cents=invoice.tax_cents,
      discount_cents=invoice.discount_cents,
      total_cents=invoice.total_cents,
      period_start=invoice.period_start,
      period_end=invoice.period_end,
      due_date=invoice.due_date,
      payment_terms=invoice.payment_terms,
      payment_method=invoice.payment_method,
      payment_reference=invoice.payment_reference,
      sent_at=invoice.sent_at,
      paid_at=invoice.paid_at,
      voided_at=invoice.voided_at,
      created_at=invoice.created_at,
      line_items=[
        InvoiceLineItemResponse(
          id=item.id,
          subscription_id=item.subscription_id,
          resource_type=item.resource_type,
          resource_id=item.resource_id,
          description=item.description,
          quantity=item.quantity,
          unit_price_cents=item.unit_price_cents,
          amount_cents=item.amount_cents,
          line_metadata=item.line_metadata,
        )
        for item in line_items
      ],
    )
  finally:
    session.close()


@router.patch("/{invoice_id}/mark-paid")
@require_admin(permissions=["subscription:write"])
async def mark_invoice_paid(
  request: Request,
  invoice_id: str,
  payment_method: str = Query(...),
  payment_reference: str | None = Query(None),
):
  """Mark an invoice as paid."""
  session = next(get_db_session())
  try:
    invoice = (
      session.query(BillingInvoice).filter(BillingInvoice.id == invoice_id).first()
    )

    if not invoice:
      raise HTTPException(status_code=404, detail="Invoice not found")

    if invoice.status == "PAID":
      raise HTTPException(status_code=400, detail="Invoice is already paid")

    invoice.mark_paid(
      session=session,
      payment_method=payment_method,
      payment_reference=payment_reference,
    )

    logger.info(
      f"Admin marked invoice {invoice_id} as paid",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "invoice_id": invoice_id,
        "payment_method": payment_method,
      },
    )

    from ...models.iam import OrgUser

    owner = (
      session.query(OrgUser)
      .join(User, OrgUser.user_id == User.id)
      .filter(
        OrgUser.org_id == invoice.org_id,
        OrgUser.role == "OWNER",
      )
      .first()
    )

    user = owner.user if owner else None

    line_items = (
      session.query(BillingInvoiceLineItem)
      .filter(BillingInvoiceLineItem.invoice_id == invoice.id)
      .all()
    )

    return InvoiceResponse(
      id=invoice.id,
      invoice_number=invoice.invoice_number,
      billing_customer_user_id=user.id if user else invoice.org_id,
      user_email=user.email if user else None,
      user_name=user.name if user else None,
      status=invoice.status,
      subtotal_cents=invoice.subtotal_cents,
      tax_cents=invoice.tax_cents,
      discount_cents=invoice.discount_cents,
      total_cents=invoice.total_cents,
      period_start=invoice.period_start,
      period_end=invoice.period_end,
      due_date=invoice.due_date,
      payment_terms=invoice.payment_terms,
      payment_method=invoice.payment_method,
      payment_reference=invoice.payment_reference,
      sent_at=invoice.sent_at,
      paid_at=invoice.paid_at,
      voided_at=invoice.voided_at,
      created_at=invoice.created_at,
      line_items=[
        InvoiceLineItemResponse(
          id=item.id,
          subscription_id=item.subscription_id,
          resource_type=item.resource_type,
          resource_id=item.resource_id,
          description=item.description,
          quantity=item.quantity,
          unit_price_cents=item.unit_price_cents,
          amount_cents=item.amount_cents,
          line_metadata=item.line_metadata,
        )
        for item in line_items
      ],
    )
  finally:
    session.close()
