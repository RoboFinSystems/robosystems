"""Invoice API models for admin endpoints."""

from datetime import datetime

from pydantic import BaseModel


class InvoiceLineItemResponse(BaseModel):
  """Response with invoice line item details."""

  id: str
  subscription_id: str | None
  resource_type: str
  resource_id: str
  description: str
  quantity: int
  unit_price_cents: int
  amount_cents: int
  line_metadata: dict | None


class InvoiceResponse(BaseModel):
  """Response with invoice details."""

  id: str
  invoice_number: str
  billing_customer_user_id: str
  user_email: str | None
  user_name: str | None
  status: str
  subtotal_cents: int
  tax_cents: int
  discount_cents: int
  total_cents: int
  period_start: datetime
  period_end: datetime
  due_date: datetime | None
  payment_method: str | None
  payment_reference: str | None
  sent_at: datetime | None
  paid_at: datetime | None
  created_at: datetime
  line_items: list[InvoiceLineItemResponse]


class UnusedTimeCreditItem(BaseModel):
  """One subscription's unused-time credit, planned or applied."""

  subscription_id: str
  org_id: str
  resource_id: str | None
  billed_invoice_number: str
  billed_invoice_status: str
  billed_cents: int
  credit_cents: int
  unused_from: datetime
  on_billed_invoice: bool


class UnusedTimeCreditRunResponse(BaseModel):
  """Result of an unused-time credit backfill pass."""

  dry_run: bool
  credits: list[UnusedTimeCreditItem]
  total_credit_cents: int
  failed_subscription_ids: list[str]
