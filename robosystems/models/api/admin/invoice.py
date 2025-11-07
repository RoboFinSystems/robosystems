"""Invoice API models for admin endpoints."""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class InvoiceLineItemResponse(BaseModel):
  """Response with invoice line item details."""

  id: str
  subscription_id: Optional[str]
  resource_type: str
  resource_id: str
  description: str
  quantity: int
  unit_price_cents: int
  amount_cents: int
  line_metadata: Optional[dict]


class InvoiceResponse(BaseModel):
  """Response with invoice details."""

  id: str
  invoice_number: str
  billing_customer_user_id: str
  user_email: Optional[str]
  user_name: Optional[str]
  status: str
  subtotal_cents: int
  tax_cents: int
  discount_cents: int
  total_cents: int
  period_start: datetime
  period_end: datetime
  due_date: Optional[datetime]
  payment_terms: str
  payment_method: Optional[str]
  payment_reference: Optional[str]
  sent_at: Optional[datetime]
  paid_at: Optional[datetime]
  voided_at: Optional[datetime]
  created_at: datetime
  line_items: List[InvoiceLineItemResponse]
