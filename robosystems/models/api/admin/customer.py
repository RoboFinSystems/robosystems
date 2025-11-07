"""Customer API models for admin endpoints."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class CustomerResponse(BaseModel):
  """Response with customer billing details."""

  user_id: str
  user_email: Optional[str]
  user_name: Optional[str]
  stripe_customer_id: Optional[str]
  has_payment_method: bool
  default_payment_method_id: Optional[str]
  invoice_billing_enabled: bool
  billing_email: Optional[str]
  billing_contact_name: Optional[str]
  payment_terms: str
  created_at: datetime
  updated_at: datetime
