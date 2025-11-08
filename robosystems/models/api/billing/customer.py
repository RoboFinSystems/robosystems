"""Customer and payment method API models."""

from pydantic import BaseModel, Field


class PaymentMethod(BaseModel):
  """Payment method information."""

  id: str = Field(..., description="Payment method ID")
  type: str = Field(..., description="Payment method type (card, bank_account, etc.)")
  brand: str | None = Field(None, description="Card brand (visa, mastercard, etc.)")
  last4: str | None = Field(None, description="Last 4 digits")
  exp_month: int | None = Field(None, description="Expiration month")
  exp_year: int | None = Field(None, description="Expiration year")
  is_default: bool = Field(
    ..., description="Whether this is the default payment method"
  )


class BillingCustomer(BaseModel):
  """Billing customer information for an organization."""

  org_id: str = Field(..., description="Organization ID")
  has_payment_method: bool = Field(
    ..., description="Whether organization has a payment method on file"
  )
  invoice_billing_enabled: bool = Field(
    ..., description="Whether invoice billing is enabled (enterprise customers)"
  )
  payment_methods: list[PaymentMethod] = Field(
    ..., description="List of payment methods on file"
  )
  stripe_customer_id: str | None = Field(
    None, description="Stripe customer ID if applicable"
  )
  created_at: str = Field(..., description="Customer creation timestamp (ISO format)")


class UpdatePaymentMethodRequest(BaseModel):
  """Request to update default payment method."""

  payment_method_id: str = Field(..., description="Payment method ID to set as default")


class UpdatePaymentMethodResponse(BaseModel):
  """Response for payment method update."""

  message: str = Field(..., description="Success message")
  payment_method: PaymentMethod = Field(..., description="Updated payment method")
