"""Invoice and payment history API models."""

from pydantic import BaseModel, Field


class InvoiceLineItem(BaseModel):
  """Invoice line item."""

  description: str = Field(..., description="Line item description")
  amount: int = Field(..., description="Amount in cents")
  quantity: int = Field(..., description="Quantity")
  period_start: str | None = Field(None, description="Billing period start")
  period_end: str | None = Field(None, description="Billing period end")


class Invoice(BaseModel):
  """Invoice information."""

  id: str = Field(..., description="Invoice ID")
  number: str | None = Field(None, description="Invoice number")
  status: str = Field(
    ..., description="Invoice status (paid, open, void, uncollectible)"
  )
  amount_due: int = Field(..., description="Amount due in cents")
  amount_paid: int = Field(..., description="Amount paid in cents")
  currency: str = Field(..., description="Currency code (usd)")
  created: str = Field(..., description="Invoice creation date (ISO format)")
  due_date: str | None = Field(None, description="Invoice due date (ISO format)")
  paid_at: str | None = Field(None, description="Payment date (ISO format)")
  invoice_pdf: str | None = Field(None, description="PDF download URL")
  hosted_invoice_url: str | None = Field(None, description="Hosted invoice URL")
  line_items: list[InvoiceLineItem] = Field(..., description="Invoice line items")
  subscription_id: str | None = Field(None, description="Associated subscription ID")


class InvoicesResponse(BaseModel):
  """Response for invoice list."""

  invoices: list[Invoice] = Field(..., description="List of invoices")
  total_count: int = Field(..., description="Total number of invoices")
  has_more: bool = Field(..., description="Whether more invoices are available")


class UpcomingInvoice(BaseModel):
  """Upcoming invoice preview."""

  amount_due: int = Field(..., description="Estimated amount due in cents")
  currency: str = Field(..., description="Currency code")
  period_start: str = Field(..., description="Billing period start")
  period_end: str = Field(..., description="Billing period end")
  line_items: list[InvoiceLineItem] = Field(..., description="Estimated line items")
  subscription_id: str | None = Field(None, description="Associated subscription ID")
