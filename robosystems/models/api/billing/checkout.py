"""API models for billing checkout operations."""

from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class CreateCheckoutRequest(BaseModel):
  """Request to create a checkout session for payment collection."""

  plan_name: str = Field(..., description="Billing plan name (e.g., 'kuzu-standard')")
  resource_type: str = Field(..., description="Resource type ('graph' or 'repository')")
  resource_config: Dict[str, Any] = Field(
    ..., description="Configuration for the resource to be provisioned"
  )


class CheckoutResponse(BaseModel):
  """Response from checkout session creation."""

  checkout_url: Optional[str] = Field(
    None, description="URL to redirect user to for payment"
  )
  session_id: Optional[str] = Field(
    None, description="Checkout session ID for status polling"
  )
  subscription_id: Optional[str] = Field(None, description="Internal subscription ID")
  requires_checkout: bool = Field(
    default=True, description="Whether checkout is required"
  )
  billing_disabled: bool = Field(
    default=False, description="Whether billing is disabled on this instance"
  )


class CheckoutStatusResponse(BaseModel):
  """Status of a checkout session."""

  status: str = Field(
    ...,
    description="Checkout status: 'pending_payment', 'provisioning', 'completed', 'failed'",
  )
  subscription_id: str = Field(..., description="Internal subscription ID")
  resource_id: Optional[str] = Field(
    None, description="Resource ID (graph_id or repository name) once provisioned"
  )
  operation_id: Optional[str] = Field(
    None, description="SSE operation ID for monitoring provisioning progress"
  )
  error: Optional[str] = Field(None, description="Error message if checkout failed")
