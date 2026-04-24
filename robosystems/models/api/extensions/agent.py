"""Agent API models — counterparty CRUD surface."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class CreateAgentRequest(BaseModel):
  agent_type: str = Field(
    ...,
    description="'customer' | 'vendor' | 'employee' | 'owner' | 'supplier' | 'government' | 'lender' | 'self' | 'other'",
  )
  name: str
  legal_name: str | None = None

  # Economic identifiers
  tax_id: str | None = None
  registration_number: str | None = None
  duns: str | None = None
  lei: str | None = None

  # Contact
  email: str | None = None
  phone: str | None = None
  address: dict | None = None

  # Source system linkage
  source: str = Field(
    "native", description="'quickbooks' | 'xero' | 'plaid' | 'native'"
  )
  external_id: str | None = None

  # State
  is_active: bool = True
  is_1099_recipient: bool = False

  metadata: dict = Field(default_factory=dict)


class UpdateAgentRequest(BaseModel):
  agent_id: str

  name: str | None = None
  legal_name: str | None = None
  tax_id: str | None = None
  registration_number: str | None = None
  duns: str | None = None
  lei: str | None = None
  email: str | None = None
  phone: str | None = None
  address: dict | None = None
  is_active: bool | None = None
  is_1099_recipient: bool | None = None
  metadata_patch: dict = Field(default_factory=dict)


class AgentResponse(BaseModel):
  model_config = ConfigDict(from_attributes=True)

  id: str
  agent_type: str
  name: str
  legal_name: str | None = None

  tax_id: str | None = None
  registration_number: str | None = None
  duns: str | None = None
  lei: str | None = None

  email: str | None = None
  phone: str | None = None
  address: dict | None = None

  source: str
  external_id: str | None = None

  is_active: bool
  is_1099_recipient: bool

  created_at: datetime | None = None
  updated_at: datetime | None = None
  created_by: str | None = None


class TransactionSummary(BaseModel):
  """Minimal transaction projection for agent activity timeline."""

  id: str
  type: str
  date: str
  amount: int
  currency: str
  status: str
  description: str | None = None
  triggered_by_event_id: str | None = None


class AgentActivityResponse(BaseModel):
  agent: AgentResponse
  recent_events: list
  recent_transactions: list[TransactionSummary]
  event_count: int
  transaction_count: int
