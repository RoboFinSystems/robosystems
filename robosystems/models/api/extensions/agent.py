"""Agent API models — counterparty CRUD surface.

An Agent is any economic counterparty the entity transacts with —
customers, vendors, employees, lenders, government bodies, etc. Agents
are referenced by transactions, events, and 1099 reporting. They carry
identity (legal name, tax ID, DUNS, LEI) and contact details, plus
source-system linkage for sync from QuickBooks / Xero / Plaid.

Agents have no balance and aren't elements: they're the "with whom"
dimension on Transactions. A counterparty can carry a default account,
which suggests where its bank lines go; it never posts by itself.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class CreateAgentRequest(BaseModel):
  """Create a new economic counterparty.

  `agent_type` is the relationship category (customer, vendor,
  employee, etc.) — the same legal entity may have multiple Agent rows
  if they play multiple roles (e.g. a vendor who also became a
  customer). `source` distinguishes integration-imported rows from
  native-created ones; `external_id` carries the source-system's
  primary key for sync.
  """

  agent_type: str = Field(
    ...,
    description=(
      "Relationship category: 'customer' | 'vendor' | 'employee' | "
      "'owner' | 'supplier' | 'government' | 'lender' | 'self' | 'other'."
    ),
  )
  name: str = Field(..., description="Display name shown in lists and on transactions.")
  legal_name: str | None = Field(
    None, description="Full registered legal name (when different from `name`)."
  )

  # Economic identifiers
  tax_id: str | None = Field(
    None, description="Tax ID (EIN / SSN / VAT). Used for 1099 / withholding reporting."
  )
  registration_number: str | None = Field(
    None, description="Registry-issued company number (e.g. state corp file number)."
  )
  duns: str | None = Field(None, description="Dun & Bradstreet DUNS number.")
  lei: str | None = Field(None, description="Legal Entity Identifier (ISO 17442).")

  # Contact
  email: str | None = None
  phone: str | None = None
  address: dict | None = Field(
    None,
    description=(
      "Free-form address object (e.g. "
      '{"line1": "...", "city": "...", "state": "...", "postal_code": "..."}).'
    ),
  )

  # Source system linkage
  source: str = Field(
    "native",
    description=(
      "Provenance: 'native' (created in-app), 'quickbooks' / 'xero' / "
      "'plaid' (synced from integration). Drives reconciliation behavior."
    ),
  )
  external_id: str | None = Field(
    None,
    description="Source system primary key — used to match on subsequent syncs.",
  )

  # State
  is_active: bool = Field(
    True,
    description="Inactive agents stay in history but can't be picked for new txns.",
  )
  is_1099_recipient: bool = Field(
    False,
    description=("Marks a vendor as 1099-eligible. Drives year-end 1099 reporting."),
  )

  metadata: dict = Field(default_factory=dict)

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "agent_type": "vendor",
          "name": "Acme Office Supplies",
          "tax_id": "12-3456789",
          "is_1099_recipient": True,
        },
        {
          "agent_type": "customer",
          "name": "Big Co Inc",
          "legal_name": "Big Co Incorporated",
          "email": "ap@bigco.example.com",
          "address": {
            "line1": "100 Main St",
            "city": "Seattle",
            "state": "WA",
            "postal_code": "98101",
          },
        },
        {
          "agent_type": "employee",
          "name": "Jane Q. Public",
          "source": "native",
        },
      ]
    }
  )


class UpdateAgentRequest(BaseModel):
  """Patch an agent. All fields except `agent_id` are optional —
  pass only what changes. `metadata_patch` is deep-merged into the
  existing metadata dict.
  """

  agent_id: str = Field(..., description="The agent to update.")

  name: str | None = None
  legal_name: str | None = None
  tax_id: str | None = None
  registration_number: str | None = None
  duns: str | None = None
  lei: str | None = None
  email: str | None = None
  phone: str | None = None
  address: dict | None = None
  is_active: bool | None = Field(
    None,
    description="Toggle activation. Inactive agents are hidden from new-transaction pickers.",
  )
  is_1099_recipient: bool | None = None
  classification_element_id: str | None = Field(
    None,
    description=(
      "The chart account this counterparty's bank lines are usually "
      "classified to, suggested on each new line. An empty string clears "
      "it. Its still-open lines are re-suggested at once. Omit to keep."
    ),
  )
  classification_mode: Literal["suggest", "always_ask"] | None = Field(
    None,
    description=(
      "`suggest` offers the default on each line; `always_ask` offers none, "
      "for a counterparty whose lines go to different accounts. Omit to keep."
    ),
  )
  metadata_patch: dict = Field(
    default_factory=dict,
    description=(
      "Deep-merged into agent.metadata. Pass `{}` to leave unchanged. The "
      "default classification is set with the fields above, not here."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "agent_id": "agt_01HVF8T0M2YTAY3BBNRH0V0",
          "is_1099_recipient": True,
          "tax_id": "12-3456789",
        },
        {
          "agent_id": "agt_01HVF8T0M2YTAY3BBNRH0V0",
          "is_active": False,
        },
      ]
    }
  )


class AgentClassification(BaseModel):
  """A counterparty's default account, learned from committed lines or set by
  hand."""

  element_id: str = Field(..., description="The default chart account.")
  account_name: str | None = Field(None, description="The account's name.")
  mode: str = Field(
    ...,
    description=("`suggest`: offered on each new line. `always_ask`: never offered."),
  )
  confirmations: int = Field(
    ..., description="Committed lines that went to this account."
  )
  overrides: int = Field(
    ..., description="Committed lines that went elsewhere while it was the default."
  )
  set_by: str | None = Field(None, description="Who set or last moved it.")
  set_at: str | None = Field(None, description="When it was set or last moved.")
  learned_from: str | None = Field(
    None, description="The committed line it was learned from, when it was."
  )


class LearnClassificationDefaultsRequest(BaseModel):
  """Seed counterparty defaults from the bank lines already committed."""

  dry_run: bool = Field(
    False, description="Report what would be learned without writing it."
  )


class LearnClassificationDefaultsResponse(BaseModel):
  """What seeding the defaults learned."""

  agents_learned: int = Field(
    ..., description="Counterparties given a default from their committed lines."
  )
  agents_kept: int = Field(
    ..., description="Counterparties that already had a default, left as they were."
  )
  lines_read: int = Field(
    ..., description="Committed lines classified to a single account that were read."
  )
  open_lines_resuggested: int = Field(
    ..., description="Still-open lines whose suggestion changed as a result."
  )
  dry_run: bool


class LedgerAgentResponse(BaseModel):
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
  classification: AgentClassification | None = Field(
    None, description="The default account its bank lines are suggested."
  )

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
  agent: LedgerAgentResponse
  recent_events: list
  recent_transactions: list[TransactionSummary]
  event_count: int
  transaction_count: int
