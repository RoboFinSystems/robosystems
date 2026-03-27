"""Ledger entity API models."""

from pydantic import BaseModel


class LedgerEntityResponse(BaseModel):
  """Entity details from the roboledger OLTP database."""

  id: str
  name: str
  legal_name: str | None = None
  uri: str | None = None

  # Identifiers
  cik: str | None = None
  ticker: str | None = None
  exchange: str | None = None
  sic: str | None = None
  sic_description: str | None = None
  category: str | None = None
  state_of_incorporation: str | None = None
  fiscal_year_end: str | None = None
  tax_id: str | None = None
  lei: str | None = None

  # Business info
  industry: str | None = None
  entity_type: str | None = None
  phone: str | None = None
  website: str | None = None
  status: str = "active"

  # Hierarchy
  is_parent: bool = True
  parent_entity_id: str | None = None

  # Source provenance
  source: str = "native"
  source_id: str | None = None
  connection_id: str | None = None

  # Address
  address_line1: str | None = None
  address_city: str | None = None
  address_state: str | None = None
  address_postal_code: str | None = None
  address_country: str | None = None

  created_at: str | None = None
  updated_at: str | None = None


class UpdateEntityRequest(BaseModel):
  """Request to update entity details. Only provided fields are updated."""

  name: str | None = None
  legal_name: str | None = None
  uri: str | None = None
  cik: str | None = None
  ticker: str | None = None
  exchange: str | None = None
  sic: str | None = None
  sic_description: str | None = None
  category: str | None = None
  state_of_incorporation: str | None = None
  fiscal_year_end: str | None = None
  tax_id: str | None = None
  lei: str | None = None
  industry: str | None = None
  entity_type: str | None = None
  phone: str | None = None
  website: str | None = None
  address_line1: str | None = None
  address_city: str | None = None
  address_state: str | None = None
  address_postal_code: str | None = None
  address_country: str | None = None
