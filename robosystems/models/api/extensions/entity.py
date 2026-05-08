"""Ledger entity API models.

The Entity is the legal/operating subject the ledger reports for —
the company whose books we're keeping. One graph typically maps to
one primary entity (e.g. a single LLC, corp, or LP); multi-entity
consolidation uses parent/child entity hierarchy via
``parent_entity_id``.
"""

from pydantic import BaseModel, ConfigDict, Field


class LedgerEntityResponse(BaseModel):
  """Entity details from the extensions OLTP database.

  Returned by entity reads and `update-entity`. Identifiers
  (CIK / ticker / SIC / LEI / tax_id) are present when sourced from
  SEC or registry data; many are null for private companies. The
  address fields are flattened (no nested object) to make them easy
  to project into reporting forms.
  """

  id: str = Field(..., description="Entity identifier (ULID).")
  name: str = Field(..., description="Display name shown in UI.")
  legal_name: str | None = Field(
    None, description="Full registered legal name (when different from `name`)."
  )
  uri: str | None = Field(None, description="Canonical URL / external identifier.")

  # Identifiers
  cik: str | None = Field(None, description="SEC CIK (Central Index Key).")
  ticker: str | None = Field(None, description="Stock ticker symbol.")
  exchange: str | None = Field(
    None, description="Listing exchange (e.g. 'NASDAQ', 'NYSE')."
  )
  sic: str | None = Field(None, description="SIC industry code.")
  sic_description: str | None = Field(None, description="SIC code description.")
  category: str | None = Field(
    None, description="Filer category (e.g. 'Large Accelerated Filer')."
  )
  state_of_incorporation: str | None = Field(
    None, description="US state or country of incorporation."
  )
  fiscal_year_end: str | None = Field(
    None, description="Fiscal year-end as MM-DD (e.g. '12-31', '06-30')."
  )
  tax_id: str | None = Field(None, description="Tax ID (EIN / SSN).")
  lei: str | None = Field(None, description="Legal Entity Identifier (ISO 17442).")

  # Business info
  industry: str | None = Field(None, description="Free-form industry label.")
  entity_type: str | None = Field(
    None, description="Legal form (e.g. 'corporation', 'llc', 'lp')."
  )
  phone: str | None = None
  website: str | None = None
  status: str = Field(
    "active", description="Operational status: 'active' | 'inactive' | 'dissolved'."
  )

  # Hierarchy
  is_parent: bool = Field(
    True,
    description="True for top-level entities; False for subsidiaries.",
  )
  parent_entity_id: str | None = Field(
    None, description="Parent entity ID for subsidiaries; null for top-level."
  )

  # Source provenance
  source: str = Field(
    "native",
    description="Provenance: 'native' | 'sec' | 'quickbooks' | 'xero' | 'plaid'.",
  )
  source_id: str | None = Field(
    None, description="Source-system primary key for sync reconciliation."
  )
  source_graph_id: str | None = Field(
    None,
    description=(
      "Origin graph for received entities (cross-graph linking, e.g. "
      "RoboInvestor portfolio holdings)."
    ),
  )
  connection_id: str | None = Field(
    None, description="Source connection that ingested this row."
  )

  # Address
  address_line1: str | None = None
  address_city: str | None = None
  address_state: str | None = None
  address_postal_code: str | None = None
  address_country: str | None = None

  created_at: str | None = None
  updated_at: str | None = None


class UpdateEntityRequest(BaseModel):
  """Update the graph's primary entity. All fields are optional —
  pass only what changes. Identifiers (CIK, LEI, tax_id) are typically
  set once at onboarding; address fields are flattened to make them
  easy to project into reporting forms (1099, state filings).

  The graph is implicit (URL path) — there's no `entity_id` field
  because the operation always targets the graph's primary entity.
  """

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
  fiscal_year_end: str | None = Field(
    None, description="Fiscal year-end as MM-DD (e.g. '12-31', '06-30')."
  )
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

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "name": "Acme Corp",
          "legal_name": "Acme Corporation, Inc.",
          "tax_id": "12-3456789",
          "state_of_incorporation": "DE",
          "fiscal_year_end": "12-31",
          "entity_type": "corporation",
        },
        {
          "address_line1": "100 Main St",
          "address_city": "Seattle",
          "address_state": "WA",
          "address_postal_code": "98101",
          "address_country": "US",
        },
        {"name": "Acme Holdings, LLC"},
      ]
    }
  )
