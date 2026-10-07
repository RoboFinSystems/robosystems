"""Ledger entity API models.

The Entity is the legal/operating subject the ledger reports for —
the company whose books we're keeping. A graph is one reporting
group: the group parent and the subsidiaries under it
(`parent_entity_id`), each keeping its own books, chart of accounts
and close. An operation that names no `entity_id` acts on the group
parent.
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
  reporting_style_id: str | None = Field(
    None,
    description=(
      "Active Reporting Style (Structure id) governing this entity's "
      "statement layout. Change it via the change-reporting-style operation."
    ),
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
  ownership_pct: float | None = Field(
    None,
    description=(
      "The parent's share of this entity, as a percent (100 = wholly "
      "owned). Null on the group parent, and where it was never recorded."
    ),
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
  """Update an entity of the graph's reporting group. All fields are
  optional — pass only what changes. Identifiers (CIK, LEI, tax_id) are
  typically set once at onboarding; address fields are flattened to make
  them easy to project into reporting forms (1099, state filings).

  Omit `entity_id` to target the group parent.
  """

  entity_id: str | None = Field(
    None,
    description=(
      "The entity to update. Omit to target the group parent — the "
      "single-entity default."
    ),
  )
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
  ownership_pct: float | None = Field(
    None,
    gt=0,
    le=100,
    description=(
      "The parent's share of this entity, as a percent. Refused on the "
      "group parent, which has no owner in the graph."
    ),
  )

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
        {"entity_id": "ent_01J9ZK3M4N5P6Q7R8S9T0V1W2X", "ownership_pct": 80},
      ]
    }
  )


class CreateEntityRequest(BaseModel):
  """Add an entity to the graph's reporting group.

  The new entity is a subsidiary of `parent_entity_id`, default the group
  parent, and keeps its own books: give it a chart next
  (`initialize-chart-of-accounts` with `entity_id`) and a calendar
  (`initialize`), then name it with `entity_id` on any ledger operation.
  A graph created without an entity gets this one as its group parent.
  There is no cap on entities in a graph; capacity is the tier's.
  """

  name: str = Field(..., min_length=1, max_length=255, description="Display name.")
  legal_name: str | None = Field(
    None, description="Registered legal name. Defaults to `name`."
  )
  entity_type: str | None = Field(
    None,
    description=(
      "Legal form: `corporation`, `llc`, `partnership`, "
      "`sole_proprietorship`, `non_profit`. Picks the default Reporting "
      "Style (partnership and llc have equity-form Styles of their own; "
      "anything else is corporate) and the equity rows of a chart template."
    ),
  )
  reporting_style_id: str | None = Field(
    None,
    description=(
      "Structure id of the Reporting Style to present under, validated in "
      "the graph like change-reporting-style. Omit to derive it from "
      "`entity_type`."
    ),
  )
  parent_entity_id: str | None = Field(
    None,
    description=(
      "The entity this one is held under. Omit for the group parent; name "
      "a subsidiary to nest a sub-group under it."
    ),
  )
  ownership_pct: float | None = Field(
    None,
    gt=0,
    le=100,
    description=(
      "The parent's share of this entity, as a percent (100 = wholly "
      "owned). Omit when not recorded. Refused on a graph's first entity, "
      "which becomes the group parent."
    ),
  )
  ticker: str | None = Field(
    None,
    min_length=1,
    max_length=10,
    description=(
      "Short symbol, unique in the graph; it prefixes the entity's account "
      "names (`coa-<ticker>:1000`). Derived from the name's initials when "
      "omitted."
    ),
  )
  uri: str | None = Field(None, description="Canonical URL / external identifier.")
  cik: str | None = None
  sic: str | None = None
  sic_description: str | None = None
  category: str | None = None
  state_of_incorporation: str | None = None
  fiscal_year_end: str | None = Field(
    None,
    description=(
      "Fiscal year-end as MM-DD. Defaults to the parent's: a graph has one "
      "fiscal cadence, and every entity's calendar follows it."
    ),
  )
  tax_id: str | None = None
  lei: str | None = None
  industry: str | None = None
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
        {"name": "Maple Court LLC", "entity_type": "llc", "ownership_pct": 100},
        {
          "name": "Harbor Property Management LLC",
          "legal_name": "Harbor Property Management, LLC",
          "entity_type": "llc",
          "ticker": "HPM",
          "parent_entity_id": "ent_01J9ZK3M4N5P6Q7R8S9T0V1W2X",
          "ownership_pct": 60,
          "state_of_incorporation": "DE",
          "tax_id": "12-3456789",
        },
      ]
    }
  )


class ChangeReportingStyleRequest(BaseModel):
  """Switch a reporting entity's Reporting Style.

  The Reporting Style governs how the entity's statements are laid out
  (equity-form, close-target concept, per-statement Networks). It's
  validated against the tenant schema — the target must be a renderable
  Style with a complete composition — before the switch is applied.
  """

  reporting_style_id: str = Field(
    ...,
    min_length=1,
    description=(
      "Structure id of the target Reporting Style. Must exist in the "
      "tenant schema with a complete Network composition."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "Target entity. Omit to target the graph's primary "
      "(earliest-created) entity — the single-entity default."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"reporting_style_id": "10d05f23-8ea8-5348-b8c9-f1e65bbda4a3"},
      ]
    }
  )


class ChangeReportingStyleResponse(BaseModel):
  """Result of a change-reporting-style operation."""

  entity_id: str = Field(..., description="Entity whose Style was targeted.")
  previous_reporting_style_id: str | None = Field(
    None, description="Style id before the change (null for legacy/unset)."
  )
  reporting_style_id: str = Field(..., description="Active Style id after the call.")
  reporting_style_code: str | None = Field(
    None,
    description="4-segment Style code (e.g. BSC-CORP-IS02-CF1), when stamped.",
  )
  changed: bool = Field(
    ..., description="False when the target equals the current Style (no-op)."
  )
