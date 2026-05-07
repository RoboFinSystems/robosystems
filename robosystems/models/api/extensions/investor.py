"""RoboInvestor API request/response models."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

from robosystems.models.api.common import (  # noqa: F401  DeleteResult re-exported
  DeleteResult,
  PaginationInfo,
)

# ── Portfolio ──────────────────────────────────────────────────────────────


class PortfolioResponse(BaseModel):
  """Read projection for a single portfolio — core fields only.

  Position-level holdings live on the dedicated portfolio-block envelope
  (`PortfolioBlockEnvelope`) returned by molecular operations.
  """

  id: str = Field(..., description="Portfolio ID (`port_*` ULID).")
  name: str = Field(..., description="Display name.")
  description: str | None = Field(None, description="Free-text description.")
  strategy: str | None = Field(
    None,
    description=(
      "Free-text strategy classification (e.g. `value`, `growth`, "
      "`income`). Open vocabulary."
    ),
  )
  inception_date: date | None = Field(
    None,
    description="Date the portfolio was established (YYYY-MM-DD).",
  )
  base_currency: str = Field(
    ...,
    description="ISO 4217 currency code used for portfolio-level aggregates.",
  )
  created_at: datetime = Field(..., description="Row creation timestamp (UTC).")
  updated_at: datetime = Field(..., description="Last-modified timestamp (UTC).")


class PortfolioListResponse(BaseModel):
  """Paginated list of portfolios."""

  portfolios: list[PortfolioResponse] = Field(
    ..., description="Portfolios on this page."
  )
  pagination: PaginationInfo = Field(..., description="Pagination cursor and totals.")


# ── Security ───────────────────────────────────────────────────────────────


class CreateSecurityRequest(BaseModel):
  """CQRS body for `POST /operations/create-security`.

  Mints a new security as Master Data — referenced by positions, never
  created inline by portfolio operations. The `terms` blob carries
  instrument-specific shape (liquidation preference, strike price,
  vesting) used by future waterfall-distribution modeling.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Common stock class A",
          "description": (
            "Vanilla equity security tied to an issuer entity. "
            "`terms` is empty — common stock has no instrument "
            "specifics beyond the share count."
          ),
          "value": {
            "entity_id": "ent_acme_holdings",
            "name": "Common Stock Class A",
            "security_type": "common_stock",
            "security_subtype": "class_a",
            "authorized_shares": 10_000_000,
            "outstanding_shares": 6_500_000,
          },
        },
        {
          "summary": "Series A convertible note",
          "description": (
            "Convertible debt with a discount and valuation cap. "
            "Conversion mechanics live in `terms`; the surface stays "
            "schemaless to support future instrument types without "
            "schema migrations."
          ),
          "value": {
            "entity_id": "ent_seed_co",
            "name": "Series A Convertible Note",
            "security_type": "convertible_note",
            "security_subtype": "series_a",
            "terms": {
              "principal_cents": 50_000_00,
              "interest_rate_bps": 800,
              "discount_pct": 20,
              "valuation_cap_cents": 10_000_000_00,
              "maturity_date": "2028-06-30",
            },
          },
        },
        {
          "summary": "Warrant pre-associated to a company graph",
          "description": (
            "Warrant linked to a tenant graph (`source_graph_id`) "
            "before the issuer entity is finalized. The strike/expiry "
            "live in `terms`."
          ),
          "value": {
            "source_graph_id": "kg_acme_pre_seed",
            "name": "Founder Warrant — 2026",
            "security_type": "warrant",
            "terms": {
              "strike_price_cents": 25,
              "shares_underlying": 100_000,
              "expiration_date": "2031-05-01",
              "vesting": "cliff_1y_then_monthly_36",
            },
          },
        },
      ]
    }
  )

  entity_id: str | None = Field(
    None,
    description=(
      "ID of the issuing entity. Optional only if `source_graph_id` "
      "is set (pre-association before the entity is finalized)."
    ),
  )
  source_graph_id: str | None = Field(
    None,
    description=(
      "Optional pre-association to a tenant company graph. Lets you "
      "mint securities for an entity that hasn't been promoted to a "
      "real `entity_id` yet."
    ),
  )
  name: str = Field(
    ...,
    min_length=1,
    max_length=200,
    description=(
      "Display name for the security (e.g. `Common Stock Class A`, "
      "`Series A Preferred`). 1-200 characters."
    ),
  )
  security_type: str = Field(
    ...,
    description=(
      "Instrument family. Open vocabulary — common values: "
      "`common_stock`, `preferred_stock`, `warrant`, "
      "`convertible_note`, `safe`, `option`, `llc_unit`, "
      "`lp_interest`, `restricted_stock_unit`."
    ),
  )
  security_subtype: str | None = Field(
    None,
    description=(
      "Free-text refinement of `security_type` (e.g. `class_a`, "
      "`series_a`, `series_seed`). No vocabulary enforcement."
    ),
  )
  terms: dict = Field(
    default_factory=dict,
    description=(
      "Instrument-specific terms blob (JSONB). Shape depends on "
      "`security_type` — common keys include `liquidation_preference`, "
      "`strike_price_cents`, `discount_pct`, `valuation_cap_cents`, "
      "`maturity_date`, `vesting`. Used by future waterfall-"
      "distribution modeling; treat as authoritative storage for "
      "instrument mechanics."
    ),
  )
  authorized_shares: int | None = Field(
    None,
    description=(
      "Total shares the issuer is authorized to issue for this class. "
      "`null` for instruments where shares aren't a meaningful unit "
      "(e.g. convertible notes pre-conversion)."
    ),
  )
  outstanding_shares: int | None = Field(
    None,
    description=(
      "Shares currently issued and outstanding. Should be "
      "≤ `authorized_shares` when both are set."
    ),
  )


class UpdateSecurityRequest(BaseModel):
  """Patchable security fields. Unset fields are left unchanged."""

  entity_id: str | None = Field(
    None,
    description="Reassign to a different issuing entity. Unset = unchanged.",
  )
  source_graph_id: str | None = Field(
    None,
    description="Update the pre-association tenant graph. Unset = unchanged.",
  )
  name: str | None = Field(
    None,
    description="New display name. Unset = unchanged.",
  )
  security_type: str | None = Field(
    None,
    description=(
      "New instrument family. Unset = unchanged. Reclassifying a "
      "security may invalidate existing `terms` shape; the operation "
      "does not validate the cross-field consistency."
    ),
  )
  security_subtype: str | None = Field(
    None,
    description="New subtype refinement. Unset = unchanged.",
  )
  terms: dict | None = Field(
    None,
    description=(
      "Replacement terms blob. Pass `null`/omit to leave existing "
      "terms unchanged; pass `{}` to clear them."
    ),
  )
  is_active: bool | None = Field(
    None,
    description=(
      "Active flag. Set `false` to soft-deactivate (positions remain "
      "addressable but the security is hidden from active lookups)."
    ),
  )
  authorized_shares: int | None = Field(
    None,
    description="New authorized share count. Unset = unchanged.",
  )
  outstanding_shares: int | None = Field(
    None,
    description="New outstanding share count. Unset = unchanged.",
  )


class UpdateSecurityOperation(UpdateSecurityRequest):
  """CQRS body for `POST /operations/update-security`."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Refresh outstanding shares after issuance round",
          "value": {
            "security_id": "sec_acme_common_a",
            "outstanding_shares": 7_250_000,
          },
        },
        {
          "summary": "Deactivate a retired class",
          "description": (
            "Soft-deactivate a security after a class consolidation. "
            "Existing positions still resolve; new active-only lookups "
            "skip it."
          ),
          "value": {
            "security_id": "sec_legacy_pref_seed",
            "is_active": False,
          },
        },
      ]
    }
  )

  security_id: str = Field(..., description="Target security ID.")


class DeleteSecurityOperation(BaseModel):
  """CQRS body for `POST /operations/delete-security` (soft delete)."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Soft-delete a security",
          "value": {"security_id": "sec_misclassified_warrant"},
        }
      ]
    }
  )

  security_id: str = Field(..., description="Target security ID.")


class SecurityResponse(BaseModel):
  """Read projection for a single security."""

  id: str = Field(..., description="Security ID (`sec_*` ULID).")
  entity_id: str | None = Field(None, description="ID of the issuing entity, when set.")
  entity_name: str | None = Field(
    None,
    description=(
      "Cached display name of the issuing entity, denormalized for "
      "list rendering. May lag the entity row's current name briefly."
    ),
  )
  source_graph_id: str | None = Field(
    None,
    description=(
      "Tenant graph this security is pre-associated to, when the "
      "issuer entity hasn't been promoted yet."
    ),
  )
  name: str = Field(..., description="Display name of the security.")
  security_type: str = Field(
    ...,
    description=(
      "Instrument family (e.g. `common_stock`, `preferred_stock`, "
      "`warrant`, `convertible_note`)."
    ),
  )
  security_subtype: str | None = Field(
    None,
    description="Optional subtype refinement (e.g. `class_a`, `series_a`).",
  )
  terms: dict = Field(
    ...,
    description=(
      "Instrument-specific terms blob. Shape depends on "
      "`security_type` — see `CreateSecurityRequest.terms` for "
      "common keys."
    ),
  )
  is_active: bool = Field(
    ...,
    description=(
      "`true` when the security is in active status; `false` after a "
      "soft-delete or deactivation."
    ),
  )
  authorized_shares: int | None = Field(
    None, description="Total shares authorized for this class, when set."
  )
  outstanding_shares: int | None = Field(
    None, description="Shares currently issued and outstanding."
  )
  created_at: datetime = Field(..., description="Row creation timestamp (UTC).")
  updated_at: datetime = Field(..., description="Last-modified timestamp (UTC).")


class SecurityListResponse(BaseModel):
  """Paginated list of securities."""

  securities: list[SecurityResponse] = Field(
    ..., description="Securities on this page."
  )
  pagination: PaginationInfo = Field(..., description="Pagination cursor and totals.")


# ── Position ───────────────────────────────────────────────────────────────


class PositionResponse(BaseModel):
  """Read projection for a single position.

  Pairs cents-precision fields (`cost_basis`, `current_value`) with
  pre-computed dollar floats (`*_dollars`) to spare clients the
  conversion. The cents fields are authoritative.
  """

  id: str = Field(..., description="Position ID (`pos_*` ULID).")
  portfolio_id: str = Field(..., description="Owning portfolio ID.")
  security_id: str = Field(..., description="Held security ID.")
  security_name: str | None = Field(
    None,
    description=(
      "Cached display name of the held security, denormalized for "
      "list rendering. May lag the security row's current name briefly."
    ),
  )
  entity_name: str | None = Field(
    None,
    description="Cached display name of the security's issuing entity.",
  )
  quantity: float = Field(
    ..., description="Quantity held in units defined by `quantity_type`."
  )
  quantity_type: str = Field(
    ...,
    description="Unit basis (`shares`, `units`, `principal`).",
  )
  cost_basis: int = Field(
    ...,
    description="Cost basis in **cents** of `currency`. Authoritative.",
  )
  cost_basis_dollars: float = Field(
    ...,
    description=(
      "Cost basis pre-converted to dollars (`cost_basis / 100`). "
      "Convenience for display; `cost_basis` is the source of truth."
    ),
  )
  currency: str = Field(
    ...,
    description="ISO 4217 currency code for `cost_basis` and `current_value`.",
  )
  current_value: int | None = Field(
    None,
    description=("Latest mark-to-market value in **cents**, or `null` if unmarked."),
  )
  current_value_dollars: float | None = Field(
    None,
    description=(
      "Current value in dollars (`current_value / 100`). `null` when "
      "`current_value` is null."
    ),
  )
  valuation_date: date | None = Field(
    None, description="Date `current_value` was sourced (YYYY-MM-DD)."
  )
  valuation_source: str | None = Field(
    None,
    description="Free-text source attribution for the current valuation.",
  )
  acquisition_date: date | None = Field(
    None, description="Date the position was acquired (YYYY-MM-DD)."
  )
  disposition_date: date | None = Field(
    None,
    description=(
      "Date the position was disposed, if `status='disposed'`. `null` "
      "for active positions."
    ),
  )
  status: str = Field(
    ...,
    description=(
      "Lifecycle state. One of: `active` (currently held), `disposed` "
      "(soft-deleted via `update-portfolio-block` dispose), `archived` "
      "(historical record only)."
    ),
  )
  notes: str | None = Field(
    None, description="Free-text notes attached to the position."
  )
  created_at: datetime = Field(..., description="Row creation timestamp (UTC).")
  updated_at: datetime = Field(..., description="Last-modified timestamp (UTC).")


class PositionListResponse(BaseModel):
  """Paginated list of positions."""

  positions: list[PositionResponse] = Field(..., description="Positions on this page.")
  pagination: PaginationInfo = Field(..., description="Pagination cursor and totals.")


# ── Holdings (aggregated view) ─────────────────────────────────────────────


class HoldingSecuritySummary(BaseModel):
  """One security held by an entity, rolled up across portfolios."""

  security_id: str = Field(..., description="Security ID.")
  security_name: str = Field(..., description="Display name of the security.")
  security_type: str = Field(
    ..., description="Instrument family (e.g. `common_stock`, `warrant`)."
  )
  quantity: float = Field(
    ..., description="Total quantity held in `quantity_type` units."
  )
  quantity_type: str = Field(
    ..., description="Unit basis (`shares`, `units`, `principal`)."
  )
  cost_basis_dollars: float = Field(
    ...,
    description="Aggregate cost basis in dollars, summed across all positions.",
  )
  current_value_dollars: float | None = Field(
    None,
    description=(
      "Aggregate current value in dollars, or `null` if any underlying "
      "position lacks a mark."
    ),
  )


class HoldingResponse(BaseModel):
  """All securities held in a single entity, rolled up across the
  caller's portfolios."""

  entity_id: str = Field(..., description="Issuing entity ID.")
  entity_name: str = Field(..., description="Display name of the entity.")
  source_graph_id: str | None = Field(
    None,
    description="Pre-association tenant graph, when set on the securities.",
  )
  securities: list[HoldingSecuritySummary] = Field(
    ..., description="One row per security held in this entity."
  )
  total_cost_basis_dollars: float = Field(
    ..., description="Sum of cost basis across all securities, in dollars."
  )
  total_current_value_dollars: float | None = Field(
    None,
    description=(
      "Sum of current value across all securities, in dollars. `null` "
      "if any security lacks a mark."
    ),
  )
  position_count: int = Field(
    ..., description="Number of distinct active positions backing these holdings."
  )


class HoldingsListResponse(BaseModel):
  """Aggregated holdings across all of the caller's portfolios."""

  holdings: list[HoldingResponse] = Field(
    ..., description="One row per issuing entity."
  )
  total_entities: int = Field(..., description="Count of entities represented.")
  total_positions: int = Field(
    ..., description="Total active positions backing these holdings."
  )


# ── Portfolio Block (molecule envelope) ────────────────────────────────────


class EntityLite(BaseModel):
  """Lightweight entity projection for embedding in portfolio-block /
  position envelopes. Carries identity-only fields; full entity data
  lives behind the Master Data entity APIs."""

  id: str = Field(..., description="Entity ID (`ent_*` ULID).")
  name: str = Field(..., description="Display name of the entity.")
  source_graph_id: str | None = Field(
    None,
    description=(
      "Tenant graph this entity is anchored to, when known. `null` "
      "for entities not yet linked to a graph."
    ),
  )


class SecurityLite(BaseModel):
  """Lightweight security projection for embedding in position
  envelopes. Skips `terms`, `outstanding_shares`, etc. — fetch the
  full `SecurityResponse` when those are needed."""

  id: str = Field(..., description="Security ID (`sec_*` ULID).")
  name: str = Field(..., description="Display name of the security.")
  security_type: str = Field(
    ...,
    description=(
      "Instrument family (e.g. `common_stock`, `preferred_stock`, `warrant`)."
    ),
  )
  security_subtype: str | None = Field(
    None, description="Optional subtype refinement (e.g. `class_a`)."
  )
  is_active: bool = Field(
    ..., description="`true` when the security is in active status."
  )
  issuer: EntityLite | None = Field(
    None,
    description=(
      "Embedded issuer entity, when one is linked. `null` for pre-issuer securities."
    ),
  )
  source_graph_id: str | None = Field(
    None, description="Tenant graph the security is pre-associated to, if any."
  )


class PositionBlock(BaseModel):
  """Position projection embedded inside a `PortfolioBlockEnvelope`.

  Pre-converts cents fields to dollars (`cost_basis_dollars`,
  `current_value_dollars`) for display; the cents-precision fields
  live on the standalone `PositionResponse`. Embeds a `SecurityLite`
  so callers can render the security name without a follow-up fetch.
  """

  id: str = Field(..., description="Position ID (`pos_*` ULID).")
  quantity: float = Field(..., description="Quantity held in `quantity_type` units.")
  quantity_type: str = Field(
    ..., description="Unit basis (`shares`, `units`, `principal`)."
  )
  cost_basis_dollars: float = Field(
    ..., description="Cost basis in dollars (pre-converted from cents)."
  )
  current_value_dollars: float | None = Field(
    None,
    description=(
      "Latest mark-to-market value in dollars. `null` when the "
      "position has not been marked."
    ),
  )
  valuation_date: date | None = Field(
    None, description="Date the current value was sourced."
  )
  valuation_source: str | None = Field(
    None, description="Free-text source attribution for the valuation."
  )
  acquisition_date: date | None = Field(
    None, description="Date the position was acquired."
  )
  status: str = Field(
    ...,
    description=(
      "Lifecycle state (`active`, `disposed`, `archived`). See "
      "`PositionResponse.status` for the full vocabulary."
    ),
  )
  notes: str | None = Field(
    None, description="Free-text notes attached to the position."
  )
  security: SecurityLite = Field(
    ..., description="Embedded security details — name, type, issuer."
  )


class PortfolioBlockEnvelope(BaseModel):
  """Molecular response shape for portfolio-block operations.

  Bundles the portfolio core, its embedded positions, and pre-computed
  totals into a single payload — the contract for `create-portfolio-block`,
  `update-portfolio-block`, and the read-side `get-portfolio-block`.
  Cents-precision values aren't surfaced here; use `PositionResponse`
  / `PortfolioResponse` for those.
  """

  id: str = Field(..., description="Portfolio ID (`port_*` ULID).")
  name: str = Field(..., description="Display name.")
  description: str | None = Field(None, description="Free-text description.")
  strategy: str | None = Field(None, description="Free-text strategy classification.")
  inception_date: date | None = Field(
    None, description="Date the portfolio was established."
  )
  base_currency: str = Field(
    ..., description="ISO 4217 currency code for portfolio aggregates."
  )
  owner: EntityLite | None = Field(
    None,
    description=(
      "Embedded owning entity, when set. `null` for unattributed portfolios."
    ),
  )
  positions: list[PositionBlock] = Field(
    ...,
    description=(
      "All positions in this portfolio, including disposed ones "
      "(filter by `status` for active-only display)."
    ),
  )
  total_cost_basis_dollars: float = Field(
    ...,
    description="Sum of `cost_basis_dollars` across every position.",
  )
  total_current_value_dollars: float | None = Field(
    None,
    description=(
      "Sum of `current_value_dollars` across every position. `null` "
      "when any active position lacks a mark."
    ),
  )
  active_position_count: int = Field(
    ..., description="Count of positions with `status='active'`."
  )
  created_at: datetime = Field(..., description="Row creation timestamp (UTC).")
  updated_at: datetime = Field(..., description="Last-modified timestamp (UTC).")


# ── Portfolio Block writes (molecule write surface) ───────────────────────


class PortfolioBlockPositionAdd(BaseModel):
  """A single new position to mint inside a portfolio-block create/update.

  References an existing security; this surface never creates securities
  (Master Data CRUD owns that lifecycle).
  """

  security_id: str = Field(
    ...,
    description=(
      "ID of the existing security this position holds. Securities are "
      "minted via `create-security`; the operation returns 404 if the "
      "ID is unknown."
    ),
  )
  quantity: float = Field(
    ...,
    description=(
      "Quantity held, in units defined by `quantity_type` (e.g. share "
      "count for `shares`, face value for `principal`)."
    ),
  )
  quantity_type: str = Field(
    "shares",
    description=(
      "Unit basis for `quantity`. Common values: `shares` (equity "
      "units), `units` (generic), `principal` (debt face value)."
    ),
  )
  cost_basis: int = Field(
    0,
    description=(
      "Total cost basis for this lot, in **cents** of `currency`. "
      "Stored as integer cents to avoid float precision drift; "
      "$1,250.00 USD is `125000`."
    ),
  )
  currency: str = Field(
    "USD",
    description="ISO 4217 currency code for `cost_basis` and `current_value`.",
  )
  current_value: int | None = Field(
    None,
    description=(
      "Latest mark-to-market value in **cents** of `currency`, or "
      "`null` if unmarked. Pair with `valuation_date` and "
      "`valuation_source` when set."
    ),
  )
  valuation_date: date | None = Field(
    None,
    description="Date `current_value` was sourced (YYYY-MM-DD).",
  )
  valuation_source: str | None = Field(
    None,
    description=(
      "Free-text source attribution for `current_value` (e.g. "
      "`manual`, `broker_statement`, vendor name)."
    ),
  )
  acquisition_date: date | None = Field(
    None,
    description="Date the position was originally acquired (YYYY-MM-DD).",
  )
  notes: str | None = Field(
    None,
    description="Free-text notes attached to the position.",
  )


class PortfolioBlockPositionUpdate(BaseModel):
  """Patch-by-id for an existing position in `update-portfolio-block`.

  Unset fields are ignored; `id` is the only required field.
  """

  id: str = Field(
    ...,
    description=(
      "Target position ID. Must belong to the portfolio identified by "
      "`portfolio_id` on the parent operation."
    ),
  )
  quantity: float | None = Field(
    None,
    description="New quantity in units of `quantity_type`. Unset = unchanged.",
  )
  quantity_type: str | None = Field(
    None,
    description=(
      "New unit basis (`shares` | `units` | `principal`). Unset = unchanged."
    ),
  )
  cost_basis: int | None = Field(
    None,
    description=(
      "New cost basis in **cents** of the position's currency. Unset = unchanged."
    ),
  )
  current_value: int | None = Field(
    None,
    description=(
      "New mark-to-market value in **cents**. Unset = unchanged. Set "
      "alongside `valuation_date` / `valuation_source` to record a "
      "fresh valuation event."
    ),
  )
  valuation_date: date | None = Field(
    None,
    description="New valuation date (YYYY-MM-DD). Unset = unchanged.",
  )
  valuation_source: str | None = Field(
    None,
    description="New valuation source attribution. Unset = unchanged.",
  )
  acquisition_date: date | None = Field(
    None,
    description="New acquisition date (YYYY-MM-DD). Unset = unchanged.",
  )
  notes: str | None = Field(
    None,
    description="New notes. Unset = unchanged.",
  )


class PortfolioBlockPositionDispose(BaseModel):
  """Dispose-by-id for an existing position in `update-portfolio-block`.

  Soft-delete: status flips to `disposed` and `disposition_date` is
  stamped. `disposition_reason`, when supplied, is recorded under
  `metadata.disposition_reason`.
  """

  id: str = Field(
    ...,
    description=(
      "Target position ID to dispose. Must belong to the portfolio "
      "identified by `portfolio_id` on the parent operation."
    ),
  )
  disposition_reason: str | None = Field(
    None,
    description=(
      "Optional free-text reason recorded under "
      "`metadata.disposition_reason` on the disposed position."
    ),
  )


class PortfolioBlockPositions(BaseModel):
  """Position deltas applied atomically inside `update-portfolio-block`."""

  add: list[PortfolioBlockPositionAdd] = Field(
    default_factory=list,
    description=(
      "New positions to mint inside this portfolio. Each references an "
      "existing `security_id`."
    ),
  )
  update: list[PortfolioBlockPositionUpdate] = Field(
    default_factory=list,
    description=(
      "Patches to existing positions, addressed by position `id`. "
      "Unset fields on each entry are left unchanged."
    ),
  )
  dispose: list[PortfolioBlockPositionDispose] = Field(
    default_factory=list,
    description=(
      "Positions to soft-dispose, addressed by position `id`. Status "
      "flips to `disposed` and `disposition_date` is stamped."
    ),
  )


class PortfolioBlockPortfolioFields(BaseModel):
  """Fields settable on the portfolio core when creating a block."""

  name: str = Field(
    ...,
    min_length=1,
    max_length=200,
    description="Display name for the portfolio. 1-200 characters.",
  )
  description: str | None = Field(
    None,
    description="Free-text description of the portfolio.",
  )
  strategy: str | None = Field(
    None,
    description=(
      "Free-text strategy classification (e.g. `value`, `growth`, "
      "`income`). Open vocabulary."
    ),
  )
  inception_date: date | None = Field(
    None,
    description="Date the portfolio was established (YYYY-MM-DD).",
  )
  base_currency: str = Field(
    "USD",
    description=(
      "ISO 4217 currency code used for portfolio-level aggregates "
      "(e.g. `total_cost_basis_dollars`)."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "ID of the owning entity (e.g. fund, trust, or person). "
      "Optional — leave unset for unattributed portfolios."
    ),
  )


class PortfolioBlockPortfolioPatch(BaseModel):
  """Patchable portfolio fields on `update-portfolio-block`. Unset fields ignored."""

  name: str | None = Field(
    None,
    description="New display name. Unset = unchanged.",
  )
  description: str | None = Field(
    None,
    description="New description. Unset = unchanged.",
  )
  strategy: str | None = Field(
    None,
    description="New strategy classification. Unset = unchanged.",
  )
  inception_date: date | None = Field(
    None,
    description="New inception date (YYYY-MM-DD). Unset = unchanged.",
  )
  base_currency: str | None = Field(
    None,
    description=(
      "New ISO 4217 base currency code. Unset = unchanged. Note: "
      "changing base currency does not retroactively reprice "
      "historical positions."
    ),
  )
  entity_id: str | None = Field(
    None,
    description="New owning-entity ID. Unset = unchanged.",
  )


class CreatePortfolioBlockRequest(BaseModel):
  """CQRS body for `POST /operations/create-portfolio-block`.

  Whole envelope validated before any DB write; the portfolio + initial
  positions land in one transaction. Each `position` references an
  existing `security_id` — the operation does not mint securities.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Portfolio with initial positions",
          "description": (
            "Create a growth-strategy portfolio for an entity with two "
            "marked positions in existing securities."
          ),
          "value": {
            "portfolio": {
              "name": "Q1 2026 Growth",
              "description": "Mid-cap public equities",
              "strategy": "growth",
              "inception_date": "2026-01-01",
              "base_currency": "USD",
              "entity_id": "ent_acme_holdings",
            },
            "positions": [
              {
                "security_id": "sec_aapl",
                "quantity": 100,
                "quantity_type": "shares",
                "cost_basis": 1850000,
                "currency": "USD",
                "current_value": 2010000,
                "valuation_date": "2026-04-30",
                "valuation_source": "broker_statement",
                "acquisition_date": "2026-01-15",
              },
              {
                "security_id": "sec_msft",
                "quantity": 50,
                "quantity_type": "shares",
                "cost_basis": 2125000,
                "currency": "USD",
                "current_value": 2240000,
                "valuation_date": "2026-04-30",
                "valuation_source": "broker_statement",
                "acquisition_date": "2026-02-08",
              },
            ],
          },
        },
        {
          "summary": "Empty portfolio",
          "description": (
            "Create a portfolio with no initial positions; populate "
            "later via `update-portfolio-block`."
          ),
          "value": {
            "portfolio": {
              "name": "Watchlist",
              "base_currency": "USD",
            },
            "positions": [],
          },
        },
      ]
    }
  )

  portfolio: PortfolioBlockPortfolioFields = Field(
    ...,
    description="Core portfolio fields — name, currency, owning entity, etc.",
  )
  positions: list[PortfolioBlockPositionAdd] = Field(
    default_factory=list,
    description=(
      "Initial positions to mint inside the new portfolio. Each "
      "references an existing security; pass `[]` to create an empty "
      "portfolio."
    ),
  )


class UpdatePortfolioBlockOperation(BaseModel):
  """CQRS body for `POST /operations/update-portfolio-block`.

  Carries an optional patch to portfolio fields and three position
  delta lists (`add` / `update` / `dispose`). All apply atomically
  with the portfolio patch — partial failures roll back.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Refresh marks and dispose a position",
          "description": (
            "Update the strategy note, mark two positions to today's "
            "close, and dispose a third — all atomically."
          ),
          "value": {
            "portfolio_id": "port_q1_growth_2026",
            "portfolio": {"description": "Pivoted toward defensive holdings"},
            "positions": {
              "add": [],
              "update": [
                {
                  "id": "pos_aapl_lot_1",
                  "current_value": 1980000,
                  "valuation_date": "2026-05-06",
                  "valuation_source": "broker_statement",
                },
                {
                  "id": "pos_msft_lot_1",
                  "current_value": 2310000,
                  "valuation_date": "2026-05-06",
                  "valuation_source": "broker_statement",
                },
              ],
              "dispose": [
                {
                  "id": "pos_oldcorp_lot_3",
                  "disposition_reason": "Liquidated; rotated capital",
                }
              ],
            },
          },
        },
        {
          "summary": "Add a new lot only",
          "description": (
            "Mint a fresh position into an existing portfolio; leave "
            "the portfolio core and other positions untouched."
          ),
          "value": {
            "portfolio_id": "port_q1_growth_2026",
            "positions": {
              "add": [
                {
                  "security_id": "sec_googl",
                  "quantity": 25,
                  "cost_basis": 4250000,
                  "acquisition_date": "2026-05-01",
                }
              ]
            },
          },
        },
      ]
    }
  )

  portfolio_id: str = Field(..., description="Target portfolio ID.")
  portfolio: PortfolioBlockPortfolioPatch = Field(
    default_factory=PortfolioBlockPortfolioPatch,
    description=(
      "Patch to portfolio core fields. Omit or leave fields unset to "
      "leave the portfolio core unchanged."
    ),
  )
  positions: PortfolioBlockPositions = Field(
    default_factory=PortfolioBlockPositions,
    description=(
      "Position deltas — additions, in-place updates, and "
      "dispositions — applied atomically with the portfolio patch."
    ),
  )


class DeletePortfolioBlockOperation(BaseModel):
  """CQRS body for `POST /operations/delete-portfolio-block`.

  Cascade-deletes the portfolio plus all of its positions. When the
  portfolio still has active positions, the operation is rejected
  unless `confirm_active_positions=true` is set — safety belt to
  prevent accidental cascade.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Delete an empty portfolio",
          "description": (
            "Default request shape — no acknowledgement needed when "
            "the portfolio has no active positions."
          ),
          "value": {"portfolio_id": "port_watchlist_2026"},
        },
        {
          "summary": "Delete a portfolio with active positions",
          "description": (
            "Cascade-delete a populated portfolio. Without "
            "`confirm_active_positions: true` the operation returns 409."
          ),
          "value": {
            "portfolio_id": "port_legacy_2024",
            "confirm_active_positions": True,
          },
        },
      ]
    }
  )

  portfolio_id: str = Field(..., description="Target portfolio ID.")
  confirm_active_positions: bool = Field(
    False,
    description=(
      "Required acknowledgement when the portfolio has active "
      "positions. Set `true` to consent to cascade-deleting them; "
      "leave `false` and the operation returns 409 if any active "
      "positions exist."
    ),
  )


class DeletePortfolioBlockResponse(BaseModel):
  """Result envelope for `delete-portfolio-block`. `positions_deleted`
  carries the count of position rows removed alongside the portfolio."""

  deleted: bool = Field(
    ...,
    description="`true` when the portfolio row was removed.",
  )
  portfolio_id: str = Field(
    ...,
    description="ID of the portfolio that was deleted.",
  )
  positions_deleted: int = Field(
    ...,
    description="Count of position rows cascade-deleted with the portfolio.",
  )
