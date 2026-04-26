"""RoboInvestor API request/response models."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

from robosystems.models.api.common import PaginationInfo

# ── Portfolio ──────────────────────────────────────────────────────────────


class PortfolioResponse(BaseModel):
  id: str
  name: str
  description: str | None = None
  strategy: str | None = None
  inception_date: date | None = None
  base_currency: str
  created_at: datetime
  updated_at: datetime


class PortfolioListResponse(BaseModel):
  portfolios: list[PortfolioResponse]
  pagination: PaginationInfo


# ── Security ───────────────────────────────────────────────────────────────


class CreateSecurityRequest(BaseModel):
  entity_id: str | None = None
  source_graph_id: str | None = None  # Pre-associate to a company graph
  name: str = Field(..., min_length=1, max_length=200)
  security_type: str  # common_stock, preferred_stock, warrant, convertible_note, etc.
  security_subtype: str | None = None
  terms: dict = Field(default_factory=dict)
  authorized_shares: int | None = None
  outstanding_shares: int | None = None


class UpdateSecurityRequest(BaseModel):
  entity_id: str | None = None
  source_graph_id: str | None = None
  name: str | None = None
  security_type: str | None = None
  security_subtype: str | None = None
  terms: dict | None = None
  is_active: bool | None = None
  authorized_shares: int | None = None
  outstanding_shares: int | None = None


class UpdateSecurityOperation(UpdateSecurityRequest):
  """CQRS body for `POST /operations/update-security`."""

  security_id: str = Field(..., description="Target security ID.")


class DeleteSecurityOperation(BaseModel):
  """CQRS body for `POST /operations/delete-security` (soft delete)."""

  security_id: str = Field(..., description="Target security ID.")


class SecurityResponse(BaseModel):
  id: str
  entity_id: str | None = None
  entity_name: str | None = None
  source_graph_id: str | None = None
  name: str
  security_type: str
  security_subtype: str | None = None
  terms: dict
  is_active: bool
  authorized_shares: int | None = None
  outstanding_shares: int | None = None
  created_at: datetime
  updated_at: datetime


class SecurityListResponse(BaseModel):
  securities: list[SecurityResponse]
  pagination: PaginationInfo


# ── Position ───────────────────────────────────────────────────────────────


class DeleteResult(BaseModel):
  """Shared response shape for soft-delete operations (e.g.,
  `delete-security`). `deleted=true` means a row was flipped; 404 is
  raised by the handler when no row existed."""

  deleted: bool


class PositionResponse(BaseModel):
  id: str
  portfolio_id: str
  security_id: str
  security_name: str | None = None
  entity_name: str | None = None
  quantity: float
  quantity_type: str
  cost_basis: int
  cost_basis_dollars: float
  currency: str
  current_value: int | None = None
  current_value_dollars: float | None = None
  valuation_date: date | None = None
  valuation_source: str | None = None
  acquisition_date: date | None = None
  disposition_date: date | None = None
  status: str
  notes: str | None = None
  created_at: datetime
  updated_at: datetime


class PositionListResponse(BaseModel):
  positions: list[PositionResponse]
  pagination: PaginationInfo


# ── Holdings (aggregated view) ─────────────────────────────────────────────


class HoldingSecuritySummary(BaseModel):
  security_id: str
  security_name: str
  security_type: str
  quantity: float
  quantity_type: str
  cost_basis_dollars: float
  current_value_dollars: float | None = None


class HoldingResponse(BaseModel):
  entity_id: str
  entity_name: str
  source_graph_id: str | None = None
  securities: list[HoldingSecuritySummary]
  total_cost_basis_dollars: float
  total_current_value_dollars: float | None = None
  position_count: int


class HoldingsListResponse(BaseModel):
  holdings: list[HoldingResponse]
  total_entities: int
  total_positions: int


# ── Portfolio Block (molecule envelope) ────────────────────────────────────


class EntityLite(BaseModel):
  id: str
  name: str
  source_graph_id: str | None = None


class SecurityLite(BaseModel):
  id: str
  name: str
  security_type: str
  security_subtype: str | None = None
  is_active: bool
  issuer: EntityLite | None = None
  source_graph_id: str | None = None


class PositionBlock(BaseModel):
  id: str
  quantity: float
  quantity_type: str
  cost_basis_dollars: float
  current_value_dollars: float | None = None
  valuation_date: date | None = None
  valuation_source: str | None = None
  acquisition_date: date | None = None
  status: str
  notes: str | None = None
  security: SecurityLite


class PortfolioBlockEnvelope(BaseModel):
  id: str
  name: str
  description: str | None = None
  strategy: str | None = None
  inception_date: date | None = None
  base_currency: str
  owner: EntityLite | None = None
  positions: list[PositionBlock]
  total_cost_basis_dollars: float
  total_current_value_dollars: float | None = None
  active_position_count: int
  created_at: datetime
  updated_at: datetime


# ── Portfolio Block writes (molecule write surface) ───────────────────────


class PortfolioBlockPositionAdd(BaseModel):
  """A single new position to mint inside a portfolio-block create/update.

  References an existing security; this surface never creates securities
  (Master Data CRUD owns that lifecycle).
  """

  security_id: str
  quantity: float
  quantity_type: str = "shares"
  cost_basis: int = 0  # cents
  currency: str = "USD"
  current_value: int | None = None  # cents
  valuation_date: date | None = None
  valuation_source: str | None = None
  acquisition_date: date | None = None
  notes: str | None = None


class PortfolioBlockPositionUpdate(BaseModel):
  """Patch-by-id for an existing position in `update-portfolio-block`.

  Unset fields are ignored; `id` is the only required field.
  """

  id: str
  quantity: float | None = None
  quantity_type: str | None = None
  cost_basis: int | None = None
  current_value: int | None = None
  valuation_date: date | None = None
  valuation_source: str | None = None
  acquisition_date: date | None = None
  notes: str | None = None


class PortfolioBlockPositionDispose(BaseModel):
  """Dispose-by-id for an existing position in `update-portfolio-block`.

  Soft-delete: status flips to `disposed` and `disposition_date` is
  stamped. `disposition_reason`, when supplied, is recorded under
  `metadata.disposition_reason`.
  """

  id: str
  disposition_reason: str | None = None


class PortfolioBlockPositions(BaseModel):
  """Position deltas applied atomically inside `update-portfolio-block`."""

  add: list[PortfolioBlockPositionAdd] = Field(default_factory=list)
  update: list[PortfolioBlockPositionUpdate] = Field(default_factory=list)
  dispose: list[PortfolioBlockPositionDispose] = Field(default_factory=list)


class PortfolioBlockPortfolioFields(BaseModel):
  """Fields settable on the portfolio core when creating a block."""

  name: str = Field(..., min_length=1, max_length=200)
  description: str | None = None
  strategy: str | None = None
  inception_date: date | None = None
  base_currency: str = "USD"
  entity_id: str | None = None


class PortfolioBlockPortfolioPatch(BaseModel):
  """Patchable portfolio fields on `update-portfolio-block`. Unset fields ignored."""

  name: str | None = None
  description: str | None = None
  strategy: str | None = None
  inception_date: date | None = None
  base_currency: str | None = None
  entity_id: str | None = None


class CreatePortfolioBlockRequest(BaseModel):
  """CQRS body for `POST /operations/create-portfolio-block`.

  Whole envelope validated before any DB write; the portfolio + initial
  positions land in one transaction. Each `position` references an
  existing `security_id` — the operation does not mint securities.
  """

  portfolio: PortfolioBlockPortfolioFields
  positions: list[PortfolioBlockPositionAdd] = Field(default_factory=list)


class UpdatePortfolioBlockOperation(BaseModel):
  """CQRS body for `POST /operations/update-portfolio-block`.

  Carries an optional patch to portfolio fields and three position
  delta lists (`add` / `update` / `dispose`). All apply atomically
  with the portfolio patch — partial failures roll back.
  """

  portfolio_id: str = Field(..., description="Target portfolio ID.")
  portfolio: PortfolioBlockPortfolioPatch = Field(
    default_factory=PortfolioBlockPortfolioPatch
  )
  positions: PortfolioBlockPositions = Field(default_factory=PortfolioBlockPositions)


class DeletePortfolioBlockOperation(BaseModel):
  """CQRS body for `POST /operations/delete-portfolio-block`.

  Cascade-deletes the portfolio plus all of its positions. When the
  portfolio still has active positions, the operation is rejected
  unless `confirm_active_positions=true` is set — safety belt to
  prevent accidental cascade.
  """

  portfolio_id: str = Field(..., description="Target portfolio ID.")
  confirm_active_positions: bool = False


class DeletePortfolioBlockResponse(BaseModel):
  """Result envelope for `delete-portfolio-block`. `positions_deleted`
  carries the count of position rows removed alongside the portfolio."""

  deleted: bool
  portfolio_id: str
  positions_deleted: int
