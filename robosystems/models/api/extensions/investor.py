"""RoboInvestor API request/response models."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field

from robosystems.models.api.common import PaginationInfo

# ── Portfolio ──────────────────────────────────────────────────────────────


class CreatePortfolioRequest(BaseModel):
  name: str = Field(..., min_length=1, max_length=200)
  description: str | None = None
  strategy: str | None = None
  inception_date: date | None = None
  base_currency: str = "USD"


class UpdatePortfolioRequest(BaseModel):
  name: str | None = None
  description: str | None = None
  strategy: str | None = None
  inception_date: date | None = None
  base_currency: str | None = None


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
  source_graph_id: str | None = None  # POC: auto-link entity from another graph
  name: str = Field(..., min_length=1, max_length=200)
  security_type: str  # common_stock, preferred_stock, warrant, convertible_note, etc.
  security_subtype: str | None = None
  terms: dict = Field(default_factory=dict)
  authorized_shares: int | None = None
  outstanding_shares: int | None = None


class UpdateSecurityRequest(BaseModel):
  name: str | None = None
  security_type: str | None = None
  security_subtype: str | None = None
  terms: dict | None = None
  is_active: bool | None = None
  authorized_shares: int | None = None
  outstanding_shares: int | None = None


class SecurityResponse(BaseModel):
  id: str
  entity_id: str | None = None
  entity_name: str | None = None
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


class CreatePositionRequest(BaseModel):
  portfolio_id: str
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


class UpdatePositionRequest(BaseModel):
  quantity: float | None = None
  quantity_type: str | None = None
  cost_basis: int | None = None
  current_value: int | None = None
  valuation_date: date | None = None
  valuation_source: str | None = None
  acquisition_date: date | None = None
  disposition_date: date | None = None
  status: str | None = None
  notes: str | None = None


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
