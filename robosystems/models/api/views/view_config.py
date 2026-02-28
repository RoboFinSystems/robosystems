from pydantic import BaseModel, Field, field_validator


class ViewAxisConfig(BaseModel):
  type: str = Field(
    ..., description="Axis type: 'element', 'period', 'dimension', 'entity'"
  )

  dimension_axis: str | None = Field(
    default=None, description="Dimension axis name for dimension-type axes"
  )
  include_null_dimension: bool = Field(
    default=False,
    description="Include facts where this dimension is NULL (default: false)",
  )

  selected_members: list[str] | None = Field(
    default=None,
    description="Specific members to include (e.g., ['2024-12-31', '2023-12-31'])",
  )
  member_order: list[str] | None = Field(
    default=None, description="Explicit ordering of members (overrides default sort)"
  )
  member_labels: dict[str, str] | None = Field(
    default=None,
    description="Custom labels for members (e.g., {'2024-12-31': 'Current Year'})",
  )

  element_order: list[str] | None = Field(
    default=None,
    description="Element ordering for hierarchy display (e.g., ['us-gaap:Assets', 'us-gaap:Cash', ...])",
  )
  element_labels: dict[str, str] | None = Field(
    default=None,
    description="Custom labels for elements (e.g., {'us-gaap:Cash': 'Cash and Cash Equivalents'})",
  )

  @field_validator("type")
  @classmethod
  def validate_axis_type(cls, v: str) -> str:
    allowed = ["element", "period", "dimension", "entity"]
    if v not in allowed:
      raise ValueError(f"Axis type must be one of {allowed}, got: {v}")
    return v


class ViewConfig(BaseModel):
  rows: list[ViewAxisConfig] = Field(
    default_factory=list, description="Row axis configuration"
  )
  columns: list[ViewAxisConfig] = Field(
    default_factory=list, description="Column axis configuration"
  )
  values: str = Field(
    default="numeric_value",
    description="Field to use for values (default: numeric_value)",
  )
  aggregation_function: str = Field(
    default="sum", description="Aggregation function: sum, average, count"
  )
  fill_value: float = Field(default=0.0, description="Value to use for missing data")


class CreateViewRequest(BaseModel):
  elements: list[str] = Field(
    default_factory=list,
    description="Element qnames (e.g., 'us-gaap:Assets'). Can combine with canonical_concepts.",
  )
  canonical_concepts: list[str] = Field(
    default_factory=list,
    description="Canonical concept names (e.g., 'revenue', 'net_income'). Matches all mapped qnames.",
  )
  periods: list[str] = Field(
    default_factory=list,
    description="Period end dates (YYYY-MM-DD format)",
  )
  entity: str | None = Field(
    None,
    description="Filter by entity ticker, CIK, or name",
  )
  entities: list[str] = Field(
    default_factory=list,
    description="Filter by multiple entity tickers (e.g., ['NVDA', 'AAPL'])",
  )
  form: str | None = Field(
    None,
    description="Filter by SEC filing form type (e.g., '10-K', '10-Q')",
  )
  fiscal_year: int | None = Field(
    None,
    description="Filter by fiscal year (e.g., 2024)",
  )
  fiscal_period: str | None = Field(
    None,
    description="Filter by fiscal period (e.g., 'FY', 'Q1', 'Q2', 'Q3')",
  )
  period_type: str | None = Field(
    None,
    description="Filter by period type: 'annual', 'quarterly', or 'instant'",
  )
  include_summary: bool = Field(
    default=False,
    description="Include summary statistics per element",
  )
  view_config: ViewConfig = Field(
    default_factory=ViewConfig, description="View/pivot configuration"
  )

  @field_validator("period_type")
  @classmethod
  def validate_period_type(cls, v: str | None) -> str | None:
    if v is not None and v not in ("annual", "quarterly", "instant"):
      raise ValueError(
        f"period_type must be 'annual', 'quarterly', or 'instant', got: {v}"
      )
    return v
