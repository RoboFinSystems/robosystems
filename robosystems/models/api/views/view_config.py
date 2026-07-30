from pydantic import BaseModel, ConfigDict, Field, field_validator


class ViewAxisConfig(BaseModel):
  """Scoping configuration for one aspect of the fact query.

  Filtering only. Ordering and labelling are presentation concerns and
  belong to the consumer that arranges the facts into a table.
  """

  type: str = Field(..., description="Axis type: 'element', 'period', 'entity'")

  include_null_dimension: bool = Field(
    default=False,
    description="Include facts where this aspect is absent (default: false)",
  )

  selected_members: list[str] | None = Field(
    default=None,
    description="Specific members to include (e.g., ['2024-12-31', '2023-12-31'])",
  )

  @field_validator("type")
  @classmethod
  def validate_axis_type(cls, v: str) -> str:
    allowed = ["element", "period", "entity"]
    if v not in allowed:
      # 'dimension' is deliberately absent: the fact query filters on
      # `has_dimensions = false`, so dimensional facts never reach the
      # response and a dimension axis could only ever be a silent no-op.
      raise ValueError(f"Axis type must be one of {allowed}, got: {v}")
    return v


class ViewConfig(BaseModel):
  rows: list[ViewAxisConfig] = Field(
    default_factory=list, description="Row axis configuration"
  )
  columns: list[ViewAxisConfig] = Field(
    default_factory=list, description="Column axis configuration"
  )


class CreateViewRequest(BaseModel):
  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "elements": ["us-gaap:Assets"],
          "entities": ["NVDA"],
          "form": "10-K",
          "period_type": "annual",
        },
        {
          "canonical_concepts": ["revenue"],
          "entities": ["NVDA", "AMD"],
          "form": "10-K",
          "fiscal_period": "FY",
          "view_config": {
            "columns": [
              {
                "type": "period",
                "selected_members": [
                  "2022-12-31",
                  "2023-12-31",
                  "2024-12-31",
                ],
              }
            ],
          },
        },
        {
          "elements": [
            "us-gaap:Assets",
            "us-gaap:Liabilities",
            "us-gaap:StockholdersEquity",
          ],
          "periods": ["2026-03-31"],
          "period_type": "instant",
          "include_summary": True,
        },
      ]
    }
  )

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
    default_factory=ViewConfig, description="Aspect scoping configuration"
  )

  @field_validator("period_type")
  @classmethod
  def validate_period_type(cls, v: str | None) -> str | None:
    if v is not None and v not in ("annual", "quarterly", "instant"):
      raise ValueError(
        f"period_type must be 'annual', 'quarterly', or 'instant', got: {v}"
      )
    return v
