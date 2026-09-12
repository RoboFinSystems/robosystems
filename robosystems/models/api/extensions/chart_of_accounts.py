"""Chart-of-accounts initialization models.

A graph with no chart of accounts — a company that never synced
QuickBooks — initializes one from a shipped template. The operation is
one-time (409 once a chart exists) and creates the chart, its
``coa_mapping`` structure and the template's CoA → rs-gaap mapping
associations atomically. Customization afterwards is
``update-taxonomy-block``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ChartTemplateKey = Literal["saas", "services", "product"]


class InitializeChartOfAccountsRequest(BaseModel):
  """Create the graph's chart of accounts from a shipped template.

  Refused (409) when the graph already has an active ``chart_of_accounts``
  taxonomy — a QuickBooks-synced tenant never needs this, and a chart is
  never replaced. The template's equity rows are mapped by the entity's
  legal form (``entity_type``: corporation / llc / partnership); omit it
  to use the graph's primary entity, falling back to corporation.
  """

  template: ChartTemplateKey = Field(
    ...,
    description=(
      "Template key: `saas` (subscription software — deferred revenue, "
      "cost of revenue, R&D / S&M / G&A), `services` (professional "
      "services — no inventory, no COGS), `product` (inventory and cost "
      "of goods sold, direct + wholesale + subscription revenue)."
    ),
  )
  entity_type: str | None = Field(
    None,
    description=(
      "Legal form for the equity mapping: `corporation`, `llc` or "
      "`partnership`. Defaults to the graph's primary entity, then to "
      "corporation."
    ),
  )
  name: str | None = Field(
    None,
    max_length=200,
    description="Chart display name. Defaults to 'Chart of Accounts'.",
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"template": "saas"},
        {"template": "services", "entity_type": "llc", "name": "Cascade Books"},
      ]
    }
  )


class ChartTemplateSummary(BaseModel):
  """One shipped chart template, for pickers."""

  key: ChartTemplateKey
  display_name: str
  description: str
  account_count: int = Field(..., description="Rows the template creates.")


class InitializeChartOfAccountsResponse(BaseModel):
  taxonomy_id: str = Field(..., description="The new chart's taxonomy id.")
  name: str
  template: ChartTemplateKey
  entity_type: str = Field(
    ..., description="Legal form the equity rows were mapped for."
  )
  elements_created: int
  mappings_created: int
  frameworks: list[str] = Field(
    default_factory=list,
    description=(
      "Frameworks the chart was mapped into — each template mapping set "
      "whose framework this graph's library carries (rs-gaap today; every "
      "framework in the graph's pin once it is plural)."
    ),
  )
  unresolved: list[str] = Field(
    default_factory=list,
    description=(
      "rs-gaap qnames the template maps to that the library did not "
      "resolve; the accounts exist and can be mapped by hand. Non-fatal."
    ),
  )
