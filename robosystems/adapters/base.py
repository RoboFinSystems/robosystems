"""Shared repository manifest type.

Imports nothing from the codebase: config/shared_repositories.py imports the
adapter manifests, which import this module.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SharedRepositoryManifest:
  """Everything about one shared repository; collected by the registry in
  config/shared_repositories.py."""

  # Identity (id is also graph_id)
  id: str  # "sec"
  name: str  # "SEC EDGAR Filings"
  description: str  # Human-readable

  # Data Source
  data_source_type: str  # "sec_edgar"
  data_source_url: str | None = None
  sync_frequency: str = "daily"

  # Schema (used by SharedRepositoryService)
  schema_type: str = "shared"
  schema_extensions: tuple[str, ...] = ()  # ("roboledger",)

  # MCP Capabilities
  has_semantic_enrichment: bool = False

  # MCP server `instructions`; None means generate from the live tool surface.
  agent_instructions: str | None = None

  # Appended to `read-graph-cypher`'s description: the rules a raw query must
  # follow to return a correct number. The tool description is the one text
  # every MCP client reads.
  cypher_query_guidance: str | None = None

  # Large text columns, as "Label.property", that this repository does not
  # serve through Cypher string matching (CONTAINS, STARTS WITH, ENDS WITH,
  # =~). `guarded_string_guidance` tells the caller where that question goes.
  guarded_string_properties: tuple[str, ...] = ()
  guarded_string_guidance: str | None = None

  # Status
  status: str = "available"  # available, coming_soon

  # Infrastructure
  graph_tier: str = "ladybug-shared"
  graph_instance_id: str = "ladybug-shared-prod"

  # Keyed by plan name; None = default limits.
  rate_limits: dict[str, dict[str, int]] | None = None

  # Keyed by plan name. Each: name, price_cents, price_monthly, price_display,
  # monthly_credits, access_level, description, features.
  plans: dict[str, dict[str, Any]] | None = None

  # Endpoint access control (defaults apply if None)
  allowed_endpoints: tuple[str, ...] | None = None
  blocked_endpoints: tuple[str, ...] | None = None

  # Decimal per operation, or None for dynamic pricing; unlisted → Decimal("1.0").
  credit_costs: dict[str, Any] | None = None
