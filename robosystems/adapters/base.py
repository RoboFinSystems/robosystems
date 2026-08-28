"""
Base adapter types for shared repository manifests.

A SharedRepositoryManifest is the single source of truth for everything about
a shared repository: identity, data source, schema, capabilities, rate limits,
and infrastructure metadata.

This module has no imports from the rest of the codebase to avoid circular
dependencies. The import chain is:
    config/shared_repositories.py -> adapters/{name}/manifest.py -> adapters/base.py
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SharedRepositoryManifest:
  """Complete definition of a shared repository.

  Adapters declare one of these to register a shared repository with the platform.
  The registry in config/shared_repositories.py collects all manifests and provides
  the query API used by billing, middleware, and operations.
  """

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

  # Agent-facing routing guidance handed to MCP clients via the server's
  # `instructions` handshake field. Authored per-repo (shared repos have a
  # fixed, curated tool set, so the text is stable). When None, the MCP layer
  # falls back to generating instructions from the graph's live tool surface.
  agent_instructions: str | None = None

  # Query guidance appended to `read-graph-cypher`'s tool description for this
  # repository: the data-model rules a raw Cypher query must honour to return
  # a correct number (consolidation, period shape, deduplication). The tool
  # description is the one place every MCP client reads — `agent_instructions`
  # is the routing layer, and client-side skills reach only our own Claude.
  cypher_query_guidance: str | None = None

  # Status
  status: str = "available"  # available, coming_soon

  # Infrastructure
  graph_tier: str = "ladybug-shared"
  graph_instance_id: str = "ladybug-shared-prod"

  # Rate limits per plan (keys are plan name strings, e.g. "starter", "advanced")
  # None = use default limits
  rate_limits: dict[str, dict[str, int]] | None = None

  # Billing: plans keyed by plan name (e.g. "starter", "advanced")
  # Each plan dict: name, price_cents, price_monthly, price_display,
  #   monthly_credits, access_level, description, features
  plans: dict[str, dict[str, Any]] | None = None

  # Endpoint access control (defaults apply if None)
  allowed_endpoints: tuple[str, ...] | None = None
  blocked_endpoints: tuple[str, ...] | None = None

  # Per-operation credit costs (Decimal values or None for dynamic pricing)
  # Operations not listed default to Decimal("1.0") in the credit service.
  credit_costs: dict[str, Any] | None = None
