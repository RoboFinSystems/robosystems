"""
Shared Repository Registry.

Single source of truth for all shared repository definitions. Manifests are
declared by adapters and collected here on first access.

This module owns all billing/rate-limit accessor functions for shared
repositories. Plans, pricing, features, and endpoint access are declared
per-repo in adapter manifests. Plans are plain strings — the manifest is the
single source of truth.

Usage:
    from robosystems.config.shared_repositories import (
        is_shared_repository,
        get_manifest,
        get_all_repository_ids,
        get_plan_details,
        get_rate_limits,
    )

    if is_shared_repository("sec"):
        manifest = get_manifest("sec")
        print(manifest.name)  # "SEC EDGAR Filings"

Adding a new shared repository:
    1. Create adapters/{name}/manifest.py with a SharedRepositoryManifest
    2. Add one import + _register() call to _load_manifests() below
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from robosystems.adapters.base import SharedRepositoryManifest

_manifests: dict[str, Any] = {}
_loaded = False


# ---------------------------------------------------------------------------
# Registry internals
# ---------------------------------------------------------------------------


def _register(manifest: Any) -> None:
  """Register a manifest in the registry."""
  if manifest.id in _manifests:
    raise ValueError(f"Duplicate shared repository manifest: {manifest.id}")
  _manifests[manifest.id] = manifest


def _load_manifests() -> None:
  """Load all adapter manifests. Called once on first access."""
  global _loaded
  if _loaded:
    return

  from robosystems.adapters.sec.manifest import SEC_MANIFEST

  _register(SEC_MANIFEST)

  # Future adapters:
  # from robosystems.adapters.industry.manifest import INDUSTRY_MANIFEST
  # _register(INDUSTRY_MANIFEST)

  _loaded = True


def _ensure_loaded() -> None:
  if not _loaded:
    _load_manifests()


# ---------------------------------------------------------------------------
# Core registry queries
# ---------------------------------------------------------------------------


def is_shared_repository(repo_id: str | None) -> bool:
  """Check if a repository ID refers to a registered shared repository."""
  if repo_id is None:
    return False
  _ensure_loaded()
  return repo_id in _manifests


def is_shared_repository_or_subgraph(graph_id: str | None) -> bool:
  """Check if a graph ID is a shared repository OR a subgraph of one.

  Examples:
      is_shared_repository_or_subgraph("sec") -> True
      is_shared_repository_or_subgraph("sec_historical") -> True
      is_shared_repository_or_subgraph("kg123") -> False
  """
  if graph_id is None:
    return False
  if is_shared_repository(graph_id):
    return True
  # Check if it's a subgraph of a shared repo (e.g., "sec_historical")
  if "_" in graph_id:
    parent = graph_id.split("_", 1)[0]
    return is_shared_repository(parent)
  return False


def resolve_shared_repository_parent(graph_id: str) -> str:
  """Resolve a graph ID to its shared repository parent.

  If graph_id is already a parent repo, returns it unchanged.
  If it's a subgraph (e.g., "sec_historical"), returns the parent ("sec").

  Args:
      graph_id: Graph identifier that is a shared repo or subgraph of one.

  Returns:
      The parent shared repository ID.

  Raises:
      ValueError: If graph_id is not a shared repository or subgraph of one.
  """
  if is_shared_repository(graph_id):
    return graph_id
  if "_" in graph_id:
    parent = graph_id.split("_", 1)[0]
    if is_shared_repository(parent):
      return parent
  raise ValueError(f"Not a shared repository or subgraph: {graph_id}")


def get_manifest(repo_id: str) -> SharedRepositoryManifest | None:
  """Get the manifest for a shared repository, or None if not found."""
  _ensure_loaded()
  return _manifests.get(repo_id)


def get_all_repository_ids() -> list[str]:
  """Get all registered shared repository IDs."""
  _ensure_loaded()
  return list(_manifests.keys())


def get_available_repositories() -> list[SharedRepositoryManifest]:
  """Get manifests for all repositories with status 'available'."""
  _ensure_loaded()
  return [m for m in _manifests.values() if m.status == "available"]


def get_all_manifests() -> dict[str, SharedRepositoryManifest]:
  """Get all registered manifests keyed by ID."""
  _ensure_loaded()
  return dict(_manifests)


# ---------------------------------------------------------------------------
# Billing / plan accessor functions (replaces RepositoryBillingConfig)
# ---------------------------------------------------------------------------


def get_plan_details(plan: str, repo_id: str | None = None) -> dict | None:
  """Get details for a repository plan.

  Args:
      plan: The plan key to look up (e.g. "starter", "advanced").
      repo_id: Optional repository ID. If None, searches all repos for the plan.

  Returns:
      Plan details dict or None if not found.
  """
  _ensure_loaded()

  if not isinstance(plan, str) or not plan:
    return None

  if repo_id is not None:
    manifest = _manifests.get(repo_id)
    if not manifest or not manifest.plans:
      return None
    return manifest.plans.get(plan)

  # No repo_id: search all manifests for the plan
  for manifest in _manifests.values():
    if manifest.plans and plan in manifest.plans:
      return manifest.plans[plan]
  return None


def get_rate_limits(repo_id: str, plan: str) -> dict[str, int] | None:
  """Get rate limits for a repository and plan combination."""
  manifest = get_manifest(repo_id)
  if not manifest or not manifest.rate_limits:
    return None
  return manifest.rate_limits.get(plan)


def get_credit_costs(repo_id: str) -> dict | None:
  """Get per-operation credit costs for a shared repository.

  Returns:
      Dict mapping operation types to Decimal costs (or None for dynamic pricing),
      or None if not configured.
  """
  manifest = get_manifest(repo_id)
  if not manifest:
    return None
  return manifest.credit_costs


def get_all_repository_configs() -> dict:
  """Get all repository configurations including enabled status and plans.

  Returns:
      Dict mapping repository IDs to their config including enabled status and plans.
  """
  _ensure_loaded()
  configs = {}

  for repo_id, manifest in _manifests.items():
    plans_dict: dict[str, dict[str, Any]] = {}
    if manifest.plans:
      for plan_key, plan_details in manifest.plans.items():
        plans_dict[plan_key] = {
          **plan_details,
          "access_level": plan_details.get("access_level", "READ"),
        }

    configs[repo_id] = {
      "enabled": manifest.status == "available",
      "coming_soon": manifest.status == "coming_soon",
      "plans": plans_dict,
    }

  return configs


def is_repository_enabled(repo_id: str) -> bool:
  """Check if a repository is enabled for subscriptions."""
  manifest = get_manifest(repo_id)
  return manifest.status == "available" if manifest else False


def is_endpoint_allowed(endpoint: str, repo_id: str | None = None) -> bool:
  """Check if an endpoint is allowed for shared repositories.

  If repo_id is provided and has explicit endpoint lists, uses those.
  Otherwise falls back to a default allow/block check across all repos.
  """
  _ensure_loaded()
  endpoint_lower = endpoint.lower()

  if repo_id is not None:
    manifest = _manifests.get(repo_id)
    if manifest:
      blocked = manifest.blocked_endpoints
      allowed = manifest.allowed_endpoints
      if blocked or allowed:
        if blocked:
          for b in blocked:
            if b in endpoint_lower:
              return False
        if allowed:
          return any(a in endpoint_lower for a in allowed)
        return True

  # Fallback: check all manifests for any that have endpoint lists
  for manifest in _manifests.values():
    if manifest.blocked_endpoints:
      for b in manifest.blocked_endpoints:
        if b in endpoint_lower:
          return False
    if manifest.allowed_endpoints:
      return any(a in endpoint_lower for a in manifest.allowed_endpoints)

  return True


def get_all_repository_pricing() -> dict:
  """Get complete pricing information for all repository plans."""
  from robosystems.config import env

  _ensure_loaded()

  # Build plans dict keyed by plan string from manifests
  plans: dict[str, dict] = {}
  for manifest in _manifests.values():
    if manifest.plans:
      for plan_key, plan_details in manifest.plans.items():
        if plan_key not in plans:
          plans[plan_key] = plan_details

  repositories = {}
  for manifest in _manifests.values():
    repositories[manifest.id] = {
      "name": manifest.name,
      "description": manifest.description,
      "status": manifest.status,
    }

  return {
    "plans": plans,
    "repositories": repositories,
    "billing_model": "No credit consumption for queries, rate-limited by subscription tier",
    "upgrade_url": f"{env.ROBOSYSTEMS_URL}/repositories/browse",
  }


def get_repository_metadata(repo_id: str) -> dict | None:
  """Get metadata for a shared repository from its manifest."""
  manifest = get_manifest(repo_id)
  if not manifest:
    return None
  return {
    "name": manifest.name,
    "description": manifest.description,
    "data_source_type": manifest.data_source_type,
    "data_source_url": manifest.data_source_url,
    "sync_frequency": manifest.sync_frequency,
    "status": manifest.status,
    "graph_tier": manifest.graph_tier,
    "graph_instance_id": manifest.graph_instance_id,
  }
