"""Resolve a request's graph and its tier for rate limiting.

The limiter runs on the API's hottest path, so the `graphs` lookup a tier
resolution needs is cached with a short TTL. Every failure mode falls back to
the *tightest* tier rather than the loosest: a cache outage or a deleted graph
must not hand out XLarge throughput.
"""

from ...config.graph_tier import GraphTier
from ...config.rate_limits import RateLimitConfig
from ...config.valkey_registry import ValkeyDatabase, create_redis_client
from ...logger import logger

# Tightest customer tier. Used whenever the real tier cannot be established,
# so degradation is always toward less throughput, never more.
FALLBACK_TIER = GraphTier.LADYBUG_STANDARD.value

# Short enough that a tier change takes effect promptly, long enough that the
# lookup does not run on every request in a burst.
_TIER_CACHE_TTL_SECONDS = 300

_CACHE_PREFIX = "ratelimit:graph_tier:"

# Sentinel stored for graph ids that resolve to nothing, so a stream of requests
# for a non-existent graph does not re-query the database each time.
_UNKNOWN = "-"

# Static path segments that occupy the graph-id position under /v1/graphs.
# Treating one as a graph id would collapse every caller of that route into a
# single shared bucket — the cross-tenant failure the shared-repository
# exclusion exists to prevent. Those routes use the general limiter today, so
# this guard is defense in depth against one of them adopting the
# subscription-aware dependency.
_STATIC_GRAPH_SEGMENTS = frozenset({"schema", "tiers", "capacity", "extensions"})

# One client for the process, created lazily. The limiter runs per request on
# the hottest path in the API; creating a client (and its connection pool) per
# call would mean a TCP/TLS handshake per request in production.
_redis_client = None


def _redis():
  global _redis_client
  if _redis_client is None:
    _redis_client = create_redis_client(ValkeyDatabase.GRAPH_ROUTING)
  return _redis_client


def extract_graph_id(path: str) -> str | None:
  """Pull the graph id out of a request path, or None if it is not graph-scoped.

  Three shapes carry a graph id:
      /v1/graphs/{graph_id}/...
      /extensions/{graph_id}/graphql
      /extensions/{domain}/{graph_id}/operations/...

  The middle form is distinguished from the last by its second segment: a
  domain is a known product name, anything else is a graph id.
  """
  parts = [segment for segment in path.split("/") if segment]

  if len(parts) >= 3 and parts[0] == "v1" and parts[1] == "graphs":
    candidate = parts[2]
    return None if candidate in _STATIC_GRAPH_SEGMENTS else candidate

  if len(parts) >= 2 and parts[0] == "extensions":
    if parts[1] in ("roboledger", "roboinvestor"):
      return parts[2] if len(parts) >= 3 else None
    return parts[1]

  return None


def _cached_tier(graph_id: str) -> str | None:
  try:
    value = _redis().get(f"{_CACHE_PREFIX}{graph_id}")
  except Exception as e:
    # A cache outage must not fail the request; fall through to the database.
    logger.debug(f"Rate limit tier cache unavailable for {graph_id}: {e}")
    return None
  return value if isinstance(value, str) else None


def _store_tier(graph_id: str, tier: str) -> None:
  try:
    _redis().setex(f"{_CACHE_PREFIX}{graph_id}", _TIER_CACHE_TTL_SECONDS, tier)
  except Exception as e:
    logger.debug(f"Could not cache tier for {graph_id}: {e}")


def resolve_graph_tier(graph_id: str) -> str:
  """Return the graph's tier, or the tightest tier if it cannot be determined.

  Never raises: rate limiting is not the place to surface a lookup failure, and
  failing open would let an error hand out more throughput than the customer
  bought.

  A tier string with no entry in SUBSCRIPTION_RATE_LIMITS — ladybug-shared on
  a repository row, or any unrecognized value — also resolves to FALLBACK_TIER:
  without this guard the limits lookup falls through to the anonymous "base"
  table, dropping a paying customer below the floor this module promises.
  """
  cached = _cached_tier(graph_id)
  if cached == _UNKNOWN:
    return FALLBACK_TIER
  if cached:
    return (
      cached if cached in RateLimitConfig.SUBSCRIPTION_RATE_LIMITS else FALLBACK_TIER
    )

  try:
    from ...database import session as session_factory
    from ...models.core.graph import Graph

    db = session_factory()
    try:
      graph = Graph.get_by_id(graph_id, db)
      tier = str(graph.graph_tier) if graph and graph.graph_tier else None
    finally:
      db.close()
  except Exception as e:
    logger.warning(f"Could not resolve tier for {graph_id}, using {FALLBACK_TIER}: {e}")
    return FALLBACK_TIER

  if not tier:
    _store_tier(graph_id, _UNKNOWN)
    return FALLBACK_TIER

  _store_tier(graph_id, tier)
  return tier if tier in RateLimitConfig.SUBSCRIPTION_RATE_LIMITS else FALLBACK_TIER
