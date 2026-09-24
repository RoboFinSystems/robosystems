"""Re-sync library taxonomy content into already-provisioned tenant schemas.

The provisioning-time copy is additive-only, so in-place library fixes never
reach a provisioned tenant. These wrappers run ``resync_library_into_tenant``
(the ``DO UPDATE`` sibling) under ``SET LOCAL robosystems.library_resync =
'on'``, the GUC the immutability trigger honors.

These wrappers have no callers: production propagation runs package-pinned
resyncs from migrations. Only safe for in-place fixes within a framework
version; structural changes need a new framework version.
"""

from __future__ import annotations

from sqlalchemy import text

from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session
from robosystems.logger import logger
from robosystems.taxonomy.writer import (
  SET_LIBRARY_RESYNC,
  CopyStats,
  resync_library_into_tenant,
)

# Mirrors ``migrations/extensions/helpers.py::for_each_tenant_schema``.
_TENANT_SCHEMA_SQL = text(
  "SELECT schema_name FROM information_schema.schemata "
  "WHERE schema_name ~ '^kg[0-9a-f]{16,}$' ORDER BY schema_name"
)


def list_tenant_schemas() -> list[str]:
  """Every provisioned tenant schema name, sorted."""
  with extensions_session(LIBRARY_GRAPH_ID, statement_timeout_ms=None) as session:
    rows = session.execute(_TENANT_SCHEMA_SQL).fetchall()
  return [row[0] for row in rows]


def resync_tenant(graph_id: str, pin: dict[str, str] | None = None) -> CopyStats:
  """Catch one tenant up to the current public library.

  The bypass GUC must be set in the same transaction as the updates.
  """
  with extensions_session(graph_id, statement_timeout_ms=None) as session:
    session.execute(text(SET_LIBRARY_RESYNC))
    stats = resync_library_into_tenant(session.connection(), graph_id, pin)
  logger.info(
    "[taxonomy-resync] %s: %d library rows inserted/updated (total)",
    graph_id,
    stats.total,
  )
  return stats


def resync_all_tenants(
  pin: dict[str, str] | None = None,
) -> dict[str, CopyStats]:
  """Catch every provisioned tenant up to the current public library.

  One transaction per schema, sequentially; a failed schema is logged and
  omitted from the returned map.
  """
  schemas = list_tenant_schemas()
  logger.info("[taxonomy-resync] re-syncing %d tenant schema(s)", len(schemas))
  results: dict[str, CopyStats] = {}
  for schema in schemas:
    try:
      results[schema] = resync_tenant(schema, pin)
    except Exception:
      logger.exception("[taxonomy-resync] %s failed — skipped", schema)
  logger.info(
    "[taxonomy-resync] done: %d/%d schema(s) re-synced",
    len(results),
    len(schemas),
  )
  return results
