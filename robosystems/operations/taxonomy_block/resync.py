"""Re-sync library taxonomy content into already-provisioned tenant schemas.

The provisioning-time copy (``taxonomy/writer.py::copy_library_into_tenant``) is
additive-only, so in-place library fixes (rule logic, element attributes, trait
names/bindings, calc weights, presentation order) never reach a tenant once it
is provisioned. This module is the
catch-up path: it runs ``resync_library_into_tenant`` (the ``DO UPDATE`` sibling)
inside a transaction that has executed ``SET LOCAL robosystems.library_resync =
'on'`` — the GUC the immutability trigger (migration 0016) honors so the seeder,
and only the seeder, may update library-origin rows in tenant scope.

**These two wrappers currently have no callers.** No admin CLI group, no job, no
publish hook invokes them — an earlier version of this docstring described that
surface as if it existed. What *does* run in production is the lower-level
``resync_library_into_tenant`` called directly from migrations (0022 rs-metric,
0023 rs-metric grown in place, 0024 rs-driver), each fanning one **package-pinned**
resync across ``for_each_tenant_schema``. So the propagation machinery is proven;
what is missing is only an operator entrypoint that does not require authoring a
migration — and, because every migration so far pinned a single package,
**rs-gaap core has never been re-propagated to a provisioned tenant**. Keep these
functions: they are the intended seam for that entrypoint.

Propagation policy: only safe for **in-place fixes within a framework version**
(mapping targets stay version-stable; filed reports pin their own FactSets; live
reports re-derive on next render — that is how a tenant *gets* the fix).
Structural reorganizations belong to a new framework version + opt-in re-pin,
never a silent re-sync.
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

# Tenant schemas are graph-id-named: ``kg`` + 16+ hex chars (mirrors
# ``migrations/extensions/helpers.py::for_each_tenant_schema``).
_TENANT_SCHEMA_SQL = text(
  "SELECT schema_name FROM information_schema.schemata "
  "WHERE schema_name ~ '^kg[0-9a-f]{16,}$' ORDER BY schema_name"
)


def list_tenant_schemas() -> list[str]:
  """Every provisioned tenant schema name, sorted."""
  with extensions_session(LIBRARY_GRAPH_ID) as session:
    rows = session.execute(_TENANT_SCHEMA_SQL).fetchall()
  return [row[0] for row in rows]


def resync_tenant(graph_id: str, pin: dict[str, str] | None = None) -> CopyStats:
  """Catch one tenant up to the current public library.

  ``extensions_session`` validates the schema name and owns commit/rollback; the
  bypass GUC is set inside that transaction so the immutability trigger permits
  the library UPDATEs, then the ``DO UPDATE`` fan-out runs across all 12 library
  tables on the same connection/transaction.
  """
  with extensions_session(graph_id) as session:
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

  Each schema runs in its own ``extensions_session`` transaction, so a failure on
  one tenant rolls back only that tenant and does not block the rest. Failures are
  logged and the schema is omitted from the returned map; the caller decides how
  to surface partial completion.

  **Sequential by design** — `O(N)` independent transactions over the tenant
  schemas. Fine at current tenant counts (tens to low hundreds); if the fleet grows
  enough that a full-fleet re-sync becomes slow, batch or parallelize here (each
  schema is independent). The admin CLI / publish hook is the caller.
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
