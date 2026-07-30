"""External provider — registration for integrations the platform does not run.

An ``external`` connection is registration + telemetry, not execution
config. It claims a source namespace for the graph: the event-source
validation in ``operations/event_block/commands.py`` accepts
``Event.source`` values that match a live external connection's
``source_name``. The integration itself runs outside the platform,
holds its own source credentials, and writes through the public API —
so there is nothing here to OAuth, sync, or clean up.
"""

from typing import Any

from sqlalchemy.orm import Session

from ...logger import logger
from ...models.api.graphs.connections import ExternalConnectionConfig
from ...models.core.connection.connection import Connection
from ...operations.connection_service import ConnectionService


async def create_external_connection(
  entity_id: str | None,
  config: ExternalConnectionConfig,
  user_id: str,
  graph_id: str,
  db: Session,
) -> str:
  """Register an external source namespace on the graph.

  Rejects a ``source_name`` already claimed on the graph — by another
  external connection or colliding with a platform provider's name. The
  partial unique index on ``(graph_id, source_name)`` is the
  race-proof backstop; this pre-check produces the friendly error.
  The connection is born ``connected`` (there is no auth handshake to
  wait for) and ``write_policy`` defaults to ``native``.
  """
  existing = Connection.get_all_for_graph(graph_id, db)
  for connection in existing:
    if (
      connection.source_name == config.source_name
      or connection.provider == config.source_name
    ):
      raise ValueError(
        f"source_name {config.source_name!r} is already registered on this "
        f"graph (connection {connection.id})"
      )

  connection_data = await ConnectionService.create_connection(
    entity_id=entity_id or "",
    provider="external",
    user_id=user_id,
    credentials=None,
    metadata={
      "status": "connected",
      "source_name": config.source_name,
      "entity_name": config.display_name,
    },
    graph_id=graph_id,
    db_session=db,
  )

  logger.info(
    f"Registered external source {config.source_name!r} on graph {graph_id} "
    f"as connection {connection_data['connection_id']}"
  )
  return connection_data["connection_id"]


async def sync_external_connection(
  connection: dict[str, Any],
  sync_options: dict[str, Any] | None,
  graph_id: str,
) -> str:
  """External connections are push-based — nothing for the platform to pull."""
  return (
    "External connections are push-based: the integration writes through "
    "the public API on its own schedule; there is nothing to sync."
  )


async def cleanup_external_connection(
  connection: dict[str, Any], graph_id: str
) -> None:
  """No credentials or provider-side state to revoke."""
  return None
