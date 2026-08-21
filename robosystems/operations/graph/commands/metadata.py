"""Edit a graph's platform-level metadata — display name, description, tags.

Backs the ``update-graph-metadata`` operation — the only writer for these
fields after provisioning, where ``GraphCreationService`` sets ``graph_name``
and persists ``description`` / ``tags`` into ``graph_metadata``.

Scope is deliberately the platform label only. The entity name that appears
on financial statements lives in the extensions OLTP database and changes
through ``update-entity``; the two are allowed to differ (an org may run
"Acme — Prod" over an entity legally named "Acme Corporation, Inc.").
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.graphs.operations import GraphMetadataResult
from robosystems.models.core.graph import Graph

__all__ = ["update_graph_metadata_cmd"]


def update_graph_metadata_cmd(
  graph_id: str,
  *,
  graph_name: str | None,
  description: str | None,
  tags: list[str] | None,
  user_id: str,
  db: Session,
) -> GraphMetadataResult:
  """Apply a partial metadata update to a graph and return the new state.

  ``None`` means "leave unchanged" for each field; the caller is expected
  to have rejected an all-``None`` update already. Requires admin on the
  graph — this is graph configuration, not graph data, so the member role
  that can write nodes is not sufficient.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.models.core.graph.graph_user import GraphUser

  # Shared repositories (SEC, etc.) are platform-managed and shown to every
  # subscriber under the same name — one tenant must not be able to relabel
  # them for everyone. Checked before the role lookup so the error names the
  # real reason rather than "admin access required".
  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Metadata on shared repository '{graph_id}' is platform-managed.",
    )

  graph = Graph.get_by_id(graph_id, db)
  if graph is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  # `user_has_admin_access` resolves a subgraph to its parent and honours the
  # implicit admin an org OWNER/ADMIN holds over graphs their org pays for.
  if not GraphUser.user_has_admin_access(user_id, graph_id, db):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Admin access to graph {graph_id} is required to edit its metadata.",
    )

  metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
  updated_fields: list[str] = []

  if graph_name is not None and graph_name != graph.graph_name:
    graph.graph_name = graph_name
    updated_fields.append("graph_name")

  # Compare against the model's coerced view, not the raw blob: a malformed
  # stored value should read as unset and be overwritten, not compared as-is.
  if description is not None and description != graph.description:
    metadata["description"] = description
    updated_fields.append("description")

  if tags is not None and tags != graph.tags:
    metadata["tags"] = tags
    updated_fields.append("tags")

  if updated_fields:
    # Reassign rather than mutate in place: SQLAlchemy does not track
    # mutations inside a plain JSONB dict, so an in-place edit would be
    # silently dropped at flush.
    graph.graph_metadata = metadata
    graph.updated_at = datetime.now(UTC)
    db.commit()
    db.refresh(graph)
    logger.info(
      f"Updated graph metadata for {graph_id}: "
      f"fields={updated_fields} by user={user_id}"
    )

  return GraphMetadataResult(
    graph_id=str(graph.graph_id),
    graph_name=str(graph.graph_name),
    description=graph.description,
    tags=graph.tags,
    updated_fields=updated_fields,
  )
