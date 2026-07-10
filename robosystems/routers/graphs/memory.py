"""Semantic memory reads — recall / list / get.

Backed by the per-graph LanceDB memory store on the writer/master instance.
Writes (remember / forget / update-memory) go through the content-ops envelope
in operations.py; this router is the read half: ranked semantic `recall` plus
the deterministic list / get governance reads.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette import status as http_status

from robosystems.database import SessionFactory
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.memory import (
  MemoryListResponse,
  MemoryRecallRequest,
  MemoryRecord,
)
from robosystems.models.api.search import SearchResponse
from robosystems.models.core import User

logger = logging.getLogger(__name__)

router = APIRouter(
  prefix="/memory",
  tags=["Memory"],
  dependencies=[Depends(subscription_aware_rate_limit_dependency)],
)


def _require_memory_enabled() -> None:
  from robosystems.config import env

  if not env.SEMANTIC_MEMORY_ENABLED:
    raise HTTPException(
      status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Semantic memory is not enabled",
    )


def _block_shared_repository(graph_id: str) -> None:
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Semantic memory is not available on shared repository graphs",
    )


def _enforce_graph_access(graph_id: str, require_write: bool = False) -> None:
  from robosystems.middleware.billing.enforcement import require_graph_access

  session = SessionFactory()
  try:
    require_graph_access(graph_id, session, require_write=require_write)
  finally:
    session.close()


async def _get_writer_client(graph_id: str):
  # Memory lives only on the writer/master (not in replica sync).
  from robosystems.graph_api.client.factory import get_graph_client

  return await get_graph_client(graph_id=graph_id, operation_type="write")


def _require_service(client):
  from robosystems.operations.memory import get_memory_service

  service = get_memory_service(client)
  if service is None:  # pragma: no cover - gated above
    raise HTTPException(status_code=503, detail="Semantic memory is not enabled")
  return service


@router.get(
  "",
  summary="List Memories",
  operation_id="list_memories",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_memories(
  graph_id: str,
  memory_type: str | None = Query(None, description="Filter by memory type"),
  source: str | None = Query(None, description="Filter by source"),
  limit: int = Query(100, ge=1, le=500),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
) -> MemoryListResponse:
  _require_memory_enabled()
  _block_shared_repository(graph_id)
  _enforce_graph_access(graph_id)

  client = await _get_writer_client(graph_id)
  try:
    service = _require_service(client)
    return await service.list_memories(
      graph_id,
      memory_type=memory_type,
      source=source,
      limit=limit,
      offset=offset,
    )
  finally:
    await client.close()


@router.get(
  "/{memory_id}",
  summary="Get Memory",
  operation_id="get_memory",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_memory(
  graph_id: str,
  memory_id: str,
  current_user: User = Depends(get_current_user_with_graph),
) -> MemoryRecord:
  _require_memory_enabled()
  _block_shared_repository(graph_id)
  _enforce_graph_access(graph_id)

  client = await _get_writer_client(graph_id)
  try:
    service = _require_service(client)
    record = await service.get_memory(graph_id, memory_id)
    if record is None:
      raise HTTPException(status_code=404, detail="Memory not found")
    return record
  finally:
    await client.close()


@router.post(
  "/recall",
  summary="Recall Semantic Memory",
  description="Ranked semantic recall over the graph's per-graph memory store. "
  "Returns scored hits in the same shape as document search.",
  operation_id="recall_memory",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    503: {"description": "Semantic memory not available"},
  },
)
async def recall_memory(
  graph_id: str,
  request: MemoryRecallRequest,
  current_user: User = Depends(get_current_user_with_graph),
) -> SearchResponse:
  _require_memory_enabled()
  _block_shared_repository(graph_id)
  _enforce_graph_access(graph_id)

  client = await _get_writer_client(graph_id)
  try:
    service = _require_service(client)
    return await service.recall(graph_id, request)
  finally:
    await client.close()
