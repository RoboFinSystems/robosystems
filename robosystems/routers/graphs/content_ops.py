"""Content operations — non-language knowledge writes (CQRS command surface).

Memory (remember / forget / update-memory), corpus documents (index-document /
delete-document), and file staging (ingest-file / delete-file) all write through
the operation envelope. Split out from ``operations.py`` (graph lifecycle) since
they're a distinct surface — the "Content Operations" tag — even though they
share the ``/operations/{op}`` path and the envelope machinery.

All handlers return ``OperationEnvelope`` with idempotency + audit, and delegate
to the operations kernel (services / commands) — no business logic here.

URL surface: ``POST /v1/graphs/{graph_id}/operations/{op_name}``
"""

from __future__ import annotations

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  Header,
  HTTPException,
  Path,
  status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  check_idempotency,
  fingerprint_body,
  get_idempotency_cache,
  log_operation_audit,
  wrap_completed,
  wrap_pending,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.graphs.operations import (
  DeleteDocumentOp,
  DeleteFileOp,
  ForgetOp,
  IndexDocumentOp,
  IngestFileOp,
  RememberOp,
  UpdateMemoryOp,
)
from robosystems.models.api.graphs.tables import FileUploadRequest
from robosystems.models.core import User

# Shared envelope-dispatch machinery lives with the lifecycle ops; content ops
# reuse it verbatim (one-way import — operations.py never imports this module).
from robosystems.routers.graphs.operations import (
  _AUDIT_EVENT,
  _GRAPH_OPS_PATH,
  _RATE_LIMIT,
  _ctx,
  _dispatch,
)

router = APIRouter()

_CONTENT_OP_TAG = "Content Operations"


def _require_memory_enabled() -> None:
  from robosystems.config import env

  if not env.SEMANTIC_MEMORY_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Semantic memory is not enabled",
    )


def _block_shared_repo(graph_id: str) -> None:
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id.lower()):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Content operations are not available on shared repositories",
    )


def _require_graph_write_access(graph_id: str) -> None:
  """Enforce write-level graph access (subscription state + write role).

  ``get_current_user_with_graph`` only proves *some* access (incl. viewer);
  content-op writes must also pass the subscription/lifecycle + write-role checks
  the resource write surfaces apply. Shared by all content-op write handlers.
  """
  from robosystems.database import SessionFactory
  from robosystems.middleware.billing.enforcement import require_graph_access

  session = SessionFactory()
  try:
    require_graph_access(graph_id, session, require_write=True)
  finally:
    session.close()


def _require_search_enabled() -> None:
  from robosystems.config import env

  if not env.SEMANTIC_SEARCH_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Document search is not enabled",
    )


def _resolve_graph_tier(graph_id: str, session) -> str:
  from robosystems.models.core.graph import Graph

  graph = Graph.get_by_id(graph_id, session)
  if graph is None or graph.graph_tier is None:
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Unable to determine subscription tier",
    )
  return graph.graph_tier


# ═══════════════════════════════════════════════════════════════════════════
# remember (semantic memory — content op)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/remember",
  response_model=OperationEnvelope,
  operation_id="opRemember",
  summary="Store a Semantic Memory",
  description="Store a semantic memory in the graph's per-graph memory store. "
  "The text is embedded locally; recall it later via `POST /memory/recall`.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/remember",
  method="POST",
  business_event_type="graph_remember",
)
async def remember_op(
  body: RememberOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  from robosystems.graph_api.client.factory import get_graph_client
  from robosystems.models.api.memory import MemoryCreateRequest
  from robosystems.operations.memory import get_memory_service

  _require_memory_enabled()
  _block_shared_repo(graph_id)
  _require_graph_write_access(graph_id)

  op_name = "remember"
  user_id = str(user.id)
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    client = await get_graph_client(graph_id=graph_id, operation_type="write")
    try:
      service = get_memory_service(client)
      if service is None:  # pragma: no cover - gated above
        raise HTTPException(status_code=503, detail="Semantic memory is not enabled")
      record = await service.remember(
        graph_id,
        MemoryCreateRequest(**body.model_dump()),
        created_by=user_id,
      )
      return record.model_dump(mode="json")
    finally:
      await client.close()

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# forget (semantic memory — content op)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/forget",
  response_model=OperationEnvelope,
  operation_id="opForget",
  summary="Delete a Semantic Memory",
  description="Delete a semantic memory by its server-generated id.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/forget",
  method="POST",
  business_event_type="graph_forget",
)
async def forget_op(
  body: ForgetOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  from robosystems.graph_api.client.factory import get_graph_client
  from robosystems.operations.memory import get_memory_service

  _require_memory_enabled()
  _block_shared_repo(graph_id)
  _require_graph_write_access(graph_id)

  op_name = "forget"
  user_id = str(user.id)
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    client = await get_graph_client(graph_id=graph_id, operation_type="write")
    try:
      service = get_memory_service(client)
      if service is None:  # pragma: no cover - gated above
        raise HTTPException(status_code=503, detail="Semantic memory is not enabled")
      result = await service.forget(graph_id, body.memory_id)
      return result.model_dump(mode="json")
    finally:
      await client.close()

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# update-memory (semantic memory — content op)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/update-memory",
  response_model=OperationEnvelope,
  operation_id="opUpdateMemory",
  summary="Update a Semantic Memory",
  description="Partially update a stored memory by its server-generated id. "
  "Only supplied fields change; the memory is re-embedded when `text` changes.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/update-memory",
  method="POST",
  business_event_type="graph_update_memory",
)
async def update_memory_op(
  body: UpdateMemoryOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  from robosystems.graph_api.client.factory import get_graph_client
  from robosystems.models.api.memory import MemoryUpdateRequest
  from robosystems.operations.memory import get_memory_service

  _require_memory_enabled()
  _block_shared_repo(graph_id)
  _require_graph_write_access(graph_id)

  op_name = "update-memory"
  user_id = str(user.id)
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    client = await get_graph_client(graph_id=graph_id, operation_type="write")
    try:
      service = get_memory_service(client)
      if service is None:  # pragma: no cover - gated above
        raise HTTPException(status_code=503, detail="Semantic memory is not enabled")
      # exclude_unset so a partial update only forwards the fields the caller
      # actually set — otherwise the unset fields dump as None and the service
      # (which keys on model_fields_set) wipes memory_type/tags/source_ref/
      # provenance to NULL.
      record = await service.update_memory(
        graph_id,
        body.memory_id,
        MemoryUpdateRequest(
          **body.model_dump(exclude={"memory_id"}, exclude_unset=True)
        ),
      )
      if record is None:
        raise HTTPException(status_code=404, detail="Memory not found")
      return record.model_dump(mode="json")
    finally:
      await client.close()

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# index-document (corpus content op)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/index-document",
  response_model=OperationEnvelope,
  operation_id="opIndexDocument",
  summary="Index a Document",
  description="Create a document (omit `document_id`) or update one (provide it). "
  "Stored in PostgreSQL, synced to OpenSearch for search.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/index-document",
  method="POST",
  business_event_type="graph_index_document",
)
async def index_document_op(
  body: IndexDocumentOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  from robosystems.database import SessionFactory
  from robosystems.models.api.search import DocumentUploadRequest
  from robosystems.operations.document_service import DocumentService

  _require_search_enabled()
  _block_shared_repo(graph_id)
  _require_graph_write_access(graph_id)

  op_name = "index-document"
  user_id = str(user.id)
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    session = SessionFactory()
    try:
      service = DocumentService(session)
      if body.document_id:
        kwargs = {
          f: getattr(body, f)
          for f in ("title", "content", "tags", "folder")
          if f in body.model_fields_set
        }
        _doc, response = service.update_document(
          graph_id=graph_id, document_id=body.document_id, **kwargs
        )
      else:
        if not body.title or body.content is None:
          raise HTTPException(
            status_code=422,
            detail="title and content are required to create a document",
          )
        tier = _resolve_graph_tier(graph_id, session)
        request = DocumentUploadRequest(
          title=body.title,
          content=body.content,
          tags=body.tags,
          folder=body.folder,
          external_id=body.external_id,
        )
        _doc, response = service.create_document(
          graph_id=graph_id, user_id=user.id, request=request, tier=tier
        )
      return response.model_dump(mode="json")
    except HTTPException:
      raise
    except KeyError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
      raise HTTPException(status_code=422, detail=str(e))
    finally:
      session.close()

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# delete-document (corpus content op)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/delete-document",
  response_model=OperationEnvelope,
  operation_id="opDeleteDocument",
  summary="Delete a Document",
  description="Delete a document from PostgreSQL and OpenSearch by id.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/delete-document",
  method="POST",
  business_event_type="graph_delete_document",
)
async def delete_document_op(
  body: DeleteDocumentOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  from robosystems.database import SessionFactory
  from robosystems.operations.document_service import DocumentService

  _require_search_enabled()
  _block_shared_repo(graph_id)
  _require_graph_write_access(graph_id)

  op_name = "delete-document"
  user_id = str(user.id)
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    session = SessionFactory()
    try:
      service = DocumentService(session)
      deleted = service.delete_document(graph_id, body.document_id)
      if not deleted:
        raise HTTPException(status_code=404, detail="Document not found")
      return {"document_id": body.document_id, "deleted": True}
    finally:
      session.close()

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# create-file-upload (raw → presigned S3 upload content op)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-file-upload",
  response_model=OperationEnvelope,
  operation_id="opCreateFileUpload",
  summary="Presign a File Upload",
  description="Presign an S3 URL for direct upload and register the file. After "
  "uploading to the returned URL, call `POST /operations/ingest-file` to stage it "
  "into DuckDB. The staging table is auto-created if missing. Not allowed on "
  "entity graphs or shared repositories.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/create-file-upload",
  method="POST",
  business_event_type="graph_create_file_upload",
)
async def create_file_upload_op(
  body: FileUploadRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.operations.graph.commands.create_file_upload import (
    create_file_upload_cmd,
  )

  op_name = "create-file-upload"
  user_id = str(user.id)
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    result = await create_file_upload_cmd(
      graph_id=graph_id, request=body, current_user=user, db=db
    )
    return result.model_dump(mode="json")

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# ingest-file (raw → staging content flow)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/ingest-file",
  response_model=OperationEnvelope,
  operation_id="opIngestFile",
  summary="Stage an Uploaded File",
  description="Mark an uploaded file ready and stage it into DuckDB. Small files "
  "stage directly (sync); large files stage via a background job (returns a "
  "pending envelope with an `operation_id` to monitor). Set `ingest_to_graph` to "
  "auto-materialize into the graph after staging.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/ingest-file",
  method="POST",
  business_event_type="graph_ingest_file",
)
async def ingest_file_op(
  body: IngestFileOp,
  background_tasks: BackgroundTasks,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.operations.graph.commands.ingest_file import ingest_file_cmd

  _block_shared_repo(graph_id)
  _require_graph_write_access(graph_id)

  op_name = "ingest-file"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await check_idempotency(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  result = await ingest_file_cmd(
    graph_id=graph_id,
    file_id=body.file_id,
    ingest_to_graph=body.ingest_to_graph,
    current_user=user,
    db=db,
    background_tasks=background_tasks,
  )

  # Async staging (Dagster) returns an operation_id → pending; direct staging with
  # no follow-on ingest completes inline → completed.
  if result.get("operation_id"):
    envelope = wrap_pending(
      op_name,
      operation_id=result["operation_id"],
      partial_result=result,
      created_by=user_id,
    )
  else:
    envelope = wrap_completed(op_name, result=result, created_by=user_id)

  if idempotency_key is not None:
    await cache.put(user_id, graph_id, op_name, idempotency_key, envelope, body_fp)

  log_operation_audit(
    operation_name=op_name,
    operation_id=envelope.operation_id,
    user_id=user_id,
    graph_id=graph_id,
    duration_ms=0.0,
    status=envelope.status,
    idempotency_key=idempotency_key,
    event=_AUDIT_EVENT,
  )
  return envelope


# ═══════════════════════════════════════════════════════════════════════════
# delete-file (raw content op)
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/delete-file",
  response_model=OperationEnvelope,
  operation_id="opDeleteFile",
  summary="Delete a File",
  description="Delete a file from S3 and PostgreSQL. `cascade=true` also removes "
  "its rows from DuckDB staging tables and marks the graph stale.",
  tags=[_CONTENT_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/delete-file",
  method="POST",
  business_event_type="graph_delete_file",
)
async def delete_file_op(
  body: DeleteFileOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.operations.graph.commands.delete_file import delete_file_cmd

  _block_shared_repo(graph_id)
  _require_graph_write_access(graph_id)

  op_name = "delete-file"
  user_id = str(user.id)
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    result = await delete_file_cmd(
      graph_id=graph_id,
      file_id=body.file_id,
      cascade=body.cascade,
      current_user=user,
      db=db,
    )
    return result.model_dump(mode="json")

  return await _dispatch(ctx, _runner, cache)
