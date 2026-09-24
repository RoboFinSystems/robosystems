"""Report lifecycle: create, regenerate, file, and filing-status transitions.
Sharing is in `distribution.py`."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from pydantic import BaseModel, ConfigDict, Field

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import user_is_graph_admin
from robosystems.middleware.extensions import OperationSpec
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES, DeleteResult
from robosystems.models.api.extensions.report_package import (
  FileReportRequest,
  TransitionFilingStatusRequest,
)
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  RegenerateReportRequest,
  ReportResponse,
)
from robosystems.models.core import User
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.reports import (
  BundleUploadError,
  InvalidFilingTransitionError,
  NoEntityError,
  NotAuthorizedError,
  ReportHasActiveSharesError,
  ReportNotFiledError,
  ReportNotFoundError,
  TaxonomyNotFoundError,
  delete_report_artifacts,
)
from robosystems.operations.roboledger.commands.reports import (
  create_report as cmd_create_report,
)
from robosystems.operations.roboledger.commands.reports import (
  delete_report as cmd_delete_report,
)
from robosystems.operations.roboledger.commands.reports import (
  file_report as cmd_file_report,
)
from robosystems.operations.roboledger.commands.reports import (
  regenerate_report as cmd_regenerate_report,
)
from robosystems.operations.roboledger.commands.reports import (
  transition_filing_status as cmd_transition_filing_status,
)
from robosystems.routers.extensions.roboledger._common import (
  _RATE_LIMIT,
  _ctx,
  _dispatch,
  _require_roboledger_write,
  make_registrar,
)

router = APIRouter()

_OP_TAG = "RoboLedger: Reports"
_registrar = make_registrar(router, _OP_TAG)


class RegenerateReportOperation(RegenerateReportRequest):
  """Regenerate facts for an existing Report. Carries `report_id` from
  the path-style RPC body; period overrides are inherited from
  :class:`RegenerateReportRequest`.
  """

  report_id: str = Field(..., description="The Report to regenerate.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0"},
        {
          "report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0",
          "period_start": "2026-04-01",
          "period_end": "2026-06-30",
        },
      ]
    },
  )


class DeleteReportOperation(BaseModel):
  """Delete a Report definition and all its facts."""

  report_id: str = Field(..., description="The Report to delete.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    },
  )


# ── Reports ──────────────────────────────────────────────────────────────────


def _validate_report_window(body: CreateReportRequest) -> None:
  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")


create_report_op = _registrar.register(
  OperationSpec(
    name="create-report",
    summary="Create Report",
    description=(
      "Generates report facts from the ledger and marks the report as published."
    ),
    command=cmd_create_report,
    request_model=CreateReportRequest,
    result_type=ReportResponse,
    business_event_type="ledger_create_report",
    requires_graph_id=True,
    pre_validate=_validate_report_window,
    error_map={
      TaxonomyNotFoundError: (422, lambda e: f"Taxonomy '{e}' not found."),
      NoEntityError: 422,
      # `_stamp_report_bundle` aborted the publish (S3 unavailable) to keep
      # "every published Report has a stored bundle"; 502 so the client retries.
      BundleUploadError: 502,
    },
    mark_stale_reason="report_generated",
  )
)


regenerate_report_op = _registrar.register(
  OperationSpec(
    name="regenerate-report",
    summary="Regenerate Report",
    description=(
      "Re-runs fact generation for an existing Report against the latest "
      "ledger state. Pass `period_start`/`period_end`/`periods` only if "
      "you want to change the reporting window."
    ),
    command=cmd_regenerate_report,
    request_model=RegenerateReportOperation,
    result_type=ReportResponse,
    business_event_type="ledger_regenerate_report",
    requires_graph_id=True,
    error_map={
      # A concurrent writer holds the rows; retryable, same 409 as the registrar.
      RowLockedError: 409,
      ReportNotFoundError: (404, lambda e: f"Report '{e}' not found."),
      NotAuthorizedError: (403, lambda _e: "Not authorized to modify this report."),
      InvalidFilingTransitionError: 422,
      # Fail loud like create-report, or ``bundle_url`` would lag the facts.
      BundleUploadError: 502,
    },
    mark_stale_reason="report_generated",
  )
)


@router.post(
  "/delete-report",
  response_model=OperationEnvelope[DeleteResult],
  operation_id="deleteReport",
  summary="Delete Report",
  description=(
    "Deletes the report definition and all generated facts. Normally "
    "restricted to the report's creator. A report shared in from another "
    "graph carries the sender's user id in `created_by`, so those may be "
    "deleted by any admin of the receiving graph — the recipient's exit "
    "from an unsolicited share. Deleting a shared copy does not affect the "
    "sender's record that they sent it."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/delete-report",
  method="POST",
  business_event_type="ledger_delete_report",
)
async def delete_report_op(
  body: DeleteReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        deleted = cmd_delete_report(
          session,
          body.report_id,
          str(user.id),
          acting_user_is_graph_admin=user_is_graph_admin(str(user.id), graph_id),
        )
      except NotAuthorizedError:
        raise HTTPException(
          status_code=403, detail="Not authorized to delete this report."
        )
      except ReportHasActiveSharesError as e:
        raise HTTPException(status_code=409, detail=str(e))
      except ReportNotFiledError as e:
        raise HTTPException(status_code=422, detail=str(e))
      except RowLockedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not deleted:
      raise HTTPException(
        status_code=404, detail=f"Report '{body.report_id}' not found."
      )
    return DeleteResult(deleted=True)

  # The OLAP projection rebuilds from OLTP, so the report leaves the graph only
  # once it's marked stale. Published artifacts are deleted after the rows
  # commit, never before (see `delete_report_artifacts`).
  def _finish_delete(_env) -> None:
    mark_graph_stale(graph_id, "report_deleted")
    delete_report_artifacts(graph_id, [body.report_id])

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=_finish_delete,
  )


@router.post(
  "/file-report",
  response_model=OperationEnvelope[ReportResponse],
  operation_id="fileReport",
  summary="File Report",
  description=(
    "Transitions the Report's filing_status to 'filed' — locks the package. "
    "Allowed from 'draft' or 'under_review'. Stamps filed_at + filed_by."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/file-report",
  method="POST",
  business_event_type="ledger_file_report",
)
async def file_report_op(
  body: FileReportRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="file-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        return cmd_file_report(session, body.report_id, filed_by=str(user.id))
      except RowLockedError as e:
        # Another lifecycle write holds the report. Retryable.
        raise HTTPException(status_code=409, detail=str(e))
      except ReportNotFoundError:
        raise HTTPException(
          status_code=404, detail=f"Report '{body.report_id}' not found."
        )
      except NotAuthorizedError as e:
        raise HTTPException(status_code=403, detail=str(e))
      except InvalidFilingTransitionError as e:
        raise HTTPException(status_code=422, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/transition-filing-status",
  response_model=OperationEnvelope[ReportResponse],
  operation_id="transitionFilingStatus",
  summary="Transition Filing Status",
  description=(
    "Move a Report along the non-file legs of the filing lifecycle "
    "(draft ↔ under_review, filed → archived). Use 'file-report' to "
    "reach 'filed' so audit fields land cleanly."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/transition-filing-status",
  method="POST",
  business_event_type="ledger_transition_filing_status",
)
async def transition_filing_status_op(
  body: TransitionFilingStatusRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="transition-filing-status",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        return cmd_transition_filing_status(
          session, body.report_id, body.target_status, acting_user_id=str(user.id)
        )
      except ReportNotFoundError:
        raise HTTPException(
          status_code=404, detail=f"Report '{body.report_id}' not found."
        )
      except NotAuthorizedError as e:
        raise HTTPException(status_code=403, detail=str(e))
      except InvalidFilingTransitionError as e:
        raise HTTPException(status_code=422, detail=str(e))
      except RowLockedError as e:
        raise HTTPException(status_code=409, detail=str(e))

  return await _dispatch(ctx, _runner, cache)
