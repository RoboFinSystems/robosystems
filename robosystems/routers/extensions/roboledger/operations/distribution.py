"""Getting a finished report to the people who read it.

Direct shares, the publish lists that address a group of recipients, and
the blocked-source-graph pair that is a recipient's exit from cross-graph
sharing. Read the current block list through the `blockedSourceGraphs`
GraphQL field.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from pydantic import BaseModel, ConfigDict, Field

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import user_is_graph_admin
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES, DeleteResult
from robosystems.models.api.extensions.blocked_source_graphs import (
  BlockedSourceGraphResponse,
  BlockSourceGraphRequest,
  BlockSourceGraphResult,
  UnblockSourceGraphRequest,
)
from robosystems.models.api.extensions.publish_lists import (
  AddMembersRequest,
  CreatePublishListRequest,
  PublishListMemberResponse,
  PublishListResponse,
  UpdatePublishListRequest,
)
from robosystems.models.api.extensions.reports import (
  RevokeReportShareRequest,
  RevokeReportShareResponse,
  ShareReportRequest,
  ShareReportResponse,
)
from robosystems.models.core import User
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.blocked_source_graphs import (
  AdminRoleRequiredError,
  SelfBlockError,
  SourceGraphNotBlockedError,
)
from robosystems.operations.roboledger.commands.blocked_source_graphs import (
  block_source_graph as cmd_block_source_graph,
)
from robosystems.operations.roboledger.commands.blocked_source_graphs import (
  unblock_source_graph as cmd_unblock_source_graph,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  MembersAlreadyPresentError,
  PublishListNameConflictError,
  PublishListNotAuthorizedError,
  PublishListNotFoundError,
  SelfAddError,
  TargetGraphMissingExtensionError,
  TargetGraphsNotFoundError,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  add_publish_list_members as cmd_add_publish_list_members,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  create_publish_list as cmd_create_publish_list,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  delete_publish_list as cmd_delete_publish_list,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  remove_publish_list_member as cmd_remove_publish_list_member,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  update_publish_list as cmd_update_publish_list,
)
from robosystems.operations.roboledger.commands.reports import (
  NotAuthorizedError,
  PublishListEmptyError,
  ReportNotFoundError,
  ReportNotPublishedError,
  ReportShareNotFoundError,
  delete_report_artifacts,
)
from robosystems.operations.roboledger.commands.reports import (
  PublishListNotFoundError as ReportPublishListNotFoundError,
)
from robosystems.operations.roboledger.commands.reports import (
  revoke_report_share as cmd_revoke_report_share,
)
from robosystems.operations.roboledger.commands.reports import (
  share_report as cmd_share_report,
)
from robosystems.routers.extensions.roboledger._common import (
  _RATE_LIMIT,
  _ctx,
  _dispatch,
  _require_roboledger_write,
  _result_payload,
  make_registrar,
)

router = APIRouter()

_OP_TAG = "RoboLedger: Report Distribution"
_registrar = make_registrar(router, _OP_TAG)


class ShareReportOperation(ShareReportRequest):
  """Share a published Report to every member of a publish list."""

  report_id: str = Field(..., description="The published Report to share.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0",
          "publish_list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0",
        },
      ]
    },
  )


class RevokeReportShareOperation(RevokeReportShareRequest):
  """Withdraw a shared Report from one recipient graph."""

  report_id: str = Field(..., description="The Report whose share to withdraw.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0",
          "target_graph_id": "kg1a2b3c4d5e6f7a8b9c",
        },
      ]
    },
  )


class UpdatePublishListOperation(UpdatePublishListRequest):
  """Update a publish list's metadata. Carries `list_id`."""

  list_id: str = Field(..., description="The publish list to update.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0", "name": "Senior Lenders"},
      ]
    },
  )


class DeletePublishListOperation(BaseModel):
  """Delete a publish list. All membership rows are removed; reports
  previously shared via this list are not affected (each share is an
  independent copy in the recipient's graph).
  """

  list_id: str = Field(..., description="The publish list to delete.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    },
  )


class AddPublishListMembersOperation(AddMembersRequest):
  """Add recipient graphs to a publish list."""

  list_id: str = Field(..., description="The publish list to add members to.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0",
          "target_graph_ids": ["kg_abc12345", "kg_def67890"],
        },
      ]
    },
  )


class RemovePublishListMemberOperation(BaseModel):
  """Remove a single recipient from a publish list."""

  list_id: str = Field(..., description="The publish list.")
  member_id: str = Field(..., description="The membership row to remove.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0",
          "member_id": "plm_01HVF8T0M2YTAY3BBNRH0V0",
        },
      ]
    },
  )


class BlockSourceGraphOperation(BlockSourceGraphRequest):
  """Bar a graph from sharing reports into this one."""


class UnblockSourceGraphOperation(UnblockSourceGraphRequest):
  """Lift a block on a source graph."""


@router.post(
  "/share-report",
  response_model=OperationEnvelope[ShareReportResponse],
  operation_id="shareReport",
  summary="Share Report",
  description=(
    "Pushes a published report to every member of the target publish "
    "list. Each share is an independent copy: the report row + all its "
    "facts are cloned into the recipient's tenant schema with "
    "`source_graph_id` / `source_report_id` provenance fields populated. "
    "Per-target outcomes (success or error) surface in the response — "
    "share does not fail-fast across targets. Recipients that have blocked "
    "this graph come back as an error for that target; withdraw a delivered "
    "copy with `revoke-report-share`."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/share-report",
  method="POST",
  business_event_type="ledger_share_report",
)
async def share_report_op(
  body: ShareReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="share-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      return cmd_share_report(
        graph_id, body.report_id, body, acting_user_id=str(user.id)
      )
    except ReportPublishListNotFoundError:
      raise HTTPException(status_code=404, detail="Publish list not found.")
    except PublishListEmptyError:
      raise HTTPException(status_code=422, detail="Publish list has no members.")
    except ReportNotFoundError:
      raise HTTPException(
        status_code=404, detail=f"Report '{body.report_id}' not found."
      )
    except NotAuthorizedError:
      raise HTTPException(
        status_code=403, detail="Not authorized to share this report."
      )
    except ReportNotPublishedError:
      raise HTTPException(
        status_code=422, detail="Only published reports can be shared."
      )
    except RowLockedError as e:
      # Another lifecycle write (or another share) holds the report.
      raise HTTPException(status_code=409, detail=str(e))

  def _mark_recipients_stale(envelope) -> None:
    # A share writes rows into each recipient's OLTP schema, but their
    # LadybugDB projection only picks the report up on a rebuild — so without
    # this, delivery depends on the recipient happening to have unrelated
    # ledger activity of their own. Mark each recipient that actually
    # received a copy. (`roboinvestor-maturity.md` D1, delivery half.)
    #
    # Read as a dict, not attributes: `wrap_completed` normalizes the
    # command's Pydantic result through `model_dump(mode="json")` before the
    # hook ever sees it. Attribute access here is not a type error — it
    # silently reads as absent, which is how this callback first shipped
    # doing nothing at all.
    for item in _result_payload(envelope).get("results") or []:
      if item.get("status") == "shared":
        mark_graph_stale(item["target_graph_id"], "report_shared_in")

  return await _dispatch(ctx, _runner, cache, on_fresh_success=_mark_recipients_stale)


@router.post(
  "/revoke-report-share",
  response_model=OperationEnvelope[RevokeReportShareResponse],
  operation_id="revokeReportShare",
  summary="Revoke Report Share",
  description=(
    "Withdraws a report previously shared to one recipient graph: deletes "
    "the copy from that recipient's schema and stamps the share record "
    "revoked. Scoped to a single recipient — withdrawing a distribution to a "
    "whole publish list is one call per member. A recipient who already "
    "deleted the copy is not an error; the share is still marked revoked and "
    "`copy_deleted` returns false. The linked entity in the recipient's graph "
    "is left in place, so an investor's declared holding survives."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/revoke-report-share",
  method="POST",
  business_event_type="ledger_revoke_report_share",
)
async def revoke_report_share_op(
  body: RevokeReportShareOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="revoke-report-share",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      return cmd_revoke_report_share(
        graph_id,
        body.report_id,
        body,
        acting_user_id=str(user.id),
        acting_user_is_graph_admin=user_is_graph_admin(str(user.id), graph_id),
      )
    except ReportNotFoundError:
      raise HTTPException(
        status_code=404, detail=f"Report '{body.report_id}' not found."
      )
    except ReportShareNotFoundError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except NotAuthorizedError:
      raise HTTPException(
        status_code=403, detail="Not authorized to revoke shares of this report."
      )

  def _mark_recipient_stale(envelope) -> None:
    # Same reasoning as the share path: the copy leaves the recipient's graph
    # only when their projection is rebuilt. Skip when nothing was deleted.
    if _result_payload(envelope).get("copy_deleted"):
      mark_graph_stale(body.target_graph_id, "report_share_revoked")

  return await _dispatch(ctx, _runner, cache, on_fresh_success=_mark_recipient_stale)


# ═══════════════════════════════════════════════════════════════════════════
# Publish Lists
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-publish-list",
  response_model=OperationEnvelope[PublishListResponse],
  operation_id="createPublishList",
  summary="Create Publish List",
  description=(
    "Create a publish list (a saved set of recipient graphs). Members "
    "are managed separately via add/remove-member operations."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/create-publish-list",
  method="POST",
  business_event_type="ledger_create_publish_list",
)
async def create_publish_list_op(
  body: CreatePublishListRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-publish-list",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        return cmd_create_publish_list(session, body, created_by=str(user.id))
      except PublishListNameConflictError as e:
        raise HTTPException(status_code=409, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/update-publish-list",
  response_model=OperationEnvelope[PublishListResponse],
  operation_id="updatePublishList",
  summary="Update Publish List",
  description="Updates the publish list's `name` and/or `description`. Membership is managed via add/remove-member operations.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/update-publish-list",
  method="POST",
  business_event_type="ledger_update_publish_list",
)
async def update_publish_list_op(
  body: UpdatePublishListOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="update-publish-list",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        return cmd_update_publish_list(
          session, body.list_id, body, acting_user_id=str(user.id)
        )
      except PublishListNotFoundError:
        raise HTTPException(status_code=404, detail="Publish list not found.")
      except PublishListNotAuthorizedError as e:
        raise HTTPException(status_code=403, detail=str(e))
      except PublishListNameConflictError as e:
        raise HTTPException(status_code=409, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/delete-publish-list",
  response_model=OperationEnvelope[DeleteResult],
  operation_id="deletePublishList",
  summary="Delete Publish List",
  description=(
    "Delete a publish list and its membership rows. Reports previously "
    "shared via this list are not affected — each share is an independent "
    "copy in the recipient's graph."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/delete-publish-list",
  method="POST",
  business_event_type="ledger_delete_publish_list",
)
async def delete_publish_list_op(
  body: DeletePublishListOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-publish-list",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        deleted = cmd_delete_publish_list(session, body.list_id, str(user.id))
      except PublishListNotAuthorizedError as e:
        raise HTTPException(status_code=403, detail=str(e))
    if not deleted:
      raise HTTPException(status_code=404, detail="Publish list not found.")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/add-publish-list-members",
  response_model=OperationEnvelope[list[PublishListMemberResponse]],
  operation_id="addPublishListMembers",
  summary="Add Members to Publish List",
  description=(
    "Add one or more recipient graphs to a publish list. Targets must "
    "exist and have the same extension enabled (e.g. roboledger). "
    "Self-graph rejected (422); already-member rejected (409)."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/add-publish-list-members",
  method="POST",
  business_event_type="ledger_add_publish_list_members",
)
async def add_publish_list_members_op(
  body: AddPublishListMembersOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="add-publish-list-members",
    idempotency_key=idempotency_key,
    body=body,
  )

  # The inherited AddMembersRequest is what the ops function takes; unwrap
  # our dispatch wrapper back to it so the ops layer sees the same shape.
  add_body = AddMembersRequest(target_graph_ids=body.target_graph_ids)

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        return cmd_add_publish_list_members(
          session, body.list_id, graph_id, add_body, added_by=str(user.id)
        )
      except PublishListNotFoundError:
        raise HTTPException(status_code=404, detail="Publish list not found.")
      except PublishListNotAuthorizedError as e:
        raise HTTPException(status_code=403, detail=str(e))
      except TargetGraphsNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
      except TargetGraphMissingExtensionError as e:
        raise HTTPException(status_code=422, detail=str(e))
      except SelfAddError:
        raise HTTPException(
          status_code=422, detail="Cannot add your own graph to a publish list."
        )
      except MembersAlreadyPresentError as e:
        raise HTTPException(status_code=409, detail=str(e))

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/remove-publish-list-member",
  response_model=OperationEnvelope[DeleteResult],
  operation_id="removePublishListMember",
  summary="Remove Member from Publish List",
  description="Remove a single recipient from a publish list.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/remove-publish-list-member",
  method="POST",
  business_event_type="ledger_remove_publish_list_member",
)
async def remove_publish_list_member_op(
  body: RemovePublishListMemberOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="remove-publish-list-member",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        deleted = cmd_remove_publish_list_member(
          session, body.list_id, body.member_id, acting_user_id=str(user.id)
        )
      except PublishListNotAuthorizedError as e:
        raise HTTPException(status_code=403, detail=str(e))
    if not deleted:
      raise HTTPException(status_code=404, detail="Member not found in this list.")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# Blocked source graphs — the recipient's exit from cross-graph sharing
#
# Sharing is authorized capability-style: whoever holds this graph's id can
# copy a published report in. These two operations are how a recipient
# declines. Read the current list via the `blockedSourceGraphs` GraphQL field.
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/block-source-graph",
  response_model=OperationEnvelope[BlockSourceGraphResult],
  operation_id="blockSourceGraph",
  summary="Block Source Graph",
  description=(
    "Bars a graph from sharing reports into this one. Subsequent "
    "`share-report` calls naming this graph fail for this target with an "
    "explicit error — blocked senders are told, not silently dropped. "
    "Idempotent: re-blocking preserves the original `blocked_at`. Set "
    "`purge` to also delete every report already shared in from that "
    "source, along with its fact sets and facts; reports this graph "
    "authored are never touched."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/block-source-graph",
  method="POST",
  business_event_type="ledger_block_source_graph",
)
async def block_source_graph_op(
  body: BlockSourceGraphOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="block-source-graph",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        return cmd_block_source_graph(
          session,
          body,
          created_by=str(user.id),
          graph_id=graph_id,
          acting_user_is_graph_admin=user_is_graph_admin(str(user.id), graph_id),
        )
      except SelfBlockError as e:
        raise HTTPException(status_code=422, detail=str(e))
      except AdminRoleRequiredError as e:
        raise HTTPException(status_code=403, detail=str(e))

  def _finish_purge(envelope) -> None:
    # Only a purge changes queryable content, and the OLAP projection is a
    # full rebuild from OLTP — a block on its own has nothing to re-project,
    # so it must not trigger one.
    payload = _result_payload(envelope)
    if not payload.get("purged_report_count"):
      return
    mark_graph_stale(graph_id, "shared_reports_purged")
    # The purge deleted rows; the senders' stored publications are still in
    # this graph's bundle prefix until this runs. It belongs here rather than
    # in the command because the rows must be committed first — deleting an
    # artifact for a purge that rolled back would destroy a live report's
    # publication, while an orphan object is recoverable.
    delete_report_artifacts(graph_id, list(payload.get("purged_report_ids") or []))

  return await _dispatch(ctx, _runner, cache, on_fresh_success=_finish_purge)


# Hand-mounted rather than declared through `_registrar`, deliberately. Every
# registrar spec is also published as an MCP tool
# (`middleware/mcp/tools/registrar.py`), and shared reports land in the
# recipient's schema where their own AI operators read them as ordinary data.
# An operator that could lift a block — acting on text a blocked sender wrote —
# would hand that sender the undo button for their own exclusion. `block` is
# hand-written for the same reason; keeping both halves off the tool surface is
# the point, not an accident of style.
@router.post(
  "/unblock-source-graph",
  response_model=OperationEnvelope[BlockedSourceGraphResponse],
  operation_id="unblockSourceGraph",
  summary="Unblock Source Graph",
  description=(
    "Lifts a block, allowing that graph to share reports into this one "
    "again. Reports removed by an earlier purge are not restored — "
    "unblocking reopens the channel, it does not undo. Requires the graph "
    "admin role: a block is a standing decision about who may write into "
    "this graph, so a member cannot reverse it over an admin's head. "
    "Returns 404 when the source was not blocked."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/unblock-source-graph",
  method="POST",
  business_event_type="ledger_unblock_source_graph",
)
async def unblock_source_graph_op(
  body: UnblockSourceGraphOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="unblock-source-graph",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    with extensions_session(graph_id) as session:
      try:
        return cmd_unblock_source_graph(
          session,
          body,
          acting_user_is_graph_admin=user_is_graph_admin(str(user.id), graph_id),
        )
      except AdminRoleRequiredError as e:
        raise HTTPException(status_code=403, detail=str(e))
      except SourceGraphNotBlockedError as e:
        raise HTTPException(status_code=404, detail=str(e))

  return await _dispatch(ctx, _runner, cache)
