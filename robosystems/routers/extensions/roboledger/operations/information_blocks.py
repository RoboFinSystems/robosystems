"""Information Blocks, the authoring unit of a report: the generic construction
envelope plus the rule engine, metric computation/assertion and the forecast
compiler."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.db.extensions import extensions_session
from robosystems.middleware.extensions import OperationSpec
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.extensions.text_blocks import (
  BindTextBlockRequest,
  BindTextBlockResponse,
)
from robosystems.models.api.information_block import (
  AssertMetricsRequest,
  AssertMetricsResponse,
  ComputeForecastRequest,
  ComputeForecastResponse,
  ComputeMetricsRequest,
  ComputeMetricsResponse,
  CreateInformationBlockRequest,
  DeleteInformationBlockRequest,
  DeleteInformationBlockResponse,
  EvaluateRulesRequest,
  EvaluateRulesResponse,
  InformationBlockEnvelope,
  UpdateInformationBlockRequest,
)
from robosystems.models.core import User
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.information_block.commands import (
  create_information_block as cmd_create_information_block,
)
from robosystems.operations.information_block.commands import (
  delete_information_block as cmd_delete_information_block,
)
from robosystems.operations.information_block.commands import (
  update_information_block as cmd_update_information_block,
)
from robosystems.operations.information_block.forecast_compute import (
  cmd_compute_forecast,
)
from robosystems.operations.information_block.metrics import (
  cmd_assert_metrics,
  cmd_compute_metrics,
)
from robosystems.operations.information_block.rules.commands import cmd_evaluate_rules
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.schedules import ScheduleNotFoundError
from robosystems.operations.roboledger.commands.text_blocks import (
  DocumentNotFoundError as TextBlockDocumentNotFoundError,
)
from robosystems.operations.roboledger.commands.text_blocks import (
  bind_text_block as cmd_bind_text_block,
)
from robosystems.routers.extensions.roboledger._common import (
  _RATE_LIMIT,
  _ctx,
  _dispatch,
  _require_roboledger_write,
  make_registrar,
)

router = APIRouter()

_OP_TAG = "RoboLedger: Information Blocks"
_registrar = make_registrar(router, _OP_TAG)


# ── Information Blocks ───────────────────────────────────────────────────────

create_information_block_op = _registrar.register(
  OperationSpec(
    name="create-information-block",
    summary="Create Information Block",
    description=(
      "Generic Information Block construction entry. `block_type` selects "
      "the registered block type; `payload` is validated against that "
      "type's creation schema at dispatch. Schedule dispatches to the "
      "existing Schedule machinery; statement block types raise 501 "
      "(use create-report instead). Authoring schedules for a close? Call "
      "`get-close-playbook` (mode='initiate') first — one schedule is a "
      "single debit/credit element pair, so multi-line entries become "
      "multiple schedules, and element ids must be real (discover via "
      "get-graph-schema / get-unmapped-elements / suggest-mapping)."
    ),
    command=cmd_create_information_block,
    request_model=CreateInformationBlockRequest,
    result_type=InformationBlockEnvelope,
    error_map={
      ValueError: 422,
      NotImplementedError: 501,
      ScheduleNotFoundError: 404,
    },
    mark_stale_reason="information_block_created",
  )
)

update_information_block_op = _registrar.register(
  OperationSpec(
    name="update-information-block",
    summary="Update Information Block",
    description=(
      "Generic Information Block update entry. Dispatches by `block_type` "
      "to the registered mutation handler. Block types whose Structures "
      "are library-seeded and immutable (statement family) surface 501."
    ),
    command=cmd_update_information_block,
    request_model=UpdateInformationBlockRequest,
    result_type=InformationBlockEnvelope,
    error_map={
      ValueError: 422,
      NotImplementedError: 501,
      ScheduleNotFoundError: 404,
      # Supersedes pending obligations the promotion sweep may hold. Retryable.
      RowLockedError: 409,
    },
    mark_stale_reason="information_block_updated",
  )
)

delete_information_block_op = _registrar.register(
  OperationSpec(
    name="delete-information-block",
    summary="Delete Information Block",
    description=(
      "Generic Information Block deletion entry. Returns a thin "
      "confirmation (deleted / structure_id / block_type / name). "
      "Block types whose Structures are library-seeded cannot be "
      "deleted per tenant and surface 501."
    ),
    command=cmd_delete_information_block,
    request_model=DeleteInformationBlockRequest,
    result_type=DeleteInformationBlockResponse,
    error_map={
      # Voids pending obligations the promotion sweep may hold. Retryable.
      RowLockedError: 409,
      ValueError: 422,
      NotImplementedError: 501,
      ScheduleNotFoundError: 404,
    },
    mark_stale_reason="information_block_deleted",
  )
)

evaluate_rules_op = _registrar.register(
  OperationSpec(
    name="evaluate-rules",
    summary="Evaluate Rules for an Information Block",
    description=(
      "Runs every rule targeting the given structure (plus element- and "
      "association-scoped rules for the structure's atoms), binds "
      "$Variable references to in-scope facts via qname lookup, writes "
      "one VerificationResult row per rule, and returns the results plus "
      "a status-keyed summary. Decoding mode, 6 patterns "
      "(EqualTo, RollUp, RollForward, SumEquals, Exists, CoExists)."
    ),
    command=cmd_evaluate_rules,
    request_model=EvaluateRulesRequest,
    result_type=EvaluateRulesResponse,
    error_map={ValueError: 422},
    requires_created_by=True,
  )
)

compute_metrics_op = _registrar.register(
  OperationSpec(
    name="compute-metrics",
    summary="Compute Metrics for a Metric Block",
    description=(
      "Resolves the Derive rules scoped to a metric block "
      "(block_type='metric'), binds each rule's operands to the entity's "
      "most recent persisted report facts at period_end, evaluates, and "
      "upserts the period's standing factset_type='metric' FactSet — one "
      "per (structure, entity, period_end), so successive runs accumulate "
      "the time series and re-running a period replaces its values. "
      "Metrics with missing operands or undefined ratios are skipped "
      "with a reason, never errored."
    ),
    command=cmd_compute_metrics,
    request_model=ComputeMetricsRequest,
    result_type=ComputeMetricsResponse,
    error_map={ValueError: 422},
    mark_stale_reason="metrics_computed",
    requires_created_by=True,
  )
)

assert_metrics_op = _registrar.register(
  OperationSpec(
    name="assert-metrics",
    summary="Assert Metrics for a Metric Block",
    description=(
      "Writes externally-observed metric values (usage counts, marketing "
      "numbers, hand-carried figures) into the period's standing "
      "factset_type='metric' FactSet with AssertedProvenance — the "
      "observation sibling of compute-metrics. One standing FactSet per "
      "(structure, entity, period_end); re-asserting a period replaces "
      "its facts. Structures carrying Derive rules are compute-owned and "
      "rejected: asserted and derived metric series keep disjoint "
      "structures. Observations must resolve to concepts on the "
      "structure's presentation catalog. Deterministic and non-AI — no "
      "credits consumed."
    ),
    command=cmd_assert_metrics,
    request_model=AssertMetricsRequest,
    result_type=AssertMetricsResponse,
    error_map={ValueError: 422},
    mark_stale_reason="metrics_asserted",
    requires_created_by=True,
  )
)

compute_forecast_op = _registrar.register(
  OperationSpec(
    name="compute-forecast",
    summary="Compute Forecast for a Forecast Block",
    description=(
      "Walks a forecast block's driver cascade month-by-month forward "
      "from its base period: lever-driven rs-driver Derive rules in "
      "dependency order, carry-forward for unmodeled income-statement "
      "lines, calc-DAG subtotals — upserting one scenario "
      "income-statement FactSet (plus a working-capital balance-sheet "
      "set) per forward month, all keyed by the block's scenario_id "
      "(NULL = actuals; scenario reads pass it as a filter). Re-running "
      "replaces each month's values. Rules with missing lever months or "
      "unbound operands are skipped with a reason (their targets fall "
      "back to carry-forward), never errored. Deterministic and non-AI "
      "— no credits consumed."
    ),
    command=cmd_compute_forecast,
    request_model=ComputeForecastRequest,
    result_type=ComputeForecastResponse,
    error_map={ValueError: 422},
    mark_stale_reason="forecast_computed",
    requires_created_by=True,
  )
)


# Hand-written (not registrar): needs the platform DB session for the
# Document lookup plus the trusted-path graph_id — the OperationSpec
# runner passes neither.
@router.post(
  "/bind-text-block",
  response_model=OperationEnvelope[BindTextBlockResponse],
  operation_id="bindTextBlock",
  summary="Bind Text Block",
  description=(
    "Bind a platform Document (markdown) — or one of its sections — to a "
    "disclosure element as a Nonnumeric text-block fact. The document "
    "stays the editable source of truth; the fact snapshots its text into "
    "a standing 'disclosure' FactSet with document provenance "
    "(document_id + section + content_hash). Re-binding the same element "
    "and period replaces the fact and refreshes the hash. Reports "
    "generated afterward snapshot the standing set, so filed reports are "
    "immutable against later document edits."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/bind-text-block",
  method="POST",
  business_event_type="ledger_bind_text_block",
)
async def bind_text_block_op(
  body: BindTextBlockRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="bind-text-block",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_bind_text_block(
          session,
          platform_db,
          graph_id,
          body,
          created_by=str(user.id),
        )
    except TextBlockDocumentNotFoundError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
      # SectionNotFound / TextBlockStructure / TextBlockElement / size cap
      raise HTTPException(status_code=422, detail=str(e))

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "text_block_bound"),
  )
