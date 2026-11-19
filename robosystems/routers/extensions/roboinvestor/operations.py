"""RoboInvestor operation routes, all declared on one `OperationRegistrar`
(which also generates the MCP tools).

Portfolio and position writes go only through the Portfolio Block envelope
ops; securities are master-data CRUD.
"""

from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Depends, HTTPException

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.extensions import (
  OperationRegistrar,
  OperationSpec,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  IdempotencyKeyConflictError,
  OperationContext,
  OperationEnvelope,
  execute_operation,
  fingerprint_body,
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.investor import (
  CreateIssuerRequest,
  CreatePortfolioBlockRequest,
  CreateSecurityRequest,
  DeletePortfolioBlockOperation,
  DeletePortfolioBlockResponse,
  DeleteResult,
  DeleteSecurityOperation,
  IngestReportRequest,
  IngestReportResponse,
  IssuerResponse,
  PortfolioBlockEnvelope,
  SecurityResponse,
  UpdatePortfolioBlockOperation,
  UpdateSecurityOperation,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  ActivePositionsRequireConfirmationError,
  DuplicateActivePositionError,
  PortfolioNotFoundError,
  PositionNotFoundError,
  PositionPortfolioMismatchError,
  SecurityNotFoundError,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  create_portfolio_block as cmd_create_portfolio_block,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  delete_portfolio_block as cmd_delete_portfolio_block,
)
from robosystems.operations.roboinvestor.commands.portfolio_block import (
  update_portfolio_block as cmd_update_portfolio_block,
)
from robosystems.operations.roboinvestor.commands.reports import (
  IngestRequestError,
  IssuerAlreadyExistsError,
)
from robosystems.operations.roboinvestor.commands.reports import (
  create_issuer as cmd_create_issuer,
)
from robosystems.operations.roboinvestor.commands.reports import (
  ingest_report as cmd_ingest_report,
)
from robosystems.operations.roboinvestor.commands.securities import (
  EntityNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  SecurityNotFoundError as SecurityMasterNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  create_security as cmd_create_security,
)
from robosystems.operations.roboinvestor.commands.securities import (
  soft_delete_security as cmd_soft_delete_security,
)
from robosystems.operations.roboinvestor.commands.securities import (
  update_security as cmd_update_security,
)
from robosystems.operations.roboledger.commands.report_import import (
  ReportImportEntityNotFoundError,
  ReportImportRefusedError,
)
from robosystems.operations.taxonomy_block.validators import (
  TaxonomyBlockValidationError,
)

router = APIRouter()

_OP_TAG = "RoboInvestor"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)


def _not_initialized_404() -> HTTPException:
  """Registrar surfaces this when the extensions schema hasn't materialized."""
  return HTTPException(status_code=404, detail="Investor module not initialized.")


def _ctx(
  *,
  graph_id: str,
  user_id: str,
  op: str,
  idempotency_key: str | None,
  body: object,
) -> OperationContext:
  return OperationContext(
    domain="roboinvestor",
    operation_name=op,
    graph_id=graph_id,
    user_id=user_id,
    idempotency_key=idempotency_key,
    body_fingerprint=fingerprint_body(body),
  )


async def _dispatch(
  ctx: OperationContext,
  runner,
  cache: IdempotencyCache,
  on_fresh_success=None,
) -> OperationEnvelope:
  """Run `execute_operation` and translate idempotency conflicts to 409."""
  try:
    return await execute_operation(
      ctx, runner, idempotency_cache=cache, on_fresh_success=on_fresh_success
    )
  except IdempotencyKeyConflictError as exc:
    raise HTTPException(status_code=409, detail=str(exc))


_registrar = OperationRegistrar(
  router=router,
  domain="roboinvestor",
  tag=_OP_TAG,
  rate_limit_dep=_RATE_LIMIT,
  ctx_builder=_ctx,
  dispatcher=_dispatch,
  session_factory=extensions_session,
  schema_missing_404=_not_initialized_404,
  user_dep=get_current_user_with_graph,
  graph_id_pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  extension="roboinvestor",
)


# ── Portfolio Block ───────────────────────────────────────────────────────

create_portfolio_block_op = _registrar.register(
  OperationSpec(
    name="create-portfolio-block",
    summary="Create Portfolio Block",
    description=(
      "Create a portfolio with optional initial positions in a single "
      "atomic envelope. Each position references an existing security; "
      "this operation never mints securities (use `create-security`). "
      "Whole envelope validates before any write."
    ),
    command=cmd_create_portfolio_block,
    request_model=CreatePortfolioBlockRequest,
    result_type=PortfolioBlockEnvelope,
    error_map={
      SecurityNotFoundError: (404, lambda e: f"Security not found: {e}"),
      DuplicateActivePositionError: (409, str),
    },
    mark_stale_reason="portfolio_block_created",
  )
)

update_portfolio_block_op = _registrar.register(
  OperationSpec(
    name="update-portfolio-block",
    summary="Update Portfolio Block",
    description=(
      "Patch portfolio fields and apply position deltas (`add` / "
      "`update` / `dispose`) atomically. Partial failures roll back."
    ),
    command=cmd_update_portfolio_block,
    request_model=UpdatePortfolioBlockOperation,
    result_type=PortfolioBlockEnvelope,
    error_map={
      PortfolioNotFoundError: (404, lambda _e: "Portfolio not found."),
      PositionNotFoundError: (404, lambda e: f"Position not found: {e}"),
      PositionPortfolioMismatchError: (409, str),
      SecurityNotFoundError: (404, lambda e: f"Security not found: {e}"),
      DuplicateActivePositionError: (409, str),
    },
    mark_stale_reason="portfolio_block_updated",
  )
)

delete_portfolio_block_op = _registrar.register(
  OperationSpec(
    name="delete-portfolio-block",
    summary="Delete Portfolio Block",
    description=(
      "Cascade-delete the portfolio plus all of its positions. When "
      "active positions exist, the request must include "
      "`confirm_active_positions: true` — safety belt to prevent "
      "accidental cascade. Disposed-only portfolios delete without "
      "the flag."
    ),
    command=cmd_delete_portfolio_block,
    request_model=DeletePortfolioBlockOperation,
    result_type=DeletePortfolioBlockResponse,
    error_map={
      PortfolioNotFoundError: (404, lambda _e: "Portfolio not found."),
      ActivePositionsRequireConfirmationError: (
        409,
        lambda e: (
          f"Portfolio has "
          f"{cast(ActivePositionsRequireConfirmationError, e).active_count} "
          "active position(s); set confirm_active_positions=true to cascade-delete."
        ),
      ),
    },
    mark_stale_reason="portfolio_block_deleted",
    requires_created_by=False,
  )
)


# ── Security (Master Data CRUD) ───────────────────────────────────────────

create_security_op = _registrar.register(
  OperationSpec(
    name="create-security",
    summary="Create Security",
    description=(
      "Register a security (common stock, preferred stock, warrant, "
      "convertible note, etc.) in this graph's registry. Its `entity_id` "
      "is the issuer, not the holder; a portfolio's `entity_id` is the "
      "owner. Optionally cross-links to the issuer's own graph via "
      "`source_graph_id` for mutual-handshake attribution."
    ),
    command=cmd_create_security,
    request_model=CreateSecurityRequest,
    result_type=SecurityResponse,
    error_map={EntityNotFoundError: (404, lambda _e: "Entity not found.")},
    mark_stale_reason="security_created",
  )
)

update_security_op = _registrar.register(
  OperationSpec(
    name="update-security",
    summary="Update Security",
    description=(
      "Update mutable fields on a security (name, type, subtype, terms, "
      "share counts, entity linkage). Unset fields are ignored."
    ),
    command=cmd_update_security,
    request_model=UpdateSecurityOperation,
    result_type=SecurityResponse,
    error_map={SecurityMasterNotFoundError: (404, lambda _e: "Security not found.")},
    mark_stale_reason="security_updated",
    requires_created_by=False,
  )
)

delete_security_op = _registrar.register(
  OperationSpec(
    name="delete-security",
    summary="Delete Security",
    description=(
      "Soft-delete the security (`is_active=false`). Historical positions "
      "referencing it remain valid."
    ),
    command=cmd_soft_delete_security,
    request_model=DeleteSecurityOperation,
    result_type=DeleteResult,
    error_map={SecurityMasterNotFoundError: (404, lambda _e: "Security not found.")},
    mark_stale_reason="security_deleted",
    requires_created_by=False,
  )
)


# ── Issuers and received reports ──────────────────────────────────────────


def _amount(value: object) -> str:
  if not isinstance(value, (int, float)):
    return str(value)
  return f"{value:,.0f}" if float(value).is_integer() else f"{value:,.2f}"


def _reason(problem: dict) -> str:
  """One refusal reason as a sentence, with its amounts written out."""
  if problem.get("code") == "does_not_foot":
    return (
      f"{problem.get('concept')} is reported as {_amount(problem.get('reported'))} "
      f"and its terms sum to {_amount(problem.get('computed'))}."
    )
  if problem.get("code") == "does_not_balance":
    return (
      f"Total assets are {_amount(problem.get('assets'))} and total liabilities "
      f"and equity are {_amount(problem.get('liabilities_and_equity'))}."
    )
  return str(problem.get("message"))


def _refusal(exc: ReportImportRefusedError) -> str:
  reasons = " ".join(_reason(p) for p in exc.problems)
  return f"The report was not loaded. {reasons}"


create_issuer_op = _registrar.register(
  OperationSpec(
    name="create-issuer",
    summary="Create Issuer",
    description=(
      "Add a company the fund holds or follows that keeps its books "
      "elsewhere. The issuer is an entity in this graph: securities attach "
      "to it with `create-security`, and its reports with `ingest-report`. A "
      "company that shares a report in from its own graph gets its entity "
      "from the share, so it needs no issuer created here."
    ),
    command=cmd_create_issuer,
    request_model=CreateIssuerRequest,
    result_type=IssuerResponse,
    error_map={IssuerAlreadyExistsError: (409, str)},
    mark_stale_reason="issuer_created",
  )
)

ingest_report_op = _registrar.register(
  OperationSpec(
    name="ingest-report",
    summary="Ingest Report",
    description=(
      "Load a report a company sent onto its entity. Give the report as "
      "`holon` (the text of a `.holon.jsonld` file) or as `report` (rows read "
      "from a PDF or a workbook: concepts, sections and values with the page "
      "or cell each came from), which is assembled into a holon here. The "
      "holon is checked before anything is written: every value resolves, "
      "duplicate values agree, every total foots to its terms, and total "
      "assets equal total liabilities and equity. A report that fails is "
      "refused with each reason and nothing is loaded. One that passes is "
      "stored as the record of what was received, and its facts become "
      "queryable from every holding in the company. A second ingest for the "
      "same entity, period end and kind replaces the first. Set `dry_run` to "
      "run the checks and see what would load without writing."
    ),
    command=cmd_ingest_report,
    request_model=IngestReportRequest,
    result_type=IngestReportResponse,
    requires_graph_id=True,
    error_map={
      IngestRequestError: (422, str),
      ReportImportEntityNotFoundError: (404, str),
      ReportImportRefusedError: (422, _refusal),
      TaxonomyBlockValidationError: (422, str),
    },
    mark_stale_reason="report_ingested",
  )
)
