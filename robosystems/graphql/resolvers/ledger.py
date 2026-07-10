"""Ledger (roboledger) GraphQL resolvers.

The endpoint is graph-scoped at `/extensions/{graph_id}/graphql`, so:

1. Auth + per-graph access are validated by `get_context` before any
   resolver runs. Resolvers don't need to call `check_graph_access`.
2. `graph_id` lives on `info.context` — resolvers read it via the
   `require_graph_id(info)` helper instead of taking it as a query
   argument.
3. Each field opens `extensions_session(graph_id)` and delegates to
   `operations/roboledger/reads/*.py`. No business logic here — the
   ops layer is the single source of truth.
"""

from __future__ import annotations

from datetime import date
from typing import NoReturn

import strawberry
from sqlalchemy.exc import ProgrammingError
from strawberry.types import Info

from robosystems.graphql.context import GraphQLContext, require_graph_id
from robosystems.graphql.resolvers._common import (
  open_extensions_session as _open_session,
)
from robosystems.graphql.resolvers._common import (
  validate_pagination as _validate_pagination,
)
from robosystems.graphql.types.ledger import (
  AccountList,
  AccountRollups,
  AccountTree,
  AccountTreeNode,
  Agent,
  ClosingBookStructures,
  Element,
  ElementList,
  EventBlock,
  FiscalCalendar,
  LedgerEntity,
  LedgerSummary,
  LedgerTransactionDetail,
  LedgerTransactionList,
  MappedTrialBalance,
  MappingCoverage,
  MappingDetail,
  OpenBalanceAggregate,
  OpenBalanceByAgent,
  PeriodCloseStatus,
  PeriodDrafts,
  PublishListDetail,
  PublishListList,
  Report,
  ReportBundleDownload,
  ReportDownloadFormat,
  ReportList,
  Statement,
  StructureList,
  Taxonomy,
  TaxonomyList,
  TrialBalance,
  UnmappedElement,
)
from robosystems.graphql.types.report_package import ReportPackage
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.reads import (
  account_rollups as reads_account_rollups,
)
from robosystems.operations.roboledger.reads import (
  accounts as reads_accounts,
)
from robosystems.operations.roboledger.reads import (
  agent as reads_agent,
)
from robosystems.operations.roboledger.reads import (
  ar_ap as reads_ar_ap,
)
from robosystems.operations.roboledger.reads import (
  closing_book as reads_closing_book,
)
from robosystems.operations.roboledger.reads import (
  entity as reads_entity,
)
from robosystems.operations.roboledger.reads import (
  event_block as reads_event_block,
)
from robosystems.operations.roboledger.reads import (
  fiscal_calendar as reads_fiscal_calendar,
)
from robosystems.operations.roboledger.reads import (
  period_drafts as reads_period_drafts,
)
from robosystems.operations.roboledger.reads import (
  publish_lists as reads_publish_lists,
)
from robosystems.operations.roboledger.reads import (
  reports as reads_reports,
)
from robosystems.operations.roboledger.reads import (
  schedules as reads_schedules,
)
from robosystems.operations.roboledger.reads import (
  summary as reads_summary,
)
from robosystems.operations.roboledger.reads import (
  taxonomies as reads_taxonomies,
)
from robosystems.operations.roboledger.reads import (
  transactions as reads_transactions,
)
from robosystems.operations.roboledger.reads import (
  trial_balance as reads_trial_balance,
)
from robosystems.operations.roboledger.reports.network_picker import (
  load_primary_reporting_style,
)
from robosystems.operations.roboledger.schedules import ScheduleService

# Services are stateless and cheap to keep as module-level singletons —
# matches the router-level `_svc` pattern already in place.
_fiscal_svc = FiscalCalendarService()
_schedule_svc = ScheduleService()


def _raise_ledger_not_initialized() -> NoReturn:
  """Raise a typed GraphQL error for an uninitialized ledger.

  Replaces the previous "swallow ValueError/ProgrammingError → return
  null" pattern. The retired REST endpoints raised HTTP 404 with
  `"Ledger not initialized. Connect a data source first."`; the
  GraphQL equivalent is a structured error with code
  `LEDGER_NOT_INITIALIZED`. Frontends can branch on the code; agents
  see a clear failure instead of an empty result.

  Uses `raise ... from None` so that when called from inside an
  `except (ValueError, ProgrammingError):` block, Python doesn't set
  `__context__` on the new exception. Strawberry's error serializer
  can otherwise leak the raw `ProgrammingError` (which contains schema
  and table names from the failing SQL statement) through the
  `extensions` field of the GraphQL error response — a tenant-isolation
  leak.
  """
  raise strawberry.exceptions.StrawberryGraphQLError(
    message="Ledger not initialized. Connect a data source first.",
    extensions={"code": "LEDGER_NOT_INITIALIZED"},
  ) from None


@strawberry.type
class LedgerQuery:
  """Read-only fields for the roboledger domain.

  Composed into the top-level `Query` root via multiple inheritance in
  `graphql/schema.py`. Every field calls into `operations/roboledger/reads/*`.
  """

  # ── Entity ──────────────────────────────────────────────────────────────

  @strawberry.field
  def entity(self, info: Info[GraphQLContext, None]) -> LedgerEntity | None:
    """Return the parent ledger entity (company) for a graph."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_entity.get_parent_entity(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return LedgerEntity.from_pydantic(response)

  @strawberry.field
  def entities(
    self,
    info: Info[GraphQLContext, None],
    source: str | None = None,
  ) -> list[LedgerEntity]:
    """List entities for a graph, optionally filtered by source."""
    try:
      with _open_session(info, "roboledger") as session:
        responses = reads_entity.list_entities(session, source=source)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return [LedgerEntity.from_pydantic(r) for r in responses]

  # ── Agents ───────────────────────────────────────────────────────────────

  @strawberry.field
  def agent(
    self,
    info: Info[GraphQLContext, None],
    id: str,
  ) -> Agent | None:
    """Fetch a single counterparty agent by id."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_agent.get_agent(session, id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return Agent.from_pydantic(response)

  @strawberry.field
  def agents(
    self,
    info: Info[GraphQLContext, None],
    agent_type: str | None = None,
    source: str | None = None,
    is_active: bool | None = True,
    limit: int = 50,
    offset: int = 0,
  ) -> list[Agent]:
    """List counterparty agents with optional filters."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboledger") as session:
        responses = reads_agent.list_agents(
          session,
          agent_type=agent_type,
          source=source,
          is_active=is_active,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return [Agent.from_pydantic(r) for r in responses]

  # ── AR / AP open balances ───────────────────────────────────────────────

  @strawberry.field
  def open_receivables(self, info: Info[GraphQLContext, None]) -> OpenBalanceAggregate:
    """Graph-wide open AR — total + counterparty count + open invoice count.

    Derived from the event duality chain: sum of unsettled
    ``invoice_issued`` / ``sales_receipt_recorded`` amounts minus
    discharges pointed at them.
    """
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_ar_ap.compute_open_receivables(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return OpenBalanceAggregate.from_pydantic(response)

  @strawberry.field
  def open_payables(self, info: Info[GraphQLContext, None]) -> OpenBalanceAggregate:
    """Graph-wide open AP — symmetric to ``open_receivables``."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_ar_ap.compute_open_payables(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return OpenBalanceAggregate.from_pydantic(response)

  @strawberry.field
  def open_receivables_by_agent(
    self, info: Info[GraphQLContext, None]
  ) -> list[OpenBalanceByAgent]:
    """Per-counterparty open AR rows, ordered by absolute balance descending."""
    try:
      with _open_session(info, "roboledger") as session:
        responses = reads_ar_ap.list_open_receivables_by_agent(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return [OpenBalanceByAgent.from_pydantic(r) for r in responses]

  @strawberry.field
  def open_payables_by_agent(
    self, info: Info[GraphQLContext, None]
  ) -> list[OpenBalanceByAgent]:
    """Per-counterparty open AP rows, ordered by absolute balance descending."""
    try:
      with _open_session(info, "roboledger") as session:
        responses = reads_ar_ap.list_open_payables_by_agent(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return [OpenBalanceByAgent.from_pydantic(r) for r in responses]

  # ── Event blocks (inbox) ────────────────────────────────────────────────

  @strawberry.field
  def event_block(
    self,
    info: Info[GraphQLContext, None],
    id: str,
  ) -> EventBlock | None:
    """Fetch a single event block by id."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_event_block.get_event_block(session, id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return EventBlock.from_pydantic(response)

  @strawberry.field
  def event_blocks(
    self,
    info: Info[GraphQLContext, None],
    event_type: str | None = None,
    event_category: str | None = None,
    status: str | None = None,
    agent_id: str | None = None,
    source: str | None = None,
    limit: int = 50,
    offset: int = 0,
  ) -> list[EventBlock]:
    """List event blocks with optional filters.

    Drives the close-workspace inbox: defaulting to recent events first
    (``occurred_at DESC``) with filters for ``status`` (``captured`` for
    inbox queue, ``committed`` for the audit-trail view), ``source``
    (``quickbooks`` / ``schedule`` / ``manual``), and ``event_type``.
    """
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboledger") as session:
        responses = reads_event_block.list_event_blocks(
          session,
          event_type=event_type,
          event_category=event_category,
          status=status,
          agent_id=agent_id,
          source=source,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return [EventBlock.from_pydantic(r) for r in responses]

  # ── Summary ─────────────────────────────────────────────────────────────

  @strawberry.field
  def summary(self, info: Info[GraphQLContext, None]) -> LedgerSummary | None:
    """Ledger counts + date range + connection metadata.

    Wire-compatible with the retired `GET /v1/ledger/{g}/summary` REST
    endpoint — opens both an extensions session (for counts/dates) and
    a platform DB session (for QB connection metadata) and merges them
    into a single response. Connection-DB failures degrade gracefully
    (zero count, null timestamp) rather than aborting the whole field.
    """
    import logging

    from sqlalchemy import func, select

    from robosystems.db.platform import platform_session
    from robosystems.models.api.extensions.summary import LedgerSummaryResponse
    from robosystems.models.core.connection.connection import Connection

    graph_id = require_graph_id(info)

    try:
      with _open_session(info, "roboledger") as session:
        counts = reads_summary.get_ledger_counts(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()

    # Connection metadata from platform DB. Failures are non-fatal.
    connection_count = 0
    last_sync_at = None
    try:
      with platform_session() as platform_db:
        conn_result = platform_db.execute(
          select(func.count(), func.max(Connection.last_sync)).where(
            Connection.graph_id == graph_id
          )
        ).one()
        connection_count = conn_result[0] or 0
        last_sync_at = conn_result[1]
    except Exception:
      logging.getLogger(__name__).warning(
        "Failed to fetch connection metadata for %s",
        graph_id,
        exc_info=True,
      )

    response = LedgerSummaryResponse(
      graph_id=graph_id,
      account_count=counts.account_count,
      transaction_count=counts.transaction_count,
      entry_count=counts.entry_count,
      line_item_count=counts.line_item_count,
      earliest_transaction_date=counts.earliest_transaction_date,
      latest_transaction_date=counts.latest_transaction_date,
      connection_count=connection_count,
      last_sync_at=last_sync_at,
    )
    return LedgerSummary.from_pydantic(response)

  # ── Accounts ────────────────────────────────────────────────────────────

  @strawberry.field
  def accounts(
    self,
    info: Info[GraphQLContext, None],
    classification: str | None = None,
    is_active: bool | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> AccountList | None:
    """Paginated Chart of Accounts listing."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_accounts.list_accounts(
          session,
          trait=classification,
          is_active=is_active,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return AccountList.from_pydantic(response)

  @strawberry.field
  def account_tree(
    self,
    info: Info[GraphQLContext, None],
    include_inactive: bool = False,
  ) -> AccountTree | None:
    """Chart of Accounts as a recursive tree.

    ``include_inactive`` defaults to ``False`` so deleted source-system
    accounts (still kept in OLTP for historical journal-line FK integrity)
    don't clutter the standard CoA view. Set ``True`` for admin / cleanup
    contexts.
    """
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_accounts.get_account_tree(
          session, include_inactive=include_inactive
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return AccountTree(
      roots=[AccountTreeNode.from_pydantic(n) for n in response.roots],
      total_accounts=response.total_accounts,
    )

  @strawberry.field
  def account_rollups(
    self,
    info: Info[GraphQLContext, None],
    mapping_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
  ) -> AccountRollups | None:
    """CoA accounts grouped by reporting element with balances."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_account_rollups.get_account_rollups(
          session,
          mapping_id=mapping_id,
          start_date=start_date,
          end_date=end_date,
        )
    except reads_account_rollups.MappingNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return AccountRollups.from_pydantic(response)

  # ── Trial balance ───────────────────────────────────────────────────────

  @strawberry.field
  def trial_balance(
    self,
    info: Info[GraphQLContext, None],
    start_date: date | None = None,
    end_date: date | None = None,
  ) -> TrialBalance | None:
    """Trial balance for posted entries in a date range."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_trial_balance.get_trial_balance(
          session, start_date=start_date, end_date=end_date
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return TrialBalance.from_pydantic(response)

  # ── Transactions ────────────────────────────────────────────────────────

  @strawberry.field
  def transactions(
    self,
    info: Info[GraphQLContext, None],
    type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> LedgerTransactionList | None:
    """Paginated list of transactions."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_transactions.list_transactions(
          session,
          type=type,
          start_date=start_date,
          end_date=end_date,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return LedgerTransactionList.from_pydantic(response)

  @strawberry.field
  def transaction(
    self,
    info: Info[GraphQLContext, None],
    transaction_id: str,
  ) -> LedgerTransactionDetail | None:
    """Single transaction with all entries and line items."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_transactions.get_transaction(session, transaction_id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return LedgerTransactionDetail.from_pydantic(response)

  # ── Taxonomies ──────────────────────────────────────────────────────────

  @strawberry.field
  def taxonomies(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_type: str | None = None,
  ) -> TaxonomyList | None:
    """List all active taxonomies, optionally filtered by type."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.list_taxonomies(
          session, taxonomy_type=taxonomy_type
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return TaxonomyList.from_pydantic(response)

  @strawberry.field
  def reporting_taxonomy(self, info: Info[GraphQLContext, None]) -> Taxonomy | None:
    """The locked US GAAP reporting taxonomy, or null."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.get_reporting_taxonomy(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return Taxonomy.from_pydantic(response)

  # ── Elements ────────────────────────────────────────────────────────────

  @strawberry.field
  def elements(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_id: str | None = None,
    source: str | None = None,
    classification: str | None = None,
    is_abstract: bool | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> ElementList | None:
    """Paginated list of taxonomy elements."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.list_elements(
          session,
          taxonomy_id=taxonomy_id,
          source=source,
          trait=classification,
          is_abstract=is_abstract,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return ElementList.from_pydantic(response)

  @strawberry.field
  def mapping_candidates(
    self,
    info: Info[GraphQLContext, None],
    classification: str,
  ) -> list[Element]:
    """rs-gaap concepts a CoA element of the given EFS ``classification``
    (asset / liability / equity / revenue / expense / gain / loss) may map
    to — limited to concepts that actually render under the active Reporting
    Style (falling back to the rs-gaap-presentation set when no Style is
    seeded), with statement-level subtotals excluded. This is the same
    candidate set the MappingOperator picks from, so the mapping UI never
    offers a target that would land a fact on an unreachable branch.
    """
    # Narrow candidates to what the entity's Reporting Style actually renders,
    # keeping the picker in sync with the renderer. Resolved from the same
    # extensions session as the query. When the tenant has no entity yet,
    # suggest_mapping_candidates falls back to the rs-gaap-presentation set —
    # still far narrower than the full vocab.
    try:
      with _open_session(info, "roboledger") as session:
        try:
          reporting_style_id = load_primary_reporting_style(session)
        except LookupError:
          reporting_style_id = None
        rows = reads_taxonomies.suggest_mapping_candidates(
          session,
          trait=classification,
          reporting_style_id=reporting_style_id,
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return [Element.from_pydantic(r) for r in rows]

  @strawberry.field
  def unmapped_elements(
    self,
    info: Info[GraphQLContext, None],
    mapping_id: str | None = None,
  ) -> list[UnmappedElement]:
    """CoA elements not yet mapped to the reporting taxonomy."""
    try:
      with _open_session(info, "roboledger") as session:
        responses = reads_taxonomies.list_unmapped_elements(
          session, mapping_id=mapping_id
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return [UnmappedElement.from_pydantic(r) for r in responses]

  # ── Structures / mappings ──────────────────────────────────────────────

  @strawberry.field
  def structures(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_id: str | None = None,
    block_type: str | None = None,
  ) -> StructureList | None:
    """List active structures."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.list_structures(
          session, taxonomy_id=taxonomy_id, block_type=block_type
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return StructureList.from_pydantic(response)

  @strawberry.field
  def mappings(self, info: Info[GraphQLContext, None]) -> StructureList | None:
    """List all active mapping structures."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.list_mappings(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return StructureList.from_pydantic(response)

  @strawberry.field
  def mapping(
    self,
    info: Info[GraphQLContext, None],
    mapping_id: str,
  ) -> MappingDetail | None:
    """Single mapping structure with all associations."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.get_mapping_detail(session, mapping_id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return MappingDetail.from_pydantic(response)

  @strawberry.field
  def mapping_coverage(
    self,
    info: Info[GraphQLContext, None],
    mapping_id: str,
  ) -> MappingCoverage | None:
    """Coverage stats for a mapping."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.get_mapping_coverage(session, mapping_id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return MappingCoverage.from_pydantic(response)

  @strawberry.field
  def mapped_trial_balance(
    self,
    info: Info[GraphQLContext, None],
    mapping_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
  ) -> MappedTrialBalance | None:
    """Trial balance rolled up to reporting concepts via mapping associations."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_taxonomies.get_mapped_trial_balance(
          session, mapping_id, start_date=start_date, end_date=end_date
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return MappedTrialBalance.from_pydantic(response)

  # ── Period close ────────────────────────────────────────────────────────

  @strawberry.field
  def period_close_status(
    self,
    info: Info[GraphQLContext, None],
    period_start: date,
    period_end: date,
  ) -> PeriodCloseStatus | None:
    """Close status for all schedules in a fiscal period."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_schedules.get_period_close_status(
          session, _schedule_svc, period_start, period_end
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return PeriodCloseStatus.from_pydantic(response)

  # ── Fiscal calendar ─────────────────────────────────────────────────────

  @strawberry.field
  def fiscal_calendar(self, info: Info[GraphQLContext, None]) -> FiscalCalendar | None:
    """Current fiscal calendar state — pointers, gap, closeable status."""
    from robosystems.db.platform import platform_session

    graph_id = require_graph_id(info)

    try:
      with _open_session(info, "roboledger") as session:
        calendar = _fiscal_svc.get(session, graph_id)
        if calendar is None:
          return None
        with platform_session() as platform_db:
          has_sync, last_sync_at = reads_fiscal_calendar.qb_sync_state(
            platform_db, graph_id
          )
        response = reads_fiscal_calendar.build_fiscal_calendar_response(
          session,
          graph_id,
          calendar,
          has_sync,
          last_sync_at,
          _fiscal_svc,
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return FiscalCalendar.from_pydantic(response)

  # ── Period drafts (close review) ────────────────────────────────────────

  @strawberry.field
  def period_drafts(
    self,
    info: Info[GraphQLContext, None],
    period: str,
  ) -> PeriodDrafts | None:
    """All draft entries for a fiscal period, ready for review before close.

    The close-review *outbox*: each draft is annotated with its QB
    write-back disposition (``willPublishToQb``) and the response carries
    a publish summary — which drafts `close-period` will push to
    QuickBooks vs. post locally only.
    """
    from robosystems.db.platform import platform_session
    from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
      resolve_writeback_connection,
    )

    graph_id = require_graph_id(info)
    try:
      with platform_session() as platform_db:
        writeback = resolve_writeback_connection(platform_db, graph_id)
      with _open_session(info, "roboledger") as session:
        response = reads_period_drafts.list_period_drafts(
          session, period, writeback=writeback
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return PeriodDrafts.from_pydantic(response)

  # ── Closing book ────────────────────────────────────────────────────────

  @strawberry.field
  def closing_book_structures(
    self, info: Info[GraphQLContext, None]
  ) -> ClosingBookStructures | None:
    """Closing book sidebar navigation (statements, schedules, rollups, etc.)."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_closing_book.get_closing_book_structures(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return ClosingBookStructures.from_pydantic(response)

  # ── Reports ─────────────────────────────────────────────────────────────

  @strawberry.field
  def reports(self, info: Info[GraphQLContext, None]) -> ReportList | None:
    """List all report definitions for this graph."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_reports.list_reports(session)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return ReportList.from_pydantic(response)

  @strawberry.field
  def report(
    self,
    info: Info[GraphQLContext, None],
    report_id: str,
  ) -> Report | None:
    """Single report definition with structures + entity name."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_reports.get_report(session, report_id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return Report.from_pydantic(response)

  @strawberry.field
  def report_package(
    self,
    info: Info[GraphQLContext, None],
    report_id: str,
  ) -> ReportPackage | None:
    """Rehydrate a Report as a package — Report metadata + N rendered
    Information Block envelopes (one per attached FactSet). Drives the
    ``/reports/[id]`` package viewer; replaces the per-statement
    ``getStatement`` round-trip path.
    """
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_reports.get_report_package(session, report_id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return ReportPackage.from_pydantic(response)

  @strawberry.field
  def report_download_url(
    self,
    info: Info[GraphQLContext, None],
    report_id: str,
    format: ReportDownloadFormat = ReportDownloadFormat.JSONLD,
    expires_in: int = reads_reports.PRESIGN_DEFAULT_SECONDS,
  ) -> ReportBundleDownload | None:
    """Presigned download URL for a published Report's serialization bundle.

    Replaces the retired `GET .../reports/{id}/download` REST resource —
    a download is a read of stored state, so it lives on the read
    surface. Every flavor resolves to a short-lived presigned S3 URL the
    client follows directly; JSON-LD is stamped at publish time, XBRL is
    materialized + cached on first request. Returns null when the report
    doesn't exist; raises a typed error when it exists but has no
    published bundle yet.
    """
    if not (60 <= expires_in <= reads_reports.PRESIGN_MAX_SECONDS):
      raise strawberry.exceptions.StrawberryGraphQLError(
        message=(
          f"expiresIn must be between 60 and "
          f"{reads_reports.PRESIGN_MAX_SECONDS} seconds"
        ),
        extensions={"code": "INVALID_EXPIRES_IN"},
      )
    graph_id = require_graph_id(info)
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_reports.get_report_download_url(
          session,
          graph_id,
          report_id,
          flavor=format.value,
          expires_in=expires_in,
        )
    except reads_reports.ReportBundleNotAvailableError as exc:
      raise strawberry.exceptions.StrawberryGraphQLError(
        message=str(exc),
        extensions={"code": "REPORT_BUNDLE_NOT_AVAILABLE"},
      )
    except reads_reports.BundleSigningError as exc:
      raise strawberry.exceptions.StrawberryGraphQLError(
        message=str(exc),
        extensions={"code": "REPORT_BUNDLE_SIGNING_FAILED"},
      )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return ReportBundleDownload.from_pydantic(response)

  @strawberry.field
  def statement(
    self,
    info: Info[GraphQLContext, None],
    report_id: str,
    block_type: str,
  ) -> Statement | None:
    """Rendered financial statement for a report + block_type."""
    graph_id = require_graph_id(info)
    try:
      with _open_session(info, "roboledger") as session:
        # Reporting Style now lives on the entity — get_statement resolves
        # the primary entity's Style from this same session (no platform hop).
        response = reads_reports.get_statement(
          session,
          graph_id,
          report_id,
          block_type,
        )
    except reads_reports.StatementStructureNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return Statement.from_pydantic(response)

  # ── Publish lists ───────────────────────────────────────────────────────

  @strawberry.field
  def publish_lists(
    self,
    info: Info[GraphQLContext, None],
    limit: int = 100,
    offset: int = 0,
  ) -> PublishListList | None:
    """Paginated list of publish lists for this graph."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_publish_lists.list_publish_lists(
          session, limit=limit, offset=offset
        )
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    return PublishListList.from_pydantic(response)

  @strawberry.field
  def publish_list(
    self,
    info: Info[GraphQLContext, None],
    list_id: str,
  ) -> PublishListDetail | None:
    """Single publish list with enriched members, or null."""
    try:
      with _open_session(info, "roboledger") as session:
        response = reads_publish_lists.get_publish_list(session, list_id)
    except (ValueError, ProgrammingError):
      _raise_ledger_not_initialized()
    if response is None:
      return None
    return PublishListDetail.from_pydantic(response)
