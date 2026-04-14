"""Ledger (roboledger) GraphQL resolvers.

Every field is a thin wrapper that:
1. Authenticates the user and checks graph access
2. Opens `extensions_session(graph_id)`
3. Delegates to `operations/roboledger/reads/*.py`

No business logic here — the ops layer is the single source of truth.
"""

from __future__ import annotations

from datetime import date

import strawberry
from sqlalchemy.exc import ProgrammingError
from strawberry.types import Info

from robosystems.graphql.auth import check_graph_access
from robosystems.graphql.context import GraphQLContext, require_user
from robosystems.graphql.types.ledger import (
  AccountList,
  AccountRollups,
  AccountTree,
  AccountTreeNode,
  ClosingBookStructures,
  ElementList,
  FiscalCalendar,
  LedgerEntity,
  LedgerSummary,
  LedgerTransactionDetail,
  LedgerTransactionList,
  MappingCoverage,
  MappingDetail,
  PeriodCloseStatus,
  PeriodDrafts,
  PublishListDetail,
  PublishListList,
  Report,
  ReportList,
  ScheduleFacts,
  ScheduleList,
  Statement,
  StructureList,
  Taxonomy,
  TaxonomyList,
  TrialBalance,
  UnmappedElement,
)
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.reads import (
  account_rollups as reads_account_rollups,
)
from robosystems.operations.roboledger.reads import (
  accounts as reads_accounts,
)
from robosystems.operations.roboledger.reads import (
  closing_book as reads_closing_book,
)
from robosystems.operations.roboledger.reads import (
  entity as reads_entity,
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
from robosystems.operations.roboledger.schedules import ScheduleService

# Services are stateless and cheap to keep as module-level singletons —
# matches the router-level `_svc` pattern already in place.
_fiscal_svc = FiscalCalendarService()
_schedule_svc = ScheduleService()


def _open_session(info: Info[GraphQLContext, None], graph_id: str):
  """Shared auth + session-open prelude for every ledger resolver."""
  user = require_user(info)
  check_graph_access(user, graph_id)
  # Local import keeps this module importable without a running extensions DB.
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)


@strawberry.type
class LedgerQuery:
  """Read-only fields for the roboledger domain.

  Composed into the top-level `Query` root via multiple inheritance in
  `graphql/schema.py`. Every field calls into `operations/roboledger/reads/*`.
  """

  # ── Entity ──────────────────────────────────────────────────────────────

  @strawberry.field
  def entity(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> LedgerEntity | None:
    """Return the parent ledger entity (company) for a graph."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_entity.get_parent_entity(session)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return LedgerEntity.from_pydantic(response)

  @strawberry.field
  def entities(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    source: str | None = None,
  ) -> list[LedgerEntity]:
    """List entities for a graph, optionally filtered by source."""
    try:
      with _open_session(info, str(graph_id)) as session:
        responses = reads_entity.list_entities(session, source=source)
    except (ValueError, ProgrammingError):
      return []
    return [LedgerEntity.from_pydantic(r) for r in responses]

  # ── Summary ─────────────────────────────────────────────────────────────

  @strawberry.field
  def summary(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> LedgerSummary | None:
    """Ledger counts + date range + connection metadata."""
    from robosystems.models.api.extensions.summary import LedgerSummaryResponse

    try:
      with _open_session(info, str(graph_id)) as session:
        counts = reads_summary.get_ledger_counts(session)
    except (ValueError, ProgrammingError):
      return None

    # Note: GraphQL does not return connection metadata from platform DB;
    # the REST endpoint merges it in, but for the GraphQL read surface we
    # keep ops-only state and let the frontend fetch connection stats
    # separately if it needs them. Matches the REST schema shape but
    # `connection_count` / `last_sync_at` default to 0 / null here.
    response = LedgerSummaryResponse(
      graph_id=str(graph_id),
      account_count=counts.account_count,
      transaction_count=counts.transaction_count,
      entry_count=counts.entry_count,
      line_item_count=counts.line_item_count,
      earliest_transaction_date=counts.earliest_transaction_date,
      latest_transaction_date=counts.latest_transaction_date,
    )
    return LedgerSummary.from_pydantic(response)

  # ── Accounts ────────────────────────────────────────────────────────────

  @strawberry.field
  def accounts(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    classification: str | None = None,
    is_active: bool | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> AccountList | None:
    """Paginated Chart of Accounts listing."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_accounts.list_accounts(
          session,
          classification=classification,
          is_active=is_active,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      return None
    return AccountList.from_pydantic(response)

  @strawberry.field
  def account_tree(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> AccountTree | None:
    """Chart of Accounts as a recursive tree."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_accounts.get_account_tree(session)
    except (ValueError, ProgrammingError):
      return None
    return AccountTree(
      roots=[AccountTreeNode.from_pydantic(n) for n in response.roots],
      total_accounts=response.total_accounts,
    )

  @strawberry.field
  def account_rollups(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    mapping_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
  ) -> AccountRollups | None:
    """CoA accounts grouped by reporting element with balances."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_account_rollups.get_account_rollups(
          session,
          mapping_id=mapping_id,
          start_date=start_date,
          end_date=end_date,
        )
    except reads_account_rollups.MappingNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      return None
    return AccountRollups.from_pydantic(response)

  # ── Trial balance ───────────────────────────────────────────────────────

  @strawberry.field
  def trial_balance(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    start_date: date | None = None,
    end_date: date | None = None,
  ) -> TrialBalance | None:
    """Trial balance for posted entries in a date range."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_trial_balance.get_trial_balance(
          session, start_date=start_date, end_date=end_date
        )
    except (ValueError, ProgrammingError):
      return None
    return TrialBalance.from_pydantic(response)

  # ── Transactions ────────────────────────────────────────────────────────

  @strawberry.field
  def transactions(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> LedgerTransactionList | None:
    """Paginated list of transactions."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_transactions.list_transactions(
          session,
          type=type,
          start_date=start_date,
          end_date=end_date,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      return None
    return LedgerTransactionList.from_pydantic(response)

  @strawberry.field
  def transaction(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    transaction_id: str,
  ) -> LedgerTransactionDetail | None:
    """Single transaction with all entries and line items."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_transactions.get_transaction(session, transaction_id)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return LedgerTransactionDetail.from_pydantic(response)

  # ── Taxonomies ──────────────────────────────────────────────────────────

  @strawberry.field
  def taxonomies(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    taxonomy_type: str | None = None,
  ) -> TaxonomyList | None:
    """List all active taxonomies, optionally filtered by type."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_taxonomies.list_taxonomies(
          session, taxonomy_type=taxonomy_type
        )
    except (ValueError, ProgrammingError):
      return None
    return TaxonomyList.from_pydantic(response)

  @strawberry.field
  def reporting_taxonomy(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> Taxonomy | None:
    """The locked US GAAP reporting taxonomy, or null."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_taxonomies.get_reporting_taxonomy(session)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return Taxonomy.from_pydantic(response)

  # ── Elements ────────────────────────────────────────────────────────────

  @strawberry.field
  def elements(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    taxonomy_id: str | None = None,
    source: str | None = None,
    classification: str | None = None,
    is_abstract: bool | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> ElementList | None:
    """Paginated list of taxonomy elements."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_taxonomies.list_elements(
          session,
          taxonomy_id=taxonomy_id,
          source=source,
          classification=classification,
          is_abstract=is_abstract,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      return None
    return ElementList.from_pydantic(response)

  @strawberry.field
  def unmapped_elements(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    mapping_id: str | None = None,
  ) -> list[UnmappedElement]:
    """CoA elements not yet mapped to the reporting taxonomy."""
    try:
      with _open_session(info, str(graph_id)) as session:
        responses = reads_taxonomies.list_unmapped_elements(
          session, mapping_id=mapping_id
        )
    except (ValueError, ProgrammingError):
      return []
    return [UnmappedElement.from_pydantic(r) for r in responses]

  # ── Structures / mappings ──────────────────────────────────────────────

  @strawberry.field
  def structures(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    taxonomy_id: str | None = None,
    structure_type: str | None = None,
  ) -> StructureList | None:
    """List active structures."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_taxonomies.list_structures(
          session, taxonomy_id=taxonomy_id, structure_type=structure_type
        )
    except (ValueError, ProgrammingError):
      return None
    return StructureList.from_pydantic(response)

  @strawberry.field
  def mappings(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> StructureList | None:
    """List all active mapping structures."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_taxonomies.list_mappings(session)
    except (ValueError, ProgrammingError):
      return None
    return StructureList.from_pydantic(response)

  @strawberry.field
  def mapping(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    mapping_id: str,
  ) -> MappingDetail | None:
    """Single mapping structure with all associations."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_taxonomies.get_mapping_detail(session, mapping_id)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return MappingDetail.from_pydantic(response)

  @strawberry.field
  def mapping_coverage(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    mapping_id: str,
  ) -> MappingCoverage | None:
    """Coverage stats for a mapping."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_taxonomies.get_mapping_coverage(session, mapping_id)
    except (ValueError, ProgrammingError):
      return None
    return MappingCoverage.from_pydantic(response)

  # ── Schedules ───────────────────────────────────────────────────────────

  @strawberry.field
  def schedules(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> ScheduleList | None:
    """List all active schedules."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_schedules.list_schedules(session, _schedule_svc)
    except (ValueError, ProgrammingError):
      return None
    return ScheduleList.from_pydantic(response)

  @strawberry.field
  def schedule_facts(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    structure_id: str,
    period_start: date | None = None,
    period_end: date | None = None,
  ) -> ScheduleFacts | None:
    """Facts for a schedule, optionally filtered by period."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_schedules.get_schedule_facts(
          session, _schedule_svc, structure_id, period_start, period_end
        )
    except (ValueError, ProgrammingError):
      return None
    return ScheduleFacts.from_pydantic(response)

  @strawberry.field
  def period_close_status(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    period_start: date,
    period_end: date,
  ) -> PeriodCloseStatus | None:
    """Close status for all schedules in a fiscal period."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_schedules.get_period_close_status(
          session, _schedule_svc, period_start, period_end
        )
    except (ValueError, ProgrammingError):
      return None
    return PeriodCloseStatus.from_pydantic(response)

  # ── Fiscal calendar ─────────────────────────────────────────────────────

  @strawberry.field
  def fiscal_calendar(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> FiscalCalendar | None:
    """Current fiscal calendar state — pointers, gap, closeable status."""
    from robosystems.database import get_db_session

    try:
      with _open_session(info, str(graph_id)) as session:
        calendar = _fiscal_svc.get(session, str(graph_id))
        if calendar is None:
          return None
        # Platform DB lookup for QB sync state. GraphQL resolver doesn't
        # have a FastAPI dependency to inject a platform session, so pull
        # one from the existing generator and close it cleanly.
        gen = get_db_session()
        try:
          platform_db = next(gen)
          has_sync, last_sync_at = reads_fiscal_calendar.qb_sync_state(
            platform_db, str(graph_id)
          )
        finally:
          gen.close()
        response = reads_fiscal_calendar.build_fiscal_calendar_response(
          session,
          str(graph_id),
          calendar,
          has_sync,
          last_sync_at,
          _fiscal_svc,
        )
    except (ValueError, ProgrammingError):
      return None
    return FiscalCalendar.from_pydantic(response)

  # ── Period drafts (close review) ────────────────────────────────────────

  @strawberry.field
  def period_drafts(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    period: str,
  ) -> PeriodDrafts | None:
    """All draft entries for a fiscal period, ready for review before close."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_period_drafts.list_period_drafts(session, period)
    except (ValueError, ProgrammingError):
      return None
    return PeriodDrafts.from_pydantic(response)

  # ── Closing book ────────────────────────────────────────────────────────

  @strawberry.field
  def closing_book_structures(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> ClosingBookStructures | None:
    """Closing book sidebar navigation (statements, schedules, rollups, etc.)."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_closing_book.get_closing_book_structures(session)
    except (ValueError, ProgrammingError):
      return None
    return ClosingBookStructures.from_pydantic(response)

  # ── Reports ─────────────────────────────────────────────────────────────

  @strawberry.field
  def reports(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> ReportList | None:
    """List all report definitions for this graph."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_reports.list_reports(session)
    except (ValueError, ProgrammingError):
      return None
    return ReportList.from_pydantic(response)

  @strawberry.field
  def report(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    report_id: str,
  ) -> Report | None:
    """Single report definition with structures + entity name."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_reports.get_report(session, report_id)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return Report.from_pydantic(response)

  @strawberry.field
  def statement(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    report_id: str,
    structure_type: str,
  ) -> Statement | None:
    """Rendered financial statement for a report + structure_type."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_reports.get_statement(session, report_id, structure_type)
    except reads_reports.StatementStructureNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return Statement.from_pydantic(response)

  # ── Publish lists ───────────────────────────────────────────────────────

  @strawberry.field
  def publish_lists(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    limit: int = 100,
    offset: int = 0,
  ) -> PublishListList | None:
    """Paginated list of publish lists for this graph."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_publish_lists.list_publish_lists(
          session, limit=limit, offset=offset
        )
    except (ValueError, ProgrammingError):
      return None
    return PublishListList.from_pydantic(response)

  @strawberry.field
  def publish_list(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    list_id: str,
  ) -> PublishListDetail | None:
    """Single publish list with enriched members, or null."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_publish_lists.get_publish_list(session, list_id)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return PublishListDetail.from_pydantic(response)
