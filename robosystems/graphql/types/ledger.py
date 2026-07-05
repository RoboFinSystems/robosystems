"""Strawberry types for ledger (roboledger) extensions queries.

Most types wrap an existing Pydantic response model from
`robosystems.models.api.extensions.*` using
`strawberry.experimental.pydantic.type(model=..., all_fields=True)` so the
GraphQL schema is derived from the REST surface. When a Pydantic field is
added to a response model, the GraphQL field appears automatically.

**Recursive types** (currently just `AccountTreeNode`) are hand-written
with a `from_pydantic` classmethod because Strawberry's pydantic decorator
cannot resolve self-references. If a future recursive response model
appears, follow the same pattern.

Strawberry's default `auto_camel_case=True` converts Python snake_case
fields to camelCase on the wire — `legal_name` → `legalName` with no
manual reshape.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

import strawberry
from strawberry.scalars import JSON

from robosystems.graphql.types._pydantic import pydantic_type

# Register PaginationInfo first — many list-returning types reference it via
# the Pydantic decorator, which fails if the Strawberry wrapper isn't yet
# known to the registry.
from robosystems.graphql.types.common import PaginationInfo  # noqa: F401
from robosystems.models.api.event_block import (
  EventBlockEnvelope as PydanticEventBlockEnvelope,
)
from robosystems.models.api.extensions.account_rollups import (
  AccountRollupGroup as PydanticAccountRollupGroup,
)
from robosystems.models.api.extensions.account_rollups import (
  AccountRollupRow as PydanticAccountRollupRow,
)
from robosystems.models.api.extensions.account_rollups import (
  AccountRollupsResponse as PydanticAccountRollupsResponse,
)
from robosystems.models.api.extensions.accounts import (
  AccountListResponse as PydanticAccountListResponse,
)
from robosystems.models.api.extensions.accounts import (
  AccountResponse as PydanticAccountResponse,
)
from robosystems.models.api.extensions.accounts import (
  AccountTreeNode as PydanticAccountTreeNode,
)
from robosystems.models.api.extensions.agent import (
  LedgerAgentResponse as PydanticAgentResponse,
)
from robosystems.models.api.extensions.ar_ap import (
  OpenBalanceAggregate as PydanticOpenBalanceAggregate,
)
from robosystems.models.api.extensions.ar_ap import (
  OpenBalanceByAgent as PydanticOpenBalanceByAgent,
)
from robosystems.models.api.extensions.closing_book import (
  ClosingBookCategory as PydanticClosingBookCategory,
)
from robosystems.models.api.extensions.closing_book import (
  ClosingBookItem as PydanticClosingBookItem,
)
from robosystems.models.api.extensions.closing_book import (
  ClosingBookStructuresResponse as PydanticClosingBookStructuresResponse,
)
from robosystems.models.api.extensions.entity import (
  LedgerEntityResponse as PydanticLedgerEntity,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  DraftEntryResponse as PydanticDraftEntryResponse,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  DraftLineItem as PydanticDraftLineItem,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  FiscalCalendarResponse as PydanticFiscalCalendarResponse,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  FiscalPeriodSummary as PydanticFiscalPeriodSummary,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  PendingObligationDetailResponse as PydanticPendingObligationDetailResponse,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  PeriodDraftsResponse as PydanticPeriodDraftsResponse,
)
from robosystems.models.api.extensions.publish_lists import (
  PublishListDetailResponse as PydanticPublishListDetailResponse,
)
from robosystems.models.api.extensions.publish_lists import (
  PublishListListResponse as PydanticPublishListListResponse,
)
from robosystems.models.api.extensions.publish_lists import (
  PublishListMemberResponse as PydanticPublishListMemberResponse,
)
from robosystems.models.api.extensions.publish_lists import (
  PublishListResponse as PydanticPublishListResponse,
)
from robosystems.models.api.extensions.reports import (
  FactRowResponse as PydanticFactRowResponse,
)
from robosystems.models.api.extensions.reports import (
  PeriodSpec as PydanticPeriodSpec,
)
from robosystems.models.api.extensions.reports import (
  ReportBundleDownloadResponse as PydanticReportBundleDownloadResponse,
)
from robosystems.models.api.extensions.reports import (
  ReportListResponse as PydanticReportListResponse,
)
from robosystems.models.api.extensions.reports import (
  ReportResponse as PydanticReportResponse,
)
from robosystems.models.api.extensions.reports import (
  StatementResponse as PydanticStatementResponse,
)
from robosystems.models.api.extensions.reports import (
  StructureSummary as PydanticStructureSummary,
)
from robosystems.models.api.extensions.reports import (
  ValidationCheckResponse as PydanticValidationCheckResponse,
)
from robosystems.models.api.extensions.schedules import (
  PeriodCloseItemResponse as PydanticPeriodCloseItemResponse,
)
from robosystems.models.api.extensions.schedules import (
  PeriodCloseStatusResponse as PydanticPeriodCloseStatusResponse,
)
from robosystems.models.api.extensions.summary import (
  LedgerSummaryResponse as PydanticLedgerSummaryResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse as PydanticAssociationResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  ElementListResponse as PydanticElementListResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  ElementResponse as PydanticElementResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  MappedTrialBalanceResponse as PydanticMappedTrialBalanceResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  MappedTrialBalanceRow as PydanticMappedTrialBalanceRow,
)
from robosystems.models.api.extensions.taxonomies import (
  MappingCoverageResponse as PydanticMappingCoverageResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  MappingDetailResponse as PydanticMappingDetailResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  StructureListResponse as PydanticStructureListResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  StructureResponse as PydanticStructureResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  SuggestedTarget as PydanticSuggestedTarget,
)
from robosystems.models.api.extensions.taxonomies import (
  TaxonomyListResponse as PydanticTaxonomyListResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  TaxonomyResponse as PydanticTaxonomyResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  UnmappedElementResponse as PydanticUnmappedElementResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  UnreachableMapping as PydanticUnreachableMapping,
)
from robosystems.models.api.extensions.transactions import (
  LedgerEntryResponse as PydanticLedgerEntryResponse,
)
from robosystems.models.api.extensions.transactions import (
  LedgerLineItemResponse as PydanticLedgerLineItemResponse,
)
from robosystems.models.api.extensions.transactions import (
  LedgerTransactionDetailResponse as PydanticLedgerTransactionDetailResponse,
)
from robosystems.models.api.extensions.transactions import (
  LedgerTransactionListResponse as PydanticLedgerTransactionListResponse,
)
from robosystems.models.api.extensions.transactions import (
  LedgerTransactionSummaryResponse as PydanticLedgerTransactionSummaryResponse,
)
from robosystems.models.api.extensions.trial_balance import (
  TrialBalanceResponse as PydanticTrialBalanceResponse,
)
from robosystems.models.api.extensions.trial_balance import (
  TrialBalanceRow as PydanticTrialBalanceRow,
)

# ── Entity ────────────────────────────────────────────────────────────────


@pydantic_type(model=PydanticLedgerEntity, all_fields=True)
class LedgerEntity:
  """The top-level ledger entity (company/organization) for a graph."""


# ── Agents ────────────────────────────────────────────────────────────────


@strawberry.type
class Agent:
  """A counterparty (customer, vendor, employee, etc.) referenced by events.

  Hand-written because `address: dict` needs the JSON scalar — Strawberry's
  pydantic decorator cannot map an untyped dict field.
  """

  id: str
  agent_type: str
  name: str
  legal_name: str | None
  tax_id: str | None
  registration_number: str | None
  duns: str | None
  lei: str | None
  email: str | None
  phone: str | None
  address: JSON | None
  source: str
  external_id: str | None
  is_active: bool
  is_1099_recipient: bool
  created_at: datetime | None
  updated_at: datetime | None
  created_by: str | None

  @classmethod
  def from_pydantic(cls, row: PydanticAgentResponse) -> Agent:
    return cls(
      id=row.id,
      agent_type=row.agent_type,
      name=row.name,
      legal_name=row.legal_name,
      tax_id=row.tax_id,
      registration_number=row.registration_number,
      duns=row.duns,
      lei=row.lei,
      email=row.email,
      phone=row.phone,
      address=row.address,
      source=row.source,
      external_id=row.external_id,
      is_active=row.is_active,
      is_1099_recipient=row.is_1099_recipient,
      created_at=row.created_at,
      updated_at=row.updated_at,
      created_by=row.created_by,
    )

  @strawberry.field
  def open_receivable(self, info: strawberry.Info) -> OpenBalanceByAgent | None:
    """This agent's open AR balance, or null when fully settled.

    Single-agent lookup; resolves in one SQL round-trip. Querying this
    on a paginated `agents` list triggers N+1 — wrap with a dataloader
    when list views need it. The drill-in detail flow (one agent at a
    time) is the supported v1 use case.
    """
    from robosystems.graphql.resolvers._common import open_extensions_session
    from robosystems.operations.roboledger.reads import ar_ap as reads_ar_ap

    with open_extensions_session(info, "roboledger") as session:
      response = reads_ar_ap.get_open_receivable_for_agent(session, self.id)
    if response is None:
      return None
    return OpenBalanceByAgent.from_pydantic(response)

  @strawberry.field
  def open_payable(self, info: strawberry.Info) -> OpenBalanceByAgent | None:
    """This agent's open AP balance, or null when fully settled.

    Same N+1 caveat as ``open_receivable``.
    """
    from robosystems.graphql.resolvers._common import open_extensions_session
    from robosystems.operations.roboledger.reads import ar_ap as reads_ar_ap

    with open_extensions_session(info, "roboledger") as session:
      response = reads_ar_ap.get_open_payable_for_agent(session, self.id)
    if response is None:
      return None
    return OpenBalanceByAgent.from_pydantic(response)


# ── AR / AP open balances ─────────────────────────────────────────────────


@pydantic_type(model=PydanticOpenBalanceAggregate, all_fields=True)
class OpenBalanceAggregate:
  """Graph-wide open AR or AP totals."""


@pydantic_type(model=PydanticOpenBalanceByAgent, all_fields=True)
class OpenBalanceByAgent:
  """Per-counterparty open balance row."""


# ── Event blocks (inbox) ──────────────────────────────────────────────────


@strawberry.type
class EventBlock:
  """An REA event in the ledger inbox.

  Hand-written because `metadata: dict[str, Any]` needs the JSON scalar —
  Strawberry's pydantic decorator cannot map an untyped dict field. The
  envelope is the canonical "something happened" record; GL rows live
  downstream behind a handler.
  """

  id: str
  event_type: str
  event_category: str
  event_class: str
  status: str
  occurred_at: datetime
  effective_at: datetime | None
  source: str
  external_id: str | None
  external_url: str | None
  amount: int | None
  currency: str
  description: str | None
  metadata: JSON
  dimension_ids: list[str]
  agent_id: str | None
  resource_type: str | None
  resource_element_id: str | None
  replaced_by_event_id: str | None
  replaces_event_id: str | None
  obligated_by_event_id: str | None
  discharges_event_id: str | None
  created_at: datetime
  created_by: str

  @classmethod
  def from_pydantic(cls, row: PydanticEventBlockEnvelope) -> EventBlock:
    return cls(
      id=row.id,
      event_type=row.event_type,
      event_category=row.event_category,
      event_class=row.event_class,
      status=row.status,
      occurred_at=row.occurred_at,
      effective_at=row.effective_at,
      source=row.source,
      external_id=row.external_id,
      external_url=row.external_url,
      amount=row.amount,
      currency=row.currency,
      description=row.description,
      metadata=row.metadata,
      dimension_ids=list(row.dimension_ids),
      agent_id=row.agent_id,
      resource_type=row.resource_type,
      resource_element_id=row.resource_element_id,
      replaced_by_event_id=row.replaced_by_event_id,
      replaces_event_id=row.replaces_event_id,
      obligated_by_event_id=row.obligated_by_event_id,
      discharges_event_id=row.discharges_event_id,
      created_at=row.created_at,
      created_by=row.created_by,
    )


# ── Summary ───────────────────────────────────────────────────────────────


@pydantic_type(model=PydanticLedgerSummaryResponse, all_fields=True)
class LedgerSummary:
  """Counts + date range + connection metadata for a ledger graph."""


# ── Accounts ──────────────────────────────────────────────────────────────


@pydantic_type(model=PydanticAccountResponse, all_fields=True)
class Account:
  """A single Chart of Accounts element."""


@pydantic_type(model=PydanticAccountListResponse, all_fields=True)
class AccountList:
  """Paginated list of Chart of Accounts elements."""


@strawberry.type
class AccountTreeNode:
  """Recursive Chart of Accounts tree node.

  Hand-written because `children: list[AccountTreeNode]` is a
  self-reference — Strawberry's `experimental.pydantic.type` decorator
  cannot resolve that automatically. Use `from_pydantic` to convert a
  `PydanticAccountTreeNode` (which `operations/roboledger/reads/accounts.py`
  returns) into this type.
  """

  id: strawberry.ID
  code: str | None
  name: str
  trait: str | None
  account_type: str | None
  balance_type: str
  depth: int
  is_active: bool
  children: list[AccountTreeNode]

  @classmethod
  def from_pydantic(cls, node: PydanticAccountTreeNode) -> AccountTreeNode:
    return cls(
      id=strawberry.ID(node.id),
      code=node.code,
      name=node.name,
      trait=node.trait,
      account_type=node.account_type,
      balance_type=node.balance_type,
      depth=node.depth,
      is_active=node.is_active,
      children=[cls.from_pydantic(c) for c in node.children],
    )


@strawberry.type
class AccountTree:
  """Chart of Accounts rendered as a parent/child tree."""

  roots: list[AccountTreeNode]
  total_accounts: int


# ── Account rollups ───────────────────────────────────────────────────────


@pydantic_type(model=PydanticAccountRollupRow, all_fields=True)
class AccountRollupRow:
  """Single CoA account row within an account rollup group."""


@pydantic_type(model=PydanticAccountRollupGroup, all_fields=True)
class AccountRollupGroup:
  """Reporting-element-keyed group of CoA accounts rolled into it."""


@pydantic_type(model=PydanticAccountRollupsResponse, all_fields=True)
class AccountRollups:
  """Account rollups — CoA accounts grouped by reporting element with balances."""


# ── Trial balance ─────────────────────────────────────────────────────────


@pydantic_type(model=PydanticTrialBalanceRow, all_fields=True)
class TrialBalanceRow:
  """Single row of the trial balance (one per account)."""


@pydantic_type(model=PydanticTrialBalanceResponse, all_fields=True)
class TrialBalance:
  """Trial balance for posted entries in a date range."""


# ── Transactions ──────────────────────────────────────────────────────────


@pydantic_type(model=PydanticLedgerTransactionSummaryResponse, all_fields=True)
class LedgerTransactionSummary:
  """Transaction header — used in list views."""


@pydantic_type(model=PydanticLedgerTransactionListResponse, all_fields=True)
class LedgerTransactionList:
  """Paginated list of transactions."""


@pydantic_type(model=PydanticLedgerLineItemResponse, all_fields=True)
class LedgerLineItem:
  """Single debit/credit line item within an entry."""


@pydantic_type(model=PydanticLedgerEntryResponse, all_fields=True)
class LedgerEntry:
  """A single journal entry with all line items."""


@pydantic_type(model=PydanticLedgerTransactionDetailResponse, all_fields=True)
class LedgerTransactionDetail:
  """Transaction with all entries and line items fully expanded."""


# ── Taxonomies / elements / structures / mappings ────────────────────────


@pydantic_type(model=PydanticTaxonomyResponse, all_fields=True)
class Taxonomy:
  """A taxonomy definition (CoA, reporting, mapping, or schedule)."""


@pydantic_type(model=PydanticTaxonomyListResponse, all_fields=True)
class TaxonomyList:
  """List of taxonomies."""


@pydantic_type(model=PydanticElementResponse, all_fields=True)
class Element:
  """Element with taxonomy context."""


@pydantic_type(model=PydanticElementListResponse, all_fields=True)
class ElementList:
  """Paginated list of elements."""


@pydantic_type(model=PydanticSuggestedTarget, all_fields=True)
class SuggestedTarget:
  """A suggested mapping target from the reporting taxonomy."""


@pydantic_type(model=PydanticUnmappedElementResponse, all_fields=True)
class UnmappedElement:
  """An element not yet mapped to the reporting taxonomy."""


@pydantic_type(model=PydanticStructureResponse, all_fields=True)
class Structure:
  """A structure (statement, mapping, schedule, etc.)."""


@pydantic_type(model=PydanticStructureListResponse, all_fields=True)
class StructureList:
  """List of structures."""


@pydantic_type(model=PydanticAssociationResponse, all_fields=True)
class Association:
  """A single element-to-element association within a mapping."""


@pydantic_type(model=PydanticMappingDetailResponse, all_fields=True)
class MappingDetail:
  """A mapping structure with all its associations expanded."""


@pydantic_type(model=PydanticUnreachableMapping, all_fields=True)
class UnreachableMappingType:
  """A CoA→rs-gaap mapping whose target doesn't reach a Network root."""


@pydantic_type(model=PydanticMappingCoverageResponse, all_fields=True)
class MappingCoverage:
  """Coverage stats for a mapping (mapped / unmapped / confidence bands)."""


# ── Fiscal calendar ───────────────────────────────────────────────────────


@pydantic_type(model=PydanticFiscalPeriodSummary, all_fields=True)
class FiscalPeriodSummary:
  """Single fiscal period row."""


@pydantic_type(model=PydanticPendingObligationDetailResponse, all_fields=True)
class PendingObligationDetail:
  """One pending schedule-derived obligation blocking close."""


@pydantic_type(model=PydanticFiscalCalendarResponse, all_fields=True)
class FiscalCalendar:
  """Current fiscal calendar state for a graph — pointers, gap, closeable."""


# ── Period drafts (close review) ──────────────────────────────────────────


@pydantic_type(model=PydanticDraftLineItem, all_fields=True)
class DraftLineItem:
  """Single line item within a draft entry."""


@pydantic_type(model=PydanticDraftEntryResponse, all_fields=True)
class DraftEntry:
  """A draft entry awaiting review before period close."""


@pydantic_type(model=PydanticPeriodDraftsResponse, all_fields=True)
class PeriodDrafts:
  """All draft entries for a fiscal period."""


# ── Schedules ─────────────────────────────────────────────────────────────


@pydantic_type(model=PydanticPeriodCloseItemResponse, all_fields=True)
class PeriodCloseItem:
  """Single schedule's close state within a fiscal period."""


@pydantic_type(model=PydanticPeriodCloseStatusResponse, all_fields=True)
class PeriodCloseStatus:
  """Close status for all schedules in a fiscal period."""


# ── Closing book ──────────────────────────────────────────────────────────


@pydantic_type(model=PydanticClosingBookItem, all_fields=True)
class ClosingBookItem:
  """Single sidebar item in the closing book viewer."""


@pydantic_type(model=PydanticClosingBookCategory, all_fields=True)
class ClosingBookCategory:
  """Sidebar category grouping multiple closing book items."""


@pydantic_type(model=PydanticClosingBookStructuresResponse, all_fields=True)
class ClosingBookStructures:
  """Closing book sidebar navigation — categories + items."""


# ── Reports ───────────────────────────────────────────────────────────────


@pydantic_type(model=PydanticPeriodSpec, all_fields=True)
class PeriodSpec:
  """A single reporting period column."""


@pydantic_type(model=PydanticStructureSummary, all_fields=True)
class StructureSummary:
  """Structure available within a report's taxonomy."""


@pydantic_type(model=PydanticReportResponse, all_fields=True)
class Report:
  """Report definition summary — structures + entity + sharing provenance."""

  # rule_summary is dict[str, int] in Pydantic — must be declared as JSON
  # here because Strawberry's pydantic derivation can't map dict[str, int]
  # to a GraphQL type automatically.
  rule_summary: strawberry.scalars.JSON | None


@pydantic_type(model=PydanticReportListResponse, all_fields=True)
class ReportList:
  """List of reports for a graph."""


@strawberry.enum
class ReportDownloadFormat(Enum):
  """Serialization flavor for a Report bundle download.

  Names map to the operations-layer ``RdfFlavor`` / ``XbrlFlavor``
  string values (``jsonld``, ``holon-jsonld``, ``xbrl-2.1``). The
  hyphenated values aren't valid GraphQL enum *names*, so the wire names
  are ``HOLON_JSONLD`` / ``XBRL_2_1`` while the resolver forwards the
  ``.value`` to the ops read.

  ``JSONLD`` is the flat canonical bundle stamped at publish;
  ``HOLON_JSONLD`` is the dataset-form named-graph holon (scene /
  boundary / projection), materialized on demand off the same bundle.
  """

  JSONLD = "jsonld"
  HOLON_JSONLD = "holon-jsonld"
  XBRL_2_1 = "xbrl-2.1"


@pydantic_type(model=PydanticReportBundleDownloadResponse, all_fields=True)
class ReportBundleDownload:
  """Presigned-URL envelope for a published Report's serialization bundle."""


@pydantic_type(model=PydanticFactRowResponse, all_fields=True)
class FactRow:
  """Single row in a rendered financial statement."""


@pydantic_type(model=PydanticValidationCheckResponse, all_fields=True)
class ValidationCheck:
  """Validation result for a rendered statement."""


@pydantic_type(model=PydanticStatementResponse, all_fields=True)
class Statement:
  """Rendered financial statement — facts viewed through a structure."""


# ── Publish lists ─────────────────────────────────────────────────────────


@pydantic_type(model=PydanticPublishListMemberResponse, all_fields=True)
class PublishListMember:
  """Target graph membership in a publish list."""


@pydantic_type(model=PydanticPublishListResponse, all_fields=True)
class PublishList:
  """Publish list summary (without full member details)."""


@pydantic_type(model=PydanticPublishListDetailResponse, all_fields=True)
class PublishListDetail:
  """Full publish list with enriched members."""


@pydantic_type(model=PydanticPublishListListResponse, all_fields=True)
class PublishListList:
  """Paginated list of publish lists."""


# ── Mapped trial balance ──────────────────────────────────────────────────


@pydantic_type(model=PydanticMappedTrialBalanceRow, all_fields=True)
class MappedTrialBalanceRow:
  """Single reporting-concept row in the mapped trial balance."""


@pydantic_type(model=PydanticMappedTrialBalanceResponse, all_fields=True)
class MappedTrialBalance:
  """Trial balance rolled up to reporting concepts via mapping associations."""
