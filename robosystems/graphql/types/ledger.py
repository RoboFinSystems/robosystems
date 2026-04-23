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

import strawberry

# Register PaginationInfo first — many list-returning types reference it via
# the Pydantic decorator, which fails if the Strawberry wrapper isn't yet
# known to the registry.
from robosystems.graphql.types.common import PaginationInfo  # noqa: F401
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


@strawberry.experimental.pydantic.type(model=PydanticLedgerEntity, all_fields=True)
class LedgerEntity:
  """The top-level ledger entity (company/organization) for a graph."""


# ── Summary ───────────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(
  model=PydanticLedgerSummaryResponse, all_fields=True
)
class LedgerSummary:
  """Counts + date range + connection metadata for a ledger graph."""


# ── Accounts ──────────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticAccountResponse, all_fields=True)
class Account:
  """A single Chart of Accounts element."""


@strawberry.experimental.pydantic.type(
  model=PydanticAccountListResponse, all_fields=True
)
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
  classification: str | None
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
      classification=node.classification,
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


@strawberry.experimental.pydantic.type(model=PydanticAccountRollupRow, all_fields=True)
class AccountRollupRow:
  """Single CoA account row within an account rollup group."""


@strawberry.experimental.pydantic.type(
  model=PydanticAccountRollupGroup, all_fields=True
)
class AccountRollupGroup:
  """Reporting-element-keyed group of CoA accounts rolled into it."""


@strawberry.experimental.pydantic.type(
  model=PydanticAccountRollupsResponse, all_fields=True
)
class AccountRollups:
  """Account rollups — CoA accounts grouped by reporting element with balances."""


# ── Trial balance ─────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticTrialBalanceRow, all_fields=True)
class TrialBalanceRow:
  """Single row of the trial balance (one per account)."""


@strawberry.experimental.pydantic.type(
  model=PydanticTrialBalanceResponse, all_fields=True
)
class TrialBalance:
  """Trial balance for posted entries in a date range."""


# ── Transactions ──────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(
  model=PydanticLedgerTransactionSummaryResponse, all_fields=True
)
class LedgerTransactionSummary:
  """Transaction header — used in list views."""


@strawberry.experimental.pydantic.type(
  model=PydanticLedgerTransactionListResponse, all_fields=True
)
class LedgerTransactionList:
  """Paginated list of transactions."""


@strawberry.experimental.pydantic.type(
  model=PydanticLedgerLineItemResponse, all_fields=True
)
class LedgerLineItem:
  """Single debit/credit line item within an entry."""


@strawberry.experimental.pydantic.type(
  model=PydanticLedgerEntryResponse, all_fields=True
)
class LedgerEntry:
  """A single journal entry with all line items."""


@strawberry.experimental.pydantic.type(
  model=PydanticLedgerTransactionDetailResponse, all_fields=True
)
class LedgerTransactionDetail:
  """Transaction with all entries and line items fully expanded."""


# ── Taxonomies / elements / structures / mappings ────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticTaxonomyResponse, all_fields=True)
class Taxonomy:
  """A taxonomy definition (CoA, reporting, mapping, or schedule)."""


@strawberry.experimental.pydantic.type(
  model=PydanticTaxonomyListResponse, all_fields=True
)
class TaxonomyList:
  """List of taxonomies."""


@strawberry.experimental.pydantic.type(model=PydanticElementResponse, all_fields=True)
class Element:
  """Element with taxonomy context."""


@strawberry.experimental.pydantic.type(
  model=PydanticElementListResponse, all_fields=True
)
class ElementList:
  """Paginated list of elements."""


@strawberry.experimental.pydantic.type(model=PydanticSuggestedTarget, all_fields=True)
class SuggestedTarget:
  """A suggested mapping target from the reporting taxonomy."""


@strawberry.experimental.pydantic.type(
  model=PydanticUnmappedElementResponse, all_fields=True
)
class UnmappedElement:
  """An element not yet mapped to the reporting taxonomy."""


@strawberry.experimental.pydantic.type(model=PydanticStructureResponse, all_fields=True)
class Structure:
  """A structure (statement, mapping, schedule, etc.)."""


@strawberry.experimental.pydantic.type(
  model=PydanticStructureListResponse, all_fields=True
)
class StructureList:
  """List of structures."""


@strawberry.experimental.pydantic.type(
  model=PydanticAssociationResponse, all_fields=True
)
class Association:
  """A single element-to-element association within a mapping."""


@strawberry.experimental.pydantic.type(
  model=PydanticMappingDetailResponse, all_fields=True
)
class MappingDetail:
  """A mapping structure with all its associations expanded."""


@strawberry.experimental.pydantic.type(
  model=PydanticMappingCoverageResponse, all_fields=True
)
class MappingCoverage:
  """Coverage stats for a mapping (mapped / unmapped / confidence bands)."""


# ── Fiscal calendar ───────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(
  model=PydanticFiscalPeriodSummary, all_fields=True
)
class FiscalPeriodSummary:
  """Single fiscal period row."""


@strawberry.experimental.pydantic.type(
  model=PydanticFiscalCalendarResponse, all_fields=True
)
class FiscalCalendar:
  """Current fiscal calendar state for a graph — pointers, gap, closeable."""


# ── Period drafts (close review) ──────────────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticDraftLineItem, all_fields=True)
class DraftLineItem:
  """Single line item within a draft entry."""


@strawberry.experimental.pydantic.type(
  model=PydanticDraftEntryResponse, all_fields=True
)
class DraftEntry:
  """A draft entry awaiting review before period close."""


@strawberry.experimental.pydantic.type(
  model=PydanticPeriodDraftsResponse, all_fields=True
)
class PeriodDrafts:
  """All draft entries for a fiscal period."""


# ── Schedules ─────────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(
  model=PydanticPeriodCloseItemResponse, all_fields=True
)
class PeriodCloseItem:
  """Single schedule's close state within a fiscal period."""


@strawberry.experimental.pydantic.type(
  model=PydanticPeriodCloseStatusResponse, all_fields=True
)
class PeriodCloseStatus:
  """Close status for all schedules in a fiscal period."""


# ── Closing book ──────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticClosingBookItem, all_fields=True)
class ClosingBookItem:
  """Single sidebar item in the closing book viewer."""


@strawberry.experimental.pydantic.type(
  model=PydanticClosingBookCategory, all_fields=True
)
class ClosingBookCategory:
  """Sidebar category grouping multiple closing book items."""


@strawberry.experimental.pydantic.type(
  model=PydanticClosingBookStructuresResponse, all_fields=True
)
class ClosingBookStructures:
  """Closing book sidebar navigation — categories + items."""


# ── Reports ───────────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticPeriodSpec, all_fields=True)
class PeriodSpec:
  """A single reporting period column."""


@strawberry.experimental.pydantic.type(model=PydanticStructureSummary, all_fields=True)
class StructureSummary:
  """Structure available within a report's taxonomy."""


@strawberry.experimental.pydantic.type(model=PydanticReportResponse, all_fields=True)
class Report:
  """Report definition summary — structures + entity + sharing provenance."""

  # rule_summary is dict[str, int] in Pydantic — must be declared as JSON
  # here because Strawberry's pydantic derivation can't map dict[str, int]
  # to a GraphQL type automatically.
  rule_summary: strawberry.scalars.JSON | None


@strawberry.experimental.pydantic.type(
  model=PydanticReportListResponse, all_fields=True
)
class ReportList:
  """List of reports for a graph."""


@strawberry.experimental.pydantic.type(model=PydanticFactRowResponse, all_fields=True)
class FactRow:
  """Single row in a rendered financial statement."""


@strawberry.experimental.pydantic.type(
  model=PydanticValidationCheckResponse, all_fields=True
)
class ValidationCheck:
  """Validation result for a rendered statement."""


@strawberry.experimental.pydantic.type(model=PydanticStatementResponse, all_fields=True)
class Statement:
  """Rendered financial statement — facts viewed through a structure."""


# ── Publish lists ─────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(
  model=PydanticPublishListMemberResponse, all_fields=True
)
class PublishListMember:
  """Target graph membership in a publish list."""


@strawberry.experimental.pydantic.type(
  model=PydanticPublishListResponse, all_fields=True
)
class PublishList:
  """Publish list summary (without full member details)."""


@strawberry.experimental.pydantic.type(
  model=PydanticPublishListDetailResponse, all_fields=True
)
class PublishListDetail:
  """Full publish list with enriched members."""


@strawberry.experimental.pydantic.type(
  model=PydanticPublishListListResponse, all_fields=True
)
class PublishListList:
  """Paginated list of publish lists."""


# ── Mapped trial balance ──────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(
  model=PydanticMappedTrialBalanceRow, all_fields=True
)
class MappedTrialBalanceRow:
  """Single reporting-concept row in the mapped trial balance."""


@strawberry.experimental.pydantic.type(
  model=PydanticMappedTrialBalanceResponse, all_fields=True
)
class MappedTrialBalance:
  """Trial balance rolled up to reporting concepts via mapping associations."""
