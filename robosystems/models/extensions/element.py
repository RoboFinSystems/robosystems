"""Element model — unified taxonomy element.

Holds Chart of Accounts entries (from QuickBooks, Xero, native), US GAAP
reporting concepts (SFAC 6, us-gaap), and any future taxonomy elements.
All materialize to Element nodes in the graph via the postgres_scanner →
DuckDB → LadybugDB pipeline.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Element(ExtensionsBase):
  __tablename__ = "elements"
  __table_args__ = (
    Index("idx_elements_classification", "classification"),
    Index("idx_elements_statement_context", "statement_context"),
    Index("idx_elements_derivation_role", "derivation_role"),
    Index("idx_elements_parent", "parent_id"),
    Index("idx_elements_external", "external_id", "external_source"),
    Index(
      "idx_elements_active",
      "is_active",
      postgresql_where="is_active = true",
    ),
    Index("idx_elements_taxonomy", "taxonomy_id"),
    Index("idx_elements_source", "source"),
    Index(
      "idx_elements_qname",
      "qname",
      unique=True,
      postgresql_where="qname IS NOT NULL",
    ),
    Index(
      "idx_elements_namespace",
      "namespace",
      postgresql_where="namespace IS NOT NULL",
    ),
    # classification = "economic nature" axis — six values plus NULL:
    #   asset | liability | equity           (balance-sheet stocks)
    #   inflow | outflow                     (credit-flow + debit-flow primitives;
    #                                         collapses SFAC 6's revenue/gain →
    #                                         inflow and expense/loss → outflow)
    #   cashflow                             (cash-statement reconciliation items
    #                                         + movement primitives — not an
    #                                         SFAC 6 concept, but IS the context
    #                                         for the cash flow statement)
    # Nullable because structural rows (hypercubes, RollUps), metadata,
    # and ratios don't fit any bucket. FAC has no gain/loss concepts;
    # SFAC 6's R/E/G/L split has moved to derivation_role refinements.
    CheckConstraint(
      "classification IS NULL OR classification IN ("
      "'asset', 'liability', 'equity', "
      "'inflow', 'outflow', 'cashflow'"
      ")",
      name="check_element_classification",
    ),
    # statement_context = which report this element belongs to. Independent
    # of classification: a CF reconciliation item like NetCashFlowFromInvesting
    # lives on the cash_flow statement but its underlying economic nature is
    # an asset movement.
    CheckConstraint(
      "statement_context IS NULL OR statement_context IN ("
      "'balance_sheet', 'income_statement', 'cash_flow', "
      "'equity_changes', 'disclosure', 'metadata', 'analysis'"
      ")",
      name="check_element_statement_context",
    ),
    # derivation_role = structural role in a report. Five values:
    #   primitive    — leaf: Revenue, AccountsReceivable, IncreaseDecreaseInInventory
    #   aggregate    — RollUp head (subtotals, totals, NetCashFlow rollups):
    #                  GrossProfit, NetIncome, NetCashFlowFromInvesting
    #   ratio        — computed metric: ReturnOnAssets, CurrentRatio
    #   identifier   — entity/document metadata: EntityCIK, FiscalYearEnd
    #   structural   — abstracts/hypercubes/LineItems with no economic
    #                  meaning, only grouping
    # "Movement" (IncreaseDecrease* BS-account deltas) folds into primitive —
    # `economic_nature=cashflow` + `statement_context=cash_flow` already
    # carry the CF-specific semantics; the leaf/aggregate distinction
    # is what derivation_role is for.
    CheckConstraint(
      "derivation_role IS NULL OR derivation_role IN ("
      "'primitive', 'aggregate', "
      "'ratio', 'identifier', 'structural'"
      ")",
      name="check_element_derivation_role",
    ),
    CheckConstraint(
      "balance_type IN ('debit', 'credit')",
      name="check_element_balance_type",
    ),
    CheckConstraint(
      "period_type IN ('duration', 'instant')",
      name="check_element_period_type",
    ),
    CheckConstraint(
      "element_type IN ('concept', 'abstract', 'axis', 'member', 'hypercube')",
      name="check_element_type",
    ),
    CheckConstraint(
      # 'system' is reserved for internal FK-anchor elements created by the
      # taxonomy seed (e.g., struct_balance_sheet) and is intentionally NOT
      # in COA_SOURCES so those rows never appear in the Chart of Accounts.
      "source IN ('sfac6', 'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
      "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system')",
      name="check_element_source",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("elem"))
  code = Column(String, nullable=True)
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # XBRL Alignment
  qname = Column(String, nullable=True)
  namespace = Column(String, nullable=True)
  uri = Column(String, nullable=True)

  # Classification axes — three orthogonal facets. `classification` is the
  # SFAC 6 economic-nature axis (asset, revenue, …); `statement_context` is
  # which report an element lives on; `derivation_role` is its structural
  # role (primitive, subtotal, reconciliation, …). All nullable: only
  # genuine SFAC 6 primitives fill every axis.
  classification = Column(String, nullable=True)
  statement_context = Column(String, nullable=True)
  derivation_role = Column(String, nullable=True)
  sub_classification = Column(String, nullable=True)
  balance_type = Column(String, nullable=False, default="debit")
  period_type = Column(String, nullable=False, default="duration")

  # Element Type
  is_abstract = Column(Boolean, nullable=False, default=False)
  is_monetary = Column(Boolean, nullable=False, default=True)
  element_type = Column(String, nullable=False, default="concept")

  # Hierarchy
  parent_id = Column(String, ForeignKey("elements.id"), nullable=True)
  depth = Column(Integer, nullable=False, default=0)
  path = Column(String, nullable=False, default="")

  # Taxonomy Membership
  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=True)
  source = Column(String, nullable=False, default="native")

  # Currency
  currency = Column(String, nullable=False, default="USD")

  # State
  is_active = Column(Boolean, nullable=False, default=True)
  is_placeholder = Column(Boolean, nullable=False, default=False)

  # External mapping (QB, Xero, etc.)
  external_id = Column(String, nullable=True)
  external_source = Column(String, nullable=True)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return f"<Element {self.qname or self.code} {self.name}>"


# Backward compatibility alias
Account = Element
