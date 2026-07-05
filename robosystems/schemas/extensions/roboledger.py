"""
RoboLedger Schema Extension for LadybugDB

Complete accounting system with both transaction and reporting capabilities.

This unified schema includes:
- 📊 Financial Reporting (XBRL, SEC filings, financial statements)
- 📒 General Ledger (transactions, journal entries, accounts)

CONTEXT-AWARE USAGE:
-------------------
Different contexts get different views of the same schema:

1. SEC Repository (reporting only):
   - Only XBRL/reporting tables visible
   - No transaction data (doesn't exist in SEC)
   - Prevents MCP agent confusion

2. Entity Graphs (full system):
   - Complete accounting capabilities
   - Both transactions AND reporting
   - Full RoboLedger functionality

For direct context control, use:
  from robosystems.schemas.loader import get_contextual_schema_loader

  # SEC repository - reporting only
  loader = get_contextual_schema_loader("repository", "sec")

  # Entity graph - full accounting
  loader = get_contextual_schema_loader("application", "roboledger")
"""

from ..models import Node, Property, Relationship

# NOTE: Before adding nodes or edges here, review schemas/base.py invariants.
# Universally-applicable concepts (Entity, Taxonomy, Element, Dimension,
# Structure, Association, Classification) belong in base.py, not here.
# Aspects (Period, Unit, Dimension) never attach to declarative nodes like
# Entity, Report, Taxonomy, Portfolio — that's a category error.

# ============================================================================
# REPORTING SECTION (XBRL/Financial Statements)
# Used by: SEC repositories, Entity graphs
# ============================================================================

REPORTING_NODES = [
  Node(
    name="Report",
    description="Financial report filed by a entity",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),  # indexed
      Property(name="name", type="STRING"),
      Property(name="accession_number", type="STRING"),
      Property(name="form", type="STRING"),
      Property(name="filing_date", type="STRING"),
      Property(name="report_date", type="STRING"),  # Period end date for the report
      Property(name="acceptance_date", type="STRING"),
      Property(name="is_inline_xbrl", type="BOOLEAN"),
      Property(name="xbrl_processor_version", type="STRING"),
      Property(name="processed", type="BOOLEAN"),
      Property(name="failed", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),
      # Fiscal context from DEI cover page (enables fiscal-aware queries)
      Property(
        name="fiscal_year_focus", type="INT32"
      ),  # From dei:DocumentFiscalYearFocus (e.g., 2024, 2025)
      Property(
        name="fiscal_period_focus", type="STRING"
      ),  # From dei:DocumentFiscalPeriodFocus (FY, Q1, Q2, Q3)
      Property(
        name="fiscal_year_end_month", type="INT32"
      ),  # Parsed from dei:CurrentFiscalYearEndDate (1-12, e.g., 12 for Dec year-end)
    ],
  ),
  Node(
    name="Fact",
    description="Individual fact/data point from financial reports. Facts may have dimensional breakdowns (segments, geography, products). Use has_dimensions=false or NOT (f)-[:FACT_HAS_DIMENSION]->() to get consolidated totals only.",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(
        name="value", type="STRING"
      ),  # required - may contain URL for externalized content
      Property(name="numeric_value", type="DOUBLE"),
      Property(name="fact_type", type="STRING"),  # 'Numeric' or 'Nonnumeric'
      Property(name="decimals", type="STRING"),
      Property(name="value_type", type="STRING"),  # 'inline' or 'external_resource'
      Property(
        name="content_type", type="STRING"
      ),  # MIME type for externalized content
      Property(
        name="has_dimensions", type="BOOLEAN"
      ),  # True if fact has dimensional breakdowns (segments, geography, etc.)
      Property(
        name="dimension_count", type="INT64"
      ),  # Number of dimensional qualifiers (0=consolidated, 1=single breakdown, 2+=complex)
    ],
  ),
  # NOTE: Structure, Association, Classification are now in schemas/base.py
  # because they're base ontology concepts (XBRL taxonomy link networks and
  # pattern metadata), not reporting-specific. Any extension that works with
  # a formal taxonomy traverses these nodes. See INVARIANT 1 in base.py.
  Node(
    name="FactSet",
    description="Logical grouping of related facts",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
    ],
  ),
]

REPORTING_RELATIONSHIPS = [
  Relationship(
    name="ENTITY_HAS_REPORT",
    from_node="Entity",
    to_node="Report",
    description="Entity has filed reports",
    properties=[
      Property(name="filing_context", type="STRING"),
    ],
  ),
  Relationship(
    name="REPORT_HAS_FACT",
    from_node="Report",
    to_node="Fact",
    description="Report contains facts",
    properties=[
      Property(name="fact_context", type="STRING"),
    ],
  ),
  Relationship(
    name="FACT_HAS_ELEMENT",
    from_node="Fact",
    to_node="Element",
    description="Fact references element",
    properties=[],
  ),
  Relationship(
    name="FACT_HAS_ENTITY",
    from_node="Fact",
    to_node="Entity",
    description="Fact belongs to reporting entity",
    properties=[
      Property(name="entity_context", type="STRING"),
    ],
  ),
  Relationship(
    name="FACT_HAS_PERIOD",
    from_node="Fact",
    to_node="Period",
    description="Fact applies to specific period",
    properties=[
      Property(name="period_context", type="STRING"),
    ],
  ),
  Relationship(
    name="FACT_HAS_UNIT",
    from_node="Fact",
    to_node="Unit",
    description="Fact has unit of measurement",
    properties=[
      Property(name="unit_context", type="STRING"),
    ],
  ),
  # Fact → Dimension relationship (XBRL dimensional qualifiers)
  Relationship(
    name="FACT_HAS_DIMENSION",
    from_node="Fact",
    to_node="Dimension",
    description="Fact has dimensional qualifiers (segments, geography, products)",
    properties=[],
  ),
  Relationship(
    name="FACT_SET_CONTAINS_FACT",
    from_node="FactSet",
    to_node="Fact",
    description="Fact set contains facts",
    properties=[],
  ),
  Relationship(
    name="STRUCTURE_HAS_FACT_SET",
    from_node="Structure",
    to_node="FactSet",
    description="Structure has a pre-computed set of facts for rendering",
    properties=[],
  ),
  # Report → FactSet (the package-mode container edge). A Report groups
  # N FactSets — one per statement Structure produced for the period.
  # Lets the graph traverse "give me the FactSets of this Report" in one
  # hop, instead of the two-hop ``Report → Fact ← FactSet`` path.
  Relationship(
    name="REPORT_HAS_FACT_SET",
    from_node="Report",
    to_node="FactSet",
    description="Report contains FactSets (one per statement Structure)",
    properties=[],
  ),
  Relationship(
    name="REPORT_USES_TAXONOMY",
    from_node="Report",
    to_node="Taxonomy",
    description="Report uses XBRL taxonomy",
    properties=[
      Property(name="taxonomy_context", type="STRING"),
    ],
  ),
  # NOTE: STRUCTURE_HAS_ASSOCIATION, ASSOCIATION_HAS_FROM_ELEMENT,
  # ASSOCIATION_HAS_TO_ELEMENT, ASSOCIATION_HAS_CLASSIFICATION, and
  # STRUCTURE_HAS_TAXONOMY are now in schemas/base.py alongside the
  # Structure / Association / Classification nodes they connect.
]

# ============================================================================
# TRANSACTION SECTION (General Ledger/Bookkeeping)
# Used by: Entity graphs only (NOT in SEC repositories)
# ============================================================================

TRANSACTION_NODES = [
  Node(
    name="Transaction",
    description="Business event that triggers financial impact (invoice, payment, deposit, etc.). "
    "Represents what happened in the real world. One transaction can produce multiple "
    "ledger entries over its lifecycle (posting, payment, reversal, adjustment).",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="number", type="STRING"),
      Property(name="amount", type="DOUBLE"),
      Property(name="description", type="STRING"),
      Property(name="date", type="DATE"),
      Property(name="reference_number", type="STRING"),
      Property(
        name="type", type="STRING"
      ),  # invoice, payment, bill, expense, deposit, etc.
      Property(
        name="currency", type="STRING"
      ),  # ISO 4217 currency code (USD, EUR, etc.)
      Property(name="merchant_name", type="STRING"),
      Property(name="category", type="STRING"),
      Property(name="pending", type="BOOLEAN"),
      Property(
        name="is_live", type="BOOLEAN"
      ),  # status <> 'void' — live-row filter for counts/sums
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Entry",
    description="Ledger entry representing the accounting interpretation of a business event. "
    "This is the journal entry — the translation of a transaction into accounting impact. "
    "Each entry independently balances (total debits = total credits) and has its own "
    "posting lifecycle. One transaction can have multiple entries (posting, reversal, adjustment).",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="number", type="STRING"),  # Journal entry ID
      Property(name="memo", type="STRING"),  # Accounting description
      Property(
        name="posting_date", type="DATE"
      ),  # When it hits the books (may differ from transaction date)
      Property(name="type", type="STRING"),  # standard, adjusting, closing, reversing
      Property(name="status", type="STRING"),  # draft, posted, reversed
      Property(
        name="is_live", type="BOOLEAN"
      ),  # status = 'posted' — the only status that affects balances
      Property(
        name="reversal_of", type="STRING"
      ),  # Entry identifier this reverses (null if not a reversal)
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="LineItem",
    description="Individual debit or credit within a ledger entry. Each line item hits "
    "a specific account. The line items within an entry must balance.",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="debit_amount", type="DOUBLE"),
      Property(name="credit_amount", type="DOUBLE"),
      Property(
        name="has_dimensions", type="BOOLEAN"
      ),  # True if line item has dimensional breakdowns
      Property(
        name="dimension_count", type="INT64"
      ),  # Number of dimensional qualifiers (0=no dimensions)
      Property(
        name="is_live", type="BOOLEAN"
      ),  # denormalized parent Entry.is_live (parent status = 'posted')
      Property(name="updated_at", type="STRING"),
    ],
  ),
]

TRANSACTION_RELATIONSHIPS = [
  Relationship(
    name="ENTITY_HAS_TRANSACTION",
    from_node="Entity",
    to_node="Transaction",
    description="Entity has financial transactions (business events)",
    properties=[
      Property(name="transaction_context", type="STRING"),
    ],
  ),
  # Transaction → Entry (one-to-many: a transaction can produce multiple entries)
  Relationship(
    name="TRANSACTION_HAS_ENTRY",
    from_node="Transaction",
    to_node="Entry",
    description="Transaction produces ledger entries (posting, reversal, adjustment)",
    properties=[
      Property(name="entry_context", type="STRING"),
    ],
  ),
  # Entry → LineItem (one-to-many: each entry has balanced debit/credit lines)
  Relationship(
    name="ENTRY_HAS_LINE_ITEM",
    from_node="Entry",
    to_node="LineItem",
    description="Ledger entry contains line items (debits and credits that must balance)",
    properties=[
      Property(name="line_item_context", type="STRING"),
    ],
  ),
  # Entry → Dimension (fund, trust account, product channel, etc.)
  Relationship(
    name="ENTRY_HAS_DIMENSION",
    from_node="Entry",
    to_node="Dimension",
    description="Ledger entry has dimensional qualifiers (fund, trust account, product channel)",
    properties=[],
  ),
  # Transaction → Dimension (source system, API endpoint, etc.)
  Relationship(
    name="TRANSACTION_HAS_DIMENSION",
    from_node="Transaction",
    to_node="Dimension",
    description="Transaction has dimensional qualifiers (source system, provenance)",
    properties=[],
  ),
  Relationship(
    name="LINE_ITEM_RELATES_TO_ELEMENT",
    from_node="LineItem",
    to_node="Element",
    description="Line item maps to XBRL element for reporting",
    properties=[
      Property(name="mapping_context", type="STRING"),
    ],
  ),
  # LineItem → Dimension relationship (department, class, location, project, etc.)
  Relationship(
    name="LINE_ITEM_HAS_DIMENSION",
    from_node="LineItem",
    to_node="Dimension",
    description="Line item has dimensional qualifiers (department, class, location, project)",
    properties=[],
  ),
  # Entry → Structure provenance (closing entries derived from schedules)
  Relationship(
    name="ENTRY_FROM_SCHEDULE",
    from_node="Entry",
    to_node="Structure",
    description="Closing entry was derived from a schedule structure",
    properties=[],
  ),
  # ── McCarthy bridge ──────────────────────────────────────────────────
  # Event → Transaction is the realization at the graph layer of
  # McCarthy 1982's vision: "accounting should be derived from underlying
  # economic events." OLTP source: transactions.triggered_by_event_id.
  Relationship(
    name="EVENT_TRIGGERS_TRANSACTION",
    from_node="Event",
    to_node="Transaction",
    description="REA bridge — the GL Transaction this Event triggered. Materialized "
    "from transactions.triggered_by_event_id; the edge exists only for transactions "
    "originating from an Event (manual-only transactions have no event).",
    properties=[],
  ),
]

# ============================================================================
# CONTEXT-AWARE LOADING
# ============================================================================


class RoboLedgerContext:
  """Context-aware schema loading for RoboLedger"""

  SEC_REPOSITORY = "sec_repository"
  FULL_ACCOUNTING = "full_accounting"
  TRANSACTION_ONLY = "transaction_only"
  REPORTING_ONLY = "reporting_only"

  @classmethod
  def get_nodes_for_context(cls, context: str) -> list[Node]:
    """Get appropriate nodes based on context"""
    if context == cls.SEC_REPOSITORY or context == cls.REPORTING_ONLY:
      # SEC only has aggregated reports, no transaction data
      return REPORTING_NODES
    elif context == cls.FULL_ACCOUNTING:
      # Complete accounting system needs everything
      return REPORTING_NODES + TRANSACTION_NODES
    elif context == cls.TRANSACTION_ONLY:
      # Some use cases might only need GL
      return TRANSACTION_NODES
    else:
      # Default to full system
      return REPORTING_NODES + TRANSACTION_NODES

  @classmethod
  def get_relationships_for_context(cls, context: str) -> list[Relationship]:
    """Get appropriate relationships based on context"""
    if context == cls.SEC_REPOSITORY or context == cls.REPORTING_ONLY:
      return REPORTING_RELATIONSHIPS
    elif context == cls.FULL_ACCOUNTING:
      return REPORTING_RELATIONSHIPS + TRANSACTION_RELATIONSHIPS
    elif context == cls.TRANSACTION_ONLY:
      return TRANSACTION_RELATIONSHIPS
    else:
      return REPORTING_RELATIONSHIPS + TRANSACTION_RELATIONSHIPS

  @classmethod
  def get_table_names_for_context(cls, context: str) -> set[str]:
    """Get node table names for a given context (useful for filtering)"""
    nodes = cls.get_nodes_for_context(context)
    return {node.name for node in nodes}

  @classmethod
  def get_all_table_names_for_context(
    cls, context: str, include_base: bool = True
  ) -> dict[str, str]:
    """Get all table names (nodes + relationships) for a context.

    IMPORTANT: Returns tables in dependency order - ALL nodes first, then ALL
    relationships. This ensures foreign key constraints are satisfied during
    materialization (nodes must exist before relationships can reference them).

    Args:
        context: One of SEC_REPOSITORY, FULL_ACCOUNTING, TRANSACTION_ONLY, REPORTING_ONLY
        include_base: Whether to include base schema tables (default: True)

    Returns:
        Dictionary mapping table name to entity type ("nodes" or "relationships")
        Order: base nodes, extension nodes, base relationships, extension relationships
    """
    from ..base import (
      BASE_NODES,
      BASE_RELATIONSHIPS,
      REPORTING_ONLY_EXCLUDED_NODES,
      REPORTING_ONLY_EXCLUDED_RELATIONSHIPS,
    )

    tables: dict[str, str] = {}

    # Get context-specific tables
    ext_nodes = cls.get_nodes_for_context(context)
    ext_relationships = cls.get_relationships_for_context(context)

    # Reporting-only contexts (SEC) skip the REA/trait base tables so this
    # materialization list matches the schema the loader builds — neither
    # creates nor materializes the empty Event/Agent/Trait tables.
    reporting_only = context in (cls.SEC_REPOSITORY, cls.REPORTING_ONLY)
    excluded_nodes = REPORTING_ONLY_EXCLUDED_NODES if reporting_only else frozenset()
    excluded_rels = (
      REPORTING_ONLY_EXCLUDED_RELATIONSHIPS if reporting_only else frozenset()
    )

    # Add ALL nodes first (base nodes before extension nodes)
    if include_base:
      for node in BASE_NODES:
        if node.name not in excluded_nodes:
          tables[node.name] = "nodes"

    for node in ext_nodes:
      tables[node.name] = "nodes"

    # Then add ALL relationships (base before extension)
    if include_base:
      for rel in BASE_RELATIONSHIPS:
        if rel.name not in excluded_rels:
          tables[rel.name] = "relationships"

    for rel in ext_relationships:
      tables[rel.name] = "relationships"

    return tables


# ============================================================================
# EXTENSION DEFINITION (for backward compatibility)
# ============================================================================

# Default to full accounting system for entity graphs
EXTENSION_NODES = REPORTING_NODES + TRANSACTION_NODES
EXTENSION_RELATIONSHIPS = REPORTING_RELATIONSHIPS + TRANSACTION_RELATIONSHIPS

# Export all components for flexibility
__all__ = [
  # Full schema (default)
  "EXTENSION_NODES",
  "EXTENSION_RELATIONSHIPS",
  # Section-specific exports
  "REPORTING_NODES",
  "REPORTING_RELATIONSHIPS",
  "TRANSACTION_NODES",
  "TRANSACTION_RELATIONSHIPS",
  # Context-aware loading
  "RoboLedgerContext",
]
