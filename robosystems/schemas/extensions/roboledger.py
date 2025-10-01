"""
RoboLedger Schema Extension for Kuzu

Complete accounting system with both transaction and reporting capabilities.

This unified schema includes:
- 📊 Financial Reporting (XBRL, SEC filings, financial statements)
- 📒 General Ledger (transactions, journal entries, accounts)
- 💼 Business Processes (workflows, disclosures, compliance)

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

from typing import List, Set
from ..models import Node, Relationship, Property

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
      Property(name="report_date", type="STRING"),
      Property(name="acceptance_date", type="STRING"),
      Property(name="period_end_date", type="STRING"),
      Property(name="entity_identifier", type="STRING"),
      Property(name="is_inline_xbrl", type="BOOLEAN"),
      Property(name="xbrl_processor_version", type="STRING"),
      Property(name="processed", type="BOOLEAN"),
      Property(name="failed", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Fact",
    description="Individual fact/data point from financial reports",
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
    ],
  ),
  Node(
    name="Structure",
    description="XBRL taxonomy structure",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="network_uri", type="STRING"),
      Property(name="definition", type="STRING"),
      Property(name="number", type="STRING"),
      Property(name="type", type="STRING"),
      Property(name="name", type="STRING"),
    ],
  ),
  Node(
    name="FactDimension",
    description="Dimensional information for facts",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="axis_uri", type="STRING"),
      Property(name="member_uri", type="STRING"),
      Property(name="type", type="STRING"),
      Property(name="is_explicit", type="BOOLEAN"),
      Property(name="is_typed", type="BOOLEAN"),
    ],
  ),
  Node(
    name="Association",
    description="Associations between elements in taxonomies",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="arcrole", type="STRING"),
      Property(name="order_value", type="DOUBLE"),
      Property(name="association_type", type="STRING"),
      Property(name="weight", type="DOUBLE"),
      Property(name="root", type="STRING"),
      Property(name="preferred_label", type="STRING"),
    ],
  ),
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
  Relationship(
    name="STRUCTURE_HAS_TAXONOMY",
    from_node="Structure",
    to_node="Taxonomy",
    description="Structure belongs to taxonomy",
    properties=[
      Property(name="taxonomy_context", type="STRING"),
    ],
  ),
  # Additional dimension relationships for XBRL fact processing
  Relationship(
    name="FACT_HAS_DIMENSION",
    from_node="Fact",
    to_node="FactDimension",
    description="Fact has dimensional qualifiers",
    properties=[],
  ),
  Relationship(
    name="FACT_DIMENSION_AXIS_ELEMENT",
    from_node="FactDimension",
    to_node="Element",
    description="Dimension axis element reference",
    properties=[],
  ),
  Relationship(
    name="FACT_DIMENSION_MEMBER_ELEMENT",
    from_node="FactDimension",
    to_node="Element",
    description="Dimension member element reference",
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
    name="REPORT_HAS_FACT_SET",
    from_node="Report",
    to_node="FactSet",
    description="Report contains fact sets",
    properties=[
      Property(name="fact_set_context", type="STRING"),
    ],
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
  Relationship(
    name="STRUCTURE_HAS_ASSOCIATION",
    from_node="Structure",
    to_node="Association",
    description="Structure contains element associations",
    properties=[
      Property(name="association_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ASSOCIATION_HAS_FROM_ELEMENT",
    from_node="Association",
    to_node="Element",
    description="Association from element (parent)",
    properties=[],
  ),
  Relationship(
    name="ASSOCIATION_HAS_TO_ELEMENT",
    from_node="Association",
    to_node="Element",
    description="Association to element (child)",
    properties=[],
  ),
]

# ============================================================================
# TRANSACTION SECTION (General Ledger/Bookkeeping)
# Used by: Entity graphs only (NOT in SEC repositories)
# ============================================================================

TRANSACTION_NODES = [
  Node(
    name="Account",
    description="Chart of accounts for general ledger",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="account_number", type="STRING"),
      Property(name="name", type="STRING"),
      Property(
        name="account_type", type="STRING"
      ),  # asset, liability, equity, revenue, expense
      Property(name="balance", type="STRING"),
      Property(name="parent_account_id", type="STRING"),
      Property(name="is_active", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Transaction",
    description="Financial transactions and journal entries",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="transaction_number", type="STRING"),
      Property(name="amount", type="DOUBLE"),
      Property(name="description", type="STRING"),
      Property(name="date", type="DATE"),
      Property(name="transaction_date", type="DATE"),
      Property(name="reference_number", type="STRING"),
      Property(name="transaction_type", type="STRING"),  # debit, credit
      Property(name="type", type="STRING"),
      Property(name="number", type="STRING"),
      Property(name="sync_hash", type="STRING"),
      Property(name="currency", type="STRING"),
      # Plaid-specific properties
      Property(name="plaid_merchant_name", type="STRING"),
      Property(name="plaid_category", type="STRING"),
      Property(name="plaid_pending", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="LineItem",
    description="Individual transaction line items for detailed accounting",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="debit_amount", type="DOUBLE"),
      Property(name="credit_amount", type="DOUBLE"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Process",
    description="Business processes and workflows",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(name="process_type", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="status", type="STRING"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Disclosure",
    description="SEC disclosure requirements and compliance",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="name", type="STRING"),
      Property(name="sec_type", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="required", type="BOOLEAN"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
]

TRANSACTION_RELATIONSHIPS = [
  Relationship(
    name="ENTITY_HAS_ACCOUNT",
    from_node="Entity",
    to_node="Account",
    description="Entity has chart of accounts",
    properties=[
      Property(name="account_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_TRANSACTION",
    from_node="Entity",
    to_node="Transaction",
    description="Entity has financial transactions",
    properties=[
      Property(name="transaction_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ACCOUNT_HAS_TRANSACTION",
    from_node="Account",
    to_node="Transaction",
    description="Account has transactions",
    properties=[
      Property(name="debit_amount", type="DOUBLE"),
      Property(name="credit_amount", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="TRANSACTION_HAS_LINE_ITEM",
    from_node="Transaction",
    to_node="LineItem",
    description="Transaction contains line items",
    properties=[
      Property(name="line_item_context", type="STRING"),
    ],
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
  Relationship(
    name="PROCESS_HAS_DISCLOSURE",
    from_node="Process",
    to_node="Disclosure",
    description="Process generates disclosure requirements",
    properties=[
      Property(name="disclosure_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ACCOUNT_PARENT_CHILD",
    from_node="Account",
    to_node="Account",
    description="Account hierarchy relationship",
    properties=[
      Property(name="hierarchy_level", type="INT64"),
    ],
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
  def get_nodes_for_context(cls, context: str) -> List[Node]:
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
  def get_relationships_for_context(cls, context: str) -> List[Relationship]:
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
  def get_table_names_for_context(cls, context: str) -> Set[str]:
    """Get table names for a given context (useful for filtering)"""
    nodes = cls.get_nodes_for_context(context)
    return {node.name for node in nodes}


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
