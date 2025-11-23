"""
Base Schema for LadybugDB

Defines common nodes and relationships shared across all applications.
This serves as the foundation that all application-specific schemas extend.
"""

from .models import Node, Relationship, Property

# Base Schema Definition - Common Foundation
# NOTE: Platform metadata (users, connections, graph metadata) are stored in PostgreSQL,
# not in the LadybugDB graph database. This schema contains only business domain concepts.
BASE_NODES = [
  Node(
    name="Entity",
    description="Core entity representing organizations, companies, subsidiaries, or other business units",
    properties=[
      Property(
        name="identifier", type="STRING", is_primary_key=True
      ),  # UUIDv7 primary key
      Property(name="uri", type="STRING"),  # XBRL entity URI
      Property(name="scheme", type="STRING"),  # XBRL entity scheme
      Property(name="cik", type="STRING"),  # SEC Central Index Key
      Property(name="ticker", type="STRING"),  # Stock ticker symbol
      Property(name="exchange", type="STRING"),  # Stock exchange (NYSE, NASDAQ, etc.)
      Property(name="name", type="STRING"),
      Property(name="legal_name", type="STRING"),
      Property(name="industry", type="STRING"),
      Property(
        name="entity_type", type="STRING"
      ),  # corporation, llc, partnership, subsidiary, operating
      Property(name="sic", type="STRING"),  # Standard Industrial Classification
      Property(name="sic_description", type="STRING"),
      Property(name="category", type="STRING"),  # Large accelerated filer, etc.
      Property(name="state_of_incorporation", type="STRING"),
      Property(name="fiscal_year_end", type="STRING"),
      Property(name="ein", type="STRING"),  # Employer Identification Number
      Property(name="tax_id", type="STRING"),
      Property(name="lei", type="STRING"),  # Legal Entity Identifier (ISO 17442)
      Property(name="phone", type="STRING"),  # Main phone number
      Property(name="website", type="STRING"),
      Property(name="status", type="STRING"),  # active, inactive, dissolved
      Property(
        name="is_parent", type="BOOLEAN"
      ),  # True if this is the top-level entity
      Property(
        name="parent_entity_id", type="STRING"
      ),  # Reference to parent entity if subsidiary
      Property(name="created_at", type="STRING"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Period",
    description="Time period for financial data and business cycles",
    properties=[
      Property(
        name="identifier", type="STRING", is_primary_key=True
      ),  # Report-specific identifier
      Property(name="uri", type="STRING"),  # Time period URI
      Property(name="instant_date", type="STRING"),  # DEPRECATED - use end_date instead
      Property(
        name="start_date", type="STRING"
      ),  # NULL for instant, populated for duration
      Property(
        name="end_date", type="STRING"
      ),  # Always populated (instant or duration end)
      Property(
        name="forever_date", type="BOOLEAN"
      ),  # Changed to BOOLEAN to match actual usage
      Property(name="fiscal_year", type="INT32"),  # Fiscal year for easier filtering
      Property(name="fiscal_quarter", type="STRING"),  # Q1-Q4, H1-H2, M9, etc.
      Property(name="is_annual", type="BOOLEAN"),  # True for ~1 year periods
      Property(name="is_quarterly", type="BOOLEAN"),  # True for ~3 month periods
      Property(name="days_in_period", type="INT32"),  # Actual duration in days
      Property(
        name="period_type", type="STRING"
      ),  # quarterly, semi_annual, nine_months, annual, other
      Property(
        name="is_ytd", type="BOOLEAN"
      ),  # True for cumulative year-to-date periods
    ],
  ),
  Node(
    name="Unit",
    description="Unit of measurement for facts and values",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="measure", type="STRING"),
      Property(name="value", type="STRING"),
      Property(name="numerator_uri", type="STRING"),
      Property(name="denominator_uri", type="STRING"),
    ],
  ),
  # XBRL Taxonomy Nodes - Global entities shared across all reports
  Node(
    name="Element",
    description="XBRL taxonomy element definition (global across all reports)",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),  # indexed
      Property(name="qname", type="STRING"),  # indexed
      Property(name="name", type="STRING"),
      Property(name="period_type", type="STRING"),  # indexed
      Property(name="type", type="STRING"),
      Property(name="balance", type="STRING"),
      Property(name="is_abstract", type="BOOLEAN"),
      Property(name="is_dimension_item", type="BOOLEAN"),
      Property(name="is_domain_member", type="BOOLEAN"),
      Property(name="is_hypercube_item", type="BOOLEAN"),
      Property(name="is_integer", type="BOOLEAN"),
      Property(name="is_numeric", type="BOOLEAN"),
      Property(name="is_shares", type="BOOLEAN"),
      Property(name="is_fraction", type="BOOLEAN"),
      Property(name="is_textblock", type="BOOLEAN"),
      Property(name="substitution_group", type="STRING"),
      Property(name="item_type", type="STRING"),
      Property(name="classification", type="STRING"),  # indexed
    ],
  ),
  Node(
    name="Label",
    description="Human-readable labels for XBRL elements (global across all reports)",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="value", type="STRING"),
      Property(name="type", type="STRING"),
      Property(name="language", type="STRING"),
    ],
  ),
  Node(
    name="Reference",
    description="Authoritative references for XBRL elements (global across all reports)",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="value", type="STRING"),
      Property(name="type", type="STRING"),
    ],
  ),
  Node(
    name="Taxonomy",
    description="Global XBRL taxonomy definitions (us-gaap, ifrs-full, etc.)",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="uri", type="STRING"),
      Property(name="name", type="STRING"),  # us-gaap, ifrs-full, etc.
      Property(name="version", type="STRING"),  # 2023, 2024, etc.
      Property(name="namespace", type="STRING"),
      Property(name="description", type="STRING"),
    ],
  ),
]

# Base Relationships - Common Foundation
# NOTE: Platform relationships (user access, connections) are managed in PostgreSQL.
# This schema contains only business domain relationships.
BASE_RELATIONSHIPS = [
  # Evolution and lineage tracking
  Relationship(
    name="ENTITY_EVOLVED_FROM",
    from_node="Entity",
    to_node="Entity",
    description="Entity evolution and transformation tracking",
    properties=[
      Property(name="evolution_type", type="STRING"),  # merger, acquisition, spin-off
      Property(name="evolution_date", type="STRING"),
      Property(name="notes", type="STRING"),
      Property(name="created_at", type="STRING"),
    ],
  ),
  # Hierarchical entity relationships
  Relationship(
    name="ENTITY_OWNS_ENTITY",
    from_node="Entity",
    to_node="Entity",
    description="Parent-subsidiary entity ownership relationships",
    properties=[
      Property(name="ownership_type", type="STRING"),  # subsidiary, division, branch
      Property(name="ownership_percentage", type="DOUBLE"),
      Property(name="effective_date", type="STRING"),
      Property(name="created_at", type="STRING"),
    ],
  ),
  # XBRL Core Relationships - Global relationships for shared XBRL concepts
  Relationship(
    name="ELEMENT_HAS_LABEL",
    from_node="Element",
    to_node="Label",
    description="Element has human-readable labels (global taxonomy concepts)",
    properties=[
      Property(name="label_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ELEMENT_HAS_REFERENCE",
    from_node="Element",
    to_node="Reference",
    description="Element has authoritative references (global taxonomy concepts)",
    properties=[
      Property(name="reference_context", type="STRING"),
    ],
  ),
  # Global Taxonomy Structure Relationships
  Relationship(
    name="ELEMENT_IN_TAXONOMY",
    from_node="Element",
    to_node="Taxonomy",
    description="Element belongs to a global taxonomy (us-gaap, ifrs-full, etc.)",
    properties=[
      Property(name="taxonomy_context", type="STRING"),
    ],
  ),
  Relationship(
    name="TAXONOMY_HAS_LABEL",
    from_node="Taxonomy",
    to_node="Label",
    description="Global taxonomy defines labels",
    properties=[
      Property(name="label_context", type="STRING"),
    ],
  ),
  Relationship(
    name="TAXONOMY_HAS_REFERENCE",
    from_node="Taxonomy",
    to_node="Reference",
    description="Global taxonomy has authoritative references",
    properties=[
      Property(name="reference_context", type="STRING"),
    ],
  ),
]
