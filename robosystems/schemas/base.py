"""
Base Schema for LadybugDB

Defines common nodes and relationships shared across all applications.
This serves as the foundation that all application-specific schemas extend.
"""

from .models import Node, Property, Relationship

# Base Schema Definition — Common Foundation
#
# INVARIANT 1 (Aspirational base): Base contains concepts that are universally
# applicable to the ontology regardless of current consumer count. Period, Unit,
# Element, Taxonomy, Dimension, Association, Structure are declared here even
# though only roboledger currently populates most of them, because the other
# planned extensions (roboinvestor, roboscm, robohrm, robofo, roboepm) will
# grow into them. The rule for promoting a concept into base is "is it
# universally applicable" — NOT "do two extensions use it today." Waiting for a
# second consumer before promoting turns every promotion into a breaking
# refactor against materialized data.
#
# INVARIANT 2 (Aspects attach only to measured events): Period, Unit, and
# Dimension are aspects that qualify measured observations (Fact in reporting,
# LineItem dimensional tags in ledger, future Trade in investor). They never
# attach to declarative nodes like Entity, Report, Taxonomy, Portfolio. Any
# edge of the form (Entity|Report|Taxonomy|Portfolio)_HAS_(Period|Unit|Dimension)
# is a category error — rewrite as a node property or as a query over the
# underlying events. Related: the same conceptual type (currency, time) can
# appear as a static attribute on a declarative node OR as an aspect edge on
# a measured event. These are distinct roles — declaration vs observation —
# and both are legitimate.
#
# NOTE: Platform metadata (users, connections, graph metadata) are stored in
# PostgreSQL, not in the LadybugDB graph database. This schema contains only
# business domain concepts.
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
      Property(name="tax_id", type="STRING"),  # Tax ID (EIN for US entities)
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
      Property(
        name="start_date", type="STRING"
      ),  # NULL for instant/forever, populated for duration
      Property(
        name="end_date", type="STRING"
      ),  # Always populated except forever (instant date or duration end)
      Property(
        name="calendar_year", type="INT32"
      ),  # Calendar year from end_date (not entity fiscal year)
      Property(
        name="calendar_quarter", type="STRING"
      ),  # Q1-Q4, H1-H2, M9, FY based on calendar months (not entity fiscal quarter)
      Property(
        name="days_in_period", type="INT32"
      ),  # Actual duration in days (0 for instant)
      Property(name="period_type", type="STRING"),  # instant, duration, forever
      Property(
        name="duration_type", type="STRING"
      ),  # quarterly, semi_annual, nine_months, annual, other (NULL for instant/forever)
      Property(
        name="calendar_period_key", type="STRING"
      ),  # Normalized calendar key for cross-company matching (e.g., "2024", "2024Q4", "2024-12-31")
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
      Property(name="canonical_concept", type="STRING"),
      Property(name="canonical_confidence", type="DOUBLE"),
      Property(name="embedding", type="FLOAT[384]"),
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
      Property(name="embedding", type="FLOAT[384]"),
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
      Property(
        name="taxonomy_type", type="STRING"
      ),  # chart_of_accounts, reporting, mapping, schedule
    ],
  ),
  Node(
    name="Dimension",
    description="Dimensional qualifier for financial data. Represents axis-member pairs "
    "for segmentation (e.g., business segment, geography, department, project). "
    "Used by both XBRL facts and accounting line items.",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      # Generic properties (all dimensions)
      Property(
        name="axis", type="STRING"
      ),  # Human-readable axis name (e.g., "Segment", "Department")
      Property(
        name="member", type="STRING"
      ),  # Human-readable member value (e.g., "North America", "Engineering")
      Property(
        name="dimension_type", type="STRING"
      ),  # xbrl_explicit, xbrl_typed, class, department, location, project, custom
      # XBRL-specific properties (null for non-XBRL dimensions)
      Property(name="axis_uri", type="STRING"),
      Property(name="member_uri", type="STRING"),
      Property(name="type", type="STRING"),  # XBRL context type (segment/scenario)
      Property(name="is_explicit", type="BOOLEAN"),
      Property(name="is_typed", type="BOOLEAN"),
    ],
  ),
  # XBRL Taxonomy Infrastructure — Structure, Association, Classification
  # These are base ontology concepts (taxonomy link networks and pattern metadata),
  # not roboledger-specific. Any extension that works with a formal taxonomy
  # (XBRL, RDF, etc.) traverses these nodes.
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
      Property(name="canonical_type", type="STRING"),
      Property(name="canonical_confidence", type="DOUBLE"),
      Property(name="embedding", type="FLOAT[384]"),
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
    name="Classification",
    description="Structural pattern classification for associations (RollUp, RollForward, Hierarchy, etc.)",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="type", type="STRING"),
      Property(name="source", type="STRING"),
      Property(name="confidence", type="DOUBLE"),
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
  # Dimension → Element relationships (axis and member definitions)
  Relationship(
    name="DIMENSION_HAS_AXIS_ELEMENT",
    from_node="Dimension",
    to_node="Element",
    description="Dimension axis element reference (defines what is being sliced)",
    properties=[],
  ),
  Relationship(
    name="DIMENSION_HAS_MEMBER_ELEMENT",
    from_node="Dimension",
    to_node="Element",
    description="Dimension member element reference (defines the specific slice value)",
    properties=[],
  ),
  # Taxonomy Structure / Association / Classification infrastructure
  # (relocated from roboledger — these are base ontology concepts, not reporting)
  Relationship(
    name="STRUCTURE_HAS_TAXONOMY",
    from_node="Structure",
    to_node="Taxonomy",
    description="Structure belongs to taxonomy",
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
  Relationship(
    name="ASSOCIATION_HAS_CLASSIFICATION",
    from_node="Association",
    to_node="Classification",
    description="Association has structural pattern classification (many-to-many)",
    properties=[],
  ),
  # Entity ↔ Taxonomy — the core "entity reports under a taxonomy" link
  Relationship(
    name="ENTITY_HAS_TAXONOMY",
    from_node="Entity",
    to_node="Taxonomy",
    description="Entity reports under this taxonomy (US GAAP, IFRS, or custom extension). "
    "Multiple taxonomies may apply across different bases (reporting, "
    "chart_of_accounts, mapping, schedule). Within each basis, at most one "
    "edge should have is_primary=true.",
    properties=[
      Property(name="is_primary", type="BOOLEAN"),  # Primary taxonomy within its basis
      Property(
        name="basis", type="STRING"
      ),  # reporting | chart_of_accounts | mapping | schedule
      Property(
        name="effective_from", type="STRING"
      ),  # Adoption start (fiscal year boundary)
      Property(name="effective_to", type="STRING"),  # Adoption end (null = current)
      Property(
        name="adoption_context", type="STRING"
      ),  # required_by_regulation | voluntary | contractual
    ],
  ),
  # Taxonomy extension chain (version upgrades, entity extensions, industry overlays)
  # NOTE: single-parent by design — a taxonomy has exactly one parent in the
  # extension chain. Secondary "extends" relationships should be modeled as
  # mapping taxonomies via source_taxonomy_id / target_taxonomy_id on the
  # Taxonomy OLTP model, not as additional TAXONOMY_EXTENDS_TAXONOMY edges.
  Relationship(
    name="TAXONOMY_EXTENDS_TAXONOMY",
    from_node="Taxonomy",
    to_node="Taxonomy",
    description="Taxonomy derives from a parent (version upgrade, entity extension, "
    "industry overlay, jurisdiction). Models us-gaap-2024 extending us-gaap-2023, "
    "or an entity's custom extension taxonomy extending the standard. "
    "Single-parent only — use mapping taxonomies for secondary relationships.",
    properties=[
      Property(
        name="extension_type", type="STRING"
      ),  # version | entity_extension | industry | jurisdiction
      Property(name="effective_date", type="STRING"),
    ],
  ),
]
