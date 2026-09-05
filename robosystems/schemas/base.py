"""
Base Schema for LadybugDB

The common nodes and relationships every extension schema builds on.
"""

from .models import Node, Property, Relationship
from .xbrl import xbrl_node_properties, xbrl_relationship_properties

# Base Schema Definition — Common Foundation
#
# INVARIANT 1 (Aspirational base): Base contains concepts that are universally
# applicable to the ontology regardless of current consumer count. Period, Unit,
# Element, Taxonomy, Dimension, Association, Structure are declared here even
# though only roboledger currently populates most of them, because further
# extensions grow into them. The rule for promoting a
# concept into base is "is it universally applicable" — NOT "do two extensions
# use it today." Waiting for a second consumer before promoting turns every
# promotion into a breaking refactor against materialized data.
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
    description="Core entity representing organizations, companies, subsidiaries, or other "
    "business units. entity_type is corporation, llc, partnership, subsidiary or "
    "operating; status is active, inactive or dissolved; is_parent marks the graph's "
    "top-level entity and parent_entity_id points a subsidiary at it; tax_id is the "
    "EIN for US entities and lei the ISO 17442 identifier.",
    properties=xbrl_node_properties("Entity"),
  ),
  Node(
    name="Period",
    description="Time period for financial data and business cycles. period_type is "
    "instant, duration or forever; start_date is null except for a duration, and "
    "end_date carries the instant date; duration_type buckets a duration as "
    "quarterly, semi_annual, nine_months, annual or other; calendar_year, "
    'calendar_quarter (Q1-Q4, H1-H2, M9, FY) and calendar_period_key ("2024", '
    '"2024Q4", "2024-12-31") place the period on the calendar for cross-company '
    "matching and are not the entity's fiscal calendar.",
    properties=xbrl_node_properties("Period"),
  ),
  Node(
    name="Unit",
    description="Unit of measurement for facts and values",
    properties=xbrl_node_properties("Unit"),
  ),
  # XBRL Taxonomy Nodes - Global entities shared across all reports
  Node(
    name="Element",
    description="XBRL taxonomy element definition (global across all reports)",
    properties=xbrl_node_properties("Element"),
  ),
  Node(
    name="Label",
    description="Human-readable labels for XBRL elements (global across all reports)",
    properties=xbrl_node_properties("Label"),
  ),
  Node(
    name="Reference",
    description="Authoritative references for XBRL elements (global across all reports)",
    properties=xbrl_node_properties("Reference"),
  ),
  Node(
    name="Taxonomy",
    description="Global XBRL taxonomy definitions (us-gaap, ifrs-full, etc.). "
    "taxonomy_type is chart_of_accounts, reporting, mapping or schedule.",
    properties=xbrl_node_properties("Taxonomy"),
  ),
  Node(
    name="Dimension",
    description="Dimensional qualifier for financial data. Represents axis-member pairs "
    "for segmentation (e.g., business segment, geography, department, project). "
    "Used by both XBRL facts and accounting line items. dimension_type is "
    "xbrl_explicit, xbrl_typed, class, department, location, project or custom; "
    "axis and member are the human-readable names, and axis_uri, member_uri and "
    "type (segment or scenario) the XBRL-specific fields, null for non-XBRL "
    "dimensions.",
    properties=xbrl_node_properties("Dimension"),
  ),
  # XBRL Taxonomy Infrastructure — Structure, Association, Trait, Classification
  # These are base ontology concepts (taxonomy link networks and pattern/trait
  # metadata), not roboledger-specific. Any extension that works with a formal
  # taxonomy (XBRL, RDF, etc.) traverses these nodes.
  Node(
    name="Structure",
    description="XBRL taxonomy structure",
    properties=xbrl_node_properties("Structure"),
  ),
  Node(
    name="Association",
    description="Associations between elements in taxonomies",
    properties=xbrl_node_properties("Association"),
  ),
  Node(
    name="Trait",
    description="FASB us-gaap metamodel vocabulary member — describes what an element IS "
    "(e.g. elementsOfFinancialStatements=asset, liquidity=current, activityType=operatingActivity). "
    "Covers 25 element-side categories: 24 metamodel axes + flowClassification.",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="category", type="STRING"),
      Property(name="type", type="STRING"),
      Property(name="source", type="STRING"),
      Property(name="confidence", type="DOUBLE"),
    ],
  ),
  Node(
    name="Classification",
    description="Structural pattern classification for associations — describes what KIND OF PATTERN "
    "a set of associations forms (RollUp, RollForward, AssetsRollUp, etc.). "
    "Covers 3 association-side categories: concept_arrangement, member_arrangement, named_disclosure.",
    properties=xbrl_node_properties("Classification"),
  ),
  # ── REA primitives ──────────────────────────────────────────────────
  # Agent + Event are universal REA primitives, promoted to base per
  # INVARIANT 1. Today only roboledger populates them. Shared-repository
  # graphs (e.g. SEC) get empty node tables — harmless, materialization
  # writes no rows.
  Node(
    name="Agent",
    description="REA counterparty — the external actor a business event is exchanged with "
    "(customer, vendor, employee, bank, regulator, …). Distinct from Entity, which is "
    "the reporting entity that owns the graph. Mirrored from the extensions OLTP "
    "agents table; JSONB columns (address, metadata) and connection scoping are not "
    "materialized.",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),  # ULID "agt_*"
      Property(name="uri", type="STRING"),  # canonical URI for cross-graph reference
      Property(
        name="agent_type", type="STRING"
      ),  # customer | vendor | employee | owner | supplier | government | lender | self | other
      Property(name="name", type="STRING"),
      Property(name="legal_name", type="STRING"),
      Property(name="tax_id", type="STRING"),
      Property(name="registration_number", type="STRING"),
      Property(name="duns", type="STRING"),
      Property(name="lei", type="STRING"),
      Property(name="email", type="STRING"),
      Property(name="phone", type="STRING"),
      Property(
        name="source", type="STRING"
      ),  # native | quickbooks | xero | plaid | ...
      Property(name="external_id", type="STRING"),
      Property(name="is_active", type="BOOLEAN"),
      Property(name="is_1099_recipient", type="BOOLEAN"),
      Property(name="created_at", type="STRING"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Event",
    description="REA economic or support event — 'something happened in the real world'. "
    "The canonical record from which GL postings (Transaction → Entry → LineItem) are "
    "derived. Carries canonical action verb refinement (event_action) plus the duality "
    "and correction chains as graph edges. Mirrored from the extensions OLTP events "
    "table; JSONB metadata is not materialized. amount is converted from BigInteger "
    "cents to DOUBLE currency-major (matches Transaction.amount convention).",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),  # ULID "evt_*"
      Property(name="uri", type="STRING"),
      Property(name="event_type", type="STRING"),  # open vocabulary
      Property(
        name="event_category", type="STRING"
      ),  # economic: sales|purchase|... ; support: control|approval|...
      Property(name="event_class", type="STRING"),  # economic | support | operational
      Property(
        name="event_action", type="STRING"
      ),  # Canonical 19-verb action vocabulary (see EVENT_ACTIONS in models)
      Property(
        name="resource_type", type="STRING"
      ),  # goods | services | money | right | obligation | information | labor
      Property(name="occurred_at", type="STRING"),  # ISO 8601
      Property(name="effective_at", type="STRING"),
      Property(
        name="status", type="STRING"
      ),  # captured | classified | committed | pending | fulfilled | voided | superseded
      Property(
        name="is_live", type="BOOLEAN"
      ),  # status NOT IN ('voided','superseded') — safe default for counts/sums
      Property(
        name="source", type="STRING"
      ),  # manual|system|schedule | connected provider | registered external source_name
      Property(name="external_id", type="STRING"),
      Property(name="external_url", type="STRING"),
      Property(name="amount", type="DOUBLE"),  # currency-major; OLTP cents / 100
      Property(name="currency", type="STRING"),  # ISO 4217
      Property(name="description", type="STRING"),
      Property(name="created_at", type="STRING"),
      Property(name="created_by", type="STRING"),
    ],
  ),
]

# Base Relationships - Common Foundation
# NOTE: Platform relationships (user access, connections) are managed in PostgreSQL.
# This schema contains only business domain relationships.
BASE_RELATIONSHIPS = [
  # NOTE: parent-subsidiary ownership has no edge here — nothing writes one on
  # either path (SEC or OLTP materialization). Add ENTITY_OWNS_ENTITY when
  # multi-entity consolidation ships; OLTP entities.parent_entity_id is the
  # designated source.
  # XBRL Core Relationships - Global relationships for shared XBRL concepts
  Relationship(
    name="ELEMENT_HAS_LABEL",
    from_node="Element",
    to_node="Label",
    description="Element has human-readable labels (global taxonomy concepts)",
    properties=xbrl_relationship_properties("ELEMENT_HAS_LABEL"),
  ),
  Relationship(
    name="ELEMENT_HAS_REFERENCE",
    from_node="Element",
    to_node="Reference",
    description="Element has authoritative references (global taxonomy concepts)",
    properties=xbrl_relationship_properties("ELEMENT_HAS_REFERENCE"),
  ),
  # Global Taxonomy Structure Relationships
  # NOTE: there is no direct element↔taxonomy membership edge — membership is
  # derived through Structure associations (STRUCTURE_HAS_TAXONOMY +
  # STRUCTURE_HAS_ASSOCIATION).
  Relationship(
    name="TAXONOMY_HAS_LABEL",
    from_node="Taxonomy",
    to_node="Label",
    description="Taxonomy defines a label FOR a specific element. On the SEC "
    "shared repository the from-Taxonomy is the filer's per-report extension "
    "taxonomy, so this edge is report-scoped: it says 'this report labels "
    "element_uri as <Label> under <Label.type> role'. The `element_uri` property "
    "re-attaches the element the label is for — without it, the report-scoped "
    "lookup (taxonomy ∩ element via the content-addressed shared Label pool) "
    "bleeds cross-concept label texts. URI (not qname) is the join key so it "
    "stays exact for filer-extension elements whose prefix isn't reconstructible "
    "downstream.",
    properties=xbrl_relationship_properties("TAXONOMY_HAS_LABEL"),
  ),
  Relationship(
    name="TAXONOMY_HAS_REFERENCE",
    from_node="Taxonomy",
    to_node="Reference",
    description="Global taxonomy has authoritative references",
    properties=xbrl_relationship_properties("TAXONOMY_HAS_REFERENCE"),
  ),
  # Dimension → Element relationships (axis and member definitions)
  Relationship(
    name="DIMENSION_HAS_AXIS_ELEMENT",
    from_node="Dimension",
    to_node="Element",
    description="Dimension axis element reference (defines what is being sliced)",
    properties=xbrl_relationship_properties("DIMENSION_HAS_AXIS_ELEMENT"),
  ),
  Relationship(
    name="DIMENSION_HAS_MEMBER_ELEMENT",
    from_node="Dimension",
    to_node="Element",
    description="Dimension member element reference (defines the specific slice value)",
    properties=xbrl_relationship_properties("DIMENSION_HAS_MEMBER_ELEMENT"),
  ),
  # Taxonomy Structure / Association / Classification infrastructure
  # (relocated from roboledger — these are base ontology concepts, not reporting)
  Relationship(
    name="STRUCTURE_HAS_TAXONOMY",
    from_node="Structure",
    to_node="Taxonomy",
    description="Structure belongs to taxonomy",
    properties=xbrl_relationship_properties("STRUCTURE_HAS_TAXONOMY"),
  ),
  Relationship(
    name="STRUCTURE_HAS_ASSOCIATION",
    from_node="Structure",
    to_node="Association",
    description="Structure contains element associations",
    properties=xbrl_relationship_properties("STRUCTURE_HAS_ASSOCIATION"),
  ),
  Relationship(
    name="ASSOCIATION_HAS_FROM_ELEMENT",
    from_node="Association",
    to_node="Element",
    description="Association from element (parent)",
    properties=xbrl_relationship_properties("ASSOCIATION_HAS_FROM_ELEMENT"),
  ),
  Relationship(
    name="ASSOCIATION_HAS_TO_ELEMENT",
    from_node="Association",
    to_node="Element",
    description="Association to element (child)",
    properties=xbrl_relationship_properties("ASSOCIATION_HAS_TO_ELEMENT"),
  ),
  Relationship(
    name="ELEMENT_HAS_TRAIT",
    from_node="Element",
    to_node="Trait",
    description="Element has FASB metamodel trait assignment (many-to-many; one edge per axis category)",
    properties=[],
  ),
  Relationship(
    name="ASSOCIATION_HAS_CLASSIFICATION",
    from_node="Association",
    to_node="Classification",
    description="Association has structural pattern classification (many-to-many)",
    properties=xbrl_relationship_properties("ASSOCIATION_HAS_CLASSIFICATION"),
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
  # ── REA edges ────────────────────────────────────────────────────────
  # Entity owns its Agent/Event records. REA duality + correction chains
  # are self-referential on Event.
  Relationship(
    name="ENTITY_HAS_AGENT",
    from_node="Entity",
    to_node="Agent",
    description="Entity owns this counterparty agent record. Materialized as a "
    "single-Entity-per-graph fanout — every Agent row is owned by the graph's "
    "reporting entity.",
    properties=[],
  ),
  Relationship(
    name="ENTITY_HAS_EVENT",
    from_node="Entity",
    to_node="Event",
    description="Entity owns this business event record. Materialized as a "
    "single-Entity-per-graph fanout — every Event row is owned by the graph's "
    "reporting entity.",
    properties=[],
  ),
  Relationship(
    name="EVENT_INVOLVES_AGENT",
    from_node="Event",
    to_node="Agent",
    description="REA agent participation — the counterparty involved in this event. "
    "Nullable in OLTP; the edge exists only for events with a populated agent_id.",
    properties=[],
  ),
  Relationship(
    name="EVENT_AFFECTS_RESOURCE",
    from_node="Event",
    to_node="Element",
    description="REA stockflow link — the specific resource (Element plays the "
    "Resource Specification role) this event affects. Nullable in OLTP; the edge "
    "exists only for events with a populated resource_element_id.",
    properties=[],
  ),
  Relationship(
    name="EVENT_OBLIGATED_BY_EVENT",
    from_node="Event",
    to_node="Event",
    description="REA forward-materialization — this event was scheduled or obligated "
    "by an upstream event (e.g. depreciation entries point at the asset_acquired event). "
    "Self-referential; application-validated.",
    properties=[],
  ),
  Relationship(
    name="EVENT_DISCHARGES_EVENT",
    from_node="Event",
    to_node="Event",
    description="REA settlement / reciprocity — this event discharges the obligation "
    "raised by another (e.g. cash_received points at the originating sale_invoiced). "
    "Self-referential; application-validated.",
    properties=[],
  ),
  Relationship(
    name="EVENT_REPLACES_EVENT",
    from_node="Event",
    to_node="Event",
    description="Correction chain — this event supersedes the one it points at. "
    "Mirror of Event.replaces_event_id; the backward link (Event.replaced_by_event_id) "
    "is derived from the same edge in the reverse direction.",
    properties=[],
  ),
]


# ---------------------------------------------------------------------------
# Reporting-only exclusions
# ---------------------------------------------------------------------------
# The REA event/agent substrate (Event, Agent) and the advisory element Trait
# node live in the base schema because accounting (roboledger) graphs populate
# them. Reporting-only repositories — the SEC shared repo today — never create
# economic events, counterparties, or element traits. The entity↔taxonomy link
# edges are likewise tenant-only: they materialize from the extensions OLTP
# database (taxonomy adoption rows, extension chains) and have no SEC XBRL
# source. All are excluded from a reporting-only graph's schema so the empty
# node/relationship tables are neither created nor materialized.
#
# Consumed by ContextAwareSchemaLoader (schema DDL) and
# RoboLedgerContext.get_all_table_names_for_context (materialization list);
# keep both call sites using these constants so the two paths stay in lockstep.
REPORTING_ONLY_EXCLUDED_NODES: frozenset[str] = frozenset({"Event", "Agent", "Trait"})
REPORTING_ONLY_EXCLUDED_RELATIONSHIPS: frozenset[str] = frozenset(
  {
    "ENTITY_HAS_EVENT",
    "ENTITY_HAS_AGENT",
    "ELEMENT_HAS_TRAIT",
    "EVENT_INVOLVES_AGENT",
    "EVENT_AFFECTS_RESOURCE",
    "EVENT_DISCHARGES_EVENT",
    "EVENT_OBLIGATED_BY_EVENT",
    "EVENT_REPLACES_EVENT",
    # Tenant-OLTP-only taxonomy links (no SEC writer)
    "ENTITY_HAS_TAXONOMY",
    "TAXONOMY_EXTENDS_TAXONOMY",
  }
)
