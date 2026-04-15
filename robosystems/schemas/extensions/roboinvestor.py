"""
RoboInvestor Schema Extension for LadybugDB

Portfolio management, investment tracking, and securities analysis.
Extends the base schema with investment-specific entities.

Ontology:
  Entity -[ENTITY_HAS_PORTFOLIO]-> Portfolio -[PORTFOLIO_HAS_POSITION]-> Position -[POSITION_IN_SECURITY]-> Security
  Entity -[ENTITY_ISSUES_SECURITY]-> Security
  Entity -[ENTITY_HAS_REPORT]-> Report  (from roboledger schema — shared reports)
  Entity -[ENTITY_HAS_TAXONOMY]-> Taxonomy (from base schema — shared ontology)

The entity-focused pattern holds: an investing entity owns portfolios, an
issuing entity has securities, and both play equally well for private and
public (SEC) scenarios. Agent traversal is identical regardless of entity
source.
"""

from ..models import Node, Property, Relationship

# NOTE: Before adding nodes or edges here, review schemas/base.py invariants.
# Universally-applicable concepts (Entity, Taxonomy, Element, Dimension,
# Structure, Association, Classification) belong in base.py, not here.
# Aspects (Period, Unit, Dimension) never attach to declarative nodes like
# Entity, Report, Taxonomy, Portfolio — that's a category error.

# ── MVP Nodes (materialized from OLTP) ───────────────────────────────────

EXTENSION_NODES = [
  Node(
    name="Portfolio",
    description="Investment portfolio with strategy and performance tracking",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="strategy", type="STRING"),
      Property(name="inception_date", type="DATE"),
      Property(name="base_currency", type="STRING"),
    ],
  ),
  Node(
    name="Security",
    description="Ownership instrument issued by an entity (private or public)",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(name="security_type", type="STRING"),
      Property(name="security_subtype", type="STRING"),
      Property(name="is_active", type="BOOLEAN"),
      # Public market identifiers (populated when brokerage connected)
      Property(name="ticker", type="STRING"),
      Property(name="figi", type="STRING"),
      # Source provenance
      Property(name="source_graph_id", type="STRING"),
    ],
  ),
  Node(
    name="Position",
    description="Holding of a security within a portfolio",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="quantity", type="DOUBLE"),
      Property(name="quantity_type", type="STRING"),
      Property(name="cost_basis", type="DOUBLE"),
      Property(name="current_value", type="DOUBLE"),
      Property(name="currency", type="STRING"),
      Property(name="acquisition_date", type="DATE"),
      Property(name="status", type="STRING"),
    ],
  ),
  # ── Future Nodes (deferred — defined for schema completeness) ──────────
  Node(
    name="Trade",
    description="Trading transaction or capital event",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="trade_type", type="STRING"),
      Property(name="quantity", type="DOUBLE"),
      Property(name="price", type="DOUBLE"),
      Property(name="total_amount", type="DOUBLE"),
      Property(name="trade_date", type="DATE"),
      Property(name="settlement_date", type="DATE"),
    ],
  ),
  Node(
    name="Benchmark",
    description="Market benchmark for performance comparison",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="symbol", type="STRING"),
      Property(name="name", type="STRING"),
      Property(name="description", type="STRING"),
    ],
  ),
  Node(
    name="MarketData",
    description="Historical market data for securities",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="date", type="DATE"),
      Property(name="open_price", type="DOUBLE"),
      Property(name="high_price", type="DOUBLE"),
      Property(name="low_price", type="DOUBLE"),
      Property(name="close_price", type="DOUBLE"),
      Property(name="volume", type="INT64"),
      Property(name="adjusted_close", type="DOUBLE"),
    ],
  ),
]

# ── MVP Relationships ────────────────────────────────────────────────────

EXTENSION_RELATIONSHIPS = [
  # Entity → Portfolio ownership (the investing entity)
  Relationship(
    name="ENTITY_HAS_PORTFOLIO",
    from_node="Entity",
    to_node="Portfolio",
    description="Entity owns or manages this portfolio (the investing entity, not the "
    "issuing entity of a security)",
    properties=[
      Property(name="ownership_type", type="STRING"),  # direct | managed | advised
    ],
  ),
  # Core portfolio structure
  Relationship(
    name="ENTITY_ISSUES_SECURITY",
    from_node="Entity",
    to_node="Security",
    description="Entity issues securities (private or public)",
  ),
  Relationship(
    name="PORTFOLIO_HAS_POSITION",
    from_node="Portfolio",
    to_node="Position",
    description="Portfolio contains security positions",
    properties=[
      Property(name="allocation_percentage", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="POSITION_IN_SECURITY",
    from_node="Position",
    to_node="Security",
    description="Position holds specific security",
  ),
  # ── Future Relationships (deferred) ────────────────────────────────────
  Relationship(
    name="PORTFOLIO_HAS_TRADE",
    from_node="Portfolio",
    to_node="Trade",
    description="Portfolio executes trades",
  ),
  Relationship(
    name="TRADE_INVOLVES_SECURITY",
    from_node="Trade",
    to_node="Security",
    description="Trade transacts in specific security",
  ),
  Relationship(
    name="TRADE_CREATES_POSITION",
    from_node="Trade",
    to_node="Position",
    description="Trade creates or modifies position",
    properties=[
      Property(name="position_impact", type="STRING"),
    ],
  ),
  Relationship(
    name="PORTFOLIO_BENCHMARKED_AGAINST",
    from_node="Portfolio",
    to_node="Benchmark",
    description="Portfolio performance compared to benchmark",
    properties=[
      Property(name="benchmark_weight", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="SECURITY_HAS_MARKET_DATA",
    from_node="Security",
    to_node="MarketData",
    description="Security has historical market data",
    properties=[
      Property(name="data_source", type="STRING"),
    ],
  ),
]
