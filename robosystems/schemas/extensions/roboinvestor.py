"""
RoboInvestor Schema Extension for LadybugDB

Portfolio management, investment tracking, and securities analysis.
Extends the base schema with investment-specific entities.
"""

from ..models import Node, Property, Relationship

# RoboInvestor Extension Nodes
EXTENSION_NODES = [
  Node(
    name="Portfolio",
    description="Investment portfolio with strategy and performance tracking",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(name="strategy", type="STRING"),
      Property(name="risk_level", type="STRING"),  # conservative, moderate, aggressive
      Property(name="inception_date", type="DATE"),
      Property(name="total_value", type="DOUBLE"),
    ],
  ),
  Node(
    name="Security",
    description="Financial instruments and securities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="identifier", type="STRING"),  # unique
      Property(name="uri", type="STRING"),  # unique
      Property(name="name", type="STRING"),
      Property(name="ticker", type="STRING"),
      Property(name="figi", type="STRING"),
      Property(name="composite_figi", type="STRING"),
      Property(name="security_type", type="STRING"),
      Property(name="security_type2", type="STRING"),
      Property(name="security_description", type="STRING"),
      Property(name="market_sector", type="STRING"),
      Property(name="share_class_figi", type="STRING"),
      Property(name="exchange_code", type="STRING"),
      Property(name="updated_at", type="STRING"),
    ],
  ),
  Node(
    name="Position",
    description="Current holdings of securities within portfolios",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="quantity", type="DOUBLE"),
      Property(name="avg_cost_basis", type="DOUBLE"),
      Property(name="current_price", type="DOUBLE"),
      Property(name="market_value", type="DOUBLE"),
      Property(name="unrealized_gain_loss", type="DOUBLE"),
      Property(name="position_date", type="DATE"),
    ],
  ),
  Node(
    name="Trade",
    description="Individual trading transactions and activities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="trade_type", type="STRING"),  # buy, sell, dividend, split
      Property(name="quantity", type="DOUBLE"),
      Property(name="price", type="DOUBLE"),
      Property(name="commission", type="DOUBLE"),
      Property(name="total_amount", type="DOUBLE"),
      Property(name="trade_date", type="DATE"),
      Property(name="settlement_date", type="DATE"),
    ],
  ),
  Node(
    name="Benchmark",
    description="Market benchmarks for portfolio performance comparison",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="symbol", type="STRING"),
      Property(name="name", type="STRING"),
      Property(name="description", type="STRING"),
    ],
  ),
  Node(
    name="MarketData",
    description="Historical and real-time market data for securities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="date", type="DATE"),
      Property(name="open_price", type="DOUBLE"),
      Property(name="high_price", type="DOUBLE"),
      Property(name="low_price", type="DOUBLE"),
      Property(name="close_price", type="DOUBLE"),
      Property(name="volume", type="INT64"),
      Property(name="adjusted_close", type="DOUBLE"),
    ],
  ),
  Node(
    name="Dividend",
    description="Dividend payments and distributions",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(name="ex_date", type="DATE"),
      Property(name="record_date", type="DATE"),
      Property(name="payment_date", type="DATE"),
      Property(name="amount_per_share", type="DOUBLE"),
      Property(name="dividend_type", type="STRING"),  # regular, special, stock
      Property(name="currency", type="STRING"),
    ],
  ),
  Node(
    name="Risk",
    description="Risk metrics and assessments for portfolios and securities",
    properties=[
      Property(name="id", type="STRING", is_primary_key=True),
      Property(
        name="risk_type", type="STRING"
      ),  # market, credit, liquidity, operational
      Property(name="risk_level", type="STRING"),  # low, medium, high, critical
      Property(name="beta", type="DOUBLE"),
      Property(name="volatility", type="DOUBLE"),
      Property(name="value_at_risk", type="DOUBLE"),
      Property(name="assessment_date", type="DATE"),
      Property(name="notes", type="STRING"),
    ],
  ),
]

# RoboInvestor Extension Relationships
EXTENSION_RELATIONSHIPS = [
  Relationship(
    name="ENTITY_ISSUES_SECURITY",
    from_node="Entity",
    to_node="Security",
    description="Entity issues securities",
    properties=[
      Property(name="issue_date", type="STRING"),
      Property(name="security_context", type="STRING"),
    ],
  ),
  Relationship(
    name="ENTITY_HAS_PORTFOLIO",
    from_node="Entity",
    to_node="Portfolio",
    description="Entity owns and manages portfolios",
    properties=[
      Property(name="portfolio_context", type="STRING"),
    ],
  ),
  Relationship(
    name="PORTFOLIO_HAS_POSITION",
    from_node="Portfolio",
    to_node="Position",
    description="Portfolio contains security positions",
    properties=[
      Property(name="position_context", type="STRING"),
      Property(name="allocation_percentage", type="DOUBLE"),
    ],
  ),
  Relationship(
    name="POSITION_IN_SECURITY",
    from_node="Position",
    to_node="Security",
    description="Position holds specific security",
    properties=[
      Property(name="security_context", type="STRING"),
    ],
  ),
  Relationship(
    name="PORTFOLIO_HAS_TRADE",
    from_node="Portfolio",
    to_node="Trade",
    description="Portfolio executes trades",
    properties=[
      Property(name="trade_context", type="STRING"),
    ],
  ),
  Relationship(
    name="TRADE_INVOLVES_SECURITY",
    from_node="Trade",
    to_node="Security",
    description="Trade transacts in specific security",
    properties=[
      Property(name="security_context", type="STRING"),
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
  Relationship(
    name="SECURITY_PAYS_DIVIDEND",
    from_node="Security",
    to_node="Dividend",
    description="Security pays dividends to holders",
    properties=[
      Property(name="payment_context", type="STRING"),
    ],
  ),
  Relationship(
    name="PORTFOLIO_HAS_RISK",
    from_node="Portfolio",
    to_node="Risk",
    description="Portfolio risk assessment and metrics",
    properties=[
      Property(name="risk_context", type="STRING"),
    ],
  ),
  Relationship(
    name="SECURITY_HAS_RISK",
    from_node="Security",
    to_node="Risk",
    description="Security risk assessment and metrics",
    properties=[
      Property(name="risk_context", type="STRING"),
    ],
  ),
  Relationship(
    name="TRADE_CREATES_POSITION",
    from_node="Trade",
    to_node="Position",
    description="Trade creates or modifies position",
    properties=[
      Property(
        name="position_impact", type="STRING"
      ),  # create, increase, decrease, close
    ],
  ),
  Relationship(
    name="USER_MANAGES_PORTFOLIO",
    from_node="User",
    to_node="Portfolio",
    description="User has management access to portfolio",
    properties=[
      Property(name="management_role", type="STRING"),  # owner, advisor, viewer
      Property(name="permission_level", type="STRING"),
    ],
  ),
]
