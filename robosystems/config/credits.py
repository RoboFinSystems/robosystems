"""Credit costs for non-AI operations (all zero) and alert thresholds.

AI operations bill per token (billing/ai.py); tier allocations live in
billing/core.py.
"""

from decimal import Decimal


class CreditConfig:
  """Centralized credit system configuration - AI operations only."""

  OPERATION_COSTS = {
    "connection_sync": Decimal("0"),
    "mcp_call": Decimal("0"),
    "mcp_tool_call": Decimal("0"),
    "api_call": Decimal("0"),
    "query": Decimal("0"),
    "cypher_query": Decimal("0"),
    "analytics": Decimal("0"),
    "analytics_query": Decimal("0"),
    "backup": Decimal("0"),
    "backup_restore": Decimal("0"),
    "backup_export": Decimal("0"),
    "sync": Decimal("0"),
    "import": Decimal("0"),
    "data_transfer_in": Decimal("0"),
    "data_transfer_out": Decimal("0"),
    "schema_query": Decimal("0"),
    "schema_validation": Decimal("0"),
    "schema_export": Decimal("0"),
    "connection_create": Decimal("0"),
    "connection_test": Decimal("0"),
    "connection_delete": Decimal("0"),
    "database_query": Decimal("0"),
    "database_write": Decimal("0"),
  }

  # Fraction of the allocation remaining
  ALERT_THRESHOLDS = {
    "low_balance": 0.2,  # Alert when 20% remaining
    "critical_balance": 0.05,  # Critical alert at 5% remaining
    "exhausted": 0.0,  # No credits remaining
  }

  @classmethod
  def get_operation_cost(cls, operation_type: str) -> Decimal:
    """Credits for a non-AI operation type; 0 for all of them, and for unknown
    types. AI operations bill per token instead.
    """
    return cls.OPERATION_COSTS.get(operation_type, Decimal("0"))

  @classmethod
  def get_monthly_allocation(cls, tier: str) -> int:
    """Monthly credit allocation for a tier (from billing/core.py)."""
    # Late import: core.py imports CreditConfig.
    from robosystems.config.billing.core import get_tier_credit_allocation

    return get_tier_credit_allocation(tier)

  @classmethod
  def should_alert(cls, balance: int, allocation: int) -> str:
    """
    Check if a balance warrants an alert.

    Returns:
        Alert level: 'none', 'low', 'critical', or 'exhausted'
    """
    if allocation == 0:
      return "none"

    ratio = balance / allocation

    if ratio <= cls.ALERT_THRESHOLDS["exhausted"]:
      return "exhausted"
    elif ratio <= cls.ALERT_THRESHOLDS["critical_balance"]:
      return "critical"
    elif ratio <= cls.ALERT_THRESHOLDS["low_balance"]:
      return "low"

    return "none"
