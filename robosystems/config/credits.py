"""
Credit system configuration - operation costs and alerts.

This module defines credit costs for operations and alert thresholds.
Tier credit allocations are defined in billing/core.py (single source of truth).
Token pricing is defined in billing/ai.py (AIBillingConfig.TOKEN_PRICING).

TOKEN PRICING:
==============
AI agent calls consume credits based on actual token usage:
  - 3 credits per 1K input tokens
  - 15 credits per 1K output tokens
  - Typical agent call (~5K input, ~1.5K output): ~38 credits

Only AI agent operations consume credits. All other operations
(database, MCP, API, backup, sync) are included with the subscription.
"""

from decimal import Decimal


class CreditConfig:
  """Centralized credit system configuration - AI operations only."""

  # Non-AI operation costs (all included with subscription)
  # AI operations use token-based pricing — see billing/ai.py
  OPERATION_COSTS = {
    # Connection sync operations - included (not an AI operation)
    "connection_sync": Decimal("0"),  # Sync external data - included
    # All other operations are included in subscription (no credit consumption)
    "mcp_call": Decimal("0"),  # MCP protocol calls - included
    "mcp_tool_call": Decimal("0"),  # MCP tool calls - included
    "api_call": Decimal("0"),  # Standard API calls - included
    "query": Decimal("0"),  # Direct Cypher queries - included
    "cypher_query": Decimal("0"),  # Cypher query execution - included
    "analytics": Decimal("0"),  # Analytics queries - included
    "analytics_query": Decimal("0"),  # Alias for analytics - included
    "backup": Decimal("0"),  # Backup operations - included
    "backup_restore": Decimal("0"),  # Restore from backup - included
    "backup_export": Decimal("0"),  # Export backup - included
    "sync": Decimal("0"),  # Basic sync operations - included
    "import": Decimal("0"),  # Bulk import operations - included
    "data_transfer_in": Decimal("0"),  # Ingress - included
    "data_transfer_out": Decimal("0"),  # Egress - included with instance
    "schema_query": Decimal("0"),  # Basic schema info - included
    "schema_validation": Decimal("0"),  # Schema validation - included
    "schema_export": Decimal("0"),  # Full schema export - included
    "connection_create": Decimal("0"),  # Setup external connection - included
    "connection_test": Decimal("0"),  # Test connection - included
    "connection_delete": Decimal("0"),  # Remove connection - included
    "database_query": Decimal("0"),  # Database queries - included
    "database_write": Decimal("0"),  # Write operations - included
  }

  # Credit balance thresholds for alerts
  ALERT_THRESHOLDS = {
    "low_balance": 0.2,  # Alert when 20% remaining
    "critical_balance": 0.05,  # Critical alert at 5% remaining
    "exhausted": 0.0,  # No credits remaining
  }

  @classmethod
  def get_operation_cost(cls, operation_type: str) -> Decimal:
    """
    Get the cost for an operation type.

    Only AI operations (agent_call, ai_analysis) and storage consume credits.
    All other operations including MCP calls are included in the subscription.

    Args:
        operation_type: Type of operation

    Returns:
        Cost in credits (0 for non-AI operations)
    """
    return cls.OPERATION_COSTS.get(operation_type, Decimal("0"))

  @classmethod
  def get_monthly_allocation(cls, tier: str) -> int:
    """
    Get monthly credit allocation for a subscription tier.

    Delegates to billing/core.py which is the single source of truth
    for tier configuration.
    """
    # Late import to avoid circular dependency (core.py imports CreditConfig)
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
