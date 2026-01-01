"""
Credit system configuration - operation costs and alerts.

This module defines credit costs for operations and alert thresholds.
Tier credit allocations are defined in billing/core.py (single source of truth).

CREDIT VALUE ANCHOR:
====================
1 credit = 1 GB/day of storage overage = ~$0.00333 (1/3 of a cent)

This anchors credits to a tangible resource (storage) with a small margin:
- EBS cost: $0.08/GB/month
- Our price: $0.10/GB/month (30 credits x $0.00333)
- Storage margin: ~25%

AI operations are priced relative to this anchor with a 3.33x markup:
- AI margin: ~70%

CREDIT MODEL:
=============
Credits are consumed by AI operations using token-based pricing.
All database operations are included with the subscription.
Storage overage is billed via credits at 1 credit/GB/day.

AI CREDITS (Token-Based Pricing):
---------------------------------
AI agent calls consume credits based on actual token usage:
- Input tokens: 3 credits per 1K tokens
- Output tokens: 15 credits per 1K tokens
- Typical agent call (~5K input, ~1.5K output): ~38 credits

NOTE: MCP tool access is unlimited and does not consume credits.
Credits only apply to in-house AI agent operations.

INCLUDED OPERATIONS (No credit consumption):
--------------------------------------------
- All database queries (Cypher, analytics)
- All API calls (non-AI)
- MCP protocol calls (graph queries, schema info)
- Data imports/exports
- Backups and restores
- Schema operations
- Connection management
"""

from decimal import Decimal


class CreditConfig:
  """Centralized credit system configuration - AI operations only."""

  # Credit cost per operation type (in credits)
  # AI operations use TOKEN-BASED pricing (see AIBillingConfig.TOKEN_PRICING)
  # Storage overage uses CREDIT-BASED billing at a flat rate
  OPERATION_COSTS = {
    # Storage overage (per GB per day) - THE CREDIT ANCHOR
    # 1 credit = 1 GB/day = ~$0.00333 → 30 credits/GB/month = ~$0.10/GB/month
    "storage_per_gb_day": Decimal("1"),  # 1 credit per GB per day (~$0.10/GB/month)
    # Connection sync operations (potential future credit consumption)
    "connection_sync": Decimal("20"),  # Sync external data (may incur costs)
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

  # Monthly AI credit allocations by subscription tier
  # NOTE: Single source of truth is in billing/core.py (DEFAULT_GRAPH_BILLING_PLANS)
  # These values must match - validated by BillingConfig.validate_configuration()
  # Credit anchor: 1 credit = 1 GB/day storage = ~$0.00333
  # ~38 credits per typical agent call
  MONTHLY_ALLOCATIONS = {
    "ladybug-standard": 8000,  # ~200 agent calls/month
    "ladybug-large": 32000,  # ~800 agent calls/month
    "ladybug-xlarge": 100000,  # ~2,600 agent calls/month
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
    """Get monthly credit allocation for a subscription tier."""
    return cls.MONTHLY_ALLOCATIONS.get(tier, 0)

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
