"""
Centralized credit system configuration - AI-focused billing.

SIMPLIFIED CREDIT MODEL:
========================
Credits are now exclusively for AI operations that use external AI APIs
(Anthropic Claude or OpenAI GPT). All other operations are included as they
use resources already included with the Kuzu instance provisioning.

AI CREDITS:
-----------
- Agent calls: 100 credits (uses Anthropic/OpenAI API)
- Connection sync: 20 credits (when incurring external costs)

STORAGE CREDITS (Optional separate mechanism):
----------------------------------------------
- Storage: 10 credits per GB per day
- Can be billed separately from AI credits
- Allows for storage-specific credit pools

INCLUDED OPERATIONS (No credit consumption):
---------------------------------------------
- All database queries (Cypher, analytics)
- All API calls (non-AI)
- MCP protocol calls (graph queries, schema info)
- Data imports/exports
- Backups and restores
- Schema operations
- Basic connection management

RATIONALE:
----------
When a Kuzu database is provisioned, it comes with defined resource limits:
- Memory allocation (based on tier)
- CPU allocation (based on instance type)
- Storage limits (configurable)

These resources are already paid for through the instance provisioning,
so there's no need to charge credits for operations that simply use
these pre-allocated resources. Only external API calls (AI) and
potentially storage overages need credit-based billing.
"""

from decimal import Decimal

from .tier_config import get_tier_monthly_credits


class CreditConfig:
  """Centralized credit system configuration - AI operations only."""

  # Credit cost per operation type (in credits)
  # Only AI operations that use Anthropic or OpenAI APIs consume credits
  OPERATION_COSTS = {
    # AI operations (consume credits)
    "agent_call": Decimal("100"),  # AI agent calls using Anthropic/OpenAI
    "ai_analysis": Decimal("100"),  # AI/Agent analysis
    # Storage operations (per GB per day) - separate billing mechanism
    "storage_per_gb_day": Decimal("10"),  # 10 credits per GB per day
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

  # Monthly AI credit allocations by subscription tier (aligned with GraphTier)
  # Now sourced from centralized tier configuration with fallback
  MONTHLY_ALLOCATIONS = {
    "kuzu-standard": get_tier_monthly_credits("kuzu-standard"),
    "kuzu-large": get_tier_monthly_credits("kuzu-large"),
    "kuzu-xlarge": get_tier_monthly_credits("kuzu-xlarge"),
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
