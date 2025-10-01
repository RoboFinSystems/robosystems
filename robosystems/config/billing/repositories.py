"""
Shared Repository Billing Configuration.

This module defines the billing model for shared repositories (SEC, industry, economic).
Repository queries don't consume credits but are rate-limited by subscription tier.
"""

from typing import Dict, Optional
from enum import Enum


class SharedRepository(str, Enum):
  """Available shared repositories."""

  SEC = "sec"  # SEC financial filings
  INDUSTRY = "industry"  # Industry data (coming soon)
  ECONOMIC = "economic"  # Economic indicators (coming soon)


class RepositoryPlan(str, Enum):
  """Repository access plans (subscription required)."""

  STARTER = "starter"  # Basic access
  ADVANCED = "advanced"  # Professional access
  UNLIMITED = "unlimited"  # Enterprise access


class RepositoryBillingConfig:
  """Configuration for shared repository billing and rate limits."""

  # Repository subscription tiers (monthly pricing)
  REPOSITORY_PLANS = {
    RepositoryPlan.STARTER: {
      "name": "Starter",
      "price_cents": 2900,  # $29/month
      "price_display": "$29/month",
      "monthly_credits": 1000,  # Credits for AI agent operations
      "description": "Basic access for individuals and small teams",
      "features": [
        "1,000 AI agent credits per month",
        "500 queries per hour (FREE)",
        "200 MCP queries per hour (FREE)",
        "Basic rate limits",
        "CSV export only",
        "2 years historical data",
      ],
    },
    RepositoryPlan.ADVANCED: {
      "name": "Advanced",
      "price_cents": 9900,  # $99/month
      "price_display": "$99/month",
      "monthly_credits": 5000,  # Credits for AI agent operations
      "description": "Professional access for analysts and researchers",
      "features": [
        "5,000 AI agent credits per month",
        "2,000 queries per hour (FREE)",
        "1,000 MCP queries per hour (FREE)",
        "Professional rate limits",
        "Priority support",
        "CSV/JSON export",
        "5 years historical data",
      ],
    },
    RepositoryPlan.UNLIMITED: {
      "name": "Unlimited",
      "price_cents": 49900,  # $499/month
      "price_display": "$499/month",
      "monthly_credits": 50000,  # Very high AI agent credits
      "description": "Enterprise access with no limits",
      "features": [
        "50,000 AI agent credits per month",
        "Unlimited queries (FREE)",
        "Unlimited MCP queries (FREE)",
        "No daily rate limits",
        "Dedicated support",
        "Custom integrations",
        "Bulk export capabilities",
        "Full historical archive",
      ],
    },
  }

  # Rate limits by repository and plan
  RATE_LIMITS = {
    SharedRepository.SEC: {
      RepositoryPlan.STARTER: {
        # Query limits
        "queries_per_minute": 30,
        "queries_per_hour": 500,
        "queries_per_day": 5000,
        # MCP limits (AI assistants querying)
        "mcp_queries_per_minute": 10,
        "mcp_queries_per_hour": 200,
        "mcp_queries_per_day": 2000,
        # AI agent limits
        "agent_calls_per_minute": 5,
        "agent_calls_per_hour": 50,
        "agent_calls_per_day": 500,
      },
      RepositoryPlan.ADVANCED: {
        # Query limits
        "queries_per_minute": 100,
        "queries_per_hour": 2000,
        "queries_per_day": 20000,
        # MCP limits
        "mcp_queries_per_minute": 50,
        "mcp_queries_per_hour": 1000,
        "mcp_queries_per_day": 10000,
        # AI agent limits
        "agent_calls_per_minute": 20,
        "agent_calls_per_hour": 200,
        "agent_calls_per_day": 2000,
      },
      RepositoryPlan.UNLIMITED: {
        # Query limits - very high but not infinite for safety
        "queries_per_minute": 1000,
        "queries_per_hour": 20000,
        "queries_per_day": -1,  # Unlimited
        # MCP limits
        "mcp_queries_per_minute": 500,
        "mcp_queries_per_hour": -1,
        "mcp_queries_per_day": -1,
        # AI agent limits
        "agent_calls_per_minute": 100,
        "agent_calls_per_hour": -1,
        "agent_calls_per_day": -1,
      },
    },
    # Future repositories can be added here
    SharedRepository.INDUSTRY: {
      # Similar structure to SEC
    },
    SharedRepository.ECONOMIC: {
      # Similar structure to SEC
    },
  }

  # Allowed endpoints for shared repositories
  ALLOWED_ENDPOINTS = [
    "query",  # Cypher queries
    "mcp",  # MCP tool access
    "agent",  # AI agent queries (consume AI credits)
    "schema",  # Schema information
    "status",  # Repository status
    "info",  # Repository information
    "describe",  # Repository description
  ]

  # Blocked endpoints for shared repositories
  BLOCKED_ENDPOINTS = [
    "backup",  # No backups on shared data
    "restore",  # No restore operations
    "admin",  # No admin operations
    "delete",  # No delete operations
    "import",  # No imports to shared data
    "write",  # No write operations
    "update",  # No updates
    "create",  # No creates
  ]

  @classmethod
  def get_plan_details(cls, plan: RepositoryPlan) -> Optional[Dict]:
    """Get details for a repository plan."""
    return cls.REPOSITORY_PLANS.get(plan)

  @classmethod
  def get_rate_limits(
    cls, repository: SharedRepository, plan: RepositoryPlan
  ) -> Optional[Dict]:
    """Get rate limits for a repository and plan combination."""
    repo_limits = cls.RATE_LIMITS.get(repository, {})
    return repo_limits.get(plan)

  @classmethod
  def is_endpoint_allowed(cls, endpoint: str) -> bool:
    """Check if an endpoint is allowed for shared repositories."""
    endpoint_lower = endpoint.lower()

    # Check if it's explicitly blocked
    for blocked in cls.BLOCKED_ENDPOINTS:
      if blocked in endpoint_lower:
        return False

    # Check if it's in the allowed list
    return any(allowed in endpoint_lower for allowed in cls.ALLOWED_ENDPOINTS)

  @classmethod
  def get_all_repository_pricing(cls) -> Dict:
    """Get complete pricing information for all repository plans."""
    return {
      "plans": cls.REPOSITORY_PLANS,
      "repositories": {
        SharedRepository.SEC: {
          "name": "SEC Financial Data",
          "description": "Access to SEC EDGAR filings and financial data",
          "status": "available",
        },
        SharedRepository.INDUSTRY: {
          "name": "Industry Data",
          "description": "Industry benchmarks and comparative data",
          "status": "coming_soon",
        },
        SharedRepository.ECONOMIC: {
          "name": "Economic Indicators",
          "description": "Macroeconomic data and indicators",
          "status": "coming_soon",
        },
      },
      "billing_model": "No credit consumption for queries, rate-limited by subscription tier",
      "upgrade_url": "https://roboledger.ai/upgrade",
    }
