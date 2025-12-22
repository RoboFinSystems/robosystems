"""
Centralized configuration package for RoboSystems Service.

This package provides a single source of truth for all configuration settings,
including rate limits, credits, constants, and external services.

Note: Billing and repository configurations are now model-based (see unified_billing.py and UserRepository model).
"""

# Import env first to avoid circular dependencies
from .agents import (
  AgentConfig,
  AgentExecutionMode,
  BedrockModel,
  ExecutionProfile,
  ModelConfig,
)

# Core configuration modules
from .billing import (
  DEFAULT_GRAPH_BILLING_PLANS,
  AIBillingConfig,
  AIOperationType,
  BillingConfig,
  RepositoryBillingConfig,
  SharedRepository,
  StorageBillingConfig,
)
from .constants import PrefixConstants, URIConstants, XBRLConstants
from .credits import CreditConfig
from .env import EnvConfig, env
from .external_services import ExternalServicesConfig
from .query_queue import QueryQueueConfig
from .rate_limits import EndpointCategory, RateLimitConfig, RateLimitPeriod
from .validation import EnvValidator

__all__ = [
  "DEFAULT_GRAPH_BILLING_PLANS",
  "AIBillingConfig",
  "AIOperationType",
  # Agent exports
  "AgentConfig",
  "AgentExecutionMode",
  "BedrockModel",
  # Billing exports
  "BillingConfig",
  # Credit exports
  "CreditConfig",
  "EndpointCategory",
  # Environment exports
  "EnvConfig",
  # Validation exports
  "EnvValidator",
  "ExecutionProfile",
  # External services exports
  "ExternalServicesConfig",
  "ModelConfig",
  "PrefixConstants",
  # Query configuration exports
  "QueryQueueConfig",
  # Rate limit exports
  "RateLimitConfig",
  "RateLimitPeriod",
  "RepositoryBillingConfig",
  "SharedRepository",
  "StorageBillingConfig",
  # Constants exports
  "URIConstants",
  "XBRLConstants",
  "env",
]
