"""
Centralized configuration package for RoboSystems Service.

This package provides a single source of truth for all configuration settings,
including rate limits, credits, constants, and external services.

Note: Billing and repository configurations are now model-based (see unified_billing.py and UserRepository model).
"""

# Import env first to avoid circular dependencies
from .env import EnvConfig, env

# Core configuration modules
from .billing import (
  BillingConfig,
  DEFAULT_GRAPH_BILLING_PLANS,
  AIBillingConfig,
  AIOperationType,
  RepositoryBillingConfig,
  SharedRepository,
  StorageBillingConfig,
)
from .rate_limits import RateLimitConfig, RateLimitPeriod, EndpointCategory
from .credits import CreditConfig
from .constants import URIConstants, PrefixConstants, XBRLConstants
from .external_services import ExternalServicesConfig
from .query_queue import QueryQueueConfig
from .validation import EnvValidator
from .agents import (
  AgentConfig,
  BedrockModel,
  AgentExecutionMode,
  ExecutionProfile,
  ModelConfig,
)

__all__ = [
  # Billing exports
  "BillingConfig",
  "DEFAULT_GRAPH_BILLING_PLANS",
  "AIBillingConfig",
  "AIOperationType",
  "RepositoryBillingConfig",
  "SharedRepository",
  "StorageBillingConfig",
  # Rate limit exports
  "RateLimitConfig",
  "RateLimitPeriod",
  "EndpointCategory",
  # Credit exports
  "CreditConfig",
  # Constants exports
  "URIConstants",
  "PrefixConstants",
  "XBRLConstants",
  # External services exports
  "ExternalServicesConfig",
  # Query configuration exports
  "QueryQueueConfig",
  # Validation exports
  "EnvValidator",
  # Environment exports
  "EnvConfig",
  "env",
  # Agent exports
  "AgentConfig",
  "BedrockModel",
  "AgentExecutionMode",
  "ExecutionProfile",
  "ModelConfig",
]
