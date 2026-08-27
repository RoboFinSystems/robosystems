"""
Centralized configuration package for RoboSystems Service.

This package provides a single source of truth for all configuration settings,
including rate limits, credits, constants, and external services.

Note: graph tier pricing and credit allocations live in `billing/core.py`
(`DEFAULT_GRAPH_BILLING_PLANS`); repository plans live in
`shared_repositories.py` and the `UserRepository` model.
"""

# Core configuration modules
from .billing import (
  DEFAULT_GRAPH_BILLING_PLANS,
  AIBillingConfig,
  BillingConfig,
)
from .constants import PrefixConstants, URIConstants, XBRLConstants
from .credits import CreditConfig
from .env import EnvConfig, env
from .operators import (
  BedrockModel,
  ExecutionProfile,
  ModelConfig,
  OperatorConfig,
  OperatorExecutionMode,
  model_accepts_sampling_params,
)
from .query_queue import QueryQueueConfig
from .rate_limits import EndpointCategory, RateLimitConfig, RateLimitPeriod
from .validation import EnvValidator

__all__ = [
  "DEFAULT_GRAPH_BILLING_PLANS",
  "AIBillingConfig",
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
  "ModelConfig",
  # AI Operator exports
  "OperatorConfig",
  "OperatorExecutionMode",
  "PrefixConstants",
  # Query configuration exports
  "QueryQueueConfig",
  # Rate limit exports
  "RateLimitConfig",
  "RateLimitPeriod",
  # Constants exports
  "URIConstants",
  "XBRLConstants",
  "env",
  "model_accepts_sampling_params",
]
