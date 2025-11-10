"""
Centralized agent system configuration.

This module defines all configuration for the multi-agent system including:
- AI model selection and endpoints
- Execution profiles and mode limits
- Orchestrator routing configuration
- Token cost configurations
"""

from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
from decimal import Decimal

from robosystems.config import env


class BedrockModel(Enum):
  """Available AWS Bedrock Claude models."""

  SONNET_4_5 = "claude-sonnet-4-5-20250929"
  SONNET_4 = "claude-sonnet-4-20250514"
  SONNET_3_5_V2 = "claude-3-5-sonnet-20241022"  # Last resort fallback


class AgentExecutionMode(Enum):
  """Agent execution modes with different performance characteristics."""

  QUICK = "quick"
  STANDARD = "standard"
  EXTENDED = "extended"
  STREAMING = "streaming"


@dataclass
class ExecutionProfile:
  """Execution time and resource profile for an agent mode."""

  min_time_seconds: int
  max_time_seconds: int
  avg_time_seconds: int
  max_tool_calls: int
  max_input_tokens: int
  max_output_tokens: int
  timeout_seconds: int


@dataclass
class ModelConfig:
  """Configuration for AI model selection and parameters."""

  default_model: BedrockModel
  fallback_model: Optional[BedrockModel] = None
  region: str = "us-east-1"
  temperature: float = 0.7
  max_retries: int = 3
  timeout_seconds: int = 60


class AgentConfig:
  """
  Centralized configuration for the multi-agent system.

  This is the single source of truth for all agent-related settings.
  """

  # AWS Bedrock Model Configuration
  # Using regional inference profiles (us.*) for on-demand access
  BEDROCK_MODELS = {
    BedrockModel.SONNET_4_5: "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    BedrockModel.SONNET_4: "us.anthropic.claude-sonnet-4-20250514-v1:0",
    BedrockModel.SONNET_3_5_V2: "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
  }

  # Default Model Configuration
  DEFAULT_MODEL_CONFIG = ModelConfig(
    default_model=BedrockModel.SONNET_4_5,
    fallback_model=BedrockModel.SONNET_4,
    region=env.AWS_BEDROCK_REGION,
    temperature=0.7,
    max_retries=3,
    timeout_seconds=60,
  )

  # Execution Profiles by Mode
  EXECUTION_PROFILES = {
    AgentExecutionMode.QUICK: ExecutionProfile(
      min_time_seconds=2,
      max_time_seconds=5,
      avg_time_seconds=3,
      max_tool_calls=2,
      max_input_tokens=50000,
      max_output_tokens=2000,
      timeout_seconds=30,
    ),
    AgentExecutionMode.STANDARD: ExecutionProfile(
      min_time_seconds=5,
      max_time_seconds=15,
      avg_time_seconds=10,
      max_tool_calls=5,
      max_input_tokens=100000,
      max_output_tokens=4000,
      timeout_seconds=60,
    ),
    AgentExecutionMode.EXTENDED: ExecutionProfile(
      min_time_seconds=30,
      max_time_seconds=120,
      avg_time_seconds=60,
      max_tool_calls=12,
      max_input_tokens=150000,
      max_output_tokens=8000,
      timeout_seconds=300,
    ),
    AgentExecutionMode.STREAMING: ExecutionProfile(
      min_time_seconds=5,
      max_time_seconds=60,
      avg_time_seconds=20,
      max_tool_calls=8,
      max_input_tokens=100000,
      max_output_tokens=8000,
      timeout_seconds=120,
    ),
  }

  # Agent-Specific Model Overrides
  # Allows different agents to use different models if needed
  AGENT_MODEL_OVERRIDES: Dict[str, BedrockModel] = {
    # Example: "financial": BedrockModel.SONNET_4_5,
    # Example: "cypher": BedrockModel.SONNET_3_5_V2,
  }

  # Orchestrator Configuration
  ORCHESTRATOR_CONFIG = {
    "fallback_agent": "cypher",
    "confidence_threshold": 0.7,
    "max_routing_attempts": 3,
    "enable_rag": False,
    "routing_strategy": "best_match",
  }

  # Token Cost Configuration (for credit billing)
  # Cost in credits per 1000 tokens
  # AWS Pricing: $3 per MTok input, $15 per MTok output (all Sonnet models)
  # Credit conversion: 1 credit = $0.001 USD
  TOKEN_COSTS = {
    BedrockModel.SONNET_4_5: {
      "input_per_1k": Decimal("3.0"),  # $0.003 per 1k tokens
      "output_per_1k": Decimal("15.0"),  # $0.015 per 1k tokens
    },
    BedrockModel.SONNET_4: {
      "input_per_1k": Decimal("3.0"),  # $0.003 per 1k tokens
      "output_per_1k": Decimal("15.0"),  # $0.015 per 1k tokens
    },
    BedrockModel.SONNET_3_5_V2: {
      "input_per_1k": Decimal("3.0"),  # $0.003 per 1k tokens
      "output_per_1k": Decimal("15.0"),  # $0.015 per 1k tokens
    },
  }

  # Agent Capabilities Configuration
  AGENT_CAPABILITIES = {
    "cypher": {
      "supported_modes": ["quick", "standard", "extended", "streaming"],
      "requires_credits": True,
      "max_concurrent_requests": 10,
    },
    "financial": {
      "supported_modes": ["quick", "standard", "extended"],
      "requires_credits": True,
      "max_concurrent_requests": 5,
    },
  }

  @classmethod
  def get_bedrock_model_id(
    cls, model: Optional[BedrockModel] = None, agent_type: Optional[str] = None
  ) -> str:
    """
    Get the AWS Bedrock model ID for a given model or agent.

    Args:
        model: Optional model enum (overrides default)
        agent_type: Optional agent type to check for overrides

    Returns:
        Bedrock model ID string
    """
    # Check agent-specific overrides first
    if agent_type and agent_type in cls.AGENT_MODEL_OVERRIDES:
      model = cls.AGENT_MODEL_OVERRIDES[agent_type]

    # Use provided model or default
    if not model:
      model = cls.DEFAULT_MODEL_CONFIG.default_model

    return cls.BEDROCK_MODELS.get(model, cls.BEDROCK_MODELS[BedrockModel.SONNET_3_5_V2])

  @classmethod
  def get_execution_profile(cls, mode: AgentExecutionMode) -> ExecutionProfile:
    """Get execution profile for a given mode."""
    return cls.EXECUTION_PROFILES.get(
      mode, cls.EXECUTION_PROFILES[AgentExecutionMode.STANDARD]
    )

  @classmethod
  def get_mode_limits(cls, mode: str) -> Dict[str, Any]:
    """
    Get operational limits for a mode (backward compatible with BaseAgent).

    Args:
        mode: Mode name as string

    Returns:
        Dict with limits
    """
    try:
      mode_enum = AgentExecutionMode(mode.lower())
    except ValueError:
      mode_enum = AgentExecutionMode.STANDARD

    profile = cls.get_execution_profile(mode_enum)

    return {
      "max_tools": profile.max_tool_calls,
      "timeout": profile.timeout_seconds,
      "max_input_tokens": profile.max_input_tokens,
      "max_output_tokens": profile.max_output_tokens,
    }

  @classmethod
  def get_token_cost(
    cls, model: BedrockModel, input_tokens: int, output_tokens: int
  ) -> Decimal:
    """
    Calculate total credit cost for token usage.

    Args:
        model: The model used
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens

    Returns:
        Total cost in credits
    """
    costs = cls.TOKEN_COSTS.get(model, cls.TOKEN_COSTS[BedrockModel.SONNET_3_5_V2])

    input_cost = (Decimal(input_tokens) / 1000) * costs["input_per_1k"]
    output_cost = (Decimal(output_tokens) / 1000) * costs["output_per_1k"]

    return input_cost + output_cost

  @classmethod
  def get_agent_capabilities(cls, agent_type: str) -> Dict[str, Any]:
    """Get capabilities configuration for an agent type."""
    return cls.AGENT_CAPABILITIES.get(
      agent_type,
      {
        "supported_modes": ["quick", "standard", "extended"],
        "requires_credits": True,
        "max_concurrent_requests": 5,
      },
    )

  @classmethod
  def validate_configuration(cls) -> Dict[str, Any]:
    """
    Validate agent configuration consistency.

    Returns:
        Dict with validation results
    """
    issues = []

    # Validate all agent overrides reference valid models
    for agent_type, model in cls.AGENT_MODEL_OVERRIDES.items():
      if model not in cls.BEDROCK_MODELS:
        issues.append(f"Agent '{agent_type}' has invalid model override: {model}")

    # Validate default model exists
    if cls.DEFAULT_MODEL_CONFIG.default_model not in cls.BEDROCK_MODELS:
      issues.append(
        f"Default model not found: {cls.DEFAULT_MODEL_CONFIG.default_model}"
      )

    # Validate fallback agent exists in capabilities
    fallback = cls.ORCHESTRATOR_CONFIG.get("fallback_agent")
    if fallback and fallback not in cls.AGENT_CAPABILITIES:
      issues.append(f"Fallback agent not found in capabilities: {fallback}")

    # Validate all modes have execution profiles
    for mode in AgentExecutionMode:
      if mode not in cls.EXECUTION_PROFILES:
        issues.append(f"Missing execution profile for mode: {mode.value}")

    # Validate all models have token costs
    for model in cls.BEDROCK_MODELS.keys():
      if model not in cls.TOKEN_COSTS:
        issues.append(f"Missing token costs for model: {model.value}")

    return {
      "valid": len(issues) == 0,
      "issues": issues,
      "summary": {
        "models": len(cls.BEDROCK_MODELS),
        "execution_profiles": len(cls.EXECUTION_PROFILES),
        "agent_capabilities": len(cls.AGENT_CAPABILITIES),
        "token_cost_models": len(cls.TOKEN_COSTS),
      },
    }

  @classmethod
  def get_all_config(cls) -> Dict[str, Any]:
    """
    Get complete agent configuration.

    Returns:
        Complete configuration dict
    """
    return {
      "models": {
        "default": cls.DEFAULT_MODEL_CONFIG.default_model.value,
        "fallback": (
          cls.DEFAULT_MODEL_CONFIG.fallback_model.value
          if cls.DEFAULT_MODEL_CONFIG.fallback_model
          else None
        ),
        "region": cls.DEFAULT_MODEL_CONFIG.region,
        "available_models": [model.value for model in cls.BEDROCK_MODELS.keys()],
      },
      "execution_profiles": {
        mode.value: {
          "min_time": profile.min_time_seconds,
          "max_time": profile.max_time_seconds,
          "avg_time": profile.avg_time_seconds,
          "max_tools": profile.max_tool_calls,
          "max_input_tokens": profile.max_input_tokens,
          "max_output_tokens": profile.max_output_tokens,
          "timeout": profile.timeout_seconds,
        }
        for mode, profile in cls.EXECUTION_PROFILES.items()
      },
      "orchestrator": cls.ORCHESTRATOR_CONFIG,
      "agent_capabilities": cls.AGENT_CAPABILITIES,
    }
