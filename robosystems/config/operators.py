"""
AI Operator configuration: model selection and endpoints, execution profiles
and mode limits, orchestrator routing, and token costs.

"Operator" is the AI-executor concept (Claude/MCP), distinct from REA ``Agent``
(counterparty) in ``models/extensions/roboledger/agent.py``.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from robosystems.config.env import env


class BedrockModel(Enum):
  """Available AWS Bedrock Claude models."""

  SONNET_5 = "claude-sonnet-5"  # Not default — gated on the model-upgrade flip
  SONNET_4_6 = "claude-sonnet-4-6"
  SONNET_4_5 = "claude-sonnet-4-5-20250929"
  SONNET_4 = "claude-sonnet-4-20250514"  # Last resort fallback


# Claude 5-family models reject `temperature`/`top_p`/`top_k` with a 400
# (verified live: "temperature is deprecated for this model") and run
# adaptive thinking unless it is explicitly disabled. Request shaping in
# `ai_client` branches on this, keyed off the resolved Bedrock model id so
# the next model swap is a data change here, not a code change there.
_NO_SAMPLING_PARAMS_MODEL_SUBSTRINGS = ("claude-sonnet-5", "claude-opus-5")


def model_accepts_sampling_params(model_id: str) -> bool:
  """Whether a Bedrock model id accepts `temperature` (Claude 4.x family)."""
  return not any(s in model_id for s in _NO_SAMPLING_PARAMS_MODEL_SUBSTRINGS)


class OperatorExecutionMode(Enum):
  """Operator execution modes with different performance characteristics."""

  QUICK = "quick"
  STANDARD = "standard"
  EXTENDED = "extended"
  STREAMING = "streaming"


@dataclass
class ExecutionProfile:
  """Execution time and resource profile for an operator mode."""

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
  fallback_model: BedrockModel | None = None
  region: str = "us-east-1"
  temperature: float = 0.7
  max_retries: int = 3
  timeout_seconds: int = 60


class OperatorConfig:
  """
  Centralized configuration for the multi-operator system.

  This is the single source of truth for all operator-related settings.
  """

  # AWS Bedrock Model Configuration
  # Using regional inference profiles (us.*) for on-demand access
  BEDROCK_MODELS = {
    BedrockModel.SONNET_5: "us.anthropic.claude-sonnet-5",
    BedrockModel.SONNET_4_6: "us.anthropic.claude-sonnet-4-6",
    BedrockModel.SONNET_4_5: "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    BedrockModel.SONNET_4: "us.anthropic.claude-sonnet-4-20250514-v1:0",
  }

  # Default Model Configuration
  DEFAULT_MODEL_CONFIG = ModelConfig(
    default_model=BedrockModel.SONNET_4_6,
    fallback_model=BedrockModel.SONNET_4_5,
    region=env.AWS_BEDROCK_REGION,
    temperature=0.7,
    max_retries=3,
    timeout_seconds=60,
  )

  # Execution Profiles by Mode
  EXECUTION_PROFILES = {
    OperatorExecutionMode.QUICK: ExecutionProfile(
      min_time_seconds=2,
      max_time_seconds=5,
      avg_time_seconds=3,
      max_tool_calls=2,
      max_input_tokens=50000,
      max_output_tokens=2000,
      timeout_seconds=30,
    ),
    OperatorExecutionMode.STANDARD: ExecutionProfile(
      min_time_seconds=5,
      max_time_seconds=15,
      avg_time_seconds=10,
      max_tool_calls=5,
      max_input_tokens=100000,
      max_output_tokens=4000,
      timeout_seconds=60,
    ),
    OperatorExecutionMode.EXTENDED: ExecutionProfile(
      min_time_seconds=30,
      max_time_seconds=120,
      avg_time_seconds=60,
      max_tool_calls=12,
      max_input_tokens=150000,
      max_output_tokens=8000,
      timeout_seconds=300,
    ),
    OperatorExecutionMode.STREAMING: ExecutionProfile(
      min_time_seconds=5,
      max_time_seconds=60,
      avg_time_seconds=20,
      max_tool_calls=8,
      max_input_tokens=100000,
      max_output_tokens=8000,
      timeout_seconds=120,
    ),
  }

  # Operator-Specific Model Overrides
  # Allows different operators to use different models if needed
  OPERATOR_MODEL_OVERRIDES: dict[str, BedrockModel] = {
    # Example: "financial": BedrockModel.SONNET_4_5,
    # Example: "cypher": BedrockModel.SONNET_4,
  }

  # Orchestrator Configuration
  ORCHESTRATOR_CONFIG = {
    "fallback_operator": "cypher",
    "confidence_threshold": 0.7,
    "max_routing_attempts": 3,
    "enable_rag": False,
    "routing_strategy": "best_match",
  }

  # Operator Capabilities Configuration
  OPERATOR_CAPABILITIES = {
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
    cls, model: BedrockModel | None = None, operator_type: str | None = None
  ) -> str:
    """
    Get the AWS Bedrock model ID for a given model or operator.

    Args:
        model: Optional model enum (overrides default)
        operator_type: Optional operator type to check for overrides

    Returns:
        Bedrock model ID string
    """
    # Check operator-specific overrides first
    if operator_type and operator_type in cls.OPERATOR_MODEL_OVERRIDES:
      model = cls.OPERATOR_MODEL_OVERRIDES[operator_type]

    # Use provided model or default
    if not model:
      model = cls.DEFAULT_MODEL_CONFIG.default_model

    return cls.BEDROCK_MODELS.get(model, cls.BEDROCK_MODELS[BedrockModel.SONNET_4])

  @classmethod
  def get_execution_profile(cls, mode: OperatorExecutionMode) -> ExecutionProfile:
    """Get execution profile for a given mode."""
    return cls.EXECUTION_PROFILES.get(
      mode, cls.EXECUTION_PROFILES[OperatorExecutionMode.STANDARD]
    )

  @classmethod
  def get_mode_limits(cls, mode: str) -> dict[str, Any]:
    """
    Get operational limits for a mode (backward compatible with BaseOperator).

    Args:
        mode: Mode name as string

    Returns:
        Dict with limits
    """
    try:
      mode_enum = OperatorExecutionMode(mode.lower())
    except ValueError:
      mode_enum = OperatorExecutionMode.STANDARD

    profile = cls.get_execution_profile(mode_enum)

    return {
      "max_tools": profile.max_tool_calls,
      "timeout": profile.timeout_seconds,
      "max_input_tokens": profile.max_input_tokens,
      "max_output_tokens": profile.max_output_tokens,
    }

  @classmethod
  def get_operator_capabilities(cls, operator_type: str) -> dict[str, Any]:
    """Get capabilities configuration for an operator type."""
    return cls.OPERATOR_CAPABILITIES.get(
      operator_type,
      {
        "supported_modes": ["quick", "standard", "extended"],
        "requires_credits": True,
        "max_concurrent_requests": 5,
      },
    )

  @classmethod
  def validate_configuration(cls) -> dict[str, Any]:
    """
    Validate operator configuration consistency.

    Returns:
        Dict with validation results
    """
    issues = []

    # Validate all operator overrides reference valid models
    for operator_type, model in cls.OPERATOR_MODEL_OVERRIDES.items():
      if model not in cls.BEDROCK_MODELS:
        issues.append(f"Operator '{operator_type}' has invalid model override: {model}")

    # Validate default model exists
    if cls.DEFAULT_MODEL_CONFIG.default_model not in cls.BEDROCK_MODELS:
      issues.append(
        f"Default model not found: {cls.DEFAULT_MODEL_CONFIG.default_model}"
      )

    # Validate fallback operator exists in capabilities
    fallback = cls.ORCHESTRATOR_CONFIG.get("fallback_operator")
    if fallback and fallback not in cls.OPERATOR_CAPABILITIES:
      issues.append(f"Fallback operator not found in capabilities: {fallback}")

    # Validate all modes have execution profiles
    for mode in OperatorExecutionMode:
      if mode not in cls.EXECUTION_PROFILES:
        issues.append(f"Missing execution profile for mode: {mode.value}")

    return {
      "valid": len(issues) == 0,
      "issues": issues,
      "summary": {
        "models": len(cls.BEDROCK_MODELS),
        "execution_profiles": len(cls.EXECUTION_PROFILES),
        "operator_capabilities": len(cls.OPERATOR_CAPABILITIES),
      },
    }

  @classmethod
  def get_all_config(cls) -> dict[str, Any]:
    """
    Get complete operator configuration.

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
        "available_models": [model.value for model in cls.BEDROCK_MODELS],
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
      "operator_capabilities": cls.OPERATOR_CAPABILITIES,
    }
