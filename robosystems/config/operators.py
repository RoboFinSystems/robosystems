"""
AI Operator configuration: the model registry and profiles, execution
profiles and mode limits, orchestrator routing, and operator capabilities.

"Operator" is the AI-executor concept (Claude/MCP), distinct from REA ``Agent``
(counterparty) in ``models/extensions/roboledger/agent.py``.

Platform models run through Bedrock's Converse API (``operations/operators/
ai_client.py``), so a model swap is a registry row here, not a code change
there. A deployment may also turn on one self-hosted model behind an
OpenAI-compatible endpoint (``OPENAI_COMPAT_ENABLED``) — the open-source
runtime's alternative to Bedrock; it is off in hosted prod. Customer-facing
surfaces name a *profile* (economy / balanced / quality) rather than a model
id: the catalog churns, and a concrete-model enum would churn the published
SDKs with it.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from robosystems.config.env import env


class ModelProfile(Enum):
  """The stable names a caller can bind to. Which model backs each is a
  config value expected to change; the names are not."""

  ECONOMY = "economy"
  BALANCED = "balanced"
  QUALITY = "quality"


class ModelProvider(Enum):
  """Where a model is served."""

  BEDROCK = "bedrock"
  # A self-hosted OpenAI-compatible endpoint (vLLM, Ollama, LM Studio, NIM).
  OPENAI_COMPAT = "openai_compat"


class OperatorModel(Enum):
  """Registered models, by short name. The wire id lives in the registry."""

  SONNET_5 = "claude-sonnet-5"
  SONNET_4_6 = "claude-sonnet-4-6"
  SONNET_4_5 = "claude-sonnet-4-5-20250929"
  SONNET_4 = "claude-sonnet-4-20250514"  # Last resort fallback
  OPUS_5 = "claude-opus-5"
  GPT_5_6_LUNA = "gpt-5.6-luna"
  # The deployment's self-hosted model; which model that is, is the
  # deployment's choice (OPENAI_COMPAT_MODEL). Registered only when enabled.
  OPENAI_COMPAT = "openai-compat"


@dataclass(frozen=True)
class ModelSpec:
  """Everything the client and the meter need to know about one model.

  The behaviour fields are explicit rather than inferred from the id: a
  wrong guess about caching mis-prices a call, a wrong guess about the
  output cap truncates an answer silently, and a wrong guess about sampling
  parameters is a 400 on every call.
  """

  # Inference-profile id sent on the wire (`us.*` = regional, keeps
  # inference in the US; `global.*` exists for each and is a +10% saving
  # traded against data residency — not taken, see ref/economics.md).
  model_id: str
  # Key into AIBillingConfig.TOKEN_PRICING. Validated against the rate card
  # by validate_configuration(); the meter raises on a miss.
  pricing_key: str
  # Emit explicit `cachePoint` blocks (system prefix + trailing turn). Claude
  # accepts them; GPT-5.6 rejects them over Converse but caches implicitly
  # and still reports cache reads in usage, so the meter is unaffected.
  cache_points: bool
  # Whether `temperature` is accepted. The Claude 5 family and GPT-5.6 both
  # reject it with a 400.
  accepts_sampling_params: bool
  # Hard output cap when the model's is below what the execution profiles
  # ask for (EXTENDED asks 8,000). None = no cap that binds. GLM 4.7 would
  # carry 4,096 here.
  max_output_tokens: int | None = None
  # Model-specific request fields passed through Converse's
  # additionalModelRequestFields verbatim.
  additional_request_fields: dict[str, Any] = field(default_factory=dict)
  provider: ModelProvider = ModelProvider.BEDROCK


# Claude 5-family models run adaptive thinking unless it is explicitly
# disabled; thinking tokens would bill as output and eat max_tokens, so it is
# off until adopted deliberately. (Bedrock also requires thinking disabled
# whenever tool choice forces a tool.)
_CLAUDE_5_REQUEST_FIELDS: dict[str, Any] = {"thinking": {"type": "disabled"}}


# The Bedrock rows. Every one is a `us.` cross-region profile today. Most of
# the open-weight catalog is In-Region only (no `us.` / `global.` profiles),
# so the first such row brings a residency/failover field with it.
#
# Wire ids: Bedrock publishes the Claude 5 family and GPT-5.6 as
# unversioned ids (`us.anthropic.claude-sonnet-5`, no `-v1:0`); the 4.x
# rows keep the versioned form. Both shapes are verified against the
# account's inference-profile list — do not "fix" one to match the other.
_BEDROCK_MODELS: dict[OperatorModel, ModelSpec] = {
  OperatorModel.SONNET_5: ModelSpec(
    model_id="us.anthropic.claude-sonnet-5",
    pricing_key="anthropic_claude_5_sonnet",
    cache_points=True,
    accepts_sampling_params=False,
    additional_request_fields=_CLAUDE_5_REQUEST_FIELDS,
  ),
  OperatorModel.SONNET_4_6: ModelSpec(
    model_id="us.anthropic.claude-sonnet-4-6",
    pricing_key="anthropic_claude_4_sonnet",
    cache_points=True,
    accepts_sampling_params=True,
  ),
  OperatorModel.SONNET_4_5: ModelSpec(
    model_id="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    pricing_key="anthropic_claude_4_sonnet",
    cache_points=True,
    accepts_sampling_params=True,
  ),
  OperatorModel.SONNET_4: ModelSpec(
    model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
    pricing_key="anthropic_claude_4_sonnet",
    cache_points=True,
    accepts_sampling_params=True,
  ),
  OperatorModel.OPUS_5: ModelSpec(
    model_id="us.anthropic.claude-opus-5",
    pricing_key="anthropic_claude_5_opus",
    cache_points=True,
    accepts_sampling_params=False,
    additional_request_fields=_CLAUDE_5_REQUEST_FIELDS,
  ),
  # Verified over Converse 2026-09-15: tool use works, `temperature` and
  # explicit cache points are rejected, implicit caching reports through
  # usage. Not available In-Region on bedrock-runtime — the `us.` profile
  # is the only regional address.
  OperatorModel.GPT_5_6_LUNA: ModelSpec(
    model_id="us.openai.gpt-5.6-luna",
    pricing_key="openai_gpt_5_6_luna",
    cache_points=False,
    accepts_sampling_params=False,
  ),
}

# Profile → model, platform-wide. A config value: re-point a profile at
# next quarter's winner without touching any caller.
_PLATFORM_PROFILE_MODELS: dict[ModelProfile, OperatorModel] = {
  ModelProfile.ECONOMY: OperatorModel.GPT_5_6_LUNA,
  ModelProfile.BALANCED: OperatorModel.SONNET_5,
  ModelProfile.QUALITY: OperatorModel.OPUS_5,
}


def self_hosted_model_spec(model_id: str, max_output_tokens: int) -> ModelSpec:
  """The row for a deployment's self-hosted model.

  Sampling parameters are accepted by every OpenAI-compatible server we
  target; there are no cache points to send (a server that caches prefixes
  does it on its own and reports the reads in usage).
  """
  return ModelSpec(
    model_id=model_id,
    pricing_key="openai_compat",
    cache_points=False,
    accepts_sampling_params=True,
    max_output_tokens=max_output_tokens or None,
    provider=ModelProvider.OPENAI_COMPAT,
  )


def build_model_registry(
  openai_compat_enabled: bool,
  openai_compat_base_url: str,
  openai_compat_model: str,
  openai_compat_max_output_tokens: int,
) -> dict[OperatorModel, ModelSpec]:
  """The registry for this deployment. Raises on a half-configured
  self-hosted model rather than booting with a row that cannot be called."""
  registry = dict(_BEDROCK_MODELS)
  if not openai_compat_enabled:
    return registry
  if not openai_compat_base_url or not openai_compat_model:
    raise ValueError(
      "OPENAI_COMPAT_ENABLED is on but OPENAI_COMPAT_BASE_URL or "
      "OPENAI_COMPAT_MODEL is empty"
    )
  if openai_compat_max_output_tokens < 0:
    raise ValueError("OPENAI_COMPAT_MAX_OUTPUT_TOKENS cannot be negative (0 = no cap)")
  # Resolution matches short names, profile names and wire ids in turn, so a
  # self-hosted id that equals any of them would resolve — and bill — as
  # something else.
  taken = (
    {m.value for m in OperatorModel}
    | {p.value for p in ModelProfile}
    | {spec.model_id for spec in _BEDROCK_MODELS.values()}
  )
  if openai_compat_model in taken:
    raise ValueError(
      f"OPENAI_COMPAT_MODEL={openai_compat_model!r} collides with a registered "
      "model or profile name"
    )
  registry[OperatorModel.OPENAI_COMPAT] = self_hosted_model_spec(
    openai_compat_model, openai_compat_max_output_tokens
  )
  return registry


def build_profile_models(
  overrides: dict[ModelProfile, str],
  registry: dict[OperatorModel, ModelSpec],
) -> dict[ModelProfile, OperatorModel]:
  """The platform profile map with this deployment's overrides applied.

  An override names a registered short name. An unknown name, or one whose
  row this deployment does not register, fails the boot: a tier that cannot
  run is a configuration error, not something to fall back from quietly.
  """
  mapping = dict(_PLATFORM_PROFILE_MODELS)
  for profile, name in overrides.items():
    if not name:
      continue
    try:
      model = OperatorModel(name)
    except ValueError:
      raise ValueError(
        f"OPERATOR_PROFILE_{profile.name}={name!r} is not a registered model; "
        f"registered: {sorted(m.value for m in OperatorModel)}"
      ) from None
    if model not in registry:
      raise ValueError(
        f"OPERATOR_PROFILE_{profile.name}={name!r} is not available in this "
        "deployment (the self-hosted model needs OPENAI_COMPAT_ENABLED)"
      )
    mapping[profile] = model
  return mapping


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
  """Platform-wide model defaults."""

  default_profile: ModelProfile
  fallback_model: OperatorModel | None = None
  region: str = "us-east-1"
  temperature: float = 0.7
  max_retries: int = 3
  timeout_seconds: int = 60


class OperatorConfig:
  """
  Centralized configuration for the multi-operator system.

  This is the single source of truth for all operator-related settings.
  """

  # One row per model this deployment can run: the Bedrock rows always, the
  # self-hosted row only when OPENAI_COMPAT_ENABLED is on. When it is off,
  # nothing can resolve to that row and the meter refuses it.
  MODEL_REGISTRY: dict[OperatorModel, ModelSpec] = build_model_registry(
    openai_compat_enabled=env.OPENAI_COMPAT_ENABLED,
    openai_compat_base_url=env.OPENAI_COMPAT_BASE_URL,
    openai_compat_model=env.OPENAI_COMPAT_MODEL,
    openai_compat_max_output_tokens=env.OPENAI_COMPAT_MAX_OUTPUT_TOKENS,
  )

  # Profile → model. The platform mapping, re-pointed per deployment by
  # OPERATOR_PROFILE_*. No platform default moves without an A/B on the real
  # operator shape first (specs/ai-operators/llm-provider-abstraction §4.1).
  PROFILE_MODELS: dict[ModelProfile, OperatorModel] = build_profile_models(
    {
      ModelProfile.ECONOMY: env.OPERATOR_PROFILE_ECONOMY,
      ModelProfile.BALANCED: env.OPERATOR_PROFILE_BALANCED,
      ModelProfile.QUALITY: env.OPERATOR_PROFILE_QUALITY,
    },
    MODEL_REGISTRY,
  )

  # Default Model Configuration
  DEFAULT_MODEL_CONFIG = ModelConfig(
    default_profile=ModelProfile.BALANCED,
    fallback_model=OperatorModel.SONNET_4_6,
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

  # Per-operator-class routing: a profile or a pinned model. Sits between
  # an explicit per-call choice and the platform default. The natural home
  # for running RFS's own graphs on the economy profile without any
  # customer-facing choice existing.
  OPERATOR_MODEL_OVERRIDES: dict[str, ModelProfile | OperatorModel] = {
    # Example: "analyst": ModelProfile.ECONOMY,
    # Example: "mapping": OperatorModel.SONNET_5,
  }

  # Orchestrator Configuration
  ORCHESTRATOR_CONFIG = {
    "fallback_operator": "analyst",
    "confidence_threshold": 0.7,
    "max_routing_attempts": 3,
    "enable_rag": False,
    "routing_strategy": "best_match",
  }

  # Operator Capabilities Configuration
  OPERATOR_CAPABILITIES = {
    "analyst": {
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
  def to_registered_model(
    cls, choice: str | OperatorModel | ModelProfile
  ) -> OperatorModel:
    """Map a profile, short name, or wire id onto a registry key.

    Raises ValueError for anything unregistered: an unknown model is a
    configuration error, never something to guess a default for.
    """
    if isinstance(choice, ModelProfile):
      return cls.PROFILE_MODELS[choice]
    if isinstance(choice, OperatorModel):
      return choice
    try:
      return cls.PROFILE_MODELS[ModelProfile(choice)]
    except ValueError:
      pass
    try:
      return OperatorModel(choice)
    except ValueError:
      pass
    for model, spec in cls.MODEL_REGISTRY.items():
      if spec.model_id == choice:
        return model
    registered = sorted(m.value for m in cls.MODEL_REGISTRY)
    raise ValueError(
      f"Unknown model or profile {choice!r}; registered models: {registered}, "
      f"profiles: {[p.value for p in ModelProfile]}"
    )

  @classmethod
  def resolve_model(
    cls,
    model: str | OperatorModel | ModelProfile | None = None,
    operator_type: str | None = None,
  ) -> ModelSpec:
    """Resolve what runs: most specific wins.

    An explicit per-call model or profile, then the operator class's
    override, then the platform default profile.
    """
    choice: str | OperatorModel | ModelProfile | None = model
    if choice is None and operator_type:
      choice = cls.OPERATOR_MODEL_OVERRIDES.get(operator_type)
    if choice is None:
      choice = cls.DEFAULT_MODEL_CONFIG.default_profile
    return cls.MODEL_REGISTRY[cls.to_registered_model(choice)]

  @classmethod
  def get_bedrock_model_id(
    cls,
    model: str | OperatorModel | ModelProfile | None = None,
    operator_type: str | None = None,
  ) -> str:
    """The wire id `resolve_model` lands on."""
    return cls.resolve_model(model, operator_type).model_id

  @classmethod
  def pricing_key_for(cls, model: str) -> str:
    """The rate-card key a model bills under.

    Takes the wire id (what `AIResponse.model` carries) or a short name.
    Raises ValueError for an unregistered model — the meter must never
    price an unknown model at some other model's rate.
    """
    return cls.MODEL_REGISTRY[cls.to_registered_model(model)].pricing_key

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
    from robosystems.config.billing.ai import AIBillingConfig

    issues = []

    # Every registered model must bill under a key the rate card knows.
    for model, spec in cls.MODEL_REGISTRY.items():
      if spec.pricing_key not in AIBillingConfig.TOKEN_PRICING:
        issues.append(
          f"Model '{model.value}' has no rate-card entry for pricing key "
          f"'{spec.pricing_key}'"
        )

    # Every profile must land on a registered model.
    for profile in ModelProfile:
      target = cls.PROFILE_MODELS.get(profile)
      if target not in cls.MODEL_REGISTRY:
        issues.append(f"Profile '{profile.value}' maps to unregistered model: {target}")

    # Validate all operator overrides reference valid models or profiles
    for operator_type, choice in cls.OPERATOR_MODEL_OVERRIDES.items():
      try:
        cls.to_registered_model(choice)
      except ValueError:
        issues.append(
          f"Operator '{operator_type}' has invalid model override: {choice}"
        )

    # Validate the default resolves
    try:
      cls.resolve_model()
    except (ValueError, KeyError) as e:
      issues.append(f"Default model does not resolve: {e}")

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
        "models": len(cls.MODEL_REGISTRY),
        "profiles": len(cls.PROFILE_MODELS),
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
        "default": cls.DEFAULT_MODEL_CONFIG.default_profile.value,
        "default_model_id": cls.resolve_model().model_id,
        "fallback": (
          cls.DEFAULT_MODEL_CONFIG.fallback_model.value
          if cls.DEFAULT_MODEL_CONFIG.fallback_model
          else None
        ),
        "region": cls.DEFAULT_MODEL_CONFIG.region,
        "available_models": [model.value for model in cls.MODEL_REGISTRY],
        "profiles": {
          profile.value: cls.MODEL_REGISTRY[model].model_id
          for profile, model in cls.PROFILE_MODELS.items()
        },
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
