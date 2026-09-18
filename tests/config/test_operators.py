"""Operator system configuration tests."""

import pytest

from robosystems.config.billing.ai import AIBillingConfig
from robosystems.config.operators import (
  ExecutionProfile,
  ModelProfile,
  ModelProvider,
  ModelSpec,
  OperatorConfig,
  OperatorExecutionMode,
  OperatorModel,
  build_model_registry,
  build_profile_models,
)


class TestModelRegistry:
  """Every model the platform can run is a registry row, and every row is
  priced. The registry is what the client and the meter both read."""

  def test_every_platform_model_has_a_spec(self):
    for model in OperatorModel:
      if model is OperatorModel.OPENAI_COMPAT:
        continue  # a deployment's own model; see TestSelfHostedModel
      assert model in OperatorConfig.MODEL_REGISTRY
      spec = OperatorConfig.MODEL_REGISTRY[model]
      assert isinstance(spec, ModelSpec)
      assert spec.provider is ModelProvider.BEDROCK

  def test_every_spec_bills_under_a_rate_card_key(self):
    for model, spec in OperatorConfig.MODEL_REGISTRY.items():
      assert spec.pricing_key in AIBillingConfig.TOKEN_PRICING, model

  def test_every_profile_maps_to_a_registered_model(self):
    for profile in ModelProfile:
      assert OperatorConfig.PROFILE_MODELS[profile] in OperatorConfig.MODEL_REGISTRY

  def test_wire_ids_are_regional_inference_profiles(self):
    """`us.*` keeps inference in the US; `global.*` is a later margin lever."""
    for spec in OperatorConfig.MODEL_REGISTRY.values():
      assert spec.model_id.startswith("us."), spec.model_id

  def test_claude_5_family_disables_thinking_and_takes_no_sampling_params(self):
    for model in (OperatorModel.SONNET_5, OperatorModel.OPUS_5):
      spec = OperatorConfig.MODEL_REGISTRY[model]
      assert spec.accepts_sampling_params is False
      assert spec.additional_request_fields == {"thinking": {"type": "disabled"}}
      assert spec.cache_points is True

  def test_claude_4_family_accepts_sampling_params(self):
    for model in (
      OperatorModel.SONNET_4_6,
      OperatorModel.SONNET_4_5,
      OperatorModel.SONNET_4,
    ):
      spec = OperatorConfig.MODEL_REGISTRY[model]
      assert spec.accepts_sampling_params is True
      assert spec.additional_request_fields == {}

  def test_luna_takes_no_cache_points_and_no_sampling_params(self):
    """Verified over Converse 2026-09-15: explicit cachePoint blocks and
    `temperature` are both rejected; implicit caching reports through usage."""
    spec = OperatorConfig.MODEL_REGISTRY[OperatorModel.GPT_5_6_LUNA]
    assert spec.model_id == "us.openai.gpt-5.6-luna"
    assert spec.cache_points is False
    assert spec.accepts_sampling_params is False
    assert spec.additional_request_fields == {}
    assert spec.pricing_key == "openai_gpt_5_6_luna"


class TestSelfHostedModel:
  """The self-hosted model exists only in a deployment that turns it on. Off
  — hosted prod — nothing can resolve to it and the meter refuses it."""

  URL = "http://localhost:11434/v1"

  def test_off_by_default(self):
    assert OperatorModel.OPENAI_COMPAT not in OperatorConfig.MODEL_REGISTRY
    assert "openai_compat" not in AIBillingConfig.TOKEN_PRICING
    with pytest.raises(KeyError):
      OperatorConfig.resolve_model("openai-compat")

  def test_off_leaves_the_platform_registry_unchanged(self):
    registry = build_model_registry(False, self.URL, "qwen3:32b", 0)
    assert registry == OperatorConfig.MODEL_REGISTRY

  def test_enabled_registers_the_deployment_model(self):
    registry = build_model_registry(True, self.URL, "qwen3:32b", 0)
    spec = registry[OperatorModel.OPENAI_COMPAT]
    assert spec.provider is ModelProvider.OPENAI_COMPAT
    assert spec.model_id == "qwen3:32b"
    assert spec.pricing_key == "openai_compat"
    assert spec.cache_points is False
    assert spec.accepts_sampling_params is True
    assert spec.max_output_tokens is None

  def test_output_cap_is_carried(self):
    registry = build_model_registry(True, self.URL, "glm-4.7-flash", 4096)
    assert registry[OperatorModel.OPENAI_COMPAT].max_output_tokens == 4096

  @pytest.mark.parametrize(("url", "model"), [("", "qwen3:32b"), (URL, "")])
  def test_half_configured_fails(self, url, model):
    with pytest.raises(ValueError, match="OPENAI_COMPAT_BASE_URL or"):
      build_model_registry(True, url, model, 0)

  @pytest.mark.parametrize(
    "model",
    ["claude-sonnet-5", "openai-compat", "balanced", "us.anthropic.claude-sonnet-5"],
  )
  def test_id_that_would_resolve_as_something_else_fails(self, model):
    """Resolution matches short names, profiles and wire ids — a colliding
    id would run and bill as another model."""
    with pytest.raises(ValueError, match="collides"):
      build_model_registry(True, self.URL, model, 0)


class TestProfileOverrides:
  """OPERATOR_PROFILE_* re-points a tier for one deployment."""

  NONE = dict.fromkeys(ModelProfile, "")

  def test_no_overrides_keeps_the_platform_map(self):
    mapping = build_profile_models(self.NONE, OperatorConfig.MODEL_REGISTRY)
    assert mapping == OperatorConfig.PROFILE_MODELS

  def test_a_tier_can_point_at_the_self_hosted_model(self):
    registry = build_model_registry(True, "http://h/v1", "qwen3:32b", 0)
    mapping = build_profile_models(
      {**self.NONE, ModelProfile.BALANCED: "openai-compat"}, registry
    )
    assert mapping[ModelProfile.BALANCED] is OperatorModel.OPENAI_COMPAT
    assert mapping[ModelProfile.QUALITY] is OperatorModel.OPUS_5

  def test_a_tier_can_point_at_another_platform_model(self):
    mapping = build_profile_models(
      {**self.NONE, ModelProfile.QUALITY: "claude-sonnet-5"},
      OperatorConfig.MODEL_REGISTRY,
    )
    assert mapping[ModelProfile.QUALITY] is OperatorModel.SONNET_5

  def test_unknown_name_fails_the_boot(self):
    with pytest.raises(ValueError, match="OPERATOR_PROFILE_ECONOMY"):
      build_profile_models(
        {**self.NONE, ModelProfile.ECONOMY: "no-such-model"},
        OperatorConfig.MODEL_REGISTRY,
      )

  def test_self_hosted_tier_without_the_provider_fails_the_boot(self):
    with pytest.raises(ValueError, match="needs OPENAI_COMPAT_ENABLED"):
      build_profile_models(
        {**self.NONE, ModelProfile.BALANCED: "openai-compat"},
        OperatorConfig.MODEL_REGISTRY,
      )


class TestProfiles:
  def test_profile_values(self):
    assert {p.value for p in ModelProfile} == {"economy", "balanced", "quality"}

  def test_balanced_is_the_default_and_runs_sonnet_5(self):
    assert OperatorConfig.DEFAULT_MODEL_CONFIG.default_profile == ModelProfile.BALANCED
    assert (
      OperatorConfig.PROFILE_MODELS[ModelProfile.BALANCED] == OperatorModel.SONNET_5
    )
    assert OperatorConfig.get_bedrock_model_id() == "us.anthropic.claude-sonnet-5"

  def test_quality_and_economy_targets(self):
    assert OperatorConfig.PROFILE_MODELS[ModelProfile.QUALITY] == OperatorModel.OPUS_5
    assert (
      OperatorConfig.PROFILE_MODELS[ModelProfile.ECONOMY] == OperatorModel.GPT_5_6_LUNA
    )


class TestResolveModel:
  """Most specific wins: explicit choice → operator override → default."""

  def test_default(self):
    spec = OperatorConfig.resolve_model()
    assert spec.model_id == "us.anthropic.claude-sonnet-5"

  def test_explicit_enum(self):
    assert (
      OperatorConfig.resolve_model(OperatorModel.SONNET_4).model_id
      == "us.anthropic.claude-sonnet-4-20250514-v1:0"
    )

  def test_explicit_short_name_and_wire_id(self):
    assert OperatorConfig.resolve_model("claude-opus-5").model_id == (
      "us.anthropic.claude-opus-5"
    )
    assert OperatorConfig.resolve_model("us.anthropic.claude-opus-5").model_id == (
      "us.anthropic.claude-opus-5"
    )

  def test_explicit_profile_by_enum_and_by_name(self):
    assert OperatorConfig.resolve_model(ModelProfile.ECONOMY).model_id == (
      "us.openai.gpt-5.6-luna"
    )
    assert OperatorConfig.resolve_model("quality").model_id == (
      "us.anthropic.claude-opus-5"
    )

  def test_unknown_choice_raises(self):
    """An unregistered model is a configuration error, not something to
    guess a default for."""
    with pytest.raises(ValueError, match="Unknown model or profile"):
      OperatorConfig.resolve_model("not-a-real-model")

  def test_operator_override_takes_precedence_over_default(self, monkeypatch):
    monkeypatch.setitem(
      OperatorConfig.OPERATOR_MODEL_OVERRIDES, "test_agent", ModelProfile.ECONOMY
    )
    assert OperatorConfig.get_bedrock_model_id(operator_type="test_agent") == (
      "us.openai.gpt-5.6-luna"
    )

  def test_explicit_choice_beats_operator_override(self, monkeypatch):
    monkeypatch.setitem(
      OperatorConfig.OPERATOR_MODEL_OVERRIDES, "test_agent", ModelProfile.ECONOMY
    )
    assert OperatorConfig.get_bedrock_model_id(
      model=OperatorModel.SONNET_4_6, operator_type="test_agent"
    ) == ("us.anthropic.claude-sonnet-4-6")

  def test_unknown_operator_type_falls_through_to_default(self):
    assert OperatorConfig.get_bedrock_model_id(operator_type="analyst") == (
      "us.anthropic.claude-sonnet-5"
    )


class TestPricingKeyFor:
  def test_wire_id_and_short_name(self):
    assert OperatorConfig.pricing_key_for("us.anthropic.claude-sonnet-5") == (
      "anthropic_claude_5_sonnet"
    )
    assert OperatorConfig.pricing_key_for("claude-sonnet-4-6") == (
      "anthropic_claude_4_sonnet"
    )
    assert OperatorConfig.pricing_key_for("us.openai.gpt-5.6-luna") == (
      "openai_gpt_5_6_luna"
    )

  def test_unregistered_model_raises(self):
    """The meter must never price an unknown model at another model's rate."""
    with pytest.raises(ValueError, match="Unknown model or profile"):
      OperatorConfig.pricing_key_for("claude-4-sonnet")


class TestOperatorExecutionMode:
  """Tests for OperatorExecutionMode enum."""

  def test_all_modes_have_profiles(self):
    for mode in OperatorExecutionMode:
      assert mode in OperatorConfig.EXECUTION_PROFILES

  def test_mode_values(self):
    assert OperatorExecutionMode.QUICK.value == "quick"
    assert OperatorExecutionMode.STANDARD.value == "standard"
    assert OperatorExecutionMode.EXTENDED.value == "extended"
    assert OperatorExecutionMode.STREAMING.value == "streaming"


class TestGetExecutionProfile:
  """Tests for OperatorConfig.get_execution_profile."""

  def test_returns_profile(self):
    profile = OperatorConfig.get_execution_profile(OperatorExecutionMode.QUICK)
    assert isinstance(profile, ExecutionProfile)

  def test_quick_profile_values(self):
    profile = OperatorConfig.get_execution_profile(OperatorExecutionMode.QUICK)
    assert profile.max_tool_calls == 2
    assert profile.timeout_seconds == 30

  def test_extended_profile_has_higher_limits(self):
    quick = OperatorConfig.get_execution_profile(OperatorExecutionMode.QUICK)
    extended = OperatorConfig.get_execution_profile(OperatorExecutionMode.EXTENDED)
    assert extended.max_tool_calls > quick.max_tool_calls
    assert extended.max_output_tokens > quick.max_output_tokens
    assert extended.timeout_seconds > quick.timeout_seconds


class TestGetModeLimits:
  """Tests for OperatorConfig.get_mode_limits."""

  def test_returns_dict_with_expected_keys(self):
    limits = OperatorConfig.get_mode_limits("standard")
    assert "max_tools" in limits
    assert "timeout" in limits
    assert "max_input_tokens" in limits
    assert "max_output_tokens" in limits

  def test_invalid_mode_falls_back_to_standard(self):
    limits = OperatorConfig.get_mode_limits("nonexistent")
    standard = OperatorConfig.get_mode_limits("standard")
    assert limits == standard

  def test_case_insensitive(self):
    upper = OperatorConfig.get_mode_limits("QUICK")
    lower = OperatorConfig.get_mode_limits("quick")
    assert upper == lower


class TestGetOperatorCapabilities:
  """Tests for OperatorConfig.get_operator_capabilities."""

  def test_known_agent(self):
    caps = OperatorConfig.get_operator_capabilities("analyst")
    assert "supported_modes" in caps
    assert "requires_credits" in caps
    assert "max_concurrent_requests" in caps

  def test_unknown_agent_returns_defaults(self):
    caps = OperatorConfig.get_operator_capabilities("unknown_agent")
    assert caps["requires_credits"] is True
    assert "supported_modes" in caps

  def test_financial_agent(self):
    caps = OperatorConfig.get_operator_capabilities("financial")
    assert "streaming" not in caps["supported_modes"]


class TestValidateConfiguration:
  """Tests for OperatorConfig.validate_configuration."""

  def test_default_config_is_valid(self):
    result = OperatorConfig.validate_configuration()
    assert result["valid"] is True
    assert result["issues"] == []

  def test_summary_has_counts(self):
    result = OperatorConfig.validate_configuration()
    summary = result["summary"]
    assert summary["models"] == len(OperatorConfig.MODEL_REGISTRY)
    assert summary["profiles"] == len(ModelProfile)
    assert summary["execution_profiles"] == len(OperatorConfig.EXECUTION_PROFILES)
    assert summary["operator_capabilities"] == len(OperatorConfig.OPERATOR_CAPABILITIES)

  def test_unpriced_model_is_reported(self, monkeypatch):
    """A registry row without a rate-card entry is the misconfiguration the
    meter's fail-loud exists for; startup validation must name it."""
    from dataclasses import replace

    spec = OperatorConfig.MODEL_REGISTRY[OperatorModel.SONNET_4]
    monkeypatch.setitem(
      OperatorConfig.MODEL_REGISTRY,
      OperatorModel.SONNET_4,
      replace(spec, pricing_key="nonexistent_key"),
    )
    result = OperatorConfig.validate_configuration()
    assert result["valid"] is False
    assert any("nonexistent_key" in issue for issue in result["issues"])

  def test_invalid_override_is_reported(self, monkeypatch):
    monkeypatch.setitem(OperatorConfig.OPERATOR_MODEL_OVERRIDES, "x", "no-such-model")
    result = OperatorConfig.validate_configuration()
    assert result["valid"] is False
    assert any("invalid model override" in issue for issue in result["issues"])


class TestGetAllConfig:
  """Tests for OperatorConfig.get_all_config."""

  def test_returns_complete_config(self):
    config = OperatorConfig.get_all_config()
    assert "models" in config
    assert "execution_profiles" in config
    assert "orchestrator" in config
    assert "operator_capabilities" in config

  def test_models_section(self):
    config = OperatorConfig.get_all_config()
    models = config["models"]
    assert models["default"] == "balanced"
    assert models["default_model_id"] == "us.anthropic.claude-sonnet-5"
    assert "fallback" in models
    assert "region" in models
    assert set(models["available_models"]) == {
      m.value for m in OperatorConfig.MODEL_REGISTRY
    }
    assert "openai-compat" not in models["available_models"]
    assert models["profiles"]["economy"] == "us.openai.gpt-5.6-luna"

  def test_execution_profiles_section(self):
    config = OperatorConfig.get_all_config()
    profiles = config["execution_profiles"]
    for mode in OperatorExecutionMode:
      assert mode.value in profiles
