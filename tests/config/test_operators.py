"""Operator system configuration tests."""

from robosystems.config.operators import (
  BedrockModel,
  ExecutionProfile,
  OperatorConfig,
  OperatorExecutionMode,
)


class TestBedrockModel:
  """Tests for BedrockModel enum."""

  def test_all_models_have_bedrock_ids(self):
    for model in BedrockModel:
      assert model in OperatorConfig.BEDROCK_MODELS

  def test_bedrock_ids_are_strings(self):
    for model_id in OperatorConfig.BEDROCK_MODELS.values():
      assert isinstance(model_id, str)
      assert "anthropic" in model_id


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


class TestGetBedrockModelId:
  """Tests for OperatorConfig.get_bedrock_model_id."""

  def test_default_model(self):
    model_id = OperatorConfig.get_bedrock_model_id()
    assert isinstance(model_id, str)
    assert "anthropic" in model_id

  def test_specific_model(self):
    model_id = OperatorConfig.get_bedrock_model_id(model=BedrockModel.SONNET_4)
    assert "sonnet-4" in model_id

  def test_agent_type_override(self):
    # Currently empty overrides, so it falls through to default
    model_id = OperatorConfig.get_bedrock_model_id(operator_type="analyst")
    assert isinstance(model_id, str)

  def test_agent_override_takes_precedence(self):
    original = OperatorConfig.OPERATOR_MODEL_OVERRIDES.copy()
    try:
      OperatorConfig.OPERATOR_MODEL_OVERRIDES["test_agent"] = BedrockModel.SONNET_4
      model_id = OperatorConfig.get_bedrock_model_id(operator_type="test_agent")
      expected = OperatorConfig.BEDROCK_MODELS[BedrockModel.SONNET_4]
      assert model_id == expected
    finally:
      OperatorConfig.OPERATOR_MODEL_OVERRIDES = original


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
    assert summary["models"] == len(OperatorConfig.BEDROCK_MODELS)
    assert summary["execution_profiles"] == len(OperatorConfig.EXECUTION_PROFILES)
    assert summary["operator_capabilities"] == len(OperatorConfig.OPERATOR_CAPABILITIES)
    assert "token_cost_models" not in summary


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
    assert "default" in models
    assert "fallback" in models
    assert "region" in models
    assert "available_models" in models

  def test_execution_profiles_section(self):
    config = OperatorConfig.get_all_config()
    profiles = config["execution_profiles"]
    for mode in OperatorExecutionMode:
      assert mode.value in profiles


class TestModelFamilyRequestShaping:
  """Sampling-param support is keyed off the resolved Bedrock model id so a
  model swap is a data change in BEDROCK_MODELS, not a code change in
  ai_client."""

  def test_claude_4_family_accepts_sampling_params(self):
    from robosystems.config.operators import model_accepts_sampling_params

    assert model_accepts_sampling_params("us.anthropic.claude-sonnet-4-6")
    assert model_accepts_sampling_params("us.anthropic.claude-sonnet-4-5-20250929-v1:0")

  def test_claude_5_family_does_not(self):
    from robosystems.config.operators import model_accepts_sampling_params

    assert not model_accepts_sampling_params("us.anthropic.claude-sonnet-5")
    assert not model_accepts_sampling_params("global.anthropic.claude-opus-5")

  def test_sonnet_5_is_registered_but_not_default(self):
    from robosystems.config.operators import BedrockModel, OperatorConfig

    assert (
      OperatorConfig.BEDROCK_MODELS[BedrockModel.SONNET_5]
      == "us.anthropic.claude-sonnet-5"
    )
    assert OperatorConfig.DEFAULT_MODEL_CONFIG.default_model == BedrockModel.SONNET_5
    assert OperatorConfig.DEFAULT_MODEL_CONFIG.fallback_model == BedrockModel.SONNET_4_6
    assert OperatorConfig.get_bedrock_model_id() == "us.anthropic.claude-sonnet-5"
