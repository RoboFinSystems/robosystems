"""Agent system configuration tests."""

from robosystems.config.agents import (
  AgentConfig,
  AgentExecutionMode,
  BedrockModel,
  ExecutionProfile,
)


class TestBedrockModel:
  """Tests for BedrockModel enum."""

  def test_all_models_have_bedrock_ids(self):
    for model in BedrockModel:
      assert model in AgentConfig.BEDROCK_MODELS

  def test_bedrock_ids_are_strings(self):
    for model_id in AgentConfig.BEDROCK_MODELS.values():
      assert isinstance(model_id, str)
      assert "anthropic" in model_id


class TestAgentExecutionMode:
  """Tests for AgentExecutionMode enum."""

  def test_all_modes_have_profiles(self):
    for mode in AgentExecutionMode:
      assert mode in AgentConfig.EXECUTION_PROFILES

  def test_mode_values(self):
    assert AgentExecutionMode.QUICK.value == "quick"
    assert AgentExecutionMode.STANDARD.value == "standard"
    assert AgentExecutionMode.EXTENDED.value == "extended"
    assert AgentExecutionMode.STREAMING.value == "streaming"


class TestGetBedrockModelId:
  """Tests for AgentConfig.get_bedrock_model_id."""

  def test_default_model(self):
    model_id = AgentConfig.get_bedrock_model_id()
    assert isinstance(model_id, str)
    assert "anthropic" in model_id

  def test_specific_model(self):
    model_id = AgentConfig.get_bedrock_model_id(model=BedrockModel.SONNET_4)
    assert "sonnet-4" in model_id

  def test_agent_type_override(self):
    # Currently empty overrides, so it falls through to default
    model_id = AgentConfig.get_bedrock_model_id(agent_type="cypher")
    assert isinstance(model_id, str)

  def test_agent_override_takes_precedence(self):
    original = AgentConfig.AGENT_MODEL_OVERRIDES.copy()
    try:
      AgentConfig.AGENT_MODEL_OVERRIDES["test_agent"] = BedrockModel.SONNET_4
      model_id = AgentConfig.get_bedrock_model_id(agent_type="test_agent")
      expected = AgentConfig.BEDROCK_MODELS[BedrockModel.SONNET_4]
      assert model_id == expected
    finally:
      AgentConfig.AGENT_MODEL_OVERRIDES = original


class TestGetExecutionProfile:
  """Tests for AgentConfig.get_execution_profile."""

  def test_returns_profile(self):
    profile = AgentConfig.get_execution_profile(AgentExecutionMode.QUICK)
    assert isinstance(profile, ExecutionProfile)

  def test_quick_profile_values(self):
    profile = AgentConfig.get_execution_profile(AgentExecutionMode.QUICK)
    assert profile.max_tool_calls == 2
    assert profile.timeout_seconds == 30

  def test_extended_profile_has_higher_limits(self):
    quick = AgentConfig.get_execution_profile(AgentExecutionMode.QUICK)
    extended = AgentConfig.get_execution_profile(AgentExecutionMode.EXTENDED)
    assert extended.max_tool_calls > quick.max_tool_calls
    assert extended.max_output_tokens > quick.max_output_tokens
    assert extended.timeout_seconds > quick.timeout_seconds


class TestGetModeLimits:
  """Tests for AgentConfig.get_mode_limits."""

  def test_returns_dict_with_expected_keys(self):
    limits = AgentConfig.get_mode_limits("standard")
    assert "max_tools" in limits
    assert "timeout" in limits
    assert "max_input_tokens" in limits
    assert "max_output_tokens" in limits

  def test_invalid_mode_falls_back_to_standard(self):
    limits = AgentConfig.get_mode_limits("nonexistent")
    standard = AgentConfig.get_mode_limits("standard")
    assert limits == standard

  def test_case_insensitive(self):
    upper = AgentConfig.get_mode_limits("QUICK")
    lower = AgentConfig.get_mode_limits("quick")
    assert upper == lower


class TestGetAgentCapabilities:
  """Tests for AgentConfig.get_agent_capabilities."""

  def test_known_agent(self):
    caps = AgentConfig.get_agent_capabilities("cypher")
    assert "supported_modes" in caps
    assert "requires_credits" in caps
    assert "max_concurrent_requests" in caps

  def test_unknown_agent_returns_defaults(self):
    caps = AgentConfig.get_agent_capabilities("unknown_agent")
    assert caps["requires_credits"] is True
    assert "supported_modes" in caps

  def test_financial_agent(self):
    caps = AgentConfig.get_agent_capabilities("financial")
    assert "streaming" not in caps["supported_modes"]


class TestValidateConfiguration:
  """Tests for AgentConfig.validate_configuration."""

  def test_default_config_is_valid(self):
    result = AgentConfig.validate_configuration()
    assert result["valid"] is True
    assert result["issues"] == []

  def test_summary_has_counts(self):
    result = AgentConfig.validate_configuration()
    summary = result["summary"]
    assert summary["models"] == len(AgentConfig.BEDROCK_MODELS)
    assert summary["execution_profiles"] == len(AgentConfig.EXECUTION_PROFILES)
    assert summary["agent_capabilities"] == len(AgentConfig.AGENT_CAPABILITIES)
    assert "token_cost_models" not in summary


class TestGetAllConfig:
  """Tests for AgentConfig.get_all_config."""

  def test_returns_complete_config(self):
    config = AgentConfig.get_all_config()
    assert "models" in config
    assert "execution_profiles" in config
    assert "orchestrator" in config
    assert "agent_capabilities" in config

  def test_models_section(self):
    config = AgentConfig.get_all_config()
    models = config["models"]
    assert "default" in models
    assert "fallback" in models
    assert "region" in models
    assert "available_models" in models

  def test_execution_profiles_section(self):
    config = AgentConfig.get_all_config()
    profiles = config["execution_profiles"]
    for mode in AgentExecutionMode:
      assert mode.value in profiles
