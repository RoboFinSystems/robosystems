from robosystems.config import env
from robosystems.config.external_services import ExternalServicesConfig


def test_get_config_returns_expected_service_dictionary():
  config = ExternalServicesConfig.get_config("sec")

  assert config is ExternalServicesConfig.SEC_CONFIG
  assert config["base_url"] == "https://www.sec.gov"
  assert config["headers"]["User-Agent"] == env.SEC_GOV_USER_AGENT


def test_get_config_returns_empty_dict_for_unknown_service():
  assert ExternalServicesConfig.get_config("nonexistent") == {}


def test_get_api_key_reads_directly_from_environment(monkeypatch):
  monkeypatch.setenv("MY_SERVICE_API_KEY", "super-secret")

  assert ExternalServicesConfig.get_api_key("my_service") == "super-secret"


def test_get_endpoint_switches_to_quickbooks_sandbox(monkeypatch):
  monkeypatch.setattr(env, "QUICKBOOKS_SANDBOX", True)

  endpoint = ExternalServicesConfig.get_endpoint("quickbooks", "/v3/company")

  assert endpoint == (
    f"{ExternalServicesConfig.QUICKBOOKS_CONFIG['sandbox_url']}/v3/company"
  )


def test_get_endpoint_uses_production_url_when_not_sandbox(monkeypatch):
  monkeypatch.setattr(env, "QUICKBOOKS_SANDBOX", False)

  endpoint = ExternalServicesConfig.get_endpoint("quickbooks", "v3/company")

  assert endpoint == (
    f"{ExternalServicesConfig.QUICKBOOKS_CONFIG['base_url']}/v3/company"
  )


def test_is_sandbox_reflects_environment_variable(monkeypatch):
  monkeypatch.delenv("OPENFIGI_SANDBOX", raising=False)
  assert not ExternalServicesConfig.is_sandbox("openfigi")

  monkeypatch.setenv("OPENFIGI_SANDBOX", "true")
  assert ExternalServicesConfig.is_sandbox("openfigi")
