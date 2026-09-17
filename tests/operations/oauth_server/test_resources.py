"""Canonical resources, scope normalization, and the discovery documents."""

import pytest

from robosystems.operations.oauth_server.resources import (
  agnostic_target,
  authorization_server_metadata,
  bearer_challenge,
  graph_target,
  issuer,
  normalize_scope,
  prm_url,
  protected_resource_metadata,
  resolve_resource,
  roboledger_target,
  route_target,
)

KG = "kg19fb490f76871d22e835"


@pytest.mark.usefixtures("oauth_env")
class TestResolveResource:
  def test_issuer_has_no_trailing_slash(self, oauth_env):
    oauth_env.ROBOSYSTEMS_API_URL = "https://api.test.example/"
    assert issuer() == "https://api.test.example"

  def test_absent_resource_is_the_agnostic_route(self):
    target = resolve_resource(None)
    assert target == agnostic_target()
    assert target.is_agnostic
    assert target.resource == "https://api.test.example/v1/mcp"

  def test_exact_agnostic_url(self):
    assert resolve_resource("https://api.test.example/v1/mcp") == agnostic_target()

  def test_trailing_slash_is_tolerated(self):
    assert resolve_resource("https://api.test.example/v1/mcp/") == agnostic_target()

  def test_roboledger_url(self):
    target = resolve_resource("https://api.test.example/v1/mcp/roboledger")
    assert target == roboledger_target()
    assert target.resource == "https://api.test.example/v1/mcp/roboledger"
    assert target.product == "roboledger"
    # Graph-agnostic like /v1/mcp — the grant names the graph — but a
    # distinct audience: the two targets must never compare equal.
    assert target.is_agnostic
    assert target != agnostic_target()

  def test_roboledger_trailing_slash_is_tolerated(self):
    assert (
      resolve_resource("https://api.test.example/v1/mcp/roboledger/")
      == roboledger_target()
    )

  def test_route_target_picks_the_route_resource(self):
    assert route_target() == agnostic_target()
    assert route_target(product="roboledger") == roboledger_target()
    assert route_target(KG) == graph_target(KG)
    # A URL-named graph wins: the per-graph route never carries a product.
    assert route_target(KG, product="roboledger") == graph_target(KG)

  def test_per_graph_url(self):
    target = resolve_resource(f"https://api.test.example/v1/graphs/{KG}/mcp")
    assert target == graph_target(KG)
    assert target.graph_id == KG
    assert not target.is_agnostic

  @pytest.mark.parametrize(
    "value",
    [
      "https://evil.example/v1/mcp",
      "http://api.test.example/v1/mcp",
      "https://api.test.example/v1/mcp?x=1",
      "https://api.test.example/v1/mcp#frag",
      "https://api.test.example/v1/mcp/roboinvestor",
      "https://api.test.example/v1/mcp/roboledger/extra",
      "https://api.test.example/v1/mcp/roboledger?x=1",
      "https://api.test.example/v1/graphs",
      f"https://api.test.example/v1/graphs/{KG}/query",
      "https://api.test.example/v1/graphs/../mcp",
      "https://api.test.example/v1/graphs/not a graph/mcp",
      "not-a-url",
      "",
    ],
  )
  def test_everything_else_is_rejected(self, value):
    # "" resolves to agnostic (absent); the rest must be None.
    if value == "":
      assert resolve_resource(value) == agnostic_target()
    else:
      assert resolve_resource(value) is None

  def test_host_comparison_is_case_insensitive(self):
    assert resolve_resource("https://API.TEST.example/v1/mcp") == agnostic_target()


class TestNormalizeScope:
  def test_absent_defaults_to_mcp(self):
    assert normalize_scope(None) == "mcp"
    assert normalize_scope("   ") == "mcp"

  def test_supported_scopes_are_ordered_and_include_mcp(self):
    assert normalize_scope("offline_access") == "mcp offline_access"
    assert normalize_scope("offline_access mcp") == "mcp offline_access"

  def test_unknown_scope_is_rejected(self):
    assert normalize_scope("mcp admin") is None


@pytest.mark.usefixtures("oauth_env")
class TestDiscoveryDocuments:
  def test_prm_url_is_path_suffixed(self):
    assert (
      prm_url(agnostic_target())
      == "https://api.test.example/.well-known/oauth-protected-resource/v1/mcp"
    )
    assert prm_url(graph_target(KG)).endswith(f"/v1/graphs/{KG}/mcp")

  def test_protected_resource_metadata(self):
    doc = protected_resource_metadata(graph_target(KG))
    assert doc["resource"] == f"https://api.test.example/v1/graphs/{KG}/mcp"
    assert doc["authorization_servers"] == ["https://api.test.example"]
    assert doc["bearer_methods_supported"] == ["header"]
    assert "mcp" in doc["scopes_supported"]

  def test_roboledger_discovery(self):
    assert prm_url(roboledger_target()) == (
      "https://api.test.example/.well-known/oauth-protected-resource/v1/mcp/roboledger"
    )
    doc = protected_resource_metadata(roboledger_target())
    assert doc["resource"] == "https://api.test.example/v1/mcp/roboledger"
    assert doc["resource_name"] == "RoboLedger MCP"
    assert protected_resource_metadata(agnostic_target())["resource_name"] == (
      "RoboSystems MCP"
    )
    assert bearer_challenge(roboledger_target()).startswith(
      'Bearer resource_metadata="https://api.test.example/.well-known/'
      'oauth-protected-resource/v1/mcp/roboledger"'
    )

  def test_authorization_server_metadata_flags(self):
    doc = authorization_server_metadata()
    assert doc["issuer"] == "https://api.test.example"
    assert doc["authorization_endpoint"].endswith("/v1/oauth/authorize")
    assert doc["token_endpoint"].endswith("/v1/oauth/token")
    assert doc["registration_endpoint"].endswith("/v1/oauth/register")
    assert doc["code_challenge_methods_supported"] == ["S256"]
    assert "none" in doc["token_endpoint_auth_methods_supported"]
    assert doc["grant_types_supported"] == ["authorization_code", "refresh_token"]
    assert doc["authorization_response_iss_parameter_supported"] is True
    assert "offline_access" in doc["scopes_supported"]

  def test_bearer_challenge_names_resource_metadata(self):
    value = bearer_challenge(agnostic_target())
    assert value.startswith("Bearer ")
    assert 'resource_metadata="https://api.test.example/.well-known/' in value
    assert 'scope="mcp"' in value

  def test_bearer_challenge_with_error(self):
    value = bearer_challenge(
      graph_target(KG), error="invalid_token", error_description='bad "quote"'
    )
    assert value.startswith('Bearer error="invalid_token", ')
    assert "error_description=\"bad 'quote'\"" in value
