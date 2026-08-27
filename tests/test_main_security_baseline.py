"""Regression tests for the externally verified security baseline.

The July 2026 external assessment verified several controls live and
recommended pinning them against silent regression (they are
deployment-time properties): the CORS allowlist's resistance to origin
bypass variants, the response header baseline, authentication
enforcement on protected endpoints, and (added in remediation) the
RFC 9116 disclosure file. These tests are that pin.

The CORS matrix mirrors the assessment's methodology: only the exact
configured origin may be echoed back; every mutation of it must be
refused (no access-control-allow-origin header), because responses
carry access-control-allow-credentials and a permissive allowlist
would otherwise be directly exploitable.
"""

from __future__ import annotations

import pytest

from robosystems.config import env

pytestmark = pytest.mark.unit

# First dev-environment CORS origin (tests run with the development origin
# list; the prod/staging lists are pinned in tests/config/test_env.py).
ALLOWED_ORIGIN = "http://localhost:3000"

BYPASS_ORIGINS = [
  "http://evil.localhost:3000",  # subdomain-prepended
  "http://localhost:3000.evil.com",  # suffix-appended
  "http://evillocalhost:3000",  # prefix-glued
  "http://xlocalhost:3000x",  # real origin as a substring
  "https://localhost:3000",  # scheme swap
  "http://localhost:9999",  # different port
  "http://LOCALHOST:3000",  # case variant
  "null",  # null origin (sandboxed iframe / file://)
]


class TestCorsOriginAllowlist:
  def test_exact_origin_allowed_with_credentials(self, client):
    response = client.get("/", headers={"Origin": ALLOWED_ORIGIN})
    assert response.headers.get("access-control-allow-origin") == ALLOWED_ORIGIN
    assert response.headers.get("access-control-allow-credentials") == "true"

  @pytest.mark.parametrize("origin", BYPASS_ORIGINS)
  def test_bypass_variants_refused(self, client, origin):
    response = client.get("/", headers={"Origin": origin})
    assert "access-control-allow-origin" not in response.headers

  def test_preflight_refused_for_disallowed_origin(self, client):
    response = client.options(
      "/v1/user",
      headers={
        "Origin": "http://evil.localhost:3000",
        "Access-Control-Request-Method": "GET",
      },
    )
    assert "access-control-allow-origin" not in response.headers

  def test_preflight_allowed_for_exact_origin(self, client):
    response = client.options(
      "/v1/user",
      headers={
        "Origin": ALLOWED_ORIGIN,
        "Access-Control-Request-Method": "GET",
      },
    )
    assert response.headers.get("access-control-allow-origin") == ALLOWED_ORIGIN


class TestResponseHeaderBaseline:
  def test_baseline_headers_present(self, client):
    response = client.get("/")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"
    assert "geolocation=()" in response.headers["permissions-policy"]
    assert "content-security-policy" in response.headers

  def test_headers_present_on_error_responses(self, client):
    """The middleware wraps every response, including 404s."""
    response = client.get("/v1/nonexistent-path")
    assert response.status_code == 404
    assert response.headers["x-content-type-options"] == "nosniff"
    csp = response.headers["content-security-policy"]
    assert "script-src 'self'" in csp
    assert "object-src 'none'" in csp

  def test_hsts_absent_in_development(self, client):
    response = client.get("/")
    assert "strict-transport-security" not in response.headers

  def test_hsts_emitted_outside_development(self, client, monkeypatch):
    monkeypatch.setattr(env, "ENVIRONMENT", "prod", raising=False)
    response = client.get("/")
    assert response.headers["strict-transport-security"] == (
      "max-age=31536000; includeSubDomains"
    )

  def test_graphql_path_strict_outside_development(self, client, monkeypatch):
    """The GraphiQL playground is dev-only, so its relaxed CSP must be too.

    Outside development the graph-scoped GraphQL path serves no page —
    only JSON refusals — and nothing on it needs CDN, inline, or eval'd
    script. The header on those responses must be the strict API policy;
    the playground's policy carries exactly the directives the external
    assessment flagged and would read as an unremediated finding.
    """
    monkeypatch.setattr(env, "is_development", lambda: False)
    response = client.get("/extensions/kg0123456789abcdef0000/graphql")
    csp = response.headers["content-security-policy"]
    assert "'unsafe-inline'" not in csp
    assert "'unsafe-eval'" not in csp
    assert "https://" not in csp
    assert "script-src 'self';" in csp

  def test_graphql_path_relaxed_in_development(self, client, monkeypatch):
    """The playground keeps its CDN policy where it is actually served."""
    monkeypatch.setattr(env, "is_development", lambda: True)
    response = client.get("/extensions/kg0123456789abcdef0000/graphql")
    csp = response.headers["content-security-policy"]
    assert "'unsafe-eval'" in csp


class TestAuthenticationEnforcement:
  """Protected endpoints refuse unauthenticated requests with a clean 401."""

  def test_unauthenticated_request_refused(self, client):
    response = client.get("/v1/user")
    assert response.status_code == 401

  def test_invalid_api_key_refused(self, client):
    response = client.get("/v1/user", headers={"X-API-Key": "not-a-real-key"})
    assert response.status_code == 401


class TestSecurityTxt:
  def test_security_txt_served(self, client):
    response = client.get("/.well-known/security.txt")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "Contact: mailto:security@robosystems.ai" in response.text
    assert "Expires:" in response.text
    assert "Canonical: https://api.robosystems.ai/.well-known/security.txt" in (
      response.text
    )

  def test_invented_well_known_sibling_is_404(self, client):
    """Control path: proves security.txt is a genuine route, not a catch-all."""
    response = client.get("/.well-known/does-not-exist.txt")
    assert response.status_code == 404


class TestGlamaClaim:
  def test_glama_json_served(self, client):
    """Glama verifies connector ownership from this document on the MCP
    endpoint's origin; the address is a role mailbox that matches the Glama
    account, never a person."""
    response = client.get("/.well-known/glama.json")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    body = response.json()
    assert body["$schema"] == "https://glama.ai/mcp/schemas/connector.json"
    assert body["maintainers"] == [{"email": "admin@robosystems.ai"}]


class TestOpenAIAppsChallenge:
  def test_challenge_token_served_as_plain_text(self, client):
    """The ChatGPT app-directory portal verifies the MCP origin by fetching
    exactly its issued token, as plain text, from this well-known URL."""
    from pathlib import Path

    expected = Path("static/openai-apps-challenge").read_text(encoding="utf-8").strip()
    assert expected and "\n" not in expected
    response = client.get("/.well-known/openai-apps-challenge")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert response.text == expected
