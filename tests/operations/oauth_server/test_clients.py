"""Redirect matching, RFC 7591 validation, client resolution."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from robosystems.models.core import OAuthClient
from robosystems.operations.oauth_server import clients
from robosystems.operations.oauth_server.clients import (
  ClientError,
  authenticate_client,
  pick_redirect_uri,
  redirect_uri_matches,
  register_dynamic_client,
  resolve_client,
  validate_redirect_uri,
  validate_registration_metadata,
)

VSCODE_BODY = {
  "client_name": "Visual Studio Code",
  "client_uri": "https://code.visualstudio.com",
  "response_types": ["code"],
  "redirect_uris": [
    "https://insiders.vscode.dev/redirect",
    "https://vscode.dev/redirect",
    "http://127.0.0.1/",
    "http://127.0.0.1:33418/",
  ],
  "token_endpoint_auth_method": "none",
  "application_type": "native",
  "grant_types": ["authorization_code", "refresh_token", "device_code"],
}

CURSOR_BODY = {
  "client_name": "Cursor",
  "redirect_uris": [
    "http://localhost:8787/callback",
    "cursor://anysphere.cursor-mcp/oauth/callback",
    "https://www.cursor.com/agents/mcp/oauth/callback",
  ],
  "token_endpoint_auth_method": "none",
}


class TestRedirectMatching:
  def test_exact_match(self):
    assert redirect_uri_matches("https://a.example/cb", "https://a.example/cb")
    assert not redirect_uri_matches("https://a.example/cb", "https://a.example/cb2")
    assert not redirect_uri_matches("https://a.example/cb", "https://a.example:8443/cb")

  def test_loopback_ignores_port(self):
    assert redirect_uri_matches("http://127.0.0.1/", "http://127.0.0.1:33418/")
    assert redirect_uri_matches("http://127.0.0.1:33418/", "http://127.0.0.1:51234/")
    assert redirect_uri_matches(
      "http://localhost/callback", "http://localhost:8787/callback"
    )

  def test_loopback_hosts_are_not_interchangeable(self):
    assert not redirect_uri_matches(
      "http://localhost/callback", "http://127.0.0.1/callback"
    )

  def test_loopback_path_and_scheme_still_matter(self):
    assert not redirect_uri_matches("http://127.0.0.1/", "http://127.0.0.1:33418/other")
    assert not redirect_uri_matches("http://127.0.0.1/", "https://127.0.0.1:33418/")

  def test_custom_scheme_is_exact(self):
    uri = "cursor://anysphere.cursor-mcp/oauth/callback"
    assert redirect_uri_matches(uri, uri)
    assert not redirect_uri_matches(uri, uri + "?x")


class TestValidateRedirectUri:
  @pytest.mark.parametrize(
    "uri",
    [
      "https://claude.ai/api/mcp/auth_callback",
      "http://localhost/callback",
      "http://127.0.0.1:33418/",
      "http://[::1]:9/cb",
      "cursor://anysphere.cursor-mcp/oauth/callback",
      "vscode://ms-vscode.mcp/redirect",
    ],
  )
  def test_acceptable(self, uri):
    assert validate_redirect_uri(uri) is None

  @pytest.mark.parametrize(
    "uri",
    [
      "http://evil.example/callback",
      "https://a.example/cb#frag",
      "javascript:alert(1)",
      "data:text/html,hi",
      "/relative",
      "https://a.example/cb with space",
      "",
      None,
      "custom:",
    ],
  )
  def test_rejected(self, uri):
    assert validate_redirect_uri(uri) is not None


class TestValidateRegistrationMetadata:
  def test_vscode_body(self):
    meta = validate_registration_metadata(VSCODE_BODY)
    assert meta.client_name == "Visual Studio Code"
    assert meta.token_endpoint_auth_method == "none"
    assert "http://127.0.0.1/" in meta.redirect_uris
    assert meta.client_uri == "https://code.visualstudio.com"

  def test_cursor_body(self):
    meta = validate_registration_metadata(CURSOR_BODY)
    assert len(meta.redirect_uris) == 3

  def test_missing_redirect_uris(self):
    with pytest.raises(ClientError) as exc:
      validate_registration_metadata({"client_name": "x"})
    assert exc.value.error == "invalid_redirect_uri"

  def test_too_many_redirect_uris(self):
    body = {"redirect_uris": [f"https://a.example/{i}" for i in range(11)]}
    with pytest.raises(ClientError):
      validate_registration_metadata(body)

  def test_bad_redirect_uri(self):
    with pytest.raises(ClientError) as exc:
      validate_registration_metadata({"redirect_uris": ["http://evil.example/cb"]})
    assert exc.value.error == "invalid_redirect_uri"

  @pytest.mark.parametrize(
    "uri",
    [
      # urlsplit takes the host after the "@" (localhost / claude.ai); a
      # browser ends the authority at the backslash and delivers the code
      # to attacker.example.
      "http://attacker.example\\@localhost/cb",
      "https://attacker.example\\@claude.ai/cb",
      "http://user@localhost/cb",
      "https://@attacker.example/cb",
      "http://localhost:abc/cb",
      "http://127.0.0.1/cb\\x",
      "http://localhost/cb\x01",
      "http://localhost/cb x",
      "https://\u0441laude.ai/cb",
    ],
  )
  def test_ambiguous_redirect_uri_is_refused(self, uri):
    assert validate_redirect_uri(uri) is not None
    with pytest.raises(ClientError) as exc:
      validate_registration_metadata({"redirect_uris": [uri]})
    assert exc.value.error == "invalid_redirect_uri"

  @pytest.mark.parametrize(
    "uri",
    [
      "http://localhost/callback",
      "http://127.0.0.1/callback",
      "http://[::1]:8080/cb",
      "https://claude.ai/api/mcp/auth_callback",
      "https://mcp.docker.com/oauth/callback",
      "cursor://anysphere.cursor-mcp/oauth/callback",
      "https://a.example/cb?x=1&y=%20",
    ],
  )
  def test_canonical_redirect_uri_is_accepted(self, uri):
    assert validate_redirect_uri(uri) is None

  def test_unknown_auth_method(self):
    body = {**CURSOR_BODY, "token_endpoint_auth_method": "private_key_jwt"}
    with pytest.raises(ClientError) as exc:
      validate_registration_metadata(body)
    assert exc.value.error == "invalid_client_metadata"

  def test_grant_types_must_include_authorization_code(self):
    body = {**CURSOR_BODY, "grant_types": ["client_credentials"]}
    with pytest.raises(ClientError):
      validate_registration_metadata(body)

  def test_client_name_defaults_to_redirect_host(self):
    meta = validate_registration_metadata({"redirect_uris": ["https://ide.example/cb"]})
    assert meta.client_name == "ide.example"

  def test_dedupes_redirect_uris_and_truncates_name(self):
    body = {
      "client_name": "x" * 500,
      "redirect_uris": ["https://a.example/cb", "https://a.example/cb"],
    }
    meta = validate_registration_metadata(body)
    assert meta.redirect_uris == ["https://a.example/cb"]
    assert len(meta.client_name) == 100

  def test_non_object_body(self):
    with pytest.raises(ClientError):
      validate_registration_metadata(["nope"])


class TestRegisterAndResolve:
  def test_public_registration_has_no_secret(self, test_db):
    client, secret = register_dynamic_client(
      VSCODE_BODY, registration_ip="203.0.113.5", session=test_db
    )
    assert secret is None
    assert not client.is_confidential
    assert client.client_id.startswith("rfsoc_")
    assert client.registration_source == "dcr"
    assert client.expires_at is not None
    assert client.is_usable

  def test_confidential_registration_returns_secret_once(self, test_db):
    body = {**CURSOR_BODY, "token_endpoint_auth_method": "client_secret_post"}
    client, secret = register_dynamic_client(
      body, registration_ip=None, session=test_db
    )
    assert secret is not None and secret.startswith("rfsos")
    assert client.verify_secret(secret)
    assert not client.verify_secret(secret + "x")
    assert not client.verify_secret(None)
    authenticate_client(client, secret)
    with pytest.raises(ClientError) as exc:
      authenticate_client(client, "wrong")
    assert exc.value.error == "invalid_client"

  def test_per_ip_daily_cap(self, test_db):
    with patch.object(clients, "DCR_PER_IP_DAILY_CAP", 1):
      register_dynamic_client(
        CURSOR_BODY, registration_ip="198.51.100.9", session=test_db
      )
      with pytest.raises(ClientError) as exc:
        register_dynamic_client(
          CURSOR_BODY, registration_ip="198.51.100.9", session=test_db
        )
    assert "limit" in exc.value.description

  def test_resolve_unknown_and_expired(self, test_db):
    with pytest.raises(ClientError):
      resolve_client("rfsoc_nope", test_db)
    with pytest.raises(ClientError):
      resolve_client(None, test_db)
    client, _ = register_dynamic_client(
      CURSOR_BODY, registration_ip=None, session=test_db
    )
    assert resolve_client(client.client_id, test_db).id == client.id
    client.expires_at = datetime.now(UTC) - timedelta(minutes=1)
    test_db.commit()
    with pytest.raises(ClientError):
      resolve_client(client.client_id, test_db)
    client.mark_used(test_db)
    assert client.expires_at is None
    assert resolve_client(client.client_id, test_db).id == client.id

  def test_preregistered_is_trusted_and_permanent(self, test_db):
    client, secret = OAuthClient.register_preregistered(
      client_name="Anthropic held creds",
      redirect_uris=["https://claude.ai/api/mcp/auth_callback"],
      confidential=True,
      session=test_db,
    )
    assert client.is_trusted
    assert client.expires_at is None
    assert client.registration_source == "preregistered"
    assert secret and client.verify_secret(secret)

  def test_pick_redirect_uri(self, test_db):
    client, _ = register_dynamic_client(
      VSCODE_BODY, registration_ip=None, session=test_db
    )
    assert (
      pick_redirect_uri(client, "http://127.0.0.1:51000/") == "http://127.0.0.1:51000/"
    )
    with pytest.raises(ClientError) as exc:
      pick_redirect_uri(client, "https://evil.example/redirect")
    assert exc.value.error == "invalid_request"
    # Several registered → redirect_uri is required.
    with pytest.raises(ClientError):
      pick_redirect_uri(client, None)
    single, _ = OAuthClient.register_preregistered(
      client_name="one",
      redirect_uris=["https://one.example/cb"],
      confidential=False,
      session=test_db,
    )
    assert pick_redirect_uri(single, None) == "https://one.example/cb"

  def test_pick_redirect_uri_revalidates_registered_uris(self, test_db):
    client, _ = register_dynamic_client(
      CURSOR_BODY, registration_ip=None, session=test_db
    )
    # A row written under an earlier, looser rule: refused at use, not
    # only at registration.
    spoofed = "http://attacker.example\\@localhost/cb"
    client.redirect_uris = [*client.redirect_uris, spoofed]
    test_db.commit()
    with pytest.raises(ClientError) as exc:
      pick_redirect_uri(client, spoofed)
    assert exc.value.error == "invalid_request"
    assert (
      pick_redirect_uri(client, "http://localhost:9999/callback")
      == "http://localhost:9999/callback"
    )

  def test_deactivated_client_is_unusable(self, test_db):
    client, _ = register_dynamic_client(
      CURSOR_BODY, registration_ip=None, session=test_db
    )
    client.deactivate(test_db)
    with pytest.raises(ClientError):
      resolve_client(client.client_id, test_db)
