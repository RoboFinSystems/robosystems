"""Client ID Metadata Documents: the fetch fence, the cache, the mirror."""

import json
import socket
from unittest.mock import patch

import httpx
import pytest

from robosystems.models.core import OAuthClient
from robosystems.operations.oauth_server import cimd
from robosystems.operations.oauth_server.cimd import (
  fetch_client_metadata,
  get_client_metadata,
  is_cimd_client_id,
)
from robosystems.operations.oauth_server.clients import ClientError, resolve_client

VSCODE_ID = "https://vscode.dev/oauth/client-metadata.json"
VSCODE_DOC = {
  "client_id": VSCODE_ID,
  "client_name": "Visual Studio Code",
  "client_uri": "https://code.visualstudio.com",
  "redirect_uris": ["http://127.0.0.1:33418/", "https://vscode.dev/redirect"],
  "grant_types": ["authorization_code", "refresh_token", "device_code"],
  "response_types": ["code"],
  "token_endpoint_auth_method": "none",
  "application_type": "native",
}
UNKNOWN_ID = "https://ide.example/oauth/client.json"


class FakeRedis:
  def __init__(self):
    self.store = {}

  def setex(self, key, ttl, value):
    self.store[key] = (ttl, value)

  def get(self, key):
    entry = self.store.get(key)
    return entry[1] if entry else None


@pytest.fixture
def cimd_redis():
  redis = FakeRedis()
  with patch.object(cimd, "create_redis_client", return_value=redis):
    yield redis


@pytest.fixture
def public_dns():
  with patch.object(cimd, "_assert_public_host"):
    yield


def _transport(handler):
  return httpx.MockTransport(handler)


def _doc_response(doc, status=200, headers=None):
  return httpx.Response(status, json=doc, headers=headers or {})


class TestShape:
  @pytest.mark.parametrize(
    "value, expected",
    [
      (VSCODE_ID, True),
      ("https://chatgpt.com/oauth/client.json", True),
      ("https://claude.ai/oauth/claude-code-client-metadata", True),
      ("rfsoc_abc", False),
      ("http://vscode.dev/oauth/client-metadata.json", False),
      ("https://vscode.dev/", False),
      ("https://vscode.dev", False),
      ("https://user@vscode.dev/x", False),
      ("https://vscode.dev/x?y=1", False),
      ("https://vscode.dev/x#f", False),
      (None, False),
    ],
  )
  def test_is_cimd_client_id(self, value, expected):
    assert is_cimd_client_id(value) is expected


class TestPublicHostFence:
  def test_address_literal_is_refused(self):
    with pytest.raises(ClientError):
      cimd._assert_public_host("127.0.0.1")
    with pytest.raises(ClientError):
      cimd._assert_public_host("[::1]")

  @pytest.mark.parametrize(
    "address", ["10.0.0.5", "127.0.0.1", "169.254.169.254", "192.168.1.1", "0.0.0.0"]
  )
  def test_non_public_resolution_is_refused(self, address):
    infos = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))]
    with patch.object(cimd.socket, "getaddrinfo", return_value=infos):
      with pytest.raises(ClientError) as exc:
        cimd._assert_public_host("evil.example")
    assert "publicly routable" in exc.value.description

  def test_public_resolution_passes(self):
    infos = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
    with patch.object(cimd.socket, "getaddrinfo", return_value=infos):
      cimd._assert_public_host("vscode.dev")

  def test_unresolvable_is_refused(self):
    with patch.object(cimd.socket, "getaddrinfo", side_effect=socket.gaierror):
      with pytest.raises(ClientError):
        cimd._assert_public_host("nope.invalid")


@pytest.mark.usefixtures("public_dns")
class TestFetch:
  def test_valid_document(self):
    seen = {}

    def handler(request):
      seen["url"] = str(request.url)
      seen["accept"] = request.headers.get("accept")
      return _doc_response(VSCODE_DOC, headers={"cache-control": "max-age=600"})

    doc, ttl = fetch_client_metadata(VSCODE_ID, transport=_transport(handler))
    assert doc["client_name"] == "Visual Studio Code"
    assert ttl == 600
    assert seen["url"] == VSCODE_ID
    assert seen["accept"] == "application/json"

  def test_client_id_must_match_url(self):
    doc = {**VSCODE_DOC, "client_id": "https://vscode.dev/other.json"}
    with pytest.raises(ClientError) as exc:
      fetch_client_metadata(
        VSCODE_ID, transport=_transport(lambda r: _doc_response(doc))
      )
    assert "does not match" in exc.value.description

  def test_redirects_are_not_followed(self):
    def handler(request):
      return httpx.Response(302, headers={"location": "https://vscode.dev/x"})

    with pytest.raises(ClientError):
      fetch_client_metadata(VSCODE_ID, transport=_transport(handler))

  def test_non_json_and_non_object(self):
    with pytest.raises(ClientError):
      fetch_client_metadata(
        VSCODE_ID, transport=_transport(lambda r: httpx.Response(200, text="<html>"))
      )
    with pytest.raises(ClientError):
      fetch_client_metadata(
        VSCODE_ID, transport=_transport(lambda r: httpx.Response(200, json=["x"]))
      )

  def test_oversized_document(self):
    big = {**VSCODE_DOC, "pad": "x" * (cimd.CIMD_MAX_BYTES + 10)}
    with pytest.raises(ClientError) as exc:
      fetch_client_metadata(
        VSCODE_ID, transport=_transport(lambda r: _doc_response(big))
      )
    assert "too large" in exc.value.description

  def test_bogus_content_length_is_ignored(self):
    """A non-numeric Content-Length is a malformed peer, not a 500: the
    streamed size cap still applies."""
    body = json.dumps(VSCODE_DOC).encode()

    def handler(request):
      return httpx.Response(
        200,
        content=body,
        headers={"content-length": "not-a-number", "content-type": "application/json"},
      )

    doc, _ = fetch_client_metadata(VSCODE_ID, transport=_transport(handler))
    assert doc["client_id"] == VSCODE_ID

  def test_network_failure(self):
    def handler(request):
      raise httpx.ConnectError("boom")

    with pytest.raises(ClientError):
      fetch_client_metadata(VSCODE_ID, transport=_transport(handler))

  @pytest.mark.parametrize(
    "header, ttl",
    [
      (None, cimd.CIMD_CACHE_DEFAULT_SECONDS),
      ("max-age=5", cimd.CIMD_CACHE_MIN_SECONDS),
      ("max-age=999999", cimd.CIMD_CACHE_MAX_SECONDS),
      ("no-store", cimd.CIMD_CACHE_MIN_SECONDS),
    ],
  )
  def test_cache_ttl_is_clamped(self, header, ttl):
    headers = {"cache-control": header} if header else {}
    _, got = fetch_client_metadata(
      VSCODE_ID,
      transport=_transport(lambda r: _doc_response(VSCODE_DOC, headers=headers)),
    )
    assert got == ttl

  def test_not_a_cimd_url(self):
    with pytest.raises(ClientError):
      fetch_client_metadata(
        "rfsoc_abc", transport=_transport(lambda r: _doc_response({}))
      )


@pytest.mark.usefixtures("public_dns")
class TestCache:
  def test_second_call_is_served_from_cache(self, cimd_redis):
    calls = []

    def handler(request):
      calls.append(1)
      return _doc_response(VSCODE_DOC, headers={"cache-control": "max-age=120"})

    transport = _transport(handler)
    assert get_client_metadata(VSCODE_ID, transport=transport)["client_name"]
    assert get_client_metadata(VSCODE_ID, transport=transport)["client_name"]
    assert len(calls) == 1
    ((ttl, _),) = cimd_redis.store.values()
    assert ttl == 120

  def test_cache_entry_for_another_id_is_ignored(self, cimd_redis):
    key = cimd._cache_key(VSCODE_ID)
    cimd_redis.setex(key, 60, json.dumps({**VSCODE_DOC, "client_id": "https://x/y"}))
    calls = []

    def handler(request):
      calls.append(1)
      return _doc_response(VSCODE_DOC)

    get_client_metadata(VSCODE_ID, transport=_transport(handler))
    assert len(calls) == 1


@pytest.mark.usefixtures("public_dns", "cimd_redis")
class TestResolveClient:
  def _serve(self, doc):
    return patch.object(
      cimd,
      "fetch_client_metadata",
      side_effect=lambda client_id, transport=None: (doc, 60),
    )

  def test_document_is_mirrored_into_a_trusted_row(self, test_db):
    with self._serve(VSCODE_DOC):
      client = resolve_client(VSCODE_ID, test_db)
    assert client.registration_source == "cimd"
    assert client.client_id == VSCODE_ID
    assert client.client_name == "Visual Studio Code"
    assert client.is_trusted
    assert client.expires_at is None
    assert not client.is_confidential
    assert client.token_endpoint_auth_method == "none"
    assert "http://127.0.0.1:33418/" in client.redirect_uris
    assert OAuthClient.get_by_client_id(VSCODE_ID, test_db).id == client.id

  def test_unknown_host_is_mirrored_untrusted(self, test_db):
    doc = {**VSCODE_DOC, "client_id": UNKNOWN_ID, "client_name": "Some IDE"}
    with self._serve(doc):
      client = resolve_client(UNKNOWN_ID, test_db)
    assert not client.is_trusted
    assert client.client_name == "Some IDE"

  def test_private_key_jwt_is_treated_as_public(self, test_db):
    doc = {
      **VSCODE_DOC,
      "client_id": "https://chatgpt.com/oauth/client.json",
      "token_endpoint_auth_method": "private_key_jwt",
    }
    with self._serve(doc):
      client = resolve_client("https://chatgpt.com/oauth/client.json", test_db)
    assert client.token_endpoint_auth_method == "none"
    assert client.is_trusted

  def test_refresh_updates_the_mirror(self, test_db, cimd_redis):
    with self._serve(VSCODE_DOC):
      first = resolve_client(VSCODE_ID, test_db)
    # The cached document expires; the next resolve sees the new one.
    cimd_redis.store.clear()
    changed = {
      **VSCODE_DOC,
      "client_name": "VS Code",
      "redirect_uris": ["https://vscode.dev/redirect"],
    }
    with self._serve(changed):
      second = resolve_client(VSCODE_ID, test_db)
    assert second.id == first.id
    assert second.client_name == "VS Code"
    assert second.redirect_uris == ["https://vscode.dev/redirect"]

  def test_invalid_document_redirects_are_refused(self, test_db):
    doc = {**VSCODE_DOC, "redirect_uris": ["http://evil.example/cb"]}
    with self._serve(doc):
      with pytest.raises(ClientError):
        resolve_client(VSCODE_ID, test_db)

  def test_token_path_serves_the_mirror_when_the_document_is_unreachable(
    self, test_db, cimd_redis
  ):
    """The metadata host going down must not take every refresh with it.
    The token and revocation endpoints only need the client's identity to
    match the grant, so the last validated document — the mirror — stands
    in; the authorization endpoint never takes that fallback, because a
    redirect URI has to come from a live document."""
    with self._serve(VSCODE_DOC):
      mirror = resolve_client(VSCODE_ID, test_db)
    # The cached document lapses; every resolve below has to fetch.
    cimd_redis.store.clear()
    unreachable = patch.object(
      cimd,
      "fetch_client_metadata",
      side_effect=ClientError("invalid_client", "document is not available"),
    )
    with unreachable, pytest.raises(ClientError):
      resolve_client(VSCODE_ID, test_db)
    with unreachable:
      served = resolve_client(VSCODE_ID, test_db, allow_stale_mirror=True)
    assert served.id == mirror.id
    # Never a deactivated mirror, and never a mirror that does not exist.
    mirror.deactivate(test_db)
    with unreachable, pytest.raises(ClientError):
      resolve_client(VSCODE_ID, test_db, allow_stale_mirror=True)
    with unreachable, pytest.raises(ClientError):
      resolve_client(UNKNOWN_ID, test_db, allow_stale_mirror=True)

  def test_first_contact_race_updates_the_winner(self, test_db, cimd_redis):
    """Two concurrent first resolutions of one document: the loser's INSERT
    hits the unique client_id and must fall through to an update of the
    winner's row rather than surface as a 500."""
    with self._serve(VSCODE_DOC):
      winner = resolve_client(VSCODE_ID, test_db)

    real_lookup = OAuthClient.get_by_client_id
    calls = {"n": 0}

    def racing_lookup(client_id, session):
      # The loser read "no row" before the winner committed.
      calls["n"] += 1
      return None if calls["n"] == 1 else real_lookup(client_id, session)

    with patch.object(OAuthClient, "get_by_client_id", side_effect=racing_lookup):
      loser = OAuthClient.upsert_cimd(
        client_id=VSCODE_ID,
        client_name="VS Code (raced)",
        redirect_uris=["https://vscode.dev/redirect"],
        session=test_db,
        is_trusted=True,
      )
    assert loser.id == winner.id
    assert loser.client_name == "VS Code (raced)"
    assert calls["n"] >= 2

  def test_deactivated_mirror_is_not_refetched(self, test_db):
    with self._serve(VSCODE_DOC):
      client = resolve_client(VSCODE_ID, test_db)
    client.deactivate(test_db)
    with patch.object(cimd, "fetch_client_metadata") as fetch:
      with pytest.raises(ClientError):
        resolve_client(VSCODE_ID, test_db)
    fetch.assert_not_called()
