"""MercuryClient over a mock transport, and the refresh seam."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import httpx
import pytest

from robosystems.adapters.mercury.client.api import (
  ConnectionTokenSource,
  MercuryAuthError,
  MercuryClient,
  StaticToken,
  refresh_access_token,
)

BASE = "https://api-sandbox.mercury.com/api/v1"


class _Provider:
  token_url = "https://oauth2-sandbox.mercury.com/oauth2/token"
  client_id = "cid"
  client_secret = "secret"


def _client(handler, tokens=None, **kwargs) -> MercuryClient:
  transport = httpx.MockTransport(handler)
  http = httpx.Client(transport=transport)
  return MercuryClient(
    BASE, tokens or StaticToken("tok"), http=http, sleep=lambda _s: None, **kwargs
  )


@pytest.mark.unit
class TestMercuryClient:
  def test_static_token_rejects_empty(self):
    with pytest.raises(ValueError):
      StaticToken("")

  def test_accounts_sends_bearer(self):
    seen = {}

    def handler(request: httpx.Request):
      seen["auth"] = request.headers["Authorization"]
      seen["url"] = str(request.url)
      return httpx.Response(200, json={"accounts": [{"id": "a1"}]})

    assert _client(handler).accounts() == [{"id": "a1"}]
    assert seen["auth"] == "Bearer tok"
    assert seen["url"] == f"{BASE}/accounts"

  def test_credit_403_is_no_card(self):
    def handler(request):
      return httpx.Response(403, json={"error": "forbidden"})

    assert _client(handler).credit_accounts() == []

  def test_credit_500_raises_after_retries(self):
    calls = {"n": 0}

    def handler(request):
      calls["n"] += 1
      return httpx.Response(500, json={"error": "boom"})

    with pytest.raises(httpx.HTTPStatusError):
      _client(handler).credit_accounts()
    assert calls["n"] == 4

  def test_transactions_paginate_by_start_after(self):
    pages = {"n": 0}
    params_seen = []

    def handler(request):
      pages["n"] += 1
      params_seen.append(dict(request.url.params))
      if pages["n"] == 1:
        rows = [{"id": f"t{i}"} for i in range(500)]
      else:
        rows = [{"id": "t500"}]
      return httpx.Response(200, json={"transactions": rows})

    from datetime import date

    rows = _client(handler).transactions(date(2026, 1, 1), date(2026, 3, 31))
    assert len(rows) == 501
    assert params_seen[0]["start"] == "2026-01-01"
    assert params_seen[0]["end"] == "2026-03-31"
    assert params_seen[0]["order"] == "asc"
    assert "start_after" not in params_seen[0]
    assert params_seen[1]["start_after"] == "t499"

  def test_429_honours_retry_after(self):
    calls = {"n": 0}
    slept = []

    def handler(request):
      calls["n"] += 1
      if calls["n"] == 1:
        return httpx.Response(429, headers={"Retry-After": "3"}, json={})
      return httpx.Response(200, json={"categories": [{"id": "c"}]})

    transport = httpx.MockTransport(handler)
    client = MercuryClient(
      BASE,
      StaticToken("tok"),
      http=httpx.Client(transport=transport),
      sleep=slept.append,
    )
    assert client.categories() == [{"id": "c"}]
    assert slept == [3.0]

  def test_401_refreshes_once_then_retries(self):
    calls = {"n": 0}
    tokens = MagicMock()
    tokens.token.side_effect = ["stale", "fresh", "fresh"]

    def handler(request):
      calls["n"] += 1
      if request.headers["Authorization"] == "Bearer stale":
        return httpx.Response(401, json={"error": "unauthorized"})
      return httpx.Response(200, json={"accounts": []})

    assert _client(handler, tokens=tokens).accounts() == []
    tokens.invalidate.assert_called_once()
    assert calls["n"] == 2

  def test_401_twice_is_a_recoverable_auth_error(self):
    def handler(request):
      return httpx.Response(401, json={"error": "unauthorized"})

    with pytest.raises(MercuryAuthError) as excinfo:
      _client(handler).accounts()
    assert excinfo.value.recoverable is True


@pytest.mark.unit
class TestRefreshAccessToken:
  def _http(self, status: int, body: dict):
    seen = {}

    def handler(request: httpx.Request):
      seen["form"] = dict(httpx.QueryParams(request.content.decode()))
      seen["auth"] = request.headers.get("Authorization")
      return httpx.Response(status, json=body)

    return httpx.Client(transport=httpx.MockTransport(handler)), seen

  def test_refresh_resends_scope_and_rotates(self):
    http, seen = self._http(
      200,
      {
        "access_token": "new",
        "refresh_token": "rot",
        "expires_in": 3600,
        "scope": "read offline_access",
      },
    )
    fresh = refresh_access_token(_Provider(), "old", "read offline_access", http=http)
    assert seen["form"]["grant_type"] == "refresh_token"
    assert seen["form"]["scope"] == "read offline_access"
    assert seen["form"]["refresh_token"] == "old"
    assert seen["auth"].startswith("Basic ")
    assert fresh["access_token"] == "new" and fresh["refresh_token"] == "rot"
    assert fresh["expires_at"] > datetime.now(UTC) + timedelta(minutes=55)

  def test_refresh_keeps_old_refresh_token_when_not_rotated(self):
    http, _ = self._http(200, {"access_token": "new"})
    fresh = refresh_access_token(_Provider(), "old", http=http)
    assert fresh["refresh_token"] == "old"
    assert fresh["scope"] == "read offline_access"

  @pytest.mark.parametrize("code", ["invalid_grant", "invalid_scope", "invalid_client"])
  def test_grant_gone_is_not_recoverable(self, code):
    http, _ = self._http(400, {"error": code, "error_description": "gone"})
    with pytest.raises(MercuryAuthError) as excinfo:
      refresh_access_token(_Provider(), "old", http=http)
    assert excinfo.value.recoverable is False
    assert excinfo.value.code == code

  def test_server_error_is_recoverable(self):
    http, _ = self._http(503, {"error": "temporarily_unavailable"})
    with pytest.raises(MercuryAuthError) as excinfo:
      refresh_access_token(_Provider(), "old", http=http)
    assert excinfo.value.recoverable is True

  def test_network_error_is_recoverable(self):
    def handler(request):
      raise httpx.ConnectError("down")

    http = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(MercuryAuthError) as excinfo:
      refresh_access_token(_Provider(), "old", http=http)
    assert excinfo.value.recoverable is True


@pytest.mark.unit
class TestConnectionTokenSource:
  def _source(self, creds: dict, **kwargs) -> ConnectionTokenSource:
    source = ConnectionTokenSource("conn_1", provider=_Provider(), **kwargs)
    source._credentials = dict(creds)
    return source

  def test_api_key_mode_returns_the_key_and_never_refreshes(self):
    source = self._source({"auth_mode": "api_key", "api_key": "pk"})
    assert source.token() == "pk"
    source.invalidate()
    assert source.token() == "pk"

  def test_api_key_mode_without_a_key_is_fatal(self):
    with pytest.raises(MercuryAuthError) as excinfo:
      self._source({"auth_mode": "api_key"}).token()
    assert excinfo.value.recoverable is False

  def test_fresh_access_token_is_used_as_is(self):
    creds = {
      "auth_mode": "oauth",
      "access_token": "acc",
      "refresh_token": "ref",
      "expires_at": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
    }
    assert self._source(creds).token() == "acc"

  def test_expiring_token_refreshes_and_persists(self):
    creds = {
      "auth_mode": "oauth",
      "access_token": "acc",
      "refresh_token": "ref",
      "expires_at": (datetime.now(UTC) + timedelta(seconds=10)).isoformat(),
      "sync_config": {"since_date": "2026-01-01"},
    }
    source = self._source(creds)
    fresh = {
      "access_token": "acc2",
      "refresh_token": "ref2",
      "expires_at": datetime.now(UTC) + timedelta(hours=1),
      "scope": "read offline_access",
    }
    with (
      patch(
        "robosystems.adapters.mercury.client.api.refresh_access_token",
        return_value=fresh,
      ) as refresh,
      patch.object(source, "_persist") as persist,
    ):
      assert source.token() == "acc2"
    refresh.assert_called_once()
    merged = persist.call_args.args[0]
    assert merged["refresh_token"] == "ref2"
    assert merged["sync_config"] == {"since_date": "2026-01-01"}  # bundle keys survive
    assert source.credentials()["access_token"] == "acc2"

  def test_refused_refresh_marks_needs_reauth(self):
    creds = {"auth_mode": "oauth", "access_token": "acc", "refresh_token": "ref"}
    source = self._source(creds)
    with (
      patch(
        "robosystems.adapters.mercury.client.api.refresh_access_token",
        side_effect=MercuryAuthError("gone", recoverable=False, code="invalid_grant"),
      ),
      patch.object(source, "_mark_needs_reauth") as mark,
    ):
      with pytest.raises(MercuryAuthError):
        source.token()
    mark.assert_called_once()

  def test_transient_refresh_failure_does_not_mark(self):
    creds = {"auth_mode": "oauth", "access_token": "acc", "refresh_token": "ref"}
    source = self._source(creds)
    with (
      patch(
        "robosystems.adapters.mercury.client.api.refresh_access_token",
        side_effect=MercuryAuthError("blip", recoverable=True),
      ),
      patch.object(source, "_mark_needs_reauth") as mark,
    ):
      with pytest.raises(MercuryAuthError):
        source.token()
    mark.assert_not_called()

  def test_missing_refresh_token_is_fatal_and_marks(self):
    source = self._source({"auth_mode": "oauth", "access_token": "acc"})
    with patch.object(source, "_mark_needs_reauth") as mark:
      with pytest.raises(MercuryAuthError) as excinfo:
        source.token()
    assert excinfo.value.recoverable is False
    mark.assert_called_once()

  def test_expires_within_treats_missing_or_bad_stamp_as_expired(self):
    assert ConnectionTokenSource._expires_within({}, 60) is True
    assert ConnectionTokenSource._expires_within({"expires_at": "junk"}, 60) is True
    later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    assert ConnectionTokenSource._expires_within({"expires_at": later}, 60) is False
    naive = (datetime.now(UTC) + timedelta(hours=1)).replace(tzinfo=None).isoformat()
    assert ConnectionTokenSource._expires_within({"expires_at": naive}, 60) is False


def test_error_detail_helper_reads_json_and_text():
  from robosystems.adapters.mercury.client.api import _error_detail

  json_resp = httpx.Response(
    400, content=json.dumps({"error": "x", "error_description": "why"})
  )
  assert _error_detail(json_resp) == ("x", "why")
  text_resp = httpx.Response(502, content=b"<html>bad gateway</html>")
  assert _error_detail(text_resp) == (None, "<html>bad gateway</html>")
