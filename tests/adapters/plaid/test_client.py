"""PlaidClient over a mock transport: request shapes, the cursor loop, errors."""

from __future__ import annotations

import json

import httpx
import pytest

from robosystems.adapters.plaid.client.api import (
  ACCOUNT_FILTERS,
  MAX_DAYS_REQUESTED,
  PlaidClient,
  PlaidError,
  TransactionsSync,
)


def _client(handler, **kwargs) -> PlaidClient:
  return PlaidClient(
    client_id="cid",
    secret="sec",
    environment=kwargs.pop("environment", "sandbox"),
    http=httpx.Client(transport=httpx.MockTransport(handler)),
    sleep=lambda _s: None,
    **kwargs,
  )


def _error(code: str, status: int = 400) -> httpx.Response:
  return httpx.Response(
    status,
    json={
      "error_type": "ITEM_ERROR",
      "error_code": code,
      "error_message": f"{code} happened",
      "request_id": "req_1",
    },
  )


@pytest.mark.unit
class TestConstruction:
  def test_unknown_environment_is_refused(self):
    with pytest.raises(ValueError, match="Unknown Plaid environment"):
      PlaidClient(client_id="c", secret="s", environment="development")

  def test_missing_credentials_are_refused(self):
    with pytest.raises(PlaidError, match="not configured"):
      PlaidClient(client_id="", secret="s")


@pytest.mark.unit
class TestLink:
  def test_new_item_link_token_carries_products_filters_and_history(self):
    seen = {}

    def handler(request: httpx.Request):
      seen["url"] = str(request.url)
      seen["body"] = json.loads(request.content)
      return httpx.Response(200, json={"link_token": "link-1"})

    body = _client(handler).create_link_token(
      client_user_id="usr_1", days_requested=5000
    )
    assert body == {"link_token": "link-1"}
    assert seen["url"] == "https://sandbox.plaid.com/link/token/create"
    sent = seen["body"]
    assert (sent["client_id"], sent["secret"]) == ("cid", "sec")
    assert sent["user"] == {"client_user_id": "usr_1"}
    assert sent["products"] == ["transactions"]
    assert sent["account_filters"] == ACCOUNT_FILTERS
    assert sent["transactions"] == {"days_requested": MAX_DAYS_REQUESTED}
    assert "access_token" not in sent

  def test_update_mode_sends_the_item_and_no_products(self):
    seen = {}

    def handler(request: httpx.Request):
      seen["body"] = json.loads(request.content)
      return httpx.Response(200, json={"link_token": "link-2"})

    _client(handler).create_link_token(
      client_user_id="usr_1", access_token="access-1", days_requested=90
    )
    sent = seen["body"]
    assert sent["access_token"] == "access-1"
    assert sent["update"] == {"account_selection_enabled": True}
    assert "products" not in sent and "transactions" not in sent

  def test_production_host(self):
    seen = {}

    def handler(request: httpx.Request):
      seen["url"] = str(request.url)
      return httpx.Response(200, json={"access_token": "a", "item_id": "i"})

    _client(handler, environment="production").exchange_public_token("public-1")
    assert seen["url"] == "https://production.plaid.com/item/public_token/exchange"

  def test_sandbox_public_tokens_are_sandbox_only(self):
    client = _client(lambda r: httpx.Response(200, json={}), environment="production")
    with pytest.raises(PlaidError, match="only in the sandbox"):
      client.create_sandbox_public_token("ins_1")


@pytest.mark.unit
class TestTransactionsSync:
  def test_pages_until_has_more_is_false(self):
    cursors = []

    def handler(request: httpx.Request):
      body = json.loads(request.content)
      cursors.append(body.get("cursor"))
      if body.get("cursor") is None:
        return httpx.Response(
          200,
          json={
            "added": [{"transaction_id": "t1"}],
            "modified": [],
            "removed": [],
            "next_cursor": "c1",
            "has_more": True,
            "transactions_update_status": "HISTORICAL_UPDATE_COMPLETE",
          },
        )
      return httpx.Response(
        200,
        json={
          "added": [{"transaction_id": "t2"}],
          "modified": [{"transaction_id": "t0"}],
          "removed": [{"transaction_id": "tx"}],
          "next_cursor": "c2",
          "has_more": False,
          "transactions_update_status": "HISTORICAL_UPDATE_COMPLETE",
        },
      )

    result = _client(handler).sync_transactions("access-1", None)
    assert cursors == [None, "c1"]
    assert [t["transaction_id"] for t in result.added] == ["t1", "t2"]
    assert len(result.modified) == 1 and len(result.removed) == 1
    assert result.next_cursor == "c2"
    assert result.ready

  def test_mutation_during_pagination_restarts_from_the_original_cursor(self):
    calls = []

    def handler(request: httpx.Request):
      body = json.loads(request.content)
      calls.append(body.get("cursor"))
      if len(calls) == 2:
        return _error("TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION")
      has_more = body.get("cursor") == "start"
      return httpx.Response(
        200,
        json={
          "added": [{"transaction_id": f"t{len(calls)}"}],
          "modified": [],
          "removed": [],
          "next_cursor": "next" if has_more else "end",
          "has_more": has_more,
        },
      )

    result = _client(handler).sync_transactions("access-1", "start")
    assert calls == ["start", "next", "start", "next"]
    # The pages read before the mutation are discarded.
    assert [t["transaction_id"] for t in result.added] == ["t3", "t4"]

  def test_not_ready_is_reported(self):
    def handler(request):
      return httpx.Response(
        200,
        json={
          "added": [],
          "modified": [],
          "removed": [],
          "next_cursor": "",
          "has_more": False,
          "transactions_update_status": "NOT_READY",
        },
      )

    assert not _client(handler).sync_transactions("access-1", None).ready


@pytest.mark.unit
class TestErrors:
  def test_login_required_needs_reauth(self):
    client = _client(lambda r: _error("ITEM_LOGIN_REQUIRED"))
    with pytest.raises(PlaidError) as exc_info:
      client.get_accounts("access-1")
    error = exc_info.value
    assert error.code == "ITEM_LOGIN_REQUIRED"
    assert error.needs_reauth and not error.item_gone
    assert error.request_id == "req_1"

  def test_a_removed_item_is_gone(self):
    client = _client(lambda r: _error("ITEM_NOT_FOUND"))
    with pytest.raises(PlaidError) as exc_info:
      client.get_item("access-1")
    assert exc_info.value.item_gone and exc_info.value.needs_reauth

  def test_rate_limits_are_retried(self):
    responses = [_error("RATE_LIMIT_EXCEEDED", 429), httpx.Response(200, json={})]

    def handler(request):
      return responses.pop(0)

    assert _client(handler).get_item("access-1") == {}
    assert responses == []

  def test_retries_are_bounded(self):
    client = _client(lambda r: httpx.Response(503, text="down"), max_attempts=2)
    with pytest.raises(PlaidError) as exc_info:
      client.get_item("access-1")
    assert exc_info.value.status_code == 503
    assert not exc_info.value.needs_reauth


@pytest.mark.unit
def test_extend_folds_a_later_page_set_in():
  first = TransactionsSync(
    added=[{"transaction_id": "a"}],
    next_cursor="c1",
    update_status="INITIAL_UPDATE_COMPLETE",
  )
  first.extend(
    TransactionsSync(
      added=[{"transaction_id": "b"}],
      removed=[{"transaction_id": "z"}],
      accounts=[{"account_id": "acct"}],
      next_cursor="c2",
      update_status="HISTORICAL_UPDATE_COMPLETE",
    )
  )
  assert [t["transaction_id"] for t in first.added] == ["a", "b"]
  assert len(first.removed) == 1 and first.accounts == [{"account_id": "acct"}]
  assert first.next_cursor == "c2" and first.history_complete
