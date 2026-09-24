"""QBClient against a local stand-in for Intuit, over real sockets.

The real ``requests``, intuitlib and python-quickbooks stacks run end to end.
Only the far side is fake: a discovery document, a token endpoint with
Intuit's rotation rule, and an endpoint that accepts and never answers.
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import pytest
import redis
import requests

from robosystems.adapters.quickbooks.client import api
from robosystems.adapters.quickbooks.client.api import (
  QBAuthFailedError,
  QBAuthUnavailableError,
  QBClient,
)


class _Intuit:
  """Token-endpoint behaviour. ``scripted`` answers are served first; then
  the rotation rule applies: only the current refresh token is accepted,
  and each refresh issues the next one."""

  def __init__(self) -> None:
    self.scripted: list[tuple[int, dict]] = []
    self.valid_refresh = "R1"
    self.issued = 1
    self.token_delay = 0.0
    self.token_calls = 0
    self.discovery_status = 200
    self.lock = threading.Lock()

  def token(self, presented: str) -> tuple[int, dict]:
    with self.lock:
      self.token_calls += 1
      if self.scripted:
        return self.scripted.pop(0)
    time.sleep(self.token_delay)
    with self.lock:
      if presented != self.valid_refresh:
        return 400, {"error": "invalid_grant"}
      self.issued += 1
      self.valid_refresh = f"R{self.issued}"
      return 200, {
        "access_token": f"A{self.issued}",
        "refresh_token": self.valid_refresh,
        "expires_in": 3600,
        "x_refresh_token_expires_in": 8726400,
      }


@pytest.fixture
def intuit():
  state = _Intuit()

  class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
      pass

    def _send(self, status: int, body: dict) -> None:
      payload = json.dumps(body).encode()
      self.send_response(status)
      self.send_header("Content-Type", "application/json")
      self.send_header("Content-Length", str(len(payload)))
      self.end_headers()
      self.wfile.write(payload)

    def do_GET(self):
      if self.path == "/discovery" and state.discovery_status != 200:
        self._send(state.discovery_status, {})
      elif self.path == "/discovery":
        base = f"http://127.0.0.1:{self.server.server_port}"
        self._send(
          200,
          {
            "issuer": base,
            "authorization_endpoint": f"{base}/auth",
            "token_endpoint": f"{base}/token",
            "revocation_endpoint": f"{base}/revoke",
            "userinfo_endpoint": f"{base}/userinfo",
            "jwks_uri": f"{base}/jwks",
          },
        )
      elif self.path.startswith("/stall"):
        time.sleep(30)
      else:
        self._send(404, {})

    def do_POST(self):
      length = int(self.headers.get("Content-Length") or 0)
      form = dict(
        pair.split("=", 1) for pair in self.rfile.read(length).decode().split("&")
      )
      if self.path == "/token":
        status, body = state.token(form.get("refresh_token", ""))
        self._send(status, body)
      else:
        self._send(404, {})

  server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
  server.daemon_threads = True
  threading.Thread(target=server.serve_forever, daemon=True).start()
  state.base = f"http://127.0.0.1:{server.server_port}"  # type: ignore[attr-defined]
  with (
    patch.object(api.env, "INTUIT_ENVIRONMENT", f"{state.base}/discovery"),  # type: ignore[attr-defined]
    patch.object(api.env, "INTUIT_CLIENT_ID", "client"),
    patch.object(api.env, "INTUIT_CLIENT_SECRET", "secret"),
    patch.object(api.env, "INTUIT_REDIRECT_URI", "http://localhost/cb"),
    patch.object(api, "QB_TIMEOUT", (1, 1), create=True),
    patch.object(api, "QB_TOKEN_TIMEOUT", (1, 1), create=True),
    patch.object(QBClient, "_backoff", lambda self, attempt, reason: None, create=True),
  ):
    yield state
  server.shutdown()


@pytest.fixture
def store():
  """The stored credential bundle, in place of the platform DB row."""
  bundle = {"refresh_token": "R1", "access_token": "A1"}
  with (
    patch.object(
      QBClient, "_read_stored_credentials", lambda self: dict(bundle), create=True
    ),
    patch.object(
      QBClient,
      "_persist_rotated_tokens",
      lambda self, _prior: bundle.update(
        refresh_token=self.refresh_token, access_token=self.access_token
      ),
    ),
    patch.object(QBClient, "_mark_needs_reauth") as marked,
  ):
    yield bundle, marked


def _client(connection_id: str | None = "conn-1") -> QBClient:
  return QBClient(
    realm_id="123",
    qb_credentials={"refresh_token": "R1", "access_token": "A1"},
    connection_id=connection_id,
  )


@pytest.mark.unit
class TestTokenEndpointFailures:
  @pytest.mark.parametrize("status", [429, 500, 503])
  def test_a_transient_answer_is_retried_and_does_not_mark_reauth(
    self, intuit, store, status
  ):
    _bundle, marked = store
    intuit.scripted = [(status, {"error": "temporarily_unavailable"})]

    client = _client()

    assert client.refresh_token == "R2"
    assert intuit.token_calls == 2
    marked.assert_not_called()

  def test_a_persistent_503_is_recoverable_and_leaves_the_connection(
    self, intuit, store
  ):
    _bundle, marked = store
    intuit.scripted = [(503, {})] * 3

    with pytest.raises(QBAuthFailedError) as exc:
      _client()

    assert exc.value.recoverable is True
    assert intuit.token_calls == 3
    marked.assert_not_called()

  def test_invalid_grant_marks_reauth(self, intuit, store):
    _bundle, marked = store
    intuit.scripted = [(400, {"error": "invalid_grant"})]

    with pytest.raises(QBAuthFailedError) as exc:
      _client()

    assert exc.value.recoverable is False
    assert intuit.token_calls == 1
    marked.assert_called_once()

  def test_invalid_client_is_not_a_reconnect(self, intuit, store):
    _bundle, marked = store
    intuit.scripted = [(401, {"error": "invalid_client"})]

    with pytest.raises(QBAuthFailedError) as exc:
      _client()

    assert exc.value.recoverable is True
    marked.assert_not_called()


@pytest.mark.unit
@pytest.mark.timeout(20)
class TestTimeouts:
  def test_a_stalled_token_endpoint_times_out_without_a_retry(self, intuit, store):
    _bundle, marked = store
    intuit.token_delay = 5  # longer than the patched 1s read timeout

    started = time.monotonic()
    with pytest.raises(QBAuthFailedError) as exc:
      _client()

    assert time.monotonic() - started < 5
    assert exc.value.recoverable is True
    # A read timeout may follow a rotation; a second call would present a
    # dead token, so there is exactly one.
    assert intuit.token_calls == 1
    marked.assert_not_called()

  def test_the_quickbooks_session_carries_a_timeout(self, intuit, store, monkeypatch):
    # The stand-in speaks plain HTTP; the OAuth session refuses it otherwise.
    monkeypatch.setenv("OAUTHLIB_INSECURE_TRANSPORT", "1")
    client = _client()

    started = time.monotonic()
    with pytest.raises(requests.exceptions.ReadTimeout):
      client.client.session.request("GET", f"{intuit.base}/stall")
    assert time.monotonic() - started < 5


@pytest.fixture
def live_valkey():
  client = redis.Redis.from_url(
    "redis://:valkey@localhost:6379/15", decode_responses=True
  )
  try:
    client.ping()
  except Exception:
    pytest.skip("Valkey not reachable")
  client.delete("lock:qb_token:conn-1")
  with patch(
    "robosystems.config.valkey_registry.create_redis_client", return_value=client
  ):
    yield client
  client.delete("lock:qb_token:conn-1")
  client.close()


@pytest.mark.unit
@pytest.mark.timeout(30)
class TestConcurrentRefresh:
  def test_two_refreshes_of_one_connection_both_succeed(
    self, intuit, store, live_valkey
  ):
    """Both start from the same stored R1. Unserialised, the second presents
    R1 after the first rotated it and gets invalid_grant."""
    bundle, marked = store
    intuit.token_delay = 0.3
    errors: list[BaseException] = []

    def run():
      try:
        _client()
      except BaseException as e:
        errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert errors == []
    marked.assert_not_called()
    assert bundle["refresh_token"] == intuit.valid_refresh == "R3"


@pytest.mark.unit
class TestReviewFindings:
  def test_a_discovery_failure_is_recoverable(self, intuit, store):
    _bundle, marked = store
    intuit.discovery_status = 500

    with pytest.raises(QBAuthUnavailableError):
      _client()
    marked.assert_not_called()

  def test_the_lock_outlasts_the_slowest_refresh(self):
    connect, read = api.QB_TOKEN_TIMEOUT
    worst = api._REFRESH_ATTEMPTS * (connect + read) + sum(
      2 ** (n - 1) + 1 for n in range(1, api._REFRESH_ATTEMPTS)
    )
    assert worst < api._TOKEN_LOCK_TTL_SECONDS

  def test_a_recoverable_failure_is_not_a_reconnect_over_rest(self):
    from fastapi import HTTPException

    from robosystems.middleware.extensions import _raise_mapped
    from robosystems.routers.extensions.roboledger.operations import ledger

    spec = next(
      s for s in ledger._registrar.registered_specs if s.name == "execute-event-block"
    )
    with pytest.raises(HTTPException) as busy:
      _raise_mapped(QBAuthUnavailableError("busy"), spec.error_map)
    with pytest.raises(HTTPException) as dead:
      _raise_mapped(QBAuthFailedError("dead", recoverable=False), spec.error_map)

    assert (busy.value.status_code, dead.value.status_code) == (503, 401)


@pytest.mark.unit
@pytest.mark.timeout(30)
class TestLockContention:
  def test_a_held_lock_is_waited_for_never_refreshed_past(
    self, intuit, store, live_valkey
  ):
    """Refreshing past a holder presents the token it is about to supersede."""
    _bundle, marked = store
    live_valkey.set("lock:qb_token:conn-1", "someone-else", ex=30)

    with (
      patch.object(api, "_TOKEN_LOCK_WAIT_SECONDS", 0.5),
      pytest.raises(QBAuthUnavailableError),
    ):
      _client()

    assert intuit.token_calls == 0
    marked.assert_not_called()
