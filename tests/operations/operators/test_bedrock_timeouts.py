"""The Bedrock client never re-sends a generation (each one is billed), and
retries only refusals that come before a generation starts."""

from __future__ import annotations

import http.server
import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from robosystems.operations.aws.long_call import long_call_client
from robosystems.operations.operators import ai_client
from robosystems.operations.operators.ai_client import AIClient


@pytest.fixture
def slow_bedrock():
  """A local stand-in for bedrock-runtime that outlasts a short read timeout."""
  hits: list[float] = []

  class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
      hits.append(time.time())
      self.rfile.read(int(self.headers["Content-Length"]))
      time.sleep(1.5)
      body = json.dumps(
        {
          "output": {"message": {"role": "assistant", "content": [{"text": "x"}]}},
          "stopReason": "end_turn",
          "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
          "metrics": {"latencyMs": 1},
        }
      ).encode()
      try:
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
      except Exception:
        pass

    def log_message(self, *args):
      pass

  server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
  threading.Thread(target=server.serve_forever, daemon=True).start()
  yield f"http://127.0.0.1:{server.server_port}", hits
  server.shutdown()


@pytest.mark.unit
def test_a_timed_out_generation_is_sent_once(slow_bedrock):
  url, hits = slow_bedrock
  client = long_call_client(
    "bedrock-runtime",
    0.5,
    region_name="us-east-1",
    endpoint_url=url,
    aws_access_key_id="x",
    aws_secret_access_key="y",
  )

  with pytest.raises(ReadTimeoutError):
    client.converse(
      modelId="m", messages=[{"role": "user", "content": [{"text": "q"}]}]
    )

  assert len(hits) == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_only_refusals_before_generation_are_retried():
  def refusal(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "Converse")

  client = AIClient.__new__(AIClient)
  client.client = MagicMock()
  client.client.converse.side_effect = [refusal("ThrottlingException"), {"ok": True}]
  with patch.object(ai_client.asyncio, "sleep"):
    assert await client._converse_with_retry({}) == {"ok": True}
  assert client.client.converse.call_count == 2

  client.client.converse.reset_mock()
  client.client.converse.side_effect = refusal("ValidationException")
  with pytest.raises(ClientError):
    await client._converse_with_retry({})
  assert client.client.converse.call_count == 1


@pytest.fixture
def dropping_bedrock():
  """A stand-in for bedrock-runtime whose first connection dies before any
  reply, the way a pooled connection the NAT dropped while idle does."""
  import socket

  hits: list[str] = []
  body = json.dumps(
    {
      "output": {"message": {"role": "assistant", "content": [{"text": "x"}]}},
      "stopReason": "end_turn",
      "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
      "metrics": {"latencyMs": 1},
    }
  ).encode()
  listener = socket.socket()
  listener.bind(("127.0.0.1", 0))
  listener.listen()

  def serve():
    while True:
      try:
        conn, _ = listener.accept()
      except OSError:
        return
      with conn:
        conn.recv(65536)
        if not hits:
          hits.append("dropped")
          continue
        hits.append("answered")
        conn.sendall(
          b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
          + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
          + body
        )

  threading.Thread(target=serve, daemon=True).start()
  yield f"http://127.0.0.1:{listener.getsockname()[1]}", hits
  listener.close()


def _client_for(url: str) -> AIClient:
  client = AIClient.__new__(AIClient)
  client.client = long_call_client(
    "bedrock-runtime",
    5,
    region_name="us-east-1",
    endpoint_url=url,
    aws_access_key_id="x",
    aws_secret_access_key="y",
  )
  return client


_REQUEST = {"modelId": "m", "messages": [{"role": "user", "content": [{"text": "q"}]}]}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_connection_dropped_before_the_call_is_re_sent(dropping_bedrock):
  url, hits = dropping_bedrock

  response = await _client_for(url)._converse_with_retry(_REQUEST)

  assert response["stopReason"] == "end_turn"
  assert hits == ["dropped", "answered"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_drop_after_the_window_is_not_re_sent(dropping_bedrock):
  url, hits = dropping_bedrock

  with patch.object(ai_client, "_STALE_CONNECTION_WINDOW", -1):
    with pytest.raises(ai_client._STALE_CONNECTION_ERRORS):
      await _client_for(url)._converse_with_retry(_REQUEST)

  assert hits == ["dropped"]
