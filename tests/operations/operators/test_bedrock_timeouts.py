"""The Bedrock client never re-sends a generation (each one is billed), and
retries only refusals that come before a generation starts."""

from __future__ import annotations

import http.server
import json
import threading
import time
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError, ReadTimeoutError

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
  config = ai_client._BEDROCK_CONFIG.merge(Config(read_timeout=0.5))
  client = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    endpoint_url=url,
    aws_access_key_id="x",
    aws_secret_access_key="y",
    config=config,
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
