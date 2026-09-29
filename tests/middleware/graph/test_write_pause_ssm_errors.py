"""The write-pause read fails open on the errors a real SSM client raises when
the network drops or stalls (botocore attaches no response to those)."""

import socket
import threading

import boto3
import pytest
from botocore.config import Config

from robosystems.config.parameter_store import ParameterStoreManager
from robosystems.middleware.graph import write_pause


def _server(behaviour: str):
  sock = socket.socket()
  sock.bind(("127.0.0.1", 0))
  sock.listen()
  stop = threading.Event()
  held: list[socket.socket] = []

  def serve():
    sock.settimeout(0.2)
    while not stop.is_set():
      try:
        conn, _ = sock.accept()
      except TimeoutError:
        continue
      if behaviour == "close":
        conn.recv(65536)
        conn.close()
      else:
        held.append(conn)

  threading.Thread(target=serve, daemon=True).start()
  return sock, stop, held


@pytest.fixture(params=["close", "stall"])
def unreachable_ssm(request, monkeypatch):
  sock, stop, held = _server(request.param)
  client = boto3.session.Session().client(
    "ssm",
    region_name="us-east-1",
    endpoint_url=f"http://127.0.0.1:{sock.getsockname()[1]}",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    config=Config(connect_timeout=0.5, read_timeout=0.5, retries={"max_attempts": 1}),
  )
  monkeypatch.setattr(
    "robosystems.config.parameter_store._get_fast_ssm_client", lambda: client
  )
  manager = ParameterStoreManager(environment="prod")
  monkeypatch.setattr(
    "robosystems.config.parameter_store.get_parameter_manager", lambda: manager
  )
  yield manager
  stop.set()
  for conn in held:
    conn.close()
  sock.close()


@pytest.mark.unit
def test_uncached_read_returns_default_when_ssm_drops(unreachable_ssm):
  assert unreachable_ssm.get_parameter_uncached("GRAPH_WRITES_PAUSED_UNTIL") == ""


@pytest.mark.unit
def test_writes_stay_allowed_when_the_pause_cannot_be_read(unreachable_ssm):
  assert write_pause.graph_writes_paused_until() is None
  write_pause.assert_graph_writes_allowed()
