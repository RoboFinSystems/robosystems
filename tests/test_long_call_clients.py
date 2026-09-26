"""Lambda and Bedrock clients are built only through `long_call_client`.

botocore's defaults (a 60s read timeout, then retries) re-send a call that is
still running: a second billed generation, a second snapshot, a second volume
claim. The scan fails when a new call site builds one of these clients
directly.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

PACKAGE = Path(__file__).resolve().parents[1] / "robosystems"
HELPER = PACKAGE / "operations" / "aws" / "long_call.py"
LONG_CALL_SERVICES = {
  "lambda",
  "bedrock-runtime",
  "bedrock-agent-runtime",
  "sagemaker-runtime",
}


def _direct_clients(path: Path) -> list[int]:
  """Lines that pass a long-call service name straight to a `*client` call, or
  put one in a `service_name` dict entry for `boto3.client(**kwargs)`."""
  lines = []
  for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
    if isinstance(node, ast.Dict):
      for key, value in zip(node.keys, node.values, strict=True):
        if (
          isinstance(key, ast.Constant)
          and key.value == "service_name"
          and isinstance(value, ast.Constant)
          and value.value in LONG_CALL_SERVICES
        ):
          lines.append(node.lineno)
      continue
    if not isinstance(node, ast.Call):
      continue
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
    if not name.endswith("client") or name == "long_call_client":
      continue
    args = [*node.args, *(kw.value for kw in node.keywords if kw.arg == "service_name")]
    if any(
      isinstance(arg, ast.Constant) and arg.value in LONG_CALL_SERVICES for arg in args
    ):
      lines.append(node.lineno)
  return lines


def test_every_lambda_and_bedrock_client_goes_through_the_helper():
  offenders = {
    str(path.relative_to(PACKAGE.parent)): lines
    for path in PACKAGE.rglob("*.py")
    if path != HELPER and (lines := _direct_clients(path))
  }
  assert offenders == {}


def test_the_tier_upgrade_waits_out_the_volume_manager_once():
  from unittest.mock import patch

  from robosystems.operations.graph.tasks import graph_tier_upgrade

  with patch.object(graph_tier_upgrade, "env") as env:
    env.AWS_REGION = "us-east-1"
    env.is_development.return_value = False
    client = graph_tier_upgrade._get_lambda_client()

  config = client.meta.config
  assert config.read_timeout >= graph_tier_upgrade.VOLUME_MANAGER_TIMEOUT_SECONDS
  assert config.retries["total_max_attempts"] == 1
