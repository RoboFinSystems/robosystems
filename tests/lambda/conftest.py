"""Fixtures for graph volume Lambda tests.

The Lambda modules in bin/lambda/ are standalone scripts that read env vars and
build boto3 clients at import time. To exercise them with moto, we must:
  1. Set the env vars before import.
  2. Be inside a mock_aws() context before import (so module-level boto3
     resources are patched).
  3. Pre-create the DynamoDB tables, SNS topics, etc. that the module assumes.

The `gvm` and `gvd` fixtures handle that import dance via importlib.reload so
each test gets a fresh module state under a fresh mock_aws() context.
"""

import importlib
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

LAMBDA_DIR = Path(__file__).resolve().parent.parent.parent / "bin" / "lambda"


@pytest.fixture
def aws_env(monkeypatch):
  """Env vars required by Lambda modules at import time."""
  # pytest.ini sets AWS_ENDPOINT_URL=http://localhost:4566 for LocalStack-based
  # tests. Clear it here so boto3 hits the default endpoint where moto's
  # mock_aws can intercept; otherwise moto is bypassed and calls fall through
  # to LocalStack (which doesn't have all services enabled).
  monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
  monkeypatch.delenv("AWS_PROFILE", raising=False)

  monkeypatch.setenv("ENVIRONMENT", "test")
  monkeypatch.setenv("VOLUME_REGISTRY_TABLE", "test-volume-registry")
  monkeypatch.setenv("GRAPH_REGISTRY_TABLE", "test-graph-registry")
  monkeypatch.setenv(
    "INSTANCE_REGISTRY_TABLE", "robosystems-graph-test-instance-registry"
  )
  monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:test-topic")
  monkeypatch.setenv(
    "VOLUME_MANAGER_FUNCTION_ARN",
    "arn:aws:lambda:us-east-1:123456789012:function:test-volume-manager",
  )
  monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
  monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
  monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
  monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


@pytest.fixture
def aws(aws_env):
  """Active mock_aws with pre-created tables and SNS topic."""
  with mock_aws():
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    for table_name, key in [
      ("test-volume-registry", "volume_id"),
      ("test-graph-registry", "graph_id"),
      ("robosystems-graph-test-instance-registry", "instance_id"),
    ]:
      ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
      )
    sns = boto3.client("sns", region_name="us-east-1")
    sns.create_topic(Name="test-topic")
    yield


def _import_lambda(module_name: str):
  """Import (or reload) a bin/lambda module under the active mock_aws."""
  if str(LAMBDA_DIR) not in sys.path:
    sys.path.insert(0, str(LAMBDA_DIR))
  if module_name in sys.modules:
    importlib.reload(sys.modules[module_name])
  else:
    importlib.import_module(module_name)
  return sys.modules[module_name]


@pytest.fixture
def gvm(aws):
  """graph_volume_manager module under mock_aws."""
  yield _import_lambda("graph_volume_manager")


@pytest.fixture
def gvd(aws):
  """graph_volume_detachment module under mock_aws."""
  yield _import_lambda("graph_volume_detachment")


@pytest.fixture
def gvmon(aws):
  """graph_volume_monitor module under mock_aws."""
  yield _import_lambda("graph_volume_monitor")
