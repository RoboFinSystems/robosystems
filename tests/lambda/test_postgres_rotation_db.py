"""The PostgreSQL rotation steps can be retried at any point.

Against a real local PostgreSQL (a throwaway role) and moto Secrets Manager.
A retried or re-driven step must never leave the database on a password no
version of the secret holds.
"""

from __future__ import annotations

import json
import os
import uuid
from urllib.parse import urlparse

import boto3
import psycopg2
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def role():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  parsed = urlparse(url)
  name = f"rot_{uuid.uuid4().hex[:10]}"
  password = uuid.uuid4().hex
  admin = psycopg2.connect(url)
  admin.autocommit = True
  with admin.cursor() as cur:
    cur.execute(f'CREATE ROLE "{name}" LOGIN PASSWORD %s', (password,))
  db_info = {
    "host": parsed.hostname,
    "port": parsed.port or 5432,
    "database": parsed.path.lstrip("/"),
    "sslmode": "disable",
  }
  yield name, password, db_info
  with admin.cursor() as cur:
    cur.execute(f'DROP ROLE IF EXISTS "{name}"')
  admin.close()


def _logs_in(db_info, user, password) -> bool:
  try:
    psycopg2.connect(
      host=db_info["host"],
      port=db_info["port"],
      dbname=db_info["database"],
      user=user,
      password=password,
      sslmode="disable",
    ).close()
    return True
  except psycopg2.OperationalError:
    return False


@pytest.fixture
def rotation(pgrot, role, monkeypatch):
  name, password, db_info = role
  monkeypatch.setattr(pgrot, "get_database_connection_info", lambda arn, env: db_info)
  secrets = boto3.client("secretsmanager", region_name="us-east-1")
  arn = secrets.create_secret(
    Name="robosystems/prod/postgres",
    SecretString=json.dumps({"POSTGRES_USER": name, "POSTGRES_PASSWORD": password}),
  )["ARN"]
  return pgrot, secrets, arn, name, db_info


def _pending_password(secrets, arn, token) -> str:
  value = secrets.get_secret_value(
    SecretId=arn, VersionId=token, VersionStage="AWSPENDING"
  )
  return json.loads(value["SecretString"])["POSTGRES_PASSWORD"]


def test_set_secret_run_twice_leaves_the_pending_password_live(rotation):
  pgrot, secrets, arn, name, db_info = rotation
  token = uuid.uuid4().hex
  pgrot.create_secret(arn, token)
  pgrot.set_secret(arn, token, "prod")

  pgrot.set_secret(arn, token, "prod")

  assert _logs_in(db_info, name, _pending_password(secrets, arn, token))


def test_create_secret_retried_keeps_the_password_already_applied(rotation):
  pgrot, secrets, arn, name, db_info = rotation
  token = uuid.uuid4().hex
  pgrot.create_secret(arn, token)
  applied = _pending_password(secrets, arn, token)
  pgrot.set_secret(arn, token, "prod")

  pgrot.create_secret(arn, token)

  assert _pending_password(secrets, arn, token) == applied
  assert _logs_in(db_info, name, applied)
