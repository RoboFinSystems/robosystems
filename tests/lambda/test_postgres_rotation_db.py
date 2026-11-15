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
    cur.execute(f'CREATE ROLE "{name}" LOGIN CREATEROLE PASSWORD %s', (password,))
    cur.execute(f'GRANT CREATE ON DATABASE "{parsed.path.lstrip("/")}" TO "{name}"')
  db_info = {
    "host": parsed.hostname,
    "port": parsed.port or 5432,
    "database": parsed.path.lstrip("/"),
    "sslmode": "disable",
  }
  yield name, password, db_info
  with admin.cursor() as cur:
    cur.execute(
      "SELECT nspname FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner "
      "WHERE r.rolname = %s",
      (name,),
    )
    for (schema,) in cur.fetchall():
      cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
    for owner in (_scan_user(name), name):
      cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (owner,))
      if cur.fetchone():
        cur.execute(f'DROP OWNED BY "{owner}"')
        cur.execute(f'DROP ROLE "{owner}"')
  admin.close()


def _scan_user(master: str) -> str:
  return f"{master}_scan"


def seed_secret(master: str, password: str) -> str:
  return json.dumps(
    {
      "POSTGRES_USER": master,
      "POSTGRES_PASSWORD": password,
      "POSTGRES_SCAN_USER": _scan_user(master),
    }
  )


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
  monkeypatch.setenv("EXTENSIONS_DATABASE_NAME", db_info["database"])
  secrets = boto3.client("secretsmanager", region_name="us-east-1")
  arn = secrets.create_secret(
    Name="robosystems/prod/postgres",
    SecretString=seed_secret(name, password),
  )["ARN"]
  return pgrot, secrets, arn, name, db_info


def _pending(secrets, arn, token) -> dict:
  value = secrets.get_secret_value(
    SecretId=arn, VersionId=token, VersionStage="AWSPENDING"
  )
  return json.loads(value["SecretString"])


def _current(secrets, arn) -> dict:
  return json.loads(secrets.get_secret_value(SecretId=arn)["SecretString"])


def _promote(secrets, arn, token) -> None:
  stages = secrets.describe_secret(SecretId=arn)["VersionIdsToStages"]
  current = next(v for v, labels in stages.items() if "AWSCURRENT" in labels)
  secrets.update_secret_version_stage(
    SecretId=arn,
    VersionStage="AWSCURRENT",
    MoveToVersionId=token,
    RemoveFromVersionId=current,
  )


def _pending_password(secrets, arn, token) -> str:
  return _pending(secrets, arn, token)["POSTGRES_PASSWORD"]


def _as(db_info, user, password):
  conn = psycopg2.connect(
    host=db_info["host"],
    port=db_info["port"],
    dbname=db_info["database"],
    user=user,
    password=password,
    sslmode="disable",
  )
  conn.autocommit = True
  return conn


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


def test_scan_login_reads_what_the_master_owns_and_writes_nothing(rotation):
  pgrot, secrets, arn, name, db_info = rotation
  schema = f"kg{uuid.uuid4().hex[:12]}"
  master = _as(db_info, name, _current(secrets, arn)["POSTGRES_PASSWORD"])
  with master.cursor() as cur:
    cur.execute(f'CREATE SCHEMA "{schema}"')
    cur.execute(f'CREATE TABLE "{schema}".entries (id int)')
    cur.execute(f'INSERT INTO "{schema}".entries VALUES (1)')
  master.close()
  token = uuid.uuid4().hex
  pgrot.create_secret(arn, token)

  pgrot.set_secret(arn, token, "prod")

  pending = _pending(secrets, arn, token)
  scan = _as(db_info, pending["POSTGRES_SCAN_USER"], pending["POSTGRES_SCAN_PASSWORD"])
  with scan.cursor() as cur:
    cur.execute(f'SELECT count(*) FROM "{schema}".entries')
    assert cur.fetchone()[0] == 1
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
      cur.execute(f'INSERT INTO "{schema}".entries VALUES (2)')
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
      cur.execute(f'CREATE TABLE "{schema}".mine (id int)')
  scan.close()


def test_scan_login_reads_a_schema_created_after_the_rotation(rotation):
  pgrot, secrets, arn, name, db_info = rotation
  token = uuid.uuid4().hex
  pgrot.create_secret(arn, token)
  pgrot.set_secret(arn, token, "prod")
  pending = _pending(secrets, arn, token)

  schema = f"kg{uuid.uuid4().hex[:12]}"
  master = _as(db_info, name, pending["POSTGRES_PASSWORD"])
  with master.cursor() as cur:
    cur.execute(f'CREATE SCHEMA "{schema}"')
    cur.execute(f'CREATE TABLE "{schema}".entries (id int)')
  master.close()

  scan = _as(db_info, pending["POSTGRES_SCAN_USER"], pending["POSTGRES_SCAN_PASSWORD"])
  with scan.cursor() as cur:
    cur.execute(f'SELECT count(*) FROM "{schema}".entries')
    assert cur.fetchone()[0] == 0
  scan.close()


def test_each_rotation_gives_the_scan_login_a_new_password(rotation):
  pgrot, secrets, arn, name, db_info = rotation
  first = uuid.uuid4().hex
  pgrot.create_secret(arn, first)
  pgrot.set_secret(arn, first, "prod")
  old = _pending(secrets, arn, first)
  _promote(secrets, arn, first)

  second = uuid.uuid4().hex
  pgrot.create_secret(arn, second)
  pgrot.set_secret(arn, second, "prod")

  new = _pending(secrets, arn, second)
  scan_user = new["POSTGRES_SCAN_USER"]
  assert scan_user == old["POSTGRES_SCAN_USER"]
  assert _logs_in(db_info, scan_user, new["POSTGRES_SCAN_PASSWORD"])
  assert not _logs_in(db_info, scan_user, old["POSTGRES_SCAN_PASSWORD"])


def test_scan_login_failure_leaves_the_master_password_alone(rotation, monkeypatch):
  pgrot, secrets, arn, name, db_info = rotation
  current = _current(secrets, arn)
  token = uuid.uuid4().hex
  pgrot.create_secret(arn, token)

  def broken(conn, db_info, login, pending):
    raise RuntimeError("scan login could not be granted")

  monkeypatch.setattr(pgrot, "_ensure_scan_login", broken)
  with pytest.raises(RuntimeError):
    pgrot.set_secret(arn, token, "prod")

  assert _logs_in(db_info, name, current["POSTGRES_PASSWORD"])
  assert not _logs_in(db_info, name, _pending_password(secrets, arn, token))


def test_a_missing_extensions_database_skips_the_grants(rotation, monkeypatch):
  pgrot, secrets, arn, name, db_info = rotation
  monkeypatch.setenv("EXTENSIONS_DATABASE_NAME", f"absent_{uuid.uuid4().hex[:8]}")
  token = uuid.uuid4().hex
  pgrot.create_secret(arn, token)

  pgrot.set_secret(arn, token, "prod")

  assert _logs_in(db_info, name, _pending_password(secrets, arn, token))
