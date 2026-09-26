"""Journey: a PostgreSQL rotation whose testSecret fails, then resumes.

create -> set -> test (fails) -> Secrets Manager re-drives the same token from
createSecret -> set -> test -> finish -> the app refreshes and reconnects.
Against a real local PostgreSQL role and moto Secrets Manager.

Pins, at every step, that some staged version of the secret logs in, that the
resumed rotation reuses the password the first pass already applied, and that
it ends with AWSCURRENT live, AWSPENDING cleared and the old password dead.
The stronger invariant, AWSCURRENT logs in at every step, is pinned separately
as a strict xfail: the single-user strategy cannot hold it.
"""

from __future__ import annotations

import importlib
import json
import uuid

import boto3
import psycopg2
import pytest
from moto.core import DEFAULT_ACCOUNT_ID
from moto.secretsmanager.models import secretsmanager_backends

pytestmark = pytest.mark.integration

_db = importlib.import_module("tests.lambda.test_postgres_rotation_db")
role = _db.role
_logs_in = _db._logs_in

SECRET_NAME = "robosystems/prod/postgres"
STEPS = ("createSecret", "setSecret", "testSecret", "finishSecret")


@pytest.fixture
def journey(pgrot, role, monkeypatch):
  name, password, db_info = role
  monkeypatch.setattr(pgrot, "get_database_connection_info", lambda arn, env: db_info)
  secrets = boto3.client("secretsmanager", region_name="us-east-1")
  arn = secrets.create_secret(
    Name=SECRET_NAME,
    SecretString=json.dumps({"POSTGRES_USER": name, "POSTGRES_PASSWORD": password}),
  )["ARN"]
  return pgrot, secrets, arn, name, password, db_info


def _begin_rotation(arn: str, token: str) -> None:
  """What RotateSecret does before invoking the function: enable rotation and
  stage an empty AWSPENDING version under the request token."""
  secret = secretsmanager_backends[DEFAULT_ACCOUNT_ID]["us-east-1"].secrets[arn]
  secret.rotation_requested = True
  secret.rotation_enabled = True
  secret.rotation_lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:rot"
  secret.remove_version_stages_from_old_versions(["AWSPENDING"])
  secret.versions[token] = {
    "createdate": 0,
    "version_id": token,
    "version_stages": ["AWSPENDING"],
  }


def _moto_follow_awscurrent(arn: str) -> None:
  """Moto's UpdateSecretVersionStage moves the AWSCURRENT label but not the
  version a stage-less GetSecretValue returns; real Secrets Manager returns
  AWSCURRENT. Realign it so the app's stage-less read behaves as in AWS."""
  secret = secretsmanager_backends[DEFAULT_ACCOUNT_ID]["us-east-1"].secrets[arn]
  for version_id, version in secret.versions.items():
    if "AWSCURRENT" in version["version_stages"]:
      secret.reset_default_version(version, version_id)


def _invoke(pgrot, arn: str, token: str, step: str) -> None:
  pgrot.lambda_handler(
    {"SecretId": arn, "ClientRequestToken": token, "Step": step}, None
  )


def _password_at(secrets, arn: str, stage: str) -> str | None:
  try:
    value = secrets.get_secret_value(SecretId=arn, VersionStage=stage)
  except secrets.exceptions.ResourceNotFoundException:
    return None
  return json.loads(value["SecretString"])["POSTGRES_PASSWORD"]


def _stages(secrets, arn: str) -> dict[str, list[str]]:
  return secrets.describe_secret(SecretId=arn)["VersionIdsToStages"]


def _fail_first_test_connect(pgrot, monkeypatch) -> None:
  """testSecret's login times out once, as a transient RDS blip would surface."""
  real_connect = pgrot._connect
  state = {"failed": False}

  def flaky(db_info, secret, *, user=None):
    if not state["failed"]:
      state["failed"] = True
      raise psycopg2.OperationalError("timeout expired")
    return real_connect(db_info, secret, user=user)

  monkeypatch.setattr(pgrot, "_connect", flaky)


def _run_first_pass(pgrot, secrets, arn, name, db_info, token, monkeypatch, check):
  _begin_rotation(arn, token)
  _invoke(pgrot, arn, token, "createSecret")
  check("createSecret")
  _invoke(pgrot, arn, token, "setSecret")
  check("setSecret")
  _fail_first_test_connect(pgrot, monkeypatch)
  with pytest.raises(psycopg2.OperationalError):
    _invoke(pgrot, arn, token, "testSecret")
  check("testSecret (failed)")


def test_failed_test_step_resumes_to_a_clean_rotation(journey, monkeypatch):
  pgrot, secrets, arn, name, original, db_info = journey
  token = str(uuid.uuid4())

  def some_stage_logs_in(step: str) -> None:
    live = [
      stage
      for stage in ("AWSCURRENT", "AWSPENDING")
      if (pw := _password_at(secrets, arn, stage)) and _logs_in(db_info, name, pw)
    ]
    assert live, f"after {step}: no staged version of the secret logs in"

  _run_first_pass(
    pgrot, secrets, arn, name, db_info, token, monkeypatch, some_stage_logs_in
  )
  applied = _password_at(secrets, arn, "AWSPENDING")
  assert applied and applied != original
  assert token in _stages(secrets, arn)
  assert "AWSPENDING" in _stages(secrets, arn)[token]

  for step in STEPS:
    _invoke(pgrot, arn, token, step)
    some_stage_logs_in(f"resumed {step}")
    if step == "createSecret":
      assert _password_at(secrets, arn, "AWSPENDING") == applied, (
        "the resumed createSecret replaced a password setSecret already applied"
      )

  stages = _stages(secrets, arn)
  assert stages[token] == ["AWSCURRENT"]
  assert not any("AWSPENDING" in s for s in stages.values())
  assert _password_at(secrets, arn, "AWSCURRENT") == applied
  assert _password_at(secrets, arn, "AWSPREVIOUS") == original
  assert not _logs_in(db_info, name, original)

  from robosystems.config.secrets_manager import SecretsManager

  _moto_follow_awscurrent(arn)
  app = SecretsManager(environment="prod", region="us-east-1")
  app.refresh("postgres")
  refreshed = app.get_secret("postgres")
  assert refreshed["POSTGRES_PASSWORD"] == applied
  assert _logs_in(db_info, refreshed["POSTGRES_USER"], refreshed["POSTGRES_PASSWORD"])


@pytest.mark.xfail(
  strict=True,
  reason=(
    "Single-user rotation: setSecret moves the database to the AWSPENDING "
    "password while AWSCURRENT still holds the old one, and a failed testSecret "
    "leaves it there until Secrets Manager re-drives the rotation. A client "
    "reading AWSCURRENT cannot open a new connection in that window."
  ),
)
def test_awscurrent_logs_in_at_every_step(journey, monkeypatch):
  pgrot, secrets, arn, name, _original, db_info = journey
  token = str(uuid.uuid4())

  def current_logs_in(step: str) -> None:
    current = _password_at(secrets, arn, "AWSCURRENT")
    assert current and _logs_in(db_info, name, current), (
      f"after {step}: the AWSCURRENT password does not log in"
    )

  current_logs_in("start")
  _run_first_pass(
    pgrot, secrets, arn, name, db_info, token, monkeypatch, current_logs_in
  )
  for step in STEPS:
    _invoke(pgrot, arn, token, step)
    current_logs_in(f"resumed {step}")
