"""send_email_job — the raw token never enters Dagster run config.

Run config is persisted indefinitely in Dagster run storage and rendered in
its UI; the platform DB holds only the token's hash. The job therefore
carries an opaque reference and resolves it from Valkey at send time.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import build_op_context

from robosystems.config.defaults import CacheDefaults
from robosystems.dagster.jobs import notifications
from robosystems.dagster.jobs.notifications import (
  SendEmailConfig,
  build_email_job_config,
  send_email_op,
)

RAW_TOKEN = "raw-reset-token-that-must-not-persist"


class FakeValkey:
  def __init__(self) -> None:
    self.store: dict[str, str] = {}
    self.ttls: dict[str, int] = {}

  def setex(self, key: str, ttl: int, value: str) -> None:
    self.store[key] = value
    self.ttls[key] = ttl

  def get(self, key: str) -> str | None:
    return self.store.get(key)

  def delete(self, key: str) -> None:
    self.store.pop(key, None)
    self.ttls.pop(key, None)


@pytest.fixture
def valkey():
  fake = FakeValkey()
  with patch.object(notifications, "_token_store", return_value=fake):
    yield fake


def _op_config(run_config: dict) -> SendEmailConfig:
  return SendEmailConfig(**run_config["ops"]["send_email_op"]["config"])


def _ses(send_result: bool = True) -> MagicMock:
  ses = MagicMock()
  for name in (
    "send_verification_email",
    "send_password_reset_email",
    "send_org_invitation_email",
    "send_welcome_email",
  ):
    setattr(ses, name, AsyncMock(return_value=send_result))
  return ses


@pytest.mark.unit
class TestRunConfigCarriesOnlyAReference:
  def test_raw_token_absent_from_run_config(self, valkey):
    run_config = build_email_job_config(
      "password_reset", "u@example.com", "U", token=RAW_TOKEN
    )

    assert RAW_TOKEN not in json.dumps(run_config)
    cfg = run_config["ops"]["send_email_op"]["config"]
    assert cfg["token_ref"]
    assert "token" not in cfg

  def test_reference_parks_the_token_with_a_ttl(self, valkey):
    run_config = build_email_job_config(
      "email_verification", "u@example.com", "U", token=RAW_TOKEN
    )

    ref = run_config["ops"]["send_email_op"]["config"]["token_ref"]
    key = f"email_token:{ref}"
    assert valkey.store[key] == RAW_TOKEN
    assert valkey.ttls[key] == CacheDefaults.EMAIL_TOKEN_REF_TTL

  def test_no_token_means_no_reference(self, valkey):
    run_config = build_email_job_config("welcome", "u@example.com", "U")

    assert "token_ref" not in run_config["ops"]["send_email_op"]["config"]
    assert valkey.store == {}

  def test_config_schema_has_no_raw_token_field(self):
    assert "token" not in SendEmailConfig.model_fields
    assert "token_ref" in SendEmailConfig.model_fields


@pytest.mark.unit
class TestSendEmailOpResolvesTheReference:
  def test_resolves_token_and_discards_it_after_send(self, valkey):
    run_config = build_email_job_config(
      "password_reset", "u@example.com", "U", token=RAW_TOKEN
    )
    ref = run_config["ops"]["send_email_op"]["config"]["token_ref"]
    ses = _ses()

    with patch("robosystems.operations.aws.ses.ses_service", ses):
      result = send_email_op(context=build_op_context(), config=_op_config(run_config))

    assert result["success"] is True
    ses.send_password_reset_email.assert_awaited_once()
    assert ses.send_password_reset_email.await_args.kwargs["token"] == RAW_TOKEN
    assert f"email_token:{ref}" not in valkey.store

  def test_expired_reference_fails_without_sending(self, valkey):
    cfg = SendEmailConfig(
      email_type="password_reset",
      to_email="u@example.com",
      user_name="U",
      token_ref="no-longer-there",
    )
    ses = _ses()

    with patch("robosystems.operations.aws.ses.ses_service", ses):
      with pytest.raises(ValueError, match="expired or was already used"):
        send_email_op(context=build_op_context(), config=cfg)

    ses.send_password_reset_email.assert_not_awaited()

  def test_send_failure_keeps_the_reference_for_retry(self, valkey):
    run_config = build_email_job_config(
      "org_invitation",
      "u@example.com",
      "U",
      token=RAW_TOKEN,
      org_name="Acme",
    )
    ref = run_config["ops"]["send_email_op"]["config"]["token_ref"]
    ses = _ses(send_result=False)

    with patch("robosystems.operations.aws.ses.ses_service", ses):
      with pytest.raises(RuntimeError, match="Failed to send"):
        send_email_op(context=build_op_context(), config=_op_config(run_config))

    assert valkey.store[f"email_token:{ref}"] == RAW_TOKEN

  def test_welcome_email_needs_no_reference(self, valkey):
    run_config = build_email_job_config("welcome", "u@example.com", "U")
    ses = _ses()

    with patch("robosystems.operations.aws.ses.ses_service", ses):
      result = send_email_op(context=build_op_context(), config=_op_config(run_config))

    assert result["success"] is True
    ses.send_welcome_email.assert_awaited_once()
