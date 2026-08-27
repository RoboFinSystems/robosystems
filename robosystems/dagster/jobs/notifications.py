"""Dagster jobs for notification operations (email, SMS, push)."""

import secrets
from typing import Any

from dagster import (
  Backoff,
  Config,
  OpExecutionContext,
  RetryPolicy,
  job,
  op,
)

from robosystems.config.defaults import CacheDefaults
from robosystems.logger import get_logger

logger = get_logger(__name__)


class SendEmailConfig(Config):
  """Configuration for sending an email."""

  email_type: (
    str  # email_verification, password_reset, welcome, org_invitation, passkey_enrolled
  )
  to_email: str
  user_name: str
  # Opaque reference to the raw verification/reset/invitation token, which
  # is parked in Valkey for CacheDefaults.EMAIL_TOKEN_REF_TTL. The raw token
  # never enters run config: Dagster persists run config indefinitely in run
  # storage and renders it in its UI, while the platform DB deliberately
  # holds only the token's hash.
  token_ref: str | None = None
  app: str = "roboledger"
  operation_id: str | None = None  # For SSE tracking
  org_name: str | None = None  # For org_invitation emails
  inviter_name: str | None = None  # For org_invitation emails
  passkey_name: str | None = None  # For passkey_enrolled emails
  new_email: str | None = None  # For email_changed notice (masked new address)


class EmailResult:
  """Result of an email send operation."""

  def __init__(
    self,
    success: bool,
    email_type: str,
    to_email: str,
    message_id: str | None = None,
    error: str | None = None,
  ):
    self.success = success
    self.email_type = email_type
    self.to_email = to_email
    self.message_id = message_id
    self.error = error

  def to_dict(self) -> dict:
    return {
      "success": self.success,
      "email_type": self.email_type,
      "to_email": self.to_email,
      "message_id": self.message_id,
      "error": self.error,
    }


_EMAIL_TOKEN_KEY_PREFIX = "email_token:"


def _token_store() -> Any:
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

  return create_redis_client(ValkeyDatabase.AUTH)


def _stash_email_token(token: str) -> str:
  """Park a raw email token in Valkey; return the reference to carry in run config."""
  ref = secrets.token_urlsafe(32)
  _token_store().setex(
    f"{_EMAIL_TOKEN_KEY_PREFIX}{ref}", CacheDefaults.EMAIL_TOKEN_REF_TTL, token
  )
  return ref


def _fetch_email_token(ref: str) -> str | None:
  return _token_store().get(f"{_EMAIL_TOKEN_KEY_PREFIX}{ref}") or None


def _discard_email_token(ref: str) -> None:
  _token_store().delete(f"{_EMAIL_TOKEN_KEY_PREFIX}{ref}")


@op(
  retry_policy=RetryPolicy(
    max_retries=3,
    delay=5,
    backoff=Backoff.EXPONENTIAL,
  ),
  tags={"kind": "email", "category": "notification"},
)
def send_email_op(context: OpExecutionContext, config: SendEmailConfig) -> dict:
  """Send an email via SES.

  Token requirements by email_type:
  - email_verification, password_reset, org_invitation: token_ref required,
    and it must still resolve (the parked token expires after
    CacheDefaults.EMAIL_TOKEN_REF_TTL and is discarded once sent)
  - org_invitation: org_name also required
  - welcome: no token

  A send failure raises, so the op's RetryPolicy retries it. When
  operation_id is set, the result is written to the SSE operation metadata.
  """
  import asyncio

  from robosystems.operations.aws.ses import ses_service

  context.log.info(
    f"Sending {config.email_type} email to {config.to_email} for app {config.app}"
  )

  token: str | None = None
  if config.token_ref:
    token = _fetch_email_token(config.token_ref)
    if not token:
      # Expired or already sent. The caller must issue a fresh token; a
      # re-execution from the Dagster UI must not be able to resend one.
      raise ValueError(
        f"Token reference for {config.email_type} has expired or was already used"
      )

  loop = asyncio.new_event_loop()
  try:
    if config.email_type == "email_verification":
      if not token:
        raise ValueError("Token required for email_verification")
      success = loop.run_until_complete(
        ses_service.send_verification_email(
          user_email=config.to_email,
          user_name=config.user_name,
          token=token,
          app=config.app,
        )
      )
    elif config.email_type == "password_reset":
      if not token:
        raise ValueError("Token required for password_reset")
      success = loop.run_until_complete(
        ses_service.send_password_reset_email(
          user_email=config.to_email,
          user_name=config.user_name,
          token=token,
          app=config.app,
        )
      )
    elif config.email_type == "welcome":
      success = loop.run_until_complete(
        ses_service.send_welcome_email(
          user_email=config.to_email,
          user_name=config.user_name,
          app=config.app,
        )
      )
    elif config.email_type == "org_invitation":
      if not token:
        raise ValueError("Token required for org_invitation")
      if not config.org_name:
        raise ValueError("org_name required for org_invitation")
      success = loop.run_until_complete(
        ses_service.send_org_invitation_email(
          user_email=config.to_email,
          inviter_name=config.inviter_name or "A teammate",
          org_name=config.org_name,
          token=token,
          app=config.app,
        )
      )
    elif config.email_type == "passkey_enrolled":
      success = loop.run_until_complete(
        ses_service.send_passkey_enrolled_email(
          user_email=config.to_email,
          user_name=config.user_name,
          passkey_name=config.passkey_name or "a new passkey",
          app=config.app,
        )
      )
    elif config.email_type == "email_changed":
      success = loop.run_until_complete(
        ses_service.send_email_changed_notice(
          user_email=config.to_email,
          user_name=config.user_name,
          new_email=config.new_email or "a new address",
          app=config.app,
        )
      )
    else:
      raise ValueError(f"Unknown email type: {config.email_type}")

  finally:
    loop.close()

  result = EmailResult(
    success=success,
    email_type=config.email_type,
    to_email=config.to_email,
    error=None if success else "Email send failed",
  )

  if success:
    context.log.info(
      f"Successfully sent {config.email_type} email to {config.to_email}"
    )
  else:
    context.log.error(f"Failed to send {config.email_type} email to {config.to_email}")
    # Raise to trigger retry
    raise RuntimeError(f"Failed to send {config.email_type} email to {config.to_email}")

  if config.token_ref:
    # Best-effort: the TTL is the backstop, and raising here would retry
    # the op and send the email twice.
    try:
      _discard_email_token(config.token_ref)
    except Exception as e:
      context.log.warning(f"Could not discard token reference after send: {e}")

  if config.operation_id:
    _emit_email_result_to_sse(context, config.operation_id, result.to_dict())

  return result.to_dict()


def _emit_email_result_to_sse(
  context: OpExecutionContext,
  operation_id: str,
  result: dict,
) -> None:
  """Update SSE operation metadata with the email result."""
  try:
    from robosystems.middleware.sse.event_storage import SSEEventStorage

    storage = SSEEventStorage()
    storage.update_operation_result_sync(operation_id, result)
    context.log.info(f"Updated SSE metadata for operation {operation_id}")
  except Exception as e:
    context.log.warning(f"Failed to update SSE operation metadata: {e}")


@job(
  tags={
    "dagster/priority": "1",
    "dagster/max_runtime": 300,  # 5 minute max
    "category": "notification",
  },
)
def send_email_job():
  """Send an email via SES, with 3 retries at exponential backoff.

  Usage:
    from robosystems.middleware.sse import run_and_monitor_dagster_job, build_email_job_config

    # Queue email send with SSE monitoring. The raw token is parked in
    # Valkey by build_email_job_config; run config carries a reference.
    background_tasks.add_task(
      run_and_monitor_dagster_job,
      job_name="send_email_job",
      operation_id=operation_id,
      run_config=build_email_job_config(
        email_type="email_verification",
        to_email="user@example.com",
        user_name="John",
        token="abc123",
        app="roboledger",
        operation_id=operation_id,
      ),
    )
  """
  send_email_op()


# Convenience function for building email job config
def build_email_job_config(
  email_type: str,
  to_email: str,
  user_name: str,
  token: str | None = None,
  app: str = "roboledger",
  operation_id: str | None = None,
  org_name: str | None = None,
  inviter_name: str | None = None,
  passkey_name: str | None = None,
  new_email: str | None = None,
) -> dict:
  """Build the Dagster run_config for send_email_job.

  Args:
    email_type: email_verification, password_reset, welcome, or org_invitation
    token: Raw verification/reset/invitation token (required for all but
      welcome). Parked in Valkey for CacheDefaults.EMAIL_TOKEN_REF_TTL; the
      run config carries only an opaque reference to it.
    app: App identifier (roboledger, roboinvestor, robosystems)
    operation_id: SSE operation ID for progress tracking
    org_name: Organization name (required for org_invitation)
  """
  from robosystems.config import env

  config = {
    "email_type": email_type,
    "to_email": to_email,
    "user_name": user_name,
    "app": app,
  }

  if token:
    config["token_ref"] = _stash_email_token(token)

  if operation_id:
    config["operation_id"] = operation_id

  if org_name:
    config["org_name"] = org_name

  if inviter_name:
    config["inviter_name"] = inviter_name

  if passkey_name:
    config["passkey_name"] = passkey_name

  if new_email:
    config["new_email"] = new_email

  run_config: dict = {
    "ops": {
      "send_email_op": {"config": config},
    },
  }

  # In local development, use in_process executor
  if env.ENVIRONMENT == "dev":
    run_config["execution"] = {"config": {"in_process": {}}}

  return run_config
