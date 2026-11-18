"""Dagster jobs for notification operations (email, SMS, push)."""

import secrets
from typing import Any

from dagster import (
  Backoff,
  Config,
  OpExecutionContext,
  RetryPolicy,
  in_process_executor,
  job,
  op,
)

from robosystems.config.defaults import CacheDefaults
from robosystems.logger import get_logger

logger = get_logger(__name__)


class SendEmailConfig(Config):
  """Configuration for sending an email."""

  email_type: str  # a key of operations.notifications.EMAIL_SENDERS
  to_email: str
  user_name: str
  # Reference to the raw token parked in Valkey. The raw token must never enter
  # run config, which Dagster persists indefinitely and shows in its UI.
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
  """Send an email through the configured transport: SES, or SMTP when
  SMTP_HOST is set. The retry lane behind `operations.notifications.deliver_email`.

  A type that carries a token needs a `token_ref` that still resolves (the
  parked token expires after CacheDefaults.EMAIL_TOKEN_REF_TTL and is
  discarded once sent).

  A send failure raises, so the op's RetryPolicy retries it. When
  operation_id is set, the result is written to the SSE operation metadata.
  """
  import asyncio

  from robosystems.operations.notifications import send_email_by_type

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
    success = loop.run_until_complete(
      send_email_by_type(
        email_type=config.email_type,
        to_email=config.to_email,
        user_name=config.user_name,
        token=token,
        app=config.app,
        org_name=config.org_name,
        inviter_name=config.inviter_name,
        passkey_name=config.passkey_name,
        new_email=config.new_email,
      )
    )
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
  # One step: a subprocess per run only adds a second code load (~35 s in prod).
  executor_def=in_process_executor,
  tags={
    "dagster/priority": "1",
    "dagster/max_runtime": 300,  # 5 minute max
    "category": "notification",
  },
)
def send_email_job():
  """Send an email through the configured transport (SES, or SMTP when
  SMTP_HOST is set), with 3 retries at exponential backoff.

  Build run config with ``build_email_job_config``, which parks the raw token.
  """
  send_email_op()


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

  ``token`` (required except for welcome) is parked in Valkey; the run config
  carries only an opaque reference. ``org_name`` is required for org_invitation.
  """
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

  return {
    "ops": {
      "send_email_op": {"config": config},
    },
  }
