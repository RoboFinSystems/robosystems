"""Dagster jobs for notification operations (email, SMS, push)."""

from dagster import (
  Backoff,
  Config,
  OpExecutionContext,
  RetryPolicy,
  job,
  op,
)

from robosystems.logger import get_logger

logger = get_logger(__name__)


class SendEmailConfig(Config):
  """Configuration for sending an email."""

  email_type: str  # email_verification, password_reset, welcome
  to_email: str
  user_name: str
  token: str | None = None  # For verification/reset emails
  app: str = "roboledger"
  operation_id: str | None = None  # For SSE tracking


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


@op(
  retry_policy=RetryPolicy(
    max_retries=3,
    delay=5,
    backoff=Backoff.EXPONENTIAL,
  ),
  tags={"kind": "email", "category": "notification"},
)
def send_email_op(context: OpExecutionContext, config: SendEmailConfig) -> dict:
  """
  Send an email via SES with retry logic and tracking.

  This op handles all email types:
  - email_verification: Requires token
  - password_reset: Requires token
  - welcome: No token needed

  Results are logged and can be tracked via SSE if operation_id is provided.
  """
  import asyncio

  from robosystems.operations.aws.ses import ses_service

  context.log.info(
    f"Sending {config.email_type} email to {config.to_email} for app {config.app}"
  )

  # Run the async email send
  loop = asyncio.new_event_loop()
  try:
    if config.email_type == "email_verification":
      if not config.token:
        raise ValueError("Token required for email_verification")
      success = loop.run_until_complete(
        ses_service.send_verification_email(
          user_email=config.to_email,
          user_name=config.user_name,
          token=config.token,
          app=config.app,
        )
      )
    elif config.email_type == "password_reset":
      if not config.token:
        raise ValueError("Token required for password_reset")
      success = loop.run_until_complete(
        ses_service.send_password_reset_email(
          user_email=config.to_email,
          user_name=config.user_name,
          token=config.token,
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

  # Log result for observability
  if success:
    context.log.info(
      f"Successfully sent {config.email_type} email to {config.to_email}"
    )
  else:
    context.log.error(f"Failed to send {config.email_type} email to {config.to_email}")
    # Raise to trigger retry
    raise RuntimeError(f"Failed to send {config.email_type} email to {config.to_email}")

  # Update SSE if operation_id provided
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
    "dagster/max_runtime": 300,  # 5 minute max
    "category": "notification",
  },
)
def send_email_job():
  """
  Job for sending emails via SES.

  This job provides:
  - Retry logic with exponential backoff (3 retries)
  - Full observability in Dagster UI
  - Optional SSE progress tracking
  - Audit trail of all email sends

  Usage:
    from robosystems.middleware.sse import run_and_monitor_dagster_job, build_notification_job_config

    # Queue email send with SSE monitoring
    background_tasks.add_task(
      run_and_monitor_dagster_job,
      job_name="send_email_job",
      operation_id=operation_id,
      run_config=build_notification_job_config(
        "send_email_job",
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
) -> dict:
  """
  Build run_config for send_email_job.

  Args:
    email_type: Type of email (email_verification, password_reset, welcome)
    to_email: Recipient email address
    user_name: User's display name
    token: Verification/reset token (required for verification and reset emails)
    app: App identifier (roboledger, roboinvestor, robosystems)
    operation_id: Optional SSE operation ID for progress tracking

  Returns:
    run_config dictionary for Dagster
  """
  from robosystems.config import env

  config = {
    "email_type": email_type,
    "to_email": to_email,
    "user_name": user_name,
    "app": app,
  }

  if token:
    config["token"] = token

  if operation_id:
    config["operation_id"] = operation_id

  run_config: dict = {
    "ops": {
      "send_email_op": {"config": config},
    },
  }

  # In local development, use in_process executor
  if env.ENVIRONMENT == "dev":
    run_config["execution"] = {"config": {"in_process": {}}}

  return run_config
