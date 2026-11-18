"""Transactional email delivery: one dispatch over the email types, sent from
the calling process, with the Dagster email job as the retry lane."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from robosystems.logger import get_logger
from robosystems.operations.aws.ses import ses_service

logger = get_logger(__name__)

EMAIL_SENT_ASSET_KEY = "transactional_email_sent"

# A send is one HTTPS call, so a second try covers a dropped connection. What
# outlasts that goes to the job, whose backoff is longer and whose failure pages.
DIRECT_SEND_ATTEMPTS = 2
DIRECT_SEND_RETRY_DELAY_SECONDS = 1.0

TOKEN_EMAIL_TYPES = frozenset(
  {"email_verification", "password_reset", "org_invitation"}
)


def _verification(f: dict[str, Any]) -> Awaitable[bool]:
  return ses_service.send_verification_email(
    user_email=f["to_email"], user_name=f["user_name"], token=f["token"], app=f["app"]
  )


def _password_reset(f: dict[str, Any]) -> Awaitable[bool]:
  return ses_service.send_password_reset_email(
    user_email=f["to_email"], user_name=f["user_name"], token=f["token"], app=f["app"]
  )


def _welcome(f: dict[str, Any]) -> Awaitable[bool]:
  return ses_service.send_welcome_email(
    user_email=f["to_email"], user_name=f["user_name"], app=f["app"]
  )


def _org_invitation(f: dict[str, Any]) -> Awaitable[bool]:
  if not f["org_name"]:
    raise ValueError("org_name required for org_invitation")
  return ses_service.send_org_invitation_email(
    user_email=f["to_email"],
    inviter_name=f["inviter_name"] or "A teammate",
    org_name=f["org_name"],
    token=f["token"],
    app=f["app"],
  )


def _passkey_enrolled(f: dict[str, Any]) -> Awaitable[bool]:
  return ses_service.send_passkey_enrolled_email(
    user_email=f["to_email"],
    user_name=f["user_name"],
    passkey_name=f["passkey_name"] or "a new passkey",
    app=f["app"],
  )


def _email_changed(f: dict[str, Any]) -> Awaitable[bool]:
  return ses_service.send_email_changed_notice(
    user_email=f["to_email"],
    user_name=f["user_name"],
    new_email=f["new_email"] or "a new address",
    app=f["app"],
  )


EMAIL_SENDERS: dict[str, Callable[[dict[str, Any]], Awaitable[bool]]] = {
  "email_verification": _verification,
  "password_reset": _password_reset,
  "welcome": _welcome,
  "org_invitation": _org_invitation,
  "passkey_enrolled": _passkey_enrolled,
  "email_changed": _email_changed,
}


async def send_email_by_type(
  email_type: str,
  to_email: str,
  user_name: str,
  token: str | None = None,
  app: str = "roboledger",
  org_name: str | None = None,
  inviter_name: str | None = None,
  passkey_name: str | None = None,
  new_email: str | None = None,
) -> bool:
  """Send one email of `email_type`; True when the transport accepted it.

  Raises ValueError for an unknown type or a missing required field: a caller
  defect, which no retry fixes.
  """
  sender = EMAIL_SENDERS.get(email_type)
  if sender is None:
    raise ValueError(f"Unknown email type: {email_type}")
  if email_type in TOKEN_EMAIL_TYPES and not token:
    raise ValueError(f"Token required for {email_type}")

  return await sender(
    {
      "to_email": to_email,
      "user_name": user_name,
      "token": token,
      "app": app,
      "org_name": org_name,
      "inviter_name": inviter_name,
      "passkey_name": passkey_name,
      "new_email": new_email,
    }
  )


async def deliver_email(
  email_type: str,
  to_email: str,
  user_name: str,
  token: str | None = None,
  app: str = "roboledger",
  org_name: str | None = None,
  inviter_name: str | None = None,
  passkey_name: str | None = None,
  new_email: str | None = None,
) -> bool:
  """Send a transactional email now; if that fails, queue it on the email job.

  Built to run as a background task: it never raises. True means the email
  was sent or the job that retries it finished.
  """
  fields: dict[str, Any] = {
    "email_type": email_type,
    "to_email": to_email,
    "user_name": user_name,
    "token": token,
    "app": app,
    "org_name": org_name,
    "inviter_name": inviter_name,
    "passkey_name": passkey_name,
    "new_email": new_email,
  }

  if not ses_service.from_address:
    logger.warning(
      f"Cannot send {email_type} email - EMAIL_FROM_ADDRESS not configured"
    )
    return False

  for attempt in range(1, DIRECT_SEND_ATTEMPTS + 1):
    try:
      sent = await send_email_by_type(**fields)
    except ValueError as e:
      logger.error(f"Cannot send {email_type} email: {e}")
      return False
    except Exception as e:
      logger.warning(f"{email_type} email send raised on attempt {attempt}: {e}")
      sent = False

    if sent:
      await _report_sent(email_type, to_email, app)
      return True
    if attempt < DIRECT_SEND_ATTEMPTS:
      await asyncio.sleep(DIRECT_SEND_RETRY_DELAY_SECONDS)

  logger.warning(
    f"{email_type} email to {to_email} failed {DIRECT_SEND_ATTEMPTS} direct sends; "
    "queueing send_email_job"
  )
  return await _queue_on_email_job(fields)


async def _queue_on_email_job(fields: dict[str, Any]) -> bool:
  from robosystems.dagster.jobs.notifications import build_email_job_config
  from robosystems.middleware.sse.dagster_monitor import run_and_monitor_dagster_job

  try:
    await run_and_monitor_dagster_job(
      job_name="send_email_job",
      operation_id=None,
      run_config=build_email_job_config(**fields),
    )
    return True
  except Exception as e:
    logger.error(
      f"{fields['email_type']} email to {fields['to_email']} was not delivered: {e}"
    )
    return False


async def _report_sent(email_type: str, to_email: str, app: str) -> None:
  from robosystems.dagster.reporting import report_asset_materialization

  await report_asset_materialization(
    asset_key=EMAIL_SENT_ASSET_KEY,
    description=f"{email_type} email sent",
    metadata={"email_type": email_type, "to_email": to_email, "app": app},
  )
