"""deliver_email — the API sends a transactional email itself and keeps the
Dagster email job as the retry lane."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations import notifications
from robosystems.operations.notifications import deliver_email, send_email_by_type

JOB = "robosystems.middleware.sse.dagster_monitor.run_and_monitor_dagster_job"
BUILD = "robosystems.dagster.jobs.notifications.build_email_job_config"

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


def _ses(send_result: bool = True, from_address: str = "noreply@example.com"):
  ses = MagicMock()
  ses.from_address = from_address
  for name in (
    "send_verification_email",
    "send_password_reset_email",
    "send_welcome_email",
    "send_org_invitation_email",
    "send_passkey_enrolled_email",
    "send_email_changed_notice",
  ):
    setattr(ses, name, AsyncMock(return_value=send_result))
  return ses


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
  monkeypatch.setattr(notifications, "DIRECT_SEND_RETRY_DELAY_SECONDS", 0)


class TestSendEmailByType:
  async def test_every_type_reaches_its_sender(self):
    ses = _ses()
    expected = {
      "email_verification": ses.send_verification_email,
      "password_reset": ses.send_password_reset_email,
      "welcome": ses.send_welcome_email,
      "org_invitation": ses.send_org_invitation_email,
      "passkey_enrolled": ses.send_passkey_enrolled_email,
      "email_changed": ses.send_email_changed_notice,
    }
    assert set(expected) == set(notifications.EMAIL_SENDERS)

    with patch.object(notifications, "ses_service", ses):
      for email_type, sender in expected.items():
        assert await send_email_by_type(
          email_type, "u@example.com", "U", token="t", app="roboledger", org_name="Acme"
        )
        sender.assert_awaited_once()
        assert sender.await_args.kwargs["app"] == "roboledger"

  async def test_unknown_type_raises(self):
    with pytest.raises(ValueError, match="Unknown email type"):
      await send_email_by_type("payment_failed", "u@example.com", "U")

  @pytest.mark.parametrize(
    "email_type", ["email_verification", "password_reset", "org_invitation"]
  )
  async def test_token_types_require_a_token(self, email_type):
    with pytest.raises(ValueError, match="Token required"):
      await send_email_by_type(email_type, "u@example.com", "U", org_name="Acme")

  async def test_invitation_requires_an_org_name(self):
    with pytest.raises(ValueError, match="org_name required"):
      await send_email_by_type("org_invitation", "u@example.com", "U", token="t")


class TestDeliverEmail:
  async def test_sends_directly_without_the_job(self):
    ses = _ses()

    with patch.object(notifications, "ses_service", ses), patch(JOB) as job:
      assert await deliver_email(
        "email_verification", "u@example.com", "U", token="raw", app="robosystems"
      )

    ses.send_verification_email.assert_awaited_once()
    assert ses.send_verification_email.await_args.kwargs["token"] == "raw"
    job.assert_not_called()

  async def test_a_transient_failure_is_retried_in_place(self):
    ses = _ses()
    ses.send_welcome_email = AsyncMock(side_effect=[False, True])

    with patch.object(notifications, "ses_service", ses), patch(JOB) as job:
      assert await deliver_email("welcome", "u@example.com", "U")

    assert ses.send_welcome_email.await_count == 2
    job.assert_not_called()

  async def test_repeated_failure_queues_the_email_job(self):
    ses = _ses(send_result=False)
    run_config = {"ops": {"send_email_op": {"config": {}}}}

    with (
      patch.object(notifications, "ses_service", ses),
      patch(BUILD, return_value=run_config) as build,
      patch(JOB, new_callable=AsyncMock) as job,
    ):
      assert await deliver_email(
        "password_reset", "u@example.com", "U", token="raw", app="roboledger"
      )

    assert (
      ses.send_password_reset_email.await_count == notifications.DIRECT_SEND_ATTEMPTS
    )
    assert build.call_args.kwargs["email_type"] == "password_reset"
    assert build.call_args.kwargs["token"] == "raw"
    job.assert_awaited_once_with(
      job_name="send_email_job", operation_id=None, run_config=run_config
    )

  async def test_a_send_that_raises_still_reaches_the_job(self):
    ses = _ses()
    ses.send_welcome_email = AsyncMock(side_effect=RuntimeError("boom"))

    with (
      patch.object(notifications, "ses_service", ses),
      patch(BUILD, return_value={}),
      patch(JOB, new_callable=AsyncMock) as job,
    ):
      assert await deliver_email("welcome", "u@example.com", "U")

    job.assert_awaited_once()

  async def test_a_failed_job_is_reported_not_raised(self):
    ses = _ses(send_result=False)

    with (
      patch.object(notifications, "ses_service", ses),
      patch(BUILD, return_value={}),
      patch(JOB, new_callable=AsyncMock, side_effect=RuntimeError("run failed")),
    ):
      assert await deliver_email("welcome", "u@example.com", "U") is False

  async def test_a_caller_defect_is_not_queued(self):
    ses = _ses()

    with patch.object(notifications, "ses_service", ses), patch(JOB) as job:
      assert await deliver_email("email_verification", "u@example.com", "U") is False

    ses.send_verification_email.assert_not_awaited()
    job.assert_not_called()

  async def test_an_unconfigured_sender_is_not_queued(self):
    ses = _ses(from_address="")

    with patch.object(notifications, "ses_service", ses), patch(JOB) as job:
      assert await deliver_email("welcome", "u@example.com", "U") is False

    ses.send_welcome_email.assert_not_awaited()
    job.assert_not_called()

  async def test_a_sent_email_is_reported_as_an_asset_event(self):
    ses = _ses()

    with (
      patch.object(notifications, "ses_service", ses),
      patch(
        "robosystems.dagster.reporting.report_asset_materialization",
        new_callable=AsyncMock,
      ) as report,
    ):
      await deliver_email("welcome", "u@example.com", "U", app="roboinvestor")

    report.assert_awaited_once()
    assert report.await_args.kwargs["asset_key"] == notifications.EMAIL_SENT_ASSET_KEY
    assert report.await_args.kwargs["metadata"] == {
      "email_type": "welcome",
      "to_email": "u@example.com",
      "app": "roboinvestor",
    }
