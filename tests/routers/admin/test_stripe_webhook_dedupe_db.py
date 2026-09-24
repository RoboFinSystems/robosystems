"""Concurrent redelivery of one Stripe event, against a real Postgres."""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from robosystems.database import SessionFactory
from robosystems.routers.admin import webhooks


def _request():
  request = MagicMock()
  request.body = AsyncMock(return_value=b"{}")
  request.headers = {"stripe-signature": "t=1,v1=sig"}
  request.client.host = "127.0.0.1"
  return request


def _event(event_id: str):
  return {
    "id": event_id,
    "type": "invoice.updated",
    "data": {"object": {"id": "in_test_dedupe", "status": "paid"}},
  }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_delivery_already_in_flight_is_refused_not_run_twice():
  event_id = f"evt_test_{uuid.uuid4().hex[:12]}"
  provider = MagicMock()
  provider.verify_webhook.return_value = _event(event_id)
  holder, db = SessionFactory(), SessionFactory()
  try:
    holder.execute(
      text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
      {"key": f"stripe-webhook:{event_id}"},
    )
    with (
      patch.object(webhooks, "get_payment_provider", return_value=provider),
      patch.object(webhooks, "_process_webhook_event", new=AsyncMock()) as process,
    ):
      with pytest.raises(HTTPException) as refused:
        await webhooks.handle_stripe_webhook(_request(), db=db, _rate_limit=None)
      assert refused.value.status_code == 409
      process.assert_not_awaited()

      holder.rollback()
      db.rollback()
      await webhooks.handle_stripe_webhook(_request(), db=db, _rate_limit=None)
      process.assert_awaited_once()
  finally:
    holder.close()
    db.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_the_claim_survives_a_commit_on_the_request_session():
  """Provisioning shares and commits the request session; that must not
  release the claim while the delivery is still being processed."""
  event_id = f"evt_test_{uuid.uuid4().hex[:12]}"
  provider = MagicMock()
  provider.verify_webhook.return_value = _event(event_id)
  db, probe = SessionFactory(), SessionFactory()
  seen: list[bool] = []

  async def process(**_kwargs):
    db.commit()
    seen.append(
      probe.execute(
        text("SELECT pg_try_advisory_lock(hashtext(:key))"),
        {"key": f"stripe-webhook:{event_id}"},
      ).scalar()
    )

  try:
    with (
      patch.object(webhooks, "get_payment_provider", return_value=provider),
      patch.object(webhooks, "_process_webhook_event", new=process),
    ):
      await webhooks.handle_stripe_webhook(_request(), db=db, _rate_limit=None)
    assert seen == [False]
  finally:
    probe.execute(text("SELECT pg_advisory_unlock_all()"))
    probe.close()
    db.close()
