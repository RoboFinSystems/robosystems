"""Stripe webhook delivery against a real Postgres: redelivery, and what persists."""

import hashlib
import hmac
import json
import time
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


@pytest.mark.unit
def test_a_failed_commit_after_the_claim_does_not_leave_it_held():
  from robosystems.database import engine

  event_id = f"evt_test_{uuid.uuid4().hex[:12]}"
  real = engine.connect()

  class FirstCommitFails:
    def __init__(self):
      self.commits = 0

    def execute(self, *args, **kwargs):
      return real.execute(*args, **kwargs)

    def commit(self):
      self.commits += 1
      if self.commits == 1:
        raise RuntimeError("commit failed")
      real.commit()

    def invalidate(self):
      real.invalidate()

    def close(self):
      real.close()

  from sqlalchemy import create_engine
  from sqlalchemy.pool import NullPool

  fake_engine = MagicMock()
  fake_engine.connect.return_value = FirstCommitFails()
  # Unpooled, so the probe can never be the connection that took the lock.
  probe_engine = create_engine(engine.url, poolclass=NullPool)
  try:
    with patch.object(webhooks, "engine", fake_engine):
      with pytest.raises(RuntimeError):
        with webhooks._event_claim(event_id):
          pass
    with probe_engine.connect() as probe:
      assert probe.execute(
        text("SELECT pg_try_advisory_lock(hashtext(:key))"),
        {"key": f"stripe-webhook:{event_id}"},
      ).scalar()
  finally:
    probe_engine.dispose()


_WEBHOOK_SECRET = "whsec_test_payload_shape"

_PRICE = {
  "id": "price_test",
  "object": "price",
  "currency": "usd",
  "unit_amount": 2500,
  "unit_amount_decimal": "2500",
  "recurring": {"interval": "month"},
}

_DECIMAL_CARRYING_OBJECTS = {
  "customer.subscription.updated": {
    "id": "sub_test_shape",
    "object": "subscription",
    "status": "active",
    "customer": "cus_test_shape",
    "metadata": {},
    "items": {
      "object": "list",
      "has_more": False,
      "url": "/v1/subscription_items",
      "data": [
        {"id": "si_test", "object": "subscription_item", "price": _PRICE, "quantity": 1}
      ],
    },
  },
  "invoice.paid": {
    "id": "in_test_shape",
    "object": "invoice",
    "customer": "cus_test_shape",
    "amount_paid": 2500,
    "metadata": {},
    "lines": {
      "object": "list",
      "has_more": False,
      "url": "/v1/invoices/in_test_shape/lines",
      "data": [
        {
          "id": "il_test",
          "object": "line_item",
          "amount": 2500,
          "quantity_decimal": "1",
          "pricing": {"type": "price_details", "unit_amount_decimal": "2500"},
        }
      ],
    },
  },
  "price.updated": _PRICE,
}


def _signed_request(payload: str):
  timestamp = int(time.time())
  digest = hmac.new(
    _WEBHOOK_SECRET.encode(), f"{timestamp}.{payload}".encode(), hashlib.sha256
  ).hexdigest()
  request = MagicMock()
  request.body = AsyncMock(return_value=payload.encode())
  request.headers = {"stripe-signature": f"t={timestamp},v1={digest}"}
  request.client.host = "127.0.0.1"
  return request


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", sorted(_DECIMAL_CARRYING_OBJECTS))
async def test_a_verified_event_with_decimal_fields_is_recorded(
  event_type, monkeypatch
):
  """The real Stripe SDK parses decimal string fields into Decimal; the event
  the provider hands back must still persist into the JSONB audit row."""
  from robosystems.config import env
  from robosystems.models.core.billing.audit_log import BillingAuditLog

  monkeypatch.setattr(env, "STRIPE_WEBHOOK_SECRET", _WEBHOOK_SECRET)
  event_id = f"evt_test_{uuid.uuid4().hex[:12]}"
  payload = json.dumps(
    {
      "id": event_id,
      "object": "event",
      "type": event_type,
      "api_version": "2026-01-28.clover",
      "data": {"object": _DECIMAL_CARRYING_OBJECTS[event_type]},
    }
  )
  db = SessionFactory()
  try:
    with patch.multiple(
      "robosystems.dagster.jobs.billing",
      _handle_payment_succeeded=AsyncMock(),
      _handle_subscription_updated=AsyncMock(),
    ):
      await webhooks.handle_stripe_webhook(
        _signed_request(payload), db=db, _rate_limit=None
      )
    assert BillingAuditLog.is_webhook_processed(event_id, "stripe", db)
  finally:
    db.execute(
      text("DELETE FROM billing_audit_logs WHERE event_data->>'event_id' = :e"),
      {"e": event_id},
    )
    db.commit()
    db.close()
