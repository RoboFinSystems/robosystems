"""The interpolation context a handler template reads from an event."""

from __future__ import annotations

import pytest

from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.template import build_event_context


@pytest.mark.unit
def test_a_signed_bank_amount_is_offered_as_its_magnitude():
  """Bank money-out is negative and the DSL has no abs(); a rule on an outgoing
  line posts ``amount_abs`` and never a negative amount."""
  event = Event(id="evt_1", event_type="bank_transaction", amount=-54_000)
  context = build_event_context(event)
  assert context["amount"] == -54_000
  assert context["amount_abs"] == 54_000


@pytest.mark.unit
def test_an_event_without_an_amount_offers_zero():
  context = build_event_context(Event(id="evt_2", event_type="note", amount=None))
  assert context["amount_abs"] == 0
