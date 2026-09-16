"""The shared load: the hint refresh and one-event capture in its own savepoint."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from robosystems.adapters.bank_feed.load import (
  LoadReport,
  capture_event,
  earliest_plausible,
  refresh_hints,
)

KEYS = ("suggested_account_name", "classification_source")
KERNEL = "robosystems.adapters.bank_feed.load.create_event_block_in_session"


class _Session:
  def __init__(self):
    self.nested = 0

  @contextmanager
  def begin_nested(self):
    self.nested += 1
    yield


def _payload(external_id="ext_1"):
  return {
    "event_type": "bank_transaction",
    "event_category": "purchase",
    "occurred_at": "2026-03-14T00:00:00Z",
    "source": "plaid",
    "external_id": external_id,
    "amount": -540,
  }


@pytest.mark.unit
class TestRefreshHints:
  def test_no_change_is_false(self):
    event = SimpleNamespace(metadata_={"classification_source": "none"})
    assert refresh_hints(event, {"classification_source": "none"}, KEYS) is False

  def test_removed_hint_is_dropped(self):
    event = SimpleNamespace(metadata_={"suggested_account_name": "x", "keep": 1})
    assert refresh_hints(event, {}, KEYS) is True
    assert event.metadata_ == {"keep": 1}

  def test_only_listed_keys_move(self):
    event = SimpleNamespace(metadata_={"note": "old"})
    assert refresh_hints(event, {"note": "new"}, KEYS) is False
    assert event.metadata_ == {"note": "old"}


@pytest.mark.unit
class TestCaptureEvent:
  def test_captures_in_a_savepoint(self):
    session, report = _Session(), LoadReport()
    with patch(KERNEL) as create:
      assert capture_event(
        session, _payload(), graph_id="kg_1", created_by="usr_1", report=report
      )
    assert session.nested == 1 and report.events_created == 1
    assert create.call_args.kwargs["graph_id"] == "kg_1"

  def test_a_failure_is_counted_not_raised(self):
    session, report = _Session(), LoadReport()
    with patch(KERNEL, side_effect=ValueError("bad row")):
      assert not capture_event(
        session, _payload(), graph_id="kg_1", created_by="usr_1", report=report
      )
    assert report.events_failed == 1
    assert report.errors == ["ext_1: bad row"]


@pytest.mark.unit
def test_earliest_plausible_skips_placeholder_years():
  events = [
    {"occurred_at": "0001-01-01T00:00:00Z"},
    {"occurred_at": "2026-03-14T00:00:00Z"},
    {"occurred_at": "2026-02-01T00:00:00Z"},
  ]
  assert earliest_plausible(events) == "2026-02-01T00:00:00Z"
  assert earliest_plausible([]) is None
