"""Load: agents scoped to the connection, events through the kernel, re-runs."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.mercury.pipeline.load import (
  HINT_KEYS,
  _refresh_hints,
  load_feed,
)
from robosystems.adapters.mercury.pipeline.transform import ChartIndex
from tests.adapters.mercury.fixtures import (
  CARD_ID,
  CHECKING_ID,
  SAVINGS_ID,
  TREASURY_ID,
  raw_pull,
)

MODULE = "robosystems.adapters.mercury.pipeline.load"
ELEMENTS = {
  CHECKING_ID: "e_chk",
  SAVINGS_ID: "e_sav",
  TREASURY_ID: "e_tre",
  CARD_ID: "e_card",
}


class _Session:
  """A session whose ``execute`` answers the agent lookup first, then the
  existing-event lookups; ``add`` assigns ids to new agents."""

  def __init__(self, existing_agents=(), existing_events=()):
    self.calls = 0
    self._agents = list(existing_agents)
    self._events = list(existing_events)
    self.added = []
    self.flushes = 0
    self.nested = 0

  def execute(self, statement):
    self.calls += 1
    result = MagicMock()
    if self.calls == 1:
      result.all.return_value = self._agents
    else:
      result.scalars.return_value.all.return_value = self._events
    return result

  def add(self, obj):
    obj.id = f"agt_{len(self.added) + 1}"
    self.added.append(obj)

  def flush(self):
    self.flushes += 1

  @contextmanager
  def begin_nested(self):
    self.nested += 1
    yield


def _run(session, **kwargs):
  with patch(f"{MODULE}.create_event_block_in_session") as create:
    report = load_feed(
      session,
      graph_id="kg_test",
      connection_id="conn_1",
      created_by="usr_1",
      source="mercury",
      raw=raw_pull(),
      account_elements=ELEMENTS,
      chart=ChartIndex(),
      **kwargs,
    )
  return report, create


@pytest.mark.unit
class TestLoadFeed:
  def test_fresh_graph_creates_agents_and_events(self):
    session = _Session()
    report, create = _run(session)
    assert report.agents_created == 5  # Staples, Stripe, Notion, Mercury, Chase
    assert all(
      a.connection_id == "conn_1" and a.source == "mercury" for a in session.added
    )
    assert report.events_created == 9
    assert report.events_existing == 0
    assert create.call_count == 9
    body = create.call_args.args[1]
    assert body.source == "mercury"
    assert create.call_args.kwargs["graph_id"] == "kg_test"
    assert session.nested == 9
    assert report.earliest_occurred_at == "2026-03-14T15:04:05Z"

  def test_agent_ids_flow_into_events(self):
    session = _Session(existing_agents=[("cp_stripe", "agt_existing")])
    report, create = _run(session)
    bodies = {c.args[1].external_id: c.args[1] for c in create.call_args_list}
    assert bodies["mercury_txn_txn_stripe"].agent_id == "agt_existing"
    assert report.agents_created == 4

  def test_rerun_reports_existing_and_refreshes_captured_hints(self):
    stale = SimpleNamespace(
      external_id="mercury_txn_txn_office",
      status="captured",
      metadata_={"connection_id": "conn_1", "classification_source": "none"},
    )
    posted = SimpleNamespace(
      external_id="mercury_txn_txn_stripe",
      status="fulfilled",
      metadata_={"classification_source": "none"},
    )
    session = _Session(existing_events=[stale, posted])
    report, create = _run(session)
    assert report.events_updated == 1
    assert stale.metadata_["classification_source"] == "gl_allocation"
    assert stale.metadata_["suggested_account_name"] == "Office Supplies"
    assert stale.metadata_["connection_id"] == "conn_1"  # untouched keys survive
    assert report.events_existing == 1
    assert posted.metadata_ == {
      "classification_source": "none"
    }  # posted rows untouched
    assert report.events_created == 7
    assert create.call_count == 7

  def test_one_bad_row_does_not_stop_the_batch(self):
    session = _Session()
    with patch(f"{MODULE}.create_event_block_in_session") as create:
      create.side_effect = [ValueError("bad row")] + [MagicMock()] * 8
      report = load_feed(
        session,
        graph_id="kg_test",
        connection_id="conn_1",
        created_by="usr_1",
        source="mercury",
        raw=raw_pull(),
        account_elements=ELEMENTS,
        chart=ChartIndex(),
      )
    assert report.events_failed == 1
    assert report.events_created == 8
    assert report.errors and "bad row" in report.errors[0]

  def test_treasury_exclusion_flows_through(self):
    report, create = _run(_Session(), include_treasury=False)
    assert report.skipped["excluded_account"] == 1
    assert create.call_count == 8


@pytest.mark.unit
class TestRefreshHints:
  def test_no_change_is_false(self):
    event = SimpleNamespace(metadata_={"classification_source": "none"})
    assert _refresh_hints(event, {"classification_source": "none"}) is False

  def test_removed_hint_is_dropped(self):
    event = SimpleNamespace(metadata_={"suggested_account_name": "x", "keep": 1})
    assert _refresh_hints(event, {}) is True
    assert event.metadata_ == {"keep": 1}

  def test_hint_keys_are_the_mercury_signals(self):
    assert "gl_allocations" in HINT_KEYS and "connection_id" not in HINT_KEYS
