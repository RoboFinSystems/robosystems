"""Unit tests for the graph-side effects of a connection lifecycle event:
``sever_synced_chart`` (the cutover stamp) and ``purge_bank_feed`` (the
bank-feed deletion protocol)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from robosystems.operations.roboledger.commands.connections import (
  BANK_FEED_PAYLOAD_KEYS,
  SEVERABLE_SOURCES,
  purge_bank_feed,
  sever_synced_chart,
)


def _compiled(statement) -> str:
  return str(
    statement.compile(
      dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
  )


@pytest.mark.unit
class TestSeverSyncedChart:
  def test_only_quickbooks_is_severable(self) -> None:
    assert frozenset({"quickbooks"}) == SEVERABLE_SOURCES
    session = MagicMock()
    with pytest.raises(ValueError, match="cannot be severed"):
      sever_synced_chart(session, "conn_1", source="mercury")
    session.execute.assert_not_called()

  def test_stamps_only_this_connections_quickbooks_elements(self) -> None:
    session = MagicMock()
    session.execute.return_value.rowcount = 29

    stamped = sever_synced_chart(session, "conn_1")

    assert stamped == 29
    statement = session.execute.call_args.args[0]
    sql = _compiled(statement)
    assert sql.startswith("UPDATE elements SET")
    assert "source='native'" in sql
    assert "external_source=NULL" in sql
    assert "connection_id=NULL" in sql
    assert "external_id=NULL" in sql
    assert "elements.external_source = 'quickbooks'" in sql
    assert "elements.connection_id = 'conn_1'" in sql
    # The qname is deliberately kept — nothing else is touched.
    assert "qname" not in sql
    assert "taxonomy_id" not in sql

  def test_missing_rowcount_reads_as_zero(self) -> None:
    session = MagicMock()
    session.execute.return_value.rowcount = None
    assert sever_synced_chart(session, "conn_1") == 0


# ---------------------------------------------------------------------------
# purge_bank_feed — the disconnect half of a bank-feed connection
# ---------------------------------------------------------------------------


class _PurgeSession:
  """Answers the purge's reads in order: feed events, referenced agent ids,
  feed agents, linked elements. Records every statement it executes."""

  def __init__(self, events, referenced, agents, elements):
    self._reads = [events, referenced, agents, elements]
    self.statements = []
    self.flushes = 0

  def execute(self, statement):
    self.statements.append(statement)
    result = MagicMock()
    sql = _compiled(statement)
    if sql.startswith("DELETE"):
      return result
    rows = self._reads.pop(0)
    result.scalars.return_value.all.return_value = rows
    result.all.return_value = rows
    return result

  def flush(self):
    self.flushes += 1


def _event(
  id, status, metadata, agent_id=None, external_url="https://app.mercury.com/t"
):
  return SimpleNamespace(
    id=id,
    status=status,
    metadata_=metadata,
    agent_id=agent_id,
    external_url=external_url,
  )


@pytest.mark.unit
class TestPurgeBankFeed:
  def test_deletes_unposted_scrubs_posted_and_unlinks(self):
    captured = _event("evt_c", "captured", {"bank_description": "x"})
    classified = _event("evt_k", "classified", {"note": "n"})
    posted = _event(
      "evt_p",
      "fulfilled",
      {
        "connection_id": "conn_1",
        "classified_element_id": "elem_x",
        "counterparty_name": "Stripe",
        "gl_allocations": [{"gl_code_name": "500"}],
        "dashboard_link": "https://app.mercury.com/t",
      },
      agent_id="agt_keep",
    )
    clean = _event("evt_q", "committed", {"connection_id": "conn_1"}, external_url=None)
    agent_keep = SimpleNamespace(id="agt_keep")
    agent_drop = SimpleNamespace(id="agt_drop")
    created = SimpleNamespace(
      metadata_={"bank_feed": {"provider": "mercury", "account_id": "a1"}, "other": 1},
      external_source="mercury",
      external_id="a1",
      connection_id="conn_1",
    )
    linked = SimpleNamespace(
      metadata_={"bank_feed": {"provider": "mercury", "account_id": "a2"}},
      external_source="quickbooks",
      external_id="qb_9",
      connection_id="conn_qb",
    )
    session = _PurgeSession(
      [captured, classified, posted, clean],
      [("agt_keep",)],
      [agent_keep, agent_drop],
      [created, linked],
    )

    purged = purge_bank_feed(session, source="mercury", connection_id="conn_1")

    assert purged == {
      "events_deleted": 2,
      "events_scrubbed": 1,
      "agents_deleted": 1,
      "accounts_unlinked": 2,
    }
    # The posted event keeps its accounting keys and loses the payload ones.
    assert posted.metadata_ == {
      "connection_id": "conn_1",
      "classified_element_id": "elem_x",
    }
    assert posted.external_url is None
    assert clean.metadata_ == {"connection_id": "conn_1"}
    # Elements: the link goes; provenance goes only on the row the feed created.
    assert created.metadata_ == {"other": 1}
    assert created.external_source is None and created.external_id is None
    assert created.connection_id is None
    assert "bank_feed" not in linked.metadata_
    assert linked.external_source == "quickbooks" and linked.external_id == "qb_9"
    deletes = [
      _compiled(s) for s in session.statements if _compiled(s).startswith("DELETE")
    ]
    assert any("event_dimensions" in d for d in deletes)
    assert any(
      "FROM events" in d and "'evt_c'" in d and "'evt_k'" in d for d in deletes
    )
    assert any(
      "FROM agents" in d and "'agt_drop'" in d and "'agt_keep'" not in d
      for d in deletes
    )
    assert session.flushes == 2

  def test_nothing_to_purge(self):
    session = _PurgeSession([], [], [], [])
    assert purge_bank_feed(session, source="mercury", connection_id="conn_1") == {
      "events_deleted": 0,
      "events_scrubbed": 0,
      "agents_deleted": 0,
      "accounts_unlinked": 0,
    }
    assert not [s for s in session.statements if _compiled(s).startswith("DELETE")]

  def test_payload_keys_never_include_the_accounting_keys(self):
    for key in (
      "connection_id",
      "classified_element_id",
      "suggested_element_id",
      "amount",
    ):
      assert key not in BANK_FEED_PAYLOAD_KEYS
    assert "gl_allocations" in BANK_FEED_PAYLOAD_KEYS
