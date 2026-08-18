"""Re-ingest harness for the QuickBooks loader.

Asserts the four re-sync fidelity invariants survive a second loader run.
Companion to the unit tests in `test_loader.py` that cover single-call
behavior; this file specifically targets the re-sync fidelity scenarios:

- Voided events survive re-sync (`test_voided_event_survives_resync`).
- Incremental sync skips the pre-sync DELETE
  (`test_incremental_sync_does_not_wipe_history`; full assertion lives in
  ``test_loader.test_load_incremental_skips_pre_sync_wipe``).
- Element UPSERT preserves `elem_*` ULIDs
  (`test_element_upsert_preserves_ulid`).
- Payload drift on a committed event is flagged without mutating the live
  payload (covered by
  ``test_loader.test_capture_flags_drift_on_committed_event``).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch


def _stub_existing_lookup(session, rows):
  """Stub a `query(...).filter(...)[.with_for_update()].all()` load on a mock session.

  Named for its main subject, the loader's existing-**events** lookup, which
  takes an ordered row lock so a concurrent inbox approval cannot commit
  between that read and the handler dispatch below it — but the `MagicMock` chain is not
  type-differentiated, so the same helper serves the Element and Agent loads
  in this file. Both the locked and unlocked chains are stubbed, so a test can
  never accidentally assert against whichever one production stopped using.
  """
  filtered = session.query.return_value.filter.return_value
  filtered.all.return_value = rows
  # The events load is `.filter(...).order_by(id).with_for_update().all()`;
  # self-returning links keep this stub independent of the chain's order.
  filtered.order_by.return_value = filtered
  filtered.with_for_update.return_value = filtered


def _dbt_data_single_je() -> dict:
  """One balanced QB JournalEntry, two line items."""
  return {
    "transactions": [
      {
        "external_id": "JE_500",
        "external_source": "quickbooks",
        "number": "500",
        "type": "JournalEntry",
        "amount": 7500,
        "currency": "USD",
        "date": date(2026, 3, 1),
        "merchant_name": None,
        "reference_number": "REF-500",
        "description": "March consulting fee",
        "source_id": "JE_500",
        "status": "posted",
      }
    ],
    "entries": [
      {
        "external_id": "JE_500",
        "external_transaction_id": "JE_500",
        "external_source": "quickbooks",
        "number": "500",
        "type": "standard",
        "posting_date": date(2026, 3, 1),
        "memo": "March consulting fee",
        "status": "posted",
      }
    ],
    "line_items": [
      {
        "entry_external_id": "JE_500",
        "element_external_id": "elem_1000",
        "debit_amount": 7500,
        "credit_amount": 0,
        "description": "Cash received",
        "line_order": 1,
      },
      {
        "entry_external_id": "JE_500",
        "element_external_id": "elem_4000",
        "debit_amount": 0,
        "credit_amount": 7500,
        "description": "Consulting revenue",
        "line_order": 2,
      },
    ],
  }


class TestVoidedEventSurvivesResync:
  """Operator-voided events must persist through re-sync.

  Failure mode this guards against: an unconditional pre-sync DELETE
  would wipe all events for the connection, including voided ones, so the
  rejected entry would re-appear in the inbox. The DELETE is scoped to
  ``captured``/``classified`` only.
  """

  def test_voided_event_skipped_in_capture(self):
    """A voided event is neither updated nor drift-flagged.

    `_capture_transactions_as_events` finds the row via the
    `existing` lookup, recognizes `status='voided'` falls outside both
    the UPSERT branch (`captured/classified`) and the drift branch
    (`committed/fulfilled`), and leaves it untouched.
    """
    from robosystems.operations.extensions.loader import OLTPLoader

    voided_event = MagicMock()
    voided_event.external_id = "JE_500"
    voided_event.status = "voided"
    voided_event.id = "evt_voided"
    original_meta = {"status": "posted", "frozen": "by-user-rejection"}
    voided_event.metadata_ = dict(original_meta)
    voided_event.payload_drift = False

    session = MagicMock()
    _stub_existing_lookup(session, [voided_event])

    loader = OLTPLoader()
    result = loader._capture_transactions_as_events(
      session,
      _dbt_data_single_je(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Voided row falls through both branches — neither updated nor flagged.
    assert result.inserted == 0
    assert result.updated == 0
    assert result.drift_detected == 0
    # Payload + flag untouched.
    assert voided_event.metadata_ == original_meta
    assert voided_event.payload_drift is False
    session.add_all.assert_not_called()


class TestIncrementalSkipsWipe:
  """Incremental sync (no `full_rebuild`) must not wipe history.

  The bulk of this assertion lives at the `load()` level in
  ``test_loader.test_load_incremental_skips_pre_sync_wipe``. This class
  documents the contract via the `OLTPLoader.load()` docstring.
  """

  def test_load_signature_supports_since_date_and_full_rebuild(self):
    """The two contract params are part of `load()`'s signature."""
    import inspect

    from robosystems.operations.extensions.loader import OLTPLoader

    sig = inspect.signature(OLTPLoader.load)
    assert "since_date" in sig.parameters
    assert "full_rebuild" in sig.parameters
    # Both default to "safe / non-destructive" values.
    assert sig.parameters["full_rebuild"].default is False
    assert sig.parameters["since_date"].default is None


class TestCrossSourceMatcher:
  """The cross-source matcher recognises QB rows that are round-trips of
  a previously write-backed RL-originated event. Stamps confirmation,
  skips INSERT + handler re-fire."""

  def _dbt_data(self):
    """One QB transaction whose external_id was already write-backed
    to QB from an RL-originated event."""
    return {
      "transactions": [
        {
          "external_id": "QB_TXN_99001",
          "external_source": "quickbooks",
          "number": "1",
          "type": "JournalEntry",
          "amount": 10000,
          "currency": "USD",
          "date": __import__("datetime").date(2026, 5, 19),
          "source_id": "QB_TXN_99001",
        }
      ],
      "entries": [],
      "line_items": [],
    }

  def test_match_skips_insert_and_stamps_confirmation(self):
    from unittest.mock import MagicMock

    from robosystems.operations.extensions.loader import OLTPLoader

    # Pre-existing RL event that wrote-back to QB; matches the incoming
    # external_id via metadata.qb_external_id.
    rl_event = MagicMock()
    rl_event.id = "evt_rl_origin"
    rl_event.source = "manual"
    rl_event.external_id = "evt_rl_origin"  # local UUID
    rl_event.metadata_ = {"qb_external_id": "QB_TXN_99001"}

    # Two consecutive query chains:
    #  1. existing-lookup: session.query(Event).filter(...).all() → []
    #  2. cross-source:    session.query(Event).filter(...).filter(...).all() → [rl_event]
    existing_chain = MagicMock()
    existing_chain.filter.return_value.all.return_value = []
    cross_chain = MagicMock()
    cross_chain.filter.return_value.filter.return_value.all.return_value = [rl_event]

    session = MagicMock()
    session.query.side_effect = [existing_chain, cross_chain]

    loader = OLTPLoader()
    result = loader._capture_transactions_as_events(
      session,
      self._dbt_data(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # The cross-source matcher fired: confirmation stamped, no INSERT.
    assert result.cross_source_matched == 1
    assert result.inserted == 0
    assert "qb_sync_confirmed_at" in rl_event.metadata_
    assert rl_event.metadata_["qb_external_id"] == "QB_TXN_99001"  # unchanged

  def test_no_match_falls_through_to_normal_insert(self):
    """An incoming QB row with NO RL-originated counterpart goes
    through the normal INSERT path — cross_source_matched stays 0."""
    from unittest.mock import MagicMock

    from robosystems.operations.extensions.loader import OLTPLoader

    # Both query chains return empty.
    existing_chain = MagicMock()
    existing_chain.filter.return_value.all.return_value = []
    cross_chain = MagicMock()
    cross_chain.filter.return_value.filter.return_value.all.return_value = []

    session = MagicMock()
    session.query.side_effect = [existing_chain, cross_chain]

    loader = OLTPLoader()
    result = loader._capture_transactions_as_events(
      session,
      self._dbt_data(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert result.cross_source_matched == 0
    # Normal path fires — the fixture has no line_items so the entry
    # is dropped (dropped_empty_transactions=1 by the existing
    # min-2-lines hardening, not the matcher).
    assert result.dropped_empty_transactions == 1


class TestElementUpsertPreservesUlid:
  """Element UPSERT keeps `elem_*` ULIDs stable across syncs.

  Failure mode this guards against: delete-then-insert would regenerate
  every ULID on every sync, breaking downstream FK targets (user-curated
  Associations, IB Facts, cached element_id references). The element load
  uses lookup-then-update or insert, matching the Agent UPSERT pattern.
  """

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_existing_element_keeps_ulid_on_resync(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
  ):
    """An element that already exists in the tenant DB gets its `elem_*`
    ULID preserved when the loader sees the same external_id again."""
    # Local import to avoid drag-in costs at module import time
    from robosystems.models.extensions import Element
    from robosystems.operations.extensions.loader import OLTPLoader
    from tests.operations.extensions.test_loader import _make_duckdb_mock

    existing_element = MagicMock(spec=Element)
    existing_element.id = "elem_STABLE_ULID_123"
    existing_element.external_id = "qb_acct_42"
    existing_element.external_source = "quickbooks"
    existing_element.connection_id = "conn_1"
    # SQLAlchemy session.commit() / refresh() are MagicMocks so just
    # let the mutations happen — we assert the SQLAlchemy InstrumentedAttr
    # accesses by name below.

    dbt_data = {
      "elements": [
        {
          "external_id": "qb_acct_42",
          "external_source": "quickbooks",
          "code": "1000",
          "name": "Cash",
          "description": "Cash and equivalents",
          "balance_type": "debit",
          "external_parent_id": None,
          "depth": 0,
          "path": "",
          "currency": "USD",
          "is_active": True,
          "is_placeholder": False,
          "metadata": "{}",
        }
      ],
    }
    mock_duckdb_connect.return_value = _make_duckdb_mock(dbt_data)

    session = MagicMock()
    # The element-lookup query returns the existing row.
    _stub_existing_lookup(session, [existing_element])
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_1",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_1",
      # Incremental — the UPSERT path is the only thing exercised.
    )

    # UPSERT branch fired — fields were mutated on the existing row.
    assert existing_element.code == "1000"
    assert existing_element.name == "Cash"
    assert existing_element.balance_type == "debit"
    # ULID stays put — id was never reassigned.
    assert existing_element.id == "elem_STABLE_ULID_123"
    # add_all only called for NEW elements (none in this fixture).
    add_all_calls = [
      c
      for c in session.add_all.call_args_list
      if c[0][0]  # non-empty list
    ]
    assert add_all_calls == [], (
      "add_all should not be called when every incoming element matches an "
      "existing row — the UPSERT branch should mutate in place instead."
    )


class TestSyncTokenCapture:
  """SyncToken flows from the dbt transaction row into
  Event.metadata_['qb_sync_token'].

  This class asserts the capture path only — that the value is persisted
  on insert and on the captured/classified UPSERT branch, and that a
  SyncToken bump on a committed event (with no other payload change) does
  NOT trigger a false drift flag. The freshness-gate UPSERT logic is
  covered separately.
  """

  def _dbt_data_with_sync_token(self, sync_token: str | None) -> dict:
    """Single QB JournalEntry row tagged with the given SyncToken."""
    data = _dbt_data_single_je()
    data["transactions"][0]["sync_token"] = sync_token
    return data

  def test_new_event_persists_sync_token(self):
    """A first-sync event row with sync_token='3' lands as
    metadata_['qb_sync_token'] = '3' on the new Event."""
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    _stub_existing_lookup(session, [])

    loader = OLTPLoader()
    loader._capture_transactions_as_events(
      session,
      self._dbt_data_with_sync_token("3"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # The new Event was passed to session.add_all; pull it out and inspect.
    add_all_call = session.add_all.call_args_list[0]
    new_events = add_all_call[0][0]
    assert len(new_events) == 1
    assert new_events[0].metadata_["qb_sync_token"] == "3"

  def test_upsert_path_refreshes_sync_token(self):
    """An existing captured event sees its metadata_['qb_sync_token']
    refreshed when a re-sync arrives with a newer SyncToken value."""
    from robosystems.operations.extensions.loader import OLTPLoader

    captured = MagicMock()
    captured.external_id = "JE_500"
    captured.status = "captured"
    captured.id = "evt_captured"
    captured.metadata_ = {"qb_sync_token": "3", "status": "posted"}
    captured.payload_drift = False

    session = MagicMock()
    _stub_existing_lookup(session, [captured])

    loader = OLTPLoader()
    loader._capture_transactions_as_events(
      session,
      self._dbt_data_with_sync_token("5"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # The full-replacement UPSERT path stamped metadata_; the new SyncToken
    # carried through.
    assert captured.metadata_["qb_sync_token"] == "5"

  def test_sync_token_bump_alone_does_not_flag_drift(self):
    """A SyncToken bump on a committed event WITHOUT any other payload
    change must not trip the drift detector. Step 2's freshness gate
    will turn this into an idempotent no-op; for step 1 we only require
    that drift isn't false-fired."""
    from robosystems.operations.extensions.loader import OLTPLoader

    # Build the committed event's live metadata to match what the loader
    # will compute as `metadata_blob` from `_dbt_data_with_sync_token` —
    # everything identical except the sync_token field. Without the
    # qb_sync_token exclusion from the drift comparison, this would fire.
    committed = MagicMock()
    committed.external_id = "JE_500"
    committed.status = "committed"
    committed.id = "evt_committed"
    committed.payload_drift = False

    # First run the loader with sync_token='3' to capture what the
    # metadata_blob shape looks like at this code revision; reuse it
    # for the live payload so the diff is exactly the sync_token field.
    capture_session = MagicMock()
    _stub_existing_lookup(capture_session, [])
    OLTPLoader()._capture_transactions_as_events(
      capture_session,
      self._dbt_data_with_sync_token("3"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )
    initial_event = capture_session.add_all.call_args_list[0][0][0][0]
    # Strip dispatch_* fields — they get stamped by the failed auto-commit
    # in this test setup, but a real successfully-committed event wouldn't
    # carry them. The drift comparison must remain clean.
    committed.metadata_ = {
      k: v for k, v in initial_event.metadata_.items() if not k.startswith("dispatch_")
    }

    # Now re-sync with a bumped SyncToken; no other field changed.
    drift_session = MagicMock()
    _stub_existing_lookup(drift_session, [committed])

    result = OLTPLoader()._capture_transactions_as_events(
      drift_session,
      self._dbt_data_with_sync_token("4"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Drift NOT flagged — the SyncToken exclusion in the drift comparison
    # is doing its job.
    assert result.drift_detected == 0
    assert committed.payload_drift is False
    # Live payload untouched — a committed event's payload is immutable.
    assert "drift_payload" not in committed.metadata_

  def test_connection_id_change_alone_does_not_flag_drift(self):
    """A re-sync under a different connection id (reconnect minted a new
    one) must not flag drift on committed events — `connection_id` names
    the connection that synced the row, not upstream state. The stored
    value must refresh to the live connection, because write-back routing
    reads it off the event."""
    from robosystems.operations.extensions.loader import OLTPLoader

    committed = MagicMock()
    committed.external_id = "JE_500"
    committed.status = "committed"
    committed.id = "evt_committed"
    committed.payload_drift = False

    capture_session = MagicMock()
    _stub_existing_lookup(capture_session, [])
    OLTPLoader()._capture_transactions_as_events(
      capture_session,
      self._dbt_data_with_sync_token(None),
      source="quickbooks",
      connection_id="conn_old",
      created_by="user_1",
      now=datetime.now(UTC),
    )
    initial_event = capture_session.add_all.call_args_list[0][0][0][0]
    committed.metadata_ = {
      k: v for k, v in initial_event.metadata_.items() if not k.startswith("dispatch_")
    }
    assert committed.metadata_["connection_id"] == "conn_old"

    # Re-sync the identical payload under a new connection id.
    drift_session = MagicMock()
    _stub_existing_lookup(drift_session, [committed])
    result = OLTPLoader()._capture_transactions_as_events(
      drift_session,
      self._dbt_data_with_sync_token(None),
      source="quickbooks",
      connection_id="conn_new",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert result.drift_detected == 0
    assert committed.payload_drift is False
    assert "drift_payload" not in committed.metadata_
    # Routing bookkeeping refreshed to the live connection.
    assert committed.metadata_["connection_id"] == "conn_new"


class TestSyncTokenGate:
  """SyncToken freshness gate on the UPSERT path.

  The gate runs BEFORE the status branches:

  - incoming SyncToken older than live → skip stale (counter logged)
  - incoming SyncToken matches live → skip same-version (counter logged)
  - incoming SyncToken newer than live → proceed to status branch
  - either side NULL/non-numeric → proceed (no_info / backfill)
  """

  def _dbt_data_with_sync_token(self, sync_token: str | None) -> dict:
    data = _dbt_data_single_je()
    data["transactions"][0]["sync_token"] = sync_token
    return data

  def test_stale_sync_token_skips_upsert(self):
    """incoming SyncToken < existing → no UPSERT, counter increments."""
    from robosystems.operations.extensions.loader import OLTPLoader

    captured = MagicMock()
    captured.external_id = "JE_500"
    captured.status = "captured"
    captured.id = "evt_captured"
    captured.metadata_ = {"qb_sync_token": "7"}
    captured.payload_drift = False
    captured.event_type = "ORIGINAL_TYPE"

    session = MagicMock()
    _stub_existing_lookup(session, [captured])

    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._dbt_data_with_sync_token("3"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Stale skip — no update path fired, no drift, no insert.
    assert result.skipped_stale_sync_token == 1
    assert result.updated == 0
    assert result.drift_detected == 0
    # The captured event's fields were NOT touched by the UPSERT path.
    assert captured.event_type == "ORIGINAL_TYPE"

  def test_same_sync_token_skips_upsert(self):
    """incoming SyncToken == existing → no UPSERT, no work."""
    from robosystems.operations.extensions.loader import OLTPLoader

    captured = MagicMock()
    captured.external_id = "JE_500"
    captured.status = "captured"
    captured.id = "evt_captured"
    captured.metadata_ = {"qb_sync_token": "5"}
    captured.payload_drift = False
    captured.event_type = "ORIGINAL_TYPE"

    session = MagicMock()
    _stub_existing_lookup(session, [captured])

    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._dbt_data_with_sync_token("5"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert result.skipped_same_sync_token == 1
    assert result.updated == 0
    assert captured.event_type == "ORIGINAL_TYPE"

  def test_fresh_sync_token_proceeds_to_upsert(self):
    """incoming SyncToken > existing → status branch fires (captured path
    performs full payload replacement, including bumped SyncToken)."""
    from robosystems.operations.extensions.loader import OLTPLoader

    captured = MagicMock()
    captured.external_id = "JE_500"
    captured.status = "captured"
    captured.id = "evt_captured"
    captured.metadata_ = {"qb_sync_token": "3"}
    captured.payload_drift = False

    session = MagicMock()
    _stub_existing_lookup(session, [captured])

    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._dbt_data_with_sync_token("7"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Fresh — the captured/classified UPSERT branch ran, no skip counter.
    assert result.skipped_stale_sync_token == 0
    assert result.skipped_same_sync_token == 0
    assert result.updated == 1
    assert captured.metadata_["qb_sync_token"] == "7"

  def test_null_existing_token_proceeds_backfill(self):
    """A row with no SyncToken in metadata_ → 'no_info' decision,
    proceeds to UPSERT, SyncToken gets backfilled."""
    from robosystems.operations.extensions.loader import OLTPLoader

    captured = MagicMock()
    captured.external_id = "JE_500"
    captured.status = "captured"
    captured.id = "evt_captured"
    captured.metadata_ = {}  # no qb_sync_token key (backfill row)
    captured.payload_drift = False

    session = MagicMock()
    _stub_existing_lookup(session, [captured])

    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._dbt_data_with_sync_token("4"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert result.skipped_stale_sync_token == 0
    assert result.skipped_same_sync_token == 0
    assert result.updated == 1
    # Backfill: live event picks up the SyncToken on this sync.
    assert captured.metadata_["qb_sync_token"] == "4"

  def test_committed_event_with_fresh_token_advances_live_token(self):
    """Even when a committed event sees no business-payload drift, a
    bumped SyncToken still advances the live event's qb_sync_token so the
    next sync's gate comparison stays accurate."""
    from robosystems.operations.extensions.loader import OLTPLoader

    # First, build what a normal capture's metadata would look like for
    # SyncToken='3'.
    capture_session = MagicMock()
    _stub_existing_lookup(capture_session, [])
    OLTPLoader()._capture_transactions_as_events(
      capture_session,
      self._dbt_data_with_sync_token("3"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )
    initial_event = capture_session.add_all.call_args_list[0][0][0][0]
    live_meta = {
      k: v for k, v in initial_event.metadata_.items() if not k.startswith("dispatch_")
    }

    committed = MagicMock()
    committed.external_id = "JE_500"
    committed.status = "committed"
    committed.id = "evt_committed"
    committed.metadata_ = live_meta
    committed.payload_drift = False

    session = MagicMock()
    _stub_existing_lookup(session, [committed])

    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._dbt_data_with_sync_token("9"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Drift NOT flagged (only SyncToken differs), but live token advanced.
    assert result.drift_detected == 0
    assert committed.payload_drift is False
    assert committed.metadata_["qb_sync_token"] == "9"
    assert "drift_payload" not in committed.metadata_


class TestCompareSyncTokens:
  """Pure-function freshness comparison."""

  def test_decisions(self):
    from robosystems.operations.extensions.loader import _compare_sync_tokens

    assert _compare_sync_tokens(None, None) == "no_info"
    assert _compare_sync_tokens(None, "3") == "no_info"
    assert _compare_sync_tokens("3", None) == "no_info"
    assert _compare_sync_tokens("3", "3") == "same"
    assert _compare_sync_tokens("3", "5") == "fresh"
    assert _compare_sync_tokens("5", "3") == "stale"
    # Numeric comparison, not lexicographic — '10' > '9'.
    assert _compare_sync_tokens("9", "10") == "fresh"
    # Defensive: non-numeric input falls through to no_info.
    assert _compare_sync_tokens("abc", "5") == "no_info"
    assert _compare_sync_tokens("5", "abc") == "no_info"


class TestIdempotentReingest:
  """Idempotent re-ingest harness.

  Synthetic dbt-data fixtures hit ``_capture_transactions_as_events``
  directly (no live QB sandbox required for the test harness — the gate
  semantics are the contract being asserted). When a recorded-fixture set
  lands later, this class is the shape the recorded test would replace.
  """

  def _fixture(self, sync_token: str | None) -> dict:
    """Single-JE QB-shape dbt data with a given SyncToken."""
    data = _dbt_data_single_je()
    data["transactions"][0]["sync_token"] = sync_token
    return data

  def _make_existing_event(
    self,
    *,
    status: str,
    sync_token: str,
    payload_seed: dict | None = None,
  ):
    """Build a MagicMock standing in for an existing Event row.

    ``payload_seed`` controls the live metadata blob; defaults to a
    minimal-but-realistic shape including the SyncToken.
    """
    evt = MagicMock()
    evt.external_id = "JE_500"
    evt.id = "evt_existing"
    evt.status = status
    evt.payload_drift = False
    base_meta = {"qb_sync_token": sync_token, "status": "posted"}
    if payload_seed:
      base_meta.update(payload_seed)
    evt.metadata_ = base_meta
    return evt

  def test_scenario_2_resync_identical_is_noop(self):
    """Re-sync of an already-current row is a SyncToken-gate ``same``
    skip; nothing mutates."""
    from robosystems.operations.extensions.loader import OLTPLoader

    captured = self._make_existing_event(status="captured", sync_token="5")
    pre_meta = dict(captured.metadata_)
    session = MagicMock()
    _stub_existing_lookup(session, [captured])

    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._fixture("5"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert result.skipped_same_sync_token == 1
    assert result.updated == 0
    assert result.inserted == 0
    assert captured.metadata_ == pre_meta
    session.add_all.assert_not_called()

  def test_scenario_4_out_of_order_replay_skipped_stale(self):
    """Forged out-of-order CDC batch: live token is newer than incoming.
    Gate flags ``stale``, nothing mutates, no errors raised."""
    from robosystems.operations.extensions.loader import OLTPLoader

    captured = self._make_existing_event(status="captured", sync_token="12")
    pre_meta = dict(captured.metadata_)
    pre_event_type = "ORIGINAL"
    captured.event_type = pre_event_type

    session = MagicMock()
    _stub_existing_lookup(session, [captured])

    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._fixture("7"),  # older than live=12
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert result.skipped_stale_sync_token == 1
    # No mutation: metadata, fields, drift flag all untouched.
    assert captured.metadata_ == pre_meta
    assert captured.event_type == pre_event_type
    assert captured.payload_drift is False
    assert result.drift_detected == 0

  def test_scenario_5_committed_drift_flagged_live_payload_immutable(self):
    """A ``committed`` event whose QB-side payload changed gets
    ``payload_drift=True`` + ``drift_payload`` captured in metadata, but
    the live business payload is untouched.

    The bumped SyncToken also advances the live ``qb_sync_token`` so
    the next gate comparison reflects current state — bookkeeping
    metadata, not business payload.
    """
    from robosystems.operations.extensions.loader import OLTPLoader

    # Seed the live metadata to match what a captured + committed
    # event's blob would look like — same shape as `metadata_blob`
    # the loader will compute for the incoming fixture, except a
    # field that differs (qb_doc_number).
    captured_blob = self._make_existing_event(
      status="committed",
      sync_token="3",
      payload_seed={
        "qb_txn_type": "JournalEntry",
        "qb_doc_number": "ORIGINAL_DOC",  # will diff from incoming
        "qb_reference_number": "REF-500",
        "qb_merchant_name": None,
        "qb_due_date": None,
        "qb_category": None,
        "qb_source_class": "JournalEntry",
        "qb_agent_external_id": None,
        "qb_linked_txns": [],
        "connection_id": "conn_1",
        "entries": [],
      },
    )
    pre_business_payload = {
      k: v for k, v in captured_blob.metadata_.items() if k != "qb_sync_token"
    }

    session = MagicMock()
    _stub_existing_lookup(session, [captured_blob])

    # Bumped SyncToken + payload diff (loader will compute a different
    # qb_doc_number from the fixture's "number" field = "500").
    result = OLTPLoader()._capture_transactions_as_events(
      session,
      self._fixture("9"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Drift flagged; live token advanced; drift_payload captured.
    assert result.drift_detected == 1
    assert captured_blob.payload_drift is True
    assert captured_blob.metadata_["qb_sync_token"] == "9"
    assert "drift_payload" in captured_blob.metadata_
    assert "drift_detected_at" in captured_blob.metadata_
    # The live business payload's qb_doc_number is unchanged (live
    # remains ORIGINAL_DOC); the incoming version was stashed inside
    # drift_payload for the reconciliation queue.
    assert captured_blob.metadata_["qb_doc_number"] == "ORIGINAL_DOC"
    # drift_payload carries the incoming view.
    assert captured_blob.metadata_["drift_payload"]["qb_doc_number"] == "500"
    # Pre-existing business fields outside qb_doc_number stayed put.
    for key, value in pre_business_payload.items():
      if key == "qb_doc_number":
        continue
      assert captured_blob.metadata_[key] == value

  def test_two_consecutive_syncs_produce_same_state(self):
    """Running the same dbt data through
    ``_capture_transactions_as_events`` twice leaves the snapshot
    tuple ``(external_id, qb_sync_token, status, payload_drift)``
    unchanged on the second run. The SyncToken gate flags 'same' and
    the UPSERT path is a no-op.

    Note: ``dispatch_attempts`` (a bookkeeping counter on dispatch
    re-tries for captured-but-stuck events) IS expected to advance
    across re-syncs — that's the intentional inbox-retry mechanism
    that lets a previously-failed event commit once mappings improve.
    The idempotency snapshot is the tuple above, not the full metadata
    blob.
    """
    from robosystems.operations.extensions.loader import OLTPLoader

    # First sync: new event inserted with sync_token='4'.
    first_session = MagicMock()
    _stub_existing_lookup(first_session, [])

    loader = OLTPLoader()
    first_result = loader._capture_transactions_as_events(
      first_session,
      self._fixture("4"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )
    new_event = first_session.add_all.call_args_list[0][0][0][0]
    snapshot = {
      "external_id": new_event.external_id,
      "qb_sync_token": new_event.metadata_.get("qb_sync_token"),
      "status": new_event.status,
      "payload_drift": new_event.payload_drift,
    }

    # Second sync: same dbt row; gate flags 'same' and short-circuits.
    second_session = MagicMock()
    _stub_existing_lookup(second_session, [new_event])

    second_result = loader._capture_transactions_as_events(
      second_session,
      self._fixture("4"),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert first_result.inserted == 1
    assert second_result.inserted == 0
    assert second_result.updated == 0
    assert second_result.skipped_same_sync_token == 1
    assert second_result.drift_detected == 0
    # The snapshot tuple is unchanged across the re-sync.
    second_snapshot = {
      "external_id": new_event.external_id,
      "qb_sync_token": new_event.metadata_.get("qb_sync_token"),
      "status": new_event.status,
      "payload_drift": new_event.payload_drift,
    }
    assert second_snapshot == snapshot
