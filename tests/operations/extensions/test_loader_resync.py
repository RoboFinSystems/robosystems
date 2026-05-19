"""Wave 1 re-ingest harness — `quickbooks-adapter.md` §4.6.0 W5.

Asserts the four documented re-sync fidelity invariants survive a second
loader run. Companion to the unit tests in `test_loader.py` that cover
single-call behavior; this file specifically targets the Wave 1
spec-vs-code gap scenarios verified on 2026-05-18.

Each scenario maps to a §2.5.1 gap:

- G1 — Voided events survive re-sync (`test_voided_event_survives_resync`).
- G2 — Incremental sync skips pre-sync DELETE
  (`test_incremental_sync_does_not_wipe_history`; full assertion lives in
  ``test_loader.test_load_incremental_skips_pre_sync_wipe``).
- G3 — Element UPSERT preserves `elem_*` ULIDs
  (`test_element_upsert_preserves_ulid`).
- G4 — Payload drift on committed event flagged without mutating live
  payload (covered by
  ``test_loader.test_capture_flags_drift_on_committed_event``).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest  # noqa: F401  (re-exported style consistency with sibling tests)


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


class TestG1VoidedEventSurvivesResync:
  """G1 — operator-voided events must persist through re-sync.

  Today's bug: the unconditional pre-sync DELETE wipes all events for
  the connection, including voided ones, so the rejected entry
  re-appears in the inbox. Wave 1 W2 scopes the DELETE to
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
    session.query.return_value.filter.return_value.all.return_value = [voided_event]

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


class TestG2IncrementalSkipsWipe:
  """G2 — incremental sync (no `full_rebuild`) must not wipe history.

  The bulk of this assertion lives at the `load()` level in
  ``test_loader.test_load_incremental_skips_pre_sync_wipe``. This class
  documents the contract via the `OLTPLoader.load()` docstring.
  """

  def test_load_signature_supports_since_date_and_full_rebuild(self):
    """The two Wave 1 contract params are part of `load()`'s signature."""
    import inspect

    from robosystems.operations.extensions.loader import OLTPLoader

    sig = inspect.signature(OLTPLoader.load)
    assert "since_date" in sig.parameters
    assert "full_rebuild" in sig.parameters
    # Both default to "safe / non-destructive" values.
    assert sig.parameters["full_rebuild"].default is False
    assert sig.parameters["since_date"].default is None


class TestCrossSourceMatcher:
  """Phase 4 §4.2 step 4 — cross-source matcher recognises QB rows
  that are round-trips of a previously write-backed RL-originated
  event. Stamps confirmation, skips INSERT + handler re-fire."""

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


class TestG3ElementUpsertPreservesUlid:
  """G3 — Element UPSERT keeps `elem_*` ULIDs stable across syncs.

  Today's bug: delete-then-insert regenerates every ULID on every sync,
  breaking downstream FK targets (user-curated Associations, IB Facts,
  cached element_id references). Wave 1 W4 switches the element load
  to lookup-then-update or insert, matching the Agent UPSERT pattern.
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
    session.query.return_value.filter.return_value.all.return_value = [existing_element]
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
