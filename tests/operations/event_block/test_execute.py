"""Tests for `execute_event_block`.

Covers the four observable behaviors of the write-back path:

1. Native-policy events fast-path through (no QB write, status unchanged).
2. QB-authoritative + QB accepts → `qb_external_id` stamped, status =
   'fulfilled', drafts promoted to posted.
3. QB-authoritative + QB rejects → `last_outbound_error` stamped,
   status = 'pending', drafts STAY draft.
4. Missing connection_id on event metadata → fast-path with status
   unchanged (event is RL-native; nothing to publish).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

COMMANDS_MODULE = "robosystems.operations.event_block.commands"


def _make_event(
  event_id: str = "evt_test_abc",
  status: str = "classified",
  metadata: dict | None = None,
) -> MagicMock:
  """A MagicMock Event row matching the shape the command reads."""
  evt = MagicMock()
  evt.id = event_id
  evt.status = status
  evt.metadata_ = metadata or {
    "connection_id": "conn_qb_1",
    "posting_date": "2026-05-19",
    "memo": "Phase 4 test",
    "line_items": [
      {"element_id": "elem_cash", "debit_amount": 10000, "credit_amount": 0},
      {"element_id": "elem_rev", "debit_amount": 0, "credit_amount": 10000},
    ],
    "status": "draft",
  }
  return evt


def _make_session(event: MagicMock) -> MagicMock:
  """Extensions session that returns `event` for any Event.query."""
  session = MagicMock()
  session.query.return_value.filter.return_value.first.return_value = event
  return session


@pytest.mark.unit
class TestExecuteEventBlockNativeFastPath:
  """Native-policy events (and events without connection_id) do not
  trigger a QB write — fast-path return with status unchanged."""

  def test_no_connection_id_in_metadata_fast_paths(self):
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block

    evt = _make_event(metadata={"posting_date": "2026-05-19"})  # no connection_id
    session = _make_session(evt)

    result = execute_event_block(
      session,
      ExecuteEventBlockRequest(event_id="evt_test_abc"),
      created_by="user_1",
    )

    assert result.status == "classified"
    assert result.qb_external_id is None
    assert result.qb_error is None
    # Status on the event is unchanged.
    assert evt.status == "classified"

  def test_native_policy_connection_fast_paths(self):
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block

    evt = _make_event()
    session = _make_session(evt)

    mock_connection = MagicMock()
    mock_connection.write_policy = "native"
    mock_connection.provider = "quickbooks"
    mock_platform_session = MagicMock()
    mock_platform_session.__enter__ = MagicMock(return_value=mock_platform_session)
    mock_platform_session.__exit__ = MagicMock(return_value=False)

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_platform_session),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=mock_connection,
      ),
    ):
      result = execute_event_block(
        session,
        ExecuteEventBlockRequest(event_id="evt_test_abc"),
        created_by="user_1",
      )

    assert result.status == "classified"
    assert result.qb_external_id is None
    assert evt.status == "classified"

  def test_event_not_found_raises(self):
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import (
      EventNotFoundError,
      execute_event_block,
    )

    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None

    with pytest.raises(EventNotFoundError):
      execute_event_block(
        session,
        ExecuteEventBlockRequest(event_id="evt_nonexistent"),
        created_by="user_1",
      )

  def test_missing_connection_skips_write(self):
    """If `connection_id` is in metadata but the row is gone
    (soft-deleted, race), fast-path return without raising."""
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block

    evt = _make_event()
    session = _make_session(evt)

    mock_platform_session = MagicMock()
    mock_platform_session.__enter__ = MagicMock(return_value=mock_platform_session)
    mock_platform_session.__exit__ = MagicMock(return_value=False)

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_platform_session),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=None,  # connection vanished
      ),
    ):
      result = execute_event_block(
        session,
        ExecuteEventBlockRequest(event_id="evt_test_abc"),
        created_by="user_1",
      )

    assert result.status == "classified"
    assert result.qb_external_id is None
    assert result.qb_error is None


@pytest.mark.unit
class TestExecuteEventBlockQBAuthoritativeAccept:
  """QB-authoritative connection + QB accepts the JE → fulfilled."""

  def test_qb_accept_path_stamps_metadata_and_promotes_drafts(self):
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block

    evt = _make_event()
    session = _make_session(evt)

    mock_connection = MagicMock()
    mock_connection.write_policy = "qb_authoritative"
    mock_connection.provider = "quickbooks"
    mock_connection.realm_id = "9341452700148642"
    mock_cred = MagicMock()
    mock_cred.get_credentials.return_value = {
      "refresh_token": "r",
      "access_token": "a",
    }
    mock_platform_session = MagicMock()
    mock_platform_session.__enter__ = MagicMock(return_value=mock_platform_session)
    mock_platform_session.__exit__ = MagicMock(return_value=False)

    mock_qb_client_instance = MagicMock()
    mock_qb_client_instance.client = MagicMock()

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_platform_session),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=mock_connection,
      ),
      patch(
        "robosystems.models.core.connection.connection_credentials.ConnectionCredentials.get_by_connection_id",
        return_value=mock_cred,
      ),
      patch(
        "robosystems.adapters.quickbooks.client.api.QBClient",
        return_value=mock_qb_client_instance,
      ),
      patch(
        "robosystems.operations.event_block.qb_writeback.post_event_to_qb",
        # Prefixed format matching QB importer's external_id convention
        # (qb_writeback returns these prefixed so the cross-source
        # matcher sees the same `JournalEntry_<id>` shape).
        return_value=["JournalEntry_99001"],
      ),
    ):
      result = execute_event_block(
        session,
        ExecuteEventBlockRequest(event_id="evt_test_abc"),
        created_by="user_1",
      )

    assert result.status == "fulfilled"
    assert result.qb_external_id == "JournalEntry_99001"
    assert result.qb_error is None
    # Metadata stamped with the prefixed form so the cross-source
    # matcher recognises round-tripped entries on the next sync.
    assert evt.metadata_["qb_external_id"] == "JournalEntry_99001"
    assert evt.metadata_["routed_via"]["connection_id"] == "conn_qb_1"
    assert evt.metadata_["routed_via"]["qb_request_id"] == "evt_test_abc"
    # Status on the event row.
    assert evt.status == "fulfilled"
    # Entry + Transaction promote-to-posted queries fired.
    assert session.query.return_value.filter.return_value.update.call_count >= 1

  def test_multi_entry_event_joins_qb_ids_in_metadata(self):
    """Nested-shape events (multi-entry) get a comma-joined
    qb_external_id stamp; the response surfaces the primary (first)
    qb_txn_id for the caller."""
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block

    evt = _make_event(
      metadata={
        "connection_id": "conn_qb_1",
        "entries": [
          {
            "posting_date": "2026-05-19",
            "memo": "Entry 1",
            "line_items": [
              {"element_id": "elem_cash", "debit_amount": 5000, "credit_amount": 0},
              {"element_id": "elem_rev", "debit_amount": 0, "credit_amount": 5000},
            ],
          },
          {
            "posting_date": "2026-05-19",
            "memo": "Entry 2",
            "line_items": [
              {"element_id": "elem_cash", "debit_amount": 3000, "credit_amount": 0},
              {"element_id": "elem_rev", "debit_amount": 0, "credit_amount": 3000},
            ],
          },
        ],
      }
    )
    session = _make_session(evt)

    mock_connection = MagicMock(
      write_policy="qb_authoritative", provider="quickbooks", realm_id="9341"
    )
    mock_cred = MagicMock()
    mock_cred.get_credentials.return_value = {"refresh_token": "r"}
    mock_platform_session = MagicMock()
    mock_platform_session.__enter__ = MagicMock(return_value=mock_platform_session)
    mock_platform_session.__exit__ = MagicMock(return_value=False)

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_platform_session),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=mock_connection,
      ),
      patch(
        "robosystems.models.core.connection.connection_credentials.ConnectionCredentials.get_by_connection_id",
        return_value=mock_cred,
      ),
      patch("robosystems.adapters.quickbooks.client.api.QBClient"),
      patch(
        "robosystems.operations.event_block.qb_writeback.post_event_to_qb",
        return_value=["JournalEntry_AA", "JournalEntry_BB"],
      ),
    ):
      result = execute_event_block(
        session,
        ExecuteEventBlockRequest(event_id="evt_test_abc"),
        created_by="user_1",
      )

    # Response surfaces the primary (first) qb_txn_id.
    assert result.qb_external_id == "JournalEntry_AA"
    # Metadata stores the comma-joined list for cross-source matching
    # against either round-tripped entry.
    assert evt.metadata_["qb_external_id"] == "JournalEntry_AA,JournalEntry_BB"


@pytest.mark.unit
class TestExecuteEventBlockQBReject:
  """QB-authoritative connection + QB rejects → pending status,
  drafts stay draft, last_outbound_error captured."""

  def test_invalid_amount_type_raises_qbwritebackerror(self):
    """Non-int amounts (e.g., float) raise QBWritebackError instead of
    silently truncating via int()."""
    from unittest.mock import MagicMock

    from robosystems.operations.event_block.qb_writeback import (
      QBWritebackError,
      _build_qb_line,
    )

    session = MagicMock()
    with pytest.raises(QBWritebackError) as exc_info:
      _build_qb_line(
        session,
        {
          "element_id": "elem_x",
          "debit_amount": 125.50,  # float — would silently truncate to 125 cents
          "credit_amount": 0,
        },
      )
    assert exc_info.value.payload["code"] == "invalid_amount_type"
    assert "debit_amount" in exc_info.value.payload["message"]

  def test_qb_reject_path_stamps_error_and_stays_pending(self):
    from robosystems.models.api.event_block import ExecuteEventBlockRequest
    from robosystems.operations.event_block.commands import execute_event_block
    from robosystems.operations.event_block.qb_writeback import QBWritebackError

    evt = _make_event()
    session = _make_session(evt)

    mock_connection = MagicMock(
      write_policy="qb_authoritative", provider="quickbooks", realm_id="9341"
    )
    mock_cred = MagicMock()
    mock_cred.get_credentials.return_value = {"refresh_token": "r"}
    mock_platform_session = MagicMock()
    mock_platform_session.__enter__ = MagicMock(return_value=mock_platform_session)
    mock_platform_session.__exit__ = MagicMock(return_value=False)

    rejection = {
      "code": "qb_validation_error",
      "message": "Closed period in QB",
      "qb_error_code": 5060,
      "qb_response_at": "2026-05-19T05:00:00",
    }

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_platform_session),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=mock_connection,
      ),
      patch(
        "robosystems.models.core.connection.connection_credentials.ConnectionCredentials.get_by_connection_id",
        return_value=mock_cred,
      ),
      patch("robosystems.adapters.quickbooks.client.api.QBClient"),
      patch(
        "robosystems.operations.event_block.qb_writeback.post_event_to_qb",
        side_effect=QBWritebackError(rejection),
      ),
    ):
      result = execute_event_block(
        session,
        ExecuteEventBlockRequest(event_id="evt_test_abc"),
        created_by="user_1",
      )

    assert result.status == "pending"
    assert result.qb_external_id is None
    assert result.qb_error == rejection
    # Live event state.
    assert evt.status == "pending"
    assert evt.metadata_["last_outbound_error"] == rejection
    # qb_external_id should NOT be stamped on rejection.
    assert "qb_external_id" not in evt.metadata_


@pytest.mark.unit
class TestSaveWithRetry:
  """`_save_with_retry` retries transient transport errors and wraps them
  as `QBWritebackError` rather than letting
  `requests.exceptions.RequestException` propagate uncaught (which
  would surface as a 500 instead of a typed write-back error)."""

  def test_network_error_retries_then_wraps_as_qbwritebackerror(self):
    """A persistent `requests.exceptions.ConnectionError` should be
    wrapped, not propagated bare. tenacity should attempt several
    retries before giving up via `_QB_RETRY`'s `stop_max_attempt_number=5`."""
    import requests

    from robosystems.operations.event_block.qb_writeback import (
      QBWritebackError,
      _save_with_retry,
    )

    call_count = {"n": 0}

    fake_je = MagicMock()

    def always_fail(*args, **kwargs):
      call_count["n"] += 1
      raise requests.exceptions.ConnectionError("Intuit unreachable")

    fake_je.save.side_effect = always_fail

    with pytest.raises(QBWritebackError) as exc_info:
      _save_with_retry(fake_je, MagicMock(), "req_abc", "evt_abc")

    # Wrapped as transport-error code.
    assert exc_info.value.payload["code"] == "qb_transport_error"
    # tenacity attempted multiple retries before giving up.
    assert call_count["n"] >= 2

  def test_quickbooks_exception_does_not_retry(self):
    """A `QuickbooksException` (validation, balance, closed-period) is
    NOT retryable — `_is_retryable_qb_error` returns False for it.
    Should wrap into QBWritebackError on the first attempt without
    retries."""
    from quickbooks.exceptions import QuickbooksException

    from robosystems.operations.event_block.qb_writeback import (
      QBWritebackError,
      _save_with_retry,
    )

    call_count = {"n": 0}
    fake_je = MagicMock()

    def reject(*args, **kwargs):
      call_count["n"] += 1
      raise QuickbooksException("validation error", error_code=2010)

    fake_je.save.side_effect = reject

    with pytest.raises(QBWritebackError) as exc_info:
      _save_with_retry(fake_je, MagicMock(), "req_abc", "evt_abc")

    assert exc_info.value.payload["code"] == "qb_validation_error"
    # Non-retryable — exactly one attempt.
    assert call_count["n"] == 1
