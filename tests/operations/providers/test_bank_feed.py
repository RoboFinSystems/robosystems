"""The obligations every bank-feed provider keeps: the consent record and the purge."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import ProgrammingError

from robosystems.operations.providers.bank_feed import (
  purge_bank_feed_connection,
  record_bank_feed_consent,
  record_bank_feed_purged,
)

AUDIT = "robosystems.security.audit_logger.SecurityAuditLogger.log_security_event"


@pytest.mark.unit
class TestPurge:
  def test_purges_and_commits_on_the_tenant_schema(self):
    ext = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = ext
    cm.__exit__.return_value = False
    with (
      patch("robosystems.db.extensions.extensions_session", return_value=cm),
      patch(
        "robosystems.operations.roboledger.commands.connections.purge_bank_feed",
        return_value={"events_deleted": 2},
      ) as purge,
    ):
      result = purge_bank_feed_connection(
        "kg_test", provider="plaid", connection_id="conn_1"
      )
    assert result == {"events_deleted": 2}
    purge.assert_called_once_with(ext, source="plaid", connection_id="conn_1")
    ext.commit.assert_called_once()

  def test_a_missing_schema_is_nothing_to_do(self):
    exc = ProgrammingError("stmt", {}, Exception("schema"))
    with (
      patch("robosystems.db.extensions.extensions_session", side_effect=exc),
      patch("robosystems.middleware.extensions.is_schema_missing", return_value=True),
    ):
      assert purge_bank_feed_connection(
        "kg_test", provider="plaid", connection_id="conn_1"
      ) == {"events_deleted": 0, "events_scrubbed": 0, "agents_deleted": 0}

  def test_other_errors_surface(self):
    exc = ProgrammingError("stmt", {}, Exception("other"))
    with (
      patch("robosystems.db.extensions.extensions_session", side_effect=exc),
      patch("robosystems.middleware.extensions.is_schema_missing", return_value=False),
    ):
      with pytest.raises(ProgrammingError):
        purge_bank_feed_connection("kg_test", provider="plaid", connection_id="conn_1")


@pytest.mark.unit
class TestAuditRecords:
  def test_consent_names_the_provider_and_institution(self):
    from robosystems.security.audit_logger import SecurityEventType

    with patch(AUDIT) as audit:
      record_bank_feed_consent(
        provider="plaid",
        environment="sandbox",
        graph_id="kg_test",
        connection_id="conn_1",
        user_id="usr_1",
        auth_mode="link",
        scope="transactions",
        institution="First Platypus Bank",
      )
    kwargs = audit.call_args.kwargs
    assert kwargs["event_type"] is SecurityEventType.BANK_FEED_CONSENT_GRANTED
    assert kwargs["endpoint"].endswith("/oauth/callback/plaid")
    details = kwargs["details"]
    assert details["provider"] == "plaid"
    assert details["institution"] == "First Platypus Bank"
    assert details["environment"] == "sandbox"

  def test_purge_record_carries_the_counts(self):
    from robosystems.security.audit_logger import SecurityEventType

    with patch(AUDIT) as audit:
      record_bank_feed_purged(
        provider="plaid",
        connection={"user_id": "usr_1"},
        graph_id="kg_test",
        connection_id="conn_1",
        auth_mode="link",
        purged={"events_deleted": 4},
      )
    kwargs = audit.call_args.kwargs
    assert kwargs["event_type"] is SecurityEventType.BANK_FEED_PURGED
    assert kwargs["details"]["events_deleted"] == 4
    assert kwargs["details"]["provider"] == "plaid"
