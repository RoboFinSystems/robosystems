"""A comparison made under a shadow close is read back under it too, on real
Postgres: the drafts a shadow close never posts must not make it look stale."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from robosystems.models.api.extensions.reconciliations import (
  RecordStatementBalanceRequest,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  record_statement_balance,
)
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
)

from .conftest import GRAPH_ID, classified_account, entry

pytestmark = pytest.mark.unit


def test_a_shadow_comparison_stays_current_beside_a_draft(ext_session):
  session = ext_session
  session.info["graph_id"] = GRAPH_ID
  cash = classified_account(session, "Checking", "asset")
  equity = classified_account(
    session, "Members Equity", "equity", balance_type="credit"
  )
  entry(session, date(2026, 9, 5), cash, equity, 100_000)
  # A draft the shadow close will keep as an expectation, never post.
  entry(session, date(2026, 9, 20), cash, equity, 20_000, status="draft")
  session.commit()

  with patch(
    "robosystems.operations.roboledger.fiscal_calendar.qb_writeback.shadow_ledger",
    return_value=True,
  ):
    recorded = record_statement_balance(
      session,
      RecordStatementBalanceRequest(
        element_id=cash, as_of=date(2026, 9, 30), balance=1_000.00
      ),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
    session.commit()
    (rec,) = list_reconciliations(session, "2026-09").reconciliations

  assert (recorded.status, recorded.ledger_balance) == ("reconciled", 1_000.00)
  assert rec.status == "reconciled"
