"""A small synthetic Mercury pull — the shapes the sandbox returns, no real org.

Two deposit accounts, one IO card, and enough transactions to exercise every
branch of the transform: a card purchase with a GL allocation, a payout with
a custom category, a card purchase with only a merchant bucket, an interest
payment, a wire fee, an internal transfer pair, the card autopay pair, an
external transfer, a pending row and a failed row.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

CHECKING_ID = "acct_checking_1"
SAVINGS_ID = "acct_savings_1"
TREASURY_ID = "acct_treasury_1"
CARD_ID = "acct_card_1"


def _txn(
  id: str,
  account_id: str,
  amount: float,
  kind: str,
  *,
  counterparty: str | None = None,
  counterparty_id: str | None = None,
  posted_at: str | None = "2026-03-14T15:04:05Z",
  status: str = "sent",
  **extra: Any,
) -> dict[str, Any]:
  txn: dict[str, Any] = {
    "id": id,
    "accountId": account_id,
    "amount": amount,
    "kind": kind,
    "status": status,
    "createdAt": "2026-03-14T12:00:00Z",
    "postedAt": posted_at,
    "counterpartyName": counterparty,
    "counterpartyId": counterparty_id,
    "bankDescription": extra.pop("bankDescription", None),
    "dashboardLink": f"https://app.mercury.com/transactions/{id}",
  }
  txn.update(extra)
  return txn


def raw_pull() -> dict[str, Any]:
  return deepcopy(
    {
      "pulled_at": "2026-04-01T00:00:00Z",
      "since": "2026-01-01",
      "accounts": [
        {
          "id": CHECKING_ID,
          "name": "Mercury Checking ••1234",
          "kind": "checking",
          "legalBusinessName": "Cascade Books LLC",
        },
        {"id": SAVINGS_ID, "name": "Mercury Savings ••5678", "kind": "savings"},
        {"id": TREASURY_ID, "name": "Mercury Treasury", "kind": "treasury"},
      ],
      "credit_accounts": [{"id": CARD_ID, "name": "Mercury IO ••9012"}],
      "transactions": [
        # 1. card purchase coded in Mercury's accounting tab (GL allocation)
        _txn(
          "txn_office",
          CARD_ID,
          -42.5,
          "creditCardTransaction",
          counterparty="Staples",
          counterparty_id="cp_staples",
          mercuryCategory="OfficeSupplies",
          glAllocations=[{"glCodeName": "500 - Office Supplies", "amount": -42.5}],
          merchant={"category": "Office Supply Stores", "categoryCode": "5943"},
          cardId="card_1",
        ),
        # 2. payout with a custom category
        _txn(
          "txn_stripe",
          CHECKING_ID,
          95.14,
          "incomingDomesticWire",
          counterparty="Stripe",
          counterparty_id="cp_stripe",
          categoryData={"name": "Revenue"},
          bankDescription="STRIPE PAYOUT 3F2A",
        ),
        # 3. card purchase with only the merchant bucket
        _txn(
          "txn_saas",
          CARD_ID,
          -20.0,
          "creditCardTransaction",
          counterparty="Notion Labs",
          counterparty_id="cp_notion",
          mercuryCategory="Software",
        ),
        # 4. interest
        _txn(
          "txn_interest",
          SAVINGS_ID,
          1.23,
          "interestPayment",
          counterparty="Mercury",
        ),
        # 5. a wire fee
        _txn(
          "txn_fee",
          CHECKING_ID,
          -15.0,
          "wireFee",
          counterparty="Mercury",
        ),
        # 6 + 7. internal transfer checking -> savings (two legs, one event)
        _txn(
          "txn_xfer_out",
          CHECKING_ID,
          -500.0,
          "internalTransfer",
          counterparty="Mercury Savings ••5678",
        ),
        _txn(
          "txn_xfer_in",
          SAVINGS_ID,
          500.0,
          "internalTransfer",
          counterparty="Mercury Checking ••1234",
        ),
        # 8 + 9. the IO card autopay pair
        _txn(
          "txn_autopay_out",
          CHECKING_ID,
          -62.5,
          "other",
          counterparty="Mercury Credit",
          posted_at="2026-03-20T00:00:00Z",
        ),
        _txn(
          "txn_autopay_in",
          CARD_ID,
          62.5,
          "other",
          counterparty="Mercury Checking ••1234",
          posted_at="2026-03-20T00:00:00Z",
        ),
        # 10. an external transfer (owner draw? loan? unknowable)
        _txn(
          "txn_external",
          CHECKING_ID,
          -1000.0,
          "externalTransfer",
          counterparty="Chase ••4321",
        ),
        # 11 + 12. never events
        _txn(
          "txn_pending",
          CARD_ID,
          -9.99,
          "creditCardTransaction",
          counterparty="Uber",
          status="pending",
          posted_at=None,
        ),
        _txn(
          "txn_failed",
          CARD_ID,
          -300.0,
          "creditCardTransaction",
          counterparty="Delta",
          status="failed",
        ),
        # 13. a treasury trade
        _txn(
          "txn_treasury",
          TREASURY_ID,
          3.5,
          "interestPayment",
          counterparty="Mercury",
        ),
      ],
    }
  )
