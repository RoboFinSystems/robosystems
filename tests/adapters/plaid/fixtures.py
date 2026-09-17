"""A small synthetic Plaid Item — the shapes the sandbox returns, no real business.

One checking account, one savings account, one card and a loan the feed
leaves out. Transactions exercise every branch of the transform: a merchant
purchase, a payout, a bank fee, interest, a loan payment, a transfer pair
between checking and savings two days apart, a transfer with no second leg,
a pending row, a row on the loan and one before the backfill start.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

INSTITUTION = "Harborline Bank"
ITEM_ID = "item_harborline_1"
CHECKING_ID = "acc_checking"
SAVINGS_ID = "acc_savings"
CARD_ID = "acc_card"
LOAN_ID = "acc_loan"


def account(account_id: str, name: str, mask: str, type_: str, subtype: str) -> dict:
  return {
    "account_id": account_id,
    "name": name,
    "official_name": None,
    "mask": mask,
    "type": type_,
    "subtype": subtype,
    "balances": {"current": 100.0},
  }


def txn(
  transaction_id: str,
  account_id: str,
  amount: float,
  day: str,
  *,
  name: str,
  primary: str,
  detailed: str,
  merchant: str | None = None,
  entity_id: str | None = None,
  pending: bool = False,
) -> dict[str, Any]:
  return {
    "transaction_id": transaction_id,
    "account_id": account_id,
    "amount": amount,
    "iso_currency_code": "USD",
    "unofficial_currency_code": None,
    "date": day,
    "authorized_date": day,
    "name": name,
    "merchant_name": merchant,
    "merchant_entity_id": entity_id,
    "pending": pending,
    "pending_transaction_id": None,
    "payment_channel": "online",
    "transaction_code": None,
    "personal_finance_category": {
      "primary": primary,
      "detailed": detailed,
      "confidence_level": "HIGH",
    },
    "counterparties": (
      [{"name": merchant, "entity_id": entity_id, "type": "merchant"}]
      if merchant
      else []
    ),
  }


def accounts() -> list[dict[str, Any]]:
  return deepcopy(
    [
      account(CHECKING_ID, "Business Checking", "1234", "depository", "checking"),
      account(SAVINGS_ID, "Harborline Savings", "5678", "depository", "savings"),
      account(CARD_ID, "Business Card", "9012", "credit", "credit card"),
      account(LOAN_ID, "Equipment Loan", "3456", "loan", "commercial"),
    ]
  )


def transactions() -> list[dict[str, Any]]:
  return deepcopy(
    [
      txn(
        "t_coffee",
        CARD_ID,
        12.40,
        "2026-03-14",
        name="HARBOR COFFEE CO 0042",
        primary="FOOD_AND_DRINK",
        detailed="FOOD_AND_DRINK_COFFEE",
        merchant="Harbor Coffee Co",
        entity_id="ent_coffee",
      ),
      txn(
        "t_payout",
        CHECKING_ID,
        -1250.00,
        "2026-03-15",
        name="NORTHWIND PAYMENTS PAYOUT",
        primary="INCOME",
        detailed="INCOME_OTHER",
        merchant="Northwind Payments",
        entity_id="ent_northwind",
      ),
      txn(
        "t_fee",
        CHECKING_ID,
        15.00,
        "2026-03-16",
        name="MONTHLY SERVICE FEE",
        primary="BANK_FEES",
        detailed="BANK_FEES_OTHER_BANK_FEES",
      ),
      txn(
        "t_interest",
        SAVINGS_ID,
        -4.22,
        "2026-03-31",
        name="INTEREST PAYMENT",
        primary="INCOME",
        detailed="INCOME_INTEREST_EARNED",
      ),
      txn(
        "t_loanpay",
        CHECKING_ID,
        300.00,
        "2026-03-20",
        name="EQUIPMENT LOAN PMT",
        primary="LOAN_PAYMENTS",
        detailed="LOAN_PAYMENTS_OTHER_PAYMENT",
      ),
      txn(
        "t_xfer_out",
        CHECKING_ID,
        500.00,
        "2026-03-17",
        name="ONLINE TRANSFER TO SAV 5678",
        primary="TRANSFER_OUT",
        detailed="TRANSFER_OUT_SAVINGS",
      ),
      txn(
        "t_xfer_in",
        SAVINGS_ID,
        -500.00,
        "2026-03-19",
        name="ONLINE TRANSFER FROM CHK 1234",
        primary="TRANSFER_IN",
        detailed="TRANSFER_IN_ACCOUNT_TRANSFER",
      ),
      txn(
        "t_owner_draw",
        CHECKING_ID,
        200.00,
        "2026-03-21",
        name="TRANSFER TO EXTERNAL ACCT",
        primary="TRANSFER_OUT",
        detailed="TRANSFER_OUT_ACCOUNT_TRANSFER",
      ),
      txn(
        "t_pending",
        CARD_ID,
        30.00,
        "2026-03-22",
        name="PENDING PURCHASE",
        primary="GENERAL_MERCHANDISE",
        detailed="GENERAL_MERCHANDISE_OFFICE_SUPPLIES",
        merchant="Pinecrest Office Supply",
        entity_id="ent_pinecrest",
        pending=True,
      ),
      txn(
        "t_loan_side",
        LOAN_ID,
        -300.00,
        "2026-03-20",
        name="PAYMENT RECEIVED",
        primary="LOAN_PAYMENTS",
        detailed="LOAN_PAYMENTS_OTHER_PAYMENT",
      ),
      txn(
        "t_old",
        CHECKING_ID,
        9.99,
        "2025-12-30",
        name="OLD SUBSCRIPTION",
        primary="GENERAL_SERVICES",
        detailed="GENERAL_SERVICES_OTHER_GENERAL_SERVICES",
        merchant="Quillstack",
        entity_id="ent_quill",
      ),
    ]
  )
