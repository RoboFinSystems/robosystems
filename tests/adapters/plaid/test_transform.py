"""Transform: accounts, counterparties, the sign flip, transfer legs, events."""

from __future__ import annotations

from datetime import date

import pytest

from robosystems.adapters.bank_feed.chart import ChartIndex, name_key
from robosystems.adapters.plaid.pipeline.transform import (
  Leg,
  account_display_name,
  bank_accounts,
  cents,
  counterparties,
  is_transfer_candidate,
  pair_legs,
  transform,
)
from robosystems.models.api.event_block import CreateEventBlockRequest
from tests.adapters.plaid.fixtures import (
  CARD_ID,
  CHECKING_ID,
  INSTITUTION,
  ITEM_ID,
  LOAN_ID,
  SAVINGS_ID,
  accounts,
  transactions,
  txn,
)

ELEMENTS = {CHECKING_ID: "e_chk", SAVINGS_ID: "e_sav", CARD_ID: "e_card"}


def _booked():
  return bank_accounts(accounts(), institution=INSTITUTION)


def _run(**kwargs):
  return transform(
    kwargs.pop("transactions", transactions()),
    accounts=_booked(),
    connection_id="conn_1",
    item_id=ITEM_ID,
    account_elements=ELEMENTS,
    **kwargs,
  )


def _by_external_id(result):
  return {event["external_id"]: event for event in result.events}


def _leg(tid, account, amount, day):
  return Leg(
    transaction_id=tid,
    account_id=account,
    account_name=account,
    element_id=f"e_{account}",
    amount=amount,
    day=day,
    description=None,
  )


@pytest.mark.unit
class TestAmountsAndAccounts:
  @pytest.mark.parametrize(
    ("plaid", "event"),
    [
      (12.40, -1240),
      (-1250, 125000),
      (9.94589, -995),
      (0.005, -1),
      ("37.474327", -3747),
      (None, 0),
    ],
  )
  def test_cents_flips_the_sign_and_rounds_half_up(self, plaid, event):
    assert cents(plaid) == event

  def test_only_cash_and_cards_are_booked(self):
    booked = {a.account_id: a for a in _booked()}
    assert set(booked) == {CHECKING_ID, SAVINGS_ID, CARD_ID}
    assert LOAN_ID not in booked
    assert (booked[CARD_ID].trait, booked[CARD_ID].balance_type) == (
      "liability",
      "credit",
    )
    assert booked[CARD_ID].is_credit and not booked[CHECKING_ID].is_credit
    assert booked[SAVINGS_ID].kind == "savings"
    assert booked[CHECKING_ID].institution == INSTITUTION

  def test_names_carry_the_institution_once_and_the_mask(self):
    booked = {a.account_id: a.name for a in _booked()}
    assert booked[CHECKING_ID] == "Harborline Bank Business Checking ••1234"
    assert booked[SAVINGS_ID] == "Harborline Savings ••5678"
    assert account_display_name({"name": "Card"}, "") == "Card"


@pytest.mark.unit
class TestCounterparties:
  def test_one_agent_per_merchant_typed_by_net_flow(self):
    agents = {
      a["external_id"]: a
      for a in counterparties(
        transactions(), account_ids={CHECKING_ID, SAVINGS_ID, CARD_ID}, source="plaid"
      )
    }
    # Pending, transfer, loan and nameless lines make no agent.
    assert set(agents) == {"ent_coffee", "ent_northwind", "ent_quill"}
    assert agents["ent_coffee"]["agent_type"] == "vendor"
    assert agents["ent_northwind"]["agent_type"] == "customer"
    assert agents["ent_coffee"]["metadata"] == {"plaid_transactions": 1}

  def test_government_payees_and_name_keys(self):
    txn = transactions()[0]
    txn.update(
      merchant_name="County Tax Office",
      merchant_entity_id=None,
      counterparties=[],
      personal_finance_category={
        "primary": "GOVERNMENT_AND_NON_PROFIT",
        "detailed": "GOVERNMENT_AND_NON_PROFIT_TAX_PAYMENT",
      },
    )
    (agent,) = counterparties([txn], account_ids={CARD_ID}, source="plaid")
    assert agent["agent_type"] == "government"
    assert agent["external_id"] == f"name:{name_key('County Tax Office')}"


@pytest.mark.unit
class TestTransferLegs:
  def test_pairs_opposite_legs_on_different_accounts_within_the_window(self):
    out_leg = _leg("a", "chk", -500, "2026-03-17")
    in_leg = _leg("b", "sav", 500, "2026-03-19")
    pairs, unpaired = pair_legs([out_leg, in_leg])
    assert pairs == [(out_leg, in_leg)] and unpaired == []

  def test_closest_date_wins(self):
    out_leg = _leg("a", "chk", -500, "2026-03-17")
    far = _leg("far", "sav", 500, "2026-03-20")
    near = _leg("near", "card", 500, "2026-03-17")
    pairs, unpaired = pair_legs([out_leg, far, near])
    assert pairs == [(out_leg, near)] and unpaired == [far]

  @pytest.mark.parametrize(
    "other",
    [
      _leg("b", "chk", 500, "2026-03-17"),  # same account
      _leg("b", "sav", 400, "2026-03-17"),  # different amount
      _leg("b", "sav", 500, "2026-03-21"),  # outside the window
      _leg("b", "sav", -500, "2026-03-17"),  # same direction
    ],
  )
  def test_legs_that_do_not_pair(self, other):
    pairs, unpaired = pair_legs([_leg("a", "chk", -500, "2026-03-17"), other])
    assert pairs == [] and len(unpaired) == 2


@pytest.mark.unit
class TestTransform:
  def test_skips_and_counts(self):
    result = _run(since=date(2026, 1, 1))
    assert dict(result.skipped) == {
      "pending": 1,
      "excluded_account": 1,
      "before_start": 1,
    }
    assert result.classification["transfer"] == 2
    assert len(result.events) == 7

  def test_every_payload_is_a_valid_event_block_request(self):
    for event in _run().events:
      body = CreateEventBlockRequest.model_validate(event)
      assert body.source == "plaid"
      assert body.metadata["connection_id"] == "conn_1"

  def test_the_transfer_pair_collapses_to_one_event(self):
    events = _by_external_id(_run())
    pair = events["plaid_xfer_t_xfer_in"]
    assert pair["event_type"] == "internal_transfer"
    assert pair["amount"] == 50000
    assert pair["occurred_at"] == "2026-03-19T00:00:00Z"
    assert pair["resource_element_id"] == "e_sav"
    meta = pair["metadata"]
    assert (meta["from_element_id"], meta["to_element_id"]) == ("e_chk", "e_sav")
    assert meta["legs"] == ["t_xfer_out", "t_xfer_in"]
    # Each leg keeps its own date, for the leg that outlives the pair.
    assert (meta["from_date"], meta["to_date"]) == ("2026-03-17", "2026-03-19")
    assert meta["item_id"] == ITEM_ID
    assert "plaid_txn_t_xfer_out" not in events

  @pytest.mark.parametrize(
    ("primary", "detailed", "candidate"),
    [
      ("TRANSFER_IN", "TRANSFER_IN_DEPOSIT", False),
      ("TRANSFER_OUT", "TRANSFER_OUT_WITHDRAWAL", False),
      ("TRANSFER_IN", "TRANSFER_IN_ACCOUNT_TRANSFER", True),
      ("TRANSFER_OUT", "TRANSFER_OUT_SAVINGS", True),
      ("LOAN_PAYMENTS", "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT", True),
      ("TRANSFER_OUT", None, True),
      ("FOOD_AND_DRINK", "FOOD_AND_DRINK_COFFEE", False),
    ],
  )
  def test_only_lines_with_a_visible_other_side_are_transfer_candidates(
    self, primary, detailed, candidate
  ):
    line = {"personal_finance_category": {"primary": primary, "detailed": detailed}}
    assert is_transfer_candidate(line) is candidate

  def test_a_deposit_and_a_withdrawal_never_pair(self):
    # A $2,000 check deposited to savings on Monday and $2,000 drawn from
    # checking at an ATM on Tuesday are two lines, not one transfer.
    lines = [
      txn(
        "t_dep",
        SAVINGS_ID,
        -2000.00,
        "2026-03-16",
        name="MOBILE CHECK DEPOSIT",
        primary="TRANSFER_IN",
        detailed="TRANSFER_IN_DEPOSIT",
      ),
      txn(
        "t_atm",
        CHECKING_ID,
        2000.00,
        "2026-03-17",
        name="ATM WITHDRAWAL",
        primary="TRANSFER_OUT",
        detailed="TRANSFER_OUT_WITHDRAWAL",
      ),
    ]
    events = _by_external_id(_run(transactions=lines))
    assert set(events) == {"plaid_txn_t_dep", "plaid_txn_t_atm"}
    for event in events.values():
      assert event["event_type"] == "bank_transaction"
      assert "transfer_candidate" not in event["metadata"]
      assert "suggested_account_name" not in event["metadata"]

  def test_a_transfer_with_no_second_leg_is_an_external_transfer_candidate(self):
    draw = _by_external_id(_run())["plaid_txn_t_owner_draw"]
    assert draw["event_type"] == "external_transfer"
    assert draw["amount"] == -20000
    assert draw["metadata"]["transfer_candidate"] is True
    assert draw["metadata"]["classification_source"] == "transfer"
    assert "suggested_account_name" not in draw["metadata"]
    assert "agent_id" not in draw

  def test_purchase_payout_fee_interest_and_loan_payment(self):
    chart = ChartIndex(
      by_name={
        name_key("Travel & Entertainment"): "e_te",
        name_key("Consulting Revenue"): "e_rev",
      }
    )
    events = _by_external_id(_run(chart=chart, agent_ids={"ent_coffee": "agt_coffee"}))
    coffee = events["plaid_txn_t_coffee"]
    assert (coffee["event_type"], coffee["event_category"]) == (
      "bank_transaction",
      "purchase",
    )
    assert coffee["amount"] == -1240
    assert coffee["description"] == "Harbor Coffee Co"
    assert coffee["agent_id"] == "agt_coffee"
    assert coffee["resource_element_id"] == "e_card"
    assert coffee["metadata"]["suggested_account_name"] == "Business meals"
    # The hint resolves through its template alias.
    assert coffee["metadata"]["suggested_element_id"] == "e_te"
    assert coffee["metadata"]["plaid_category_detailed"] == "FOOD_AND_DRINK_COFFEE"

    payout = events["plaid_txn_t_payout"]
    assert payout["event_category"] == "sales" and payout["amount"] == 125000
    assert payout["metadata"]["suggested_account_name"] == "Other income"
    assert "suggested_element_id" not in payout["metadata"]

    fee = events["plaid_txn_t_fee"]
    assert (fee["event_type"], fee["event_category"]) == ("bank_fee", "treasury")
    assert fee["metadata"]["suggested_account_key"] == "BankFees"

    interest = events["plaid_txn_t_interest"]
    assert interest["event_category"] == "treasury"
    assert interest["metadata"]["suggested_account_key"] == "InterestIncome"

    loan = events["plaid_txn_t_loanpay"]
    assert loan["event_category"] == "treasury"
    assert loan["metadata"]["classification_source"] == "plaid_category"
    assert "suggested_account_key" not in loan["metadata"]
    assert "counterparty_external_id" not in loan["metadata"]

  def test_existing_pairs_are_excluded_and_existing_singles_never_pair(self):
    excluded = _run(exclude=frozenset({"t_xfer_out", "t_xfer_in"}))
    assert excluded.skipped["already_paired"] == 2
    assert not any(e["event_type"] == "internal_transfer" for e in excluded.events)

    unpairable = _by_external_id(_run(unpairable=frozenset({"t_xfer_in"})))
    assert unpairable["plaid_txn_t_xfer_in"]["event_type"] == "external_transfer"
    assert unpairable["plaid_txn_t_xfer_out"]["event_type"] == "external_transfer"

  def test_events_sorted_by_occurred_at(self):
    stamps = [event["occurred_at"] for event in _run().events]
    assert stamps == sorted(stamps)


@pytest.mark.unit
class TestNatureNotDirection:
  """The category and the counterparty's type follow what the bank says the
  money was, never which way it moved."""

  def test_a_vendor_refund_is_a_purchase_and_the_vendor_stays_a_vendor(self):
    refund = txn(
      "t_refund",
      CARD_ID,
      -12.40,
      "2026-03-15",
      name="HARBOR COFFEE CO REFUND",
      primary="FOOD_AND_DRINK",
      detailed="FOOD_AND_DRINK_COFFEE",
      merchant="Harbor Coffee Co",
      entity_id="ent_coffee",
    )
    result = _run(transactions=[refund])
    event = result.events[0]
    assert event["amount"] == 1240 and event["event_category"] == "purchase"
    agents = counterparties([refund], account_ids={CARD_ID}, source="plaid")
    assert [a["agent_type"] for a in agents] == ["vendor"]

  def test_income_is_sales_and_an_unattributed_deposit_is_treasury(self):
    deposit = txn(
      "t_dep",
      CHECKING_ID,
      -900.00,
      "2026-03-15",
      name="MOBILE CHECK DEPOSIT",
      primary="TRANSFER_IN",
      detailed="TRANSFER_IN_DEPOSIT",
    )
    by_id = _by_external_id(_run(transactions=[*transactions(), deposit]))
    assert by_id["plaid_txn_t_payout"]["event_category"] == "sales"
    assert by_id["plaid_txn_t_dep"]["event_category"] == "treasury"

  @pytest.mark.parametrize(
    ("institution", "account", "expected"),
    [
      ("US Bank", "Business Checking", "US Bank Business Checking ••1234"),
      ("TD Bank", "TD Beyond Checking", "TD Beyond Checking ••1234"),
      ("Harborline Bank", "Harborline Savings", "Harborline Savings ••1234"),
    ],
  )
  def test_the_institution_matches_whole_words_only(
    self, institution, account, expected
  ):
    assert account_display_name({"name": account, "mask": "1234"}, institution) == (
      expected
    )
