"""Transform: transactions → captured-event payloads with Tier-0 hints."""

from __future__ import annotations

import pytest

from robosystems.adapters.mercury.pipeline.transform import (
  ChartIndex,
  bank_accounts,
  counterparties,
  name_key,
  own_counterparty_names,
  transform,
)
from robosystems.models.api.event_block import CreateEventBlockRequest
from tests.adapters.mercury.fixtures import (
  CARD_ID,
  CHECKING_ID,
  SAVINGS_ID,
  TREASURY_ID,
  raw_pull,
)

ELEMENTS = {
  CHECKING_ID: "elem_checking",
  SAVINGS_ID: "elem_savings",
  TREASURY_ID: "elem_treasury",
  CARD_ID: "elem_card",
}


def _chart() -> ChartIndex:
  return ChartIndex(
    by_name={
      name_key("Office Supplies"): "elem_office",
      name_key("Revenue"): "elem_revenue",
      name_key("Software & subscriptions"): "elem_software",
      name_key("Interest income"): "elem_interest",
      name_key("Bank fees"): "elem_fees",
    },
    by_code={"500": "elem_office_by_code"},
  )


def _events():
  result = transform(
    raw_pull(),
    source="mercury",
    connection_id="conn_1",
    account_elements=ELEMENTS,
    chart=_chart(),
    agent_ids={"cp_stripe": "agt_stripe", "cp_staples": "agt_staples"},
  )
  return result, {event["external_id"]: event for event in result.events}


@pytest.mark.unit
class TestAccounts:
  def test_bank_accounts_include_the_card_as_a_liability(self):
    accounts = bank_accounts(raw_pull())
    assert [a.mercury_id for a in accounts] == [
      CHECKING_ID,
      SAVINGS_ID,
      TREASURY_ID,
      CARD_ID,
    ]
    card = accounts[-1]
    assert (card.kind, card.trait, card.balance_type) == (
      "credit",
      "liability",
      "credit",
    )
    assert accounts[0].legal_business_name == "Cascade Books LLC"

  def test_treasury_can_be_excluded(self):
    ids = [a.mercury_id for a in bank_accounts(raw_pull(), include_treasury=False)]
    assert TREASURY_ID not in ids and CHECKING_ID in ids

  def test_own_names_include_the_autopay_label(self):
    names = own_counterparty_names(bank_accounts(raw_pull()))
    assert "Mercury Credit" in names and "Mercury Checking ••1234" in names


@pytest.mark.unit
class TestCounterparties:
  def test_one_agent_per_third_party_typed_by_net_flow(self):
    accounts = bank_accounts(raw_pull())
    agents = {
      a["external_id"]: a
      for a in counterparties(raw_pull(), own_counterparty_names(accounts), "mercury")
    }
    assert agents["cp_stripe"]["agent_type"] == "customer"
    assert agents["cp_staples"]["agent_type"] == "vendor"
    assert agents["cp_notion"]["agent_type"] == "vendor"
    # Mercury itself (interest, fees) is 'other'; own accounts and transfers
    # never become agents; pending/failed rows are skipped.
    assert agents["name:mercury"]["agent_type"] == "other"
    assert "name:mercurysavings5678" not in agents
    assert "name:uber" not in agents and "name:delta" not in agents
    assert all(a["source"] == "mercury" for a in agents.values())


@pytest.mark.unit
class TestTransform:
  def test_counts(self):
    result, by_id = _events()
    assert result.skipped == {"pending": 1, "failed": 1}
    # 13 rows - 2 skipped - 2 pairs collapsed = 9 events
    assert len(result.events) == 9
    assert result.classification["transfer"] == 3  # two pairs + the external
    assert result.classification["gl_allocation"] == 1
    assert result.classification["custom_category"] == 1
    assert result.classification["mercury_category"] == 1
    assert result.resolved["resolved"] >= 4

  def test_every_payload_is_a_valid_event_block_request(self):
    result, _ = _events()
    for payload in result.events:
      request = CreateEventBlockRequest.model_validate(payload)
      assert request.apply_handlers is False
      assert request.source == "mercury"
      assert request.metadata["connection_id"] == "conn_1"
      assert request.resource_element_id in ELEMENTS.values()

  def test_gl_allocation_resolves_by_code_first(self):
    _, by_id = _events()
    event = by_id["mercury_txn_txn_office"]
    meta = event["metadata"]
    assert event["event_type"] == "bank_transaction"
    assert event["event_category"] == "purchase"
    assert event["event_action"] == "transfer"
    assert event["amount"] == -4250
    assert event["agent_id"] == "agt_staples"
    assert event["resource_element_id"] == "elem_card"
    assert meta["classification_source"] == "gl_allocation"
    assert meta["suggested_account_name"] == "Office Supplies"
    assert meta["suggested_element_id"] == "elem_office_by_code"
    assert meta["gl_allocations"][0]["gl_code_name"] == "500 - Office Supplies"
    assert meta["merchant_category_code"] == "5943"
    assert event["description"] == "Staples (card)"

  def test_custom_category_and_merchant_bucket(self):
    _, by_id = _events()
    stripe = by_id["mercury_txn_txn_stripe"]
    assert stripe["event_category"] == "sales"
    assert stripe["metadata"]["suggested_element_id"] == "elem_revenue"
    assert stripe["metadata"]["classification_source"] == "custom_category"
    assert stripe["description"] == "Stripe: STRIPE PAYOUT 3F2A"
    notion = by_id["mercury_txn_txn_saas"]
    assert notion["metadata"]["suggested_element_id"] == "elem_software"
    assert notion["metadata"]["classification_source"] == "mercury_category"

  def test_interest_and_fees_are_treasury_with_defaults(self):
    _, by_id = _events()
    interest = by_id["mercury_txn_txn_interest"]
    assert interest["event_category"] == "treasury"
    assert interest["metadata"]["suggested_element_id"] == "elem_interest"
    fee = by_id["mercury_txn_txn_fee"]
    assert fee["event_type"] == "bank_fee"
    assert fee["metadata"]["suggested_element_id"] == "elem_fees"
    assert "agent_id" not in fee  # Mercury is not a counterparty on its own fee

  def test_internal_transfer_pairs_collapse_to_one_event(self):
    _, by_id = _events()
    assert "mercury_txn_txn_xfer_out" not in by_id
    assert "mercury_txn_txn_xfer_in" not in by_id
    xfer = by_id["mercury_xfer_txn_xfer_in"]
    assert xfer["event_type"] == "internal_transfer"
    assert xfer["event_action"] == "move"
    assert xfer["amount"] == 50000
    assert xfer["resource_element_id"] == "elem_savings"
    assert xfer["metadata"]["from_element_id"] == "elem_checking"
    assert xfer["metadata"]["to_element_id"] == "elem_savings"
    assert sorted(xfer["metadata"]["legs"]) == ["txn_xfer_in", "txn_xfer_out"]
    assert (
      xfer["description"]
      == "Transfer Mercury Checking ••1234 to Mercury Savings ••5678"
    )

  def test_card_autopay_is_a_transfer_pair_too(self):
    _, by_id = _events()
    autopay = by_id["mercury_xfer_txn_autopay_in"]
    assert autopay["event_type"] == "internal_transfer"
    assert autopay["metadata"]["from_element_id"] == "elem_checking"
    assert autopay["metadata"]["to_element_id"] == "elem_card"

  def test_external_transfer_carries_no_suggestion(self):
    _, by_id = _events()
    external = by_id["mercury_txn_txn_external"]
    assert external["event_type"] == "external_transfer"
    assert "suggested_account_name" not in external["metadata"]
    assert external["metadata"]["classification_source"] == "transfer"

  def test_excluding_treasury_drops_its_rows(self):
    result = transform(
      raw_pull(),
      source="mercury",
      connection_id="conn_1",
      account_elements=ELEMENTS,
      include_treasury=False,
    )
    assert result.skipped["excluded_account"] == 1
    assert "mercury_txn_txn_treasury" not in {e["external_id"] for e in result.events}

  def test_no_chart_index_means_hints_only(self):
    result = transform(
      raw_pull(),
      source="mercury",
      connection_id="conn_1",
      account_elements=ELEMENTS,
    )
    assert result.resolved["resolved"] == 0
    assert result.resolved["hint_only"] >= 4

  def test_placeholder_posted_at_falls_back_to_created_at(self):
    raw = raw_pull()
    raw["transactions"][0]["postedAt"] = "0001-01-01T00:00:00Z"
    raw["transactions"][0]["createdAt"] = "2026-03-14T12:00:00Z"
    result = transform(
      raw, source="mercury", connection_id="conn_1", account_elements=ELEMENTS
    )
    event = next(
      e for e in result.events if e["external_id"] == "mercury_txn_txn_office"
    )
    assert event["occurred_at"] == "2026-03-14T12:00:00Z"
    assert event["metadata"]["posted_at"] == "0001-01-01T00:00:00Z"  # raw value kept

  def test_events_sorted_by_occurred_at(self):
    result, _ = _events()
    stamps = [e["occurred_at"] for e in result.events]
    assert stamps == sorted(stamps)


@pytest.mark.unit
class TestChartIndex:
  def test_resolve_by_name_normalizes(self):
    index = ChartIndex(by_name={name_key("Bank Fees"): "e1"})
    assert index.resolve("bank-fees") == "e1"
    assert index.resolve(None, "nothing") is None

  def test_resolve_gl_code_prefers_the_code(self):
    index = ChartIndex(
      by_name={name_key("Office Supplies"): "by_name"}, by_code={"500": "by_code"}
    )
    assert index.resolve_gl_code("500 - Office Supplies") == "by_code"
    assert index.resolve_gl_code("501 - Office Supplies") == "by_name"
    assert index.resolve_gl_code("Yachts") is None
    assert index.resolve_gl_code(None) is None
