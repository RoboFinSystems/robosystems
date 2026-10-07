"""Link or create — one chart account per bank account, through the envelope."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.bank_feed.accounts import (
  BANK_FEED_KEY,
  ChartRequiredError,
  _next_code,
  _unique_qname,
  account_entities,
  build_chart_index,
  chart_indexes,
  link_bank_accounts,
)
from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex

MODULE = "robosystems.adapters.bank_feed.accounts"


@pytest.fixture(autouse=True)
def _plain_prefix():
  """The group parent's chart: the plain prefix, no qname taken yet."""
  with (
    patch(f"{MODULE}.chart_qname_prefix", return_value="coa"),
    patch(f"{MODULE}.taken_qnames", return_value=set()),
  ):
    yield


def _element(
  id,
  name,
  *,
  code=None,
  qname=None,
  metadata=None,
  is_active=True,
  taxonomy_id="tax_1",
):
  return SimpleNamespace(
    id=id,
    name=name,
    code=code,
    qname=qname or f"coa:{code or name.replace(' ', '')}",
    metadata_=metadata or {},
    is_active=is_active,
    external_source=None,
    external_id=None,
    connection_id=None,
    taxonomy_id=taxonomy_id,
  )


def _checking(account_id="acct_1", name="Mercury Checking ••1234") -> BankAccount:
  return BankAccount(
    account_id=account_id,
    name=name,
    kind="checking",
    trait="asset",
    balance_type="debit",
    institution="Mercury",
  )


def _card(account_id="acct_card") -> BankAccount:
  return BankAccount(
    account_id=account_id,
    name="Mercury IO ••9012",
    kind="credit",
    trait="liability",
    balance_type="credit",
    institution="Mercury",
  )


class _Session:
  """Just enough of a Session: ``execute(...).scalars().all()`` returns the
  element list handed in, in order of calls."""

  def __init__(self, *result_sets):
    self._results = list(result_sets)
    self.flushed = 0

  def execute(self, statement):
    rows = self._results.pop(0) if self._results else []
    result = MagicMock()
    result.scalars.return_value.all.return_value = rows
    result.scalars.return_value.__iter__ = lambda self_: iter(rows)
    result.all.return_value = rows
    return result

  def flush(self):
    self.flushed += 1


@pytest.mark.unit
class TestLinkBankAccounts:
  def test_no_chart_is_refused(self):
    with patch(f"{MODULE}.active_chart_id", return_value=None):
      with pytest.raises(ChartRequiredError):
        link_bank_accounts(
          _Session(),
          [_checking()],
          provider="mercury",
          connection_id="conn_1",
          created_by="u",
        )

  def test_previously_linked_account_resolves_by_feed_metadata(self):
    linked = _element(
      "e1",
      "Operating cash",
      metadata={BANK_FEED_KEY: {"provider": "mercury", "account_id": "acct_1"}},
    )
    session = _Session([linked])
    with patch(f"{MODULE}.active_chart_id", return_value="tax_1"):
      result = link_bank_accounts(
        session,
        [_checking()],
        provider="mercury",
        connection_id="conn_1",
        created_by="u",
      )
    assert result.links == {"acct_1": "e1"}
    assert (result.linked, result.created) == (1, 0)

  def test_name_match_links_and_stamps(self):
    existing = _element("e1", "Mercury Checking 1234")
    session = _Session([existing])
    with patch(f"{MODULE}.active_chart_id", return_value="tax_1"):
      result = link_bank_accounts(
        session,
        [_checking()],
        provider="mercury",
        connection_id="conn_1",
        created_by="u",
      )
    assert result.links == {"acct_1": "e1"}
    assert existing.metadata_[BANK_FEED_KEY]["account_id"] == "acct_1"
    assert existing.metadata_[BANK_FEED_KEY]["connection_id"] == "conn_1"
    # Linking never rewrites provenance on the tenant's own element.
    assert existing.external_source is None

  def test_unmatched_accounts_are_created_through_the_envelope(self):
    cash = _element("e1", "Cash", code="1000")
    created_checking = _element(
      "e2", "Mercury Checking ••1234", code="1010", qname="coa:1010"
    )
    created_card = _element("e3", "Mercury IO ••9012", code="2110", qname="coa:2110")
    session = _Session([cash], [created_checking, created_card])
    with (
      patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
      patch(f"{MODULE}.update_chart_block") as update,
    ):
      result = link_bank_accounts(
        session,
        [_checking(), _card()],
        provider="mercury",
        connection_id="conn_1",
        created_by="u",
      )
    payload = update.call_args.args[1]
    assert payload.taxonomy_id == "tax_1"
    reqs = {r.name: r for r in payload.elements_to_add}
    assert reqs["Mercury Checking ••1234"].qname == "coa:1010"
    assert reqs["Mercury Checking ••1234"].code == "1010"
    assert reqs["Mercury Checking ••1234"].period_type == "instant"
    assert reqs["Mercury IO ••9012"].qname == "coa:2110"
    assert reqs["Mercury IO ••9012"].trait == "liability"
    assert (
      reqs["Mercury IO ••9012"].metadata[BANK_FEED_KEY]["account_id"] == "acct_card"
    )
    assert result.links == {"acct_1": "e2", "acct_card": "e3"}
    assert (result.linked, result.created) == (0, 2)
    assert created_checking.external_source == "mercury"
    assert created_checking.external_id == "acct_1"
    assert created_checking.connection_id == "conn_1"

  def test_chart_without_codes_creates_name_qnames(self):
    existing = _element("e1", "Cash", qname="qb:Cash")
    created = _element("e2", "Mercury Checking ••1234", qname="coa:MercuryChecking1234")
    session = _Session([existing], [created])
    with (
      patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
      patch(f"{MODULE}.update_chart_block") as update,
    ):
      link_bank_accounts(
        session,
        [_checking()],
        provider="mercury",
        connection_id="conn_1",
        created_by="u",
      )
    req = update.call_args.args[1].elements_to_add[0]
    assert req.code is None
    assert req.qname == "coa:MercuryChecking1234"

  def test_create_that_leaves_no_row_is_an_error(self):
    session = _Session([], [])
    with (
      patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
      patch(f"{MODULE}.update_chart_block"),
    ):
      with pytest.raises(RuntimeError, match="was not created"):
        link_bank_accounts(
          session,
          [_checking()],
          provider="mercury",
          connection_id="conn_1",
          created_by="u",
        )


@pytest.mark.unit
class TestLinksAcrossCharts:
  """A feed account moved to a subsidiary's chart is found there on the next
  sync; a name match and a new account are the group parent's chart only."""

  def test_a_link_in_another_entitys_chart_is_honoured(self):
    moved = _element(
      "e_sub",
      "Chase Checking ••1234",
      taxonomy_id="tax_sub",
      metadata={
        BANK_FEED_KEY: {
          "provider": "plaid",
          "account_id": "acct_1",
          "connection_id": "conn_1",
        }
      },
    )
    session = _Session([moved])
    with (
      patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
      patch(f"{MODULE}.update_chart_block") as update,
    ):
      result = link_bank_accounts(
        session, [_checking()], provider="plaid", connection_id="conn_1", created_by="u"
      )
    assert result.links == {"acct_1": "e_sub"}
    assert (result.linked, result.created) == (1, 0)
    update.assert_not_called()

  def test_a_name_match_in_another_entitys_chart_is_not_claimed(self):
    sibling = _element("e_sub", "Mercury Checking 1234", taxonomy_id="tax_sub")
    created = _element("e2", "Mercury Checking ••1234", qname="coa:MercuryChecking1234")
    session = _Session([sibling], [created])
    with (
      patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
      patch(f"{MODULE}.update_chart_block") as update,
    ):
      result = link_bank_accounts(
        session,
        [_checking()],
        provider="mercury",
        connection_id="conn_1",
        created_by="u",
      )
    # Created on the parent's chart, not linked to the subsidiary's account.
    assert result.links == {"acct_1": "e2"}
    assert update.call_args.args[1].taxonomy_id == "tax_1"
    assert BANK_FEED_KEY not in sibling.metadata_

  def test_accounts_are_created_on_the_feeds_entitys_chart(self):
    """A feed connected for a subsidiary lands its accounts on that chart."""
    created = _element(
      "e2", "Mercury Checking ••1234", qname="coa-sub:MercuryChecking1234"
    )
    session = _Session([], [created])
    with (
      patch(f"{MODULE}.active_chart_id", return_value="tax_sub") as chart,
      patch(f"{MODULE}.chart_qname_prefix", return_value="coa-sub"),
      patch(f"{MODULE}.update_chart_block") as update,
    ):
      result = link_bank_accounts(
        session,
        [_checking()],
        provider="mercury",
        connection_id="conn_1",
        created_by="u",
        entity_id="ent_sub",
      )
    chart.assert_called_once_with(session, "ent_sub")
    request = update.call_args.args[1]
    assert request.taxonomy_id == "tax_sub"
    # The subsidiary's chart has its own qname prefix.
    assert request.elements_to_add[0].qname == "coa-sub:MercuryChecking1234"
    assert result.links == {"acct_1": "e2"}

  def test_an_entity_without_a_chart_is_refused_by_name(self):
    with patch(f"{MODULE}.active_chart_id", return_value=None):
      with pytest.raises(ChartRequiredError, match="ent_sub"):
        link_bank_accounts(
          _Session(),
          [_checking()],
          provider="plaid",
          connection_id="conn_1",
          created_by="u",
          entity_id="ent_sub",
        )

  def test_codes_are_allocated_against_the_parents_chart_only(self):
    sub_taken = _element("e_sub", "Sub checking", code="1010", taxonomy_id="tax_sub")
    created = _element("e2", "Mercury Checking ••1234", qname="coa:MercuryChecking1234")
    session = _Session([sub_taken], [created])
    with (
      patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
      patch(f"{MODULE}.update_chart_block") as update,
    ):
      link_bank_accounts(
        session,
        [_checking()],
        provider="mercury",
        connection_id="conn_1",
        created_by="u",
      )
    # The parent's chart has no codes, so the new account gets none; the
    # subsidiary's 1010 is not in the way.
    assert update.call_args.args[1].elements_to_add[0].code is None


@pytest.mark.unit
class TestHelpers:
  def test_next_code_skips_taken(self):
    taken = {"1010", "1020"}
    assert _next_code(taken, 1010) == "1030"
    assert "1030" in taken

  def test_unique_qname_suffixes(self):
    taken = {"coa:1010"}
    assert _unique_qname(taken, "1010") == "coa:10102"
    assert _unique_qname(set(), "Cash") == "coa:Cash"
    assert _unique_qname({"coa:1010"}, "1010", "coa-cadence") == "coa-cadence:1010"

  def test_build_chart_index_reads_name_and_code(self):
    session = MagicMock()
    session.execute.return_value.all.return_value = [
      ("e1", "Bank Fees", "6100"),
      ("e2", "Cash", None),
    ]
    with patch(f"{MODULE}.active_chart_id", return_value="tax_1"):
      index = build_chart_index(session)
    assert index.resolve("bank fees") == "e1"
    assert index.resolve("6100") == "e1"
    assert index.resolve("cash") == "e2"

  def test_build_chart_index_without_chart_is_empty(self):
    with patch(f"{MODULE}.active_chart_id", return_value=None):
      assert build_chart_index(MagicMock()).resolve("cash") is None

  def test_build_chart_index_reads_the_named_entitys_chart(self):
    session = MagicMock()
    session.execute.return_value.all.return_value = [("e9", "Rent", "6500")]
    with patch(f"{MODULE}.active_chart_id", return_value="tax_sub") as chart:
      index = build_chart_index(session, "ent_sub")
    chart.assert_called_once_with(session, "ent_sub")
    assert index.resolve("rent") == "e9"

  def test_chart_indexes_one_per_entity(self):
    with patch(f"{MODULE}.build_chart_index", return_value=ChartIndex()) as build:
      indexes = chart_indexes(MagicMock(), ["ent_b", "ent_a", "ent_a", None])
    assert sorted(indexes) == ["ent_a", "ent_b"]
    assert build.call_count == 2

  def test_account_entities_falls_back_to_the_parent(self):
    session = MagicMock()
    session.execute.return_value.all.return_value = [
      SimpleNamespace(element_id="e_sub", entity_id="ent_sub"),
      SimpleNamespace(element_id="e_par", entity_id=None),
    ]
    owners = account_entities(session, ["e_sub", "e_par", None], parent_id="ent_p")
    assert owners == {"e_sub": "ent_sub", "e_par": "ent_p"}
    assert account_entities(session, [], parent_id="ent_p") == {}


@pytest.mark.unit
def test_another_providers_link_does_not_claim_the_account():
  mercury_linked = _element(
    "e1",
    "Operating cash",
    metadata={BANK_FEED_KEY: {"provider": "mercury", "account_id": "acct_1"}},
  )
  created = _element("e2", "Mercury Checking ••1234", qname="coa:MercuryChecking1234")
  session = _Session([mercury_linked], [created])
  with (
    patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
    patch(f"{MODULE}.update_chart_block") as update,
  ):
    result = link_bank_accounts(
      session, [_checking()], provider="plaid", connection_id="conn_2", created_by="u"
    )
  assert (result.linked, result.created) == (0, 1)
  link = update.call_args.args[1].elements_to_add[0].metadata[BANK_FEED_KEY]
  assert link["provider"] == "plaid" and link["institution"] == "Mercury"


@pytest.mark.unit
def test_an_account_another_connection_feeds_is_not_claimed_by_name():
  owned = _element(
    "e1",
    "Mercury Checking ••1234",
    qname="coa:MercuryChecking1234",
    metadata={
      BANK_FEED_KEY: {
        "provider": "mercury",
        "account_id": "merc_1",
        "connection_id": "conn_mercury",
      }
    },
  )
  created = _element("e2", "Mercury Checking ••1234", qname="coa:MercuryChecking12342")
  session = _Session([owned], [created])
  with (
    patch(f"{MODULE}.active_chart_id", return_value="tax_1"),
    patch(f"{MODULE}.update_chart_block") as update,
  ):
    result = link_bank_accounts(
      session,
      [_checking()],
      provider="plaid",
      connection_id="conn_plaid",
      created_by="u",
    )
  assert (result.linked, result.created) == (0, 1)
  assert result.links == {"acct_1": "e2"}
  # The other feed's link is untouched.
  assert owned.metadata_[BANK_FEED_KEY]["provider"] == "mercury"
  assert update.call_args.args[1].elements_to_add[0].qname == "coa:MercuryChecking12342"


@pytest.mark.unit
def test_a_connection_reclaims_its_own_account_under_a_new_account_id():
  own = _element(
    "e1",
    "Mercury Checking ••1234",
    metadata={
      BANK_FEED_KEY: {
        "provider": "plaid",
        "account_id": "old_item_acct",
        "connection_id": "conn_plaid",
      }
    },
  )
  session = _Session([own])
  with patch(f"{MODULE}.active_chart_id", return_value="tax_1"):
    result = link_bank_accounts(
      session,
      [_checking()],
      provider="plaid",
      connection_id="conn_plaid",
      created_by="u",
    )
  assert result.links == {"acct_1": "e1"} and result.created == 0
  assert own.metadata_[BANK_FEED_KEY]["account_id"] == "acct_1"
