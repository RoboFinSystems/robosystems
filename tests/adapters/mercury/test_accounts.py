"""Link or create — one chart account per bank account, through the envelope."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.mercury.pipeline.accounts import (
  BANK_FEED_KEY,
  ChartRequiredError,
  _next_code,
  _unique_qname,
  build_chart_index,
  link_bank_accounts,
)
from robosystems.adapters.mercury.pipeline.transform import BankAccount

MODULE = "robosystems.adapters.mercury.pipeline.accounts"


def _element(id, name, *, code=None, qname=None, metadata=None, is_active=True):
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
  )


def _checking(mercury_id="acct_1", name="Mercury Checking ••1234") -> BankAccount:
  return BankAccount(
    mercury_id=mercury_id,
    name=name,
    kind="checking",
    trait="asset",
    balance_type="debit",
  )


def _card(mercury_id="acct_card") -> BankAccount:
  return BankAccount(
    mercury_id=mercury_id,
    name="Mercury IO ••9012",
    kind="credit",
    trait="liability",
    balance_type="credit",
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
          _Session(), [_checking()], connection_id="conn_1", created_by="u"
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
        session, [_checking()], connection_id="conn_1", created_by="u"
      )
    assert result.links == {"acct_1": "e1"}
    assert (result.linked, result.created) == (1, 0)

  def test_name_match_links_and_stamps(self):
    existing = _element("e1", "Mercury Checking 1234")
    session = _Session([existing])
    with patch(f"{MODULE}.active_chart_id", return_value="tax_1"):
      result = link_bank_accounts(
        session, [_checking()], connection_id="conn_1", created_by="u"
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
        session, [_checking(), _card()], connection_id="conn_1", created_by="u"
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
      link_bank_accounts(session, [_checking()], connection_id="conn_1", created_by="u")
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
          session, [_checking()], connection_id="conn_1", created_by="u"
        )


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
