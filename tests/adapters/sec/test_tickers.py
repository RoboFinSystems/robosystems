"""EDGAR's ticker map applied over the tickers frozen in processed rows."""

import json
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.sec.tickers import (
  TICKER_MAP_KEY,
  Listing,
  apply_ticker_map,
  choose_ticker,
  current_ticker_map,
  parse_ticker_map,
  updated_aliases,
)

ACME = "0000000001"
BETA = "0000000002"
GAMMA = "0000000003"


def _filer(ticker, name="ACME CORP"):
  return {"ticker": ticker, "name": name, "exchange": "NYSE"}


def _raw(*rows):
  return {
    str(i): {"cik_str": cik, "ticker": ticker, "title": title}
    for i, (cik, ticker, title) in enumerate(rows)
  }


@pytest.mark.unit
class TestParseTickerMap:
  def test_pads_ciks_and_keeps_every_ticker_in_file_order(self):
    parsed = parse_ticker_map(
      _raw((1, "ACME", "Acme"), (2, "BETA", "Beta"), (1, "ACM-P", "Acme"))
    )
    assert parsed[ACME] == Listing(("ACME", "ACM-P"), "Acme")
    assert parsed[BETA].tickers == ("BETA",)

  def test_a_row_without_a_ticker_or_cik_is_skipped(self):
    assert parse_ticker_map(_raw((1, "", "Acme"), ("", "BETA", "Beta"))) == {}


@pytest.mark.unit
class TestChooseTicker:
  def test_keeps_the_current_ticker_while_it_is_listed(self):
    assert choose_ticker("ACME", ("ACM", "ACME")) == "ACME"

  def test_a_renamed_filer_takes_what_is_listed(self):
    assert choose_ticker("BK", ("BNY", "BNY-PK")) == "BNY"

  def test_a_new_listing_prefers_the_plain_ticker(self):
    assert choose_ticker(None, ("FTRA-UN", "FTRA", "FTRA-WT")) == "FTRA"
    assert choose_ticker(None, ("TBCVW", "TBCV", "TBCVU")) == "TBCV"

  def test_a_listed_warrant_gives_way_to_a_plain_ticker(self):
    assert choose_ticker("NXNVW", ("NN", "NXNVW")) == "NN"

  def test_a_share_class_stays_when_nothing_plain_is_listed(self):
    assert choose_ticker("BRK-B", ("BRK-A", "BRK-B")) == "BRK-B"

  def test_only_a_preferred_listed_is_taken(self):
    assert choose_ticker("AHL", ("AHL-PD", "AHL-PE")) == "AHL-PD"


@pytest.mark.unit
class TestApplyTickerMap:
  def test_corrects_a_renamed_filer_and_lists_a_new_one(self):
    rows = {ACME: _filer("BK"), BETA: _filer(None, "BETA INC")}
    ticker_map = {
      ACME: Listing(("BNY",), "ACME CORP"),
      BETA: Listing(("BETA",), "BETA INC"),
    }

    out = apply_ticker_map(rows, ticker_map, filed={ACME, BETA})

    assert out[ACME]["ticker"] == "BNY"
    assert out[BETA]["ticker"] == "BETA"
    assert out[ACME]["exchange"] == "NYSE"

  def test_absence_from_the_map_clears_nothing(self):
    out = apply_ticker_map({ACME: _filer("ACME")}, {}, filed={ACME})
    assert out[ACME]["ticker"] == "ACME"

  def test_a_filer_off_the_map_keeps_the_ticker_it_last_published(self):
    out = apply_ticker_map({ACME: _filer("OLD")}, {}, filed={ACME}, prior={ACME: "NEW"})
    assert out[ACME]["ticker"] == "NEW"

  def test_a_ticker_another_filer_still_holds_is_held_back(self):
    rows = {ACME: _filer("XOM"), BETA: _filer("ZZZ", "BETA INC")}

    out = apply_ticker_map(rows, {BETA: Listing(("XOM",))}, filed={ACME, BETA})

    assert out[ACME]["ticker"] == "XOM"
    assert out[BETA]["ticker"] == "ZZZ"

  def test_two_filers_already_on_a_ticker_are_left_as_they_are(self):
    rows = {ACME: _filer("XOM"), BETA: _filer("XOM", "BETA INC")}

    out = apply_ticker_map(rows, {BETA: Listing(("XOM",))}, filed={ACME, BETA})

    assert (out[ACME]["ticker"], out[BETA]["ticker"]) == ("XOM", "XOM")

  def test_a_ticker_its_old_filer_is_leaving_can_be_taken(self):
    rows = {ACME: _filer("SATS"), BETA: _filer("ASST", "BETA INC")}
    ticker_map = {ACME: Listing(("ECHO",)), BETA: Listing(("SATS",))}

    out = apply_ticker_map(rows, ticker_map, filed={ACME, BETA})

    assert (out[ACME]["ticker"], out[BETA]["ticker"]) == ("ECHO", "SATS")

  def test_holding_one_filer_back_can_hold_back_the_next(self):
    rows = {ACME: _filer("AAA"), BETA: _filer("BBB", "B"), GAMMA: _filer("CCC", "C")}
    # BETA wants CCC, which stays held, so AAA's mover cannot take BBB either.
    ticker_map = {ACME: Listing(("BBB",)), BETA: Listing(("CCC",))}

    out = apply_ticker_map(rows, ticker_map, filed={ACME, BETA, GAMMA})

    assert [out[c]["ticker"] for c in (ACME, BETA, GAMMA)] == ["AAA", "BBB", "CCC"]

  def test_a_filer_with_no_filings_holds_nothing_back(self):
    rows = {ACME: _filer("XOM"), BETA: _filer("ZZZ", "BETA INC")}

    out = apply_ticker_map(rows, {BETA: Listing(("XOM",))}, filed={BETA})

    assert out[BETA]["ticker"] == "XOM"
    assert out[ACME]["ticker"] == "XOM"

  def test_a_renamed_company_takes_edgars_title(self):
    rows = {ACME: _filer("ACME", "OLD NAME INC")}

    out = apply_ticker_map(
      rows, {ACME: Listing(("ACME",), "New Name Inc")}, filed={ACME}
    )

    assert out[ACME]["name"] == "New Name Inc"

  def test_a_title_differing_only_in_punctuation_keeps_the_stored_name(self):
    rows = {ACME: _filer("ACME", "ACME CORP /DE/")}

    out = apply_ticker_map(
      rows, {ACME: Listing(("ACME",), "Acme Corp DE")}, filed={ACME}
    )

    assert out[ACME]["name"] == "ACME CORP /DE/"

  def test_an_unchanged_ticker_keeps_its_stored_spelling(self):
    out = apply_ticker_map(
      {ACME: _filer("acme")}, {ACME: Listing(("ACME",))}, filed={ACME}
    )
    assert out[ACME]["ticker"] == "acme"


@pytest.mark.unit
class TestUpdatedAliases:
  def test_a_rename_adds_an_alias(self):
    aliases = updated_aliases({}, {ACME: "BNY"}, {ACME: "BK"}, "2026-11-05")
    assert aliases == {"BK": {"cik": ACME, "ticker": "BNY", "since": "2026-11-05"}}

  def test_no_change_adds_nothing(self):
    assert updated_aliases({}, {ACME: "ACME"}, {ACME: "acme"}, "2026-11-05") == {}

  def test_a_second_rename_points_the_first_alias_at_the_new_ticker(self):
    first = updated_aliases({}, {ACME: "BBB"}, {ACME: "AAA"}, "2026-01-01")

    second = updated_aliases(first, {ACME: "CCC"}, {ACME: "BBB"}, "2026-02-01")

    assert second["AAA"] == {"cik": ACME, "ticker": "CCC", "since": "2026-01-01"}
    assert second["BBB"] == {"cik": ACME, "ticker": "CCC", "since": "2026-02-01"}

  def test_an_alias_ends_when_another_filer_lists_under_it(self):
    first = updated_aliases({}, {ACME: "NEW"}, {ACME: "OLD"}, "2026-01-01")

    second = updated_aliases(
      first, {ACME: "NEW", BETA: "OLD"}, {ACME: "NEW"}, "2026-02-01"
    )

    assert "OLD" not in second

  def test_an_alias_ends_when_its_filer_returns_to_the_ticker(self):
    first = updated_aliases({}, {ACME: "NEW"}, {ACME: "OLD"}, "2026-01-01")

    second = updated_aliases(first, {ACME: "OLD"}, {ACME: "NEW"}, "2026-02-01")

    assert "OLD" not in second
    assert second["NEW"]["ticker"] == "OLD"

  def test_an_alias_survives_its_filer_leaving_the_listing(self):
    first = updated_aliases({}, {ACME: "NEW"}, {ACME: "OLD"}, "2026-01-01")

    assert updated_aliases(first, {}, {}, "2026-02-01") == first


class _NoSuchKey(Exception):
  pass


def _s3(stored=None):
  s3 = MagicMock()
  s3.exceptions.NoSuchKey = _NoSuchKey
  if stored is None:
    s3.get_object.side_effect = _NoSuchKey()
  else:
    s3.get_object.return_value = {
      "Body": MagicMock(read=lambda: json.dumps(stored).encode())
    }
  return s3


@pytest.mark.unit
class TestCurrentTickerMap:
  def _edgar(self, raw=None, error=None):
    client = MagicMock()
    client.company_tickers.return_value = raw
    client.company_tickers.side_effect = error
    return patch(
      "robosystems.adapters.sec.client.edgar.edgar_client", return_value=client
    )

  def test_a_good_fetch_is_returned_and_kept(self):
    s3 = _s3()
    raw = _raw((1, "ACME", "Acme"))
    with self._edgar(raw):
      result = current_ticker_map(s3, "raw")

    assert result == {ACME: Listing(("ACME",), "Acme")}
    put = s3.put_object.call_args.kwargs
    assert put["Key"] == TICKER_MAP_KEY
    assert json.loads(put["Body"]) == raw

  def test_a_failed_fetch_uses_the_last_good_copy(self):
    s3 = _s3(_raw((1, "ACME", "Acme")))
    with self._edgar(error=RuntimeError("503")):
      result = current_ticker_map(s3, "raw")

    assert result == {ACME: Listing(("ACME",), "Acme")}
    s3.put_object.assert_not_called()

  def test_a_short_fetch_is_rejected(self):
    stored = _raw(*[(i, f"T{i}", "x") for i in range(1, 101)])
    s3 = _s3(stored)
    with self._edgar(_raw(*[(i, f"T{i}", "x") for i in range(1, 50)])):
      result = current_ticker_map(s3, "raw")

    assert result is not None and len(result) == 100
    s3.put_object.assert_not_called()

  def test_an_empty_fetch_with_nothing_stored_is_no_map(self):
    with self._edgar({}):
      assert current_ticker_map(_s3(), "raw") is None

  def test_no_fetch_and_nothing_stored_is_no_map(self):
    with self._edgar(error=RuntimeError("503")):
      assert current_ticker_map(_s3(), "raw") is None

  @pytest.mark.parametrize("body", [b"not json", b"[]", b'{"0": "row"}', b"\xff\xfe"])
  def test_a_corrupt_stored_copy_is_replaced_by_a_good_fetch(self, body):
    s3 = _s3()
    s3.get_object.side_effect = None
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: body)}
    with self._edgar(_raw((1, "ACME", "Acme"))):
      result = current_ticker_map(s3, "raw")

    assert result == {ACME: Listing(("ACME",), "Acme")}
    s3.put_object.assert_called_once()

  def test_a_corrupt_stored_copy_and_no_fetch_is_no_map(self):
    s3 = _s3()
    s3.get_object.side_effect = None
    s3.get_object.return_value = {"Body": MagicMock(read=lambda: b"not json")}
    with self._edgar(error=RuntimeError("503")):
      assert current_ticker_map(s3, "raw") is None

  def test_a_stored_copy_that_cannot_be_read_fails_the_run(self):
    s3 = _s3()
    s3.get_object.side_effect = RuntimeError("AccessDenied")
    with self._edgar(_raw((1, "ACME", "Acme"))), pytest.raises(RuntimeError):
      current_ticker_map(s3, "raw")
