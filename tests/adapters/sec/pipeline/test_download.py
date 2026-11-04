"""Tests for the SEC download asset's 429 handling."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.adapters.sec.pipeline.download import (
  _get_with_429_retry,
  merge_recent_submissions,
)


def _response(status, content=b""):
  response = AsyncMock()
  response.status = status
  response.headers = {"Retry-After": "0"}
  response.read = AsyncMock(return_value=content)
  response.raise_for_status = MagicMock()
  response.__aenter__ = AsyncMock(return_value=response)
  response.__aexit__ = AsyncMock()
  return response


@pytest.mark.unit
class TestGetWith429Retry:
  @pytest.mark.asyncio
  async def test_retries_in_place_under_a_held_slot(self):
    """The retry runs inside the caller's slot rather than taking a second one."""
    semaphore = asyncio.Semaphore(1)
    session = MagicMock()
    session.get = MagicMock(side_effect=[_response(429), _response(200, b"zip")])

    async def download():
      async with semaphore:
        return await _get_with_429_retry(session, "u", MagicMock())

    assert await asyncio.wait_for(download(), timeout=2) == (200, b"zip")
    assert session.get.call_count == 2

  @pytest.mark.asyncio
  async def test_gives_up_after_max_retries(self):
    session = MagicMock()
    session.get = MagicMock(side_effect=lambda _url: _response(429))

    assert await _get_with_429_retry(session, "u", MagicMock()) == (429, b"")
    assert session.get.call_count == 4

  @pytest.mark.asyncio
  async def test_404_is_returned_without_retry(self):
    session = MagicMock()
    session.get = MagicMock(return_value=_response(404))

    assert await _get_with_429_retry(session, "u", MagicMock()) == (404, b"")
    assert session.get.call_count == 1

  @pytest.mark.asyncio
  async def test_retry_after_is_capped(self):
    slow = _response(429)
    slow.headers = {"Retry-After": "99999"}
    session = MagicMock()
    session.get = MagicMock(side_effect=[slow, _response(200, b"zip")])

    with patch("asyncio.sleep", new=AsyncMock()) as sleep:
      await _get_with_429_retry(session, "u", MagicMock())

    sleep.assert_awaited_once_with(300)


@pytest.mark.unit
class TestMergeRecentSubmissions:
  def _master(self):
    return {
      "tickers": ["BK"],
      "name": "OLD NAME",
      "filings": {"accessionNumber": ["a1"], "form": ["10-K"]},
      "_metadata": {"paginationFilesMerged": 3},
    }

  def _recent(self, *accessions):
    return {
      "tickers": ["BNY", "BNY-PK"],
      "name": "NEW NAME",
      "exchanges": ["NYSE"],
      "filings": {
        "recent": {
          "accessionNumber": list(accessions),
          "form": ["10-Q"] * len(accessions),
        }
      },
    }

  def test_the_header_refreshes_with_a_new_filing(self):
    merged = merge_recent_submissions(self._master(), self._recent("a2", "a1"))

    assert merged["tickers"] == ["BNY", "BNY-PK"]
    assert merged["name"] == "NEW NAME"
    assert merged["exchanges"] == ["NYSE"]
    assert merged["filings"]["accessionNumber"] == ["a2", "a1"]
    assert merged["_metadata"]["totalFilings"] == 2
    assert merged["_metadata"]["paginationFilesMerged"] == 3

  def test_the_header_refreshes_with_no_new_filing(self):
    merged = merge_recent_submissions(self._master(), self._recent("a1"))

    assert merged["tickers"] == ["BNY", "BNY-PK"]
    assert merged["filings"]["accessionNumber"] == ["a1"]

  def test_the_recent_page_never_replaces_the_stored_history(self):
    merged = merge_recent_submissions(self._master(), self._recent())

    assert "recent" not in merged["filings"]
