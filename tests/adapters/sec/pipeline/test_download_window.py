"""The download's look-back: an intraday pass discovers the last days of the
quarter, not the quarter."""

import json
import re
from datetime import date
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
import responses
from dagster import build_asset_context

from robosystems.adapters.sec.pipeline import download as module
from robosystems.adapters.sec.pipeline.configs import SECDownloadConfig
from robosystems.adapters.sec.pipeline.download import sec_raw_filings

pytestmark = pytest.mark.unit

EFTS = re.compile(r"https://efts\.sec\.gov/LATEST/search-index.*")


def _discover(partition: str, today: date, since_days: int | None) -> tuple[dict, list]:
  """Run discovery only (a dry run with no submissions); returns the run's
  metadata and the EFTS queries it made."""
  hit = {
    "_id": "0000000007-25-000011:tdwi-20250331.htm",
    "_source": {"ciks": ["0000000007"], "form": "10-K", "file_date": "2025-11-04"},
  }
  body = json.dumps({"hits": {"total": {"value": 1}, "hits": [hit]}})
  with (
    responses.RequestsMock(assert_all_requests_are_fired=False) as http,
    patch.object(module, "_eastern_today", return_value=today),
  ):
    http.add(responses.GET, EFTS, body=body, content_type="application/json")
    result = sec_raw_filings(
      build_asset_context(partition_key=partition),
      config=SECDownloadConfig(
        form_types=["10-K"],
        since_days=since_days,
        dry_run=True,
        skip_submissions=True,
      ),
      s3=MagicMock(),
      db=MagicMock(),
    )
    queries = [parse_qs(urlparse(call.request.url).query) for call in http.calls]
  return dict(result.metadata), queries


def test_a_look_back_asks_efts_for_its_days_only():
  metadata, queries = _discover("2025-Q4", date(2025, 11, 5), since_days=2)
  assert metadata["filings_found"] == 1
  assert {(q["startdt"][0], q["enddt"][0]) for q in queries} == {
    ("2025-11-03", "2025-11-05")
  }


def test_a_look_back_stops_at_the_first_day_of_the_quarter():
  _metadata, queries = _discover("2025-Q4", date(2025, 10, 1), since_days=2)
  assert {(q["startdt"][0], q["enddt"][0]) for q in queries} == {
    ("2025-10-01", "2025-10-01")
  }


def test_a_quarter_outside_the_look_back_asks_nothing():
  metadata, queries = _discover("2025-Q3", date(2025, 11, 5), since_days=2)
  assert metadata["filings_found"] == 0
  assert queries == []


def test_without_a_look_back_the_whole_quarter_is_read():
  _metadata, queries = _discover("2025-Q4", date(2025, 11, 5), since_days=None)
  assert {(q["startdt"][0], q["enddt"][0]) for q in queries} == {
    ("2025-10-01", "2025-12-31")
  }
