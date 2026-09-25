"""A filing that lives only in a submissions pagination page gets its form.

When the stored master is short or missing, the live header carries only the
recent page. The loader reads the complete history, so a retried filing can
succeed; a page that fails raises instead of leaving the form unknown. EDGAR
is stubbed at the HTTP layer and S3 with moto, so xbrlkit's own page merge
runs.
"""

from __future__ import annotations

import json
import re

import boto3
import pytest
import responses
from moto import mock_aws

from robosystems.adapters.sec.client.edgar import IncompleteSubmissions
from robosystems.adapters.sec.processors.metadata import SECMetadataLoader
from robosystems.config.storage.shared import DataSourceType, get_raw_key

pytestmark = pytest.mark.unit

CIK = "0000000001"
BUCKET = "sec-raw-test"
OLD_ACCESSION = "0000000001-12-000001"
PAGE = "CIK0000000001-submissions-001.json"

HEADER = {
  "cik": "1",
  "name": "Driftline Bancorp",
  "filings": {
    "recent": {"accessionNumber": ["0000000001-25-000009"], "form": ["10-Q"]},
    "files": [{"name": PAGE}],
  },
}
OLDER_PAGE = {"accessionNumber": [OLD_ACCESSION], "form": ["10-K"]}
SHORT_MASTER = {
  "cik": "1",
  "name": "Driftline Bancorp",
  "filings": {"accessionNumber": ["0000000001-25-000009"], "form": ["10-Q"]},
  "_metadata": {"paginationFilesMerged": 0},
}


def _edgar(page_status: int = 200) -> None:
  responses.add(
    responses.GET, re.compile(rf".*/submissions/CIK{CIK}\.json$"), json=HEADER
  )
  responses.add(
    responses.GET,
    re.compile(rf".*/submissions/{re.escape(PAGE)}$"),
    json=OLDER_PAGE,
    status=page_status,
  )


@pytest.fixture()
def s3():
  with mock_aws():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    yield client


def _store_master(s3, master: dict) -> None:
  key = get_raw_key(DataSourceType.SEC, "submissions", f"{CIK}.json")
  s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(master))


@pytest.mark.parametrize("master", [SHORT_MASTER, None], ids=["short master", "none"])
@responses.activate
def test_an_older_filing_is_found_in_the_pagination_pages(s3, master):
  if master is not None:
    _store_master(s3, master)
  _edgar()

  _, report = SECMetadataLoader().get_metadata(
    CIK, OLD_ACCESSION, s3_client=s3, bucket=BUCKET
  )

  assert report.get("form") == "10-K"


@responses.activate
def test_a_failed_page_fails_the_filing_as_retryable(s3):
  _store_master(s3, SHORT_MASTER)
  _edgar(page_status=500)

  with pytest.raises(IncompleteSubmissions):
    SECMetadataLoader().get_metadata(CIK, OLD_ACCESSION, s3_client=s3, bucket=BUCKET)
