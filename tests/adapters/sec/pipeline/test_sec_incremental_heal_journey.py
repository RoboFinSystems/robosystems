"""Journey: a failed EDGAR page on the first build heals on the next pass, and a
filing a stale snapshot does not hold is still processed, exactly once.

Pins the lifecycle that produced the September second-order bugs: a failed
submissions page read as "no filings" (a final skip where a retry was meant),
and a stale submissions snapshot hiding a filing from the incremental pass.
The real download and process assets run; EDGAR is faked at the HTTP layer
with ``responses`` (so xbrlkit's page merge runs), S3 is moto, the SourceFile
ledger is the test Postgres, and only the XBRL parse is stubbed. The filing
ZIPs are pre-placed in the raw bucket, since the ZIP fetch is not the seam.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import responses
from dagster import build_asset_context
from moto import mock_aws

from robosystems.adapters.sec.pipeline.configs import (
  SECDownloadConfig,
  SECProcessConfig,
)
from robosystems.adapters.sec.pipeline.download import sec_raw_filings
from robosystems.adapters.sec.pipeline.process import sec_processed_filings
from robosystems.adapters.sec.pipeline.sensors import requeue_failed_files
from robosystems.config import env
from robosystems.config.storage.shared import DataSourceType, get_raw_key
from robosystems.models.core import SourceFile

pytestmark = pytest.mark.unit

QUARTER = "2025-Q1"
CIK = "0000000007"
FILER = "Tidewater Instruments Inc."
PAGE = f"CIK{CIK}-submissions-001.json"

# Filed in the quarter but listed only in the pagination page.
PAGED = "0000000007-25-000011"
# Filed after the stored snapshot was written; only the live header has it.
LATE = "0000000007-25-000024"
# Already on the recent page when the build starts.
RECENT = "0000000007-25-000030"


@dataclass
class Edgar:
  """What EDGAR answers right now; the test flips pages up and down."""

  page_down: bool = False
  header_down: bool = False
  recent: list[tuple[str, str]] = field(default_factory=lambda: [(RECENT, "8-K")])
  quarter_hits: list[tuple[str, str]] = field(default_factory=list)

  def header(self) -> dict:
    return {
      "cik": CIK.lstrip("0"),
      "name": FILER,
      "tickers": ["TDWI"],
      "exchanges": ["Nasdaq"],
      "filings": {
        "recent": {
          "accessionNumber": [a for a, _ in self.recent],
          "form": [f for _, f in self.recent],
          "filingDate": ["2025-03-01" for _ in self.recent],
        },
        "files": [{"name": PAGE}],
      },
    }

  @staticmethod
  def page() -> dict:
    return {
      "accessionNumber": [PAGED],
      "form": ["10-K"],
      "filingDate": ["2025-02-14"],
    }

  def efts(self) -> dict:
    hits = [
      {
        "_id": f"{accession}:tdwi-20250331.htm",
        "_source": {
          "ciks": [CIK],
          "form": form,
          "file_date": "2025-02-14",
          "display_names": [f"{FILER} (TDWI) (CIK {CIK})"],
        },
      }
      for accession, form in self.quarter_hits
    ]
    return {"hits": {"total": {"value": len(hits)}, "hits": hits}}

  def install(self, mock: responses.RequestsMock) -> None:
    def reply(is_down: Callable[[], bool], body: Callable[[], dict]):
      def callback(_request):
        if is_down():
          return 500, {}, "Internal Server Error"
        return 200, {"Content-Type": "application/json"}, json.dumps(body())

      return callback

    mock.add_callback(
      responses.GET,
      re.compile(r"https://efts\.sec\.gov/LATEST/search-index.*"),
      callback=lambda _r: (200, {}, json.dumps(self.efts())),
    )
    mock.add_callback(
      responses.GET,
      re.compile(rf".*/submissions/CIK{CIK}\.json$"),
      callback=reply(lambda: self.header_down, self.header),
    )
    mock.add_callback(
      responses.GET,
      re.compile(rf".*/submissions/{re.escape(PAGE)}$"),
      callback=reply(lambda: self.page_down, self.page),
    )


class FakeXBRLProcessor:
  """Stands in for the Arelle parse: records what it was handed, emits one
  Fact table so the filing counts as extracted."""

  calls: list[tuple[str, str | None]] = []

  def __init__(self, *, sec_report: dict, output_dir: str, **_: Any) -> None:
    self._report = sec_report
    self._out = Path(output_dir)

  def process(self) -> None:
    FakeXBRLProcessor.calls.append(
      (self._report["accessionNumber"], self._report.get("form"))
    )
    (self._out / "nodes").mkdir(parents=True, exist_ok=True)
    table = pa.table({"identifier": [self._report["accessionNumber"]]})
    # A file handle, not a path: path resolution goes through pyarrow's
    # filesystem registry, which another test module's imports can leave
    # double-registered.
    with (self._out / "nodes" / "Fact.parquet").open("wb") as out:
      pq.write_table(table, out)


@pytest.fixture()
def world(test_db, monkeypatch):
  FakeXBRLProcessor.calls = []
  edgar = Edgar()
  # pytest.ini points boto3 at LocalStack; moto intercepts only without it.
  monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
  with (
    mock_aws(),
    responses.RequestsMock(assert_all_requests_are_fired=False) as http,
    patch(
      "robosystems.adapters.sec.processors.processing.XBRLGraphProcessor",
      FakeXBRLProcessor,
    ),
    patch("robosystems.adapters.sec.config.XBRL_SEMANTIC_ENRICHMENT", False),
  ):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=env.SHARED_RAW_BUCKET)
    s3.create_bucket(Bucket=env.SHARED_PROCESSED_BUCKET)
    edgar.install(http)

    @contextmanager
    def get_session():
      yield test_db
      test_db.commit()

    yield SimpleNamespace(
      edgar=edgar,
      s3=s3,
      session=test_db,
      s3_resource=SimpleNamespace(client=s3),
      db_resource=SimpleNamespace(get_session=get_session),
    )


def _place_zip(s3, accession: str) -> None:
  buffer = io.BytesIO()
  with zipfile.ZipFile(buffer, "w") as zf:
    zf.writestr("tdwi-20250331.htm", "<html><body>Tidewater</body></html>")
  key = get_raw_key(DataSourceType.SEC, "year=2025", CIK, f"{accession}.zip")
  s3.put_object(Bucket=env.SHARED_RAW_BUCKET, Key=key, Body=buffer.getvalue())


def _download(w) -> None:
  sec_raw_filings(
    build_asset_context(partition_key=QUARTER),
    config=SECDownloadConfig(ciks=[CIK], form_types=["10-K", "10-Q"]),
    s3=w.s3_resource,
    db=w.db_resource,
  )


def _process(w) -> None:
  sec_processed_filings(
    build_asset_context(partition_key=QUARTER),
    config=SECProcessConfig(),
    s3=w.s3_resource,
    db=w.db_resource,
  )


def _master(w) -> dict | None:
  key = get_raw_key(DataSourceType.SEC, "submissions", f"{CIK}.json")
  try:
    body = w.s3.get_object(Bucket=env.SHARED_RAW_BUCKET, Key=key)["Body"].read()
  except w.s3.exceptions.NoSuchKey:
    return None
  return json.loads(body)


def _row(w, accession: str) -> SourceFile:
  row = (
    w.session.query(SourceFile)
    .filter(SourceFile.graph_id == "sec", SourceFile.source_id == accession)
    .one()
  )
  w.session.refresh(row)
  return row


def _next_night(w) -> None:
  """A day passes: every failed file is past its retry backoff."""
  w.session.query(SourceFile).filter(SourceFile.status == "error").update(
    {SourceFile.last_attempt_at: datetime.now(UTC) - timedelta(days=1)},
    synchronize_session=False,
  )
  w.session.commit()


def test_a_failed_page_heals_and_a_stale_snapshot_hides_nothing(world):
  w = world

  # 1. First build: EDGAR fails the one pagination page that lists the filing.
  w.edgar.page_down = True
  w.edgar.quarter_hits = [(PAGED, "10-K")]
  _place_zip(w.s3, PAGED)

  _download(w)

  assert _master(w) is None, "a short history must not be stored as the master"
  assert _row(w, PAGED).status == "pending"

  _process(w)

  first = _row(w, PAGED)
  assert first.status == "error", "a failed page read is retryable, never a skip"
  assert first.attempts == 1
  assert FakeXBRLProcessor.calls == []

  # 2. The incremental refresh with the page healthy picks the filing up.
  w.edgar.page_down = False
  _next_night(w)

  _download(w)
  master = _master(w)
  assert master is not None
  assert PAGED in master["filings"]["accessionNumber"]
  assert master["_metadata"]["paginationFilesMerged"] == 1

  assert requeue_failed_files(w.session, quarter=QUARTER) == 1
  _process(w)

  assert _row(w, PAGED).status == "success"
  assert FakeXBRLProcessor.calls == [(PAGED, "10-K")]

  # 3. A later filing lands while the header read fails: the snapshot stays
  # stale, yet the ZIP is registered and the next pass processes it once.
  w.edgar.recent = [(LATE, "10-Q"), (RECENT, "8-K")]
  w.edgar.quarter_hits = [(PAGED, "10-K"), (LATE, "10-Q")]
  w.edgar.header_down = True
  _place_zip(w.s3, LATE)

  _download(w)

  stale = _master(w)
  assert stale is not None
  assert LATE not in stale["filings"]["accessionNumber"]
  assert _row(w, LATE).status == "pending"

  w.edgar.header_down = False
  _process(w)
  _process(w)

  late = _row(w, LATE)
  assert late.status == "success"
  assert late.attempts == 1
  assert _row(w, PAGED).status == "success"
  assert FakeXBRLProcessor.calls == [(PAGED, "10-K"), (LATE, "10-Q")]
