"""SEC filer and report metadata from S3 submissions snapshots, with an API fallback."""

import json
from typing import Any, cast

from robosystems.config.storage.shared import DataSourceType, get_raw_key
from robosystems.logger import get_logger

logger = get_logger(__name__)


class SECMetadataLoader:
  """Caches each CIK's submissions in memory for the processing run."""

  def __init__(self):
    self._cache: dict[str, dict] = {}

  def clear_cache(self) -> None:
    self._cache.clear()

  def _load_submissions_from_s3(self, s3_client, bucket: str, cik: str) -> dict | None:
    """Load a CIK's submissions snapshot from S3, or None if absent."""
    try:
      s3_key = get_raw_key(DataSourceType.SEC, "submissions", f"{cik}.json")
      response = s3_client.get_object(Bucket=bucket, Key=s3_key)
      return json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
      return None
    except Exception as e:
      logger.debug("Failed to load submissions from S3 for CIK %s: %s", cik, e)
      return None

  def get_metadata(
    self,
    cik: str,
    accession: str,
    s3_client=None,
    bucket: str | None = None,
  ) -> tuple[dict, dict]:
    """``(sec_filer, sec_report)`` for a filing; ``accession`` carries dashes.

    Uses the download phase's S3 snapshot, calling the SEC API only without one.
    """
    from robosystems.adapters.sec.client.edgar import edgar_client

    submissions: dict[str, Any] | None = None

    if cik in self._cache:
      submissions = self._cache[cik]

    if submissions is None and s3_client is not None and bucket is not None:
      submissions = self._load_submissions_from_s3(s3_client, bucket, cik)
      if submissions:
        self._cache[cik] = submissions

    if submissions is None:
      logger.warning("No S3 snapshot for CIK %s, falling back to SEC API", cik)
      submissions = cast(dict[str, Any], edgar_client().submissions(cik))
      self._cache[cik] = submissions

    sec_filer = {
      "cik": cik,
      "name": submissions.get("name"),
      "entity_name": submissions.get("name"),  # Alternative key used by processor
      "ticker": (
        submissions.get("tickers", [None])[0] if submissions.get("tickers") else None
      ),
      "exchange": (
        submissions.get("exchanges", [None])[0]
        if submissions.get("exchanges")
        else None
      ),
      "sic": submissions.get("sic"),
      "sicDescription": submissions.get("sicDescription"),
      "stateOfIncorporation": submissions.get("stateOfIncorporation"),
      "fiscalYearEnd": submissions.get("fiscalYearEnd"),
      "ein": submissions.get("ein"),
      "entityType": submissions.get("entityType"),
      "category": submissions.get("category"),
      "website": submissions.get("website") or submissions.get("investorWebsite"),
      "phone": submissions.get("phone"),
    }

    # Two shapes appear here: the merged snapshot written by the download
    # phase puts the filing columns directly under "filings", while a raw SEC
    # submissions.json nests the first page under "filings"."recent".
    sec_report: dict = {"accessionNumber": accession}
    filings_data = submissions.get("filings", {})

    if "accessionNumber" in filings_data:
      filings = filings_data
    else:
      filings = filings_data.get("recent", {})

    def safe_get(field: str, idx: int, default=None):
      """Safely get value from filings list with bounds checking."""
      lst = filings.get(field, [])
      return lst[idx] if idx < len(lst) else default

    if filings and "accessionNumber" in filings:
      accession_numbers = filings["accessionNumber"]
      for i, acc_num in enumerate(accession_numbers):
        if acc_num == accession:
          sec_report = {
            "accessionNumber": accession,
            "form": safe_get("form", i),
            "filingDate": safe_get("filingDate", i),
            "reportDate": safe_get("reportDate", i),
            "acceptanceDateTime": safe_get("acceptanceDateTime", i),
            "primaryDocument": safe_get("primaryDocument", i),
            "periodOfReport": safe_get("periodOfReport", i),
            "isXBRL": bool(safe_get("isXBRL", i, False)),
            "isInlineXBRL": bool(safe_get("isInlineXBRL", i, False)),
          }
          break

    return sec_filer, sec_report
