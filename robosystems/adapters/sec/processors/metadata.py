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
    # The live SEC header per CIK, read once when its snapshot missed an
    # accession and searched for every later miss from that filer.
    self._live: dict[str, dict] = {}

  def clear_cache(self) -> None:
    self._cache.clear()
    self._live.clear()

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

    Uses the download phase's S3 snapshot, calling the SEC API without one,
    or once per CIK when the snapshot does not hold the accession.
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
      self._live[cik] = submissions

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

    sec_report = _find_report(submissions, accession)
    if sec_report is None:
      if cik not in self._live:
        # A snapshot can predate the filing (a failed refresh, a short master);
        # the live header's recent page carries every new filing.
        logger.warning(
          "Accession %s not in the submissions snapshot for CIK %s; re-reading the SEC API",
          accession,
          cik,
        )
        self._live[cik] = cast(dict[str, Any], edgar_client().submissions(cik))
      sec_report = _find_report(self._live[cik], accession)
    if sec_report is None:
      sec_report = {"accessionNumber": accession}

    return sec_filer, sec_report


def _find_report(submissions: dict, accession: str) -> dict | None:
  """The filing's row from a submissions document, or None when absent.

  Two shapes appear here: the merged snapshot written by the download phase
  puts the filing columns directly under "filings", while a raw SEC
  submissions.json nests the first page under "filings"."recent".
  """
  filings_data = submissions.get("filings", {})
  filings = (
    filings_data
    if "accessionNumber" in filings_data
    else filings_data.get("recent", {})
  )

  def safe_get(field: str, idx: int, default=None):
    lst = filings.get(field, [])
    return lst[idx] if idx < len(lst) else default

  for i, acc_num in enumerate(filings.get("accessionNumber", []) if filings else []):
    if acc_num == accession:
      return {
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
  return None
