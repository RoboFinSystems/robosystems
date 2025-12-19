import os
from io import BytesIO
from typing import cast
from zipfile import BadZipFile, ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from retrying import retry

from robosystems.config import ExternalServicesConfig
from robosystems.logger import logger

SEC_CONFIG = ExternalServicesConfig.SEC_CONFIG
SEC_BASE_URL = SEC_CONFIG["base_url"]
SEC_DATA_BASE_URL = SEC_CONFIG["data_base_url"]
SEC_HEADERS = SEC_CONFIG["headers"]
SEC_REQUEST_TIMEOUT = 30  # seconds


# Global flag for test mode (can be set by tests)
_test_mode = False


# Create conditional decorator that uses fast retries in test environment
def conditional_sec_retry(**retry_kwargs):
  """Apply retry decorator with test-friendly settings."""

  def decorator(func):
    def wrapper(*args, **kwargs):
      # Check if we're in a test environment at call time
      is_testing = (
        _test_mode or os.getenv("PYTEST_CURRENT_TEST") or os.getenv("TESTING")
      )
      if is_testing:
        # Use very fast retries for tests - create a new retry decorator
        test_kwargs = retry_kwargs.copy()
        if "wait_fixed" in test_kwargs:
          test_kwargs["wait_fixed"] = 1  # 1ms fixed wait
        if "wait_random_min" in test_kwargs:
          test_kwargs["wait_random_min"] = 1
        if "wait_random_max" in test_kwargs:
          test_kwargs["wait_random_max"] = 5
        # Apply fast retry and call
        fast_retry_func = retry(**test_kwargs)(func)
        return fast_retry_func(*args, **kwargs)
      else:
        # Apply normal retry and call
        normal_retry_func = retry(**retry_kwargs)(func)
        return normal_retry_func(*args, **kwargs)

    return wrapper

  return decorator


# Function to enable test mode
def enable_test_mode():
  global _test_mode
  _test_mode = True


def disable_test_mode():
  global _test_mode
  _test_mode = False


class SECClient:
  def __init__(self, cik=None):
    logger.debug(f"Initializing SECClient with CIK: {cik}")
    self._headers = SEC_HEADERS
    self.cik = str(cik) if cik is not None else None

  @conditional_sec_retry(
    stop_max_attempt_number=2,  # Only retry once
    wait_fixed=45000,  # Wait 45 seconds if rate limited
    retry_on_exception=lambda e: isinstance(e, (requests.HTTPError, ValueError))
    and "rate limited" in str(e),
  )
  def get_companies(self):
    logger.debug("Fetching companies from SEC")
    url = os.path.join(SEC_BASE_URL, "files/company_tickers.json")
    try:
      logger.debug(f"Making request to {url}")
      resp = requests.get(url, headers=self._headers, timeout=SEC_REQUEST_TIMEOUT)
      logger.debug(f"Request status code: {resp.status_code}")

      # Check for empty response (SEC throttling)
      if not resp.content or len(resp.content.strip()) == 0:
        logger.warning(
          "SEC returned empty response for companies - rate limited, will retry after delay"
        )
        raise requests.HTTPError("SEC returned empty response - rate limited")

      try:
        result = resp.json()
      except ValueError:
        logger.warning("SEC returned non-JSON response for companies")
        raise requests.HTTPError("SEC returned invalid JSON - rate limited")

      logger.debug(f"Successfully retrieved {len(result)} companies")
      return result
    except Exception as e:
      logger.error(f"Error getting companies: {e}")
      raise e

  def get_companies_df(self):
    logger.debug("Converting companies JSON to DataFrame")
    co_json = self.get_companies()
    df = pd.DataFrame(co_json)
    result = df.T
    logger.debug(f"Created DataFrame with {len(result)} rows")
    return result

  @conditional_sec_retry(
    stop_max_attempt_number=2,  # Only retry once after initial attempt
    wait_fixed=60000,  # Wait 60 seconds between attempts when rate limited
    retry_on_exception=lambda e: isinstance(e, requests.HTTPError)
    and "rate limited" in str(e),
  )
  def get_submissions(self, file=None):
    if self.cik is None:
      raise ValueError("CIK is required for get_submissions")
    full_cik = str(self.cik).zfill(10)
    file_to_use = file if file else f"CIK{full_cik}.json"
    logger.debug(f"Fetching submissions from file: {file_to_use}")

    url = os.path.join(SEC_DATA_BASE_URL, "submissions", file_to_use)
    try:
      logger.debug(f"Making request to {url}")
      resp = requests.get(url, headers=self._headers, timeout=SEC_REQUEST_TIMEOUT)
      logger.debug(f"Request status code: {resp.status_code}")

      # Check for empty response (SEC throttling behavior)
      if not resp.content or len(resp.content.strip()) == 0:
        logger.warning(
          f"SEC returned empty response for {self.cik} - likely rate limited"
        )
        raise requests.HTTPError("SEC returned empty response - rate limited")

      # Try to parse JSON
      try:
        result = resp.json()
      except ValueError as json_error:
        # SEC sometimes returns HTML or empty content when rate limited
        logger.warning(
          f"SEC returned non-JSON response for {self.cik}: {str(json_error)[:100]}"
        )
        logger.debug(f"Response content: {resp.content[:200]}")
        raise requests.HTTPError("SEC returned invalid JSON - likely rate limited")

      if "filings" in result:
        logger.debug(
          f"Retrieved {len(result['filings'].get('recent', []))} recent filings"
        )
      return result
    except Exception as e:
      logger.error(f"Error getting submissions for {self.cik}: {e}")
      raise e

  def submissions_df(self):
    logger.debug(f"Creating submissions DataFrame for CIK: {self.cik}")
    subs = self.get_submissions()
    subs_db = pd.DataFrame(subs["filings"]["recent"])
    logger.debug(f"Initial DataFrame size: {len(subs_db)} rows")

    if "files" in subs["filings"]:
      logger.debug(f"Found {len(subs['filings']['files'])} additional files to process")
      for f in subs["filings"]["files"]:
        logger.debug(f"Processing additional file: {f['name']}")
        subs = self.get_submissions(f["name"])
        df = pd.DataFrame(subs)
        subs_db = pd.concat([subs_db, df], ignore_index=True)

    bool_cols = ["isXBRL", "isInlineXBRL"]
    for b in bool_cols:
      subs_db[b] = subs_db[b].astype(bool)

    logger.debug(f"Final submissions DataFrame size: {len(subs_db)} rows")
    return subs_db

  def get_report_url(self, sec_report):
    if self.cik is None:
      logger.error("Cannot generate report URL: CIK is not set")
      return None
    logger.debug(
      f"Getting report URL for accession number: {sec_report['accessionNumber']}"
    )
    accno = sec_report["accessionNumber"].replace("-", "")

    if sec_report["isInlineXBRL"]:
      logger.debug("Report is inline XBRL")
      filename = sec_report["primaryDocument"]
      accno = sec_report["accessionNumber"].replace("-", "")
      # For EDGAR URLs, CIK should not have leading zeros
      cik_no_leading_zeros = str(int(self.cik))
      url = os.path.join(
        SEC_BASE_URL, "Archives/edgar/data", cik_no_leading_zeros, accno, filename
      )
      logger.debug(f"Generated URL: {url}")
      return url
    elif not sec_report["isInlineXBRL"]:
      logger.debug("Report is not inline XBRL, getting XBRL ZIP URL")
      xbrlzip_url = self.get_xbrlzip_url(sec_report)
      logger.debug(f"Downloading XBRL ZIP from: {xbrlzip_url}")
      xbrl_zip = self.download_xbrlzip(xbrlzip_url)

      if xbrl_zip is None:
        logger.debug("XBRL ZIP download failed, falling back to largest XML file")
        accno = sec_report["accessionNumber"].replace("-", "")
        if self.cik is None:
          logger.error("Cannot generate filing URL: CIK is not set")
          return None
        # For EDGAR URLs, CIK should not have leading zeros
        cik_no_leading_zeros = str(int(self.cik))
        filing_url = os.path.join(
          SEC_BASE_URL, "Archives/edgar/data", cik_no_leading_zeros, accno
        )
        return self.get_largest_xml_file(filing_url)

      filename = None
      logger.debug(
        f"Searching for XSD file in ZIP with {len(xbrl_zip.namelist())} files"
      )
      for f in xbrl_zip.namelist():
        if ".xsd" in f:
          filename = f.replace(".xsd", ".xml")
          logger.debug(f"Found XSD file, corresponding XML file: {filename}")
          break

      if filename is None:
        logger.warning("No XSD file found in XBRL ZIP")
        return None

      # For EDGAR URLs, CIK should not have leading zeros
      cik_no_leading_zeros = str(int(self.cik))
      url = os.path.join(
        SEC_BASE_URL, "Archives/edgar/data", cik_no_leading_zeros, accno, filename
      )
      logger.debug(f"Generated URL: {url}")
      return url

  @conditional_sec_retry(
    stop_max_attempt_number=2,  # Only retry once
    wait_fixed=75000,  # Wait 75 seconds if rate limited (longer for downloads)
    retry_on_exception=lambda e: isinstance(e, requests.HTTPError)
    and "rate limited" in str(e),
  )
  def download_xbrlzip(self, xbrlzip_url):
    logger.debug(f"Downloading XBRL ZIP from: {xbrlzip_url}")
    try:
      response = requests.get(
        xbrlzip_url, headers=self._headers, timeout=SEC_REQUEST_TIMEOUT
      )

      # Check for empty response (SEC throttling)
      if not response.content or len(response.content) == 0:
        logger.warning(
          f"SEC returned empty file for {xbrlzip_url} - rate limited, will retry after delay"
        )
        raise requests.HTTPError("SEC returned empty file - rate limited")

      resp = response.content
      logger.debug(f"Downloaded ZIP file of size: {len(resp)} bytes")
      xbrl_zip = ZipFile(BytesIO(resp))
      logger.debug(
        f"Successfully created ZIP object with {len(xbrl_zip.namelist())} files"
      )
      return xbrl_zip
    except Exception as e:
      if isinstance(e, BadZipFile):
        logger.error(f"Error downloading XBRL zip for {xbrlzip_url}: {e}")
        return None
      else:
        logger.error(f"Unexpected error downloading XBRL zip: {e}")
        raise e

  def get_xbrlzip_url(self, filing):
    if self.cik is None:
      raise ValueError("CIK is required for get_xbrlzip_url")
    logger.debug(f"Generating XBRL ZIP URL for filing: {filing['accessionNumber']}")
    long_accno = filing["accessionNumber"]
    accno = long_accno.replace("-", "")
    filename = f"{long_accno}-xbrl.zip"
    # For EDGAR URLs, CIK should not have leading zeros
    cik_no_leading_zeros = str(int(self.cik))
    url = os.path.join(
      SEC_BASE_URL, "Archives/edgar/data", cik_no_leading_zeros, accno, filename
    )
    logger.debug(f"Generated XBRL ZIP URL: {url}")
    return url

  @conditional_sec_retry(
    stop_max_attempt_number=3, wait_random_min=5000, wait_random_max=10000
  )
  def get_largest_xml_file(self, filing_url):
    """
    Scrapes the SEC filing page and returns information about the largest XML file.

    Args:
        filing_url (str): The URL of the SEC filing page

    Returns:
        dict: Information about the largest XML file including:
            - filename: Name of the file
            - size: Size in bytes
            - url: Full URL to the file
    """
    logger.debug(f"Finding largest XML file at URL: {filing_url}")
    try:
      logger.debug(f"Making request to {filing_url}")
      resp = requests.get(
        filing_url, headers=self._headers, timeout=SEC_REQUEST_TIMEOUT
      )
      logger.debug(f"Request status code: {resp.status_code}")
      soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
      logger.error(f"Error getting largest XML file for {filing_url}: {e}")
      raise e

    # Find the table with file information
    table = soup.find("table")
    if not table:
      logger.error("Could not find file table on the page")
      return None
    table = cast(Tag, table)

    largest_xml_url = None
    max_size = 0

    # Process each row in the table
    logger.debug("Scanning table for XML files")
    xml_files_count = 0
    for row in table.find_all("tr")[1:]:  # Skip header row
      row = cast(Tag, row)
      cols = row.find_all("td")
      filename = cols[0].text.strip()
      if not filename.endswith(".xml"):
        continue

      xml_files_count += 1
      size_text = cols[1].text.strip()
      if size_text == "":
        continue

      size = float(size_text)
      if size > max_size:
        max_size = size
        col_tag = cast(Tag, cols[0])
        link_element = col_tag.find("a")
        if link_element:
          link_element = cast(Tag, link_element)
          file_url = link_element.get("href")
          largest_xml_url = file_url
        logger.debug(f"Found larger XML file: {filename}, size: {size}")

    logger.debug(f"Found {xml_files_count} XML files in total")
    if largest_xml_url is None:
      logger.error(f"Could not find XML file for {filing_url}")
      return None

    url = SEC_BASE_URL + largest_xml_url
    logger.debug(f"Largest XML file URL: {url}")
    return url
