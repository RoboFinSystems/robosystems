import hashlib
from datetime import datetime
from typing import Any

from robosystems.logger import logger

# S3 key truncation lengths for readability
FACT_ID_TRUNCATE_LENGTH = 8
CONTENT_HASH_TRUNCATE_LENGTH = 12
CONTENT_HASH_LOG_LENGTH = 8


class TextBlockExternalizer:
  def __init__(
    self,
    s3_client,
    bucket: str,
    cdn_url: str | None,
    threshold: int,
    enabled: bool = True,
  ):
    self.s3_client = s3_client
    self.bucket = bucket
    self.cdn_url = cdn_url
    self.threshold = threshold
    self.enabled = enabled

    self.upload_queue: list[tuple[str, str, str]] = []
    self.upload_map: dict[str, dict[str, Any]] = {}
    self.content_cache: dict[str, dict[str, Any]] = {}

    if self.enabled and self.bucket:
      logger.info(
        f"S3 externalization enabled: bucket={self.bucket}, threshold={self.threshold} bytes"
      )

  def should_externalize(self, value: Any) -> bool:
    if not self.enabled or not value:
      return False

    value_str = str(value)

    if "<" in value_str and ">" in value_str:
      return True

    return len(value_str) > self.threshold

  def queue_value_for_s3(
    self,
    value: Any,
    fact_id: str,
    entity_data: dict | None,
    report_data: dict | None,
  ) -> dict[str, Any] | None:
    if not self.s3_client or not self.bucket:
      logger.warning("S3 client not initialized, cannot externalize value")
      return None

    try:
      value_str = str(value)

      content_hash = hashlib.sha256(value_str.encode()).hexdigest()

      if content_hash in self.content_cache:
        cached_result = self.content_cache[content_hash]
        logger.debug(
          f"Cache hit for content hash {content_hash[:CONTENT_HASH_LOG_LENGTH]}, reusing URL: {cached_result['url']}"
        )
        return cached_result

      content_type = (
        "text/html" if "<" in value_str and ">" in value_str else "text/plain"
      )
      file_extension = "html" if content_type == "text/html" else "txt"

      s3_key = self._generate_s3_key_with_hash(
        content_hash, entity_data, report_data, file_extension
      )

      if self._check_s3_object_exists(s3_key):
        logger.debug(
          f"S3 object already exists for content hash {content_hash[:CONTENT_HASH_LOG_LENGTH]}: {s3_key}"
        )
        if self.cdn_url:
          external_url = f"{self.cdn_url}/{s3_key}"
        else:
          external_url = f"https://{self.bucket}.s3.amazonaws.com/{s3_key}"

        result = {
          "url": external_url,
          "value_type": "external",
          "content_type": content_type,
        }

        self.content_cache[content_hash] = result
        return result

      self.upload_queue.append((value_str, self.bucket, s3_key))

      if self.cdn_url:
        external_url = f"{self.cdn_url}/{s3_key}"
      else:
        external_url = f"https://{self.bucket}.s3.amazonaws.com/{s3_key}"

      self.upload_map[fact_id] = {
        "url": external_url,
        "key": s3_key,
        "content_type": content_type,
      }

      result = {
        "url": external_url,
        "value_type": "external",
        "content_type": content_type,
      }

      self.content_cache[content_hash] = result

      return result

    except Exception as e:
      logger.error(f"Error queueing value for S3: {e}")
      return None

  def process_batch_uploads(self) -> None:
    if not self.upload_queue:
      return

    if not self.s3_client:
      logger.warning("S3 client not initialized, cannot process batch uploads")
      return

    logger.info(f"Starting batch upload of {len(self.upload_queue)} items to S3")

    results = self.s3_client.batch_upload_strings(
      items=self.upload_queue,
      content_type=None,
      max_workers=10,
      max_retries=3,
    )

    successful = sum(1 for success in results.values() if success)
    failed = len(results) - successful

    if failed > 0:
      logger.warning(
        f"Batch S3 upload completed with {failed} failures out of {len(results)} total"
      )
      for key, success in results.items():
        if not success:
          logger.error(f"Failed to upload: {key}")
    else:
      logger.info(f"Successfully uploaded all {successful} items to S3")

    self.upload_queue = []

  def externalize_value_to_s3(
    self,
    value: Any,
    fact_id: str,
    entity_data: dict | None,
    report_data: dict | None,
  ) -> dict[str, Any] | None:
    if not self.s3_client or not self.bucket:
      logger.warning("S3 client not initialized, cannot externalize value")
      return None

    try:
      value_str = str(value)

      content_type = (
        "text/html" if "<" in value_str and ">" in value_str else "text/plain"
      )
      file_extension = "html" if content_type == "text/html" else "txt"

      year = None
      cik = None
      accession = None

      if report_data:
        filing_date = report_data.get("filing_date")
        if filing_date:
          year = filing_date[:4]

        if entity_data:
          cik = entity_data.get("cik")

        accession = report_data.get("accession_number")

      if not year:
        year = datetime.now().strftime("%Y")
      if not cik:
        cik = "unknown"
      if not accession:
        accession = "unknown"

      fact_id_short = (
        fact_id[:FACT_ID_TRUNCATE_LENGTH]
        if len(fact_id) > FACT_ID_TRUNCATE_LENGTH
        else fact_id
      )
      s3_key = f"{year}/{cik}/{accession}/fact_{fact_id_short}.{file_extension}"

      logger.debug(f"Uploading large value to S3: s3://{self.bucket}/{s3_key}")
      self.s3_client.upload_string(
        content=value_str,
        bucket=self.bucket,
        key=s3_key,
        content_type=content_type,
      )

      if self.cdn_url:
        external_url = f"{self.cdn_url}/{s3_key}"
      else:
        external_url = f"s3://{self.bucket}/{s3_key}"

      return {
        "url": external_url,
        "value_type": "external_resource",
        "value_size": len(value_str),
        "content_type": content_type,
      }

    except Exception as e:
      logger.error(f"Failed to externalize value to S3: {e}")
      return None

  def _generate_s3_key(
    self,
    fact_id: str,
    entity_data: dict | None,
    report_data: dict | None,
    file_extension: str,
  ) -> str:
    year = None
    cik = None
    accession = None

    if report_data:
      filing_date = report_data.get("filing_date")
      if filing_date:
        year = filing_date[:4]

      if entity_data:
        cik = entity_data.get("cik")

      accession = report_data.get("accession_number")

    if not year:
      year = datetime.now().strftime("%Y")
    if not cik:
      cik = "unknown"
    if not accession:
      accession = "unknown"

    fact_id_short = (
      fact_id[:FACT_ID_TRUNCATE_LENGTH]
      if len(fact_id) > FACT_ID_TRUNCATE_LENGTH
      else fact_id
    )
    s3_key = f"{year}/{cik}/{accession}/fact_{fact_id_short}.{file_extension}"

    return s3_key

  def _generate_s3_key_with_hash(
    self,
    content_hash: str,
    entity_data: dict | None,
    report_data: dict | None,
    file_extension: str,
  ) -> str:
    year = None
    cik = None
    accession = None

    if report_data:
      filing_date = report_data.get("filing_date")
      if filing_date:
        year = filing_date[:4]

      if entity_data:
        cik = entity_data.get("cik")

      accession = report_data.get("accession_number")

    if not year:
      year = datetime.now().strftime("%Y")
    if not cik:
      cik = "unknown"
    if not accession:
      accession = "unknown"

    content_hash_short = content_hash[:CONTENT_HASH_TRUNCATE_LENGTH]
    s3_key = f"{year}/{cik}/{accession}/fact_{content_hash_short}.{file_extension}"

    return s3_key

  def _check_s3_object_exists(self, s3_key: str) -> bool:
    if not self.s3_client or not self.bucket:
      return False

    try:
      return self.s3_client.object_exists(self.bucket, s3_key)
    except Exception as e:
      logger.debug(f"Error checking S3 object existence: {e}")
      return False
