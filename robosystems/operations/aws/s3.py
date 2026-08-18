"""
S3 adapter for graph database backup storage with compression and lifecycle management.
"""

import asyncio
import gzip
import hashlib
import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, NoCredentialsError

from robosystems.config import env
from robosystems.config.tuning import TuningConfig
from robosystems.logger import logger


class S3Client:
  """Sync S3 client for general object storage (XBRL textblocks, temp files).

  Most methods swallow errors and return a falsy value rather than raising,
  so callers can translate to HTTP without try/except. ``iter_object_keys``
  is the exception: a failed or truncated listing must not look empty.
  """

  def __init__(self, region_name: str | None = None, endpoint_url: str | None = None):
    """Defaults to ``env.AWS_DEFAULT_REGION`` / ``env.AWS_ENDPOINT_URL``."""
    self.region_name = region_name or env.AWS_DEFAULT_REGION
    self.endpoint_url = endpoint_url or env.AWS_ENDPOINT_URL
    # Optional second endpoint used only when *signing presigned URLs*.
    # In dev the API container reaches LocalStack via docker DNS
    # (``http://localstack:4566``), but that hostname isn't reachable from
    # the host browser. Setting ``AWS_S3_PRESIGN_ENDPOINT_URL`` to
    # ``http://localhost:4566`` produces signed URLs that the browser can
    # follow. Unset in staging/prod — boto3's default real-AWS endpoints
    # are already browser-reachable, so the override stays None and the
    # presign client is the same instance as the upload/download client.
    self.presign_endpoint_url = env.AWS_S3_PRESIGN_ENDPOINT_URL or None

    # Build the primary client (used for upload, download, head, list, etc.).
    self.s3_client = self._build_client(self.endpoint_url)

    # Lazy-initialised secondary client for presigned URL signing.
    # ``_build_client`` is cheap (boto3 creates the underlying session on
    # first request), so we materialise this up front only when the
    # override is actually set; otherwise reuse the primary client.
    self._presign_client = (
      self._build_client(self.presign_endpoint_url)
      if self.presign_endpoint_url
      else self.s3_client
    )

    logger.debug(f"Initialized S3Client for region {self.region_name}")

  def _build_client(self, endpoint_url: str | None) -> Any:
    """Construct an underlying boto3 ``s3`` client.

    Centralises the credential + retry config so both the primary
    upload/download client and the optional presign-only client land
    on the same defaults.
    """
    s3_config: dict[str, Any] = {
      "region_name": self.region_name,
    }
    # Only include endpoint_url if set (empty string breaks boto3)
    if endpoint_url:
      s3_config["endpoint_url"] = endpoint_url

    # Prefer IAM roles over access keys for security
    # In production/staging: Use ECS task role automatically
    # In development: Use AWS CLI profile or access keys as fallback
    if env.ENVIRONMENT in ["prod", "staging"]:
      # Use IAM role automatically - boto3 will detect ECS task role
      logger.debug("Using IAM role for S3 access (production/staging)")
    elif env.AWS_S3_ACCESS_KEY_ID:
      # Development fallback: use access keys if provided
      logger.debug("Using access keys for S3 access (development)")
      s3_config["aws_access_key_id"] = env.AWS_S3_ACCESS_KEY_ID
      if env.AWS_S3_SECRET_ACCESS_KEY:
        s3_config["aws_secret_access_key"] = env.AWS_S3_SECRET_ACCESS_KEY
    else:
      # Development: try to use AWS CLI profile
      logger.debug("Using default AWS credentials chain for S3 access")

    return boto3.client(
      "s3",
      config=BotoConfig(retries={"mode": "adaptive", "max_attempts": 3}),
      **s3_config,
    )

  def upload_string(
    self,
    content: str,
    bucket: str,
    key: str,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
  ) -> bool:
    """Upload a UTF-8 string as an S3 object. False on any failure.

    Retries come from the boto3 client's adaptive retry configuration.
    """
    content_bytes = content.encode("utf-8")

    put_args: dict[str, Any] = {
      "Bucket": bucket,
      "Key": key,
      "Body": content_bytes,
    }

    if content_type:
      put_args["ContentType"] = content_type

    if metadata:
      put_args["Metadata"] = metadata

    security_errors = {"AccessDenied"}

    try:
      self.s3_client.put_object(**put_args)
      logger.debug(
        f"Successfully uploaded {len(content_bytes)} bytes to s3://{bucket}/{key}"
      )
      return True

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "")
      if error_code in security_errors:
        logger.critical(
          f"S3 SECURITY VIOLATION - {error_code}: Bucket={bucket}, Key={key}, "
          f"Error={e!s}"
        )
      else:
        logger.error(f"S3 upload failed ({error_code}): {e}")
      return False

    except Exception as e:
      logger.error(f"Unexpected error uploading to S3: {e}")
      return False

  def upload_bytes(
    self,
    content: bytes,
    bucket: str,
    key: str,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
  ) -> bool:
    """Upload raw bytes as an S3 object. False on any failure.

    The binary sibling of :meth:`upload_string`, for artifacts that aren't
    UTF-8 text (e.g. the XBRL serialization zip).
    """
    put_args: dict[str, Any] = {
      "Bucket": bucket,
      "Key": key,
      "Body": content,
    }

    if content_type:
      put_args["ContentType"] = content_type

    if metadata:
      put_args["Metadata"] = metadata

    security_errors = {"AccessDenied"}

    try:
      self.s3_client.put_object(**put_args)
      logger.debug(f"Successfully uploaded {len(content)} bytes to s3://{bucket}/{key}")
      return True

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "")
      if error_code in security_errors:
        logger.critical(
          f"S3 SECURITY VIOLATION - {error_code}: Bucket={bucket}, Key={key}, "
          f"Error={e!s}"
        )
      else:
        logger.error(f"S3 upload failed ({error_code}): {e}")
      return False

    except Exception as e:
      logger.error(f"Unexpected error uploading bytes to S3: {e}")
      return False

  def upload_file(
    self,
    file_path: str,
    bucket: str,
    key: str,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
  ) -> bool:
    """Upload a local file to S3. False on any failure."""
    extra_args = {}
    if content_type:
      extra_args["ContentType"] = content_type
    if metadata:
      extra_args["Metadata"] = metadata

    try:
      self.s3_client.upload_file(
        file_path, bucket, key, ExtraArgs=extra_args if extra_args else None
      )
      logger.debug(f"Successfully uploaded file {file_path} to s3://{bucket}/{key}")
      return True

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "")
      logger.error(f"S3 file upload failed ({error_code}): {e}")
      return False

    except Exception as e:
      logger.error(f"Unexpected error uploading file to S3: {e}")
      return False

  def download_file(
    self,
    bucket: str,
    key: str,
    file_path: str,
  ) -> bool:
    """Download an S3 object to a local path. False if absent or unreadable."""
    try:
      self.s3_client.download_file(bucket, key, file_path)
      logger.debug(f"Downloaded s3://{bucket}/{key} to {file_path}")
      return True

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "")
      if error_code == "AccessDenied":
        logger.error(f"S3 download access denied: s3://{bucket}/{key}")
      else:
        logger.debug(f"S3 download not available ({error_code}): s3://{bucket}/{key}")
      return False

    except Exception as e:
      logger.debug(f"S3 download not available: {e}")
      return False

  def download_string(self, bucket: str, key: str) -> str | None:
    """Download an S3 object as a UTF-8 string, or None if unavailable."""
    try:
      response = self.s3_client.get_object(Bucket=bucket, Key=key)
      content = response["Body"].read().decode("utf-8")
      logger.debug(
        f"Successfully downloaded {len(content)} characters from s3://{bucket}/{key}"
      )
      return content

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "")
      if error_code == "NoSuchKey":
        logger.warning(f"Object not found: s3://{bucket}/{key}")
      else:
        logger.error(f"S3 download failed ({error_code}): {e}")
      return None

    except Exception as e:
      logger.error(f"Unexpected error downloading from S3: {e}")
      return None

  def delete_object(self, bucket: str, key: str) -> bool:
    """Delete an S3 object. Denials are logged at CRITICAL for the audit trail."""
    try:
      self.s3_client.delete_object(Bucket=bucket, Key=key)
      logger.debug(f"Successfully deleted s3://{bucket}/{key}")
      return True

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "")

      # Log security errors for audit trail
      if error_code in {"AccessDenied", "UnauthorizedAccess"}:
        logger.critical(
          f"S3 SECURITY VIOLATION - {error_code}: Delete operation denied for "
          f"Bucket={bucket}, Key={key}, Error={e!s}"
        )
      else:
        logger.error(f"Failed to delete from S3: {e}")
      return False
    except Exception as e:
      logger.error(f"Unexpected error deleting from S3: {e}")
      return False

  def object_exists(self, bucket: str, key: str) -> bool:
    """True when the object exists. Errors other than 404 also return False."""
    try:
      self.s3_client.head_object(Bucket=bucket, Key=key)
      return True
    except ClientError as e:
      if e.response["Error"]["Code"] == "404":
        return False
      else:
        logger.error(f"Error checking object existence: {e}")
        return False
    except Exception as e:
      logger.error(f"Unexpected error checking object existence: {e}")
      return False

  def generate_presigned_url(
    self,
    bucket: str,
    key: str,
    expires_in: int = 300,
    response_content_type: str | None = None,
    response_content_disposition: str | None = None,
  ) -> str | None:
    """Sign a time-limited download URL, or None if signing fails.

    ``response_content_type`` / ``response_content_disposition`` override the
    headers S3 returns, for artifacts whose stored ``Content-Type`` doesn't
    match the desired download behaviour. Use the async
    :meth:`S3BackupAdapter.generate_download_url` inside an event loop.
    """
    params: dict[str, Any] = {"Bucket": bucket, "Key": key}
    if response_content_type:
      params["ResponseContentType"] = response_content_type
    if response_content_disposition:
      params["ResponseContentDisposition"] = response_content_disposition
    try:
      # ``_presign_client`` is the same instance as ``s3_client`` unless
      # ``AWS_S3_PRESIGN_ENDPOINT_URL`` is set (dev-only LocalStack path).
      return self._presign_client.generate_presigned_url(
        "get_object",
        Params=params,
        ExpiresIn=expires_in,
      )
    except ClientError as e:
      logger.error(
        f"Failed to sign presigned URL for s3://{bucket}/{key}: {e}",
      )
      return None
    except Exception as e:
      logger.error(
        f"Unexpected error signing presigned URL for s3://{bucket}/{key}: {e}",
      )
      return None

  def list_objects(
    self, bucket: str, prefix: str | None = None, max_keys: int = 1000
  ) -> list[str]:
    """List object keys under ``prefix``. Single page — no pagination."""
    try:
      params = {
        "Bucket": bucket,
        "MaxKeys": max_keys,
      }

      if prefix:
        params["Prefix"] = prefix

      response = self.s3_client.list_objects_v2(**params)

      if "Contents" not in response:
        return []

      return [obj["Key"] for obj in response["Contents"]]

    except ClientError as e:
      logger.error(f"Failed to list objects from S3: {e}")
      return []
    except Exception as e:
      logger.error(f"Unexpected error listing objects from S3: {e}")
      return []

  def iter_object_keys(self, bucket: str, prefix: str | None = None) -> Iterator[str]:
    """Yield every object key under ``prefix``.

    Paginates ``list_objects_v2``. Raises on S3 failure so a caller can
    tell a failed list from an empty prefix — ``list_objects`` cannot.
    """
    paginator = self.s3_client.get_paginator("list_objects_v2")
    params: dict[str, Any] = {"Bucket": bucket}
    if prefix:
      params["Prefix"] = prefix
    for page in paginator.paginate(**params):
      for obj in page.get("Contents") or []:
        key = obj.get("Key")
        if key:
          yield key

  def batch_upload_strings(
    self,
    items: list[tuple[str, str, str]],  # List of (content, bucket, key) tuples
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
    max_workers: int | None = None,
  ) -> dict[str, bool]:
    """Upload ``(content, bucket, key)`` triples in parallel, keyed by S3 key.

    ``max_workers`` defaults to the SSM-tunable ``TuningConfig`` value. An item
    that raises rather than returning False is logged but omitted from the
    result dict, so a caller checking coverage must compare against ``items``.
    """
    if max_workers is None:
      max_workers = TuningConfig.get_max_workers()

    results = {}

    def upload_item(item: tuple[str, str, str]) -> tuple[str, bool]:
      """Upload a single item."""
      content, bucket, key = item
      success = self.upload_string(
        content=content,
        bucket=bucket,
        key=key,
        content_type=content_type,
        metadata=metadata,
      )
      return key, success

    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
      # Submit all upload tasks
      futures = [executor.submit(upload_item, item) for item in items]

      # Collect results as they complete
      for future in futures:
        try:
          key, success = future.result()
          results[key] = success
          if not success:
            logger.warning(f"Failed to upload batch item: {key}")
        except Exception as e:
          logger.error(f"Error in batch upload: {e}")
          # If we can extract the key from the exception context, mark it as failed
          # Otherwise, we'll just log the error

    # Log batch upload summary
    successful = sum(1 for success in results.values() if success)
    total = len(items)
    logger.info(f"Batch upload completed: {successful}/{total} successful")

    return results


@dataclass
class BackupMetadata:
  """Metadata for a graph database backup."""

  graph_id: str
  backup_type: str  # 'full' or 'incremental'
  timestamp: datetime
  original_size: int
  compressed_size: int
  checksum: str
  compression_ratio: float
  node_count: int
  relationship_count: int
  backup_duration_seconds: float
  database_version: str | None = None
  backup_format: str = "cypher"
  s3_key: str | None = None
  # Key of the sidecar JSON written alongside the payload. Carried so the
  # caller can record it: retention deletes the sidecar only when the row knows
  # where it is, and nothing used to tell the row.
  s3_metadata_key: str | None = None
  # Whether the archive carries the graph's semantic memory store: "included",
  # "absent", or None for a backup taken before memory was captured at all.
  # None is a third state, not a synonym for "absent" — see the manifest
  # contract in the backup spec.
  memory: str | None = None
  # Set only when the archive's contents differed from the live database at
  # backup time. Absent on the ordinary path.
  payload_delta: dict[str, Any] | None = None

  def to_dict(self) -> dict[str, Any]:
    """Convert metadata to dictionary for JSON serialization."""
    data = asdict(self)
    data["timestamp"] = self.timestamp.isoformat()
    return data

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> "BackupMetadata":
    """Create metadata from dictionary."""
    data["timestamp"] = datetime.fromisoformat(data["timestamp"])
    return cls(**data)


class S3BackupAdapter:
  """Async S3 store for graph database backups, keyed by graph and timestamp.

  Layout: ``graph-backups/databases/{graph_id}/{backup_type}/backup-{ts}{ext}``
  with a sibling JSON blob at ``graph-backups/metadata/{graph_id}/backup-{ts}``.
  Callers must pass the *same* timestamp to upload and to any later lookup —
  the timestamp is the only join key between the two paths.

  This class compresses; it does not encrypt at the application layer. Objects
  are written with S3 server-side encryption (SSE-AES256), which is what
  protects them at rest.
  """

  def __init__(
    self,
    bucket_name: str | None = None,
    region: str | None = None,
    enable_compression: bool = True,
  ):
    """Defaults to the ``USER_DATA_BUCKET`` / ``AWS_DEFAULT_REGION`` env vars."""
    self.bucket_name = bucket_name or env.USER_DATA_BUCKET
    self.region = region or env.AWS_DEFAULT_REGION
    self.enable_compression = enable_compression

    if not self.bucket_name:
      raise ValueError(
        "S3 bucket name must be provided via parameter or USER_DATA_BUCKET env var"
      )

    # Initialize S3 client
    self._init_s3_client()

    logger.info(
      f"S3BackupAdapter initialized: bucket={self.bucket_name}, "
      f"compression={self.enable_compression}"
    )

  def _init_s3_client(self):
    """Build the boto3 client and fail fast if the bucket isn't reachable."""
    try:
      # Falls back to the general AWS credentials when no S3-specific ones are set.
      s3_config = env.get_s3_config()

      access_key = s3_config.get("aws_access_key_id")
      secret_key = s3_config.get("aws_secret_access_key")
      endpoint_url = s3_config.get("endpoint_url")
      region = s3_config.get("region_name", self.region)

      session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
      )

      from botocore.config import Config

      config = Config(
        max_pool_connections=50,  # Sized for parallel part uploads
        retries={
          "mode": "adaptive",
          "max_attempts": 3,
        },
        s3={
          "use_accelerate_endpoint": False,  # Enable if Transfer Acceleration is on
          "payload_signing_enabled": True,
        },
      )

      # Endpoint override exists for LocalStack in dev; unset against real AWS.
      if endpoint_url:
        self.s3_client = session.client("s3", config=config, endpoint_url=endpoint_url)
        logger.info(f"Using custom S3 endpoint: {endpoint_url}")
      else:
        self.s3_client = session.client("s3", config=config)

      self.multipart_threshold = 100 * 1024 * 1024  # Switch to multipart above 100 MB
      self.multipart_chunksize = 50 * 1024 * 1024  # 50 MB per part

      self.s3_client.head_bucket(Bucket=self.bucket_name)
      logger.info(f"S3 connection established to bucket: {self.bucket_name}")

    except NoCredentialsError:
      logger.error("AWS credentials not found")
      raise
    except ClientError as e:
      logger.error(f"Failed to connect to S3 bucket {self.bucket_name}: {e}")
      raise

  def _generate_backup_path(
    self,
    graph_id: str,
    backup_type: str,
    timestamp: datetime,
    file_extension: str | None = None,
  ) -> str:
    """S3 key for the backup payload. ``file_extension`` wins when given."""
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    if file_extension:
      extension = file_extension
    else:
      extension = ".lbug"
      if self.enable_compression:
        extension += ".gz"

    return f"graph-backups/databases/{graph_id}/{backup_type}/backup-{timestamp_str}{extension}"

  def _generate_metadata_path(self, graph_id: str, timestamp: datetime) -> str:
    """S3 key for the sidecar metadata JSON matching the same timestamp."""
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    return f"graph-backups/metadata/{graph_id}/backup-{timestamp_str}.json"

  # Payload extensions that are already compressed archives. Gzipping one of
  # these buys almost nothing and breaks the contract the extension states: the
  # download endpoint presigns the stored object directly, so a `.lbug.zip` that
  # is really gzip-wrapping-a-zip cannot be opened by anything that trusts the
  # name. Every export format lands as a zip, so in practice this covers them all.
  _ALREADY_COMPRESSED_EXTENSIONS = (".zip", ".gz", ".zst")

  _GZIP_MAGIC = b"\x1f\x8b"

  def _should_compress(self, file_extension: str | None) -> bool:
    """Whether to gzip a payload with this extension before storing it."""
    if not self.enable_compression:
      return False
    if not file_extension:
      return True
    return not file_extension.lower().endswith(self._ALREADY_COMPRESSED_EXTENSIONS)

  def _compress_data(self, data: bytes) -> bytes:
    """Gzip, unless compression is disabled for this adapter."""
    if not self.enable_compression:
      return data

    return gzip.compress(data)

  def _decompress_data(self, data: bytes) -> bytes:
    """Un-gzip a payload if it is gzipped, otherwise return it unchanged.

    Detection is by magic bytes rather than by the adapter's ``enable_compression``
    flag, because the stored population is mixed: objects written before archives
    stopped being double-compressed are gzipped, and newer ones are not. A flag
    cannot describe both, and gzip is self-identifying, so the payload is allowed
    to answer the question itself. This also removes the old requirement that read
    and write agree on a setting that could be changed between them.
    """
    if data[:2] == self._GZIP_MAGIC:
      return gzip.decompress(data)
    return data

  def _calculate_checksum(self, data: bytes) -> str:
    """SHA-256 of the *uncompressed* payload."""
    return hashlib.sha256(data).hexdigest()

  async def _multipart_upload(
    self,
    bucket: str,
    key: str,
    data: bytes,
    metadata: dict[str, str] | None = None,
  ) -> None:
    """Upload ``data`` in ``multipart_chunksize`` parts, aborting on failure.

    The abort is what keeps a failed upload from leaving billable orphan parts
    in the bucket, so the ``except`` branch must never be skipped.
    """
    import math

    response = await asyncio.get_event_loop().run_in_executor(
      None,
      lambda: self.s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=key,
        ContentType="application/octet-stream",
        ServerSideEncryption="AES256",
        Metadata=metadata or {},
      ),
    )

    upload_id = response["UploadId"]
    parts = []

    try:
      total_size = len(data)
      num_parts = math.ceil(total_size / self.multipart_chunksize)

      logger.info(
        f"Multipart upload: {num_parts} parts, {total_size / (1024 * 1024):.1f} MB total"
      )

      for part_num in range(1, num_parts + 1):
        start = (part_num - 1) * self.multipart_chunksize
        end = min(start + self.multipart_chunksize, total_size)
        part_data = data[start:end]

        part_response = await asyncio.get_event_loop().run_in_executor(
          None,
          lambda pn=part_num, pd=part_data: self.s3_client.upload_part(
            Bucket=bucket,
            Key=key,
            PartNumber=pn,
            UploadId=upload_id,
            Body=pd,
          ),
        )

        parts.append(
          {
            "PartNumber": part_num,
            "ETag": part_response["ETag"],
          }
        )

        if part_num % 10 == 0 or part_num == num_parts:
          logger.info(f"Uploaded part {part_num}/{num_parts}")

      await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.complete_multipart_upload(
          Bucket=bucket,
          Key=key,
          UploadId=upload_id,
          MultipartUpload={"Parts": parts},
        ),
      )

      logger.info(f"Multipart upload completed: {key}")

    except Exception as e:
      logger.error(f"Multipart upload failed, aborting: {e}")
      await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.abort_multipart_upload(
          Bucket=bucket,
          Key=key,
          UploadId=upload_id,
        ),
      )
      raise

  async def upload_backup(
    self,
    graph_id: str,
    backup_data: bytes,
    backup_type: str,
    metadata: dict[str, Any],
    timestamp: datetime | None = None,
    file_extension: str | None = None,
  ) -> BackupMetadata:
    """Compress and store a backup plus its metadata sidecar.

    ``backup_type`` must be ``full`` or ``incremental``. ``timestamp`` should
    always be supplied by the caller: it is the key both S3 paths are derived
    from, so generating one here would desynchronise the payload from the
    metadata the caller records. ``file_extension`` overrides the default
    ``.lbug[.gz]`` suffix (e.g. ``.csv.zip``).

    The stored checksum covers the pre-compression bytes.
    """
    if not isinstance(backup_data, bytes):
      raise TypeError(f"backup_data must be bytes, got {type(backup_data)}")

    if len(backup_data) == 0:
      raise ValueError("backup_data cannot be empty")

    if backup_type not in ("full", "incremental"):
      raise ValueError(
        f"Invalid backup_type: {backup_type}. Must be 'full' or 'incremental'"
      )

    if not graph_id:
      raise ValueError("graph_id cannot be empty")

    if timestamp is None:
      # This should only happen for direct API calls, not backup operations
      timestamp = datetime.now(UTC)
      logger.warning(
        f"No timestamp provided to upload_backup - this may cause S3 key mismatches. Generated: {timestamp.isoformat()}"
      )
    else:
      logger.info(
        f"Using provided timestamp for upload_backup: {timestamp.isoformat()}"
      )

    original_size = len(backup_data)
    original_checksum = self._calculate_checksum(backup_data)

    processed_data = backup_data

    if self._should_compress(file_extension):
      processed_data = self._compress_data(processed_data)
      compressed_size = len(processed_data)
      if original_size > 0:
        compression_ratio = (original_size - compressed_size) / original_size
        logger.info(
          f"Compression: {original_size} -> {compressed_size} bytes ({compression_ratio:.1%} reduction)"
        )
      else:
        compression_ratio = 0.0
        logger.info(
          f"Empty backup: {original_size} -> {compressed_size} bytes (no compression)"
        )
    else:
      compressed_size = original_size
      compression_ratio = 0.0
      logger.info(
        f"Storing {file_extension or 'payload'} as-is: already a compressed archive"
      )

    backup_path = self._generate_backup_path(
      graph_id, backup_type, timestamp, file_extension
    )
    metadata_path = self._generate_metadata_path(graph_id, timestamp)
    logger.info(
      f"Generated S3 backup_path: {backup_path} (file_extension={file_extension})"
    )

    try:
      backup_size = len(processed_data)

      if backup_size > self.multipart_threshold:
        logger.info(
          f"Using multipart upload for large backup ({backup_size / (1024 * 1024):.1f} MB)"
        )
        await self._multipart_upload(
          bucket=self.bucket_name,
          key=backup_path,
          data=processed_data,
          metadata={
            "graph-id": graph_id,
            "backup-type": backup_type,
            "timestamp": timestamp.isoformat(),
            "original-size": str(original_size),
            "compressed": str(self.enable_compression),
          },
        )
      else:
        await asyncio.get_event_loop().run_in_executor(
          None,
          lambda: self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=backup_path,
            Body=processed_data,
            ContentType="application/octet-stream",
            ServerSideEncryption="AES256",
            Metadata={
              "graph-id": graph_id,
              "backup-type": backup_type,
              "timestamp": timestamp.isoformat(),
              "original-size": str(original_size),
              "compressed": str(self.enable_compression),
            },
          ),
        )

      backup_metadata = BackupMetadata(
        graph_id=graph_id,
        backup_type=backup_type,
        timestamp=timestamp,
        original_size=original_size,
        compressed_size=compressed_size,
        checksum=original_checksum,
        compression_ratio=compression_ratio,
        node_count=metadata.get("node_count", 0),
        relationship_count=metadata.get("relationship_count", 0),
        backup_duration_seconds=metadata.get("backup_duration_seconds", 0.0),
        database_version=metadata.get("database_version")
        or metadata.get("lbug_version"),
        # The caller already puts the requested format in `metadata`; reading
        # it here rather than hardcoding keeps the stored record honest the
        # first time a format other than a full dump is offered. Latent today
        # — the public create route accepts full dumps only.
        backup_format=metadata.get("backup_format", "full_dump"),
        s3_key=backup_path,
        s3_metadata_key=metadata_path,
        memory=metadata.get("memory"),
        payload_delta=metadata.get("payload_delta"),
      )

      metadata_json = json.dumps(backup_metadata.to_dict(), indent=2)
      await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.put_object(
          Bucket=self.bucket_name,
          Key=metadata_path,
          Body=metadata_json.encode("utf-8"),
          ContentType="application/json",
          ServerSideEncryption="AES256",
        ),
      )

      logger.info(
        f"Backup uploaded successfully: {backup_path} "
        f"({original_size} -> {compressed_size} bytes)"
      )

      return backup_metadata

    except ClientError as e:
      logger.error(f"Failed to upload backup to S3: {e}")
      raise

  async def download_backup_legacy(
    self, graph_id: str, timestamp: datetime, backup_type: str = "full"
  ) -> bytes:
    """Alias for :meth:`download_backup_by_timestamp`, kept for older callers."""
    backup_path = self._generate_backup_path(graph_id, backup_type, timestamp)
    return await self.download_backup_by_key(backup_path)

  async def download_backup_by_key(self, s3_key: str) -> bytes:
    """Fetch a backup payload by exact key and decompress it."""
    logger.info(f"Attempting to download from S3 key: {s3_key}")

    try:
      response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key),
      )

      processed_data = response["Body"].read()

      original_size = len(processed_data)
      processed_data = self._decompress_data(processed_data)
      if len(processed_data) != original_size:
        logger.info(f"Decompression: {original_size} -> {len(processed_data)} bytes")

      logger.info(f"Backup downloaded successfully: {s3_key}")
      return processed_data

    except ClientError as e:
      logger.error(f"Failed to download backup from S3: {e}")
      raise

  async def list_backups(self, graph_id: str | None = None) -> list[dict[str, Any]]:
    """List backup payloads, newest first, optionally scoped to one graph.

    Reads the object listing rather than the metadata sidecars, so entries are
    available even when a metadata write failed.
    """
    prefix = "graph-backups/databases/"
    if graph_id:
      prefix += f"{graph_id}/"

    try:
      paginator = self.s3_client.get_paginator("list_objects_v2")
      backups = []

      # boto3's paginator is sync; each page is fetched on the calling thread.
      page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
      for page in page_iterator:
        if "Contents" in page:
          for obj in page["Contents"]:
            key = obj["Key"]
            if any(
              ext in key
              for ext in ["/backup-", ".zip", ".cypher", ".json", ".parquet", ".lbug"]
            ) and not key.endswith("/"):
              # Parse backup information from key
              parts = key.split("/")
              if len(parts) >= 4:
                backups.append(
                  {
                    "graph_id": parts[2],
                    "backup_type": parts[3],
                    "key": key,
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                  }
                )

      return sorted(backups, key=lambda x: x["last_modified"], reverse=True)

    except ClientError as e:
      logger.error(f"Failed to list backups: {e}")
      raise

  async def delete_backup(
    self, graph_id: str, timestamp: datetime, backup_type: str = "full"
  ) -> bool:
    """Delete a backup payload and its metadata sidecar together.

    The default ``.lbug[.gz]`` extension is assumed, so a backup uploaded with
    an explicit ``file_extension`` is not matched by this path.
    """
    backup_path = self._generate_backup_path(graph_id, backup_type, timestamp)
    metadata_path = self._generate_metadata_path(graph_id, timestamp)

    try:
      delete_objects = [{"Key": backup_path}, {"Key": metadata_path}]

      await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.delete_objects(
          Bucket=self.bucket_name, Delete={"Objects": delete_objects}
        ),
      )

      logger.info(f"Backup deleted successfully: {backup_path}")
      return True

    except ClientError as e:
      logger.error(f"Failed to delete backup: {e}")
      return False

  async def setup_lifecycle_policy(self) -> bool:
    """Apply the storage-class tiering and 7-year expiry for ``graph-backups/``.

    Overwrites the bucket's whole lifecycle configuration, so any rule not
    declared here is dropped.
    """
    lifecycle_config = {
      "Rules": [
        {
          "ID": "GraphBackupLifecycle",
          "Status": "Enabled",
          "Filter": {"Prefix": "graph-backups/"},
          "Transitions": [
            {"Days": 30, "StorageClass": "STANDARD_IA"},
            {"Days": 90, "StorageClass": "GLACIER"},
            {"Days": 365, "StorageClass": "DEEP_ARCHIVE"},
          ],
          "Expiration": {
            "Days": 2555  # 7 years
          },
        }
      ]
    }

    try:
      await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.put_bucket_lifecycle_configuration(
          Bucket=self.bucket_name, LifecycleConfiguration=lifecycle_config
        ),
      )

      logger.info("S3 lifecycle policy configured successfully")
      return True

    except ClientError as e:
      logger.error(f"Failed to set lifecycle policy: {e}")
      return False

  async def get_backup_metadata(
    self, graph_id: str, backup_id: str
  ) -> dict[str, Any] | None:
    """Read the metadata sidecar. ``backup_id`` is a ``%Y%m%d_%H%M%S`` stamp.

    Returns None (not raising) when the key is missing or unparseable.
    """
    try:
      timestamp = datetime.strptime(backup_id, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)
      metadata_path = self._generate_metadata_path(graph_id, timestamp)

      response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.get_object(Bucket=self.bucket_name, Key=metadata_path),
      )

      metadata_json = response["Body"].read().decode("utf-8")
      metadata = json.loads(metadata_json)

      return metadata

    except (ClientError, ValueError, json.JSONDecodeError) as e:
      logger.warning(f"Failed to get backup metadata for {backup_id}: {e}")
      return None

  async def get_backup_metadata_by_key(self, s3_key: str) -> BackupMetadata | None:
    """Read the metadata sidecar for a payload key, or None if unresolvable.

    Reverses the key layout
    ``graph-backups/databases/{graph_id}/{backup_type}/backup-{ts}{ext}`` to
    recover the graph and timestamp the sidecar is filed under.
    """
    try:
      parts = s3_key.split("/")
      if len(parts) < 5 or parts[0] != "graph-backups" or parts[1] != "databases":
        logger.warning(f"Invalid backup key format: {s3_key}")
        return None

      graph_id = parts[2]
      backup_filename = parts[4]

      if not backup_filename.startswith("backup-"):
        logger.warning(f"Invalid backup filename format: {backup_filename}")
        return None

      timestamp_with_ext = backup_filename[7:]
      # Strip every suffix: extensions stack (.lbug.zip, .lbug.zip.enc, ...).
      timestamp_str = timestamp_with_ext.split(".")[0]

      timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)

      metadata_path = self._generate_metadata_path(graph_id, timestamp)

      response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.get_object(Bucket=self.bucket_name, Key=metadata_path),
      )

      metadata_json = response["Body"].read().decode("utf-8")
      metadata_dict = json.loads(metadata_json)

      return BackupMetadata.from_dict(metadata_dict)

    except (ClientError, ValueError, json.JSONDecodeError, IndexError) as e:
      logger.warning(f"Failed to get backup metadata from S3 key {s3_key}: {e}")
      return None

  async def generate_download_url(
    self, graph_id: str, backup_id: str, expires_in: int = 3600
  ) -> str:
    """Presign a download URL for a full backup. Raises if no object matches.

    Lists by key prefix because the stored extension varies by backup format,
    so the exact key can't be reconstructed from ``backup_id`` alone.
    """
    try:
      backup_path = None
      prefix = f"graph-backups/databases/{graph_id}/full/backup-{backup_id}"

      paginator = self.s3_client.get_paginator("list_objects_v2")
      page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

      for page in page_iterator:
        if "Contents" in page:
          for obj in page["Contents"]:
            if obj["Key"].startswith(prefix):
              backup_path = obj["Key"]
              break
        if backup_path:
          break

      if not backup_path:
        raise ValueError(f"Backup file not found for {backup_id}")

      url = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.generate_presigned_url(
          "get_object",
          Params={"Bucket": self.bucket_name, "Key": backup_path},
          ExpiresIn=expires_in,
        ),
      )

      return url

    except Exception as e:
      logger.error(f"Failed to generate download URL for {backup_id}: {e}")
      raise

  async def download_backup(
    self,
    graph_id: str,
    backup_id: str | None = None,
    timestamp: datetime | None = None,
    backup_type: str = "full",
  ) -> bytes:
    """Download a backup identified by either ``backup_id`` or ``timestamp``.

    ``backup_id`` is the ``%Y%m%d_%H%M%S`` string form and takes precedence;
    exactly one of the two must be given.
    """
    try:
      if backup_id is not None:
        parsed_timestamp = datetime.strptime(backup_id, "%Y%m%d_%H%M%S").replace(
          tzinfo=UTC
        )
        return await self.download_backup_by_timestamp(
          graph_id, parsed_timestamp, backup_type
        )
      elif timestamp is not None:
        return await self.download_backup_by_timestamp(graph_id, timestamp, backup_type)
      else:
        raise ValueError("Either backup_id or timestamp must be provided")

    except Exception as e:
      logger.error(f"Failed to download backup: {e}")
      raise

  async def download_backup_by_timestamp(
    self, graph_id: str, timestamp: datetime, backup_type: str = "full"
  ) -> bytes:
    """Download a backup whose key uses the default extension for this adapter."""
    backup_path = self._generate_backup_path(graph_id, backup_type, timestamp)
    return await self.download_backup_by_key(backup_path)

  def health_check(self) -> dict[str, Any]:
    """Report bucket reachability and the adapter's compression setting."""
    try:
      self.s3_client.head_bucket(Bucket=self.bucket_name)

      return {
        "status": "healthy",
        "bucket": self.bucket_name,
        "region": self.region,
        "compression": self.enable_compression,
        "encryption": "s3-sse-aes256",
      }

    except Exception as e:
      return {
        "status": "unhealthy",
        "error": str(e),
        "bucket": self.bucket_name,
      }


def create_s3_backup_adapter(**kwargs) -> S3BackupAdapter:
  """Build an :class:`S3BackupAdapter`; ``kwargs`` override the env defaults."""
  return S3BackupAdapter(**kwargs)
