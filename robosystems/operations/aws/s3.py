"""
S3 adapter for graph database backup storage with encryption and lifecycle management.
"""

import asyncio
import gzip
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import time

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from robosystems.logger import logger
from robosystems.config import env


class S3Client:
  """
  General-purpose S3 client for various operations.

  This client provides common S3 operations like upload, download, and delete
  for general use cases like storing XBRL textblocks, temporary files, etc.
  """

  def __init__(
    self, region_name: Optional[str] = None, endpoint_url: Optional[str] = None
  ):
    """
    Initialize S3 client.

    Args:
        region_name: AWS region (defaults to env.AWS_DEFAULT_REGION)
        endpoint_url: Custom endpoint URL (e.g., for LocalStack)
    """
    self.region_name = region_name or env.AWS_DEFAULT_REGION
    self.endpoint_url = endpoint_url or env.AWS_ENDPOINT_URL

    # Initialize boto3 client
    # Use S3-specific credentials if provided, otherwise rely on IAM roles
    s3_config = {
      "region_name": self.region_name,
      "endpoint_url": self.endpoint_url,
    }

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

    self.s3_client = boto3.client("s3", **s3_config)

    logger.debug(f"Initialized S3Client for region {self.region_name}")

  def upload_string(
    self,
    content: str,
    bucket: str,
    key: str,
    content_type: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
  ) -> bool:
    """
    Upload a string as an S3 object with retry logic.

    Args:
        content: String content to upload
        bucket: S3 bucket name
        key: S3 object key
        content_type: MIME type for the content
        metadata: Additional metadata for the object
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        True if successful, False otherwise
    """
    # Convert string to bytes
    content_bytes = content.encode("utf-8")

    # Prepare put_object arguments
    put_args = {
      "Bucket": bucket,
      "Key": key,
      "Body": content_bytes,
    }

    if content_type:
      put_args["ContentType"] = content_type

    if metadata:
      put_args["Metadata"] = metadata

    # Non-retryable error codes with security classification
    non_retryable = {"AccessDenied", "InvalidBucketName", "NoSuchBucket"}
    security_errors = {"AccessDenied", "UnauthorizedAccess", "TokenRefreshRequired"}

    for attempt in range(max_retries):
      try:
        # Upload to S3
        self.s3_client.put_object(**put_args)

        logger.debug(
          f"Successfully uploaded {len(content_bytes)} bytes to s3://{bucket}/{key}"
        )
        return True

      except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")

        # Log security errors separately for audit trail
        if error_code in security_errors:
          logger.critical(
            f"S3 SECURITY VIOLATION - {error_code}: Bucket={bucket}, Key={key}, "
            f"Error={str(e)}, Attempt={attempt + 1}"
          )
          # Security errors should not be retried
          return False

        # Don't retry other non-retryable errors
        if error_code in non_retryable:
          logger.error(f"Non-retryable S3 error {error_code}: {e}")
          return False

        # Last attempt failed
        if attempt == max_retries - 1:
          logger.error(f"Failed to upload to S3 after {max_retries} attempts: {e}")
          return False

        # Exponential backoff: 1, 2, 4 seconds
        wait_time = 2**attempt
        logger.warning(
          f"S3 upload attempt {attempt + 1} failed, retrying in {wait_time}s: {e}"
        )
        time.sleep(wait_time)

      except Exception as e:
        logger.error(f"Unexpected error uploading to S3: {e}")
        return False

    return False

  def upload_file(
    self,
    file_path: str,
    bucket: str,
    key: str,
    content_type: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
  ) -> bool:
    """
    Upload a file to S3 with retry logic.

    Args:
        file_path: Path to the file to upload
        bucket: S3 bucket name
        key: S3 object key
        content_type: MIME type for the content
        metadata: Additional metadata for the object
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        True if successful, False otherwise
    """
    # Prepare extra args
    extra_args = {}

    if content_type:
      extra_args["ContentType"] = content_type

    if metadata:
      extra_args["Metadata"] = metadata

    # Non-retryable error codes
    non_retryable = {"AccessDenied", "InvalidBucketName", "NoSuchBucket"}

    for attempt in range(max_retries):
      try:
        # Upload file
        self.s3_client.upload_file(
          file_path, bucket, key, ExtraArgs=extra_args if extra_args else None
        )

        logger.debug(f"Successfully uploaded file {file_path} to s3://{bucket}/{key}")
        return True

      except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")

        # Don't retry non-retryable errors
        if error_code in non_retryable:
          logger.error(f"Non-retryable S3 error {error_code}: {e}")
          return False

        # Last attempt failed
        if attempt == max_retries - 1:
          logger.error(f"Failed to upload file to S3 after {max_retries} attempts: {e}")
          return False

        # Exponential backoff: 1, 2, 4 seconds
        wait_time = 2**attempt
        logger.warning(
          f"S3 file upload attempt {attempt + 1} failed, retrying in {wait_time}s: {e}"
        )
        time.sleep(wait_time)

      except Exception as e:
        logger.error(f"Unexpected error uploading file to S3: {e}")
        return False

    return False

  def download_string(
    self, bucket: str, key: str, max_retries: int = 3
  ) -> Optional[str]:
    """
    Download an S3 object as a string with retry logic.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        Content as string if successful, None otherwise
    """
    # Non-retryable error codes
    non_retryable = {"NoSuchKey", "NoSuchBucket", "AccessDenied"}

    for attempt in range(max_retries):
      try:
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        logger.debug(
          f"Successfully downloaded {len(content)} characters from s3://{bucket}/{key}"
        )
        return content

      except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")

        # Don't retry non-retryable errors
        if error_code in non_retryable:
          if error_code == "NoSuchKey":
            logger.warning(f"Object not found: s3://{bucket}/{key}")
          else:
            logger.error(f"Non-retryable S3 error {error_code}: {e}")
          return None

        # Last attempt failed
        if attempt == max_retries - 1:
          logger.error(f"Failed to download from S3 after {max_retries} attempts: {e}")
          return None

        # Exponential backoff: 1, 2, 4 seconds
        wait_time = 2**attempt
        logger.warning(
          f"S3 download attempt {attempt + 1} failed, retrying in {wait_time}s: {e}"
        )
        time.sleep(wait_time)

      except Exception as e:
        logger.error(f"Unexpected error downloading from S3: {e}")
        return None

    return None

  def delete_object(self, bucket: str, key: str) -> bool:
    """
    Delete an S3 object.

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        True if successful, False otherwise
    """
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
          f"Bucket={bucket}, Key={key}, Error={str(e)}"
        )
      else:
        logger.error(f"Failed to delete from S3: {e}")
      return False
    except Exception as e:
      logger.error(f"Unexpected error deleting from S3: {e}")
      return False

  def object_exists(self, bucket: str, key: str) -> bool:
    """
    Check if an S3 object exists.

    Args:
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        True if object exists, False otherwise
    """
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

  def list_objects(
    self, bucket: str, prefix: Optional[str] = None, max_keys: int = 1000
  ) -> List[str]:
    """
    List objects in an S3 bucket.

    Args:
        bucket: S3 bucket name
        prefix: Prefix to filter objects
        max_keys: Maximum number of keys to return

    Returns:
        List of object keys
    """
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

  def batch_upload_strings(
    self,
    items: List[Tuple[str, str, str]],  # List of (content, bucket, key) tuples
    content_type: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None,
    max_workers: int = 10,
    max_retries: int = 3,
  ) -> Dict[str, bool]:
    """
    Upload multiple strings to S3 in parallel with retry logic.

    Args:
        items: List of tuples containing (content, bucket, key)
        content_type: MIME type for all content
        metadata: Additional metadata for all objects
        max_workers: Maximum number of parallel uploads (default: 10)
        max_retries: Maximum number of retry attempts per upload (default: 3)

    Returns:
        Dictionary mapping S3 keys to upload success status
    """
    results = {}

    def upload_item(item: Tuple[str, str, str]) -> Tuple[str, bool]:
      """Upload a single item with retry logic."""
      content, bucket, key = item
      success = self.upload_string(
        content=content,
        bucket=bucket,
        key=key,
        content_type=content_type,
        metadata=metadata,
        max_retries=max_retries,
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
  database_version: Optional[str] = None
  backup_format: str = "cypher"
  s3_key: Optional[str] = None
  is_encrypted: bool = False  # Track if data was encrypted before S3 upload
  encryption_method: Optional[str] = None  # e.g., "fernet", "aes-256-gcm"

  def to_dict(self) -> Dict[str, Any]:
    """Convert metadata to dictionary for JSON serialization."""
    data = asdict(self)
    data["timestamp"] = self.timestamp.isoformat()
    return data

  @classmethod
  def from_dict(cls, data: Dict[str, Any]) -> "BackupMetadata":
    """Create metadata from dictionary."""
    data["timestamp"] = datetime.fromisoformat(data["timestamp"])
    return cls(**data)


class S3BackupAdapter:
  """S3 adapter for graph database backup storage with encryption and lifecycle management."""

  def __init__(
    self,
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
    enable_compression: bool = True,
  ):
    """
    Initialize S3 backup adapter.

    Args:
        bucket_name: S3 bucket name (defaults to AWS_S3_BUCKET env var)
        region: AWS region (defaults to AWS_REGION env var)
        enable_compression: Enable gzip compression
    Note: Encryption is handled by the backup task using security/encryption.py
    """
    self.bucket_name = bucket_name or env.AWS_S3_BUCKET
    self.region = region or env.AWS_DEFAULT_REGION
    self.enable_compression = enable_compression

    if not self.bucket_name:
      raise ValueError(
        "S3 bucket name must be provided via parameter or AWS_S3_BUCKET env var"
      )

    # Initialize S3 client
    self._init_s3_client()

    logger.info(
      f"S3BackupAdapter initialized: bucket={self.bucket_name}, "
      f"compression={self.enable_compression}"
    )

  def _init_s3_client(self):
    """Initialize S3 client with credentials."""
    try:
      # Get S3-specific configuration (falls back to general AWS credentials if needed)
      s3_config = env.get_s3_config()

      # Extract credentials and endpoint
      access_key = s3_config.get("aws_access_key_id")
      secret_key = s3_config.get("aws_secret_access_key")
      endpoint_url = s3_config.get("endpoint_url")
      region = s3_config.get("region_name", self.region)

      session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
      )

      # Configure S3 client with performance optimizations
      from botocore.config import Config

      config = Config(
        max_pool_connections=50,  # Increase connection pool for parallel uploads
        retries={
          "mode": "adaptive",  # Use adaptive retry mode
          "max_attempts": 3,
        },
        s3={
          "use_accelerate_endpoint": False,  # Can enable if S3 Transfer Acceleration is configured
          "payload_signing_enabled": True,
        },
      )

      # Create S3 client with optional endpoint override for LocalStack
      if endpoint_url:
        self.s3_client = session.client("s3", config=config, endpoint_url=endpoint_url)
        logger.info(f"Using custom S3 endpoint: {endpoint_url}")
      else:
        self.s3_client = session.client("s3", config=config)

      # Configure multipart upload thresholds for large backups
      self.multipart_threshold = (
        100 * 1024 * 1024
      )  # 100MB - start multipart for files > 100MB
      self.multipart_chunksize = 50 * 1024 * 1024  # 50MB chunks for multipart uploads

      # Test connection
      self.s3_client.head_bucket(Bucket=self.bucket_name)
      logger.info(f"S3 connection established to bucket: {self.bucket_name}")

    except NoCredentialsError:
      logger.error("AWS credentials not found")
      raise
    except ClientError as e:
      logger.error(f"Failed to connect to S3 bucket {self.bucket_name}: {e}")
      raise

  # Encryption methods removed - handled by security/encryption.py module

  def _generate_backup_path(
    self,
    graph_id: str,
    backup_type: str,
    timestamp: datetime,
    file_extension: Optional[str] = None,
  ) -> str:
    """Generate S3 key path for backup file."""
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    # Use provided extension or default to .lbug with optional compression
    if file_extension:
      # Extension explicitly provided - use as-is
      extension = file_extension
    else:
      # No extension provided - generate default with compression if enabled
      extension = ".lbug"
      if self.enable_compression:
        extension += ".gz"

    return f"graph-backups/databases/{graph_id}/{backup_type}/backup-{timestamp_str}{extension}"

  def _generate_metadata_path(self, graph_id: str, timestamp: datetime) -> str:
    """Generate S3 key path for backup metadata."""
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    return f"graph-backups/metadata/{graph_id}/backup-{timestamp_str}.json"

  def _compress_data(self, data: bytes) -> bytes:
    """Compress data using gzip."""
    if not self.enable_compression:
      return data

    return gzip.compress(data)

  def _decompress_data(self, data: bytes) -> bytes:
    """Decompress gzip data."""
    if not self.enable_compression:
      return data

    return gzip.decompress(data)

  # Encryption/decryption methods removed - handled by security/encryption.py module

  def _calculate_checksum(self, data: bytes) -> str:
    """Calculate SHA-256 checksum of data."""
    return hashlib.sha256(data).hexdigest()

  async def _multipart_upload(
    self,
    bucket: str,
    key: str,
    data: bytes,
    metadata: Optional[Dict[str, str]] = None,
  ) -> None:
    """
    Perform multipart upload for large files.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        data: Data to upload
        metadata: Optional metadata for the object
    """
    import math

    # Initiate multipart upload
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
      # Calculate number of parts
      total_size = len(data)
      num_parts = math.ceil(total_size / self.multipart_chunksize)

      logger.info(
        f"Multipart upload: {num_parts} parts, {total_size / (1024 * 1024):.1f} MB total"
      )

      # Upload parts
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

      # Complete multipart upload
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
      # Abort multipart upload on failure
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
    metadata: Dict[str, Any],
    timestamp: Optional[datetime] = None,
    file_extension: Optional[str] = None,
  ) -> BackupMetadata:
    """
    Upload backup data to S3 with compression and encryption.

    Args:
        graph_id: Graph database identifier
        backup_data: Raw backup data (Cypher format)
        backup_type: 'full' or 'incremental'
        metadata: Additional metadata for the backup
        timestamp: Timestamp to use for consistent S3 key generation (REQUIRED for backup operations)
        file_extension: Optional file extension to use (e.g., '.csv.zip', '.json.zip', '.parquet.zip')

    Returns:
        BackupMetadata: Metadata about the uploaded backup
    """
    # Input validation
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
      timestamp = datetime.now(timezone.utc)
      logger.warning(
        f"No timestamp provided to upload_backup - this may cause S3 key mismatches. Generated: {timestamp.isoformat()}"
      )
    else:
      logger.info(
        f"Using provided timestamp for upload_backup: {timestamp.isoformat()}"
      )

    original_size = len(backup_data)

    # Calculate original checksum
    original_checksum = self._calculate_checksum(backup_data)

    # Process data (compress then encrypt)
    processed_data = backup_data

    if self.enable_compression:
      processed_data = self._compress_data(processed_data)
      compressed_size = len(processed_data)
      # Handle empty backup case to avoid division by zero
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

    # Encryption handled by backup task, not here

    # Generate S3 paths
    backup_path = self._generate_backup_path(
      graph_id, backup_type, timestamp, file_extension
    )
    metadata_path = self._generate_metadata_path(graph_id, timestamp)
    logger.info(
      f"Generated S3 backup_path: {backup_path} (file_extension={file_extension})"
    )

    try:
      # Upload backup data - use multipart for large files
      backup_size = len(processed_data)

      if backup_size > self.multipart_threshold:
        # Use multipart upload for large backups
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
            "encrypted": str(metadata.get("is_encrypted", False)),
          },
        )
      else:
        # Use regular upload for smaller backups
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
              "encrypted": str(metadata.get("is_encrypted", False)),
            },
          ),
        )

      # Create backup metadata
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
        backup_format="full_dump",
        s3_key=backup_path,
        is_encrypted=metadata.get("is_encrypted", False),
        encryption_method=metadata.get("encryption_method"),
      )

      # Upload metadata
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

  # Legacy method - kept for backward compatibility
  async def download_backup_legacy(
    self, graph_id: str, timestamp: datetime, backup_type: str = "full"
  ) -> bytes:
    """
    Download and decrypt backup data from S3 (legacy method).

    Args:
        graph_id: Graph database identifier
        timestamp: Backup timestamp
        backup_type: 'full' or 'incremental'

    Returns:
        bytes: Decrypted backup data
    """
    backup_path = self._generate_backup_path(graph_id, backup_type, timestamp)
    return await self.download_backup_by_key(backup_path)

  async def download_backup_by_key(self, s3_key: str) -> bytes:
    """
    Download and decrypt backup data from S3 using exact S3 key.

    Args:
        s3_key: Exact S3 key path to the backup file

    Returns:
        bytes: Decrypted backup data
    """
    logger.info(f"Attempting to download from S3 key: {s3_key}")

    try:
      # Download backup data
      response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key),
      )

      encrypted_data = response["Body"].read()

      # Process data (decrypt then decompress)
      processed_data = encrypted_data

      # Decryption handled by restore task, not here

      if self.enable_compression:
        original_size = len(processed_data)
        processed_data = self._decompress_data(processed_data)
        logger.info(f"Decompression: {original_size} -> {len(processed_data)} bytes")

      logger.info(f"Backup downloaded successfully: {s3_key}")
      return processed_data

    except ClientError as e:
      logger.error(f"Failed to download backup from S3: {e}")
      raise

  async def list_backups(self, graph_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List available backups in S3.

    Args:
        graph_id: Optional filter by graph ID

    Returns:
        List of backup information
    """
    prefix = "graph-backups/databases/"
    if graph_id:
      prefix += f"{graph_id}/"

    try:
      paginator = self.s3_client.get_paginator("list_objects_v2")
      backups = []

      # Get pages synchronously since boto3 paginator is not async
      page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
      for page in page_iterator:
        if "Contents" in page:
          for obj in page["Contents"]:
            key = obj["Key"]
            # Check for backup files with various extensions
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
    """
    Delete a backup from S3.

    Args:
        graph_id: Graph database identifier
        timestamp: Backup timestamp
        backup_type: 'full' or 'incremental'

    Returns:
        bool: True if deletion was successful
    """
    backup_path = self._generate_backup_path(graph_id, backup_type, timestamp)
    metadata_path = self._generate_metadata_path(graph_id, timestamp)

    try:
      # Delete backup data and metadata
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
    """
    Set up S3 lifecycle policy for cost optimization.

    Returns:
        bool: True if policy was set successfully
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
  ) -> Optional[Dict[str, Any]]:
    """
    Get backup metadata by backup ID.

    Args:
        graph_id: Graph database identifier
        backup_id: Backup identifier (timestamp string)

    Returns:
        Backup metadata dictionary or None if not found
    """
    try:
      # Parse backup_id as timestamp
      timestamp = datetime.strptime(backup_id, "%Y%m%d_%H%M%S").replace(
        tzinfo=timezone.utc
      )
      metadata_path = self._generate_metadata_path(graph_id, timestamp)

      # Download metadata from S3
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

  async def get_backup_metadata_by_key(self, s3_key: str) -> Optional[BackupMetadata]:
    """
    Get backup metadata by extracting info from S3 backup key.

    Args:
        s3_key: S3 key of the backup file (e.g., graph-backups/databases/graph_id/full/backup-20241115_023045.lbug.zip)

    Returns:
        BackupMetadata object or None if metadata not found
    """
    try:
      # Extract graph_id and timestamp from backup key
      # Format: graph-backups/databases/{graph_id}/{backup_type}/backup-{timestamp}{extension}
      parts = s3_key.split("/")
      if len(parts) < 5 or parts[0] != "graph-backups" or parts[1] != "databases":
        logger.warning(f"Invalid backup key format: {s3_key}")
        return None

      graph_id = parts[2]
      backup_filename = parts[4]

      # Extract timestamp from filename (e.g., backup-20241115_023045.lbug.zip -> 20241115_023045)
      if not backup_filename.startswith("backup-"):
        logger.warning(f"Invalid backup filename format: {backup_filename}")
        return None

      timestamp_with_ext = backup_filename[7:]
      # Remove all extensions (could be .lbug.zip, .lbug.zip.enc, etc.)
      timestamp_str = timestamp_with_ext.split(".")[0]

      # Parse timestamp
      timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S").replace(
        tzinfo=timezone.utc
      )

      # Generate metadata path
      metadata_path = self._generate_metadata_path(graph_id, timestamp)

      # Download metadata from S3
      response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: self.s3_client.get_object(Bucket=self.bucket_name, Key=metadata_path),
      )

      metadata_json = response["Body"].read().decode("utf-8")
      metadata_dict = json.loads(metadata_json)

      # Convert dict to BackupMetadata object
      return BackupMetadata.from_dict(metadata_dict)

    except (ClientError, ValueError, json.JSONDecodeError, IndexError) as e:
      logger.warning(f"Failed to get backup metadata from S3 key {s3_key}: {e}")
      return None

  async def generate_download_url(
    self, graph_id: str, backup_id: str, expires_in: int = 3600
  ) -> str:
    """
    Generate a presigned URL for downloading a backup.

    Args:
        graph_id: Graph database identifier
        backup_id: Backup identifier (timestamp string)
        expires_in: URL expiration time in seconds

    Returns:
        Presigned download URL
    """
    try:
      # Try to find the backup file with any extension
      backup_path = None
      prefix = f"graph-backups/databases/{graph_id}/full/backup-{backup_id}"

      # List objects to find the actual backup file
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

      # Generate presigned URL
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
    backup_id: Optional[str] = None,
    timestamp: Optional[datetime] = None,
    backup_type: str = "full",
  ) -> bytes:
    """
    Download backup data by backup ID or timestamp (unified method).

    Args:
        graph_id: Graph database identifier
        backup_id: Backup identifier (timestamp string) - new API
        timestamp: Backup timestamp - legacy API
        backup_type: 'full' or 'incremental' - used with timestamp

    Returns:
        Decrypted backup data
    """
    try:
      # Handle both new API (backup_id) and legacy API (timestamp)
      if backup_id is not None:
        # New API: Parse backup_id as timestamp
        parsed_timestamp = datetime.strptime(backup_id, "%Y%m%d_%H%M%S").replace(
          tzinfo=timezone.utc
        )
        return await self.download_backup_by_timestamp(
          graph_id, parsed_timestamp, backup_type
        )
      elif timestamp is not None:
        # Legacy API: Use timestamp directly
        return await self.download_backup_by_timestamp(graph_id, timestamp, backup_type)
      else:
        raise ValueError("Either backup_id or timestamp must be provided")

    except Exception as e:
      logger.error(f"Failed to download backup: {e}")
      raise

  async def download_backup_by_timestamp(
    self, graph_id: str, timestamp: datetime, backup_type: str = "full"
  ) -> bytes:
    """
    Download backup data by timestamp (renamed from original download_backup).

    Args:
        graph_id: Graph database identifier
        timestamp: Backup timestamp
        backup_type: 'full' or 'incremental'

    Returns:
        Decrypted backup data
    """
    backup_path = self._generate_backup_path(graph_id, backup_type, timestamp)
    return await self.download_backup_by_key(backup_path)

  def health_check(self) -> Dict[str, Any]:
    """
    Perform health check on S3 connectivity.

    Returns:
        Health status information
    """
    try:
      # Test bucket access
      self.s3_client.head_bucket(Bucket=self.bucket_name)

      # Test encryption if enabled
      encryption_status = "handled by backup task"

      return {
        "status": "healthy",
        "bucket": self.bucket_name,
        "region": self.region,
        "compression": self.enable_compression,
        "encryption": encryption_status,
      }

    except Exception as e:
      return {
        "status": "unhealthy",
        "error": str(e),
        "bucket": self.bucket_name,
      }


# Factory function for creating S3 adapter
def create_s3_backup_adapter(**kwargs) -> S3BackupAdapter:
  """
  Factory function to create S3 backup adapter with default configuration.

  Args:
      **kwargs: Optional configuration overrides

  Returns:
      S3BackupAdapter: Configured adapter instance
  """
  return S3BackupAdapter(**kwargs)
