"""S3 storage resource for Dagster.

Bucket Configuration:
    Default bucket is USER_DATA_BUCKET (robosystems-user-{env}).
    For shared data operations (SEC, FRED, etc.), use explicit bucket names:
    - SHARED_RAW_BUCKET: Raw downloaded data (robosystems-shared-raw-{env})
    - SHARED_PROCESSED_BUCKET: Processed parquet files (robosystems-shared-processed-{env})
"""

from typing import Any, BinaryIO

import boto3
from botocore.config import Config
from dagster import ConfigurableResource

from robosystems.config import env


class S3Resource(ConfigurableResource):
  """AWS S3 resource for storing and retrieving pipeline data."""

  bucket_name: str = ""
  region_name: str = ""

  @property
  def bucket(self) -> str:
    """Get the configured bucket name.

    Falls back to USER_DATA_BUCKET if not explicitly set.
    """
    return self.bucket_name or env.USER_DATA_BUCKET

  @property
  def client(self) -> Any:
    """Get an S3 client."""
    kwargs: dict[str, Any] = {
      "region_name": self.region_name or env.AWS_REGION,
    }
    # Support LocalStack for local development
    if env.AWS_ENDPOINT_URL:
      kwargs["endpoint_url"] = env.AWS_ENDPOINT_URL
      # Disable checksum validation — LocalStack's checksums are unreliable
      kwargs["config"] = Config(response_checksum_validation="when_required")
    return boto3.client("s3", **kwargs)

  def upload_file(
    self,
    file_obj: BinaryIO,
    key: str,
    content_type: str = "application/octet-stream",
  ) -> str:
    """Upload a file to ``key`` in the configured bucket, returning its S3 URI."""
    self.client.upload_fileobj(
      file_obj,
      self.bucket,
      key,
      ExtraArgs={"ContentType": content_type},
    )
    return f"s3://{self.bucket}/{key}"

  def download_file(self, key: str, file_obj: BinaryIO) -> None:
    """Download the object at ``key`` into ``file_obj``."""
    self.client.download_fileobj(self.bucket, key, file_obj)

  def list_objects(self, prefix: str) -> list[dict[str, Any]]:
    """List objects under ``prefix`` as {key, size, last_modified} dicts."""
    paginator = self.client.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
      for obj in page.get("Contents", []):
        objects.append(
          {
            "key": obj["Key"],
            "size": obj["Size"],
            "last_modified": obj["LastModified"],
          }
        )

    return objects

  def get_presigned_url(self, key: str, expiration: int = 3600) -> str:
    """Generate a presigned GET URL, valid for ``expiration`` seconds."""
    return self.client.generate_presigned_url(
      "get_object",
      Params={"Bucket": self.bucket, "Key": key},
      ExpiresIn=expiration,
    )
