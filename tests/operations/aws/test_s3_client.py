"""
Comprehensive unit tests for S3Client and S3BackupAdapter.

Tests cover:
- S3Client: init, upload/download/delete/list/batch operations, retry logic
- S3BackupAdapter: path generation, compression, checksum, multipart upload,
  upload_backup, download_backup_by_key, BackupMetadata serialization
"""

import gzip
import hashlib
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from robosystems.operations.aws.s3 import BackupMetadata, S3BackupAdapter, S3Client


def _make_client_error(code: str, message: str = "error") -> ClientError:
  """Helper to construct a botocore ClientError with a given error code."""
  return ClientError(
    {"Error": {"Code": code, "Message": message}},
    "TestOperation",
  )


# ---------------------------------------------------------------------------
# S3Client Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestS3ClientInit:
  """Tests for S3Client.__init__ credential resolution."""

  @patch("robosystems.operations.aws.s3.boto3.client")
  @patch("robosystems.operations.aws.s3.env")
  def test_init_defaults_from_env(self, mock_env, mock_boto_client):
    """Init uses env defaults for region and endpoint."""
    mock_env.AWS_DEFAULT_REGION = "us-west-2"
    mock_env.AWS_ENDPOINT_URL = ""
    mock_env.AWS_S3_PRESIGN_ENDPOINT_URL = ""
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = ""
    mock_env.AWS_S3_SECRET_ACCESS_KEY = ""

    client = S3Client()

    assert client.region_name == "us-west-2"
    assert client.endpoint_url == ""
    # Only the primary client is constructed when the presign endpoint
    # override is unset — the presign client reuses the primary instance.
    mock_boto_client.assert_called_once()
    call_kwargs = mock_boto_client.call_args[1]
    assert call_kwargs["region_name"] == "us-west-2"
    assert "endpoint_url" not in call_kwargs

  @patch("robosystems.operations.aws.s3.boto3.client")
  @patch("robosystems.operations.aws.s3.env")
  def test_init_explicit_region_and_endpoint(self, mock_env, mock_boto_client):
    """Init respects explicit region_name and endpoint_url args."""
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_ENDPOINT_URL = ""
    mock_env.AWS_S3_PRESIGN_ENDPOINT_URL = ""
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = ""
    mock_env.AWS_S3_SECRET_ACCESS_KEY = ""

    client = S3Client(region_name="eu-west-1", endpoint_url="http://localhost:4566")

    assert client.region_name == "eu-west-1"
    assert client.endpoint_url == "http://localhost:4566"
    call_kwargs = mock_boto_client.call_args[1]
    assert call_kwargs["endpoint_url"] == "http://localhost:4566"

  @patch("robosystems.operations.aws.s3.boto3.client")
  @patch("robosystems.operations.aws.s3.env")
  def test_init_prod_uses_iam_role(self, mock_env, mock_boto_client):
    """In prod/staging, no access keys are passed (IAM role used)."""
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_ENDPOINT_URL = ""
    mock_env.AWS_S3_PRESIGN_ENDPOINT_URL = ""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_S3_ACCESS_KEY_ID = "some-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = "some-secret"

    S3Client()

    call_kwargs = mock_boto_client.call_args[1]
    assert "aws_access_key_id" not in call_kwargs
    assert "aws_secret_access_key" not in call_kwargs

  @patch("robosystems.operations.aws.s3.boto3.client")
  @patch("robosystems.operations.aws.s3.env")
  def test_init_staging_uses_iam_role(self, mock_env, mock_boto_client):
    """Staging environment also uses IAM role, not access keys."""
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_ENDPOINT_URL = ""
    mock_env.AWS_S3_PRESIGN_ENDPOINT_URL = ""
    mock_env.ENVIRONMENT = "staging"
    mock_env.AWS_S3_ACCESS_KEY_ID = "key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = "secret"

    S3Client()

    call_kwargs = mock_boto_client.call_args[1]
    assert "aws_access_key_id" not in call_kwargs

  @patch("robosystems.operations.aws.s3.boto3.client")
  @patch("robosystems.operations.aws.s3.env")
  def test_init_dev_uses_access_keys(self, mock_env, mock_boto_client):
    """Dev environment uses access keys when provided."""
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_ENDPOINT_URL = ""
    mock_env.AWS_S3_PRESIGN_ENDPOINT_URL = ""
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "AKIA-TEST"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = "secret-test"

    S3Client()

    call_kwargs = mock_boto_client.call_args[1]
    assert call_kwargs["aws_access_key_id"] == "AKIA-TEST"
    assert call_kwargs["aws_secret_access_key"] == "secret-test"

  @patch("robosystems.operations.aws.s3.boto3.client")
  @patch("robosystems.operations.aws.s3.env")
  def test_init_dev_no_keys_uses_default_chain(self, mock_env, mock_boto_client):
    """Dev environment without access keys falls back to default cred chain."""
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_ENDPOINT_URL = ""
    mock_env.AWS_S3_PRESIGN_ENDPOINT_URL = ""
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = ""
    mock_env.AWS_S3_SECRET_ACCESS_KEY = ""

    S3Client()

    call_kwargs = mock_boto_client.call_args[1]
    assert "aws_access_key_id" not in call_kwargs

  @patch("robosystems.operations.aws.s3.boto3.client")
  @patch("robosystems.operations.aws.s3.env")
  def test_init_with_presign_endpoint_builds_second_client(
    self, mock_env, mock_boto_client
  ):
    """When ``AWS_S3_PRESIGN_ENDPOINT_URL`` is set (dev/localstack), the
    client builds a second boto3 client just for presigned URL signing
    so signed URLs are reachable from the host browser while internal
    upload/download traffic stays on the docker DNS endpoint."""
    mock_env.AWS_DEFAULT_REGION = "us-east-1"
    mock_env.AWS_ENDPOINT_URL = "http://localstack:4566"
    mock_env.AWS_S3_PRESIGN_ENDPOINT_URL = "http://localhost:4566"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = ""
    mock_env.AWS_S3_SECRET_ACCESS_KEY = ""

    S3Client()

    assert mock_boto_client.call_count == 2
    primary_kwargs = mock_boto_client.call_args_list[0][1]
    presign_kwargs = mock_boto_client.call_args_list[1][1]
    assert primary_kwargs["endpoint_url"] == "http://localstack:4566"
    assert presign_kwargs["endpoint_url"] == "http://localhost:4566"


@pytest.mark.unit
class TestS3ClientUploadString:
  """Tests for S3Client.upload_string with retry logic."""

  def _make_client(self):
    """Create an S3Client with mocked internals."""
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  def test_upload_string_success(self):
    """Successful upload returns True."""
    client = self._make_client()
    client.s3_client = MagicMock()

    result = client.upload_string("hello", "bucket", "key.txt")

    assert result is True
    client.s3_client.put_object.assert_called_once()
    call_kwargs = client.s3_client.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "bucket"
    assert call_kwargs["Key"] == "key.txt"
    assert call_kwargs["Body"] == b"hello"

  def test_upload_string_with_content_type_and_metadata(self):
    """Content-type and metadata are forwarded to put_object."""
    client = self._make_client()
    client.s3_client = MagicMock()

    result = client.upload_string(
      "data",
      "bucket",
      "key",
      content_type="text/plain",
      metadata={"author": "test"},
    )

    assert result is True
    call_kwargs = client.s3_client.put_object.call_args[1]
    assert call_kwargs["ContentType"] == "text/plain"
    assert call_kwargs["Metadata"] == {"author": "test"}

  def test_upload_string_access_denied_returns_false(self):
    """AccessDenied (security error) returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.put_object.side_effect = _make_client_error("AccessDenied")

    result = client.upload_string("data", "bucket", "key")

    assert result is False
    assert client.s3_client.put_object.call_count == 1

  def test_upload_string_client_error_returns_false(self):
    """ClientError returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.put_object.side_effect = _make_client_error("InternalError")

    result = client.upload_string("data", "bucket", "key")

    assert result is False

  def test_upload_string_unexpected_exception(self):
    """Generic exceptions return False immediately."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.put_object.side_effect = RuntimeError("boom")

    result = client.upload_string("data", "bucket", "key")

    assert result is False


@pytest.mark.unit
class TestS3ClientUploadFile:
  """Tests for S3Client.upload_file with retry logic."""

  def _make_client(self):
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  def test_upload_file_success(self):
    """Successful file upload returns True."""
    client = self._make_client()
    client.s3_client = MagicMock()

    result = client.upload_file("/tmp/test.txt", "bucket", "key.txt")

    assert result is True
    client.s3_client.upload_file.assert_called_once_with(
      "/tmp/test.txt", "bucket", "key.txt", ExtraArgs=None
    )

  def test_upload_file_with_content_type(self):
    """Extra args are forwarded when content_type is set."""
    client = self._make_client()
    client.s3_client = MagicMock()

    result = client.upload_file(
      "/tmp/f.txt", "bucket", "key", content_type="text/plain"
    )

    assert result is True
    call_args = client.s3_client.upload_file.call_args
    assert call_args[1]["ExtraArgs"]["ContentType"] == "text/plain"

  def test_upload_file_client_error_returns_false(self):
    """ClientError returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.upload_file.side_effect = _make_client_error("InvalidBucketName")

    result = client.upload_file("/tmp/f.txt", "bucket", "key")

    assert result is False
    assert client.s3_client.upload_file.call_count == 1

  def test_upload_file_unexpected_error(self):
    """Generic exception returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.upload_file.side_effect = RuntimeError("disk error")

    result = client.upload_file("/tmp/f.txt", "bucket", "key")

    assert result is False


@pytest.mark.unit
class TestS3ClientDownloadFile:
  """Tests for S3Client.download_file with retry logic."""

  def _make_client(self):
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  def test_download_file_success(self):
    """Successful download returns True."""
    client = self._make_client()
    client.s3_client = MagicMock()

    result = client.download_file("bucket", "key.txt", "/tmp/out.txt")

    assert result is True
    client.s3_client.download_file.assert_called_once_with(
      "bucket", "key.txt", "/tmp/out.txt"
    )

  def test_download_file_no_such_key(self):
    """NoSuchKey returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.download_file.side_effect = _make_client_error("NoSuchKey")

    result = client.download_file("bucket", "key.txt", "/tmp/out.txt")

    assert result is False
    assert client.s3_client.download_file.call_count == 1

  def test_download_file_access_denied(self):
    """AccessDenied returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.download_file.side_effect = _make_client_error("AccessDenied")

    result = client.download_file("bucket", "key.txt", "/tmp/out.txt")

    assert result is False
    assert client.s3_client.download_file.call_count == 1

  def test_download_file_generic_exception(self):
    """Non-ClientError exceptions return False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.download_file.side_effect = OSError("disk full")

    result = client.download_file("bucket", "key", "/tmp/out")

    assert result is False


@pytest.mark.unit
class TestS3ClientDownloadString:
  """Tests for S3Client.download_string with UTF-8 decoding."""

  def _make_client(self):
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  def test_download_string_success(self):
    """Successful download returns decoded UTF-8 string."""
    client = self._make_client()
    client.s3_client = MagicMock()
    body_mock = MagicMock()
    body_mock.read.return_value = b"hello world"
    client.s3_client.get_object.return_value = {"Body": body_mock}

    result = client.download_string("bucket", "key.txt")

    assert result == "hello world"

  def test_download_string_no_such_key_returns_none(self):
    """NoSuchKey returns None."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.get_object.side_effect = _make_client_error("NoSuchKey")

    result = client.download_string("bucket", "missing.txt")

    assert result is None
    assert client.s3_client.get_object.call_count == 1

  def test_download_string_access_denied_returns_none(self):
    """AccessDenied returns None."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.get_object.side_effect = _make_client_error("AccessDenied")

    result = client.download_string("bucket", "secret.txt")

    assert result is None
    assert client.s3_client.get_object.call_count == 1

  def test_download_string_client_error_returns_none(self):
    """ClientError returns None."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.get_object.side_effect = _make_client_error("InternalError")

    result = client.download_string("bucket", "key")

    assert result is None

  def test_download_string_unexpected_error(self):
    """Non-ClientError returns None."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.get_object.side_effect = RuntimeError("boom")

    result = client.download_string("bucket", "key")

    assert result is None


@pytest.mark.unit
class TestS3ClientDeleteObject:
  """Tests for S3Client.delete_object."""

  def _make_client(self):
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  def test_delete_object_success(self):
    """Successful deletion returns True."""
    client = self._make_client()
    client.s3_client = MagicMock()

    result = client.delete_object("bucket", "key")

    assert result is True
    client.s3_client.delete_object.assert_called_once_with(Bucket="bucket", Key="key")

  def test_delete_object_access_denied(self):
    """AccessDenied returns False (security violation logged)."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.delete_object.side_effect = _make_client_error("AccessDenied")

    result = client.delete_object("bucket", "key")

    assert result is False

  def test_delete_object_client_error(self):
    """Generic ClientError returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.delete_object.side_effect = _make_client_error("InternalError")

    result = client.delete_object("bucket", "key")

    assert result is False

  def test_delete_object_unexpected_exception(self):
    """Non-ClientError returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.delete_object.side_effect = RuntimeError("boom")

    result = client.delete_object("bucket", "key")

    assert result is False


@pytest.mark.unit
class TestS3ClientObjectExists:
  """Tests for S3Client.object_exists."""

  def _make_client(self):
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  def test_object_exists_returns_true(self):
    """head_object success means object exists."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.head_object.return_value = {}

    assert client.object_exists("bucket", "key") is True

  def test_object_exists_returns_false_for_404(self):
    """404 error means object does not exist."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.head_object.side_effect = ClientError(
      {"Error": {"Code": "404", "Message": "Not Found"}},
      "HeadObject",
    )

    assert client.object_exists("bucket", "key") is False

  def test_object_exists_returns_false_on_other_client_error(self):
    """Non-404 ClientError returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.head_object.side_effect = _make_client_error("Forbidden")

    assert client.object_exists("bucket", "key") is False

  def test_object_exists_returns_false_on_unexpected_error(self):
    """Generic exception returns False."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.head_object.side_effect = RuntimeError("network error")

    assert client.object_exists("bucket", "key") is False


@pytest.mark.unit
class TestS3ClientListObjects:
  """Tests for S3Client.list_objects."""

  def _make_client(self):
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  def test_list_objects_returns_keys(self):
    """Returns list of object keys."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.list_objects_v2.return_value = {
      "Contents": [
        {"Key": "a.txt"},
        {"Key": "b.txt"},
      ]
    }

    result = client.list_objects("bucket")

    assert result == ["a.txt", "b.txt"]

  def test_list_objects_with_prefix(self):
    """Prefix parameter is forwarded to S3."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.list_objects_v2.return_value = {
      "Contents": [{"Key": "data/file.txt"}]
    }

    result = client.list_objects("bucket", prefix="data/")

    assert result == ["data/file.txt"]
    call_kwargs = client.s3_client.list_objects_v2.call_args[1]
    assert call_kwargs["Prefix"] == "data/"

  def test_list_objects_empty_bucket(self):
    """Empty bucket returns empty list."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.list_objects_v2.return_value = {}

    result = client.list_objects("bucket")

    assert result == []

  def test_list_objects_client_error(self):
    """ClientError returns empty list."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.list_objects_v2.side_effect = _make_client_error("AccessDenied")

    result = client.list_objects("bucket")

    assert result == []

  def test_list_objects_unexpected_error(self):
    """Generic exception returns empty list."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.list_objects_v2.side_effect = RuntimeError("network error")

    result = client.list_objects("bucket")

    assert result == []

  def test_list_objects_max_keys(self):
    """max_keys parameter is forwarded."""
    client = self._make_client()
    client.s3_client = MagicMock()
    client.s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "a"}]}

    client.list_objects("bucket", max_keys=5)

    call_kwargs = client.s3_client.list_objects_v2.call_args[1]
    assert call_kwargs["MaxKeys"] == 5


@pytest.mark.unit
class TestS3ClientBatchUploadStrings:
  """Tests for S3Client.batch_upload_strings with ThreadPoolExecutor."""

  def _make_client(self):
    with (
      patch("robosystems.operations.aws.s3.boto3.client"),
      patch("robosystems.operations.aws.s3.env") as mock_env,
    ):
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.AWS_ENDPOINT_URL = ""
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_S3_ACCESS_KEY_ID = ""
      mock_env.AWS_S3_SECRET_ACCESS_KEY = ""
      client = S3Client()
    return client

  @patch("robosystems.operations.aws.s3.TuningConfig.get_max_workers", return_value=2)
  def test_batch_upload_all_success(self, mock_max_workers):
    """All items upload successfully."""
    client = self._make_client()
    client.s3_client = MagicMock()

    items = [
      ("content1", "bucket", "key1"),
      ("content2", "bucket", "key2"),
    ]

    results = client.batch_upload_strings(items)

    assert results == {"key1": True, "key2": True}

  @patch("robosystems.operations.aws.s3.TuningConfig.get_max_workers", return_value=2)
  def test_batch_upload_partial_failure(self, mock_max_workers):
    """Some items fail while others succeed."""
    client = self._make_client()
    client.s3_client = MagicMock()

    def put_side_effect(**kwargs):
      if kwargs["Key"] == "key2":
        raise _make_client_error("AccessDenied")

    client.s3_client.put_object.side_effect = put_side_effect

    items = [
      ("content1", "bucket", "key1"),
      ("content2", "bucket", "key2"),
      ("content3", "bucket", "key3"),
    ]

    results = client.batch_upload_strings(items)

    assert results["key1"] is True
    assert results["key2"] is False
    assert results["key3"] is True

  def test_batch_upload_explicit_max_workers(self):
    """Explicit max_workers overrides TuningConfig."""
    client = self._make_client()
    client.s3_client = MagicMock()

    items = [("content", "bucket", "key1")]

    with patch(
      "robosystems.operations.aws.s3.TuningConfig.get_max_workers"
    ) as mock_tuning:
      results = client.batch_upload_strings(items, max_workers=4)

      # TuningConfig should NOT be called when max_workers is explicit
      mock_tuning.assert_not_called()

    assert results["key1"] is True

  @patch("robosystems.operations.aws.s3.TuningConfig.get_max_workers", return_value=2)
  def test_batch_upload_with_metadata(self, mock_max_workers):
    """Content type and metadata are passed to individual uploads."""
    client = self._make_client()
    client.s3_client = MagicMock()

    items = [("data", "bucket", "key1")]
    results = client.batch_upload_strings(
      items, content_type="application/json", metadata={"source": "test"}
    )

    assert results["key1"] is True
    call_kwargs = client.s3_client.put_object.call_args[1]
    assert call_kwargs["ContentType"] == "application/json"
    assert call_kwargs["Metadata"] == {"source": "test"}

  @patch("robosystems.operations.aws.s3.TuningConfig.get_max_workers", return_value=1)
  def test_batch_upload_empty_items(self, mock_max_workers):
    """Empty items list returns empty results."""
    client = self._make_client()
    client.s3_client = MagicMock()

    results = client.batch_upload_strings([])

    assert results == {}


# ---------------------------------------------------------------------------
# BackupMetadata Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBackupMetadata:
  """Tests for BackupMetadata to_dict / from_dict serialization."""

  def test_to_dict_serializes_timestamp(self):
    """to_dict converts datetime to ISO format string."""
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)
    meta = BackupMetadata(
      graph_id="kg123",
      backup_type="full",
      timestamp=ts,
      original_size=1000,
      compressed_size=500,
      checksum="abc123",
      compression_ratio=0.5,
      node_count=10,
      relationship_count=5,
      backup_duration_seconds=2.5,
    )

    d = meta.to_dict()

    assert d["graph_id"] == "kg123"
    assert d["timestamp"] == ts.isoformat()
    assert d["original_size"] == 1000
    assert d["compressed_size"] == 500
    assert d["checksum"] == "abc123"
    assert d["compression_ratio"] == 0.5
    assert d["node_count"] == 10
    assert d["relationship_count"] == 5

  def test_from_dict_roundtrip(self):
    """from_dict(to_dict()) produces an equivalent object."""
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)
    original = BackupMetadata(
      graph_id="kg999",
      backup_type="incremental",
      timestamp=ts,
      original_size=2000,
      compressed_size=800,
      checksum="sha256hash",
      compression_ratio=0.6,
      node_count=50,
      relationship_count=25,
      backup_duration_seconds=5.0,
      database_version="0.10.1",
      backup_format="cypher",
      s3_key="graph-backups/databases/kg999/incremental/backup-20250615_123000.lbug.gz",
    )

    d = original.to_dict()
    restored = BackupMetadata.from_dict(d)

    assert restored.graph_id == original.graph_id
    assert restored.backup_type == original.backup_type
    assert restored.timestamp == original.timestamp
    assert restored.original_size == original.original_size
    assert restored.compressed_size == original.compressed_size
    assert restored.checksum == original.checksum

  def test_from_dict_with_json_roundtrip(self):
    """BackupMetadata survives JSON serialization and deserialization."""
    ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    meta = BackupMetadata(
      graph_id="g1",
      backup_type="full",
      timestamp=ts,
      original_size=100,
      compressed_size=50,
      checksum="abc",
      compression_ratio=0.5,
      node_count=1,
      relationship_count=0,
      backup_duration_seconds=0.1,
    )

    json_str = json.dumps(meta.to_dict())
    restored = BackupMetadata.from_dict(json.loads(json_str))

    assert restored.graph_id == "g1"
    assert restored.timestamp == ts


# ---------------------------------------------------------------------------
# S3BackupAdapter Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestS3BackupAdapterPathGeneration:
  """Tests for S3BackupAdapter path generation methods."""

  def _make_adapter(self):
    """Create adapter with mocked S3 connection."""
    with (
      patch("robosystems.operations.aws.s3.env") as mock_env,
      patch("robosystems.operations.aws.s3.boto3.Session"),
    ):
      mock_env.USER_DATA_BUCKET = "test-bucket"
      mock_env.AWS_DEFAULT_REGION = "us-east-1"
      mock_env.get_s3_config.return_value = {
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
        "endpoint_url": "http://localhost:4566",
      }

      # Mock head_bucket to succeed
      with patch.object(S3BackupAdapter, "_init_s3_client"):
        adapter = S3BackupAdapter(bucket_name="test-bucket")
        adapter.s3_client = MagicMock()
        adapter.enable_compression = True
        adapter.multipart_threshold = 100 * 1024 * 1024
        adapter.multipart_chunksize = 50 * 1024 * 1024
        return adapter

  def test_generate_backup_path_with_compression(self):
    """Backup path includes .gz when compression is enabled."""
    adapter = self._make_adapter()
    adapter.enable_compression = True
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)

    path = adapter._generate_backup_path("kg123", "full", ts)

    assert path == "graph-backups/databases/kg123/full/backup-20250615_123000.lbug.gz"

  def test_generate_backup_path_without_compression(self):
    """Backup path has no .gz when compression is disabled."""
    adapter = self._make_adapter()
    adapter.enable_compression = False
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)

    path = adapter._generate_backup_path("kg123", "full", ts)

    assert path == "graph-backups/databases/kg123/full/backup-20250615_123000.lbug"

  def test_generate_backup_path_explicit_extension(self):
    """Explicit file_extension overrides default."""
    adapter = self._make_adapter()
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)

    path = adapter._generate_backup_path("kg123", "full", ts, file_extension=".csv.zip")

    assert path == "graph-backups/databases/kg123/full/backup-20250615_123000.csv.zip"

  def test_generate_backup_path_incremental(self):
    """Backup path uses backup_type correctly."""
    adapter = self._make_adapter()
    ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)

    path = adapter._generate_backup_path("kg456", "incremental", ts)

    assert "incremental" in path
    assert "kg456" in path

  def test_generate_metadata_path(self):
    """Metadata path format is correct."""
    adapter = self._make_adapter()
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)

    path = adapter._generate_metadata_path("kg123", ts)

    assert path == "graph-backups/metadata/kg123/backup-20250615_123000.json"


@pytest.mark.unit
class TestS3BackupAdapterCompression:
  """Tests for S3BackupAdapter compression and decompression."""

  def _make_adapter(self):
    with patch.object(S3BackupAdapter, "_init_s3_client"):
      with patch("robosystems.operations.aws.s3.env") as mock_env:
        mock_env.USER_DATA_BUCKET = "test-bucket"
        mock_env.AWS_DEFAULT_REGION = "us-east-1"
        adapter = S3BackupAdapter(bucket_name="test-bucket")
        adapter.s3_client = MagicMock()
        adapter.multipart_threshold = 100 * 1024 * 1024
        adapter.multipart_chunksize = 50 * 1024 * 1024
        return adapter

  def test_compress_data_enabled(self):
    """Compression produces smaller gzip output."""
    adapter = self._make_adapter()
    adapter.enable_compression = True
    data = b"A" * 10000

    compressed = adapter._compress_data(data)

    assert len(compressed) < len(data)
    # Verify it's valid gzip
    assert gzip.decompress(compressed) == data

  def test_compress_data_disabled(self):
    """With compression disabled, returns data unchanged."""
    adapter = self._make_adapter()
    adapter.enable_compression = False
    data = b"hello world"

    result = adapter._compress_data(data)

    assert result == data

  def test_decompress_data_enabled(self):
    """Decompression restores original data."""
    adapter = self._make_adapter()
    adapter.enable_compression = True
    original = b"test data for decompression"
    compressed = gzip.compress(original)

    result = adapter._decompress_data(compressed)

    assert result == original

  def test_decompress_data_disabled(self):
    """With compression disabled, returns data unchanged."""
    adapter = self._make_adapter()
    adapter.enable_compression = False
    data = b"raw data"

    result = adapter._decompress_data(data)

    assert result == data

  def test_compress_decompress_roundtrip(self):
    """compress then decompress produces original data."""
    adapter = self._make_adapter()
    adapter.enable_compression = True
    original = b"roundtrip test data with unicode: \xc3\xa9\xc3\xa0"

    compressed = adapter._compress_data(original)
    restored = adapter._decompress_data(compressed)

    assert restored == original


@pytest.mark.unit
class TestS3BackupAdapterChecksum:
  """Tests for S3BackupAdapter checksum calculation."""

  def _make_adapter(self):
    with patch.object(S3BackupAdapter, "_init_s3_client"):
      with patch("robosystems.operations.aws.s3.env") as mock_env:
        mock_env.USER_DATA_BUCKET = "test-bucket"
        mock_env.AWS_DEFAULT_REGION = "us-east-1"
        adapter = S3BackupAdapter(bucket_name="test-bucket")
        adapter.s3_client = MagicMock()
        return adapter

  def test_calculate_checksum_sha256(self):
    """Checksum is a valid SHA-256 hex digest."""
    adapter = self._make_adapter()
    data = b"test data"

    checksum = adapter._calculate_checksum(data)

    assert len(checksum) == 64
    assert checksum == hashlib.sha256(data).hexdigest()

  def test_calculate_checksum_empty_data(self):
    """Empty data produces a valid checksum."""
    adapter = self._make_adapter()
    checksum = adapter._calculate_checksum(b"")

    assert len(checksum) == 64
    assert checksum == hashlib.sha256(b"").hexdigest()

  def test_calculate_checksum_deterministic(self):
    """Same input always produces same checksum."""
    adapter = self._make_adapter()
    data = b"consistent data"

    assert adapter._calculate_checksum(data) == adapter._calculate_checksum(data)


@pytest.mark.unit
class TestS3BackupAdapterMultipartUpload:
  """Tests for S3BackupAdapter._multipart_upload."""

  def _make_adapter(self):
    with patch.object(S3BackupAdapter, "_init_s3_client"):
      with patch("robosystems.operations.aws.s3.env") as mock_env:
        mock_env.USER_DATA_BUCKET = "test-bucket"
        mock_env.AWS_DEFAULT_REGION = "us-east-1"
        adapter = S3BackupAdapter(bucket_name="test-bucket")
        adapter.s3_client = MagicMock()
        adapter.multipart_threshold = 100 * 1024 * 1024
        adapter.multipart_chunksize = 10  # Small chunk size for testing
        return adapter

  @pytest.mark.asyncio
  async def test_multipart_upload_single_part(self):
    """Data smaller than chunk size is uploaded as a single part."""
    adapter = self._make_adapter()
    adapter.multipart_chunksize = 100  # Bigger than data
    adapter.s3_client.create_multipart_upload.return_value = {"UploadId": "upload-1"}
    adapter.s3_client.upload_part.return_value = {"ETag": '"etag1"'}
    adapter.s3_client.complete_multipart_upload.return_value = {}

    data = b"small data"

    await adapter._multipart_upload("bucket", "key", data)

    adapter.s3_client.create_multipart_upload.assert_called_once()
    adapter.s3_client.upload_part.assert_called_once()
    adapter.s3_client.complete_multipart_upload.assert_called_once()

  @pytest.mark.asyncio
  async def test_multipart_upload_multiple_parts(self):
    """Data larger than chunk size is split into multiple parts."""
    adapter = self._make_adapter()
    adapter.multipart_chunksize = 5  # Very small for testing
    adapter.s3_client.create_multipart_upload.return_value = {"UploadId": "upload-2"}
    adapter.s3_client.upload_part.return_value = {"ETag": '"etag"'}
    adapter.s3_client.complete_multipart_upload.return_value = {}

    # 12 bytes / 5 byte chunks = 3 parts
    data = b"twelve bytes"

    await adapter._multipart_upload("bucket", "key", data)

    assert adapter.s3_client.upload_part.call_count == 3
    # Verify complete was called with 3 parts
    complete_call = adapter.s3_client.complete_multipart_upload.call_args
    parts = complete_call[1]["MultipartUpload"]["Parts"]
    assert len(parts) == 3
    part_numbers = [p["PartNumber"] for p in parts]
    assert part_numbers == [1, 2, 3]

  @pytest.mark.asyncio
  async def test_multipart_upload_aborts_on_failure(self):
    """Multipart upload is aborted if a part upload fails."""
    adapter = self._make_adapter()
    adapter.multipart_chunksize = 5
    adapter.s3_client.create_multipart_upload.return_value = {"UploadId": "upload-3"}
    adapter.s3_client.upload_part.side_effect = RuntimeError("part upload failed")
    adapter.s3_client.abort_multipart_upload.return_value = {}

    data = b"fail data here"

    with pytest.raises(RuntimeError, match="part upload failed"):
      await adapter._multipart_upload("bucket", "key", data)

    adapter.s3_client.abort_multipart_upload.assert_called_once_with(
      Bucket="bucket", Key="key", UploadId="upload-3"
    )

  @pytest.mark.asyncio
  async def test_multipart_upload_with_metadata(self):
    """Metadata is passed to create_multipart_upload."""
    adapter = self._make_adapter()
    adapter.multipart_chunksize = 100
    adapter.s3_client.create_multipart_upload.return_value = {"UploadId": "upload-4"}
    adapter.s3_client.upload_part.return_value = {"ETag": '"etag"'}
    adapter.s3_client.complete_multipart_upload.return_value = {}

    meta = {"graph-id": "kg123"}
    await adapter._multipart_upload("bucket", "key", b"data", metadata=meta)

    create_call = adapter.s3_client.create_multipart_upload.call_args
    assert create_call[1]["Metadata"] == meta


@pytest.mark.unit
class TestS3BackupAdapterUploadBackup:
  """Tests for S3BackupAdapter.upload_backup full flow."""

  def _make_adapter(self):
    with patch.object(S3BackupAdapter, "_init_s3_client"):
      with patch("robosystems.operations.aws.s3.env") as mock_env:
        mock_env.USER_DATA_BUCKET = "test-bucket"
        mock_env.AWS_DEFAULT_REGION = "us-east-1"
        adapter = S3BackupAdapter(bucket_name="test-bucket")
        adapter.s3_client = MagicMock()
        adapter.bucket_name = "test-bucket"
        adapter.enable_compression = True
        adapter.multipart_threshold = 100 * 1024 * 1024
        adapter.multipart_chunksize = 50 * 1024 * 1024
        return adapter

  @pytest.mark.asyncio
  async def test_upload_backup_basic_flow(self):
    """Upload backup with compression, checksum, metadata."""
    adapter = self._make_adapter()
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)
    # Use repetitive data that compresses well (small data can expand with gzip headers)
    backup_data = b"CREATE (n:Node {id: 'test'})\n" * 100
    metadata = {"node_count": 1, "relationship_count": 0}

    result = await adapter.upload_backup(
      graph_id="kg123",
      backup_data=backup_data,
      backup_type="full",
      metadata=metadata,
      timestamp=ts,
    )

    assert isinstance(result, BackupMetadata)
    assert result.graph_id == "kg123"
    assert result.backup_type == "full"
    assert result.original_size == len(backup_data)
    assert result.compressed_size < result.original_size
    assert result.compression_ratio > 0
    assert len(result.checksum) == 64
    assert result.node_count == 1
    assert result.relationship_count == 0

    # put_object should be called twice: backup data + metadata JSON
    assert adapter.s3_client.put_object.call_count == 2

  @pytest.mark.asyncio
  async def test_upload_backup_without_compression(self):
    """Upload without compression has zero compression_ratio."""
    adapter = self._make_adapter()
    adapter.enable_compression = False
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)

    result = await adapter.upload_backup(
      graph_id="kg123",
      backup_data=b"data",
      backup_type="full",
      metadata={},
      timestamp=ts,
    )

    assert result.compression_ratio == 0.0
    assert result.compressed_size == result.original_size

  @pytest.mark.asyncio
  async def test_upload_backup_rejects_non_bytes(self):
    """TypeError raised when backup_data is not bytes."""
    adapter = self._make_adapter()

    with pytest.raises(TypeError, match="backup_data must be bytes"):
      await adapter.upload_backup(
        graph_id="kg123",
        backup_data="string, not bytes",
        backup_type="full",
        metadata={},
      )

  @pytest.mark.asyncio
  async def test_upload_backup_rejects_empty_data(self):
    """ValueError raised when backup_data is empty."""
    adapter = self._make_adapter()

    with pytest.raises(ValueError, match="backup_data cannot be empty"):
      await adapter.upload_backup(
        graph_id="kg123",
        backup_data=b"",
        backup_type="full",
        metadata={},
      )

  @pytest.mark.asyncio
  async def test_upload_backup_rejects_invalid_type(self):
    """ValueError raised for invalid backup_type."""
    adapter = self._make_adapter()

    with pytest.raises(ValueError, match="Invalid backup_type"):
      await adapter.upload_backup(
        graph_id="kg123",
        backup_data=b"data",
        backup_type="snapshot",
        metadata={},
      )

  @pytest.mark.asyncio
  async def test_upload_backup_rejects_empty_graph_id(self):
    """ValueError raised when graph_id is empty."""
    adapter = self._make_adapter()

    with pytest.raises(ValueError, match="graph_id cannot be empty"):
      await adapter.upload_backup(
        graph_id="",
        backup_data=b"data",
        backup_type="full",
        metadata={},
      )

  @pytest.mark.asyncio
  async def test_upload_backup_uses_multipart_for_large_data(self):
    """Data exceeding multipart_threshold uses _multipart_upload."""
    adapter = self._make_adapter()
    adapter.enable_compression = False  # Avoid compression shrinking the data
    adapter.multipart_threshold = 10  # Very small threshold for testing
    ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)

    with patch.object(adapter, "_multipart_upload") as mock_multipart:
      await adapter.upload_backup(
        graph_id="kg123",
        backup_data=b"A" * 20,
        backup_type="full",
        metadata={},
        timestamp=ts,
      )

      mock_multipart.assert_called_once()

  @pytest.mark.asyncio
  async def test_upload_backup_generates_timestamp_when_none(self):
    """When no timestamp is provided, one is generated."""
    adapter = self._make_adapter()

    result = await adapter.upload_backup(
      graph_id="kg123",
      backup_data=b"test",
      backup_type="full",
      metadata={},
      timestamp=None,
    )

    assert result.timestamp is not None
    assert isinstance(result.timestamp, datetime)

  @pytest.mark.asyncio
  async def test_upload_backup_with_file_extension(self):
    """Custom file_extension is used in the S3 path."""
    adapter = self._make_adapter()
    ts = datetime(2025, 6, 15, 12, 30, 0, tzinfo=UTC)

    result = await adapter.upload_backup(
      graph_id="kg123",
      backup_data=b"data",
      backup_type="full",
      metadata={},
      timestamp=ts,
      file_extension=".parquet.zip",
    )

    assert result.s3_key.endswith(".parquet.zip")


@pytest.mark.unit
class TestS3BackupAdapterDownloadBackupByKey:
  """Tests for S3BackupAdapter.download_backup_by_key."""

  def _make_adapter(self):
    with patch.object(S3BackupAdapter, "_init_s3_client"):
      with patch("robosystems.operations.aws.s3.env") as mock_env:
        mock_env.USER_DATA_BUCKET = "test-bucket"
        mock_env.AWS_DEFAULT_REGION = "us-east-1"
        adapter = S3BackupAdapter(bucket_name="test-bucket")
        adapter.s3_client = MagicMock()
        adapter.bucket_name = "test-bucket"
        adapter.enable_compression = True
        adapter.multipart_threshold = 100 * 1024 * 1024
        adapter.multipart_chunksize = 50 * 1024 * 1024
        return adapter

  @pytest.mark.asyncio
  async def test_download_backup_by_key_with_decompression(self):
    """Downloaded data is decompressed when compression is enabled."""
    adapter = self._make_adapter()
    original_data = b"original backup data"
    compressed = gzip.compress(original_data)
    body_mock = MagicMock()
    body_mock.read.return_value = compressed
    adapter.s3_client.get_object.return_value = {"Body": body_mock}

    result = await adapter.download_backup_by_key("some/path/backup.lbug.gz")

    assert result == original_data

  @pytest.mark.asyncio
  async def test_download_backup_by_key_without_compression(self):
    """Raw data returned when compression is disabled."""
    adapter = self._make_adapter()
    adapter.enable_compression = False
    raw_data = b"raw backup"
    body_mock = MagicMock()
    body_mock.read.return_value = raw_data
    adapter.s3_client.get_object.return_value = {"Body": body_mock}

    result = await adapter.download_backup_by_key("some/key")

    assert result == raw_data

  @pytest.mark.asyncio
  async def test_download_backup_by_key_raises_on_client_error(self):
    """ClientError from S3 is propagated."""
    adapter = self._make_adapter()
    adapter.s3_client.get_object.side_effect = _make_client_error("NoSuchKey")

    with pytest.raises(ClientError):
      await adapter.download_backup_by_key("missing/key")
