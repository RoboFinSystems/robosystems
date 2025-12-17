import pytest
from unittest.mock import MagicMock, patch

from robosystems.adapters.sec.processors.textblock import (
  TextBlockExternalizer,
)


@pytest.fixture
def mock_s3_client():
  client = MagicMock()
  client.upload_string = MagicMock()
  client.batch_upload_strings = MagicMock(return_value={"key1": True, "key2": True})
  client.object_exists = MagicMock(return_value=False)
  return client


@pytest.fixture
def entity_data():
  return {"cik": "0000320193", "name": "Apple Inc"}


@pytest.fixture
def report_data():
  return {
    "filing_date": "2023-09-30",
    "accession_number": "0000320193-23-000077",
  }


class TestTextBlockExternalizerInitialization:
  def test_initialization_basic(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
      enabled=True,
    )

    assert externalizer.s3_client == mock_s3_client
    assert externalizer.bucket == "test-bucket"
    assert externalizer.cdn_url == "https://cdn.example.com"
    assert externalizer.threshold == 5000
    assert externalizer.enabled is True
    assert externalizer.upload_queue == []
    assert externalizer.upload_map == {}
    assert externalizer.content_cache == {}

  def test_initialization_disabled(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
      enabled=False,
    )

    assert externalizer.enabled is False

  def test_initialization_without_cdn(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    assert externalizer.cdn_url is None


class TestShouldExternalize:
  def test_should_externalize_disabled(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
      enabled=False,
    )

    result = externalizer.should_externalize("Some text content")

    assert result is False

  def test_should_externalize_empty_value(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    assert externalizer.should_externalize(None) is False
    assert externalizer.should_externalize("") is False

  def test_should_externalize_html_content(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer.should_externalize("<p>HTML content</p>")

    assert result is True

  def test_should_externalize_exceeds_threshold(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=100,
    )

    long_text = "a" * 150

    result = externalizer.should_externalize(long_text)

    assert result is True

  def test_should_not_externalize_below_threshold(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=1000,
    )

    short_text = "Short text"

    result = externalizer.should_externalize(short_text)

    assert result is False


class TestGenerateS3Key:
  def test_generate_s3_key_with_full_data(
    self, mock_s3_client, entity_data, report_data
  ):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer._generate_s3_key(
      "fact_12345678abcdef", entity_data, report_data, "html"
    )

    assert result == "2023/0000320193/0000320193-23-000077/fact_fact_123.html"

  def test_generate_s3_key_without_entity(self, mock_s3_client, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer._generate_s3_key(
      "fact_12345678abcdef", None, report_data, "txt"
    )

    assert result == "2023/unknown/0000320193-23-000077/fact_fact_123.txt"

  def test_generate_s3_key_without_report(self, mock_s3_client, entity_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    with patch(
      "robosystems.adapters.sec.processors.textblock.datetime"
    ) as mock_datetime:
      mock_datetime.now.return_value.strftime.return_value = "2024"

      result = externalizer._generate_s3_key("fact_123", entity_data, None, "html")

      assert result.startswith("2024/")
      assert "unknown" in result

  def test_generate_s3_key_short_fact_id(
    self, mock_s3_client, entity_data, report_data
  ):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer._generate_s3_key("fact123", entity_data, report_data, "html")

    assert "fact_fact123.html" in result


class TestGenerateS3KeyWithHash:
  def test_generate_s3_key_with_hash(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    content_hash = "abcdef1234567890" * 4

    result = externalizer._generate_s3_key_with_hash(
      content_hash, entity_data, report_data, "html"
    )

    assert result == "2023/0000320193/0000320193-23-000077/fact_abcdef123456.html"

  def test_generate_s3_key_with_hash_no_data(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    content_hash = "abcdef1234567890" * 4

    with patch(
      "robosystems.adapters.sec.processors.textblock.datetime"
    ) as mock_datetime:
      mock_datetime.now.return_value.strftime.return_value = "2024"

      result = externalizer._generate_s3_key_with_hash(content_hash, None, None, "txt")

      assert result.startswith("2024/")
      assert "unknown" in result
      assert "fact_abcdef123456.txt" in result


class TestCheckS3ObjectExists:
  def test_check_object_exists_true(self, mock_s3_client):
    mock_s3_client.object_exists.return_value = True

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer._check_s3_object_exists("path/to/object.html")

    assert result is True
    mock_s3_client.object_exists.assert_called_once_with(
      "test-bucket", "path/to/object.html"
    )

  def test_check_object_exists_false(self, mock_s3_client):
    mock_s3_client.object_exists.return_value = False

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer._check_s3_object_exists("path/to/object.html")

    assert result is False

  def test_check_object_exists_no_client(self):
    externalizer = TextBlockExternalizer(
      s3_client=None,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer._check_s3_object_exists("path/to/object.html")

    assert result is False

  def test_check_object_exists_exception(self, mock_s3_client):
    mock_s3_client.object_exists.side_effect = Exception("S3 error")

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer._check_s3_object_exists("path/to/object.html")

    assert result is False


class TestQueueValueForS3:
  def test_queue_value_html_content(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
    )

    html_content = "<p>Test HTML content</p>"

    result = externalizer.queue_value_for_s3(
      html_content, "fact123", entity_data, report_data
    )

    assert result is not None
    assert result["value_type"] == "external"
    assert result["content_type"] == "text/html"
    assert "cdn.example.com" in result["url"]
    assert len(externalizer.upload_queue) == 1

  def test_queue_value_plain_text(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    text_content = "Plain text content"

    result = externalizer.queue_value_for_s3(
      text_content, "fact123", entity_data, report_data
    )

    assert result is not None
    assert result["content_type"] == "text/plain"
    assert "s3.amazonaws.com" in result["url"]

  def test_queue_value_cache_hit(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
    )

    content = "<p>Test content</p>"

    result1 = externalizer.queue_value_for_s3(
      content, "fact1", entity_data, report_data
    )
    result2 = externalizer.queue_value_for_s3(
      content, "fact2", entity_data, report_data
    )

    assert result1 is not None
    assert result2 is not None
    assert result1["url"] == result2["url"]
    assert len(externalizer.upload_queue) == 1

  def test_queue_value_object_exists(self, mock_s3_client, entity_data, report_data):
    mock_s3_client.object_exists.return_value = True

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
    )

    content = "<p>Test content</p>"

    result = externalizer.queue_value_for_s3(
      content, "fact123", entity_data, report_data
    )

    assert result is not None
    assert len(externalizer.upload_queue) == 0

  def test_queue_value_no_s3_client(self, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=None,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer.queue_value_for_s3(
      "content", "fact123", entity_data, report_data
    )

    assert result is None

  def test_queue_value_exception(self, mock_s3_client, entity_data, report_data):
    with patch(
      "robosystems.adapters.sec.processors.textblock.hashlib.sha256"
    ) as mock_sha256:
      mock_sha256.side_effect = Exception("Hash error")

      externalizer = TextBlockExternalizer(
        s3_client=mock_s3_client,
        bucket="test-bucket",
        cdn_url=None,
        threshold=5000,
      )

      result = externalizer.queue_value_for_s3(
        "content", "fact123", entity_data, report_data
      )

      assert result is None


class TestProcessBatchUploads:
  def test_process_batch_uploads_success(self, mock_s3_client):
    mock_s3_client.batch_upload_strings.return_value = {
      "key1": True,
      "key2": True,
      "key3": True,
    }

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.upload_queue = [
      ("content1", "test-bucket", "key1"),
      ("content2", "test-bucket", "key2"),
      ("content3", "test-bucket", "key3"),
    ]

    externalizer.process_batch_uploads()

    mock_s3_client.batch_upload_strings.assert_called_once()
    assert len(externalizer.upload_queue) == 0

  def test_process_batch_uploads_partial_failure(self, mock_s3_client):
    mock_s3_client.batch_upload_strings.return_value = {
      "key1": True,
      "key2": False,
      "key3": True,
    }

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.upload_queue = [
      ("content1", "test-bucket", "key1"),
      ("content2", "test-bucket", "key2"),
      ("content3", "test-bucket", "key3"),
    ]

    externalizer.process_batch_uploads()

    assert len(externalizer.upload_queue) == 0

  def test_process_batch_uploads_empty_queue(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.process_batch_uploads()

    mock_s3_client.batch_upload_strings.assert_not_called()

  def test_process_batch_uploads_no_client(self):
    externalizer = TextBlockExternalizer(
      s3_client=None,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.upload_queue = [("content1", "test-bucket", "key1")]

    externalizer.process_batch_uploads()

    assert len(externalizer.upload_queue) == 1


class TestExternalizeValueToS3:
  def test_externalize_value_html(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
    )

    html_content = "<p>Test HTML</p>"

    result = externalizer.externalize_value_to_s3(
      html_content, "fact_12345678", entity_data, report_data
    )

    assert result is not None
    assert result["value_type"] == "external_resource"
    assert result["content_type"] == "text/html"
    assert "cdn.example.com" in result["url"]
    assert result["value_size"] == len(html_content)
    mock_s3_client.upload_string.assert_called_once()

  def test_externalize_value_plain_text(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    text_content = "Plain text content"

    result = externalizer.externalize_value_to_s3(
      text_content, "fact123", entity_data, report_data
    )

    assert result is not None
    assert result["content_type"] == "text/plain"
    assert "s3://" in result["url"]

  def test_externalize_value_without_entity(self, mock_s3_client, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer.externalize_value_to_s3(
      "content", "fact123", None, report_data
    )

    assert result is not None

  def test_externalize_value_without_report(self, mock_s3_client, entity_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    with patch(
      "robosystems.adapters.sec.processors.textblock.datetime"
    ) as mock_datetime:
      mock_datetime.now.return_value.strftime.return_value = "2024"

      result = externalizer.externalize_value_to_s3(
        "content", "fact123", entity_data, None
      )

      assert result is not None

  def test_externalize_value_no_client(self, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=None,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer.externalize_value_to_s3(
      "content", "fact123", entity_data, report_data
    )

    assert result is None

  def test_externalize_value_exception(self, mock_s3_client, entity_data, report_data):
    mock_s3_client.upload_string.side_effect = Exception("Upload failed")

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer.externalize_value_to_s3(
      "content", "fact123", entity_data, report_data
    )

    assert result is None
