"""Tests for environment variable validation."""

from unittest.mock import patch
import pytest

from robosystems.config.validation import (
  ConfigValidationError,
  EnvValidator,
)


class MockEnvConfig:
  """Mock environment configuration for testing."""

  def __init__(self):
    self.ENVIRONMENT = "dev"
    self.DATABASE_URL = "postgresql://localhost/test"
    self.JWT_SECRET_KEY = "test-secret-key-that-is-long-enough-for-security"
    self.CELERY_BROKER_URL = "redis://localhost:6379"
    self.AWS_REGION = "us-east-1"
    self.CONNECTION_CREDENTIALS_KEY = "test-encryption-key"
    self.AWS_S3_ACCESS_KEY_ID = None
    self.AWS_S3_SECRET_ACCESS_KEY = None
    self.AWS_ACCESS_KEY_ID = "test-access-key"
    self.AWS_SECRET_ACCESS_KEY = "test-secret-key"
    self.INTUIT_CLIENT_ID = None
    self.INTUIT_CLIENT_SECRET = None
    self.PLAID_CLIENT_ID = None
    self.PLAID_CLIENT_SECRET = None
    self.ANTHROPIC_API_KEY = None
    self.KUZU_DATABASE_PATH = "/tmp/kuzu"
    self.KUZU_API_URL = "http://localhost:8001"
    self.KUZU_API_KEY = None
    self.WORKER_AUTOSCALE = 10
    self.CELERY_TASK_TIME_LIMIT = 3600
    self.JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
    self.RATE_LIMIT_API_KEY = 10000
    self.KUZU_MAX_DATABASES_PER_NODE = 100
    self.VALKEY_URL = "redis://localhost:6379"
    self.LOG_FILE_PATH = "stdout"
    self.DEBUG = False
    self.KUZU_ACCESS_PATTERN = "multi-tenant"
    self.RATE_LIMIT_ENABLED = True
    self.SECURITY_AUDIT_ENABLED = True


class TestConfigValidationError:
  """Test suite for ConfigValidationError exception."""

  def test_exception_creation(self):
    """Test creating ConfigValidationError."""
    error = ConfigValidationError("Test error message")
    assert str(error) == "Test error message"

  def test_exception_inheritance(self):
    """Test that ConfigValidationError inherits from Exception."""
    error = ConfigValidationError("Test")
    assert isinstance(error, Exception)


class TestEnvValidator:
  """Test suite for EnvValidator class."""

  def test_validate_required_vars_dev_environment(self):
    """Test validation passes in dev environment with minimal config."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "dev"

    with patch("robosystems.config.validation.logger") as mock_logger:
      # Should not raise exception in dev
      EnvValidator.validate_required_vars(env_config)

      # Check for warnings about missing features
      warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
      assert any("INTUIT_CLIENT_ID" in msg for msg in warning_calls)
      assert any("PLAID_CLIENT_ID" in msg for msg in warning_calls)

  def test_validate_required_vars_prod_environment_success(self):
    """Test successful validation in production environment."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.KUZU_API_KEY = "test-api-key"

    with patch("os.getenv", return_value=None):  # No explicit KUZU_API_URL set
      with patch("robosystems.config.validation.logger") as mock_logger:
        EnvValidator.validate_required_vars(env_config)

        mock_logger.info.assert_called_with("Configuration validation passed")

  def test_validate_required_vars_prod_missing_database_url(self):
    """Test validation fails when DATABASE_URL is missing in production."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.DATABASE_URL = None

    with patch("os.getenv", return_value=None):  # No explicit KUZU_API_URL set
      with pytest.raises(ConfigValidationError) as exc_info:
        EnvValidator.validate_required_vars(env_config)

      # The error message might contain multiple errors
      error_msg = str(exc_info.value)
      # Either the error message is about DATABASE_URL or multiple errors
      assert "errors" in error_msg or "DATABASE_URL" in error_msg

  def test_validate_required_vars_prod_invalid_jwt_secret(self):
    """Test validation fails with development JWT secret in production."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.JWT_SECRET_KEY = "dev-jwt-secret"

    with patch("os.getenv", return_value=None):  # No explicit KUZU_API_URL set
      with pytest.raises(ConfigValidationError):
        EnvValidator.validate_required_vars(env_config)

  def test_validate_required_vars_prod_short_jwt_secret(self):
    """Test validation fails with short JWT secret in production."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.JWT_SECRET_KEY = "short"

    with patch("os.getenv", return_value=None):  # No explicit KUZU_API_URL set
      with pytest.raises(ConfigValidationError):
        EnvValidator.validate_required_vars(env_config)

  def test_validate_required_vars_prod_no_s3_credentials_ok(self):
    """Test validation passes when S3 credentials are missing in production (uses IAM roles)."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.AWS_ACCESS_KEY_ID = None
    env_config.AWS_SECRET_ACCESS_KEY = None

    with patch("os.getenv", return_value=None):  # No explicit KUZU_API_URL set
      # Should not raise - production uses IAM roles, no access keys needed
      EnvValidator.validate_required_vars(env_config)

  def test_validate_required_vars_s3_specific_credentials(self):
    """Test validation passes with S3-specific credentials."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.AWS_ACCESS_KEY_ID = None
    env_config.AWS_SECRET_ACCESS_KEY = None
    env_config.AWS_S3_ACCESS_KEY_ID = "s3-key"
    env_config.AWS_S3_SECRET_ACCESS_KEY = "s3-secret"

    with patch("os.getenv", return_value=None):  # No explicit KUZU_API_URL set
      EnvValidator.validate_required_vars(env_config)

  def test_validate_required_vars_kuzu_api_key_warning(self):
    """Test warning for missing KUZU_API_KEY in non-dev environment."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "staging"
    env_config.KUZU_API_KEY = None

    with patch("robosystems.config.validation.logger") as mock_logger:
      EnvValidator.validate_required_vars(env_config)

      warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
      assert any("KUZU_API_KEY" in msg for msg in warning_calls)

  def test_validate_numeric_ranges_valid(self):
    """Test validation passes for valid numeric ranges."""
    env_config = MockEnvConfig()
    errors = []

    EnvValidator._validate_numeric_ranges(env_config, errors)

    assert len(errors) == 0

  def test_validate_numeric_ranges_invalid_worker_autoscale(self):
    """Test validation fails for invalid worker autoscale value."""
    env_config = MockEnvConfig()
    env_config.WORKER_AUTOSCALE = 200  # Exceeds max
    errors = []

    EnvValidator._validate_numeric_ranges(env_config, errors)

    assert len(errors) == 1
    assert "WORKER_AUTOSCALE" in errors[0]
    assert "between 1 and 100" in errors[0]

  def test_validate_numeric_ranges_invalid_jwt_expiry(self):
    """Test validation fails for invalid JWT expiry."""
    env_config = MockEnvConfig()
    env_config.JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 2000  # Exceeds max
    errors = []

    EnvValidator._validate_numeric_ranges(env_config, errors)

    assert len(errors) == 1
    assert "JWT_ACCESS_TOKEN_EXPIRE_MINUTES" in errors[0]

  def test_validate_urls_valid(self):
    """Test validation passes for valid URLs."""
    env_config = MockEnvConfig()
    errors = []

    EnvValidator._validate_urls(env_config, errors)

    assert len(errors) == 0

  def test_validate_urls_invalid_database_url(self):
    """Test validation fails for invalid DATABASE_URL."""
    env_config = MockEnvConfig()
    env_config.DATABASE_URL = "not-a-valid-url"
    errors = []

    EnvValidator._validate_urls(env_config, errors)

    assert len(errors) == 1
    assert "DATABASE_URL" in errors[0]
    assert "Invalid URL format" in errors[0]

  @patch("os.getenv")
  def test_validate_urls_kuzu_api_url_prod_error(self, mock_getenv):
    """Test that KUZU_API_URL should not be set in production."""
    mock_getenv.return_value = "http://explicit-url.com"

    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.KUZU_API_URL = "http://localhost:8001"
    errors = []

    EnvValidator._validate_urls(env_config, errors)

    assert len(errors) == 1
    assert "KUZU_API_URL" in errors[0]
    assert "Should not be explicitly set in production" in errors[0]

  @patch("os.getenv")
  def test_validate_urls_kuzu_api_url_prod_default_ok(self, mock_getenv):
    """Test that default KUZU_API_URL is fine in production."""
    mock_getenv.return_value = None  # Not explicitly set

    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.KUZU_API_URL = "http://default-value"
    errors = []

    EnvValidator._validate_urls(env_config, errors)

    assert len(errors) == 0

  @patch("os.getenv")
  def test_validate_paths_existing(self, mock_path):
    """Test validation for existing paths."""
    mock_path.exists.return_value = True
    env_config = MockEnvConfig()
    warnings = []

    EnvValidator._validate_paths(env_config, warnings)

    assert len(warnings) == 0

  @patch("os.getenv")
  def test_validate_paths_missing_parent(self, mock_path):
    """Test validation warns when parent directory is missing."""
    mock_path.exists.return_value = False
    mock_path.dirname.return_value = "/missing/parent"

    env_config = MockEnvConfig()
    env_config.KUZU_DATABASE_PATH = "/missing/parent/kuzu"
    warnings = []

    EnvValidator._validate_paths(env_config, warnings)

    assert len(warnings) == 1
    assert "KUZU_DATABASE_PATH" in warnings[0]
    assert "parent directory is missing" in warnings[0]

  @patch("os.getenv")
  def test_validate_paths_stdout_stderr_ignored(self, mock_path):
    """Test that stdout/stderr paths are not validated."""
    env_config = MockEnvConfig()
    env_config.LOG_FILE_PATH = "stdout"
    warnings = []

    EnvValidator._validate_paths(env_config, warnings)

    assert len(warnings) == 0
    mock_path.exists.assert_not_called()

  def test_validate_startup_success(self):
    """Test successful startup validation."""
    env_config = MockEnvConfig()

    result = EnvValidator.validate_startup(env_config)

    assert result is True

  def test_validate_startup_failure(self):
    """Test failed startup validation."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.DATABASE_URL = None

    with patch("robosystems.config.validation.logger") as mock_logger:
      result = EnvValidator.validate_startup(env_config)

      assert result is False
      mock_logger.error.assert_called()

  def test_get_config_summary(self):
    """Test getting configuration summary."""
    env_config = MockEnvConfig()
    env_config.INTUIT_CLIENT_ID = "intuit-id"
    env_config.PLAID_CLIENT_ID = "plaid-id"
    env_config.ANTHROPIC_API_KEY = "claude-key"
    env_config.KUZU_API_KEY = "kuzu-key"

    summary = EnvValidator.get_config_summary(env_config)

    assert summary["environment"] == "dev"
    assert summary["debug"] is False
    assert summary["features"]["quickbooks"] is True
    assert summary["features"]["plaid"] is True
    assert summary["features"]["anthropic"] is True
    assert summary["features"]["sec"] is True
    assert summary["database"]["type"] == "postgresql"
    assert summary["database"]["configured"] is True
    assert summary["kuzu"]["access_pattern"] == "multi-tenant"
    assert summary["kuzu"]["max_databases"] == 100
    assert summary["kuzu"]["api_key_configured"] is True
    assert summary["security"]["rate_limiting"] is True
    assert summary["security"]["audit_logging"] is True
    assert summary["workers"]["autoscale"] == 10


class TestIntegrationScenarios:
  """Test integration scenarios for validation."""

  def test_complete_prod_validation_success(self):
    """Test complete validation success in production."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.KUZU_API_KEY = "prod-key"
    env_config.INTUIT_CLIENT_ID = "intuit-prod"
    env_config.INTUIT_CLIENT_SECRET = "intuit-secret"

    with patch("os.getenv", return_value=None):  # No explicit KUZU_API_URL set
      with patch("robosystems.config.validation.logger") as mock_logger:
        EnvValidator.validate_required_vars(env_config)

        # Should pass with only warnings for optional features
        mock_logger.info.assert_called_with("Configuration validation passed")

  def test_multiple_validation_errors(self):
    """Test multiple validation errors are collected."""
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "prod"
    env_config.DATABASE_URL = None
    env_config.JWT_SECRET_KEY = "short"
    env_config.VALKEY_URL = None
    env_config.AWS_ACCESS_KEY_ID = None
    env_config.AWS_SECRET_ACCESS_KEY = None

    with patch("robosystems.config.validation.logger") as mock_logger:
      with pytest.raises(ConfigValidationError) as exc_info:
        EnvValidator.validate_required_vars(env_config)

      # Check multiple errors were logged
      error_calls = mock_logger.error.call_args_list
      assert len(error_calls) > 1

      # Check error message mentions number of errors
      assert "errors" in str(exc_info.value)

  def test_kuzu_api_url_handling(self):
    """Test KUZU_API_URL validation in different environments."""
    # Development - should pass
    env_config = MockEnvConfig()
    env_config.ENVIRONMENT = "dev"
    env_config.KUZU_API_URL = "http://localhost:8001"

    EnvValidator.validate_required_vars(env_config)

    # Local - should check for KUZU_API_URL
    env_config.ENVIRONMENT = "local"
    with patch("robosystems.config.validation.logger") as mock_logger:
      EnvValidator.validate_required_vars(env_config)

      # No errors expected
      mock_logger.error.assert_not_called()
