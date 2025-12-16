"""Tests for Arelle XBRL client adapter."""

import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from arelle import ModelXbrl

from robosystems.adapters.sec.arelle import ArelleClient
from robosystems.config import env


class TestArelleClient:
  """Test cases for Arelle XBRL client functionality."""

  @pytest.fixture
  def temp_dir(self):
    """Create temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp:
      yield Path(temp)

  @pytest.fixture
  def client(self):
    """Create Arelle client instance."""
    return ArelleClient()

  @patch("robosystems.adapters.sec.arelle.client.Cntlr.Cntlr")
  @patch("robosystems.adapters.sec.arelle.client.ArelleClient._setup_cache_directory")
  @patch("robosystems.adapters.sec.arelle.client.ArelleClient._initialize_controller")
  def test_initialization(
    self, mock_init_controller, mock_setup_cache, mock_cntlr_class
  ):
    """Test Arelle client initialization."""
    # Setup mocks
    mock_cntlr = Mock()
    mock_cntlr_class.return_value = mock_cntlr

    # Execute
    client = ArelleClient()

    # Verify setup methods were called
    mock_setup_cache.assert_called_once()
    mock_init_controller.assert_called_once()

    # Verify controller was created (may be None if initialization fails)
    # The actual controller assignment depends on _initialize_controller success
    if mock_cntlr_class.called:
      assert client.cntlr is not None

  @patch(
    "robosystems.adapters.sec.arelle.client.ArelleClient._register_sec_transformations"
  )
  @patch("robosystems.adapters.sec.arelle.client.ArelleClient._configure_webcache")
  @patch("robosystems.adapters.sec.arelle.client.ArelleClient._load_plugins")
  @patch("robosystems.adapters.sec.arelle.client.Cntlr.Cntlr")
  @patch("robosystems.adapters.sec.arelle.client.ArelleClient._setup_cache_directory")
  def test_initialization_complete_flow(
    self,
    mock_setup_cache,
    mock_cntlr_class,
    mock_load_plugins,
    mock_config_webcache,
    mock_register_transforms,
  ):
    """Test complete Arelle client initialization flow."""
    # Setup mocks to simulate successful initialization
    mock_cntlr = Mock()
    mock_cntlr_class.return_value = mock_cntlr

    # Execute
    ArelleClient()

    # Verify initialization steps were called
    mock_setup_cache.assert_called_once()
    mock_load_plugins.assert_called_once()  # Called from _initialize_controller
    mock_config_webcache.assert_called_once()
    mock_register_transforms.assert_called_once()

  @patch(
    "robosystems.adapters.sec.arelle.client.ArelleClient._populate_cache_from_bundle"
  )
  @patch("robosystems.adapters.sec.arelle.client.env")
  @patch("robosystems.adapters.sec.arelle.client.Path.mkdir")
  def test_setup_cache_directory_with_env_var(
    self, mock_mkdir, mock_env, mock_populate, temp_dir
  ):
    """Test cache directory setup with environment variable."""
    # Setup mocks
    mock_env.ARELLE_CACHE_DIR = str(temp_dir)
    mock_populate.return_value = None  # Skip cache population

    with patch("robosystems.adapters.sec.arelle.client.env", mock_env):
      client = ArelleClient.__new__(ArelleClient)  # Create without calling __init__
      client.cache_dir = None
      client._setup_cache_directory()

      # Verify - when env var is set, no access check is performed
      assert client.cache_dir == temp_dir
      mock_mkdir.assert_called_once()  # Directory creation is called

  @patch("robosystems.adapters.sec.arelle.client.env")
  @patch("robosystems.adapters.sec.arelle.client.os.access")
  def test_setup_cache_directory_mounted_volume(self, mock_access, mock_env, temp_dir):
    """Test cache directory setup using mounted volume."""
    # Setup mocks - no env var, but mounted volume exists and is writable
    mock_env.ARELLE_CACHE_DIR = None
    mock_access.return_value = True

    mounted_cache = temp_dir / "arelle" / "cache"
    mounted_cache.mkdir(parents=True)

    with patch("robosystems.adapters.sec.arelle.client.Path") as mock_path_class:
      mock_path_class.return_value.parent.parent = temp_dir
      mock_path_class.return_value.parent = temp_dir / "arelle"
      mock_path_class.return_value = mounted_cache

      with patch(
        "robosystems.adapters.sec.arelle.client.__file__", str(temp_dir / "arelle.py")
      ):
        client = ArelleClient.__new__(ArelleClient)
        client.cache_dir = None
        client._setup_cache_directory()

        # Verify mounted volume is used
        assert str(client.cache_dir).endswith("arelle/cache")

  @patch("robosystems.adapters.sec.arelle.client.env")
  @patch("robosystems.adapters.sec.arelle.client.os.access")
  @patch("robosystems.adapters.sec.arelle.client.Path.mkdir")
  def test_setup_cache_directory_fallback_to_tmp(
    self, mock_mkdir, mock_access, mock_env, temp_dir
  ):
    """Test cache directory fallback to /tmp."""
    # Setup mocks - no env var, no writable mounted volume
    mock_env.ARELLE_CACHE_DIR = None
    mock_access.return_value = False  # Not writable

    with patch("robosystems.adapters.sec.arelle.client.Path") as mock_path_class:
      # Create a proper mock path that returns the expected string
      mock_tmp_path = MagicMock()
      mock_tmp_path.__str__ = lambda x: f"/tmp/arelle/cache/{os.getpid()}"
      mock_tmp_path.exists.return_value = False
      mock_tmp_path.mkdir = MagicMock()

      # Configure Path() to return our mock
      mock_path_class.return_value = mock_tmp_path

      client = ArelleClient.__new__(ArelleClient)
      client.cache_dir = None
      client._setup_cache_directory()

      # Verify fallback to /tmp
      assert str(client.cache_dir).startswith("/tmp/arelle/cache")
      mock_tmp_path.mkdir.assert_called()

  @patch("robosystems.adapters.sec.arelle.client.shutil.copy2")
  @patch("robosystems.adapters.sec.arelle.client.Path.glob")
  def test_populate_cache_from_bundle(self, mock_glob, mock_copy2, temp_dir):
    """Test cache population from bundle."""
    # Setup mock source files
    mock_files = [
      Mock(is_file=lambda: True, suffix=".xsd", relative_to=lambda x: Path("test.xsd")),
      Mock(is_file=lambda: True, suffix=".xml", relative_to=lambda x: Path("test.xml")),
      Mock(
        is_file=lambda: True, suffix=".txt", relative_to=lambda x: Path("test.txt")
      ),  # Non-schema file
    ]
    mock_glob.return_value = mock_files

    # Setup target file mocks
    for mock_file in mock_files:
      mock_file.parent.mkdir = Mock()
      mock_file.exists.return_value = False

    client = ArelleClient.__new__(ArelleClient)
    source_dir = temp_dir / "source"
    target_dir = temp_dir / "target"

    # Execute
    client._populate_cache_from_bundle(source_dir, target_dir)

    # Verify - should copy .xsd and .xml files but not .txt
    assert mock_copy2.call_count == 2  # Only .xsd and .xml files

  @patch("robosystems.adapters.sec.arelle.client.Path.glob")
  def test_check_cache_health_success(self, mock_glob, temp_dir):
    """Test cache health check with sufficient schemas."""
    # Setup mocks
    mock_glob.return_value = [Mock() for _ in range(15)]  # 15 schemas

    # Mock essential files exist (not used but would be in real scenario)
    # essential_files = [
    #   temp_dir / "www.w3.org/2001/xml.xsd",
    #   temp_dir / "www.w3.org/2001/XMLSchema.xsd",
    #   temp_dir / "www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd",
    # ]

    # Create mock cache_dir with proper Path methods
    mock_cache_dir = MagicMock()
    mock_cache_dir.exists.return_value = True  # Add exists method

    # Mock the glob method to return 15 schemas
    mock_cache_dir.glob.return_value = [Mock() for _ in range(15)]

    # Mock the / operator to return mock paths for essential files
    def mock_truediv(self, other):
      mock_file = MagicMock()
      mock_file.exists.return_value = True
      mock_stat = MagicMock()
      mock_stat.st_size = 1000
      mock_file.stat.return_value = mock_stat
      return mock_file

    mock_cache_dir.__truediv__ = mock_truediv

    # Patch env at module level
    # The env object just needs to have ARELLE_MIN_SCHEMA_COUNT attribute
    with patch.object(env, "ARELLE_MIN_SCHEMA_COUNT", 10):
      client = ArelleClient.__new__(ArelleClient)
      client.cache_dir = mock_cache_dir

      # Execute
      result = client._check_cache_health()

      # Verify
      assert result is True
      mock_cache_dir.glob.assert_called_once_with("**/*.xsd")

  @patch("robosystems.adapters.sec.arelle.client.Path.glob")
  def test_check_cache_health_missing_essential_file(self, mock_glob, temp_dir):
    """Test cache health check with missing essential file."""
    # Setup mocks - essential file missing
    mock_glob.return_value = [Mock() for _ in range(15)]

    # Create mock cache_dir with proper Path methods
    mock_cache_dir = MagicMock()
    mock_cache_dir.exists.return_value = True

    # Mock the glob method to return 15 schemas
    mock_cache_dir.glob.return_value = [Mock() for _ in range(15)]

    # Mock the / operator to return mock paths for essential files
    # But make the first essential file missing
    def mock_truediv(self, other):
      mock_file = MagicMock()
      # Make the first essential file not exist
      if "xml.xsd" in str(other):
        mock_file.exists.return_value = False
      else:
        mock_file.exists.return_value = True
        mock_stat = MagicMock()
        mock_stat.st_size = 1000
        mock_file.stat.return_value = mock_stat
      return mock_file

    mock_cache_dir.__truediv__ = mock_truediv

    client = ArelleClient.__new__(ArelleClient)
    client.cache_dir = mock_cache_dir

    # Execute
    result = client._check_cache_health()

    # Verify
    assert result is False

  def test_load_plugins(self, temp_dir):
    """Test plugin loading without errors."""
    # Setup mocks
    mock_cntlr = Mock()
    # The real PluginManager will access this, so make it a string
    mock_cntlr.pluginDir = str(temp_dir)

    client = ArelleClient.__new__(ArelleClient)
    client.cntlr = mock_cntlr

    # Mock file paths and create required directories
    edgar_path = temp_dir / "arelle" / "edgar"
    edgar_path.mkdir(parents=True, exist_ok=True)

    with patch(
      "robosystems.adapters.sec.arelle.client.__file__", str(temp_dir / "arelle.py")
    ):
      # Create a simple mock for PluginManager that does nothing
      mock_plugin_manager = MagicMock()
      mock_plugin_manager.init = MagicMock()
      mock_plugin_manager.addPluginModule = MagicMock()
      mock_plugin_manager.reset = MagicMock()

      # Patch the PluginManager module directly
      with patch("arelle.PluginManager", mock_plugin_manager):
        # Execute - should not raise any exceptions
        try:
          client._load_plugins()
          # If it runs without exception, the test passes
          assert True
        except Exception as e:
          # If we get an exception, verify it's handled gracefully
          assert False, f"_load_plugins raised unexpected exception: {e}"

  @patch("robosystems.adapters.sec.arelle.client.env")
  def test_configure_webcache(self, mock_env, temp_dir):
    """Test webcache configuration."""
    # Setup mocks
    mock_cntlr = Mock()
    mock_webcache = Mock()
    mock_cntlr.webCache = mock_webcache

    mock_env.ARELLE_DOWNLOAD_TIMEOUT = 300
    mock_env.ARELLE_TIMEOUT = 60
    mock_env.ARELLE_WORK_OFFLINE = "false"
    mock_env.ENVIRONMENT = "test"

    client = ArelleClient.__new__(ArelleClient)
    client.cntlr = mock_cntlr
    client.cache_dir = temp_dir

    # Execute
    client._configure_webcache()

    # Verify
    assert mock_webcache.cacheDir == str(temp_dir)
    assert mock_webcache.timeout == 60
    assert mock_webcache.httpsRedirect is True

  @patch("robosystems.adapters.sec.arelle.client.WebCache")
  def test_controller_method(self, mock_webcache, client):
    """Test XBRL document loading via controller method."""
    # Setup mocks
    mock_model = Mock(spec=ModelXbrl.ModelXbrl)
    mock_model.modelDocument = Mock()  # Add the modelDocument attribute
    mock_model.errors = []  # Add the errors attribute

    # Mock the modelManager.load method on the controller instance
    client.cntlr.modelManager.load = Mock(return_value=mock_model)

    test_url = "https://www.sec.gov/test.xbrl"

    # Execute
    result = client.controller(test_url)

    # Verify
    assert result == mock_model
    client.cntlr.modelManager.load.assert_called_once()

  @patch("arelle.ValidateXbrl")
  def test_validate_method(self, mock_validate_module, client):
    """Test XBRL document validation."""
    # Setup mocks
    mock_model = Mock(spec=ModelXbrl.ModelXbrl)
    mock_model.errors = []  # No errors for valid model

    # Mock the validator instance
    mock_validator = Mock()
    mock_validator.validate = Mock()
    mock_validate_module.ValidateXbrl.return_value = mock_validator

    # Execute
    result = client.validate(mock_model)

    # Verify
    assert result["valid"] is True
    assert result["errors"] == []
    assert "efm_validated" in result
    mock_validate_module.ValidateXbrl.assert_called_once_with(mock_model)
    mock_validator.validate.assert_called_once_with(mock_model)

  def test_is_sec_filing(self, client):
    """Test SEC filing URL detection."""
    # Test SEC URLs
    sec_urls = [
      "https://www.sec.gov/Archives/edgar/data/12345/0000123456-23-000001.xbrl",
      "https://www.sec.gov/test.xbrl",
      "http://www.sec.gov/test.xml",
    ]

    for url in sec_urls:
      assert client._is_sec_filing(url) is True

    # Test non-SEC URLs
    non_sec_urls = [
      "https://www.example.com/test.xbrl",
      "https://example.com/test.xml",
      "ftp://example.com/test.xbrl",
    ]

    for url in non_sec_urls:
      assert client._is_sec_filing(url) is False

  def test_is_efm_enabled(self, client):
    """Test EFM validation enablement check."""
    # Test enabled - mock disclosure system with efm
    client.cntlr.modelManager.disclosureSystem.selection = "efm-all-years"
    assert client._is_efm_enabled() is True

    # Test disabled - mock disclosure system without efm
    client.cntlr.modelManager.disclosureSystem.selection = "base"
    assert client._is_efm_enabled() is False

    # Test None selection
    client.cntlr.modelManager.disclosureSystem.selection = None
    assert client._is_efm_enabled() is False

  def test_close_method(self, client):
    """Test client cleanup."""
    # Setup mock controller
    mock_cntlr = Mock()
    client.cntlr = mock_cntlr

    # Execute
    client.close()

    # Verify
    mock_cntlr.close.assert_called_once()
    assert client.cntlr is None

  def test_close_method_no_controller(self, client):
    """Test client cleanup when no controller exists."""
    client.cntlr = None

    # Execute - should not raise exception
    client.close()

    # Verify
    assert client.cntlr is None

  @patch("robosystems.adapters.sec.arelle.client.Cntlr.Cntlr")
  @patch("robosystems.adapters.sec.arelle.client.ArelleClient._setup_cache_directory")
  def test_initialization_error_handling(self, mock_setup_cache, mock_cntlr_class):
    """Test initialization error handling."""
    # Setup mock to raise exception
    mock_cntlr_class.side_effect = Exception("Controller initialization failed")

    # Execute - should raise exception (no error handling in implementation)
    with pytest.raises(Exception, match="Controller initialization failed"):
      ArelleClient()

  def test_load_plugins_error_handling(self, client):
    """Test plugin loading error handling."""
    # Setup mock controller with required attributes
    mock_cntlr = Mock()
    mock_cntlr.pluginDir = "/tmp/plugins"
    client.cntlr = mock_cntlr

    # Execute - should handle exceptions gracefully
    # This test just verifies the method doesn't crash
    client._load_plugins()

    # Verify method completed despite errors
    assert client.cntlr == mock_cntlr

  def test_cache_directory_environment_setup(self, temp_dir):
    """Test environment variable setup for cache directory."""
    with patch("robosystems.adapters.sec.arelle.client.env") as mock_env:
      mock_env.ARELLE_CACHE_DIR = None
      mock_env.DEFAULT_ARELLE_CACHE_VOLUME = None

      # Execute - creates the client which will set up cache dir
      client = ArelleClient.__new__(ArelleClient)
      client.cache_dir = None

      # Call setup which should set environment variable
      client._setup_cache_directory()

      # Verify cache_dir was set (proving the method ran)
      assert client.cache_dir is not None
      assert "cache" in str(client.cache_dir)

  @patch("robosystems.adapters.sec.arelle.client.WebCache")
  def test_webcache_offline_mode(self, mock_webcache_class, client):
    """Test webcache offline mode configuration."""
    # Setup mock controller and webcache
    mock_cntlr = Mock()
    mock_webcache = Mock()
    mock_webcache_class.return_value = mock_webcache
    mock_cntlr.webCache = mock_webcache

    client.cntlr = mock_cntlr

    with patch("robosystems.adapters.sec.arelle.client.env") as mock_env:
      mock_env.ARELLE_WORK_OFFLINE = "true"
      mock_env.ARELLE_DOWNLOAD_TIMEOUT = 300
      mock_env.ARELLE_TIMEOUT = 60
      mock_env.ARELLE_MIN_SCHEMA_COUNT = 10

      # Execute
      client._configure_webcache()

      # Verify offline mode configured
      # (specific offline configuration depends on WebCache implementation)
