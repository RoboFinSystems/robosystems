import logging
import os
import shutil
from pathlib import Path
from typing import Optional, Dict, Any
from email.message import EmailMessage
from arelle import Cntlr, ModelXbrl
from arelle.Version import __version__ as ARELLE_VERSION
from arelle.WebCache import WebCache
from ..config import env

logger = logging.getLogger(__name__)


class ArelleClient:
  """
  Arelle client for loading and validating XBRL documents.

  This client handles the HTTP/HTTPS redirect issues with xbrl.org
  and properly configures EFM validation for SEC filings.
  """

  def __init__(self):
    """Initialize the Arelle client with proper configuration."""
    self.cntlr = None
    self.cache_dir = None
    self._setup_cache_directory()
    self._initialize_controller()

  def _setup_cache_directory(self):
    """Setup and populate Arelle's cache directory with pre-cached schemas."""
    # Determine cache directory
    if env.ARELLE_CACHE_DIR:
      # Use configured cache directory
      self.cache_dir = Path(env.ARELLE_CACHE_DIR)
    else:
      # Try to use the mounted volume first (local development)
      mounted_cache = Path(__file__).parent.parent / "arelle" / "cache"
      if mounted_cache.exists() and os.access(mounted_cache, os.W_OK):
        # We have write access to the mounted cache
        self.cache_dir = mounted_cache
      else:
        # Use /tmp for production/staging where filesystem is read-only
        self.cache_dir = Path("/tmp/arelle/cache")

    # Create cache directory if it doesn't exist
    try:
      self.cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
      # Fallback to /tmp if we can't create the directory
      logger.warning(f"Cannot create cache at {self.cache_dir}: {e}, using /tmp")
      self.cache_dir = Path("/tmp/arelle/cache")
      self.cache_dir.mkdir(parents=True, exist_ok=True)

    # Check if we have pre-populated cache
    pre_cache_dir = Path(__file__).parent.parent / "arelle" / "cache"
    if pre_cache_dir.exists():
      logger.debug(f"Found pre-cached schemas at: {pre_cache_dir}")
      # Copy pre-cached schemas to Arelle's cache directory
      self._populate_cache_from_bundle(pre_cache_dir, self.cache_dir)
    else:
      logger.warning(f"No pre-cached schemas found at: {pre_cache_dir}")

    # Set environment variable for Arelle to use this cache
    # Arelle expects XDG_CACHE_HOME/arelle/cache structure
    if str(self.cache_dir).endswith("/arelle/cache"):
      os.environ["XDG_CACHE_HOME"] = str(self.cache_dir.parent.parent)
    else:
      # If using a custom path, set it directly
      os.environ["XDG_CACHE_HOME"] = str(self.cache_dir.parent)

    logger.debug(f"Arelle cache directory: {self.cache_dir}")
    logger.debug(f"XDG_CACHE_HOME set to: {os.environ.get('XDG_CACHE_HOME')}")

  def _populate_cache_from_bundle(self, source_dir: Path, target_dir: Path):
    """Copy pre-cached schemas to Arelle's cache directory."""
    schemas_copied = 0
    schemas_found = 0

    for source_file in source_dir.glob("**/*"):
      if source_file.is_file() and source_file.suffix in [".xsd", ".dtd", ".xml"]:
        schemas_found += 1
        # Compute relative path
        relative_path = source_file.relative_to(source_dir)
        target_file = target_dir / relative_path

        # Skip if already exists and has content
        if target_file.exists() and target_file.stat().st_size > 0:
          logger.debug(f"Schema already cached: {relative_path}")
          continue

        # Create parent directories
        target_file.parent.mkdir(parents=True, exist_ok=True)

        # Copy file
        try:
          shutil.copy2(source_file, target_file)
          schemas_copied += 1
          logger.debug(f"Copied schema: {relative_path}")
        except Exception as e:
          logger.warning(f"Failed to copy schema {relative_path}: {e}")

    logger.debug(
      f"Cache population: found {schemas_found} schemas, copied {schemas_copied} new ones"
    )
    logger.debug(
      f"Cache directory contents: {list(target_dir.glob('*'))[:5]}..."
    )  # Show first 5 dirs

  def _check_cache_health(self) -> bool:
    """Check if we have sufficient cached schemas to run offline."""
    if not self.cache_dir or not self.cache_dir.exists():
      return False

    # Check for essential W3C schemas
    essential_files = [
      "www.w3.org/2001/xml.xsd",
      "www.w3.org/2001/XMLSchema.xsd",
      "www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd",
    ]

    for file_path in essential_files:
      full_path = self.cache_dir / file_path
      if not full_path.exists() or full_path.stat().st_size == 0:
        logger.debug(f"Missing essential schema: {file_path}")
        return False

    # Count total schemas
    schema_count = len(list(self.cache_dir.glob("**/*.xsd")))
    logger.debug(f"Cache health check: {schema_count} schemas found")

    # Get minimum schema count from environment (default 10 for basic operation)
    min_schema_count = (
      env.ARELLE_MIN_SCHEMA_COUNT if hasattr(env, "ARELLE_MIN_SCHEMA_COUNT") else 10
    )
    return schema_count >= min_schema_count

  def _initialize_controller(self):
    """Initialize Arelle controller with proper settings."""
    logger.debug(f"Initializing Arelle controller (version {ARELLE_VERSION})")

    # Initialize controller
    self.cntlr = Cntlr.Cntlr(
      hasGui=False,
      logFileName=env.ARELLE_LOG_FILE,
      logFileMode="w",
      uiLang=None,
      disable_persistent_config=True,
    )

    # Load plugins after controller initialization
    self._load_plugins()

    # Configure webcache settings
    self._configure_webcache()

    # Register SEC transformations
    self._register_sec_transformations()

  def _load_plugins(self):
    """Load required plugins including EFM validation."""
    if not self.cntlr:
      return

    logger.debug("Loading Arelle plugins")

    # Import and use PluginManager directly
    from arelle import PluginManager
    import sys

    # Add the local EDGAR plugin path to Python path
    edgar_path = os.path.join(os.path.dirname(__file__), "..", "arelle", "EDGAR")
    edgar_path = os.path.abspath(edgar_path)
    if edgar_path not in sys.path:
      sys.path.insert(0, edgar_path)
      logger.debug(f"Added EDGAR plugin path: {edgar_path}")

    # Also add the parent arelle directory to the path
    arelle_plugin_path = os.path.join(os.path.dirname(__file__), "..", "arelle")
    arelle_plugin_path = os.path.abspath(arelle_plugin_path)
    if arelle_plugin_path not in sys.path:
      sys.path.insert(0, arelle_plugin_path)

    # Initialize the plugin manager with the controller
    PluginManager.init(self.cntlr, loadPluginConfig=False)

    # Load plugins that exist
    plugins_to_load = [
      "inlineXbrlDocumentSet",  # For inline XBRL support
      "validate/EFM",  # EFM validation (from base Arelle)
    ]

    for plugin in plugins_to_load:
      try:
        PluginManager.addPluginModule(plugin)
        logger.debug(f"Added plugin module: {plugin}")
      except Exception as e:
        logger.warning(f"Could not load plugin {plugin}: {e}")

    # Load EDGAR plugin - it has 'import': ('EDGAR/render', ) in __pluginInfo__
    try:
      # Load the EDGAR plugin module directly
      PluginManager.addPluginModule("EDGAR")
      logger.debug("Added EDGAR plugin module")

      # Also try loading the transform module specifically
      PluginManager.addPluginModule("EDGAR.transform")
      logger.debug("Added EDGAR.transform module")
    except Exception as e:
      logger.warning(f"Could not load EDGAR plugin modules: {e}")

    # Reset the plugin config to activate loaded plugins
    try:
      PluginManager.reset()
      logger.debug("Plugin manager reset to activate plugins")
    except Exception as e:
      logger.warning(f"Could not reset plugin manager: {e}")

    # After plugins are loaded, check if we need to register SEC transformations
    self._setup_sec_transforms()

  def _configure_webcache(self):
    """Configure webcache to handle HTTP/HTTPS redirect issues and use pre-cached schemas."""
    if not self.cntlr or not self.cntlr.webCache:
      logger.warning("WebCache not initialized")
      return

    webcache = self.cntlr.webCache

    # Set cache directory
    if self.cache_dir:
      webcache.cacheDir = str(self.cache_dir)
      logger.debug(f"WebCache using directory: {webcache.cacheDir}")

    # Set timeout configuration from environment variable
    max_timeout = env.ARELLE_DOWNLOAD_TIMEOUT
    timeout = min(env.ARELLE_TIMEOUT, max_timeout)
    webcache.timeout = timeout
    logger.debug(f"WebCache timeout set to {timeout}s (env: {env.ENVIRONMENT})")

    # Disable retries at the WebCache level
    # maxRetries attribute doesn't exist, retries are handled in _wrap_webcache_with_retry

    # Configure HTTPS redirect behavior
    webcache.httpsRedirect = True

    # Set offline mode if needed or if we detect rate limiting
    work_offline = env.ARELLE_WORK_OFFLINE.lower() == "true"

    # Check cache health but don't force offline - we want to fetch missing schemas
    if self.cache_dir and self._check_cache_health():
      logger.debug(
        "Pre-cached schemas detected, will use cache first but fetch missing ones"
      )
      # Don't force offline - allow fetching schemas we don't have cached
      # work_offline = True

    webcache.workOffline = work_offline

    # Configure redirect overrides for problematic domains
    self._configure_redirects(webcache)

    # Add retry logic wrapper for rate limiting
    self._wrap_webcache_with_retry(webcache)

    if work_offline:
      logger.debug("Arelle configured for offline mode - using pre-cached schemas only")
    else:
      logger.debug(
        "Arelle configured for hybrid mode - cache first, then web with retry"
      )

  def _configure_redirects(self, webcache: WebCache):
    """Configure specific redirects for problematic domains."""
    # Override problematic HTTP URLs to use HTTPS
    redirect_domains = {
      "http://www.w3.org": "https://www.w3.org",
      "http://www.xbrl.org": "https://www.xbrl.org",
      "http://xbrl.org": "https://xbrl.org",
      "http://www.sec.gov": "https://www.sec.gov",
      "http://xbrl.sec.gov": "https://xbrl.sec.gov",
      "http://xbrl.fasb.org": "https://xbrl.fasb.org",
      "http://taxonomies.xbrl.org": "https://taxonomies.xbrl.org",
    }

    # Apply redirects to webcache normalize URL function
    original_normalize = webcache.normalizeUrl

    def normalize_with_redirects(url: Optional[str], base: Optional[str] = None) -> str:
      # First apply original normalization
      normalized = original_normalize(url, base)

      # Then apply our redirects
      for http_url, https_url in redirect_domains.items():
        if normalized.startswith(http_url):
          normalized = normalized.replace(http_url, https_url, 1)
          logger.debug(f"Redirected {url} to {normalized}")

      return normalized

    webcache.normalizeUrl = normalize_with_redirects

  def _wrap_webcache_with_retry(self, webcache: WebCache):
    """Wrap WebCache methods to handle rate limiting without retries."""
    import urllib.error
    from urllib.response import addinfourl
    from io import BytesIO

    # Create a wrapper class for BytesIO that mimics urllib response
    class HTTPResponseWrapper:
      """Wrapper to make BytesIO behave like an HTTP response."""

      def __init__(self, bytesio, url, headers=None):
        self.bytesio = bytesio
        self.url = url
        self.headers = headers or {}
        self.msg = EmailMessage()
        for key, value in self.headers.items():
          self.msg[key] = value

      def read(self, amt=None):
        return self.bytesio.read(amt)

      def readline(self):
        return self.bytesio.readline()

      def readlines(self):
        return self.bytesio.readlines()

      def close(self):
        return self.bytesio.close()

      def info(self):
        """Return headers - required by Arelle."""
        return self.msg

      def getcode(self):
        """Return HTTP status code."""
        return 200

      def geturl(self):
        """Return the URL."""
        return self.url

      def __enter__(self):
        return self

      def __exit__(self, *args):
        self.close()
        return False

      # Make it seekable like BytesIO
      def seek(self, *args, **kwargs):
        return self.bytesio.seek(*args, **kwargs)

      def tell(self):
        return self.bytesio.tell()

    def create_response_wrapper(content_bytes, url):
      """Create a proper response object from bytes."""
      bytesio = BytesIO(content_bytes)
      headers = {
        "Content-Type": "application/xml",
        "Content-Length": str(len(content_bytes)),
      }
      # Create addinfourl object that Arelle expects
      msg = EmailMessage()
      for key, value in headers.items():
        msg[key] = value
      return addinfourl(bytesio, msg, url)

    # Override the _downloadFile method to prevent retries
    original_download = (
      webcache._downloadFile if hasattr(webcache, "_downloadFile") else None
    )
    if original_download:

      def download_without_retry(
        url, filepath, retrievingDueToRecheckInterval=False, retryCount=5
      ):
        """Force retryCount to 1 to prevent retry loops."""
        return original_download(
          url, filepath, retrievingDueToRecheckInterval, retryCount=1
        )

      webcache._downloadFile = download_without_retry
      logger.debug("Disabled Arelle retry mechanism")

    original_opener = webcache.opener.open if hasattr(webcache, "opener") else None

    if not original_opener:
      return

    def open_with_fail_fast(fullurl, data=None, timeout=None):
      """Open URL but fail fast on rate limiting - no retries."""
      # Log what we're trying to fetch for debugging
      if not fullurl.startswith("file://") and "://" in fullurl:
        logger.debug(f"WebCache request for: {fullurl}")

        # Check if it's a W3C/XBRL schema that should be cached
        if any(
          domain in fullurl
          for domain in [
            "www.w3.org",
            "www.xbrl.org",
            "xbrl.org",
            "xbrl.sec.gov",
            "xbrl.fasb.org",
          ]
        ):
          # Try to find it in our pre-populated cache FIRST to avoid rate limiting
          if webcache.cacheDir:
            from urllib.parse import urlparse

            parsed = urlparse(fullurl)

            # Check multiple possible cache locations
            cache_paths = []

            # Try direct path
            cache_paths.append(
              Path(webcache.cacheDir) / parsed.netloc / parsed.path.lstrip("/")
            )

            # Try with http/https subdirectory
            for protocol in ["http", "https"]:
              cache_paths.append(
                Path(webcache.cacheDir)
                / protocol
                / parsed.netloc
                / parsed.path.lstrip("/")
              )

            # If it's HTTP, also try HTTPS version
            if fullurl.startswith("http://"):
              https_parsed = urlparse(fullurl.replace("http://", "https://", 1))
              cache_paths.append(
                Path(webcache.cacheDir)
                / https_parsed.netloc
                / https_parsed.path.lstrip("/")
              )
              cache_paths.append(
                Path(webcache.cacheDir)
                / "https"
                / https_parsed.netloc
                / https_parsed.path.lstrip("/")
              )

            # Check if any cache file exists
            for potential_cache_file in cache_paths:
              if potential_cache_file.exists():
                logger.debug(
                  f"Using pre-cached file instead of fetching: {fullurl} -> {potential_cache_file}"
                )
                # Return the cached content with proper wrapper
                try:
                  with open(potential_cache_file, "rb") as f:
                    content = f.read()
                    return create_response_wrapper(content, fullurl)
                except Exception as e:
                  logger.warning(
                    f"Could not read cache file {potential_cache_file}: {e}"
                  )

            # Log if not found in cache
            logger.debug(f"Not in pre-populated cache, will try to fetch: {fullurl}")

      try:
        return original_opener(fullurl, data, timeout)

      except urllib.error.HTTPError as e:
        if e.code == 503 or e.code == 429:  # Service Unavailable or Too Many Requests
          logger.warning(
            f"Rate limited ({e.code}) on {fullurl}, returning empty to continue processing"
          )

          # Check if we have it in cache (try both HTTP and HTTPS versions)
          if webcache.cacheDir:
            try:
              # Try the original URL first
              cache_filepath = webcache.urlToCacheFilepath(fullurl)

              # If not found and it's HTTP, try HTTPS version
              if (
                not cache_filepath or not os.path.exists(cache_filepath)
              ) and fullurl.startswith("http://"):
                https_url = fullurl.replace("http://", "https://", 1)
                cache_filepath = webcache.urlToCacheFilepath(https_url)
                logger.debug(
                  f"Trying HTTPS cache path for HTTP URL: {fullurl} -> {https_url}"
                )

              # Also check the direct cache structure (domain/path)
              if not cache_filepath or not os.path.exists(cache_filepath):
                from urllib.parse import urlparse

                parsed = urlparse(fullurl)
                # Try both http and https subdirectories
                for protocol in ["http", "https", ""]:
                  if protocol:
                    potential_path = (
                      Path(webcache.cacheDir)
                      / protocol
                      / parsed.netloc
                      / parsed.path.lstrip("/")
                    )
                  else:
                    potential_path = (
                      Path(webcache.cacheDir) / parsed.netloc / parsed.path.lstrip("/")
                    )

                  if potential_path.exists():
                    cache_filepath = str(potential_path)
                    logger.debug(f"Found cache file at: {cache_filepath}")
                    break

              if cache_filepath and os.path.exists(cache_filepath):
                logger.debug(
                  f"Using cached version after rate limit: {fullurl} from {cache_filepath}"
                )
                with open(cache_filepath, "rb") as f:
                  content = f.read()
                  logger.debug(f"Loaded {len(content)} bytes from cache")
                  return create_response_wrapper(content, fullurl)
              else:
                logger.warning(
                  f"Cache file not found for {fullurl}, tried: {cache_filepath}"
                )
            except Exception as e:
              logger.error(f"Could not check cache for {fullurl}: {e}")

          # Return empty response immediately - no retries
          logger.warning(f"No cached version for {fullurl}, returning empty")
          return create_response_wrapper(b"", fullurl)
        else:
          # For other errors, just propagate them
          raise
      except Exception as e:
        logger.warning(f"Error fetching {fullurl}: {e}")
        raise

    # Replace the opener's open method
    if hasattr(webcache, "opener"):
      webcache.opener.open = open_with_fail_fast

  def _register_sec_transformations(self):
    """Register SEC inline XBRL transformations."""
    # This will be called from _setup_sec_transforms after plugins are loaded
    pass

  def _setup_sec_transforms(self):
    """Setup SEC inline XBRL transformations after plugins are loaded."""
    if not self.cntlr:
      return

    try:
      # The EDGAR transform plugin should have registered the SEC transformations
      # Let's verify they're loaded
      from arelle import FunctionIxt

      # SEC transformation namespace
      sec_namespace = "http://www.sec.gov/inlineXBRL/transformation/2015-08-31"

      # Try to manually register SEC transforms if they're not loaded
      try:
        # Import the EDGAR transform module
        import EDGAR.transform as edgar_transform  # type: ignore[import]

        # The EDGAR transform module has functions like duryear, durmonth, etc.
        sec_transforms = {
          "duryear": edgar_transform.duryear,
          "durmonth": edgar_transform.durmonth,
          "durweek": edgar_transform.durweek,
          "durday": edgar_transform.durday,
          "durhour": edgar_transform.durhour,
          "datequarterend": edgar_transform.datequarterend,
          "numwordsen": edgar_transform.numwordsen,
          "durwordsen": edgar_transform.durwordsen,
          "boolballotbox": edgar_transform.boolballotbox,
          "yesnoballotbox": edgar_transform.yesnoballotbox,
          "numinf": edgar_transform.numinf,
          "numneginf": edgar_transform.numneginf,
          "numnan": edgar_transform.numnan,
          "stateprovnameen": edgar_transform.stateprovnameen,
          "exchnameen": edgar_transform.exchnameen,
          "entityfilercategoryen": edgar_transform.entityfilercategoryen,
          "countrynameen": edgar_transform.countrynameen,
          "edgarprovcountryen": edgar_transform.edgarprovcountryen,
        }

        # Register the transforms with FunctionIxt
        if not hasattr(FunctionIxt, "ixtNamespaceFunctions"):
          FunctionIxt.ixtNamespaceFunctions = {}

        if sec_namespace not in FunctionIxt.ixtNamespaceFunctions:
          FunctionIxt.ixtNamespaceFunctions[sec_namespace] = sec_transforms
          logger.debug(f"Manually registered {len(sec_transforms)} SEC transformations")
        else:
          logger.debug("SEC transformations already registered")

      except ImportError as e:
        logger.warning(f"Could not import EDGAR transform module: {e}")

      # Verify registration
      if hasattr(FunctionIxt, "ixtNamespaceFunctions"):
        if sec_namespace in FunctionIxt.ixtNamespaceFunctions:
          transform_count = len(FunctionIxt.ixtNamespaceFunctions[sec_namespace])
          logger.debug(
            f"SEC transformations successfully registered: {transform_count} transforms"
          )
        else:
          logger.warning(f"SEC transformation namespace not found: {sec_namespace}")
      else:
        logger.warning("FunctionIxt does not have ixtNamespaceFunctions attribute")

    except Exception as e:
      logger.warning(f"Error setting up SEC transforms: {e}")

  def controller(self, url: str) -> ModelXbrl.ModelXbrl:
    """
    Load an XBRL document and return the model.

    Args:
        url: URL or file path to the XBRL document

    Returns:
        ModelXbrl instance with loaded document

    Raises:
        Exception: If document fails to load
    """
    if not self.cntlr:
      self._initialize_controller()

    assert self.cntlr is not None  # Ensure controller is initialized

    logger.debug(f"Loading XBRL document from: {url}")

    # Set up filing parameters for EFM validation if needed
    if self._is_sec_filing(url):
      self._configure_efm_validation()

    try:
      # Load the model
      modelXbrl = self.cntlr.modelManager.load(url)

      if not modelXbrl or modelXbrl.modelDocument is None:
        raise Exception(f"Failed to load XBRL document from {url}")

      # Log any errors
      if modelXbrl.errors:
        logger.error(f"Errors loading {url}: {modelXbrl.errors}")

      return modelXbrl

    except Exception as e:
      logger.error(f"Error loading XBRL document: {str(e)}")
      raise

  def validate(self, modelXbrl: ModelXbrl.ModelXbrl) -> Dict[str, Any]:
    """
    Validate an XBRL model with EFM validation if applicable.

    Args:
        modelXbrl: The model to validate

    Returns:
        Dictionary with validation results
    """
    logger.debug("Starting XBRL validation")

    # Import validation module
    from arelle import ValidateXbrl

    # Create validator
    validate = ValidateXbrl.ValidateXbrl(modelXbrl)

    # Set validation parameters
    validate.validate(modelXbrl)

    # Collect results
    results = {
      "valid": len(modelXbrl.errors) == 0,
      "errors": [str(e) for e in modelXbrl.errors],
      "warnings": [],  # Warnings are not available in this version
      "efm_validated": self._is_efm_enabled(),
    }

    logger.info(f"Validation complete: {results['valid']}")
    return results

  def _is_sec_filing(self, url: str) -> bool:
    """Check if the URL appears to be an SEC filing."""
    sec_indicators = [
      "sec.gov",
      "edgar",
      "10-K",
      "10-Q",
      "8-K",
      "20-F",
      "S-1",
      "DEF14A",
    ]
    url_lower = url.lower()
    return any(indicator.lower() in url_lower for indicator in sec_indicators)

  def _configure_efm_validation(self):
    """Configure EFM validation parameters."""
    if not self.cntlr:
      return

    logger.info("Configuring EFM validation for SEC filing")

    # Set disclosure system - try different EFM disclosure system names
    disclosure_systems = ["efm-all-years", "efm-entire-us", "efm", "us-gaap"]

    for ds in disclosure_systems:
      try:
        self.cntlr.modelManager.disclosureSystem.select(ds)
        logger.info(f"Successfully set disclosure system to: {ds}")
        break
      except Exception as e:
        logger.debug(f"Could not set disclosure system to {ds}: {e}")

    # Set additional EFM parameters if supported
    if hasattr(self.cntlr, "efmFilingParms"):
      setattr(
        self.cntlr,
        "efmFilingParms",
        {
          "exhibitType": "EX-101",
          "includeExhibit": True,
        },
      )

  def _is_efm_enabled(self) -> bool:
    """Check if EFM validation is enabled."""
    if not self.cntlr:
      return False

    disclosure_system = self.cntlr.modelManager.disclosureSystem.selection
    return bool(disclosure_system and "efm" in disclosure_system.lower())

  def close(self):
    """Clean up resources."""
    if self.cntlr:
      self.cntlr.close()
      self.cntlr = None


# For backward compatibility
if __name__ == "__main__":
  # Test the client
  client = ArelleClient()
  test_url = "https://www.sec.gov/Archives/edgar/data/1045810/000104581025000116/nvda-20250427.htm"

  try:
    model = client.controller(test_url)
    results = client.validate(model)
    print(f"Validation results: {results}")
  except Exception as e:
    print(f"Error: {e}")
  finally:
    client.close()
