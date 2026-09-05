import os
import shutil
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from arelle import Cntlr, ModelXbrl
from arelle.Version import __version__ as ARELLE_VERSION
from arelle.WebCache import WebCache
from xbrlkit.parse import register_sec_transforms
from xbrlkit.parse.arelle_load import SEC_IXT_NAMESPACE

from robosystems.adapters.sec.config import (
  ARELLE_DOWNLOAD_TIMEOUT,
  ARELLE_LOG_FILE,
  ARELLE_MIN_SCHEMA_COUNT,
  ARELLE_TIMEOUT,
  ARELLE_WORK_OFFLINE,
)
from robosystems.config import env
from robosystems.logger import get_logger

logger = get_logger(__name__)


class ArelleClient:
  """
  Loads and validates XBRL documents through Arelle.

  Works around the HTTP/HTTPS redirect behavior of xbrl.org and configures
  EFM validation for SEC filings. Schemas are served from a pre-populated
  local cache so a filing run does not hammer (and get throttled by) the
  taxonomy hosts.
  """

  def __init__(self):
    self.cntlr = None
    self.cache_dir = None
    self._setup_cache_directory()
    self._initialize_controller()

  def _setup_cache_directory(self):
    """Pick a writable cache directory and seed it with the bundled schemas."""
    if env.ARELLE_CACHE_DIR:
      self.cache_dir = Path(env.ARELLE_CACHE_DIR)
    else:
      # The mounted volume is writable in local development; prod/staging run
      # on a read-only filesystem and fall back to /tmp.
      mounted_cache = Path(__file__).parent.parent / "arelle" / "cache"
      if mounted_cache.exists() and os.access(mounted_cache, os.W_OK):
        self.cache_dir = mounted_cache
      else:
        self.cache_dir = Path("/tmp/arelle/cache")

    try:
      self.cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
      logger.warning(f"Cannot create cache at {self.cache_dir}: {e}, using /tmp")
      self.cache_dir = Path("/tmp/arelle/cache")
      self.cache_dir.mkdir(parents=True, exist_ok=True)

    pre_cache_dir = Path(__file__).parent.parent / "arelle" / "cache"
    if pre_cache_dir.exists():
      logger.debug(f"Found pre-cached schemas at: {pre_cache_dir}")
      self._populate_cache_from_bundle(pre_cache_dir, self.cache_dir)
    else:
      logger.warning(f"No pre-cached schemas found at: {pre_cache_dir}")

    # Arelle reads its cache from XDG_CACHE_HOME/arelle/cache, so point
    # XDG_CACHE_HOME at whichever ancestor makes that resolve to cache_dir.
    if str(self.cache_dir).endswith("/arelle/cache"):
      os.environ["XDG_CACHE_HOME"] = str(self.cache_dir.parent.parent)
    else:
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
        relative_path = source_file.relative_to(source_dir)
        target_file = target_dir / relative_path

        if target_file.exists() and target_file.stat().st_size > 0:
          logger.debug(f"Schema already cached: {relative_path}")
          continue

        target_file.parent.mkdir(parents=True, exist_ok=True)

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

    schema_count = len(list(self.cache_dir.glob("**/*.xsd")))
    logger.debug(f"Cache health check: {schema_count} schemas found")

    return schema_count >= ARELLE_MIN_SCHEMA_COUNT

  def _initialize_controller(self):
    """Initialize Arelle controller with proper settings."""
    logger.debug(f"Initializing Arelle controller (version {ARELLE_VERSION})")

    self.cntlr = Cntlr.Cntlr(
      hasGui=False,
      logFileName=ARELLE_LOG_FILE,
      logFileMode="w",
      uiLang=None,
      disable_persistent_config=True,
    )

    # Order matters: plugins must exist before the webcache is reconfigured,
    # and the SEC transforms are registered by the EDGAR plugin.
    self._load_plugins()
    self._configure_webcache()
    self._register_sec_transformations()

  def _load_plugins(self):
    """Load the inline-XBRL document-set plugin and the SEC transforms.

    ``inlineXbrlDocumentSet`` is load-bearing: without it Arelle treats an
    inline 10-K as plain HTML and every fact silently drops. The SEC
    ``ixt-sec`` transforms come from xbrlkit's vendored EDGAR transform
    registry (``_setup_sec_transforms``). EFM validation is not loaded: the
    validator left arelle-release for the Arelle/EDGAR repository, and
    nothing in the pipeline consumes its findings.
    """
    if not self.cntlr:
      return

    logger.debug("Loading Arelle plugins")

    from arelle import PluginManager

    PluginManager.init(self.cntlr, loadPluginConfig=False)

    try:
      PluginManager.addPluginModule("inlineXbrlDocumentSet")
      logger.debug("Added plugin module: inlineXbrlDocumentSet")
    except Exception as e:
      logger.warning(f"Could not load plugin inlineXbrlDocumentSet: {e}")

    # reset() is what actually activates everything added above.
    try:
      PluginManager.reset()
      logger.debug("Plugin manager reset to activate plugins")
    except Exception as e:
      logger.warning(f"Could not reset plugin manager: {e}")

    self._setup_sec_transforms()

  def _configure_webcache(self):
    """Configure webcache to handle HTTP/HTTPS redirect issues and use pre-cached schemas."""
    if not self.cntlr or not self.cntlr.webCache:
      logger.warning("WebCache not initialized")
      return

    webcache = self.cntlr.webCache

    if self.cache_dir:
      webcache.cacheDir = str(self.cache_dir)
      logger.debug(f"WebCache using directory: {webcache.cacheDir}")

    max_timeout = ARELLE_DOWNLOAD_TIMEOUT
    timeout = min(ARELLE_TIMEOUT, max_timeout)
    webcache.timeout = timeout
    logger.debug(f"WebCache timeout set to {timeout}s (env: {env.ENVIRONMENT})")

    # WebCache exposes no maxRetries knob; retry behavior is imposed in
    # _wrap_webcache_with_retry instead.

    webcache.httpsRedirect = True

    work_offline = ARELLE_WORK_OFFLINE

    # A healthy cache does NOT force offline mode — the cache is consulted
    # first, and anything missing is still fetched from the web.
    if self.cache_dir and self._check_cache_health():
      logger.debug(
        "Pre-cached schemas detected, will use cache first but fetch missing ones"
      )

    webcache.workOffline = work_offline

    self._configure_redirects(webcache)
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

    original_normalize = webcache.normalizeUrl

    def normalize_with_redirects(url: str | None, base: str | None = None) -> str:
      normalized = original_normalize(url, base)

      for http_url, https_url in redirect_domains.items():
        if normalized.startswith(http_url):
          normalized = normalized.replace(http_url, https_url, 1)
          logger.debug(f"Redirected {url} to {normalized}")

      return normalized

    webcache.normalizeUrl = normalize_with_redirects

  def _wrap_webcache_with_retry(self, webcache: WebCache):
    """Wrap WebCache methods to handle rate limiting without retries."""
    import urllib.error
    from io import BytesIO
    from urllib.response import addinfourl

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
      # Arelle expects an addinfourl, not a bare file object.
      msg = EmailMessage()
      for key, value in headers.items():
        msg[key] = value
      return addinfourl(bytesio, msg, url)

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
      if not fullurl.startswith("file://") and "://" in fullurl:
        logger.debug(f"WebCache request for: {fullurl}")

        # W3C / XBRL schema hosts throttle aggressively, so the local cache is
        # searched before any network call for those domains.
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
          if webcache.cacheDir:
            from urllib.parse import urlparse

            parsed = urlparse(fullurl)

            # Cached schemas land under either {host}/{path} or
            # {scheme}/{host}/{path} depending on which writer put them there,
            # so every layout is probed before giving up on the cache.
            cache_paths = []

            cache_paths.append(
              Path(webcache.cacheDir) / parsed.netloc / parsed.path.lstrip("/")
            )

            for protocol in ["http", "https"]:
              cache_paths.append(
                Path(webcache.cacheDir)
                / protocol
                / parsed.netloc
                / parsed.path.lstrip("/")
              )

            # Schemas are usually cached under their https:// form even when
            # the taxonomy references them over http://.
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

            for potential_cache_file in cache_paths:
              if potential_cache_file.exists():
                logger.debug(
                  f"Using pre-cached file instead of fetching: {fullurl} -> {potential_cache_file}"
                )
                try:
                  with open(potential_cache_file, "rb") as f:
                    content = f.read()
                    return create_response_wrapper(content, fullurl)
                except Exception as e:
                  logger.warning(
                    f"Could not read cache file {potential_cache_file}: {e}"
                  )

            logger.debug(f"Not in pre-populated cache, will try to fetch: {fullurl}")

      try:
        return original_opener(fullurl, data, timeout)

      except urllib.error.HTTPError as e:
        if e.code == 503 or e.code == 429:  # Service Unavailable or Too Many Requests
          logger.warning(
            f"Rate limited ({e.code}) on {fullurl}, returning empty to continue processing"
          )

          # Fall back to the cache before giving up, probing the same layouts
          # as the pre-fetch path above.
          if webcache.cacheDir:
            try:
              cache_filepath = webcache.urlToCacheFilepath(fullurl)

              if (
                not cache_filepath or not os.path.exists(cache_filepath)
              ) and fullurl.startswith("http://"):
                https_url = fullurl.replace("http://", "https://", 1)
                cache_filepath = webcache.urlToCacheFilepath(https_url)
                logger.debug(
                  f"Trying HTTPS cache path for HTTP URL: {fullurl} -> {https_url}"
                )

              if not cache_filepath or not os.path.exists(cache_filepath):
                from urllib.parse import urlparse

                parsed = urlparse(fullurl)
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

          # An empty body lets the filing keep processing; retrying here would
          # only deepen the throttle.
          logger.warning(f"No cached version for {fullurl}, returning empty")
          return create_response_wrapper(b"", fullurl)
        else:
          raise
      except Exception as e:
        logger.warning(f"Error fetching {fullurl}: {e}")
        raise

    if hasattr(webcache, "opener"):
      webcache.opener.open = open_with_fail_fast

  def _register_sec_transformations(self):
    """No-op hook; the real work happens in `_setup_sec_transforms`."""
    pass

  def _setup_sec_transforms(self):
    """Register the SEC inline-XBRL transforms (``ixt-sec``) with Arelle.

    Standalone arelle-release does not carry the SEC ``2015-08-31``
    transforms (``stateprovnameen``, ``numwordsen``, ``durwordsen``, …);
    without them every SEC-formatted cover-page fact parses to
    ``(ixTransformValueError)``. xbrlkit vendors the EDGAR plugin's transform
    registry and registers it directly into the namespace map Arelle
    resolves ``ix:`` transforms against. Idempotent.
    """
    if not self.cntlr:
      return

    from arelle import FunctionIxt

    register_sec_transforms()

    registered = getattr(FunctionIxt, "ixtNamespaceFunctions", {}).get(
      SEC_IXT_NAMESPACE
    )
    if registered:
      logger.debug(f"SEC transformations registered: {len(registered)} transforms")
    else:
      logger.warning(
        f"SEC transformation namespace not registered: {SEC_IXT_NAMESPACE}"
      )

  def controller(self, url: str) -> ModelXbrl.ModelXbrl:
    """Load an XBRL document (URL or local path) into a ModelXbrl.

    EFM validation parameters are configured first when the URL looks like an
    SEC filing. Raises if the document fails to load.
    """
    if not self.cntlr:
      self._initialize_controller()

    assert self.cntlr is not None  # Ensure controller is initialized

    logger.debug(f"Loading XBRL document from: {url}")

    if self._is_sec_filing(url):
      self._configure_efm_validation()

    try:
      modelXbrl = self.cntlr.modelManager.load(url)

      if not modelXbrl or modelXbrl.modelDocument is None:
        raise Exception(f"Failed to load XBRL document from {url}")

      if modelXbrl.errors:
        logger.error(f"Errors loading {url}: {modelXbrl.errors}")

      return modelXbrl

    except Exception as e:
      logger.error(f"Error loading XBRL document: {e!s}")
      raise

  def validate(self, modelXbrl: ModelXbrl.ModelXbrl) -> dict[str, Any]:
    """Validate a model, applying EFM rules when the filing is an SEC one.

    Returns `{"valid", "errors", "warnings", "efm_validated"}`.
    """
    logger.debug("Starting XBRL validation")

    from arelle import ValidateXbrl

    validate = ValidateXbrl.ValidateXbrl(modelXbrl)
    validate.validate(modelXbrl)

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

    # Arelle builds vary in which EFM disclosure system names they expose;
    # take the first that the installed version accepts.
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
      self.cntlr.efmFilingParms = {"exhibitType": "EX-101", "includeExhibit": True}

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


if __name__ == "__main__":
  # Smoke test: load and validate one real filing.
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
