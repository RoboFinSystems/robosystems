#!/usr/bin/env python
"""
Arelle Cache Manager - Download, bundle, and manage Arelle schemas and EDGAR plugin.

This script handles:
1. Downloading XBRL schemas for offline processing
2. Fetching the EDGAR plugin from GitHub
3. Creating tar.gz bundles for fast Docker builds
4. Extracting bundles during Docker build
5. Checking if cache needs updating
"""

import argparse
import json
import logging
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class ArelleCacheManager:
  """Manages Arelle schema cache and EDGAR plugin bundles."""

  # Core XBRL schemas that must be cached
  SCHEMAS = [
    # W3C base schemas
    ("http://www.w3.org/2001/xml.xsd", "www.w3.org/2001/xml.xsd"),
    ("http://www.w3.org/2001/XMLSchema.dtd", "www.w3.org/2001/XMLSchema.dtd"),
    ("http://www.w3.org/2001/XMLSchema.xsd", "www.w3.org/2001/XMLSchema.xsd"),
    ("http://www.w3.org/2001/datatypes.dtd", "www.w3.org/2001/datatypes.dtd"),
    ("http://www.w3.org/1999/xlink.xsd", "www.w3.org/1999/xlink.xsd"),
    ("http://www.w3.org/XML/1998/namespace.xsd", "www.w3.org/XML/1998/namespace.xsd"),
    # Core XBRL schemas
    (
      "http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd",
      "www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd",
    ),
    (
      "http://www.xbrl.org/2003/xbrl-linkbase-2003-12-31.xsd",
      "www.xbrl.org/2003/xbrl-linkbase-2003-12-31.xsd",
    ),
    (
      "http://www.xbrl.org/2003/xl-2003-12-31.xsd",
      "www.xbrl.org/2003/xl-2003-12-31.xsd",
    ),
    (
      "http://www.xbrl.org/2003/xlink-2003-12-31.xsd",
      "www.xbrl.org/2003/xlink-2003-12-31.xsd",
    ),
    ("http://www.xbrl.org/2005/xbrldt-2005.xsd", "www.xbrl.org/2005/xbrldt-2005.xsd"),
    (
      "http://www.xbrl.org/2006/ref-2006-02-27.xsd",
      "www.xbrl.org/2006/ref-2006-02-27.xsd",
    ),
    (
      "http://www.xbrl.org/dtr/type/numeric-2009-12-16.xsd",
      "www.xbrl.org/dtr/type/numeric-2009-12-16.xsd",
    ),
    (
      "http://www.xbrl.org/dtr/type/nonNumeric-2009-12-16.xsd",
      "www.xbrl.org/dtr/type/nonNumeric-2009-12-16.xsd",
    ),
    (
      "http://www.xbrl.org/lrr/role/reference-2009-12-16.xsd",
      "www.xbrl.org/lrr/role/reference-2009-12-16.xsd",
    ),
    # Additional XBRL schemas (for https variants)
    (
      "https://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd",
      "www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd",
    ),
    (
      "https://www.xbrl.org/2003/xbrl-linkbase-2003-12-31.xsd",
      "www.xbrl.org/2003/xbrl-linkbase-2003-12-31.xsd",
    ),
    # Inline XBRL schemas
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1-modules.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1-modules.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1-definitions.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1-definitions.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1-model.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1-model.xsd",
    ),
    # XHTML schemas for inline XBRL
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-datatypes-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-datatypes-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-framework-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-framework-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-attribs-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-attribs-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-text-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-text-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-blkphras-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-blkphras-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-blkstruct-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-blkstruct-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-inlphras-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-inlphras-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-inlstruct-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-inlstruct-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-hypertext-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-hypertext-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-list-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-list-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-struct-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-struct-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-edit-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-edit-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-bdo-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-bdo-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-style-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-style-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-inlstyle-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-inlstyle-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-image-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-image-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-csismap-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-csismap-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-ssismap-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-ssismap-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-object-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-object-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-param-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-param-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-table-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-table-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-form-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-form-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-ruby-basic-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-ruby-basic-1.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-events-1.xsd",
      "www.xbrl.org/2013/inlineXBRL/xhtml/xhtml-events-1.xsd",
    ),
    # Modified XBRL schemas for inline XBRL
    (
      "http://www.xbrl.org/2013/inlineXBRL/xbrl/xl-2003-12-31.xsd",
      "www.xbrl.org/2013/inlineXBRL/xbrl/xl-2003-12-31.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xbrl/xlink-2003-12-31.xsd",
      "www.xbrl.org/2013/inlineXBRL/xbrl/xlink-2003-12-31.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xbrl/xbrl-instance-2003-12-31-ixmod.xsd",
      "www.xbrl.org/2013/inlineXBRL/xbrl/xbrl-instance-2003-12-31-ixmod.xsd",
    ),
    (
      "http://www.xbrl.org/2013/inlineXBRL/xbrl/xbrl-linkbase-2003-12-31-ixmod.xsd",
      "www.xbrl.org/2013/inlineXBRL/xbrl/xbrl-linkbase-2003-12-31-ixmod.xsd",
    ),
    # Additional XBRL types and roles
    (
      "https://www.xbrl.org/dtr/type/2022-03-31/types.xsd",
      "www.xbrl.org/dtr/type/2022-03-31/types.xsd",
    ),
    (
      "https://www.xbrl.org/2020/extensible-enumerations-2.0.xsd",
      "www.xbrl.org/2020/extensible-enumerations-2.0.xsd",
    ),
    (
      "https://www.xbrl.org/dtr/type/2020-01-21/types.xsd",
      "www.xbrl.org/dtr/type/2020-01-21/types.xsd",
    ),
    (
      "https://www.xbrl.org/lrr/role/negated-2009-12-16.xsd",
      "www.xbrl.org/lrr/role/negated-2009-12-16.xsd",
    ),
    (
      "https://www.xbrl.org/lrr/role/net-2009-12-16.xsd",
      "www.xbrl.org/lrr/role/net-2009-12-16.xsd",
    ),
    # SEC schemas
    (
      "https://xbrl.sec.gov/dei/2024/dei-2024.xsd",
      "xbrl.sec.gov/dei/2024/dei-2024.xsd",
    ),
    (
      "https://xbrl.sec.gov/dei/2023/dei-2023.xsd",
      "xbrl.sec.gov/dei/2023/dei-2023.xsd",
    ),
    (
      "https://xbrl.sec.gov/dei/2022/dei-2022.xsd",
      "xbrl.sec.gov/dei/2022/dei-2022.xsd",
    ),
    # FASB US-GAAP schemas
    (
      "https://xbrl.fasb.org/us-gaap/2024/elts/us-gaap-2024.xsd",
      "xbrl.fasb.org/us-gaap/2024/elts/us-gaap-2024.xsd",
    ),
    (
      "https://xbrl.fasb.org/us-gaap/2023/elts/us-gaap-2023.xsd",
      "xbrl.fasb.org/us-gaap/2023/elts/us-gaap-2023.xsd",
    ),
    (
      "https://xbrl.fasb.org/us-gaap/2022/elts/us-gaap-2022.xsd",
      "xbrl.fasb.org/us-gaap/2022/elts/us-gaap-2022.xsd",
    ),
  ]

  def __init__(self, project_root: Optional[Path] = None):
    """Initialize the cache manager."""
    if project_root:
      self.project_root = Path(project_root)
    else:
      # Find project root by looking for pyproject.toml
      current = Path.cwd()
      while current != current.parent:
        if (current / "pyproject.toml").exists():
          self.project_root = current
          break
        current = current.parent
      else:
        self.project_root = Path.cwd()

    self.cache_dir = self.project_root / "robosystems" / "arelle" / "cache"
    self.edgar_dir = self.project_root / "robosystems" / "arelle" / "EDGAR"
    self.bundles_dir = self.project_root / "robosystems" / "arelle" / "bundles"

  def download_schema(self, url: str, cache_path: Path, retries: int = 3) -> bool:
    """Download a single schema file."""
    for attempt in range(retries):
      try:
        logger.debug(f"Downloading ({attempt + 1}/{retries}): {url}")

        # Create parent directory if needed
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Download the file
        with urllib.request.urlopen(url, timeout=30) as response:
          content = response.read()

        # Write to cache
        cache_path.write_bytes(content)
        logger.info(f"Successfully cached: {cache_path.name} ({len(content)} bytes)")
        return True

      except Exception as e:
        logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
        if attempt < retries - 1:
          time.sleep(1)  # Brief delay between retries

    logger.error(f"Failed to download after {retries} attempts: {url}")
    return False

  def download_schemas(self) -> int:
    """Download all XBRL schemas."""
    logger.info("Downloading XBRL schemas...")

    # Create cache directory
    self.cache_dir.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped = 0
    failed = 0

    for url, relative_path in self.SCHEMAS:
      cache_path = self.cache_dir / relative_path

      # Skip if already cached
      if cache_path.exists():
        logger.debug(f"Already cached: {relative_path}")
        skipped += 1
        continue

      # Download the schema
      if self.download_schema(url, cache_path):
        downloaded += 1
      else:
        failed += 1

    # Create metadata file
    metadata_path = self.cache_dir / "cache_metadata.json"
    metadata = {
      "created": datetime.now().isoformat(),
      "total_schemas": len(self.SCHEMAS),
      "downloaded": downloaded,
      "skipped": skipped,
      "failed": failed,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2))

    logger.info(
      f"Schema download complete: {downloaded} new, {skipped} existing, {failed} failed"
    )
    return downloaded + skipped

  def fetch_edgar_plugin(self) -> bool:
    """Fetch EDGAR plugin from GitHub."""
    logger.info("Fetching EDGAR plugin from GitHub...")

    # Clean existing EDGAR directory
    if self.edgar_dir.exists():
      shutil.rmtree(self.edgar_dir)

    # Work in the parent directory (arelle/) and let git create EDGAR/
    parent_dir = self.edgar_dir.parent
    parent_dir.mkdir(parents=True, exist_ok=True)

    try:
      # Use git to fetch EDGAR plugin (shallow clone) in parent directory
      # The --prefix=EDGAR will create the EDGAR subdirectory
      commands = [
        ["git", "init"],
        [
          "git",
          "remote",
          "add",
          "edgar-upstream",
          "https://github.com/Arelle/EDGAR.git",
        ],
        ["git", "fetch", "edgar-upstream", "master", "--depth=1"],
        ["git", "read-tree", "--prefix=EDGAR", "-u", "edgar-upstream/master"],
      ]

      for cmd in commands:
        result = subprocess.run(cmd, cwd=parent_dir, capture_output=True, text=True)
        if result.returncode != 0:
          logger.error(f"Git command failed: {' '.join(cmd)}")
          logger.error(f"Error: {result.stderr}")
          return False

      # Remove git metadata from parent directory
      git_dir = parent_dir / ".git"
      if git_dir.exists():
        shutil.rmtree(git_dir)

      logger.info("EDGAR plugin fetched successfully")
      return True

    except Exception as e:
      logger.error(f"Failed to fetch EDGAR plugin: {e}")
      return False

  def create_bundles(self) -> Tuple[Optional[Path], Optional[Path]]:
    """Create tar.gz bundles for schemas and EDGAR plugin."""
    logger.info("Creating cache bundles...")

    # Create bundles directory
    self.bundles_dir.mkdir(parents=True, exist_ok=True)

    # Get current date for versioning
    date_str = datetime.now().strftime("%Y%m%d")

    schema_bundle = None
    edgar_bundle = None

    # Create schemas bundle
    if self.cache_dir.exists() and list(self.cache_dir.iterdir()):
      schema_bundle_path = self.bundles_dir / f"arelle-schemas-{date_str}.tar.gz"
      logger.info(f"  Creating schema bundle: {schema_bundle_path.name}")

      with tarfile.open(schema_bundle_path, "w:gz") as tar:
        tar.add(self.cache_dir, arcname="cache")

      # Create symlink to latest
      latest_link = self.bundles_dir / "arelle-schemas-latest.tar.gz"
      if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
      latest_link.symlink_to(schema_bundle_path.name)

      size_mb = schema_bundle_path.stat().st_size / (1024 * 1024)
      logger.info(f"  Schema bundle created: {size_mb:.1f}MB")
      schema_bundle = schema_bundle_path
    else:
      logger.warning("  No schemas found to bundle")

    # Create EDGAR bundle
    edgar_path = self.edgar_dir / "EDGAR"
    if edgar_path.exists() and list(edgar_path.iterdir()):
      edgar_bundle_path = self.bundles_dir / f"edgar-plugin-{date_str}.tar.gz"
      logger.info(f"  Creating EDGAR bundle: {edgar_bundle_path.name}")

      with tarfile.open(edgar_bundle_path, "w:gz") as tar:
        tar.add(edgar_path, arcname="EDGAR")

      # Create symlink to latest
      latest_link = self.bundles_dir / "edgar-plugin-latest.tar.gz"
      if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
      latest_link.symlink_to(edgar_bundle_path.name)

      size_mb = edgar_bundle_path.stat().st_size / (1024 * 1024)
      logger.info(f"  EDGAR bundle created: {size_mb:.1f}MB")
      edgar_bundle = edgar_bundle_path
    else:
      logger.warning("  No EDGAR plugin found to bundle")

    # Show bundle summary
    logger.info("\nBundle Summary:")
    for bundle in self.bundles_dir.glob("*.tar.gz"):
      if not bundle.is_symlink():
        size_mb = bundle.stat().st_size / (1024 * 1024)
        logger.info(f"  {bundle.name}: {size_mb:.1f}MB")

    return schema_bundle, edgar_bundle

  def extract_bundles(self) -> bool:
    """Extract cache bundles for Docker build."""
    logger.info("Extracting cache bundles...")

    success = True

    # Extract schemas
    schema_bundle = self.bundles_dir / "arelle-schemas-latest.tar.gz"
    if schema_bundle.exists():
      logger.info("  Extracting schema bundle...")
      with tarfile.open(schema_bundle, "r:gz") as tar:
        tar.extractall(self.cache_dir.parent)
      logger.info("  Schemas extracted")
    else:
      logger.warning("  No schema bundle found")
      success = False

    # Extract EDGAR
    edgar_bundle = self.bundles_dir / "edgar-plugin-latest.tar.gz"
    if edgar_bundle.exists():
      logger.info("  Extracting EDGAR bundle...")
      # Extract to parent directory since tar contains EDGAR/ as root
      edgar_parent = self.edgar_dir.parent
      edgar_parent.mkdir(parents=True, exist_ok=True)
      with tarfile.open(edgar_bundle, "r:gz") as tar:
        tar.extractall(edgar_parent)
      logger.info("  EDGAR extracted")
    else:
      logger.warning("  No EDGAR bundle found")
      success = False

    return success

  def check_update_needed(self) -> bool:
    """Check if cache bundles need updating (>30 days old)."""
    logger.info("Checking if cache update is needed...")

    # Check schema bundle age
    schema_bundle = self.bundles_dir / "arelle-schemas-latest.tar.gz"
    if schema_bundle.exists():
      age = datetime.now() - datetime.fromtimestamp(schema_bundle.stat().st_mtime)
      if age > timedelta(days=30):
        logger.warning(f"  Schema bundle is {age.days} days old")
        return True
    else:
      logger.warning("  No schema bundle found")
      return True

    # Check EDGAR bundle age
    edgar_bundle = self.bundles_dir / "edgar-plugin-latest.tar.gz"
    if edgar_bundle.exists():
      age = datetime.now() - datetime.fromtimestamp(edgar_bundle.stat().st_mtime)
      if age > timedelta(days=30):
        logger.warning(f"  EDGAR bundle is {age.days} days old")
        return True
    else:
      logger.warning("  No EDGAR bundle found")
      return True

    logger.info("  Bundles are up to date")
    return False

  def clean(self):
    """Clean all cache directories."""
    logger.info("Cleaning cache directories...")

    if self.cache_dir.exists():
      shutil.rmtree(self.cache_dir)
      logger.info(f"  Removed: {self.cache_dir}")

    if self.edgar_dir.exists():
      shutil.rmtree(self.edgar_dir)
      logger.info(f"  Removed: {self.edgar_dir}")

    logger.info("Cleaned")

  def update(self):
    """Full update: download schemas, fetch EDGAR, create bundles."""
    logger.info("Updating all caches...")

    # Download schemas
    self.download_schemas()

    # Fetch EDGAR plugin
    self.fetch_edgar_plugin()

    # Create bundles
    self.create_bundles()

    logger.info("\nCache update complete!")
    logger.info("\nNext steps:")
    logger.info("  1. Commit the bundles: git add robosystems/arelle/bundles/*.tar.gz")
    logger.info("  2. Docker build will use these bundles automatically")


def main():
  """Main entry point."""
  parser = argparse.ArgumentParser(
    description="Arelle Cache Manager - Download, bundle, and manage Arelle schemas"
  )

  subparsers = parser.add_subparsers(dest="command", help="Commands")

  # Update command
  subparsers.add_parser(
    "update", help="Download schemas, fetch EDGAR, and create bundles"
  )

  # Download command
  subparsers.add_parser("download", help="Download XBRL schemas only")

  # Fetch-edgar command
  subparsers.add_parser("fetch-edgar", help="Fetch EDGAR plugin only")

  # Dev-init command
  subparsers.add_parser(
    "dev-init", help="Initialize EDGAR for dev environment if missing"
  )

  # Bundle command
  subparsers.add_parser("bundle", help="Create tar.gz bundles from existing cache")

  # Extract command
  subparsers.add_parser("extract", help="Extract bundles (used in Docker build)")

  # Check command
  subparsers.add_parser("check", help="Check if bundles need updating")

  # Clean command
  subparsers.add_parser("clean", help="Remove cache directories")

  parser.add_argument(
    "--project-root", help="Project root directory (auto-detected if not specified)"
  )

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    sys.exit(1)

  # Create manager
  manager = ArelleCacheManager(args.project_root)

  # Execute command
  if args.command == "update":
    manager.update()
  elif args.command == "download":
    manager.download_schemas()
  elif args.command == "fetch-edgar":
    manager.fetch_edgar_plugin()
  elif args.command == "dev-init":
    # Only run in dev environment
    import os

    if os.getenv("ENVIRONMENT") != "dev":
      sys.exit(0)

    # Check if EDGAR already exists
    if not manager.edgar_dir.exists():
      logger.info("EDGAR plugin not found in dev environment, fetching from GitHub...")
      if manager.fetch_edgar_plugin():
        logger.info("EDGAR plugin fetched successfully")
      else:
        logger.warning(
          "Warning: Failed to fetch EDGAR plugin, XBRL processing may fail"
        )
        sys.exit(1)
    else:
      logger.info("EDGAR plugin already exists")
  elif args.command == "bundle":
    manager.create_bundles()
  elif args.command == "extract":
    if not manager.extract_bundles():
      sys.exit(1)
  elif args.command == "check":
    if manager.check_update_needed():
      logger.info("\nRun: just cache-arelle-update")
      sys.exit(0)
    else:
      sys.exit(1)
  elif args.command == "clean":
    manager.clean()


if __name__ == "__main__":
  main()
