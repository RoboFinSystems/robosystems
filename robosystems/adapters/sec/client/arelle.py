"""The platform's Arelle load: xbrlkit's cache-first loader on the platform's
cache directory.

Everything that keeps a corpus run alive against the taxonomy hosts is
xbrlkit's — the DTS served from a persistent cache in Arelle's layout, fetches
spaced per host with a bounded ``Retry-After`` wait, no re-validation of a
cached file, and a loud ``DtsResolutionError`` when a document cannot be
resolved (``ref/adapters.md`` §2.8). What is the platform's is where the
cache lives: ``env.ARELLE_CACHE_DIR`` in a deployment, seeded from the schema
bundle at image build (``Dockerfile``: ``xbrlkit cache extract``); the repo's
``arelle/cache`` locally. A directory the process cannot write to falls back
to ``/tmp``, seeded from it, so a read-only mount still serves the bundle.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from xbrlkit.parse import close, load_model

from robosystems.adapters.sec.config import (
  ARELLE_TIMEOUT,
  ARELLE_WORK_OFFLINE,
  xbrlkit_config,
)
from robosystems.config import env
from robosystems.logger import get_logger

if TYPE_CHECKING:
  from arelle.ModelXbrl import ModelXbrl

logger = get_logger(__name__)

DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent / "arelle" / "cache"
FALLBACK_CACHE_DIR = Path("/tmp/arelle/cache")


def arelle_cache_dir() -> Path:
  """The cache directory the load uses.

  ``env.ARELLE_CACHE_DIR`` when set, else the repo's ``arelle/cache``. If that
  directory cannot be created or written, ``/tmp/arelle/cache`` is used
  instead and seeded from it once, so the baked bundle is still served.
  """
  chosen = Path(env.ARELLE_CACHE_DIR) if env.ARELLE_CACHE_DIR else DEFAULT_CACHE_DIR
  try:
    chosen.mkdir(parents=True, exist_ok=True)
  except OSError as exc:
    logger.warning(f"Cannot create Arelle cache at {chosen}: {exc}; using /tmp")
    return _fallback_from(chosen)
  if not os.access(chosen, os.W_OK):
    logger.warning(f"Arelle cache at {chosen} is not writable; using /tmp")
    return _fallback_from(chosen)
  return chosen


def _fallback_from(seed: Path) -> Path:
  FALLBACK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
  if seed.exists():
    copied = 0
    for source in seed.rglob("*"):
      if not source.is_file():
        continue
      target = FALLBACK_CACHE_DIR / source.relative_to(seed)
      if target.exists() and target.stat().st_size > 0:
        continue
      target.parent.mkdir(parents=True, exist_ok=True)
      try:
        shutil.copy2(source, target)
        copied += 1
      except OSError as exc:
        logger.debug(f"Could not seed {target}: {exc}")
    if copied:
      logger.info(f"Seeded {FALLBACK_CACHE_DIR} with {copied} files from {seed}")
  return FALLBACK_CACHE_DIR


def load_filing(path: str) -> ModelXbrl:
  """Load one filing (a local instance document, or an inline document set
  surrogate path) with the platform's cache and fetch settings.

  Raises ``xbrlkit.parse.DtsResolutionError`` when part of the DTS could not
  be resolved; the pipeline records the filing as failed and retries it later
  rather than indexing a filing missing the concepts a schema declared.
  """
  return load_model(
    path,
    cache_dir=arelle_cache_dir(),
    offline=ARELLE_WORK_OFFLINE,
    timeout=ARELLE_TIMEOUT,
    config=xbrlkit_config(),
  )


def close_filing(model_xbrl: ModelXbrl | None) -> None:
  """Release a loaded filing's model and its controller."""
  if model_xbrl is None:
    return
  try:
    model_xbrl.close()
  except Exception as exc:
    logger.warning(f"Error closing ModelXbrl: {exc}")
  close(getattr(getattr(model_xbrl, "modelManager", None), "cntlr", None))


__all__ = ["arelle_cache_dir", "close_filing", "load_filing"]
