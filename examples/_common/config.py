"""Shared config I/O for demo scripts.

Single source of truth for reading and writing the ``.local/config.json``
file the demos use to remember provisioned users + per-demo graph slots.
``examples/credentials/utils.py`` provisions the user; this module is
where every other demo reads/writes that state.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


def load_credentials(path: Path) -> dict[str, Any] | None:
  """Load credentials if they exist."""
  if path.exists():
    with path.open() as fh:
      return json.load(fh)
  return None


def save_credentials(path: Path, data: dict[str, Any]) -> None:
  """Persist credential data."""
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("w") as fh:
    json.dump(data, fh, indent=2)
  print(f"\n💾 Credentials saved to: {path}")


def get_user_id(path: Path) -> str | None:
  """Get user_id from credentials."""
  credentials = load_credentials(path)
  if not credentials:
    return None
  return credentials.get("user_id") or credentials.get("user", {}).get("id")


def get_graph_id(path: Path, demo_name: str) -> str | None:
  """Get graph_id for a specific demo from credentials."""
  credentials = load_credentials(path)
  if not credentials:
    return None
  graphs = credentials.get("graphs", {})
  demo_data = graphs.get(demo_name, {})
  return demo_data.get("graph_id")


def save_graph_id(
  path: Path, demo_name: str, graph_id: str, graph_created_at: str
) -> None:
  """Save graph_id for a specific demo to credentials."""
  credentials = load_credentials(path)
  if not credentials:
    raise ValueError(f"No credentials found at {path}")

  if "graphs" not in credentials:
    credentials["graphs"] = {}

  credentials["graphs"][demo_name] = {
    "graph_id": graph_id,
    "graph_created_at": graph_created_at,
  }

  save_credentials(path, credentials)


# ── High-level config helpers used by demo entry points ─────────────

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / ".local" / "config.json"


def require_config() -> dict[str, Any]:
  """Load ``.local/config.json`` or exit with a setup hint.

  The demos all share this contract: ``just demo-user`` provisions the
  file; every other script reads it. This is the canonical entry point
  — use ``load_credentials`` for the lower-level "may or may not exist"
  shape.
  """
  cfg = load_credentials(CONFIG_PATH)
  if cfg is None:
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  return cfg


def require_api_key(cfg: dict[str, Any]) -> str:
  """Pull the API key from a loaded config or exit with a setup hint."""
  api_key = cfg.get("api_key")
  if not api_key:
    raise SystemExit("No api_key in .local/config.json — run `just demo-user` first.")
  return api_key


def cached_graph_id(cfg: dict[str, Any], slot: str) -> str | None:
  """Return the cached ``graph_id`` for ``slot`` from a loaded config.

  Returns ``None`` if the slot is absent — callers decide whether that's
  an error or a "create a new graph" signal. Use ``get_graph_id`` if you
  want to read directly from a path.
  """
  return cfg.get("graphs", {}).get(slot, {}).get("graph_id")


def require_cached_graph_id(cfg: dict[str, Any], slot: str, demo_recipe: str) -> str:
  """Like ``cached_graph_id`` but exits with a setup hint if missing.

  ``demo_recipe`` is the just-recipe name to suggest in the error
  message (e.g. ``"just demo-roboledger"``).
  """
  graph_id = cached_graph_id(cfg, slot)
  if not graph_id:
    raise SystemExit(
      f"No {slot} graph cached in .local/config.json. "
      f"Pass a graph_id or run `{demo_recipe}` first."
    )
  return graph_id


def now_timestamp() -> str:
  """The standard ``graph_created_at`` format used in config.json."""
  return time.strftime("%Y-%m-%d %H:%M:%S")
