"""Shared SDK plumbing for demo scripts.

One place that constructs the ``LedgerClient`` from a loaded config, and
the few read patterns that recur across demos. The transport contract
for the demos is "everything customer-facing goes through this client";
reset / validate / reconcile may hit the DB directly, but anything
emulating a customer surface (download bundles, list reports, fetch
graphs) flows through here.
"""

from __future__ import annotations

from typing import Any

from robosystems_client.clients import LedgerClient

from examples._common.config import require_api_key


def make_ledger_client(
  cfg: dict[str, Any],
  *,
  timeout: int = 60,
  headers: dict[str, str] | None = None,
) -> LedgerClient:
  """Build a ``LedgerClient`` from a loaded ``.local/config.json``.

  ``cfg`` is the dict returned by ``_common.config.require_config()``.
  Exits with a setup hint if no API key is present.
  """
  base_url = cfg.get("base_url", "http://localhost:8000")
  api_key = require_api_key(cfg)
  return LedgerClient(
    {
      "base_url": base_url,
      "token": api_key,
      "headers": headers or {},
      "timeout": timeout,
    }
  )


def latest_report_id(client: LedgerClient, graph_id: str) -> str:
  """Return the most-recent Report id for ``graph_id`` via the SDK.

  Calls ``LedgerClient.list_reports`` — same surface a frontend or MCP
  agent would consume. In a demo run there is only ever one Report; if
  it has no stamped bundle, the download endpoint will 404 (loudly).
  """
  reports = client.list_reports(graph_id) or []
  if not reports:
    raise SystemExit(f"No Report found for graph {graph_id}.")
  reports.sort(key=lambda r: r.get("created_at") or "", reverse=True)
  return str(reports[0]["id"])
