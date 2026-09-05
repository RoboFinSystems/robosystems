"""EDGAR access: xbrlkit's ``EdgarClient`` with the platform's settings.

The client — ticker map, submissions header and its paged history, the
throttle policy for EDGAR's two throttle signatures — is xbrlkit's; this
module binds it to the platform's User-Agent and timeouts. ``SEC_BASE_URL``
is the Archives host the pipeline builds filing URLs on.
"""

from __future__ import annotations

from xbrlkit.edgar import EdgarClient

from ..config import SEC_CONFIG, xbrlkit_config

SEC_BASE_URL = SEC_CONFIG["base_url"]
SEC_DATA_BASE_URL = SEC_CONFIG["data_base_url"]


def edgar_client() -> EdgarClient:
  """A fresh EDGAR client on the platform's configuration."""
  return EdgarClient(xbrlkit_config())


__all__ = ["SEC_BASE_URL", "SEC_DATA_BASE_URL", "edgar_client"]
