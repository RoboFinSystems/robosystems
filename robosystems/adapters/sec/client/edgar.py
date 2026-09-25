"""EDGAR access: xbrlkit's ``EdgarClient`` with the platform's settings.

The client — ticker map, submissions header and its paged history, the
throttle policy for EDGAR's two throttle signatures — is xbrlkit's; this
module binds it to the platform's User-Agent and timeouts. ``SEC_BASE_URL``
is the Archives host the pipeline builds filing URLs on.
"""

from __future__ import annotations

import threading
import time

from xbrlkit.edgar import EdgarClient

from ..config import SEC_CONFIG, xbrlkit_config

SEC_BASE_URL = SEC_CONFIG["base_url"]
SEC_DATA_BASE_URL = SEC_CONFIG["data_base_url"]


class _SharedRateLimiter:
  """One process-wide spacing of EDGAR requests, safe across threads.

  Every client shares it: a limiter per client lets concurrent builds add
  their rates together past EDGAR's fair-access limit, which answers with a
  block of 403s.
  """

  def __init__(self, per_sec: float) -> None:
    self._interval = 1.0 / per_sec if per_sec > 0 else 0.0
    self._next = 0.0
    self._lock = threading.Lock()

  def wait(self) -> None:
    if self._interval <= 0:
      return
    with self._lock:
      now = time.monotonic()
      slot = max(now, self._next)
      self._next = slot + self._interval
    delay = slot - time.monotonic()
    if delay > 0:
      time.sleep(delay)


_limiter: _SharedRateLimiter | None = None
_limiter_lock = threading.Lock()


def _shared_limiter(per_sec: float) -> _SharedRateLimiter:
  global _limiter
  with _limiter_lock:
    if _limiter is None:
      _limiter = _SharedRateLimiter(per_sec)
    return _limiter


def edgar_client() -> EdgarClient:
  """An EDGAR client on the platform's configuration. Its own HTTP session,
  and the process-wide request limiter."""
  config = xbrlkit_config()
  client = EdgarClient(config)
  client._limiter = _shared_limiter(config.rate_limit_per_sec)  # type: ignore[assignment]
  return client


class IncompleteSubmissions(RuntimeError):
  """A filer's submissions history came back missing pages."""


def complete_submissions_strict(cik: str) -> dict:
  """Every page of a filer's submissions, or :class:`IncompleteSubmissions`.

  xbrlkit merges what it could fetch and skips a page that failed; stored as
  the filer's master, that short history is never repaired, because later
  runs only prepend the recent page.
  """
  client = edgar_client()
  header = client.submissions(cik)
  filings = header.get("filings") if isinstance(header, dict) else None
  pages = (filings or {}).get("files") if isinstance(filings, dict) else None
  declared = len([p for p in (pages or []) if isinstance(p, dict) and p.get("name")])
  complete = client.complete_submissions(cik)
  merged = int((complete.get("_metadata") or {}).get("paginationFilesMerged") or 0)
  if merged < declared:
    raise IncompleteSubmissions(
      f"CIK {cik}: {merged} of {declared} submissions pages fetched"
    )
  return complete


__all__ = [
  "SEC_BASE_URL",
  "SEC_DATA_BASE_URL",
  "IncompleteSubmissions",
  "complete_submissions_strict",
  "edgar_client",
]
