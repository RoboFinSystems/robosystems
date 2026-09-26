"""EDGAR access stays inside the fair-access rate and never stores a short history."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest
import requests

pytestmark = pytest.mark.unit


def test_concurrent_clients_share_one_request_rate():
  """Five builds in parallel must not add their rates together."""
  from robosystems.adapters.sec.client.edgar import edgar_client

  stamps: list[float] = []
  lock = threading.Lock()

  def worker():
    client = edgar_client()
    # Ten requests: unshared limiters would land them all within ~0.2s.
    for _ in range(2):
      client._limiter.wait()
      with lock:
        stamps.append(time.monotonic())

  threads = [threading.Thread(target=worker) for _ in range(5)]
  for t in threads:
    t.start()
  for t in threads:
    t.join()

  per_sec = edgar_client().config.rate_limit_per_sec
  stamps.sort()
  busiest = max(sum(1 for s in stamps if start <= s < start + 1.0) for start in stamps)
  assert busiest <= per_sec + 1


def test_a_submissions_page_that_fails_fails_the_filer():
  """xbrlkit merges the pages it could fetch; a short master is never repaired."""
  from xbrlkit.edgar import EdgarClient

  from robosystems.adapters.sec.client.edgar import (
    IncompleteSubmissions,
    complete_submissions_strict,
  )

  header = {
    "name": "Example Corp",
    "filings": {
      "recent": {"accessionNumber": ["a-new"], "form": ["10-K"]},
      "files": [{"name": "p1.json"}, {"name": "p2.json"}, {"name": "p3.json"}],
    },
  }

  def fetch(self, name):
    if name.startswith("CIK"):
      return header
    if name == "p2.json":
      raise requests.HTTPError("403 blocked")
    return {"accessionNumber": [f"a-{name}"], "form": ["10-K"]}

  with patch.object(EdgarClient, "_get_submissions", fetch):
    with pytest.raises(IncompleteSubmissions):
      complete_submissions_strict("1045810")


def test_a_master_stored_short_is_detected_against_the_live_header():
  from robosystems.adapters.sec.pipeline.download import _stored_master_is_short

  header = {"filings": {"files": [{"name": "p1.json"}, {"name": "p2.json"}]}}
  assert _stored_master_is_short({"_metadata": {"paginationFilesMerged": 1}}, header)
  assert _stored_master_is_short({}, header)
  assert not _stored_master_is_short(
    {"_metadata": {"paginationFilesMerged": 2}}, header
  )
  assert not _stored_master_is_short({}, {"filings": {"recent": {}}})
