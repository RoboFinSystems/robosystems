"""boto3 clients for synchronous calls that can outlast botocore's defaults.

botocore gives up reading after 60s and retries. On a call that is still
running, the retry repeats it: a second Bedrock generation billed, a second
snapshot, a second volume claim. Every Lambda and Bedrock client is built
here (`tests/test_long_call_clients.py` enforces it).
"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.config import Config


def long_call_client(service: str, read_timeout: float, **kwargs: Any) -> Any:
  """A client that waits `read_timeout` seconds for a reply and never retries.

  `read_timeout` must cover the callee's own limit. `kwargs` go to
  `boto3.client` (region, endpoint, credentials).
  """
  config = Config(
    connect_timeout=10,
    read_timeout=read_timeout,
    retries={"total_max_attempts": 1},
  )
  return boto3.client(service_name=service, config=config, **kwargs)
