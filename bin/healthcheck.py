#!/usr/bin/env python3
"""HTTP health probe for container health checks.

GET the URL and exit 0 on a 2xx response, 1 otherwise. Standard library only,
so the runtime image needs no curl (and therefore no libcurl or libssh2).

Usage: healthcheck.py URL [TIMEOUT_SECONDS]
"""

import sys
import urllib.error
import urllib.request


def main() -> int:
  if len(sys.argv) < 2:
    print("usage: healthcheck.py URL [TIMEOUT_SECONDS]", file=sys.stderr)
    return 2
  url = sys.argv[1]
  timeout = float(sys.argv[2]) if len(sys.argv) > 2 else 8.0
  try:
    with urllib.request.urlopen(url, timeout=timeout) as response:
      return 0 if 200 <= response.status < 300 else 1
  except (urllib.error.URLError, OSError, ValueError) as exc:
    print(f"healthcheck failed: {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
  sys.exit(main())
