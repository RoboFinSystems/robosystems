"""Background task worker entry point.

Usage: uv run python -m robosystems.worker
"""

import asyncio

from robosystems.worker.consumer import run

if __name__ == "__main__":
  asyncio.run(run())
