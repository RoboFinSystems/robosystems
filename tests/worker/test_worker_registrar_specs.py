"""The worker process must see the registrar operations its operators call."""

import subprocess
import sys

import pytest

# Imported in a fresh interpreter: pytest's own imports load the API routers,
# which would register the specs and hide the defect.
_WORKER_SHAPED = """
import robosystems.worker.consumer  # noqa: F401
from robosystems.middleware.mcp.tools.registrar import build_tools_for_extension

for ext in ("roboledger", "roboinvestor"):
  tools = build_tools_for_extension(extension=ext, client=None)
  print(ext, len(tools), "create-mapping-association" in tools)
"""


@pytest.mark.unit
def test_worker_builds_registrar_tools_without_the_api():
  out = subprocess.run(
    [sys.executable, "-c", _WORKER_SHAPED],
    capture_output=True,
    text=True,
    timeout=120,
  )
  assert out.returncode == 0, out.stderr[-2000:]
  counts = {
    line.split()[0]: (int(line.split()[1]), line.split()[2] == "True")
    for line in out.stdout.splitlines()
    if line.startswith(("roboledger", "roboinvestor"))
  }
  assert counts["roboledger"][0] > 0
  assert counts["roboledger"][1], "the Mapping Operator's write tool is missing"
  assert counts["roboinvestor"][0] > 0
