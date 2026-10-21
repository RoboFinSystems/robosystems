"""DB tests that provision a named tenant schema each need their own name.

xdist runs test files on separate workers, so two files on one schema drop and
re-provision it under each other: the losing test finds its seeded rows gone
and fails at setup, only in parallel runs.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_TESTS = Path(__file__).parent
_NAMED_SCHEMA = re.compile(r"provision_tenant_schema\(|DROP SCHEMA IF EXISTS \{")
_GRAPH = re.compile(r'^GRAPH\s*=\s*"([^"]+)"', re.MULTILINE)


def test_no_two_files_provision_the_same_tenant_schema():
  owners: dict[str, list[str]] = defaultdict(list)
  for path in _TESTS.rglob("*_db.py"):
    source = path.read_text()
    if not _NAMED_SCHEMA.search(source):
      continue
    for graph in _GRAPH.findall(source):
      owners[graph].append(str(path.relative_to(_TESTS)))
  shared = {graph: files for graph, files in owners.items() if len(files) > 1}
  assert not shared, f"tenant schemas provisioned by more than one file: {shared}"
