"""QuickBooks keeps one company's books per graph, the group parent's. These
are the sites that assume it, and so the ones several companies per graph
(one per entity) must change. The inventory is exact: a new site fails, and so
does a fixed one until it is struck off, so the list stays the work."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_ROOT = Path(__file__).resolve().parents[3]
_PACKAGE = _ROOT / "robosystems"

_SINGLE_REALM_SITES: dict[str, tuple[re.Pattern[str], Counter[str]]] = {
  # An event is unique on (source, external_id), and QuickBooks ids are
  # unique only within a company, so a second company's ids collide.
  "event identity without an entity": (
    re.compile(r"Event\.external_id\s*(==|\.in_\()"),
    Counter(
      {
        "robosystems/adapters/bank_feed/load.py": 1,
        "robosystems/adapters/quickbooks/pipeline/cdc.py": 1,
        "robosystems/operations/event_block/commands.py": 1,
        "robosystems/operations/event_block/python_handlers/payment_received.py": 1,
        "robosystems/operations/extensions/loader.py": 3,
      }
    ),
  ),
  # The graph's QuickBooks connection, as though it had one.
  "a graph-wide QuickBooks connection pick": (
    re.compile(r"Connection\.provider\s*==\s*\"quickbooks\""),
    Counter(
      {
        "robosystems/operations/roboledger/fiscal_calendar/qb_writeback.py": 2,
        "robosystems/operations/roboledger/reads/fiscal_calendar.py": 1,
      }
    ),
  ),
  # One pipeline working directory per graph: two companies syncing at once
  # would write over each other's extract.
  "a per-graph pipeline work directory": (
    re.compile(r"get_pipeline_work_dir\(\s*config\.graph_id\s*\)"),
    Counter(
      {
        "robosystems/adapters/quickbooks/pipeline/extract.py": 1,
        "robosystems/adapters/quickbooks/pipeline/load.py": 4,
        "robosystems/adapters/quickbooks/pipeline/transform.py": 1,
      }
    ),
  ),
}


@pytest.mark.parametrize("kind", sorted(_SINGLE_REALM_SITES))
def test_single_realm_sites_are_the_known_ones(kind: str):
  pattern, known = _SINGLE_REALM_SITES[kind]
  found: Counter[str] = Counter()
  for path in _PACKAGE.rglob("*.py"):
    hits = len(pattern.findall(path.read_text()))
    if hits:
      found[str(path.relative_to(_ROOT))] = hits
  added = found - known
  removed = known - found
  assert not added, (
    f"New {kind}: {dict(added)}. QuickBooks will keep one company per entity; "
    "scope the new code by entity or connection instead."
  )
  assert not removed, (
    f"Fewer of {kind} than recorded: {dict(removed)}. Strike them off here."
  )
