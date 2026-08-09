"""Structural guard: every extensions write op that changes materialized
content must mark its graph stale.

The Dagster materialization sensor triggers purely on `Graph.graph_stale`, so
an operation that writes OLTP rows without setting `mark_stale_reason` leaves
those rows invisible to LadybugDB until something *else* marks the graph. The
failure is silent in the worst way: the write succeeds, the API returns 200,
and the graph simply answers as though the data were never there.

RoboInvestor showed the defect in its pure form — zero of six operations
declared the flag, so a graph whose only activity was RoboInvestor never
materialized at all. RoboLedger showed the same class in a form that hides
better: five operations wrote materialized tables without the flag, and got
away with it because a QuickBooks sync or a period close usually marks the
graph soon afterward. That makes delivery *incidental* rather than guaranteed,
which is the same shape as a shared report that only lands if the recipient
happens to have unrelated activity.

Asserted over the registrars' real specs rather than a fixed list, so an
operation added later is covered on the day it is written.
"""

from __future__ import annotations

import pytest

import robosystems.routers.extensions.roboinvestor.operations as _roboinvestor_ops
import robosystems.routers.extensions.roboledger.operations as _roboledger_ops
from robosystems.middleware.extensions import OperationRegistrar

# Importing the two operations modules is what registers their specs on the
# registrars; referenced here so neither import reads as unused and gets pruned.
_SPEC_SOURCES = (_roboinvestor_ops.__name__, _roboledger_ops.__name__)

# Operations that legitimately do not mark the graph stale, each with the
# reason it is exempt. Every entry was verified against
# `operations/extensions/materialize.py` — both the `TABLE_EXTENSIONS` map and
# the per-table SELECT that would have to read the written rows.
#
# Adding a name here is a decision on the record, not a way to quiet the test:
# state which table the operation writes and why the graph never reads it.
_EXEMPT: dict[str, str] = {
  "change-reporting-style": (
    "Writes entities.reporting_style_id. The Entity SELECT does not carry that "
    "column, so the Style changes render-time output rather than materialized "
    "rows; statement Structures themselves are library-seeded and immutable."
  ),
  "evaluate-rules": (
    "Writes verification_results, which has zero references in materialize.py "
    "— there is no corresponding graph node."
  ),
  "create-event-handler": (
    "Writes handler configuration; event_handlers has zero references in "
    "materialize.py."
  ),
  "update-event-handler": (
    "Writes handler configuration; event_handlers has zero references in "
    "materialize.py."
  ),
  "preview-event-block": (
    "Resolves handlers and returns the projected debits and credits. Writes "
    "nothing at all."
  ),
}

_EXTENSIONS = ("roboledger", "roboinvestor")


def _specs():
  pairs = []
  for extension in _EXTENSIONS:
    pairs.extend(
      (extension, spec)
      for _reg, spec in OperationRegistrar.specs_for_extension(extension)
    )
  return pairs


@pytest.mark.unit
class TestGraphStalenessCoverage:
  def test_specs_are_registered(self) -> None:
    """Guard the guard: if imports stop populating the registrars, every
    assertion below passes vacuously."""
    specs = _specs()
    assert len(specs) >= 30, (
      f"expected the full extensions operation surface, got {len(specs)} — "
      f"import wiring or a feature flag changed (sources: {_SPEC_SOURCES})"
    )

  def test_every_write_op_marks_the_graph_stale(self) -> None:
    missing = [
      f"{extension}:{spec.name}"
      for extension, spec in _specs()
      if spec.mark_stale_reason is None and spec.name not in _EXEMPT
    ]
    assert not missing, (
      "Operations that never mark the graph stale, so their writes never "
      f"reach LadybugDB: {missing}. Either add mark_stale_reason, or add the "
      "operation to _EXEMPT with the table it writes and why the graph "
      "never reads it."
    )

  def test_exemptions_still_name_real_operations(self) -> None:
    """A renamed or deleted operation must not leave a dead exemption behind:
    a stale entry silently exempts nothing while reading as considered."""
    registered = {spec.name for _extension, spec in _specs()}
    orphaned = sorted(set(_EXEMPT) - registered)
    assert not orphaned, (
      f"_EXEMPT names operations that are no longer registered: {orphaned}. "
      "Remove them, or correct the name if the operation was renamed."
    )

  def test_exemptions_have_not_been_overtaken(self) -> None:
    """If an exempt operation later gains the flag, the exemption is wrong and
    the reasoning recorded next to it is misleading."""
    overtaken = sorted(
      spec.name
      for _extension, spec in _specs()
      if spec.name in _EXEMPT and spec.mark_stale_reason is not None
    )
    assert not overtaken, (
      f"These operations now declare mark_stale_reason: {overtaken}. Delete "
      "their _EXEMPT entries — the recorded reason no longer holds."
    )
