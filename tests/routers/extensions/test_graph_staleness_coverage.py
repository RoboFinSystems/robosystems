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

**And over the hand-written routes too.** Enumerating only `OperationSpec`s
covers 29 of roboledger's 48 operations; the other 19 are hand-written
`@router.post` handlers the registrar never sees, which is how `update-entity`
kept writing materialized Entity columns without ever marking its graph while
this file read as though it held the whole surface. (`update-entity` is a
registrar spec as of 2026-08-20, along with `create-report` and
`regenerate-report`; the hand-written half shrank by three accordingly.) Every hand-written route
must be classified below — marking stale in its handler, marking stale in the
command or tool it delegates to, or exempt with the reason — so a new one
cannot land undeclared.
"""

from __future__ import annotations

import importlib
import inspect

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

_OPERATION_ROUTERS = {
  "roboledger": _roboledger_ops,
  "roboinvestor": _roboinvestor_ops,
}

# ── Hand-written routes ────────────────────────────────────────────────────
# Three ways a hand-written operation can be correct, and every one of them
# has to be stated. `_EXEMPT` above covers registrar specs only.

# The handler itself calls `mark_graph_stale` (directly or via the
# `on_fresh_success` hook it passes to `_dispatch`).
# `create-report`, `regenerate-report` and `update-entity` used to live here.
# They became registrar specs, so `mark_stale_reason` on the spec is what
# covers them now and `test_every_write_op_marks_the_graph_stale` above is the
# assertion that holds them to it.
_STALE_IN_HANDLER = {
  "bind-text-block",
  "block-source-graph",
  "delete-report",
  "revoke-report-share",
  "share-report",
}

# The handler delegates, and the mark lives in the callee. Recorded as a
# dotted path so the assertion reads the callee's real source rather than
# trusting the label — `close_period` marking its own graph is what lets
# `backfill-plan-history` mark nothing of its own.
_STALE_IN_CALLEE = {
  "close-period": "robosystems.operations.roboledger.commands.fiscal_calendar.close_period",
  "reopen-period": "robosystems.operations.roboledger.commands.fiscal_calendar.reopen_period",
  # Backfill drives reopen/close per period; both mark.
  "backfill-plan-history": "robosystems.operations.roboledger.commands.fiscal_calendar.close_period",
  # Async: enqueues the mapping operator, whose writes all land through this
  # direct MCP tool. The registrar-published tools inherit the mark from
  # their `OperationSpec`; this one is hand-written and marks for itself.
  "auto-map-elements": "robosystems.middleware.mcp.tools.taxonomy_tools.CreateMappingAssociationTool._execute_sync",
}

# Same bar as `_EXEMPT`: name the table written and why the graph never
# reads it. Verified against the `postgres_scan` sources in
# `operations/extensions/materialize.py`.
_HAND_WRITTEN_EXEMPT: dict[str, str] = {
  "initialize": (
    "Writes fiscal_calendars + fiscal_periods. Neither table is scanned by "
    "materialize.py — the fiscal calendar has no graph node."
  ),
  "set-close-target": (
    "Writes fiscal_calendars.close_target — a scheduling intent, not ledger "
    "content, and the table is not scanned by materialize.py."
  ),
  "file-report": (
    "Writes reports.filing_status. The Report SELECT does not carry that "
    "column and filters on generation_status, so filing a report changes no "
    "materialized value."
  ),
  "transition-filing-status": (
    "Same column and same reasoning as file-report: filing_status is not materialized."
  ),
  "create-publish-list": "publish_lists is not scanned by materialize.py.",
  "update-publish-list": "publish_lists is not scanned by materialize.py.",
  "delete-publish-list": "publish_lists is not scanned by materialize.py.",
  "add-publish-list-members": "publish_lists is not scanned by materialize.py.",
  "remove-publish-list-member": "publish_lists is not scanned by materialize.py.",
  "unblock-source-graph": (
    "Writes blocked_source_graphs, which is not scanned by materialize.py. "
    "Unlike block, it never purges — nothing leaves the graph, so there is "
    "nothing to re-project."
  ),
}


def _hand_written():
  """Route name → (extension, endpoint) for every non-registrar operation."""
  out = {}
  for extension, module in _OPERATION_ROUTERS.items():
    spec_names = {
      spec.name for _reg, spec in OperationRegistrar.specs_for_extension(extension)
    }
    for route in module.router.routes:
      name = getattr(route, "path", "").lstrip("/")
      if name and name not in spec_names:
        out[name] = (extension, route.endpoint)
  return out


def _resolve(dotted: str):
  """Import a `module.attr` or `module.Class.method` path."""
  parts = dotted.split(".")
  for split in range(len(parts) - 1, 0, -1):
    try:
      obj = importlib.import_module(".".join(parts[:split]))
    except ImportError:
      continue
    for attr in parts[split:]:
      obj = getattr(obj, attr)
    return obj
  raise ImportError(f"Could not resolve {dotted}")


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

  def test_every_hand_written_route_is_classified(self) -> None:
    """The gap this file used to have: a hand-written `@router.post` is
    invisible to the spec enumeration above, so it could write materialized
    tables and mark nothing while the suite stayed green."""
    hand_written = _hand_written()
    # Floor sits just under the real count (19 after `create-report`,
    # `regenerate-report` and `update-entity` moved to the registrar) so a
    # scan that silently stops matching fails instead of passing vacuously.
    assert len(hand_written) >= 17, (
      f"expected the hand-written operation surface, got {len(hand_written)} "
      "— route wiring or a feature flag changed"
    )
    classified = _STALE_IN_HANDLER | set(_STALE_IN_CALLEE) | set(_HAND_WRITTEN_EXEMPT)
    unclassified = sorted(set(hand_written) - classified)
    assert not unclassified, (
      f"Hand-written operations with no staleness decision on record: "
      f"{unclassified}. Add each to _STALE_IN_HANDLER, _STALE_IN_CALLEE, or "
      "_HAND_WRITTEN_EXEMPT with the table it writes and why."
    )
    stale_names = sorted(classified - set(hand_written))
    assert not stale_names, (
      f"These names are classified but no longer routed: {stale_names}. "
      "Remove them, or correct the name if the operation was renamed."
    )

  def test_handlers_that_claim_to_mark_stale_actually_do(self) -> None:
    """Reading the handler's source keeps the table above honest: deleting a
    `mark_graph_stale` call fails here instead of silently downgrading the
    operation to the exempt behavior."""
    hand_written = _hand_written()
    silent = sorted(
      name
      for name in _STALE_IN_HANDLER
      if name in hand_written
      and "mark_graph_stale" not in inspect.getsource(hand_written[name][1])
    )
    assert not silent, (
      f"Handlers listed in _STALE_IN_HANDLER that no longer call "
      f"mark_graph_stale: {silent}."
    )

  def test_callees_that_claim_to_mark_stale_actually_do(self) -> None:
    """Same guard one level down, for handlers that delegate the mark."""
    missing = sorted(
      f"{name} -> {dotted}"
      for name, dotted in _STALE_IN_CALLEE.items()
      if "mark_graph_stale" not in inspect.getsource(_resolve(dotted))
    )
    assert not missing, (
      f"Delegated staleness marks that are no longer in the callee: {missing}."
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
