"""MCP tool annotations: the four behavior hints, explicit on every tool.

Directory reviews read them as claims about the tool and check each against
what it does, so they are declared per tool rather than derived from the
read/write split in ``classification`` (which decides authorization, and is
deliberately coarser: a preview is a "write" there so viewers can't call it).

The hints follow the MCP spec:

- ``readOnlyHint`` — changes nothing it can be asked to change.
- ``destructiveHint`` — may overwrite or remove existing data; ``False``
  means its writes are only additive.
- ``idempotentHint`` — repeating a call with the same arguments has no
  further effect.
- ``openWorldHint`` — reaches a system outside RoboSystems (QuickBooks, a
  bank feed).
"""

from __future__ import annotations

from typing import Any, NamedTuple

from .classification import CYPHER_READ_TOOLS, READ_ONLY_MCP_TOOLS


class ToolHints(NamedTuple):
  read_only: bool
  destructive: bool
  idempotent: bool
  open_world: bool


_READ = ToolHints(read_only=True, destructive=False, idempotent=True, open_world=False)

# An unlisted write gets the most cautious claim on every hint.
_UNKNOWN_WRITE = ToolHints(
  read_only=False, destructive=True, idempotent=False, open_world=True
)


def _write(
  *, destructive: bool, idempotent: bool, open_world: bool = False
) -> ToolHints:
  return ToolHints(
    read_only=False,
    destructive=destructive,
    idempotent=idempotent,
    open_world=open_world,
  )


_ADDS = _write(destructive=False, idempotent=False)
_ADDS_ONCE = _write(destructive=False, idempotent=True)
_REPLACES = _write(destructive=True, idempotent=True)
_REPLACES_ONCE_PER_STATE = _write(destructive=True, idempotent=False)

# Every tool outside READ_ONLY_MCP_TOOLS / CYPHER_READ_TOOLS. A new write
# tool belongs here; `test_annotations` fails until it is.
WRITE_TOOL_HINTS: dict[str, ToolHints] = {
  # Period close. close-period publishes opted-in drafts to QuickBooks.
  "close-period": _write(destructive=True, idempotent=False, open_world=True),
  "reopen-period": _REPLACES_ONCE_PER_STATE,
  # Pulls from the user's QuickBooks / bank connection; full_rebuild resets
  # captured state.
  "sync-connection": _write(destructive=True, idempotent=False, open_world=True),
  # Ledger events and entries. apply_handlers can write posted entries
  # (imports, reversals) that only a reversing entry undoes.
  "create-event-block": _write(destructive=True, idempotent=False),
  "update-event-block": _REPLACES_ONCE_PER_STATE,
  # Creates the entry in QuickBooks; the event id is the request id, so a
  # repeat creates nothing.
  "execute-event-block": _write(destructive=True, idempotent=True, open_world=True),
  # Reads the trial balance from QuickBooks, so it stays behind the write
  # role: a viewer does not get to spend the tenant's QuickBooks calls.
  "preview-reconciliations": _READ._replace(open_world=True),
  # Replaces the period's recorded comparison; reads QuickBooks to make it.
  "refresh-reconciliations": _write(destructive=True, idempotent=True, open_world=True),
  "set-reconciliation-policy": _REPLACES,
  # Adds a review record; signing a period already reviewed adds nothing.
  "sign-off-reconciliation": _ADDS_ONCE,
  # Replaces the balance recorded for that account and date.
  "record-statement-balance": _REPLACES,
  "resolve-reconciling-item": _REPLACES_ONCE_PER_STATE,
  "update-journal-entry": _REPLACES,
  "delete-journal-entry": _REPLACES,
  "create-event-handler": _ADDS,
  "update-event-handler": _REPLACES,
  # Schedules.
  "promote-obligations": _ADDS_ONCE,
  "terminate-schedule": _REPLACES,
  "rebuild-schedule": _REPLACES,
  "backfill-plan-history": _ADDS_ONCE,
  # Counterparties and the reporting entity.
  "create-agent": _ADDS,
  "update-agent": _REPLACES,
  "update-entity": _REPLACES,
  # Taxonomy and mapping.
  "initialize-chart-of-accounts": _ADDS_ONCE,
  "create-taxonomy-block": _ADDS,
  "update-taxonomy-block": _REPLACES_ONCE_PER_STATE,
  "delete-taxonomy-block": _REPLACES,
  "link-entity-taxonomy": _REPLACES,
  "change-reporting-style": _REPLACES,
  "create-mapping-association": _ADDS_ONCE,
  "delete-mapping-association": _REPLACES,
  # Information blocks, metrics, forecasts.
  "create-information-block": _ADDS,
  "update-information-block": _REPLACES,
  "delete-information-block": _REPLACES,
  "evaluate-rules": _ADDS,
  "compute-metrics": _REPLACES,
  "assert-metrics": _REPLACES,
  "compute-forecast": _REPLACES,
  # Reports and disclosure text.
  "create-report": _ADDS,
  "regenerate-report": _REPLACES,
  "delete-report": _REPLACES,
  "bind-text-block": _REPLACES,
  # Documents and memory.
  "create-document": _ADDS,
  "update-document": _REPLACES,
  "delete-document": _REPLACES,
  "remember": _ADDS,
  "update-memory": _REPLACES,
  "forget": _REPLACES,
  # Graph administration.
  "set-write-policy": _REPLACES,
  "materialize": _REPLACES,
  "create-backup": _ADDS,
  "create-subgraph": _ADDS,
  "delete-subgraph": _REPLACES,
  "add-node-table": _ADDS_ONCE,
  "add-relationship-table": _ADDS_ONCE,
  "write-graph-cypher": _REPLACES_ONCE_PER_STATE,
  # RoboInvestor.
  "create-security": _ADDS,
  "update-security": _REPLACES,
  "delete-security": _REPLACES,
  "create-portfolio-block": _ADDS,
  "update-portfolio-block": _REPLACES,
  "delete-portfolio-block": _REPLACES,
}


def tool_hints(name: str) -> ToolHints:
  if name in READ_ONLY_MCP_TOOLS or name in CYPHER_READ_TOOLS:
    return _READ
  return WRITE_TOOL_HINTS.get(name, _UNKNOWN_WRITE)


def tool_annotations(name: str, title: str) -> dict[str, Any]:
  hints = tool_hints(name)
  return {
    "title": title,
    "readOnlyHint": hints.read_only,
    "destructiveHint": hints.destructive,
    "idempotentHint": hints.idempotent,
    "openWorldHint": hints.open_world,
  }
