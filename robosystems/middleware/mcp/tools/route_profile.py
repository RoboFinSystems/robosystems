"""What an MCP route serves of a graph's tools.

The universal and per-graph routes serve every tool as it is. The
`/v1/mcp/roboledger` route backs directory listings, which review one frozen
tool list per URL and need every operation a model can call visible up
front: it withholds `ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS`, and its event-block
tools run only the built-in event kinds, each posted by the platform's own
handler and named in the tool's schema. An event type of a tenant's own,
posted by a rule it authored, is never reached from it.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from robosystems.logger import logger

from .manager import ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS

# One line per built-in kind; `test_route_profile` holds it to the handler
# registry, so a new platform handler cannot ship undescribed.
BUILT_IN_EVENT_TYPES: dict[str, str] = {
  "journal_entry_recorded": (
    "a balanced journal entry (`metadata.line_items`), drafted for the close "
    "to post, or posted with `metadata.status='posted'`"
  ),
  "journal_entry_reversed": "reverses a posted journal entry with a reversing entry",
  "schedule_entry_due": (
    "drafts or refreshes a schedule's closing entry for one period"
  ),
  "schedule_created": (
    "capture-only: the record a schedule's obligations point at, which "
    "creating a schedule writes itself"
  ),
  "asset_disposed": (
    "ends a schedule at a disposal, retiring its later months, and posts the "
    "disposal entry"
  ),
  "invoice_issued": "an invoice to a customer, posted as its journal entry",
  "bill_received": "a bill from a vendor, posted as its journal entry",
  "payment_received": (
    "a customer payment, posted as its journal entry and linked to the "
    "invoice it settles"
  ),
  "bill_paid": (
    "a payment of a bill, posted as its journal entry and linked to the bill it settles"
  ),
  "sales_receipt_recorded": "a sale paid at once, posted as its journal entry",
  "cash_expense_recorded": "an expense paid at once, posted as its journal entry",
  "check_written": "a check written, posted as its journal entry",
  "credit_card_charge": "a credit card charge, posted as its journal entry",
  "credit_card_refund": "a credit card refund, posted as its journal entry",
  "deposit_received": "a bank deposit, posted as its journal entry",
  "inventory_adjusted": "an inventory adjustment, posted as its journal entry",
  "bank_transaction": (
    "a bank-feed line; it posts against its bank account once classified to an account"
  ),
  "bank_fee": "a bank fee from a feed; it posts once classified",
  "external_transfer": (
    "money moved between a group bank account and one outside the group; it "
    "posts once classified"
  ),
  "internal_transfer": (
    "money moved between two of the group's own bank accounts; both legs are "
    "known, so it posts as it is"
  ),
}

# Tools whose `event_type` argument picks the handler that runs.
_EVENT_TYPE_ARGUMENT_TOOLS = frozenset({"create-event-block", "preview-event-block"})
# A tool that runs the handler of an event already recorded.
_STORED_EVENT_TYPE_TOOLS = frozenset({"update-event-block"})


def _event_type_schema() -> dict[str, Any]:
  kinds = "\n".join(
    f"- `{kind}`: {meaning}" for kind, meaning in BUILT_IN_EVENT_TYPES.items()
  )
  return {
    "type": "string",
    "enum": list(BUILT_IN_EVENT_TYPES),
    "description": (
      "The kind of event. One of the built-in kinds, each posted by the "
      f"platform's own handler:\n{kinds}"
    ),
  }


def _refused_event_type(event_type: object) -> str:
  return (
    f"event_type {event_type!r} is not available on this connection: it runs "
    "only the built-in event kinds listed in the tool's event_type schema, "
    "each through the platform's own handler. An event of another kind is "
    "recorded in the RoboLedger app."
  )


def _stored_event_type(graph_id: str, event_id: str) -> str | None:
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.roboledger.reads.event_block import get_event_type

  with extensions_session(graph_id) as session:
    return get_event_type(session, event_id)


@dataclass(frozen=True)
class RouteProfile:
  """A route's cut of the tool list: names it withholds, and whether its
  event-block tools are held to the built-in event kinds."""

  excluded_tools: frozenset[str] = frozenset()
  built_in_event_types_only: bool = False

  def serve(
    self, tools: list[dict[str, Any]], also_excluded: frozenset[str] = frozenset()
  ) -> list[dict[str, Any]]:
    """The tool definitions this route lists, filtered before instructions
    are built so they never name a withheld tool."""
    excluded = self.excluded_tools | also_excluded
    served = [t for t in tools if t.get("name") not in excluded]
    if self.built_in_event_types_only:
      served = [_held_to_built_in_kinds(t) for t in served]
    return served

  def withholds(self, name: str) -> bool:
    return name in self.excluded_tools

  def screens(self, name: str) -> bool:
    """Whether a call to ``name`` has arguments ``refusal`` must check."""
    return self.built_in_event_types_only and (
      name in _EVENT_TYPE_ARGUMENT_TOOLS or name in _STORED_EVENT_TYPE_TOOLS
    )

  def refusal(self, graph_id: str, name: str, arguments: dict[str, Any]) -> str | None:
    """Why this route refuses a call to a tool it serves, or None: a client
    can send an argument the schema never offered. Run after authorization,
    since it may read the event the call names."""
    if not self.built_in_event_types_only:
      return None
    if name in _EVENT_TYPE_ARGUMENT_TOOLS:
      event_type = arguments.get("event_type")
      if event_type not in BUILT_IN_EVENT_TYPES:
        return _refused_event_type(event_type)
    elif name in _STORED_EVENT_TYPE_TOOLS:
      event_id = arguments.get("event_id")
      if isinstance(event_id, str) and event_id:
        try:
          event_type = _stored_event_type(graph_id, event_id)
        except Exception:
          logger.warning(
            "Could not read the event type of %s on %s",
            event_id,
            graph_id,
            exc_info=True,
          )
          return "The event's kind could not be checked; try again."
        # An unknown event is the operation's own not-found to answer.
        if event_type is not None and event_type not in BUILT_IN_EVENT_TYPES:
          return _refused_event_type(event_type)
    return None


def _held_to_built_in_kinds(tool: dict[str, Any]) -> dict[str, Any]:
  name = tool.get("name")
  if name in _EVENT_TYPE_ARGUMENT_TOOLS:
    held = copy.deepcopy(tool)
    held["inputSchema"].setdefault("properties", {})["event_type"] = (
      _event_type_schema()
    )
    held["description"] = (
      f"{held.get('description', '')}\n\nOn this connection `event_type` is one "
      "of the built-in kinds its schema lists, each posted by the platform's "
      "own handler."
    )
    return held
  if name in _STORED_EVENT_TYPE_TOOLS:
    held = dict(tool)
    held["description"] = (
      f"{held.get('description', '')}\n\nOn this connection only an event of a "
      "built-in kind (the `event_type` list on create-event-block) can be "
      "updated."
    )
    return held
  return tool


UNIVERSAL_ROUTE = RouteProfile()
ROBOLEDGER_ROUTE = RouteProfile(
  excluded_tools=ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS, built_in_event_types_only=True
)
