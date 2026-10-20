"""The aged receivables and payables read; there is no write behind it."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.reads.ar_ap import (
  get_aged_balances,
  list_open_items,
)

from ._errors import database_failure
from .constants import ENTITY_ID_ARGUMENT


class AgedBalancesTool:
  """Receivables or payables aged at a date, or one counterparty's open items."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "aged-balances",
      "description": """Receivables or payables aged at a date: who owes the company and how late it is, or whom it owes and when.

**WHEN TO USE:**
- "Who owes us money?", "what's overdue?", "whom do we owe, and when is it due?"
- Month-end review: the aging at the month-end date, and each entity's open items tied out to its receivables or payables account
- One counterparty's open invoices or bills: pass `agent_id`

**PARAMETERS:**
- kind (required): `receivables` or `payables`
- as_of (optional, YYYY-MM-DD): the date to age at; omit for today. A payment dated after it has not happened yet
- entity_id (optional): omit for the group parent
- scope (optional, default `entity`): `group` sums the parent and the members consolidated in full, and sets apart what they owe each other
- agent_id (optional): that counterparty's open items instead of the summary

**RETURNS:**
Amounts are in cents.
- Without `agent_id`: `buckets` (current, 1–30, 31–60, 61–90 and over 90 days past due, and unapplied credit from overpayments), `agents` (a row per counterparty, largest first), `defaulted_due_date_count` (items with no due date of their own, aged as due on receipt), and `tie_outs`: each entity's open items against its control accounts, with `difference_cents` named and never plugged. For a group, `intercompany` holds the items the members owe each other, and `pairs` sets both sides of each pair side by side.
- With `agent_id`: `items`, oldest first, each with its due date, `due_date_basis`, days past due and bucket.

**RELATED TOOLS:**
- agent-activity — a counterparty's events and transactions
- update-event-block — correct an invoice's or bill's `due_date`""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "kind": {"type": "string", "enum": ["receivables", "payables"]},
          "as_of": {
            "type": "string",
            "description": "The date to age at, YYYY-MM-DD. Omit for today.",
          },
          "entity_id": ENTITY_ID_ARGUMENT,
          "scope": {
            "type": "string",
            "enum": ["entity", "group"],
            "default": "entity",
          },
          "agent_id": {
            "type": "string",
            "description": "One counterparty (agt_ prefixed).",
          },
        },
        "required": ["kind"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    kind = arguments.get("kind")
    scope = arguments.get("scope") or "entity"
    agent_id = arguments.get("agent_id")
    entity_id = arguments.get("entity_id")
    if kind not in ("receivables", "payables"):
      return {
        "error": "invalid_arguments",
        "message": "kind must be 'receivables' or 'payables'",
      }
    if scope not in ("entity", "group"):
      return {
        "error": "invalid_arguments",
        "message": "scope must be 'entity' or 'group'",
      }
    if agent_id and scope == "group":
      return {
        "error": "invalid_arguments",
        "message": "agent_id reads one entity's items; name its entity_id instead",
      }
    try:
      as_of = (
        date.fromisoformat(arguments["as_of"])
        if arguments.get("as_of")
        else date.today()
      )
    except ValueError:
      return {"error": "invalid_arguments", "message": "as_of must be YYYY-MM-DD"}

    try:
      with extensions_session(self.client.graph_id) as session:
        if agent_id:
          items = list_open_items(
            session, kind, as_of=as_of, entity_id=entity_id, agent_id=agent_id
          )
          return {
            "kind": kind,
            "as_of": as_of.isoformat(),
            "agent_id": agent_id,
            "item_count": len(items),
            "items": [item.model_dump(mode="json") for item in items],
          }
        return get_aged_balances(
          session, kind, as_of=as_of, entity_id=entity_id, scope=scope
        ).model_dump(mode="json")
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("aged-balances", exc)
    except Exception as exc:
      logger.warning(f"aged-balances failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


__all__ = ["AgedBalancesTool"]
