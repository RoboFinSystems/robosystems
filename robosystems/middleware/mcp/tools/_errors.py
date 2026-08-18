"""Database-fault translation for hand-written MCP tools.

The registrar-published tools already answer a missing tenant schema with
``not_initialized`` and any other database fault with a fixed message. The
hand-written tools each carried a catch-all that returned ``str(exc)`` — for a
DBAPI error that string carries the SQL and its bound parameters, which is not
something to put in front of the LLM. Every hand-written tool routes its
``SQLAlchemyError`` arm through here so both tool families answer alike; the
domain catch-alls that follow keep their messages, since those are domain
text, not driver output.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from robosystems.logger import logger
from robosystems.middleware.extensions import is_schema_missing

LEDGER_NOT_INITIALIZED = "Ledger not initialized. Connect a data source first."


def database_failure(
  tool_name: str,
  exc: SQLAlchemyError,
  *,
  not_initialized_message: str | None = LEDGER_NOT_INITIALIZED,
) -> dict[str, Any]:
  """The MCP answer for a database error raised inside a tool.

  ``not_initialized_message`` is the tenant-schema-missing answer; pass
  ``None`` for tools that read the platform database, where a missing
  relation is never "not initialized".
  """
  if (
    not_initialized_message is not None
    and isinstance(exc, ProgrammingError)
    and is_schema_missing(exc)
  ):
    return {"error": "not_initialized", "message": not_initialized_message}
  logger.warning("MCP tool %s hit a database error", tool_name, exc_info=True)
  return {
    "error": "command_failed",
    "message": f"{tool_name} failed on a database error; see server logs",
  }


__all__ = ["LEDGER_NOT_INITIALIZED", "database_failure"]
