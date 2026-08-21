"""Database-fault translation for hand-written MCP tools.

The registrar-published tools already answer a missing tenant schema with
``not_initialized`` and any other database fault with a fixed message. Driver
output must never reach the LLM, so every hand-written tool routes its
``SQLAlchemyError`` arm through here and both tool families answer alike. The
domain catch-alls that follow keep their own messages, since those are domain
text rather than driver output.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from robosystems.db.extensions import is_statement_timeout
from robosystems.logger import logger
from robosystems.middleware.extensions import (
  STATEMENT_TIMEOUT_DETAIL,
  is_schema_missing,
)

LEDGER_NOT_INITIALIZED = "Ledger not initialized. Connect a data source first."


def statement_timeout_answer(tool_name: str) -> dict[str, Any]:
  """The MCP answer for a statement the session ceiling cancelled — the same
  fixed text REST returns as 504, so the LLM can narrow or retry."""
  logger.warning("MCP tool %s exceeded the statement ceiling", tool_name)
  return {"error": "statement_timeout", "message": STATEMENT_TIMEOUT_DETAIL}


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
  if is_statement_timeout(exc):
    return statement_timeout_answer(tool_name)
  logger.warning("MCP tool %s hit a database error", tool_name, exc_info=True)
  return {
    "error": "command_failed",
    "message": f"{tool_name} failed on a database error; see server logs",
  }


__all__ = ["LEDGER_NOT_INITIALIZED", "database_failure", "statement_timeout_answer"]
