"""Argument checks and fault answers the hand-written read tools share."""

from __future__ import annotations

from datetime import date
from typing import Any

from robosystems.logger import logger


def invalid(message: str) -> dict[str, Any]:
  return {"error": "invalid_arguments", "message": message}


def unexpected(tool_name: str) -> dict[str, Any]:
  """A fault that is not the caller's: the detail goes to the log, never to
  the client."""
  logger.warning("MCP tool %s failed", tool_name, exc_info=True)
  return {"error": "command_failed", "message": f"{tool_name} failed"}


def bounded_int(
  arguments: dict[str, Any], key: str, *, default: int, low: int, high: int
) -> int:
  """`arguments[key]`, held to the range its schema declares; ValueError
  names the argument and the range."""
  value = arguments.get(key)
  if value is None:
    return default
  try:
    if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
      raise ValueError
    number = int(value)
  except TypeError, ValueError:
    number = low - 1
  if not low <= number <= high:
    raise ValueError(f"{key} must be between {low} and {high}")
  return number


def optional_date(arguments: dict[str, Any], key: str) -> date | None:
  """`arguments[key]` as a date; ValueError names the argument."""
  value = arguments.get(key)
  if not value:
    return None
  try:
    return date.fromisoformat(value)
  except TypeError, ValueError:
    raise ValueError(f"{key} must be YYYY-MM-DD") from None


__all__ = ["bounded_int", "invalid", "optional_date", "unexpected"]
