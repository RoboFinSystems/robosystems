"""
Input validation shared across Graph API routers and services.
"""

import re
from typing import Any

from fastapi import HTTPException, status


def validate_database_name(graph_id: str) -> str:
  """Validate a graph ID, returning it unchanged.

  Graph IDs become filesystem paths, so the name is restricted to
  alphanumerics, underscores and dashes, at most 64 characters. Raises
  ``HTTPException`` on anything else.
  """
  if not graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Graph ID cannot be empty"
    )

  if ".." in graph_id or "/" in graph_id or "\\" in graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Graph ID contains invalid characters",
    )

  if not re.match(r"^[a-zA-Z0-9_-]+$", graph_id):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Graph ID must be alphanumeric with underscores/dashes only",
    )

  if len(graph_id) > 64:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Graph ID too long (max 64 characters)",
    )

  return graph_id


def validate_query_parameters(params: dict[str, Any] | None) -> None:
  """Validate query parameters against name, size and type limits.

  The limits bound how much work a single request can force the engine to do;
  nested dictionaries are validated recursively. Raises ``HTTPException`` on
  a violation.
  """
  if not params:
    return

  if len(params) > 50:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Too many parameters (max 50)"
    )

  for key, value in params.items():
    # Dots are allowed so recursive calls can report a nested path as the name.
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$", key):
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid parameter name: {key}"
      )

    if isinstance(value, str):
      if len(value) > 10000:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' value too long (max 10000 characters)",
        )
    elif isinstance(value, (list, tuple)):
      if len(value) > 1000:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' array too large (max 1000 elements)",
        )
      # Element count alone does not bound the payload; total size does.
      total_size = sum(len(str(item)) for item in value)
      if total_size > 50000:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' array total size too large",
        )
    elif isinstance(value, dict):
      if len(value) > 100:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' object too large (max 100 keys)",
        )
      validate_query_parameters({f"{key}.{k}": v for k, v in value.items()})
    elif isinstance(value, (int, float)):
      if abs(value) > 1e15:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' numeric value out of bounds",
        )
    elif value is None or isinstance(value, bool):
      pass
    else:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Parameter '{key}' has unsupported type: {type(value).__name__}",
      )
