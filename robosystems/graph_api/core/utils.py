"""
Utility functions for the Graph API.
"""

import re
from typing import Any

from fastapi import HTTPException, status


def validate_database_name(graph_id: str) -> str:
  """
  Validate and sanitize graph ID to prevent path traversal.

  Args:
      graph_id: Graph ID to validate

  Returns:
      Sanitized graph ID

  Raises:
      HTTPException: If graph ID is invalid
  """
  if not graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Graph ID cannot be empty"
    )

  # Check for path traversal attempts
  if ".." in graph_id or "/" in graph_id or "\\" in graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Graph ID contains invalid characters",
    )

  # Ensure alphanumeric + underscore/dash only
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
  """
  Validate query parameters for safety.

  Args:
      params: Dictionary of query parameters

  Raises:
      HTTPException: If parameters are invalid
  """
  if not params:
    return

  if len(params) > 50:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Too many parameters (max 50)"
    )

  for key, value in params.items():
    # Validate parameter names (allow dots for nested parameters)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$", key):
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid parameter name: {key}"
      )

    # Validate parameter values based on type
    if isinstance(value, str):
      # Check string parameter size
      if len(value) > 10000:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' value too long (max 10000 characters)",
        )
    elif isinstance(value, (list, tuple)):
      # Check array size
      if len(value) > 1000:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' array too large (max 1000 elements)",
        )
      # Check total size of array elements
      total_size = sum(len(str(item)) for item in value)
      if total_size > 50000:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' array total size too large",
        )
    elif isinstance(value, dict):
      # Check object size
      if len(value) > 100:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' object too large (max 100 keys)",
        )
      # Recursively validate nested objects
      validate_query_parameters({f"{key}.{k}": v for k, v in value.items()})
    elif isinstance(value, (int, float)):
      # Check numeric bounds
      if abs(value) > 1e15:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Parameter '{key}' numeric value out of bounds",
        )
    elif value is None or isinstance(value, bool):
      # These are allowed
      pass
    else:
      # Reject unknown types
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Parameter '{key}' has unsupported type: {type(value).__name__}",
      )
