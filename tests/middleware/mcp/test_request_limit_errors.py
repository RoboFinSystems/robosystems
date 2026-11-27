"""A refused oversized request names its limit; other query errors stay hidden."""

from unittest.mock import patch

import pytest

from robosystems.config import env
from robosystems.middleware.mcp.client import GraphMCPClient


def _sanitize(message: str) -> str:
  client = GraphMCPClient.__new__(GraphMCPClient)
  with (
    patch.object(env, "is_development", return_value=False),
    patch.object(env, "is_staging", return_value=False),
  ):
    return client._sanitize_error_message(Exception(message), "query execution")


@pytest.mark.unit
@pytest.mark.parametrize(
  "detail",
  [
    "Parameter 'rows' array total size too large (max 50000 characters across its items)",
    "Parameter 'rows' array too large (max 1000 elements)",
    "Parameter 'rows.summary' value too long (max 10000 characters)",
    "Too many parameters (max 50)",
  ],
)
def test_a_request_limit_is_named_in_production(detail):
  message = _sanitize(f"Graph API error 400: {detail}")
  assert detail in message
  assert "400" not in message


@pytest.mark.unit
def test_a_schema_error_is_still_hidden_in_production():
  message = _sanitize("Binder exception: Cannot find property secret_col for n.")
  assert "secret_col" not in message
