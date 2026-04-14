"""Tests for the Strawberry context builder.

Covers the auth contract documented in `graphql/context.py:get_context`:

1. No credentials → anonymous (user=None) for introspection bypass
2. Credentials presented but invalid → re-raise transport 401
3. Valid credentials → user populated, graph access enforced
4. Valid credentials but no graph access → 403

The "credentials presented but invalid" case is the security-critical
one — without it, expired tokens would silently be downgraded to
anonymous access, letting them keep getting schema introspection
without any indication something is wrong.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.graphql.context import get_context

MODULE = "robosystems.graphql.context"
GRAPH_ID = "kg01234567890abcdef"


def _make_request(headers: dict[str, str] | None = None) -> MagicMock:
  """Build a stub Request with controllable headers."""
  request = MagicMock()
  request.headers = headers or {}
  return request


@pytest.mark.asyncio
class TestGetContextAuthContract:
  """The four-way auth contract documented on `get_context`."""

  async def test_no_credentials_returns_anonymous(self):
    """Case 1: no X-API-Key, no Authorization → user=None (introspection OK)."""
    request = _make_request(headers={})

    with (
      patch(
        f"{MODULE}.get_current_user",
        new_callable=AsyncMock,
        side_effect=HTTPException(status_code=401, detail="No credentials"),
      ),
      patch(f"{MODULE}.check_graph_access") as mock_check,
    ):
      ctx = await get_context(request=request, api_key=None, graph_id=GRAPH_ID)

    assert ctx["user"] is None
    assert ctx["graph_id"] == GRAPH_ID
    mock_check.assert_not_called()

  async def test_invalid_api_key_re_raises_401(self):
    """Case 2a: X-API-Key present but bad → real 401, NOT silent anonymous."""
    request = _make_request(headers={})

    with patch(
      f"{MODULE}.get_current_user",
      new_callable=AsyncMock,
      side_effect=HTTPException(status_code=401, detail="Invalid API key"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_context(request=request, api_key="rfs_bad_key", graph_id=GRAPH_ID)

    assert exc_info.value.status_code == 401

  async def test_invalid_jwt_re_raises_401(self):
    """Case 2b: Authorization header present but bad → real 401."""
    request = _make_request(headers={"Authorization": "Bearer expired.jwt.token"})

    with patch(
      f"{MODULE}.get_current_user",
      new_callable=AsyncMock,
      side_effect=HTTPException(status_code=401, detail="Token expired"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_context(request=request, api_key=None, graph_id=GRAPH_ID)

    assert exc_info.value.status_code == 401

  async def test_valid_credentials_populate_user_and_check_access(self):
    """Case 3: valid creds → user populated, check_graph_access runs."""
    request = _make_request(headers={})
    user = MagicMock()
    user.id = "usr_test"

    with (
      patch(f"{MODULE}.get_current_user", new_callable=AsyncMock, return_value=user),
      patch(f"{MODULE}.check_graph_access") as mock_check,
    ):
      ctx = await get_context(request=request, api_key="rfs_valid", graph_id=GRAPH_ID)

    assert ctx["user"] is user
    assert ctx["graph_id"] == GRAPH_ID
    mock_check.assert_called_once_with(user, GRAPH_ID)

  async def test_valid_credentials_but_no_graph_access_raises_403(self):
    """Case 4: valid creds, but the user can't see this graph → 403."""
    request = _make_request(headers={})
    user = MagicMock()

    with (
      patch(f"{MODULE}.get_current_user", new_callable=AsyncMock, return_value=user),
      patch(
        f"{MODULE}.check_graph_access",
        side_effect=HTTPException(status_code=403, detail="Forbidden"),
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_context(request=request, api_key="rfs_valid", graph_id=GRAPH_ID)

    assert exc_info.value.status_code == 403
