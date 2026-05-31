# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportOptionalMemberAccess=false
"""Regression tests for QuickBooks disconnect → Intuit token revocation.

Locks in the fix that disconnecting a QuickBooks connection revokes the stored
refresh token at Intuit (tearing the grant down provider-side), and that the
step is best-effort (never blocks deletion).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

MODULE = "robosystems.operations.providers.quickbooks_provider"
GID = "kg" + "0" * 16


def _session_cm(db: MagicMock) -> MagicMock:
  """A stand-in for ``platform_session()`` as a context manager."""
  cm = MagicMock()
  cm.__enter__.return_value = db
  cm.__exit__.return_value = False
  return cm


def _creds(data: dict | None) -> MagicMock | None:
  if data is None:
    return None
  creds = MagicMock()
  creds.get_credentials.return_value = data
  return creds


@pytest.mark.unit
class TestQuickBooksCleanupRevoke:
  @pytest.mark.asyncio
  async def test_revokes_stored_refresh_token(self) -> None:
    from robosystems.operations.providers.quickbooks_provider import (
      cleanup_quickbooks_connection,
    )

    with (
      patch(
        "robosystems.database.platform_session", return_value=_session_cm(MagicMock())
      ),
      patch(
        "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
        return_value=_creds({"refresh_token": "rt_abc", "access_token": "at_x"}),
      ),
      patch(
        f"{MODULE}.quickbooks_oauth_provider.revoke_token",
        new=AsyncMock(return_value=True),
      ) as revoke,
    ):
      await cleanup_quickbooks_connection(
        {"connection_id": "conn_1", "entity_id": "e1"}, GID
      )
    # Revokes the REFRESH token (invalidates the whole grant), not the access token.
    revoke.assert_awaited_once_with("rt_abc")

  @pytest.mark.asyncio
  async def test_no_credentials_skips_revoke(self) -> None:
    from robosystems.operations.providers.quickbooks_provider import (
      cleanup_quickbooks_connection,
    )

    with (
      patch(
        "robosystems.database.platform_session", return_value=_session_cm(MagicMock())
      ),
      patch(
        "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
        return_value=None,
      ),
      patch(
        f"{MODULE}.quickbooks_oauth_provider.revoke_token", new=AsyncMock()
      ) as revoke,
    ):
      await cleanup_quickbooks_connection(
        {"connection_id": "conn_1", "entity_id": "e1"}, GID
      )
    revoke.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_missing_connection_id_is_handled(self) -> None:
    from robosystems.operations.providers.quickbooks_provider import (
      cleanup_quickbooks_connection,
    )

    with patch(
      f"{MODULE}.quickbooks_oauth_provider.revoke_token", new=AsyncMock()
    ) as revoke:
      # No id on the payload → log + return, no revoke, no raise.
      await cleanup_quickbooks_connection({"entity_id": "e1"}, GID)
    revoke.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_revoke_failure_is_swallowed(self) -> None:
    """Best-effort: a revoke exception must not propagate (deletion proceeds)."""
    from robosystems.operations.providers.quickbooks_provider import (
      cleanup_quickbooks_connection,
    )

    with (
      patch(
        "robosystems.database.platform_session", return_value=_session_cm(MagicMock())
      ),
      patch(
        "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
        return_value=_creds({"refresh_token": "rt_abc"}),
      ),
      patch(
        f"{MODULE}.quickbooks_oauth_provider.revoke_token",
        new=AsyncMock(side_effect=RuntimeError("boom")),
      ),
    ):
      # Must not raise.
      await cleanup_quickbooks_connection(
        {"connection_id": "conn_1", "entity_id": "e1"}, GID
      )
