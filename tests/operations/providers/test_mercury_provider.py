# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""Tests for the Mercury provider: endpoints, the two credential modes,
sync dispatch, and the disconnect protocol."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.connections import MercuryConnectionConfig

MODULE = "robosystems.operations.providers.mercury_provider"


def _provider(environment: str = "sandbox"):
  with patch(f"{MODULE}.env") as mock_env:
    mock_env.MERCURY_ENVIRONMENT = environment
    mock_env.MERCURY_CLIENT_ID = "cid"
    mock_env.MERCURY_CLIENT_SECRET = "csecret"
    from robosystems.operations.providers.mercury_provider import (
      MercuryOAuthProvider,
    )

    provider = MercuryOAuthProvider()
    # Properties read env lazily; keep the patch alive for the caller.
    return provider, mock_env


@pytest.mark.unit
class TestMercuryOAuthProvider:
  def test_sandbox_hosts(self):
    provider, _ = _provider("sandbox")
    assert provider.authorize_url == "https://oauth2-sandbox.mercury.com/oauth2/auth"
    assert provider.token_url == "https://oauth2-sandbox.mercury.com/oauth2/token"
    assert provider.revoke_url == "https://oauth2-sandbox.mercury.com/oauth2/revoke"
    assert provider.api_base_url == "https://api-sandbox.mercury.com/api/v1"

  def test_production_hosts(self):
    provider, _ = _provider("production")
    assert provider.authorize_url == "https://oauth2.mercury.com/oauth2/auth"
    assert provider.api_base_url == "https://api.mercury.com/api/v1"

  def test_protocol_surface(self):
    provider, _ = _provider()
    assert provider.name == "mercury"
    assert provider.scopes == ["read", "offline_access"]
    assert provider.get_additional_auth_params() == {}
    # scope on every refresh — Mercury's most common partner failure
    assert provider.get_refresh_params() == {"scope": "read offline_access"}

  def test_extract_provider_data_carries_the_sync_config(self):
    provider, _ = _provider()
    data = provider.extract_provider_data({"sync_config": {"since_date": "2026-01-01"}})
    assert data == {"auth_mode": "oauth", "sync_config": {"since_date": "2026-01-01"}}
    assert provider.extract_provider_data({}) == {
      "auth_mode": "oauth",
      "sync_config": {},
    }

  @pytest.mark.asyncio
  async def test_get_entity_info_reads_the_first_account(self):
    provider, _ = _provider()
    response = MagicMock(
      status_code=200,
      content=b"{}",
      json=MagicMock(
        return_value={
          "accounts": [
            {"id": "a1", "legalBusinessName": "Cascade Books LLC"},
            {"id": "a2"},
          ]
        }
      ),
    )
    client = AsyncMock()
    client.get = AsyncMock(return_value=response)
    client.__aenter__.return_value = client
    with patch(f"{MODULE}.httpx.AsyncClient", return_value=client):
      info = await provider.get_entity_info("tok")
    assert info == {"legal_business_name": "Cascade Books LLC", "account_count": 2}
    assert client.get.call_args.kwargs["headers"]["Authorization"] == "Bearer tok"

  @pytest.mark.asyncio
  async def test_get_entity_info_non_200_is_empty(self):
    provider, _ = _provider()
    response = MagicMock(status_code=401, content=b"", json=MagicMock(return_value={}))
    client = AsyncMock()
    client.get = AsyncMock(return_value=response)
    client.__aenter__.return_value = client
    with patch(f"{MODULE}.httpx.AsyncClient", return_value=client):
      assert await provider.get_entity_info("tok") == {}
      assert await provider.validate_connection("tok") is False

  @pytest.mark.asyncio
  async def test_revoke_token_posts_basic_auth(self):
    provider, _ = _provider()
    response = MagicMock(status_code=200, text="")
    client = AsyncMock()
    client.post = AsyncMock(return_value=response)
    client.__aenter__.return_value = client
    with (
      patch(f"{MODULE}.httpx.AsyncClient", return_value=client),
      patch(f"{MODULE}.env") as mock_env,
    ):
      mock_env.MERCURY_CLIENT_ID = "cid"
      mock_env.MERCURY_CLIENT_SECRET = "csecret"
      assert await provider.revoke_token("ref") is True
    kwargs = client.post.call_args.kwargs
    assert kwargs["data"] == {"token": "ref"}
    assert kwargs["auth"] == ("cid", "csecret")

  @pytest.mark.asyncio
  async def test_revoke_token_failure_is_false(self):
    provider, _ = _provider()
    response = MagicMock(status_code=400, text="nope")
    client = AsyncMock()
    client.post = AsyncMock(return_value=response)
    client.__aenter__.return_value = client
    with patch(f"{MODULE}.httpx.AsyncClient", return_value=client):
      assert await provider.revoke_token("ref") is False


@pytest.mark.unit
class TestCreateMercuryConnection:
  @pytest.mark.asyncio
  async def test_oauth_mode_parks_the_sync_config_on_a_pending_row(self):
    from robosystems.operations.providers.mercury_provider import (
      create_mercury_connection,
    )

    with patch(
      f"{MODULE}.ConnectionService.create_connection",
      new_callable=AsyncMock,
      return_value={"connection_id": "conn_new"},
    ) as create:
      connection_id = await create_mercury_connection(
        "kg_test",
        MercuryConnectionConfig(since_date=date(2026, 1, 1), include_treasury=False),
        "usr_1",
        "kg_test",
        MagicMock(),
      )
    assert connection_id == "conn_new"
    kwargs = create.call_args.kwargs
    assert kwargs["provider"] == "mercury"
    assert kwargs["graph_id"] == "kg_test"
    assert kwargs["metadata"] == {
      "status": "pending_oauth",
      "institution_name": "Mercury",
    }
    assert kwargs["credentials"] == {
      "auth_mode": "oauth",
      "sync_config": {"since_date": "2026-01-01", "include_treasury": False},
    }

  @pytest.mark.asyncio
  async def test_oauth_mode_with_no_config_uses_defaults(self):
    from robosystems.operations.providers.mercury_provider import (
      create_mercury_connection,
    )

    with patch(
      f"{MODULE}.ConnectionService.create_connection",
      new_callable=AsyncMock,
      return_value={"connection_id": "conn_new"},
    ) as create:
      await create_mercury_connection("kg_test", None, "usr_1", "kg_test", MagicMock())
    assert create.call_args.kwargs["credentials"]["sync_config"] == {
      "since_date": None,
      "include_treasury": True,
    }

  @pytest.mark.asyncio
  async def test_api_key_mode_is_refused_when_the_flag_is_off(self):
    from robosystems.operations.providers.mercury_provider import (
      create_mercury_connection,
    )

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(
        f"{MODULE}.ConnectionService.create_connection", new_callable=AsyncMock
      ) as create,
    ):
      mock_env.MERCURY_API_KEY_CONNECTIONS_ENABLED = False
      with pytest.raises(ValueError, match="not enabled"):
        await create_mercury_connection(
          "kg_test",
          MercuryConnectionConfig(api_key="secret-token-123"),
          "usr_1",
          "kg_test",
          MagicMock(),
        )
    create.assert_not_called()

  @pytest.mark.asyncio
  async def test_api_key_mode_proves_the_key_connects_and_syncs(self):
    from robosystems.operations.providers.mercury_provider import (
      create_mercury_connection,
    )

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(
        f"{MODULE}.mercury_oauth_provider.get_entity_info",
        new_callable=AsyncMock,
        return_value={"legal_business_name": "Cascade Books LLC", "account_count": 2},
      ),
      patch(
        f"{MODULE}.ConnectionService.create_connection",
        new_callable=AsyncMock,
        return_value={"connection_id": "conn_key"},
      ) as create,
      patch(f"{MODULE}.record_bank_feed_consent") as consent,
      patch(f"{MODULE}._dispatch_first_sync", new_callable=AsyncMock) as dispatch,
    ):
      mock_env.MERCURY_API_KEY_CONNECTIONS_ENABLED = True
      connection_id = await create_mercury_connection(
        "kg_test",
        MercuryConnectionConfig(api_key="secret-token-123"),
        "usr_1",
        "kg_test",
        MagicMock(),
      )
    assert connection_id == "conn_key"
    kwargs = create.call_args.kwargs
    assert kwargs["metadata"]["status"] == "connected"
    assert kwargs["metadata"]["entity_name"] == "Cascade Books LLC"
    assert kwargs["credentials"]["auth_mode"] == "api_key"
    assert kwargs["credentials"]["api_key"] == "secret-token-123"
    consent.assert_called_once()
    assert consent.call_args.kwargs["auth_mode"] == "api_key"
    dispatch.assert_awaited_once_with("kg_test", "conn_key", "usr_1")

  @pytest.mark.asyncio
  async def test_api_key_mode_rejects_a_bad_key_with_400(self):
    from robosystems.operations.providers.mercury_provider import (
      create_mercury_connection,
    )

    with (
      patch(f"{MODULE}.env") as mock_env,
      patch(
        f"{MODULE}.mercury_oauth_provider.get_entity_info",
        new_callable=AsyncMock,
        return_value={},
      ),
      patch(
        f"{MODULE}.ConnectionService.create_connection", new_callable=AsyncMock
      ) as create,
    ):
      mock_env.MERCURY_API_KEY_CONNECTIONS_ENABLED = True
      with pytest.raises(HTTPException) as excinfo:
        await create_mercury_connection(
          "kg_test",
          MercuryConnectionConfig(api_key="secret-token-123"),
          "usr_1",
          "kg_test",
          MagicMock(),
        )
    assert excinfo.value.status_code == 400
    create.assert_not_called()

  @pytest.mark.asyncio
  async def test_first_sync_dispatch_failure_does_not_fail_the_connect(self):
    from robosystems.operations.providers.mercury_provider import _dispatch_first_sync

    with patch(
      "robosystems.operations.connection_service.dispatch_connection_sync",
      new_callable=AsyncMock,
      side_effect=RuntimeError("dagster down"),
    ):
      await _dispatch_first_sync("kg_test", "conn_1", "usr_1")  # no raise


@pytest.mark.unit
class TestSyncMercuryConnection:
  @pytest.mark.asyncio
  async def test_submits_the_job_with_the_asset_config(self):
    from robosystems.operations.providers.mercury_provider import (
      sync_mercury_connection,
    )

    connection = {"connection_id": "conn_1", "user_id": "usr_1", "metadata": {}}
    with patch(
      "robosystems.middleware.sse.dagster_monitor.submit_dagster_job_sync",
      return_value="run_42",
    ) as submit:
      outcome = await sync_mercury_connection(
        connection,
        {"full_rebuild": True, "since_date": "2026-01-01", "sync_lock_id": "lock_1"},
        "kg_test",
      )
    assert outcome.dispatched and outcome.task_id == "run_42"
    kwargs = submit.call_args.kwargs
    assert kwargs["job_name"] == "mercury_sync"
    assert kwargs["tags"] == {
      "graph_id": "kg_test",
      "connection_id": "conn_1",
      "pipeline": "mercury",
    }
    assert kwargs["run_config"] == {
      "ops": {
        "mercury_feed": {
          "config": {
            "graph_id": "kg_test",
            "connection_id": "conn_1",
            "user_id": "usr_1",
            "full_rebuild": True,
            "lookback_days": 60,
            "since_date": "2026-01-01",
            "sync_lock_id": "lock_1",
          }
        }
      }
    }

  @pytest.mark.asyncio
  async def test_defaults_without_options(self):
    from robosystems.operations.providers.mercury_provider import (
      sync_mercury_connection,
    )

    with patch(
      "robosystems.middleware.sse.dagster_monitor.submit_dagster_job_sync",
      return_value="run_1",
    ) as submit:
      await sync_mercury_connection({"connection_id": "c", "user_id": "u"}, None, "kg")
    config = submit.call_args.kwargs["run_config"]["ops"]["mercury_feed"]["config"]
    assert config["full_rebuild"] is False
    assert config["since_date"] == "" and config["sync_lock_id"] == ""


@pytest.mark.unit
class TestCleanupMercuryConnection:
  def _creds(self, bundle: dict):
    creds = MagicMock()
    creds.get_credentials.return_value = bundle
    return creds

  def _platform_session(self, creds):
    session = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False
    return session, cm

  @pytest.mark.asyncio
  async def test_oauth_disconnect_revokes_purges_and_empties(self):
    from robosystems.operations.providers.mercury_provider import (
      cleanup_mercury_connection,
    )

    creds = self._creds(
      {"auth_mode": "oauth", "refresh_token": "ref", "access_token": "acc"}
    )
    session, cm = self._platform_session(creds)
    with (
      patch("robosystems.database.platform_session", return_value=cm),
      patch(
        "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
        return_value=creds,
      ),
      patch(
        f"{MODULE}.mercury_oauth_provider.revoke_token",
        new_callable=AsyncMock,
        return_value=True,
      ) as revoke,
      patch(
        f"{MODULE}._purge_feed",
        return_value={
          "events_deleted": 3,
          "events_scrubbed": 1,
          "agents_deleted": 2,
          "accounts_unlinked": 2,
        },
      ) as purge,
      patch(
        "robosystems.security.audit_logger.SecurityAuditLogger.log_security_event"
      ) as audit,
    ):
      await cleanup_mercury_connection(
        {"connection_id": "conn_1", "user_id": "usr_1"}, "kg_test"
      )
    revoke.assert_awaited_once_with("ref")
    purge.assert_called_once_with("kg_test", "conn_1")
    emptied = creds.update_credentials.call_args.args[0]
    assert emptied["auth_mode"] == "oauth" and "revoked_at" in emptied
    assert "refresh_token" not in emptied and "api_key" not in emptied
    details = audit.call_args.kwargs["details"]
    assert details["events_deleted"] == 3 and details["provider"] == "mercury"

  @pytest.mark.asyncio
  async def test_api_key_disconnect_does_not_revoke(self):
    from robosystems.operations.providers.mercury_provider import (
      cleanup_mercury_connection,
    )

    creds = self._creds({"auth_mode": "api_key", "api_key": "pk"})
    session, cm = self._platform_session(creds)
    with (
      patch("robosystems.database.platform_session", return_value=cm),
      patch(
        "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
        return_value=creds,
      ),
      patch(
        f"{MODULE}.mercury_oauth_provider.revoke_token", new_callable=AsyncMock
      ) as revoke,
      patch(f"{MODULE}._purge_feed", return_value={}),
      patch("robosystems.security.audit_logger.SecurityAuditLogger.log_security_event"),
    ):
      await cleanup_mercury_connection({"connection_id": "conn_1"}, "kg_test")
    revoke.assert_not_awaited()
    assert creds.update_credentials.call_args.args[0]["auth_mode"] == "api_key"

  @pytest.mark.asyncio
  async def test_revocation_error_does_not_block_the_purge(self):
    from robosystems.operations.providers.mercury_provider import (
      cleanup_mercury_connection,
    )

    creds = self._creds({"auth_mode": "oauth", "refresh_token": "ref"})
    session, cm = self._platform_session(creds)
    with (
      patch("robosystems.database.platform_session", return_value=cm),
      patch(
        "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
        return_value=creds,
      ),
      patch(
        f"{MODULE}.mercury_oauth_provider.revoke_token",
        new_callable=AsyncMock,
        side_effect=httpx.ConnectError("down"),
      ),
      patch(f"{MODULE}._purge_feed", return_value={}) as purge,
      patch("robosystems.security.audit_logger.SecurityAuditLogger.log_security_event"),
    ):
      await cleanup_mercury_connection({"connection_id": "conn_1"}, "kg_test")
    purge.assert_called_once()

  @pytest.mark.asyncio
  async def test_missing_id_is_a_no_op(self):
    from robosystems.operations.providers.mercury_provider import (
      cleanup_mercury_connection,
    )

    with patch(f"{MODULE}._purge_feed") as purge:
      await cleanup_mercury_connection({}, "kg_test")
    purge.assert_not_called()

  def test_purge_feed_on_a_missing_schema_is_nothing_to_do(self):
    from sqlalchemy.exc import ProgrammingError

    from robosystems.operations.providers.mercury_provider import _purge_feed

    exc = ProgrammingError("stmt", {}, Exception("schema"))
    with (
      patch("robosystems.db.extensions.extensions_session", side_effect=exc),
      patch("robosystems.middleware.extensions.is_schema_missing", return_value=True),
    ):
      assert _purge_feed("kg_test", "conn_1") == {
        "events_deleted": 0,
        "events_scrubbed": 0,
        "agents_deleted": 0,
      }

  def test_purge_feed_other_errors_surface(self):
    from sqlalchemy.exc import ProgrammingError

    from robosystems.operations.providers.mercury_provider import _purge_feed

    exc = ProgrammingError("stmt", {}, Exception("other"))
    with (
      patch("robosystems.db.extensions.extensions_session", side_effect=exc),
      patch("robosystems.middleware.extensions.is_schema_missing", return_value=False),
    ):
      with pytest.raises(ProgrammingError):
        _purge_feed("kg_test", "conn_1")


@pytest.mark.unit
def test_record_bank_feed_consent_writes_the_daa_record():
  from robosystems.operations.providers.mercury_provider import (
    record_bank_feed_consent,
  )
  from robosystems.security.audit_logger import SecurityEventType

  with patch(
    "robosystems.security.audit_logger.SecurityAuditLogger.log_security_event"
  ) as audit:
    record_bank_feed_consent(
      graph_id="kg_test",
      connection_id="conn_1",
      user_id="usr_1",
      auth_mode="oauth",
      scope="read offline_access",
      organization="Cascade Books LLC",
    )
  kwargs = audit.call_args.kwargs
  assert kwargs["event_type"] is SecurityEventType.BANK_FEED_CONSENT_GRANTED
  assert kwargs["user_id"] == "usr_1"
  details = kwargs["details"]
  assert details["organization"] == "Cascade Books LLC"
  assert details["scope"] == "read offline_access"
  assert details["auth_mode"] == "oauth"
  assert "granted_at" in details
