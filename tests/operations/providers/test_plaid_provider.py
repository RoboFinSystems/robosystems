# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""The Plaid provider: create, Link tokens, completing Link, sync, disconnect."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.adapters.plaid.client import PlaidError
from robosystems.models.api.graphs.connections import PlaidConnectionConfig
from robosystems.operations.providers.plaid_provider import (
  account_fingerprint,
  days_requested,
)
from robosystems.operations.providers.types import SyncOutcome

MODULE = "robosystems.operations.providers.plaid_provider"
ACCOUNTS = [
  {
    "account_id": "a1",
    "name": "Checking",
    "mask": "1234",
    "type": "depository",
    "subtype": "checking",
  },
  {
    "account_id": "a2",
    "name": "Card",
    "mask": "9012",
    "type": "credit",
    "subtype": "credit card",
  },
  {
    "account_id": "a3",
    "name": "Loan",
    "mask": "3456",
    "type": "loan",
    "subtype": "commercial",
  },
]


def _client(**overrides):
  client = MagicMock()
  client.exchange_public_token.return_value = {
    "access_token": "access-new",
    "item_id": "item-new",
  }
  client.get_accounts.return_value = {
    "accounts": ACCOUNTS,
    "item": {
      "item_id": "item-new",
      "institution_id": "ins_1",
      "institution_name": "Harborline Bank",
    },
  }
  client.create_link_token.return_value = {"link_token": "link-1"}
  for name, value in overrides.items():
    setattr(client, name, value)
  return client


@pytest.mark.unit
class TestHelpers:
  def test_days_requested(self):
    assert days_requested("2026-09-01", date(2026, 9, 16)) == 16
    assert days_requested(None) is None

  def test_fingerprint_keeps_cash_and_cards(self):
    assert [a["account_id"] for a in account_fingerprint(ACCOUNTS)] == ["a1", "a2"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_opens_a_pending_connection_with_the_sync_config():
  from robosystems.operations.providers.plaid_provider import create_plaid_connection

  with patch(
    f"{MODULE}.ConnectionService.create_connection",
    new_callable=AsyncMock,
    return_value={"connection_id": "conn_1"},
  ) as create:
    connection_id = await create_plaid_connection(
      "kg_1",
      PlaidConnectionConfig(since_date=date(2026, 1, 1)),
      "usr_1",
      "kg_1",
      MagicMock(),
    )
  assert connection_id == "conn_1"
  kwargs = create.call_args.kwargs
  assert kwargs["provider"] == "plaid"
  assert kwargs["metadata"] == {"status": "pending_oauth"}
  assert kwargs["credentials"] == {
    "auth_mode": "link",
    "sync_config": {"since_date": "2026-01-01"},
  }


@pytest.mark.unit
@pytest.mark.asyncio
class TestLinkToken:
  async def test_new_item(self):
    from robosystems.operations.providers.plaid_provider import create_link_token

    client = _client()
    with (
      patch(
        f"{MODULE}._credentials", return_value={"sync_config": {"since_date": None}}
      ),
      patch(f"{MODULE}.plaid_client", return_value=client),
    ):
      assert await create_link_token("conn_1", "usr_1", MagicMock()) == {
        "link_token": "link-1"
      }
    kwargs = client.create_link_token.call_args.kwargs
    assert kwargs["access_token"] is None and kwargs["client_user_id"] == "usr_1"
    client.close.assert_called_once()

  async def test_update_mode_on_the_connections_item(self):
    from robosystems.operations.providers.plaid_provider import create_link_token

    client = _client()
    with (
      patch(f"{MODULE}._credentials", return_value={"access_token": "access-1"}),
      patch(f"{MODULE}.plaid_client", return_value=client),
    ):
      await create_link_token("conn_1", "usr_1", MagicMock())
    assert client.create_link_token.call_args.kwargs["access_token"] == "access-1"

  async def test_a_gone_item_falls_back_to_a_new_one(self):
    from robosystems.operations.providers.plaid_provider import create_link_token

    client = _client()
    client.create_link_token.side_effect = [
      PlaidError("gone", code="ITEM_NOT_FOUND"),
      {"link_token": "link-fresh"},
    ]
    with (
      patch(f"{MODULE}._credentials", return_value={"access_token": "access-dead"}),
      patch(f"{MODULE}.plaid_client", return_value=client),
    ):
      assert await create_link_token("conn_1", "usr_1", MagicMock()) == {
        "link_token": "link-fresh"
      }
    assert "access_token" not in client.create_link_token.call_args.kwargs

  async def test_other_errors_surface(self):
    from robosystems.operations.providers.plaid_provider import create_link_token

    client = _client()
    client.create_link_token.side_effect = PlaidError("bad", code="INVALID_FIELD")
    with (
      patch(f"{MODULE}._credentials", return_value={"access_token": "access-1"}),
      patch(f"{MODULE}.plaid_client", return_value=client),
    ):
      with pytest.raises(PlaidError):
        await create_link_token("conn_1", "usr_1", MagicMock())


@pytest.mark.unit
@pytest.mark.asyncio
class TestCompleteLink:
  def _patches(self, client, credentials, *, duplicate=None, stored=True):
    registry = MagicMock()
    registry.sync_connection = AsyncMock(
      return_value=SyncOutcome(status="dispatched", task_id="run_1")
    )
    return (
      registry,
      patch(f"{MODULE}.plaid_client", return_value=client),
      patch(f"{MODULE}._credentials", return_value=credentials),
      patch(f"{MODULE}.find_duplicate_item", return_value=duplicate),
      patch(
        f"{MODULE}.ConnectionService.update",
        new_callable=AsyncMock,
        return_value=stored,
      ),
      patch(f"{MODULE}.record_bank_feed_consent"),
      patch("robosystems.operations.providers.registry.provider_registry", registry),
    )

  async def _complete(self, client, credentials, connection=None, **kwargs):
    from robosystems.operations.providers.plaid_provider import complete_plaid_link

    registry, *patches = self._patches(client, credentials, **kwargs)
    with (
      patches[0],
      patches[1],
      patches[2],
      patches[3] as update,
      patches[4] as consent,
      patches[5],
    ):
      result = await complete_plaid_link(
        graph_id="kg_1",
        connection=connection
        or {"connection_id": "conn_1", "metadata": {"last_sync": None}},
        connection_id="conn_1",
        public_token="public-1",
        user_id="usr_1",
        db=MagicMock(),
      )
    return result, update, consent, registry

  async def test_a_new_item_is_stored_recorded_and_backfilled(self):
    client = _client()
    result, update, consent, registry = await self._complete(
      client, {"auth_mode": "link", "sync_config": {"since_date": "2026-01-01"}}
    )
    assert result["success"] and result["auto_sync_task_id"] == "run_1"
    assert result["message"] == "Harborline Bank connected through Plaid"
    kwargs = update.call_args.kwargs
    creds = kwargs["credentials"]
    assert creds["access_token"] == "access-new" and creds["item_id"] == "item-new"
    assert creds["institution_id"] == "ins_1" and creds["cursor"] is None
    assert creds["sync_config"] == {"since_date": "2026-01-01"}
    assert [a["mask"] for a in creds["accounts"]] == ["1234", "9012"]
    assert kwargs["status"] == "connected"
    assert kwargs["metadata"]["institution_name"] == "Harborline Bank"
    assert consent.call_args.kwargs["provider"] == "plaid"
    assert registry.sync_connection.call_args.args[2] == {"full_rebuild": True}

  async def test_update_mode_keeps_the_item_and_its_cursor(self):
    client = _client()
    client.exchange_public_token.side_effect = PlaidError(
      "nothing to exchange", code="INVALID_PUBLIC_TOKEN"
    )
    client.get_accounts.return_value = {
      "accounts": ACCOUNTS,
      "item": {
        "item_id": "item-1",
        "institution_id": "ins_1",
        "institution_name": "Harborline Bank",
      },
    }
    _result, update, _consent, registry = await self._complete(
      client,
      {"access_token": "access-1", "item_id": "item-1", "cursor": "c5"},
      connection={
        "connection_id": "conn_1",
        "metadata": {"last_sync": "2026-09-01T00:00:00"},
      },
    )
    creds = update.call_args.kwargs["credentials"]
    assert creds["access_token"] == "access-1" and creds["cursor"] == "c5"
    assert registry.sync_connection.call_args.args[2] is None
    client.remove_item.assert_not_called()

  async def test_a_fresh_item_replacing_a_dead_one_removes_the_old(self):
    client = _client()
    _result, update, _consent, registry = await self._complete(
      client,
      {"access_token": "access-dead", "item_id": "item-dead", "cursor": "c5"},
      connection={
        "connection_id": "conn_1",
        "metadata": {"last_sync": "2026-09-01T00:00:00"},
      },
    )
    client.remove_item.assert_called_once_with("access-dead")
    assert update.call_args.kwargs["credentials"]["cursor"] is None
    assert registry.sync_connection.call_args.args[2] == {"full_rebuild": True}

  async def test_a_duplicate_item_is_removed_and_refused(self):
    from robosystems.operations.providers.plaid_provider import (
      DuplicateBankConnectionError,
    )

    client = _client()
    with pytest.raises(DuplicateBankConnectionError) as exc_info:
      await self._complete(client, {}, duplicate="conn_other")
    assert exc_info.value.existing_connection_id == "conn_other"
    client.remove_item.assert_called_once_with("access-new")

  async def test_a_duplicate_is_refused_even_when_removal_fails(self):
    from robosystems.operations.providers.plaid_provider import (
      DuplicateBankConnectionError,
    )

    client = _client()
    client.remove_item.side_effect = PlaidError("down", code="INTERNAL_SERVER_ERROR")
    with pytest.raises(DuplicateBankConnectionError):
      await self._complete(client, {}, duplicate="conn_other")

  async def test_a_failed_credential_write_is_an_error(self):
    with pytest.raises(RuntimeError, match="could not be stored"):
      await self._complete(_client(), {}, stored=False)


@pytest.mark.unit
class TestFindDuplicate:
  def _other(self, provider="plaid", id_="conn_other"):
    other = MagicMock()
    other.provider = provider
    other.id = id_
    return other

  def _find(self, stored, *, fingerprint, others=None):
    from robosystems.operations.providers.plaid_provider import find_duplicate_item

    with (
      patch(
        "robosystems.models.core.connection.connection.Connection.get_all_for_graph",
        return_value=others if others is not None else [self._other()],
      ),
      patch(f"{MODULE}._credentials", return_value=stored),
    ):
      return find_duplicate_item(
        "kg_1",
        connection_id="conn_1",
        institution_id="ins_1",
        fingerprint=fingerprint,
        db=MagicMock(),
      )

  def test_same_institution_sharing_an_account_is_a_duplicate(self):
    stored = {
      "institution_id": "ins_1",
      "accounts": [{"mask": "1234", "subtype": "checking"}],
    }
    fingerprint = [
      {"mask": "1234", "subtype": "checking"},
      {"mask": "9012", "subtype": "credit card"},
    ]
    assert self._find(stored, fingerprint=fingerprint) == "conn_other"

  def test_different_accounts_at_the_same_institution_are_not(self):
    stored = {
      "institution_id": "ins_1",
      "accounts": [{"mask": "4877", "subtype": "checking"}],
    }
    assert (
      self._find(stored, fingerprint=[{"mask": "3032", "subtype": "checking"}]) is None
    )

  def test_without_masks_accounts_are_known_by_name(self):
    stored = {
      "institution_id": "ins_1",
      "accounts": [{"mask": None, "name": "Operating", "subtype": "checking"}],
    }
    same = [{"mask": None, "name": "operating", "subtype": "checking"}]
    other = [{"mask": None, "name": "Reserve", "subtype": "checking"}]
    assert self._find(stored, fingerprint=same) == "conn_other"
    assert self._find(stored, fingerprint=other) is None

  def test_an_item_with_no_booked_accounts_is_never_a_duplicate(self):
    stored = {"institution_id": "ins_1", "accounts": []}
    assert self._find(stored, fingerprint=[]) is None

  def test_other_institutions_and_providers_are_ignored(self):
    stored = {
      "institution_id": "ins_2",
      "accounts": [{"mask": "1234", "subtype": "checking"}],
    }
    fingerprint = [{"mask": "1234", "subtype": "checking"}]
    assert self._find(stored, fingerprint=fingerprint) is None
    assert (
      self._find({}, fingerprint=fingerprint, others=[self._other(provider="mercury")])
      is None
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sync_submits_the_plaid_job():
  from robosystems.operations.providers.plaid_provider import sync_plaid_connection

  with patch(
    "robosystems.middleware.sse.dagster_monitor.submit_dagster_job_sync",
    return_value="run_7",
  ) as submit:
    outcome = await sync_plaid_connection(
      {"connection_id": "conn_1", "user_id": "usr_1"},
      {"full_rebuild": True, "sync_lock_id": "lock_1"},
      "kg_1",
    )
  assert outcome == SyncOutcome(status="dispatched", task_id="run_7")
  kwargs = submit.call_args.kwargs
  assert kwargs["job_name"] == "plaid_sync"
  config = kwargs["run_config"]["ops"]["plaid_feed"]["config"]
  assert config == {
    "graph_id": "kg_1",
    "connection_id": "conn_1",
    "user_id": "usr_1",
    "full_rebuild": True,
    "since_date": "",
    "sync_lock_id": "lock_1",
  }


@pytest.mark.unit
@pytest.mark.asyncio
class TestCleanup:
  def _session_cm(self):
    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__exit__.return_value = False
    return cm

  async def _cleanup(self, client, credentials):
    from robosystems.operations.providers.plaid_provider import cleanup_plaid_connection

    creds_row = MagicMock()
    with (
      patch(
        "robosystems.database.platform_session", side_effect=lambda: self._session_cm()
      ),
      patch(f"{MODULE}._credentials", return_value=credentials),
      patch(f"{MODULE}.plaid_client", return_value=client),
      patch(
        "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
        return_value=creds_row,
      ),
      patch(
        f"{MODULE}.purge_bank_feed_connection", return_value={"events_deleted": 4}
      ) as purge,
      patch(f"{MODULE}.record_bank_feed_purged") as purged,
    ):
      await cleanup_plaid_connection(
        {"connection_id": "conn_1", "user_id": "usr_1"}, "kg_1"
      )
    return creds_row, purge, purged

  async def test_removes_the_item_purges_and_empties_the_credentials(self):
    client = _client()
    creds_row, purge, purged = await self._cleanup(client, {"access_token": "access-1"})
    client.remove_item.assert_called_once_with("access-1")
    purge.assert_called_once_with("kg_1", provider="plaid", connection_id="conn_1")
    emptied = creds_row.update_credentials.call_args.args[0]
    assert emptied["auth_mode"] == "link" and "access_token" not in emptied
    assert purged.call_args.kwargs["purged"] == {"events_deleted": 4}

  async def test_a_failed_removal_still_purges(self):
    client = _client()
    client.remove_item.side_effect = PlaidError("gone", code="ITEM_NOT_FOUND")
    _row, purge, _purged = await self._cleanup(client, {"access_token": "access-1"})
    purge.assert_called_once()

  async def test_a_connection_that_never_linked_skips_removal(self):
    client = _client()
    _row, purge, _purged = await self._cleanup(client, {})
    client.remove_item.assert_not_called()
    purge.assert_called_once()
