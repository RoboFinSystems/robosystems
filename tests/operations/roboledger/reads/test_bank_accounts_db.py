"""``bankAccounts``: every account a feed books to or a source system calls a
bank account, each with the entity whose chart it is in."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from robosystems.adapters.bank_feed.accounts import BANK_FEED_KEY
from robosystems.models.extensions import Element, Entity, EntityTaxonomy, Taxonomy
from robosystems.operations.roboledger.reads.bank_accounts import (
  ConnectionHealth,
  list_bank_accounts,
)
from tests.ledger_entity import PARENT_ENTITY_ID, entity_account
from tests.operations.roboledger.reads.test_resolve_parent_entity_db import ext_session

__all__ = ["ext_session"]

pytestmark = pytest.mark.unit

SUB = "ent_sub"


def _seed(db):
  db.add(
    Entity(
      id=PARENT_ENTITY_ID,
      name="Cascade",
      is_parent=True,
      source="native",
      created_by="t",
    )
  )
  db.add(
    Entity(
      id=SUB,
      name="Cadence",
      is_parent=False,
      parent_entity_id=PARENT_ENTITY_ID,
      source="native",
      created_by="t",
    )
  )
  db.flush()
  chart = Taxonomy(name="Chart", taxonomy_type="chart_of_accounts", created_by="t")
  db.add(chart)
  db.flush()
  db.add(
    EntityTaxonomy(
      entity_id=PARENT_ENTITY_ID,
      taxonomy_id=chart.id,
      basis="chart_of_accounts",
      is_primary=True,
    )
  )
  rows = {
    "plaid": Element(
      name="Chase Checking ••1234",
      code="1010",
      taxonomy_id=chart.id,
      metadata_={
        BANK_FEED_KEY: {
          "provider": "plaid",
          "account_id": "acc_1",
          "account_name": "Chase Checking ••1234",
          "institution": "Chase",
          "kind": "checking",
          "connection_id": "conn_plaid",
        }
      },
      created_by="t",
    ),
    "qb": Element(
      name="Operating Account",
      code="1000",
      taxonomy_id=chart.id,
      source="quickbooks",
      external_source="quickbooks",
      external_id="35",
      connection_id="conn_qb",
      metadata_={"account_type": "Bank", "account_sub_type": "Checking"},
      created_by="t",
    ),
    "card": Element(
      name="Amex",
      code="2100",
      balance_type="credit",
      taxonomy_id=chart.id,
      source="quickbooks",
      metadata_={"account_type": "Credit Card"},
      created_by="t",
    ),
    "expense": Element(
      name="Office Supplies", code="6100", taxonomy_id=chart.id, created_by="t"
    ),
  }
  for element in rows.values():
    db.add(element)
  db.flush()
  sub_checking = entity_account(db, SUB, "Cadence Checking")
  element = db.get(Element, sub_checking)
  element.metadata_ = {
    BANK_FEED_KEY: {
      "provider": "plaid",
      "account_id": "acc_2",
      "account_name": "Cadence Checking",
      "institution": "Chase",
      "kind": "checking",
      "connection_id": "conn_plaid",
    }
  }
  db.flush()
  return {key: str(row.id) for key, row in rows.items()} | {"sub": sub_checking}


def test_every_bank_account_across_the_group_with_its_entity(ext_session):
  ids = _seed(ext_session)
  synced = datetime(2026, 10, 7, 12, tzinfo=UTC)
  response = list_bank_accounts(
    ext_session,
    connections={
      "conn_plaid": ConnectionHealth(
        status="needs_reauth", last_sync_at=synced, last_sync_status="success"
      ),
      "conn_qb": ConnectionHealth(status="active", institution="Cascade LLC"),
    },
  )
  by_id = {row.id: row for row in response.accounts}
  assert set(by_id) == {ids["plaid"], ids["qb"], ids["card"], ids["sub"]}
  assert response.total == 4

  plaid = by_id[ids["plaid"]]
  assert (plaid.source, plaid.kind, plaid.entity_name) == ("plaid", "bank", "Cascade")
  assert plaid.entity_id == PARENT_ENTITY_ID
  assert (plaid.institution, plaid.feed_account_id) == ("Chase", "acc_1")
  assert plaid.connection_status == "needs_reauth"
  assert plaid.last_sync_at == synced and plaid.last_sync_status == "success"

  qb = by_id[ids["qb"]]
  assert (qb.source, qb.connection_id, qb.institution) == (
    "quickbooks",
    "conn_qb",
    "Cascade LLC",
  )
  assert qb.feed_account_id is None and qb.connection_status == "active"

  card = by_id[ids["card"]]
  assert (card.kind, card.source, card.connection_status) == ("credit", None, None)

  sub = by_id[ids["sub"]]
  assert (sub.entity_id, sub.entity_name, sub.source) == (SUB, "Cadence", "plaid")


def test_one_entitys_accounts(ext_session):
  ids = _seed(ext_session)
  response = list_bank_accounts(ext_session, entity_id=SUB)
  assert [row.id for row in response.accounts] == [ids["sub"]]
  parent = list_bank_accounts(ext_session, entity_id=PARENT_ENTITY_ID)
  assert ids["sub"] not in {row.id for row in parent.accounts}
  assert parent.total == 3
