"""``link-bank-account`` against a real database.

Moving a feed account into a subsidiary's chart is a write across three
tables — the link on the elements, the open lines, their suggestions and
classifications — and whether the next sync then finds the moved link is
what decides if an account is created twice. Both need the real schema.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import select

from robosystems.adapters.bank_feed.accounts import (
  BANK_FEED_KEY,
  ChartRequiredError,
  account_entities,
  link_bank_accounts,
)
from robosystems.adapters.bank_feed.chart import BankAccount
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.bank_accounts import LinkBankAccountRequest
from robosystems.models.extensions import (
  Element,
  ElementTrait,
  Entity,
  EntityTaxonomy,
  Taxonomy,
  Trait,
)
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.commands import create_event_block_in_session
from robosystems.operations.roboledger.commands.bank_accounts import (
  AccountAlreadyFedError,
  FeedAccountNotFoundError,
  NotAChartAccountError,
  link_bank_account,
)
from tests.ledger_entity import PARENT_ENTITY_ID
from tests.operations.roboledger.commands.test_reconciling_items_db import (
  _skip_platform_db_checks,
  session,
)

__all__ = ["_skip_platform_db_checks", "session"]

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
CONNECTION = "conn_plaid_1"
ACCOUNT = "acc_chk"
SUB_ENTITY = "ent_test_sub"


def _link(account_id: str = ACCOUNT, connection_id: str = CONNECTION) -> dict:
  return {
    "provider": "plaid",
    "account_id": account_id,
    "account_name": "Chase Checking ••1234",
    "institution": "Chase",
    "kind": "checking",
    "connection_id": connection_id,
  }


def _parent_chart(db) -> str:
  chart = Taxonomy(
    name="Parent chart", taxonomy_type="chart_of_accounts", created_by="test"
  )
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
  db.flush()
  return str(chart.id)


def _trait(db, identifier: str) -> str:
  trait = db.execute(
    select(Trait.id).where(
      Trait.category == "elementsOfFinancialStatements", Trait.identifier == identifier
    )
  ).scalar_one_or_none()
  if trait is None:
    row = Trait(
      category="elementsOfFinancialStatements", identifier=identifier, name=identifier
    )
    db.add(row)
    db.flush()
    trait = row.id
  return str(trait)


def _element(
  db, chart_id, name, *, code=None, metadata=None, provenance=False, trait="asset"
):
  # Every element on a chart carries a qname and a trait: the envelope
  # refuses to add to a chart with one it cannot address or classify.
  element = Element(
    name=name,
    code=code,
    qname=f"coa:{code or name.replace(' ', '').replace('•', '')}",
    taxonomy_id=chart_id,
    metadata_=metadata or {},
    created_by="test",
  )
  if provenance:
    element.external_source = "plaid"
    element.external_id = ACCOUNT
    element.connection_id = CONNECTION
  db.add(element)
  db.flush()
  db.add(ElementTrait(element_id=element.id, trait_id=_trait(db, trait)))
  db.flush()
  return str(element.id)


def _subsidiary(db) -> str:
  db.add(
    Entity(
      id=SUB_ENTITY,
      name="Cadence Field Services",
      is_parent=False,
      parent_entity_id=PARENT_ENTITY_ID,
      source="native",
      created_by="test",
    )
  )
  db.flush()
  return SUB_ENTITY


def _sub_chart(db, entity_id: str) -> str:
  """The subsidiary's own chart, linked to it."""
  chart = Taxonomy(
    name=f"Chart of {entity_id}",
    taxonomy_type="chart_of_accounts",
    standard="coa-cadence",
    created_by="test",
  )
  db.add(chart)
  db.flush()
  db.add(
    EntityTaxonomy(
      entity_id=entity_id,
      taxonomy_id=chart.id,
      basis="chart_of_accounts",
      is_primary=True,
    )
  )
  db.flush()
  return str(chart.id)


def _line(db, checking, *, external_id, contra=None, classified=False, status=None):
  event, _ = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="bank_transaction",
      event_category="purchase",
      event_class="economic",
      event_action="transfer",
      resource_type="money",
      source="plaid",
      external_id=external_id,
      occurred_at=datetime(2026, 7, 9),
      amount=-4200,
      resource_element_id=checking,
      apply_handlers=False,
      metadata={
        "connection_id": CONNECTION,
        "account_id": ACCOUNT,
        "suggested_account_name": "Office Supplies",
        "suggested_element_id": contra,
        **(
          {"classified_element_id": contra, "classified_by": "ai", "basis": "rule"}
          if classified
          else {}
        ),
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  if classified:
    event.status = "classified"
  if status:
    event.status = status
  db.flush()
  return str(event.id)


def _seed(db):
  """The parent's chart with the feed's checking account and an expense; a
  subsidiary with its own chart holding only a rent account."""
  parent_chart = _parent_chart(db)
  checking = _element(
    db,
    parent_chart,
    "Chase Checking ••1234",
    metadata={BANK_FEED_KEY: _link()},
    provenance=True,
  )
  supplies = _element(db, parent_chart, "Office Supplies", code="6100", trait="expense")
  sub = _subsidiary(db)
  rent = _element(db, _sub_chart(db, sub), "Rent", code="6500", trait="expense")
  return parent_chart, checking, supplies, sub, rent


def test_moving_an_account_to_a_subsidiary_creates_it_there_and_moves_open_lines(
  session,
):
  _parent_chart_id, checking, supplies, sub, _rent = _seed(session)
  captured = _line(session, checking, external_id="plaid_txn_a", contra=supplies)
  classified = _line(
    session, checking, external_id="plaid_txn_b", contra=supplies, classified=True
  )
  posted = _line(
    session, checking, external_id="plaid_txn_c", contra=supplies, status="committed"
  )

  result = link_bank_account(
    session,
    LinkBankAccountRequest(connection_id=CONNECTION, account_id=ACCOUNT, entity_id=sub),
    "user_test",
  )

  assert result.account_created and result.changed
  assert result.entity_id == sub and result.previous_element_id == checking
  assert (result.events_repointed, result.events_unclassified) == (2, 1)

  new = session.get(Element, result.element_id)
  assert new.metadata_[BANK_FEED_KEY]["account_id"] == ACCOUNT
  assert new.name == "Chase Checking ••1234"
  # The subsidiary's chart has its own qname prefix.
  assert str(new.qname).startswith("coa-cadence:")
  assert account_entities(session, [new.id], parent_id=PARENT_ENTITY_ID) == {
    str(new.id): sub
  }
  # The feed's provenance moved with the link; the old account is an ordinary
  # account of the parent's now.
  assert (new.external_source, new.external_id) == ("plaid", ACCOUNT)
  old = session.get(Element, checking)
  assert BANK_FEED_KEY not in old.metadata_
  assert old.external_source is None and old.external_id is None

  a = session.get(Event, captured)
  assert a.resource_element_id == result.element_id and a.entity_id == sub
  # "Office Supplies" is not on the subsidiary's chart: the name stays, the
  # resolved id goes.
  assert a.metadata_["suggested_account_name"] == "Office Supplies"
  assert "suggested_element_id" not in a.metadata_
  b = session.get(Event, classified)
  assert b.status == "captured" and b.entity_id == sub
  assert "classified_element_id" not in b.metadata_ and "basis" not in b.metadata_
  c = session.get(Event, posted)
  assert c.resource_element_id == checking and c.entity_id == PARENT_ENTITY_ID


def test_a_suggestion_present_on_the_new_chart_is_resolved_there(session):
  _chart, checking, supplies, sub, rent = _seed(session)
  # Same name as the parent's account, its own code: qnames are unique
  # across the whole schema, not per chart.
  sub_supplies = _element(
    session,
    session.get(Element, rent).taxonomy_id,
    "Office Supplies",
    code="6110",
    trait="expense",
  )
  captured = _line(session, checking, external_id="plaid_txn_a", contra=supplies)
  classified = _line(
    session, checking, external_id="plaid_txn_b", contra=supplies, classified=True
  )

  result = link_bank_account(
    session,
    LinkBankAccountRequest(connection_id=CONNECTION, account_id=ACCOUNT, entity_id=sub),
    "user_test",
  )

  assert session.get(Event, captured).metadata_["suggested_element_id"] == sub_supplies
  # The classification named the parent's account, so it is dropped even
  # though a same-named account exists: nobody chose that one.
  b = session.get(Event, classified)
  assert b.status == "captured" and result.events_unclassified == 1
  assert b.metadata_["suggested_element_id"] == sub_supplies


def test_the_next_sync_finds_the_moved_link_and_creates_nothing(session):
  _chart, _checking, _supplies, sub, _rent = _seed(session)
  moved = link_bank_account(
    session,
    LinkBankAccountRequest(connection_id=CONNECTION, account_id=ACCOUNT, entity_id=sub),
    "user_test",
  )
  session.flush()

  result = link_bank_accounts(
    session,
    [
      BankAccount(
        ACCOUNT, "Chase Checking ••1234", "checking", "asset", "debit", "Chase"
      )
    ],
    provider="plaid",
    connection_id=CONNECTION,
    created_by="user_test",
  )
  assert result.links == {ACCOUNT: moved.element_id}
  assert (result.linked, result.created) == (1, 0)


def test_linking_to_an_existing_account_moves_the_link_without_creating(session):
  _chart, checking, _supplies, sub, rent = _seed(session)
  result = link_bank_account(
    session,
    LinkBankAccountRequest(
      connection_id=CONNECTION, account_id=ACCOUNT, element_id=rent
    ),
    "user_test",
  )
  assert not result.account_created and result.element_id == rent
  assert result.entity_id == sub
  assert (
    session.get(Element, rent).metadata_[BANK_FEED_KEY]["connection_id"] == CONNECTION
  )
  assert BANK_FEED_KEY not in session.get(Element, checking).metadata_
  # The tenant's own account never takes the feed's provenance.
  assert session.get(Element, rent).external_source is None


def test_the_same_account_is_a_no_op(session):
  _chart, checking, _supplies, _sub, _rent = _seed(session)
  result = link_bank_account(
    session,
    LinkBankAccountRequest(
      connection_id=CONNECTION, account_id=ACCOUNT, element_id=checking
    ),
    "user_test",
  )
  assert not result.changed and result.element_id == checking
  assert (
    session.get(Element, checking).metadata_[BANK_FEED_KEY]["account_id"] == ACCOUNT
  )


def test_an_account_another_connection_feeds_is_refused(session):
  chart, _checking, _supplies, _sub, _rent = _seed(session)
  other = _element(
    session,
    chart,
    "Mercury Checking",
    metadata={
      BANK_FEED_KEY: {**_link("merc_1", "conn_mercury"), "provider": "mercury"}
    },
  )
  with pytest.raises(AccountAlreadyFedError):
    link_bank_account(
      session,
      LinkBankAccountRequest(
        connection_id=CONNECTION, account_id=ACCOUNT, element_id=other
      ),
      "user_test",
    )


def test_an_entity_without_a_chart_is_refused(session):
  _element(
    session,
    _parent_chart(session),
    "Chase Checking ••1234",
    metadata={BANK_FEED_KEY: _link()},
  )
  sub = _subsidiary(session)
  with pytest.raises(ChartRequiredError):
    link_bank_account(
      session,
      LinkBankAccountRequest(
        connection_id=CONNECTION, account_id=ACCOUNT, entity_id=sub
      ),
      "user_test",
    )


def test_an_account_outside_the_named_entitys_chart_is_refused(session):
  _chart, _checking, _supplies, _sub, rent = _seed(session)
  with pytest.raises(NotAChartAccountError):
    link_bank_account(
      session,
      LinkBankAccountRequest(
        connection_id=CONNECTION,
        account_id=ACCOUNT,
        element_id=rent,
        entity_id=PARENT_ENTITY_ID,
      ),
      "user_test",
    )


def test_an_unknown_feed_account_is_refused(session):
  _seed(session)
  with pytest.raises(FeedAccountNotFoundError):
    link_bank_account(
      session,
      LinkBankAccountRequest(
        connection_id=CONNECTION, account_id="acc_nope", entity_id=SUB_ENTITY
      ),
      "user_test",
    )


def test_a_pair_books_on_its_receiving_leg(session):
  _chart, checking, _supplies, sub, _rent = _seed(session)
  savings = _element(
    session,
    session.execute(select(Taxonomy.id)).scalars().first(),
    "Chase Savings ••5678",
    metadata={BANK_FEED_KEY: _link("acc_sav")},
  )
  pair, _ = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="internal_transfer",
      event_category="treasury",
      event_class="economic",
      event_action="move",
      resource_type="money",
      source="plaid",
      external_id="plaid_xfer_t_in_t_out",
      occurred_at=datetime(2026, 7, 9),
      amount=50000,
      resource_element_id=savings,
      apply_handlers=False,
      metadata={
        "connection_id": CONNECTION,
        "kind": "internal_transfer",
        "from_element_id": checking,
        "to_element_id": savings,
        "legs": ["t_out", "t_in"],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  result = link_bank_account(
    session,
    LinkBankAccountRequest(connection_id=CONNECTION, account_id=ACCOUNT, entity_id=sub),
    "user_test",
  )
  moved = session.get(Event, str(pair.id))
  assert moved.metadata_["from_element_id"] == result.element_id
  assert moved.metadata_["to_element_id"] == savings
  # The receiving account stayed on the parent, so the pair still books
  # there — and it is now a pair across two entities, which is reported.
  assert moved.entity_id == PARENT_ENTITY_ID
  assert result.pairs_across_entities == 1

  # Move the other leg too: the pair is whole again, on the subsidiary.
  whole = link_bank_account(
    session,
    LinkBankAccountRequest(
      connection_id=CONNECTION, account_id="acc_sav", entity_id=sub
    ),
    "user_test",
  )
  assert whole.pairs_across_entities == 0
  followed = session.get(Event, str(pair.id))
  assert followed.entity_id == sub
  assert followed.metadata_["to_element_id"] == whole.element_id
  assert followed.resource_element_id == whole.element_id
