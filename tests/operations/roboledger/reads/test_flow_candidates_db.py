"""The flows a line may be tagged with, on real Postgres: the leaf rows of
the entity's cash flow statement, which is the set ``resolve_flows`` accepts,
and every traited flow when no statement is composed."""

from __future__ import annotations

import pytest

from robosystems.config.constants import ReportingStyleConstants
from robosystems.models.api.extensions.entity import CreateEntityRequest
from robosystems.models.extensions import (
  Association,
  Element,
  ElementTrait,
  ReportingStyleNetwork,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.event_block.python_handlers.bank_feed import resolve_flows
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.roboledger.commands.entity import create_entity
from robosystems.operations.roboledger.reads.taxonomies import list_flow_candidates
from robosystems.operations.roboledger.reports.network_picker import (
  entity_taggable_flows,
  load_entity_reporting_style,
)
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

NOTE_PROCEEDS = "rs-gaap:ProceedsFromShortTermDebt"
CONTRIBUTIONS = "rs-gaap:ProceedsFromContributedCapital"
CAPEX = "rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment"
SUBTOTAL = "rs-gaap:NetCashProvidedByUsedInFinancingActivities"


def _element(session, qname: str, name: str) -> Element:
  element = Element(
    name=name,
    qname=qname,
    balance_type="debit",
    period_type="duration",
    created_by="test",
  )
  session.add(element)
  session.flush()
  return element


def _flow(session, qname: str, name: str, activity: str) -> Element:
  trait = (
    session.query(Trait)
    .filter(Trait.category == "activityType", Trait.identifier == activity)
    .first()
  )
  if trait is None:
    trait = Trait(category="activityType", identifier=activity)
    session.add(trait)
    session.flush()
  element = _element(session, qname, name)
  session.add(ElementTrait(element_id=element.id, trait_id=trait.id, is_primary=True))
  session.flush()
  return element


@pytest.fixture()
def flows(two_entities):
  t = two_entities
  entity_account(t.session, t.parent.id, "Checking")
  t.note = _flow(
    t.session, NOTE_PROCEEDS, "Proceeds from Short-Term Debt", "financingActivity"
  )
  t.contribution = _flow(
    t.session, CONTRIBUTIONS, "Proceeds from Contributed Capital", "financingActivity"
  )
  t.capex = _flow(
    t.session,
    CAPEX,
    "Payments to Acquire Property, Plant, and Equipment",
    "investingActivity",
  )
  t.session.commit()
  return t


def _compose_statement(t, rows: list[Element], *, style: str | None = None) -> None:
  """A cash flow network for ``style`` (the parent's by default) presenting
  ``rows`` under one subtotal, each footing it through a calc arc."""
  session = t.session
  taxonomy = Taxonomy(
    name="pres", taxonomy_type="reporting_standard", created_by="test"
  )
  session.add(taxonomy)
  session.flush()
  network = Structure(
    name="Cash Flow — Indirect",
    block_type="cash_flow_statement",
    taxonomy_id=taxonomy.id,
    created_by="test",
  )
  session.add(network)
  session.flush()
  subtotal = session.query(Element).filter(Element.qname == SUBTOTAL).first()
  if subtotal is None:
    subtotal = _element(session, SUBTOTAL, "Net Cash from Financing")
  for order, leaf in enumerate(rows, start=1):
    for association_type in ("presentation", "calculation"):
      session.add(
        Association(
          structure_id=network.id,
          from_element_id=subtotal.id,
          to_element_id=leaf.id,
          association_type=association_type,
          order_value=order,
          weight=1.0,
          created_by="test",
        )
      )
  session.add(
    ReportingStyleNetwork(
      reporting_style_id=style or load_entity_reporting_style(session, t.parent.id),
      statement_type="cash_flow_statement",
      network_id=network.id,
    )
  )
  session.commit()


def test_the_statements_leaf_rows_by_section_then_name(flows):
  """Only the rows the statement presents, investing before financing and by
  name within each; the subtotal they foot and a traited flow with no row
  stay out."""
  _compose_statement(flows, [flows.note, flows.capex])

  candidates = list_flow_candidates(
    flows.session, load_entity_reporting_style(flows.session, flows.parent.id)
  )

  assert [(c.qname, c.activity) for c in candidates] == [
    (CAPEX, "investing"),
    (NOTE_PROCEEDS, "financing"),
  ]
  assert candidates[1].element_id == str(flows.note.id)
  assert candidates[1].name == "Proceeds from Short-Term Debt"


def test_with_no_statement_every_traited_flow_is_offered(flows):
  """A Style with no cash flow network composed: the write path checks by
  trait alone, so the picker offers every investing or financing flow, and
  never an account."""
  candidates = list_flow_candidates(
    flows.session, load_entity_reporting_style(flows.session, flows.parent.id)
  )

  assert [c.qname for c in candidates] == [CAPEX, CONTRIBUTIONS, NOTE_PROCEEDS]


def test_a_flow_carrying_both_activities_is_listed_once_under_investing(flows):
  session = flows.session
  investing = (
    session.query(Trait)
    .filter(Trait.category == "activityType", Trait.identifier == "investingActivity")
    .one()
  )
  session.add(ElementTrait(element_id=flows.note.id, trait_id=investing.id))
  session.commit()

  candidates = list_flow_candidates(
    session, load_entity_reporting_style(session, flows.parent.id)
  )

  assert [(c.qname, c.activity) for c in candidates] == [
    (CAPEX, "investing"),
    (NOTE_PROCEEDS, "investing"),
    (CONTRIBUTIONS, "financing"),
  ]


def test_the_consolidation_entity_carries_every_group_entitys_rows(flows):
  """It offsets a member's flow in the member's form, so its lines take a
  row the parent's statement lacks; an operating entity's take only its own."""
  session = flows.session
  flows.sub.reporting_style_id = ReportingStyleConstants.LLC_STYLE_ID
  _compose_statement(flows, [flows.note])
  _compose_statement(
    flows, [flows.contribution], style=ReportingStyleConstants.LLC_STYLE_ID
  )
  adjustments = create_entity(
    session,
    CreateEntityRequest(name="Consolidation Adjustments", entity_role="consolidation"),
    created_by="u",
  )

  assert entity_taggable_flows(session, flows.parent.id) == {NOTE_PROCEEDS}
  assert entity_taggable_flows(session, flows.sub.id) == {CONTRIBUTIONS}
  assert entity_taggable_flows(session, adjustments.id) == {
    NOTE_PROCEEDS,
    CONTRIBUTIONS,
  }
  offered = list_flow_candidates(
    session,
    load_entity_reporting_style(session, adjustments.id),
    entity_id=adjustments.id,
  )
  assert {c.qname for c in offered} == {NOTE_PROCEEDS, CONTRIBUTIONS}
  assert resolve_flows(session, {CONTRIBUTIONS}, entity_id=adjustments.id)
  with pytest.raises(HandlerMetadataValidationError):
    resolve_flows(session, {CONTRIBUTIONS}, entity_id=flows.parent.id)
  # A flow no entity of the group presents stays refused.
  with pytest.raises(HandlerMetadataValidationError):
    resolve_flows(session, {CAPEX}, entity_id=adjustments.id)
