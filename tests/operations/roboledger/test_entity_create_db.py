"""``create-entity`` adds an entity to the group and ``update-entity`` edits
any entity of it. Runs the commands' real SQL against a tenant schema."""

from __future__ import annotations

import pytest

from robosystems.config.constants import ReportingStyleConstants
from robosystems.models.api.extensions.entity import (
  CreateEntityRequest,
  UpdateEntityRequest,
)
from robosystems.models.extensions import Entity
from robosystems.operations.roboledger.commands.entity import (
  EntityHierarchyError,
  EntityTickerTakenError,
  ParentEntityNotFoundError,
  create_entity,
  update_entity,
)
from robosystems.operations.roboledger.commands.reporting_style import (
  ReportingStyleInvalidError,
)
from robosystems.operations.roboledger.entity_scope import (
  EntityNotInGraphError,
  resolve_entity_id,
)
from robosystems.operations.roboledger.reads.entity import get_entity, list_entities

pytestmark = pytest.mark.unit


class TestCreateSubsidiary:
  def test_a_new_entity_is_a_subsidiary_of_the_group_parent(self, two_entities):
    session, parent = two_entities.session, two_entities.parent
    parent.fiscal_year_end = "06-30"
    session.flush()

    created = create_entity(
      session,
      CreateEntityRequest(name="Cedar Row LLC", entity_type="llc", ownership_pct=100),
      created_by="usr_1",
    )

    assert created.is_parent is False
    assert created.parent_entity_id == parent.id
    assert created.ownership_pct == 100.0
    assert created.source == "native"
    assert created.status == "active"
    assert created.legal_name == "Cedar Row LLC"
    assert created.reporting_style_id == ReportingStyleConstants.LLC_STYLE_ID
    # The group has one fiscal cadence; a sub that names no year end follows it.
    assert created.fiscal_year_end == "06-30"
    assert created.ticker == "CRL"
    assert created.uri == f"https://robosystems.ai/entities#{created.id}"
    # Nothing moved: the parent is still the group parent.
    assert resolve_entity_id(session) == parent.id
    assert resolve_entity_id(session, created.id) == created.id

  def test_a_sub_group_nests_under_a_named_subsidiary(self, two_entities):
    session, sub = two_entities.session, two_entities.sub

    created = create_entity(
      session,
      CreateEntityRequest(
        name="Maple Court Parking LLC",
        parent_entity_id=sub.id,
        ownership_pct=51.5,
        fiscal_year_end="12-31",
      ),
      created_by="usr_1",
    )

    assert created.parent_entity_id == sub.id
    assert created.ownership_pct == 51.5
    assert created.fiscal_year_end == "12-31"
    assert created.reporting_style_id == ReportingStyleConstants.DEFAULT_STYLE_ID

  def test_a_parent_must_be_one_of_this_graphs_own_entities(self, two_entities):
    session, linked = two_entities.session, two_entities.linked

    with pytest.raises(EntityNotInGraphError):
      create_entity(
        session,
        CreateEntityRequest(name="Stray LLC", parent_entity_id="ent_nowhere"),
        created_by="usr_1",
      )
    # A linked counterparty is another graph's company, never a parent here.
    with pytest.raises(EntityNotInGraphError):
      create_entity(
        session,
        CreateEntityRequest(name="Stray LLC", parent_entity_id=linked.id),
        created_by="usr_1",
      )

  def test_the_first_entity_of_a_graph_becomes_its_group_parent(self, tenant_session):
    with pytest.raises(EntityHierarchyError):
      create_entity(
        tenant_session,
        CreateEntityRequest(name="Harbor Holdings", ownership_pct=100),
        created_by="usr_1",
      )

    created = create_entity(
      tenant_session,
      CreateEntityRequest(name="Harbor Holdings", entity_type="corporation"),
      created_by="usr_1",
    )

    assert created.is_parent is True
    assert created.parent_entity_id is None
    assert created.ownership_pct is None
    assert resolve_entity_id(tenant_session) == created.id

    # And the next one hangs under it.
    sub = create_entity(
      tenant_session, CreateEntityRequest(name="Harbor Pier LLC"), created_by="usr_1"
    )
    assert sub.parent_entity_id == created.id
    assert resolve_entity_id(tenant_session) == created.id


class TestTicker:
  def test_an_explicit_ticker_is_kept_uppercase_and_must_be_free(self, two_entities):
    session, parent = two_entities.session, two_entities.parent
    parent.ticker = "HH"
    session.flush()

    created = create_entity(
      session, CreateEntityRequest(name="Pier One LLC", ticker="pier"), created_by="u"
    )
    assert created.ticker == "PIER"

    with pytest.raises(EntityTickerTakenError):
      create_entity(
        session, CreateEntityRequest(name="Other LLC", ticker="hh"), created_by="u"
      )

  def test_a_derived_ticker_steps_past_a_sibling_with_the_same_initials(
    self, two_entities
  ):
    session = two_entities.session

    first = create_entity(
      session, CreateEntityRequest(name="Maple Court LLC"), created_by="u"
    )
    second = create_entity(
      session, CreateEntityRequest(name="Mill Creek LLC"), created_by="u"
    )

    assert first.ticker == "MCL"
    assert second.ticker == "MCL2"

  def test_a_single_word_name_takes_its_first_letters(self, two_entities):
    created = create_entity(
      two_entities.session, CreateEntityRequest(name="Driftline"), created_by="u"
    )
    assert created.ticker == "DRIF"


class TestReportingStyle:
  def test_a_named_style_must_render_in_this_graph(self, two_entities):
    # A bare tenant has no Style structures at all.
    with pytest.raises(ReportingStyleInvalidError):
      create_entity(
        two_entities.session,
        CreateEntityRequest(
          name="Styled LLC",
          reporting_style_id=ReportingStyleConstants.PARTNERSHIP_STYLE_ID,
        ),
        created_by="u",
      )

  def test_the_style_follows_the_legal_form(self, two_entities):
    session = two_entities.session
    partnership = create_entity(
      session,
      CreateEntityRequest(name="Pier Partners LP", entity_type="partnership"),
      created_by="u",
    )
    llc = create_entity(
      session,
      CreateEntityRequest(name="Pier LLC", entity_type="Limited_Liability_Company"),
      created_by="u",
    )
    corp = create_entity(session, CreateEntityRequest(name="Pier Inc"), created_by="u")
    assert (
      partnership.reporting_style_id == ReportingStyleConstants.PARTNERSHIP_STYLE_ID
    )
    assert llc.reporting_style_id == ReportingStyleConstants.LLC_STYLE_ID
    assert corp.reporting_style_id == ReportingStyleConstants.DEFAULT_STYLE_ID


class TestUpdateAnyEntity:
  def test_update_defaults_to_the_group_parent(self, two_entities):
    session, parent, sub = two_entities.session, two_entities.parent, two_entities.sub

    updated = update_entity(
      session, UpdateEntityRequest(name="Harbor Holdings Inc"), created_by="u"
    )

    assert updated.id == parent.id
    assert session.get(Entity, parent.id).name == "Harbor Holdings Inc"
    assert session.get(Entity, sub.id).name == "Maple Court LLC"

  def test_update_names_a_subsidiary(self, two_entities):
    session, parent, sub = two_entities.session, two_entities.parent, two_entities.sub

    updated = update_entity(
      session,
      UpdateEntityRequest(
        entity_id=sub.id, name="Maple Court Holdings LLC", ownership_pct=80
      ),
      created_by="u",
    )

    assert updated.id == sub.id
    assert updated.ownership_pct == 80.0
    assert session.get(Entity, parent.id).name == "Harbor Holdings"

  def test_ownership_is_refused_on_the_group_parent(self, two_entities):
    session, parent = two_entities.session, two_entities.parent

    with pytest.raises(EntityHierarchyError):
      update_entity(session, UpdateEntityRequest(ownership_pct=50), created_by="u")
    with pytest.raises(EntityHierarchyError):
      update_entity(
        session,
        UpdateEntityRequest(entity_id=parent.id, ownership_pct=50),
        created_by="u",
      )

  def test_an_entity_id_outside_the_graph_is_refused(self, two_entities):
    session, linked = two_entities.session, two_entities.linked

    with pytest.raises(EntityNotInGraphError):
      update_entity(
        session, UpdateEntityRequest(entity_id="ent_nowhere", name="x"), created_by="u"
      )
    with pytest.raises(EntityNotInGraphError):
      update_entity(
        session, UpdateEntityRequest(entity_id=linked.id, name="x"), created_by="u"
      )

  def test_only_entity_id_is_an_empty_update(self, two_entities):
    with pytest.raises(ValueError, match="No fields"):
      update_entity(
        two_entities.session,
        UpdateEntityRequest(entity_id=two_entities.sub.id),
        created_by="u",
      )

  def test_a_graph_with_no_entity_has_nothing_to_update(self, tenant_session):
    with pytest.raises(ParentEntityNotFoundError):
      update_entity(tenant_session, UpdateEntityRequest(name="x"), created_by="u")


class TestReads:
  def test_get_entity_names_one_or_defaults_to_the_parent(self, two_entities):
    session, parent, sub = two_entities.session, two_entities.parent, two_entities.sub

    assert get_entity(session).id == parent.id
    assert get_entity(session, sub.id).id == sub.id
    with pytest.raises(EntityNotInGraphError):
      get_entity(session, two_entities.linked.id)

  def test_get_entity_is_none_on_a_graph_with_no_entity(self, tenant_session):
    assert get_entity(tenant_session) is None

  def test_the_list_carries_the_hierarchy(self, two_entities):
    session, parent, sub = two_entities.session, two_entities.parent, two_entities.sub
    create_entity(
      session,
      CreateEntityRequest(name="Cedar Row LLC", ownership_pct=75),
      created_by="u",
    )

    rows = {r.name: r for r in list_entities(session)}

    assert rows["Harbor Holdings"].parent_entity_id is None
    assert rows["Maple Court LLC"].parent_entity_id == parent.id
    assert rows["Cedar Row LLC"].parent_entity_id == parent.id
    assert rows["Cedar Row LLC"].ownership_pct == 75.0
    assert rows["Sender Co"].source == "linked"
    assert sub.id in {r.id for r in rows.values()}
