"""``create-entity`` adds an entity to the group and ``update-entity`` edits
any entity of it. Runs the commands' real SQL against a tenant schema."""

from __future__ import annotations

from datetime import date

import pytest

from robosystems.config.constants import ReportingStyleConstants
from robosystems.models.api.extensions.entity import (
  CreateEntityRequest,
  UpdateEntityRequest,
)
from robosystems.models.extensions import Entity
from robosystems.operations.roboledger.commands.entity import (
  EntityHierarchyError,
  EntityMembershipError,
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
  GroupMember,
  group_members,
  membership_window,
  resolve_entity_id,
)
from robosystems.operations.roboledger.reads.entity import get_entity, list_entities

pytestmark = pytest.mark.unit


class TestConsolidationFields:
  @pytest.mark.parametrize(
    ("share", "method"),
    [
      (100, "full"),
      (70, "full"),
      (50, "full"),
      (40, "equity"),
      (20, "equity"),
      (10, "cost"),
    ],
  )
  def test_the_share_implies_the_method(self, two_entities, share, method):
    created = create_entity(
      two_entities.session,
      CreateEntityRequest(name="Pier Market Kiosk LLC", ownership_pct=share),
      created_by="usr_1",
    )
    assert created.consolidation_method == method
    assert created.entity_role == "operating"
    assert created.joined_on is None and created.left_on is None

  def test_an_unrecorded_share_is_summed_in_full(self, two_entities):
    created = create_entity(
      two_entities.session, CreateEntityRequest(name="Cedar Row LLC"), created_by="u"
    )
    assert created.consolidation_method == "full"

  def test_a_person_overrides_the_method_control_is_not_the_percentage(
    self, two_entities
  ):
    created = create_entity(
      two_entities.session,
      CreateEntityRequest(
        name="Managed Fund LP",
        ownership_pct=45,
        consolidation_method="full",
        joined_on=date(2026, 3, 14),
      ),
      created_by="u",
    )
    assert created.consolidation_method == "full"
    assert created.joined_on == date(2026, 3, 14)

  def test_the_group_parent_is_operating_with_no_method(self, tenant_session):
    # A graph's first entity becomes its group parent, so neither holds on it.
    for body in (
      CreateEntityRequest(name="Harbor Holdings", entity_role="consolidation"),
      CreateEntityRequest(name="Harbor Holdings", consolidation_method="full"),
    ):
      with pytest.raises(EntityHierarchyError):
        create_entity(tenant_session, body, created_by="u")
    created = create_entity(
      tenant_session, CreateEntityRequest(name="Harbor Holdings"), created_by="u"
    )
    assert (created.consolidation_method, created.entity_role) == ("none", "operating")

  def test_the_consolidation_entity_takes_none(self, two_entities):
    created = create_entity(
      two_entities.session,
      CreateEntityRequest(
        name="Consolidation Adjustments", entity_role="consolidation"
      ),
      created_by="u",
    )
    assert (created.consolidation_method, created.entity_role) == (
      "none",
      "consolidation",
    )
    with pytest.raises(EntityHierarchyError):
      create_entity(
        two_entities.session,
        CreateEntityRequest(
          name="Eliminations", entity_role="consolidation", consolidation_method="full"
        ),
        created_by="u",
      )

  def test_a_new_share_re_derives_the_method_unless_both_are_given(self, two_entities):
    session, sub = two_entities.session, two_entities.sub
    updated = update_entity(
      session, UpdateEntityRequest(entity_id=sub.id, ownership_pct=30), created_by="u"
    )
    assert updated.consolidation_method == "equity"
    updated = update_entity(
      session,
      UpdateEntityRequest(
        entity_id=sub.id, ownership_pct=30, consolidation_method="full"
      ),
      created_by="u",
    )
    assert updated.consolidation_method == "full"

  def test_marking_an_entity_consolidation_sets_its_method_to_none(self, two_entities):
    session, sub = two_entities.session, two_entities.sub
    updated = update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, entity_role="consolidation"),
      created_by="u",
    )
    assert (updated.consolidation_method, updated.entity_role) == (
      "none",
      "consolidation",
    )
    with pytest.raises(EntityHierarchyError):
      update_entity(
        session,
        UpdateEntityRequest(entity_id=sub.id, consolidation_method="equity"),
        created_by="u",
      )

  def test_the_group_parent_refuses_a_role_and_a_method(self, two_entities):
    session = two_entities.session
    for body in (
      UpdateEntityRequest(entity_role="consolidation"),
      UpdateEntityRequest(consolidation_method="full"),
    ):
      with pytest.raises(EntityHierarchyError):
        update_entity(session, body, created_by="u")
    updated = update_entity(
      session, UpdateEntityRequest(consolidation_method="none"), created_by="u"
    )
    assert updated.consolidation_method == "none"

  def test_membership_ends_after_it_starts(self, two_entities):
    session, sub = two_entities.session, two_entities.sub
    updated = update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, joined_on=date(2026, 2, 1)),
      created_by="u",
    )
    assert updated.joined_on == date(2026, 2, 1)
    with pytest.raises(EntityMembershipError):
      update_entity(
        session,
        UpdateEntityRequest(entity_id=sub.id, left_on=date(2026, 1, 31)),
        created_by="u",
      )
    updated = update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, left_on=date(2026, 12, 31)),
      created_by="u",
    )
    assert updated.left_on == date(2026, 12, 31)
    listed = {e.id: e for e in list_entities(session)}
    assert listed[sub.id].left_on == date(2026, 12, 31)


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

  def test_a_rename_onto_a_siblings_ticker_is_refused(self, two_entities):
    """The rule holds on update too, with the same normalization."""
    session, parent, sub = two_entities.session, two_entities.parent, two_entities.sub
    parent.ticker = "HH"
    session.flush()

    with pytest.raises(EntityTickerTakenError):
      update_entity(
        session, UpdateEntityRequest(entity_id=sub.id, ticker="hh"), created_by="u"
      )
    renamed = update_entity(
      session, UpdateEntityRequest(entity_id=sub.id, ticker="mcx"), created_by="u"
    )
    assert renamed.ticker == "MCX"
    # Keeping one's own ticker is not a collision with oneself.
    kept = update_entity(
      session, UpdateEntityRequest(entity_id=sub.id, ticker="MCX"), created_by="u"
    )
    assert kept.ticker == "MCX"

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


class TestGroupMembers:
  def test_full_members_then_the_adjustments_column(self, two_entities):
    session, parent, sub = two_entities.session, two_entities.parent, two_entities.sub
    create_entity(
      session,
      CreateEntityRequest(name="Pier Market Kiosk LLC", ownership_pct=40),
      created_by="u",
    )
    create_entity(
      session, CreateEntityRequest(name="Stake Co", ownership_pct=10), created_by="u"
    )
    adjustments = create_entity(
      session,
      CreateEntityRequest(
        name="Consolidation Adjustments", entity_role="consolidation"
      ),
      created_by="u",
    )

    combined = group_members(session, parent.id, consolidated=False)
    consolidated = group_members(session, parent.id, consolidated=True)

    assert [m.id for m in combined] == [parent.id, sub.id]
    assert [m.id for m in consolidated] == [parent.id, sub.id, adjustments.id]
    assert consolidated[-1].role == "consolidation"

  def test_membership_is_by_month(self):
    member = GroupMember("ent", "operating", date(2026, 3, 14), date(2026, 9, 20))
    assert not member.is_member_for(date(2026, 2, 1), date(2026, 2, 28))
    assert member.is_member_for(date(2026, 3, 1), date(2026, 3, 31))
    assert member.is_member_for(date(2026, 9, 1), date(2026, 9, 30))
    assert not member.is_member_for(date(2026, 10, 1), date(2026, 10, 31))
    always = GroupMember("ent", "operating", None, None)
    assert always.is_member_for(date(2000, 1, 1), date(2000, 1, 31))

  def test_a_wide_window_holds_only_the_member_months(self):
    member = GroupMember("ent", "operating", date(2026, 3, 14), date(2026, 9, 20))
    year = (date(2026, 1, 1), date(2026, 12, 31))
    assert membership_window(member, *year) == (date(2026, 3, 1), date(2026, 9, 30))
    assert membership_window(member, date(2026, 10, 1), date(2026, 12, 31)) is None
    q3 = (date(2026, 7, 1), date(2026, 9, 30))
    assert membership_window(member, *q3) == q3


class TestConsolidationFollowUps:
  def test_leaving_the_consolidation_role_re_derives_the_method(self, two_entities):
    """Typed back to operating, an entity takes the method its share implies
    instead of staying at none and out of every sum."""
    session, sub = two_entities.session, two_entities.sub
    update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, entity_role="consolidation"),
      created_by="u",
    )
    back = update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, entity_role="operating"),
      created_by="u",
    )
    assert (back.entity_role, back.consolidation_method) == ("operating", "full")
    update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, entity_role="consolidation"),
      created_by="u",
    )
    back = update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, entity_role="operating", ownership_pct=40),
      created_by="u",
    )
    assert back.consolidation_method == "equity"

  def test_an_explicit_null_reopens_membership(self, two_entities):
    session, sub = two_entities.session, two_entities.sub
    left = update_entity(
      session,
      UpdateEntityRequest(entity_id=sub.id, left_on=date(2026, 6, 30)),
      created_by="u",
    )
    assert left.left_on == date(2026, 6, 30)
    reopened = update_entity(
      session, UpdateEntityRequest(entity_id=sub.id, left_on=None), created_by="u"
    )
    assert reopened.left_on is None
