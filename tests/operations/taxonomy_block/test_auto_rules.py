"""Tests for structural auto-rule emission."""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.taxonomy_block.auto_rules import emit_auto_rules


def _fake_taxonomy(
  taxonomy_type: str = "custom_ontology",
  *,
  parent_taxonomy_id: str | None = None,
) -> MagicMock:
  t = MagicMock()
  t.id = "tax_1"
  t.taxonomy_type = taxonomy_type
  t.parent_taxonomy_id = parent_taxonomy_id
  return t


def _fake_structure(sid: str, concept_arrangement: str | None = None) -> MagicMock:
  s = MagicMock()
  s.id = sid
  s.concept_arrangement = concept_arrangement
  return s


def _calc_row(
  parent_id: str,
  child_id: str,
  *,
  parent_qname: str,
  child_qname: str,
  weight: float = 1.0,
  order_value: float | None = None,
) -> MagicMock:
  row = MagicMock()
  row.from_element_id = parent_id
  row.to_element_id = child_id
  row.weight = weight
  row.order_value = order_value
  row.parent_qname = parent_qname
  row.parent_name = parent_qname.split(":")[-1]
  row.child_qname = child_qname
  row.child_name = child_qname.split(":")[-1]
  return row


class TestEmitAutoRules:
  def test_no_structures_emits_taxonomy_level_rules_only(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy()
    emit_auto_rules(session, taxonomy, [], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    patterns = {r.rule_check_kind for r in added}
    assert patterns == {"UniqueQNameInTaxonomy"}
    session.flush.assert_called_once()

  def test_structure_emits_three_per_structure_rules(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy()
    s1 = _fake_structure("str_1")
    emit_auto_rules(session, taxonomy, [s1], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    patterns = {r.rule_check_kind for r in added}
    assert {"NoCycles", "NoOrphanArcs", "ParentBeforeChild"}.issubset(patterns)

  def test_two_structures_emit_six_structure_rules(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy()
    emit_auto_rules(
      session,
      taxonomy,
      [_fake_structure("s1"), _fake_structure("s2")],
      created_by="usr_1",
    )
    added = [c.args[0] for c in session.add.call_args_list]
    structure_rules = [r for r in added if r.target_kind == "structure"]
    assert len(structure_rules) == 6

  def test_coa_emits_leaf_has_classification(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("chart_of_accounts")
    emit_auto_rules(session, taxonomy, [], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    patterns = {r.rule_check_kind for r in added}
    assert "LeafHasClassification" in patterns

  def test_non_coa_does_not_emit_leaf_has_classification(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    emit_auto_rules(session, taxonomy, [], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    patterns = {r.rule_check_kind for r in added}
    assert "LeafHasClassification" not in patterns

  def test_extend_mode_emits_library_origin_immutability(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("reporting_extension", parent_taxonomy_id="lib_42")
    emit_auto_rules(session, taxonomy, [], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    patterns = {r.rule_check_kind for r in added}
    assert "LibraryOriginImmutability" in patterns

  def test_declarative_mode_does_not_emit_library_origin_immutability(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology", parent_taxonomy_id=None)
    emit_auto_rules(session, taxonomy, [], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    patterns = {r.rule_check_kind for r in added}
    assert "LibraryOriginImmutability" not in patterns

  def test_all_auto_rules_carry_auto_origin(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("chart_of_accounts", parent_taxonomy_id="lib_1")
    emit_auto_rules(
      session,
      taxonomy,
      [_fake_structure("s1")],
      created_by="usr_1",
    )
    added = [c.args[0] for c in session.add.call_args_list]
    assert all(r.rule_origin == "auto" for r in added)

  def test_all_auto_rules_populate_check_kind_not_pattern(self) -> None:
    """XOR contract: all auto-rules are structural (rule_check_kind set,
    rule_pattern is None). Enforced by the check_rule_pattern_kind_xor
    CHECK constraint on the rules table."""
    session = MagicMock()
    taxonomy = _fake_taxonomy("chart_of_accounts", parent_taxonomy_id="lib_1")
    emit_auto_rules(
      session,
      taxonomy,
      [_fake_structure("s1")],
      created_by="usr_1",
    )
    added = [c.args[0] for c in session.add.call_args_list]
    assert all(r.rule_pattern is None for r in added)
    assert all(r.rule_check_kind is not None for r in added)

  def test_taxonomy_level_rules_use_taxonomy_target_kind(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy()
    emit_auto_rules(session, taxonomy, [], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    for r in added:
      assert r.target_kind == "taxonomy"
      assert r.target_taxonomy_id == "tax_1"

  def test_structure_rules_use_structure_target_kind(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy()
    emit_auto_rules(session, taxonomy, [_fake_structure("s_abc")], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    structure_rules = [r for r in added if r.target_kind == "structure"]
    assert all(r.target_structure_id == "s_abc" for r in structure_rules)

  def test_coa_full_count(self) -> None:
    """CoA with 2 structures: UniqueQName + LeafHasClass + 3*2 structure rules = 8."""
    session = MagicMock()
    taxonomy = _fake_taxonomy("chart_of_accounts")
    emit_auto_rules(
      session,
      taxonomy,
      [_fake_structure("s1"), _fake_structure("s2")],
      created_by="usr_1",
    )
    added = session.add.call_args_list
    assert len(added) == 8

  def test_extend_coa_full_count(self) -> None:
    """Extend CoA with 1 structure: UniqueQName + LibraryImmut + LeafHasClass + 3 = 6."""
    session = MagicMock()
    taxonomy = _fake_taxonomy("chart_of_accounts", parent_taxonomy_id="lib_1")
    emit_auto_rules(
      session,
      taxonomy,
      [_fake_structure("s1")],
      created_by="usr_1",
    )
    added = session.add.call_args_list
    assert len(added) == 6

  def test_created_by_propagates(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy()
    emit_auto_rules(session, taxonomy, [], created_by="seeder_bot")

    added = [c.args[0] for c in session.add.call_args_list]
    assert all(r.created_by == "seeder_bot" for r in added)


class TestRollupEmission:
  """Auto-emitted RollUp footing rules for authored roll_up structures."""

  def test_roll_up_structure_with_calc_arcs_emits_rollup_rule(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("reporting_extension", parent_taxonomy_id="lib_1")
    note = _fake_structure("struct_note", concept_arrangement="roll_up")
    session.execute.return_value.fetchall.return_value = [
      _calc_row(
        "elem_total",
        "elem_raw",
        parent_qname="rs-gaap:InventoryNet",
        child_qname="driftline:InventoryRawMaterials",
        order_value=1.0,
      ),
      _calc_row(
        "elem_total",
        "elem_fg",
        parent_qname="rs-gaap:InventoryNet",
        child_qname="driftline:InventoryFinishedGoods",
        order_value=2.0,
      ),
    ]

    emit_auto_rules(session, taxonomy, [note], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    rollups = [r for r in added if r.rule_pattern == "RollUp"]
    assert len(rollups) == 1
    rule = rollups[0]
    assert rule.rule_origin == "auto"
    assert rule.target_kind == "structure"
    assert rule.target_structure_id == "struct_note"
    # Parent-first variables with explicit element ids — qname resolution
    # must not be load-bearing for tenant-authored concepts.
    assert rule.rule_variables[0]["variable_element_id"] == "elem_total"
    assert rule.rule_variables[0]["variable_qname"] == "rs-gaap:InventoryNet"
    child_ids = {v["variable_element_id"] for v in rule.rule_variables[1:]}
    assert child_ids == {"elem_raw", "elem_fg"}
    assert rule.rule_expression == (
      "$InventoryNet = $InventoryRawMaterials + $InventoryFinishedGoods"
    )

  def test_negative_weight_renders_minus_term(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("reporting_extension", parent_taxonomy_id="lib_1")
    note = _fake_structure("struct_note", concept_arrangement="roll_up")
    session.execute.return_value.fetchall.return_value = [
      _calc_row(
        "elem_net",
        "elem_gross",
        parent_qname="ext:Net",
        child_qname="ext:Gross",
        order_value=1.0,
      ),
      _calc_row(
        "elem_net",
        "elem_allowance",
        parent_qname="ext:Net",
        child_qname="ext:Allowance",
        weight=-1.0,
        order_value=2.0,
      ),
    ]

    emit_auto_rules(session, taxonomy, [note], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    rule = next(r for r in added if r.rule_pattern == "RollUp")
    assert rule.rule_expression == "$Net = $Gross - $Allowance"

  def test_roll_up_without_calc_arcs_emits_no_rollup(self) -> None:
    """Presentation-only roll_up renders but has nothing to foot."""
    session = MagicMock()
    taxonomy = _fake_taxonomy("reporting_extension", parent_taxonomy_id="lib_1")
    note = _fake_structure("struct_note", concept_arrangement="roll_up")
    session.execute.return_value.fetchall.return_value = []

    emit_auto_rules(session, taxonomy, [note], created_by="usr_1")

    added = [c.args[0] for c in session.add.call_args_list]
    assert not [r for r in added if r.rule_pattern == "RollUp"]

  def test_non_roll_up_structure_skips_rollup_query(self) -> None:
    session = MagicMock()
    taxonomy = _fake_taxonomy("reporting_extension", parent_taxonomy_id="lib_1")
    plain = _fake_structure("struct_plain", concept_arrangement=None)

    emit_auto_rules(session, taxonomy, [plain], created_by="usr_1")

    session.execute.assert_not_called()
