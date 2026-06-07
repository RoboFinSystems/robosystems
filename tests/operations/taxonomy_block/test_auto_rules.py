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


def _fake_structure(sid: str) -> MagicMock:
  s = MagicMock()
  s.id = sid
  return s


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
