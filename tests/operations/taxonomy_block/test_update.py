"""Tests for Taxonomy Block update handlers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.taxonomy_block import (
  ElementUpdatePatch,
  StructureUpdatePatch,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import custom_ontology


def _fake_taxonomy(taxonomy_type: str, *, is_locked: bool = False) -> MagicMock:
  taxonomy = MagicMock()
  taxonomy.id = "tax_42"
  taxonomy.taxonomy_type = taxonomy_type
  taxonomy.is_locked = is_locked
  taxonomy.standard = None
  taxonomy.name = "Original"
  taxonomy.description = "orig"
  taxonomy.version = "v1"
  taxonomy.parent_taxonomy_id = None
  taxonomy.namespace_uri = None
  return taxonomy


class TestCustomOntologyUpdate:
  def test_wrong_type_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("chart_of_accounts")
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_42")
    with pytest.raises(ValueError) as exc:
      custom_ontology.update(session, payload, "usr_1")
    assert "custom_ontology" in str(exc.value)

  def test_missing_taxonomy_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_missing")
    with pytest.raises(ValueError) as exc:
      custom_ontology.update(session, payload, "usr_1")
    assert "not a custom_ontology" in str(exc.value)

  def test_top_level_field_update_only(self) -> None:
    """Empty-delta update still allows name/description/version rename."""
    session = MagicMock()
    tax = _fake_taxonomy("custom_ontology")
    session.get.return_value = tax
    with (
      patch(
        "robosystems.operations.taxonomy_block.custom_ontology.validate_update_envelope",
        return_value=[],
      ),
    ):
      payload = UpdateTaxonomyBlockRequest(
        taxonomy_id="tax_42",
        name="Renamed",
        description="new desc",
        version="v2",
      )
      result = custom_ontology.update(session, payload, "usr_1")
    assert result == "tax_42"
    assert tax.name == "Renamed"
    assert tax.description == "new desc"
    assert tax.version == "v2"


# ── Delta validator unit tests ─────────────────────────────────────────────


def _mock_execute_result(value: list) -> MagicMock:
  """Build a MagicMock whose .all() and .scalars().all() return ``value``."""
  result = MagicMock()
  result.all.return_value = value
  scalars = MagicMock()
  scalars.all.return_value = value
  result.scalars.return_value = scalars
  return result


class TestValidateElementsToUpdate:
  def test_unknown_qname_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_update,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result([])  # no rows for lookup
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      elements_to_update=[ElementUpdatePatch(qname="x:Missing", name="M")],
    )
    issues = _validate_elements_to_update(session, taxonomy, payload, {})
    codes = {i.code for i in issues}
    assert "unknown_element_qname" in codes

  def test_library_origin_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_update,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result(
      [("x:Seeded", "library-seeder")]
    )
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      elements_to_update=[ElementUpdatePatch(qname="x:Seeded", name="patched")],
    )
    issues = _validate_elements_to_update(
      session, taxonomy, payload, {"x:Seeded": "e_seed"}
    )
    codes = {i.code for i in issues}
    assert "library_element_immutable" in codes

  def test_tenant_element_accepted(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_update,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result([("x:Tenant", "usr_1")])
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      elements_to_update=[ElementUpdatePatch(qname="x:Tenant", name="new")],
    )
    issues = _validate_elements_to_update(
      session, taxonomy, payload, {"x:Tenant": "e_1"}
    )
    assert issues == []


class TestValidateElementsToRemove:
  def test_unknown_qname_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_remove,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result([])
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:Missing"]
    )
    issues = _validate_elements_to_remove(session, taxonomy, payload, {})
    codes = {i.code for i in issues}
    assert "unknown_element_qname" in codes

  def test_library_origin_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_remove,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result(
      [("x:Seeded", "e_seed", "library-seeder")]
    )
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:Seeded"]
    )
    issues = _validate_elements_to_remove(
      session, taxonomy, payload, {"x:Seeded": "e_seed"}
    )
    codes = {i.code for i in issues}
    assert "library_element_immutable" in codes

  def test_facts_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_remove,
    )

    session = MagicMock()
    # 1st call: lookup existing rows
    # 2nd call: Fact.id → [fact_1] (non-empty triggers rejection)
    session.execute.side_effect = [
      _mock_execute_result([("x:A", "e_a", "usr_1")]),
      _mock_execute_result(["fact_1"]),
    ]
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:A"]
    )
    issues = _validate_elements_to_remove(session, taxonomy, payload, {"x:A": "e_a"})
    codes = {i.code for i in issues}
    assert "element_has_facts" in codes

  def test_line_items_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_remove,
    )

    session = MagicMock()
    session.execute.side_effect = [
      _mock_execute_result([("x:A", "e_a", "usr_1")]),
      _mock_execute_result([]),  # no facts
      _mock_execute_result(["li_1", "li_2"]),  # two line items
    ]
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:A"]
    )
    issues = _validate_elements_to_remove(session, taxonomy, payload, {"x:A": "e_a"})
    codes = {i.code for i in issues}
    assert "element_has_line_items" in codes

  def test_cross_taxonomy_mapping_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_remove,
    )

    session = MagicMock()
    session.execute.side_effect = [
      _mock_execute_result([("x:A", "e_a", "usr_1")]),
      _mock_execute_result([]),  # facts
      _mock_execute_result([]),  # line_items
      _mock_execute_result(["assoc_other"]),  # cross-taxonomy mapping
    ]
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:A"]
    )
    issues = _validate_elements_to_remove(session, taxonomy, payload, {"x:A": "e_a"})
    codes = {i.code for i in issues}
    assert "element_has_cross_taxonomy_mappings" in codes

  def test_unincluded_children_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_remove,
    )

    session = MagicMock()
    session.execute.side_effect = [
      _mock_execute_result([("x:Parent", "e_parent", "usr_1")]),
      _mock_execute_result([]),  # facts
      _mock_execute_result([]),  # line_items
      _mock_execute_result([]),  # cross-taxonomy
      _mock_execute_result(["e_child_unremoved"]),  # orphan child
    ]
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:Parent"]
    )
    issues = _validate_elements_to_remove(
      session, taxonomy, payload, {"x:Parent": "e_parent"}
    )
    codes = {i.code for i in issues}
    assert "element_has_children" in codes

  def test_clean_removal_accepted(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_elements_to_remove,
    )

    session = MagicMock()
    session.execute.side_effect = [
      _mock_execute_result([("x:A", "e_a", "usr_1")]),
      _mock_execute_result([]),
      _mock_execute_result([]),
      _mock_execute_result([]),
      _mock_execute_result([]),
    ]
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:A"]
    )
    issues = _validate_elements_to_remove(session, taxonomy, payload, {"x:A": "e_a"})
    assert issues == []


class TestValidateStructuresToUpdate:
  def test_unknown_structure_id_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_structures_to_update,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      structures_to_update=[StructureUpdatePatch(structure_id="s_other", name="x")],
    )
    issues = _validate_structures_to_update(session, taxonomy, payload, {"s_mine"})
    codes = {i.code for i in issues}
    assert "unknown_structure_id" in codes


class TestValidateStructuresToRemove:
  def test_unknown_id_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_structures_to_remove,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", structures_to_remove=["s_other"]
    )
    issues = _validate_structures_to_remove(session, taxonomy, payload, {"s_mine"})
    codes = {i.code for i in issues}
    assert "unknown_structure_id" in codes


class TestValidateAssociationsToRemove:
  def test_unknown_association_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_associations_to_remove,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result([])
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", associations_to_remove=["assoc_missing"]
    )
    issues = _validate_associations_to_remove(session, taxonomy, payload, {"s_mine"})
    codes = {i.code for i in issues}
    assert "unknown_association_id" in codes

  def test_foreign_taxonomy_association_rejected(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_associations_to_remove,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result(
      [("assoc_foreign", "s_foreign")]
    )
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", associations_to_remove=["assoc_foreign"]
    )
    issues = _validate_associations_to_remove(session, taxonomy, payload, {"s_mine"})
    codes = {i.code for i in issues}
    assert "association_not_in_taxonomy" in codes

  def test_in_taxonomy_association_accepted(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _validate_associations_to_remove,
    )

    session = MagicMock()
    session.execute.return_value = _mock_execute_result([("assoc_mine", "s_mine")])
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", associations_to_remove=["assoc_mine"]
    )
    issues = _validate_associations_to_remove(session, taxonomy, payload, {"s_mine"})
    assert issues == []


# ── Apply helper unit tests ─────────────────────────────────────────────────


class TestApplyElementsToUpdate:
  def test_mutates_non_none_fields(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_elements_to_update,
    )

    session = MagicMock()
    element = MagicMock()
    element.id = "e_1"
    element.qname = "x:A"
    element.name = "Old"
    element.description = "old desc"
    element.balance_type = "debit"
    element.period_type = "duration"
    element.is_monetary = True
    element.code = "1000"
    element.parent_id = None
    element.metadata_ = {}

    # Two queries: first for element_by_qname (limited to patched qnames),
    # second for all_elements_by_qname.
    session.execute.side_effect = [
      _mock_execute_result([element]),
      _mock_execute_result([element]),
    ]

    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      elements_to_update=[
        ElementUpdatePatch(
          qname="x:A",
          name="New",
          description="new desc",
          balance_type="credit",
        )
      ],
    )
    apply_elements_to_update(session, taxonomy, payload, "usr_1")

    assert element.name == "New"
    assert element.description == "new desc"
    assert element.balance_type == "credit"
    # Unpatched fields untouched
    assert element.period_type == "duration"
    assert element.is_monetary is True

  def test_noop_when_no_updates(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_elements_to_update,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_42")
    apply_elements_to_update(session, taxonomy, payload, "usr_1")
    session.execute.assert_not_called()


class TestApplyElementsToRemove:
  def test_deletes_element_and_side_tables(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_elements_to_remove,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", elements_to_remove=["x:A", "x:B"]
    )
    apply_elements_to_remove(session, taxonomy, payload, {"x:A": "e_a", "x:B": "e_b"})

    # 1 UPDATE + 5 DELETEs (associations + 3 side tables + elements) = 6 calls
    assert session.execute.call_count == 6
    session.flush.assert_called_once()

  def test_noop_when_nothing_to_remove(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_elements_to_remove,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_42")
    apply_elements_to_remove(session, taxonomy, payload, {})
    session.execute.assert_not_called()


class TestApplyStructuresToRemove:
  def test_deletes_structures_cascades_rules_and_associations(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_structures_to_remove,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", structures_to_remove=["s_1", "s_2"]
    )
    apply_structures_to_remove(session, taxonomy, payload)

    # 4 DELETEs: verification results FIRST (they FK rules and
    # structures with no ON DELETE), then rules, associations, structures
    assert session.execute.call_count == 4
    deleted_tables = [c.args[0].table.name for c in session.execute.call_args_list]
    assert deleted_tables == [
      "verification_results",
      "rules",
      "associations",
      "structures",
    ]
    session.flush.assert_called_once()

  def test_noop_when_nothing_to_remove(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_structures_to_remove,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_42")
    apply_structures_to_remove(session, taxonomy, payload)
    session.execute.assert_not_called()


class TestApplyAssociationsToRemove:
  def test_deletes_associations(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_associations_to_remove,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", associations_to_remove=["a_1", "a_2"]
    )
    apply_associations_to_remove(session, taxonomy, payload)

    session.execute.assert_called_once()
    session.flush.assert_called_once()

  def test_noop_when_empty(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_associations_to_remove,
    )

    session = MagicMock()
    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_42")
    apply_associations_to_remove(session, taxonomy, payload)
    session.execute.assert_not_called()


class TestApplyStructuresToUpdate:
  def test_mutates_name_description_and_role_uri(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_structures_to_update,
    )

    session = MagicMock()
    structure = MagicMock()
    structure.id = "s_1"
    structure.name = "Old"
    structure.description = "old desc"
    structure.metadata_ = {"existing": "kept"}

    session.execute.return_value = _mock_execute_result([structure])

    taxonomy = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      structures_to_update=[
        StructureUpdatePatch(
          structure_id="s_1",
          name="New",
          description="new desc",
          role_uri="http://role/x",
        )
      ],
    )
    apply_structures_to_update(session, taxonomy, payload, "usr_1")

    assert structure.name == "New"
    assert structure.description == "new desc"
    assert structure.metadata_["role_uri"] == "http://role/x"
    assert structure.metadata_["existing"] == "kept"


class TestApplyStructuresToUpdateConceptArrangement:
  def test_sets_concept_arrangement_when_patched(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_structures_to_update,
    )

    session = MagicMock()
    structure = MagicMock()
    structure.id = "s_1"
    structure.concept_arrangement = "set"
    session.execute.return_value = _mock_execute_result([structure])

    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      structures_to_update=[
        StructureUpdatePatch(structure_id="s_1", concept_arrangement="roll_up")
      ],
    )
    apply_structures_to_update(
      session, _fake_taxonomy("reporting_extension"), payload, "usr_1"
    )
    assert structure.concept_arrangement == "roll_up"

  def test_leaves_concept_arrangement_when_not_patched(self) -> None:
    from robosystems.operations.taxonomy_block.update_apply import (
      apply_structures_to_update,
    )

    session = MagicMock()
    structure = MagicMock()
    structure.id = "s_1"
    structure.concept_arrangement = "set"
    session.execute.return_value = _mock_execute_result([structure])

    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      structures_to_update=[StructureUpdatePatch(structure_id="s_1", name="Renamed")],
    )
    apply_structures_to_update(
      session, _fake_taxonomy("reporting_extension"), payload, "usr_1"
    )
    assert structure.concept_arrangement == "set"

  def test_patch_rejects_unknown_concept_arrangement(self) -> None:
    with pytest.raises(Exception):
      StructureUpdatePatch(structure_id="s_1", concept_arrangement="not_a_cap")

  def test_patch_accepts_text_block(self) -> None:
    patch_model = StructureUpdatePatch(
      structure_id="s_1", concept_arrangement="text_block"
    )
    assert patch_model.concept_arrangement == "text_block"


class TestUpdateRefreshesAutoRules:
  """Every handler's update() re-derives auto rules after applying deltas."""

  def _run_update(self, module_name: str, taxonomy_type: str) -> MagicMock:
    import importlib

    module = importlib.import_module(
      f"robosystems.operations.taxonomy_block.{module_name}"
    )
    session = MagicMock()
    tax = _fake_taxonomy(taxonomy_type)
    session.get.return_value = tax
    session.execute.return_value = _mock_execute_result([])
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42", structures_to_remove=["s_gone"]
    )
    with (
      patch(
        f"robosystems.operations.taxonomy_block.{module_name}.validate_update_envelope",
        return_value=[],
      ),
      patch(
        f"robosystems.operations.taxonomy_block.{module_name}.refresh_auto_rules"
      ) as refresh,
    ):
      module.update(session, payload, "usr_1")
    return refresh

  def test_custom_ontology_update_calls_refresh(self) -> None:
    refresh = self._run_update("custom_ontology", "custom_ontology")
    refresh.assert_called_once()
    assert refresh.call_args.kwargs == {"updated_by": "usr_1"}

  def test_chart_of_accounts_update_calls_refresh(self) -> None:
    refresh = self._run_update("chart_of_accounts", "chart_of_accounts")
    refresh.assert_called_once()

  def test_reporting_extension_update_calls_refresh(self) -> None:
    refresh = self._run_update("reporting_extension", "reporting_extension")
    refresh.assert_called_once()


class TestResolveForeignElementQnames:
  """Library-element endpoints must project to their real qnames — an
  extend-mode note arcs FROM a library concept, and before this fix every
  update to such a taxonomy 422'd with phantom_from_ref."""

  def _assoc(self, from_id: str, to_id: str) -> MagicMock:
    a = MagicMock()
    a.from_element_id = from_id
    a.to_element_id = to_id
    return a

  def _element(self, eid: str, parent_id: str | None = None) -> MagicMock:
    e = MagicMock()
    e.id = eid
    e.parent_id = parent_id
    return e

  def test_resolves_library_arc_endpoints_and_parents(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _resolve_foreign_element_qnames,
    )

    session = MagicMock()
    session.execute.return_value.all.return_value = [
      ("elem_lib_total", "rs-gaap:InventoryNet"),
    ]
    qname_by_id = {"elem_member": "driftline:RawMaterials"}
    elements = [self._element("elem_member", parent_id="elem_lib_total")]
    associations = [self._assoc("elem_lib_total", "elem_member")]

    _resolve_foreign_element_qnames(session, qname_by_id, elements, associations)

    assert qname_by_id["elem_lib_total"] == "rs-gaap:InventoryNet"
    session.execute.assert_called_once()

  def test_no_foreign_ids_skips_query(self) -> None:
    from robosystems.operations.taxonomy_block.update_validator import (
      _resolve_foreign_element_qnames,
    )

    session = MagicMock()
    qname_by_id = {"elem_a": "x:A", "elem_b": "x:B"}
    elements = [self._element("elem_a"), self._element("elem_b", parent_id="elem_a")]
    associations = [self._assoc("elem_a", "elem_b")]

    _resolve_foreign_element_qnames(session, qname_by_id, elements, associations)

    session.execute.assert_not_called()
