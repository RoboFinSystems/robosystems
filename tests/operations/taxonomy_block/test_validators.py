"""Validator pipeline tests — one happy + one rejection per phase."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  TaxonomyBlockAssociationRequest,
  TaxonomyBlockElementRequest,
  TaxonomyBlockRuleRequest,
  TaxonomyBlockStructureRequest,
)
from robosystems.operations.taxonomy_block.validators import (
  TaxonomyBlockValidationError,
  _detect_cycle,
  validate_create_envelope,
)


def _session_empty() -> MagicMock:
  """Session returning empty library lookup (no parent_taxonomy_id)."""
  session = MagicMock()
  session.execute.return_value.all.return_value = []
  return session


def _basic_coa(
  elements=None, structures=None, associations=None, rules=None
) -> CreateTaxonomyBlockRequest:
  return CreateTaxonomyBlockRequest(
    name="Test CoA",
    taxonomy_type="chart_of_accounts",
    elements=elements
    or [
      TaxonomyBlockElementRequest(
        qname="coa:Cash",
        name="Cash",
        trait="asset",
        balance_type="debit",
        period_type="instant",
      )
    ],
    structures=structures or [],
    associations=associations or [],
    rules=rules or [],
  )


class TestReferenceResolution:
  def test_happy_path_no_issues(self) -> None:
    issues = validate_create_envelope(_basic_coa(), _session_empty())
    assert issues == []

  def test_phantom_parent_ref_rejected(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="coa:A",
          name="A",
          trait="asset",
          balance_type="debit",
          period_type="instant",
          parent_ref="coa:GhostParent",
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "phantom_parent_ref" for i in issues)

  def test_phantom_structure_ref_rejected(self) -> None:
    payload = _basic_coa(
      associations=[
        TaxonomyBlockAssociationRequest(
          structure_ref="ghost_structure",
          from_ref="coa:Cash",
          to_ref="coa:Cash",
          association_type="presentation",
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "phantom_structure_ref" for i in issues)


class TestUniqueness:
  def test_duplicate_qname_rejected(self) -> None:
    dup = TaxonomyBlockElementRequest(
      qname="coa:Cash",
      name="Cash",
      trait="asset",
      balance_type="debit",
      period_type="instant",
    )
    payload = _basic_coa(elements=[dup, dup.model_copy()])
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "duplicate_element_qname" for i in issues)

  def test_duplicate_structure_name_rejected(self) -> None:
    s = TaxonomyBlockStructureRequest(name="main", block_type="chart_of_accounts")
    payload = _basic_coa(structures=[s, s.model_copy()])
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "duplicate_structure_name" for i in issues)


class TestStructuralIntegrity:
  def test_cycle_in_parent_child_rejected(self) -> None:
    elements = [
      TaxonomyBlockElementRequest(
        qname=f"coa:{n}",
        name=n,
        trait="asset",
        balance_type="debit",
        period_type="instant",
      )
      for n in ("A", "B", "C")
    ]
    structures = [
      TaxonomyBlockStructureRequest(name="main", block_type="chart_of_accounts")
    ]
    associations = [
      TaxonomyBlockAssociationRequest(
        structure_ref="main",
        from_ref="coa:A",
        to_ref="coa:B",
        association_type="presentation",
      ),
      TaxonomyBlockAssociationRequest(
        structure_ref="main",
        from_ref="coa:B",
        to_ref="coa:C",
        association_type="presentation",
      ),
      TaxonomyBlockAssociationRequest(
        structure_ref="main",
        from_ref="coa:C",
        to_ref="coa:A",
        association_type="presentation",
      ),
    ]
    payload = _basic_coa(
      elements=elements, structures=structures, associations=associations
    )
    issues = validate_create_envelope(payload, _session_empty())
    cycle_issues = [i for i in issues if i.code == "cycle_detected"]
    assert len(cycle_issues) == 1

  def test_no_cycle_happy_path(self) -> None:
    elements = [
      TaxonomyBlockElementRequest(
        qname=f"coa:{n}",
        name=n,
        trait="asset",
        balance_type="debit",
        period_type="instant",
      )
      for n in ("A", "B", "C")
    ]
    structures = [
      TaxonomyBlockStructureRequest(name="main", block_type="chart_of_accounts")
    ]
    associations = [
      TaxonomyBlockAssociationRequest(
        structure_ref="main",
        from_ref="coa:A",
        to_ref="coa:B",
        association_type="presentation",
      ),
      TaxonomyBlockAssociationRequest(
        structure_ref="main",
        from_ref="coa:B",
        to_ref="coa:C",
        association_type="presentation",
      ),
    ]
    payload = _basic_coa(
      elements=elements, structures=structures, associations=associations
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert not any(i.code == "cycle_detected" for i in issues)


class TestTypeSpecific:
  def test_coa_missing_classification_rejected(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="coa:NoClass",
          name="NoClass",
          balance_type="debit",
          period_type="instant",
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "coa_missing_classification" for i in issues)

  def test_coa_period_type_stock_mismatch_rejected(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="coa:WrongPeriod",
          name="WrongPeriod",
          trait="asset",
          balance_type="debit",
          period_type="duration",
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "coa_period_type_mismatch" for i in issues)

  def test_custom_ontology_allows_no_classification(self) -> None:
    payload = CreateTaxonomyBlockRequest(
      name="KPIs",
      taxonomy_type="custom_ontology",
      elements=[
        TaxonomyBlockElementRequest(
          qname="kpi:NPS",
          name="NPS",
        )
      ],
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert not any(i.phase == "type_specific" for i in issues)


class TestLibraryOriginRespect:
  def test_smuggled_library_origin_rejected(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="coa:Sneaky",
          name="Sneaky",
          trait="asset",
          balance_type="debit",
          period_type="instant",
          metadata={"origin": "library"},
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "library_origin_smuggled" for i in issues)


class TestRuleExpressionParse:
  def test_valid_expression_accepted(self) -> None:
    payload = _basic_coa(
      rules=[
        TaxonomyBlockRuleRequest(
          name="assets-equal",
          rule_category="FundamentalAccountingConceptRelation",
          rule_pattern="EqualTo",
          expression="$Assets = $Liab + $Equity",
          variables=[
            {"variable_name": "Assets", "variable_qname": "us-gaap:Assets"},
            {"variable_name": "Liab", "variable_qname": "us-gaap:Liabilities"},
            {"variable_name": "Equity", "variable_qname": "us-gaap:StockholdersEquity"},
          ],
          target_taxonomy_self=True,
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert not any(i.phase == "rule_expression_parse" for i in issues)

  def test_unparseable_expression_rejected(self) -> None:
    payload = _basic_coa(
      rules=[
        TaxonomyBlockRuleRequest(
          name="bad-syntax",
          rule_category="FundamentalAccountingConceptRelation",
          rule_pattern="EqualTo",
          expression="$ + $ = ??",
          variables=[],
          target_taxonomy_self=True,
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "unparseable_expression" for i in issues)

  def test_duplicate_variable_name_rejected(self) -> None:
    """Same-named variables silently merge in the engine's name-keyed
    fact bindings — the gate rejects them outright."""
    payload = _basic_coa(
      rules=[
        TaxonomyBlockRuleRequest(
          name="colliding",
          rule_category="FundamentalAccountingConceptRelation",
          rule_pattern="EqualTo",
          expression="$Total = $InventoryNet + $InventoryNet",
          variables=[
            {"variable_name": "Total", "variable_qname": "ext:Total"},
            {"variable_name": "InventoryNet", "variable_qname": "ext:InventoryNet"},
            {
              "variable_name": "InventoryNet",
              "variable_qname": "rs-gaap:InventoryNet",
            },
          ],
          target_taxonomy_self=True,
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "duplicate_variable_name" for i in issues)

  def test_rollup_parent_not_first_rejected(self) -> None:
    """RollUp rules must list the LHS parent subtotal at variables[0] —
    the arc-derived evaluator's legacy fallback trusts that position."""
    payload = _basic_coa(
      rules=[
        TaxonomyBlockRuleRequest(
          name="children-first",
          rule_category="FundamentalAccountingConceptRelation",
          rule_pattern="RollUp",
          expression="$Total = $Raw + $Finished",
          variables=[
            {"variable_name": "Raw", "variable_qname": "ext:Raw"},
            {"variable_name": "Finished", "variable_qname": "ext:Finished"},
            {"variable_name": "Total", "variable_qname": "ext:Total"},
          ],
          target_taxonomy_self=True,
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert any(i.code == "rollup_parent_not_first" for i in issues)

  def test_rollup_parent_first_accepted(self) -> None:
    payload = _basic_coa(
      rules=[
        TaxonomyBlockRuleRequest(
          name="parent-first",
          rule_category="FundamentalAccountingConceptRelation",
          rule_pattern="RollUp",
          expression="$Total = $Raw + $Finished",
          variables=[
            {"variable_name": "Total", "variable_qname": "ext:Total"},
            {"variable_name": "Raw", "variable_qname": "ext:Raw"},
            {"variable_name": "Finished", "variable_qname": "ext:Finished"},
          ],
          target_taxonomy_self=True,
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert not any(i.phase == "rule_expression_parse" for i in issues)


class TestErrorAggregation:
  def test_all_issues_raised_together(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="coa:Dup",
          name="Dup",
          trait="asset",
          balance_type="debit",
          period_type="instant",
        ),
        TaxonomyBlockElementRequest(
          qname="coa:Dup",
          name="Dup",
          trait="asset",
          balance_type="debit",
          period_type="instant",
        ),
      ],
      associations=[
        TaxonomyBlockAssociationRequest(
          structure_ref="nonexistent",
          from_ref="coa:Dup",
          to_ref="coa:Ghost",
          association_type="presentation",
        )
      ],
    )
    issues = validate_create_envelope(payload, _session_empty())
    codes = {i.code for i in issues}
    assert "duplicate_element_qname" in codes
    assert "phantom_structure_ref" in codes
    assert "phantom_to_ref" in codes

  def test_error_type_carries_issues(self) -> None:
    err = TaxonomyBlockValidationError(
      [
        # Minimal issue for construction verification
      ]
    )
    assert isinstance(err, ValueError)
    assert err.issues == []


class TestCycleDetection:
  def test_no_cycle_returns_none(self) -> None:
    assert _detect_cycle([("A", "B"), ("B", "C")]) is None

  def test_simple_cycle_detected(self) -> None:
    cycle = _detect_cycle([("A", "B"), ("B", "A")])
    assert cycle is not None
    assert "A" in cycle and "B" in cycle

  def test_longer_cycle_detected(self) -> None:
    cycle = _detect_cycle([("A", "B"), ("B", "C"), ("C", "A")])
    assert cycle is not None

  def test_empty_arcs_returns_none(self) -> None:
    assert _detect_cycle([]) is None


def test_pytest_is_wired() -> None:
  """Smoke — confirms pytest markers and module paths resolve."""
  with pytest.raises(TaxonomyBlockValidationError):
    raise TaxonomyBlockValidationError([])


class TestLibraryNamespaceProtection:
  """Tenants anchor to library concepts; they never declare in library
  namespaces — rs-gaap is platform-owned and evolves wholesale."""

  def _session_with_library_prefixes(self, rows_per_call: list) -> MagicMock:
    session = MagicMock()
    results = []
    for rows in rows_per_call:
      result = MagicMock()
      result.all.return_value = rows
      results.append(result)
    session.execute.side_effect = results
    return session

  def test_declaring_under_library_prefix_rejected(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="rs-gaap:SneakyAssets",
          name="Sneaky Assets",
          trait="asset",
          balance_type="debit",
          period_type="instant",
        )
      ]
    )
    session = self._session_with_library_prefixes([[("rs-gaap",), ("disclosures",)]])
    issues = validate_create_envelope(payload, session)
    codes = {i.code for i in issues}
    assert "library_namespace_reserved" in codes

  def test_own_namespace_declaration_accepted(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="acme:Widgets",
          name="Widgets",
          trait="asset",
          balance_type="debit",
          period_type="instant",
        )
      ]
    )
    session = self._session_with_library_prefixes([[("rs-gaap",), ("disclosures",)]])
    issues = validate_create_envelope(payload, session)
    assert not any(i.phase == "namespace_protection" for i in issues)

  def test_referencing_library_concepts_stays_legal(self) -> None:
    """The extend-and-map shape: own-namespace member parented under and
    arced from a library total. References resolve; only declarations
    are namespace-guarded."""
    payload = CreateTaxonomyBlockRequest(
      name="Acme Extension",
      taxonomy_type="reporting_extension",
      parent_taxonomy_id="tax_lib",
      elements=[
        TaxonomyBlockElementRequest(
          qname="acme:InventoryPackaging",
          name="Packaging",
          parent_ref="rs-gaap:InventoryNet",
          balance_type="debit",
          period_type="instant",
        )
      ],
      structures=[],
      associations=[],
      rules=[],
    )
    # 1st execute: _load_library_qnames resolves the referenced parent;
    # 2nd execute: the reserved-prefix load.
    session = self._session_with_library_prefixes(
      [[("rs-gaap:InventoryNet",)], [("rs-gaap",)]]
    )
    issues = validate_create_envelope(payload, session)
    assert not any(i.phase == "namespace_protection" for i in issues)
    assert not any(i.code == "phantom_parent_ref" for i in issues)

  def test_no_library_rows_disables_protection(self) -> None:
    payload = _basic_coa(
      elements=[
        TaxonomyBlockElementRequest(
          qname="rs-gaap:Whatever",
          name="Whatever",
          trait="asset",
          balance_type="debit",
          period_type="instant",
        )
      ]
    )
    issues = validate_create_envelope(payload, _session_empty())
    assert not any(i.phase == "namespace_protection" for i in issues)
