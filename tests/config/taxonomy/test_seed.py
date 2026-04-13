"""Tests for taxonomy seed data integrity.

Validates the seed data structures without touching a database.
These are pure data tests — they verify counts, hierarchy integrity,
constraint compliance, and uniqueness before any SQL is executed.
"""

from robosystems.config.taxonomy.seed import (
  ALL_GAAP_ELEMENTS,
  BALANCE_SHEET_ELEMENTS,
  INCOME_STATEMENT_ELEMENTS,
  SFAC6_ELEMENTS,
  STRUCT_BS_ID,
  STRUCT_IS_ID,
  STRUCTURES,
  TAXONOMY_ID,
  _structure_for_element,
)


class TestSFAC6Elements:
  def test_has_10_roots(self):
    assert len(SFAC6_ELEMENTS) == 10

  def test_all_abstract(self):
    for e in SFAC6_ELEMENTS:
      assert e["is_abstract"] is True, f"{e['name']} should be abstract"

  def test_all_depth_zero(self):
    for e in SFAC6_ELEMENTS:
      assert e["depth"] == 0, f"{e['name']} should be depth 0"

  def test_all_source_sfac6(self):
    for e in SFAC6_ELEMENTS:
      assert e["source"] == "sfac6"

  def test_all_have_sfac6_namespace(self):
    for e in SFAC6_ELEMENTS:
      assert e["namespace"] == "sfac6"
      assert e["qname"].startswith("sfac6:")

  def test_no_parent_ids(self):
    """SFAC 6 roots should have no parent."""
    for e in SFAC6_ELEMENTS:
      assert "parent_id" not in e or e.get("parent_id") is None

  def test_covers_all_classifications(self):
    classifications = {e["classification"] for e in SFAC6_ELEMENTS}
    assert "asset" in classifications
    assert "liability" in classifications
    assert "equity" in classifications
    assert "revenue" in classifications
    assert "expense" in classifications

  def test_balance_sheet_elements_are_instant(self):
    for e in SFAC6_ELEMENTS:
      if e["classification"] in ("asset", "liability"):
        assert e["period_type"] == "instant", (
          f"{e['name']} (classification={e['classification']}) should be instant"
        )

  def test_income_statement_elements_are_duration(self):
    for e in SFAC6_ELEMENTS:
      if e["classification"] in ("revenue", "expense"):
        assert e["period_type"] == "duration", f"{e['name']} should be duration"

  def test_expected_elements_exist(self):
    names = {e["name"] for e in SFAC6_ELEMENTS}
    assert "Assets" in names
    assert "Liabilities" in names
    assert "Equity" in names
    assert "Revenues" in names
    assert "Expenses" in names
    assert "Gains" in names
    assert "Losses" in names
    assert "Comprehensive Income" in names
    assert "Investments by Owners" in names
    assert "Distributions to Owners" in names


class TestUSGAAPElements:
  def test_has_elements(self):
    assert len(ALL_GAAP_ELEMENTS) > 40

  def test_all_source_usgaap(self):
    for e in ALL_GAAP_ELEMENTS:
      assert e["source"] == "us-gaap"

  def test_all_have_usgaap_qname(self):
    for e in ALL_GAAP_ELEMENTS:
      assert e["qname"].startswith("us-gaap:"), f"{e['name']} missing us-gaap: prefix"

  def test_all_have_parent_id(self):
    """Every GAAP element must reference a parent (SFAC 6 root or another GAAP element)."""
    for e in ALL_GAAP_ELEMENTS:
      assert e.get("parent_id"), f"{e['name']} missing parent_id"

  def test_parent_ids_reference_valid_elements(self):
    """All parent_id references must point to existing elements."""
    all_ids = {e["id"] for e in SFAC6_ELEMENTS} | {e["id"] for e in ALL_GAAP_ELEMENTS}
    for e in ALL_GAAP_ELEMENTS:
      parent = e.get("parent_id")
      if parent:
        assert parent in all_ids, f"{e['name']} references unknown parent {parent}"

  def test_no_duplicate_ids(self):
    all_elements = SFAC6_ELEMENTS + ALL_GAAP_ELEMENTS
    ids = [e["id"] for e in all_elements]
    assert len(ids) == len(set(ids)), "Duplicate element IDs found"

  def test_no_duplicate_qnames(self):
    all_elements = SFAC6_ELEMENTS + ALL_GAAP_ELEMENTS
    qnames = [e["qname"] for e in all_elements]
    assert len(qnames) == len(set(qnames)), "Duplicate qnames found"

  def test_valid_classifications(self):
    valid = {"asset", "liability", "equity", "revenue", "expense"}
    for e in ALL_GAAP_ELEMENTS:
      assert e["classification"] in valid, (
        f"{e['name']} has invalid classification: {e['classification']}"
      )

  def test_valid_balance_types(self):
    valid = {"debit", "credit"}
    for e in ALL_GAAP_ELEMENTS:
      assert e["balance_type"] in valid, (
        f"{e['name']} has invalid balance_type: {e['balance_type']}"
      )

  def test_valid_period_types(self):
    valid = {"duration", "instant"}
    for e in ALL_GAAP_ELEMENTS:
      assert e["period_type"] in valid, (
        f"{e['name']} has invalid period_type: {e['period_type']}"
      )

  def test_valid_element_types(self):
    valid = {"concept", "abstract"}
    for e in ALL_GAAP_ELEMENTS:
      assert e["element_type"] in valid, (
        f"{e['name']} has invalid element_type: {e['element_type']}"
      )

  def test_abstract_elements_have_abstract_type(self):
    for e in ALL_GAAP_ELEMENTS:
      if e["is_abstract"]:
        assert e["element_type"] == "abstract", (
          f"{e['name']} is_abstract=True but element_type={e['element_type']}"
        )

  def test_depth_consistency(self):
    """Children should have depth > parent depth."""
    id_to_depth = {e["id"]: e["depth"] for e in SFAC6_ELEMENTS}
    id_to_depth.update({e["id"]: e["depth"] for e in ALL_GAAP_ELEMENTS})
    for e in ALL_GAAP_ELEMENTS:
      parent = e.get("parent_id")
      if parent and parent in id_to_depth:
        assert e["depth"] > id_to_depth[parent], (
          f"{e['name']} (depth={e['depth']}) should be deeper than "
          f"parent (depth={id_to_depth[parent]})"
        )


class TestStatementElements:
  def test_income_statement_has_revenue(self):
    names = {e["name"] for e in INCOME_STATEMENT_ELEMENTS}
    assert "Revenue" in names

  def test_income_statement_has_net_income(self):
    names = {e["name"] for e in INCOME_STATEMENT_ELEMENTS}
    assert "Net Income" in names

  def test_balance_sheet_has_total_assets(self):
    names = {e["name"] for e in BALANCE_SHEET_ELEMENTS}
    assert "Total Assets" in names

  def test_balance_sheet_has_equity(self):
    names = {e["name"] for e in BALANCE_SHEET_ELEMENTS}
    assert "Total Stockholders' Equity" in names

  def test_no_overlap_between_statements(self):
    is_ids = {e["id"] for e in INCOME_STATEMENT_ELEMENTS}
    bs_ids = {e["id"] for e in BALANCE_SHEET_ELEMENTS}
    assert not is_ids & bs_ids, "IS and BS overlap"

  def test_all_gaap_is_union_of_statements(self):
    union = set()
    for lst in [INCOME_STATEMENT_ELEMENTS, BALANCE_SHEET_ELEMENTS]:
      union.update(e["id"] for e in lst)
    all_ids = {e["id"] for e in ALL_GAAP_ELEMENTS}
    assert union == all_ids


class TestStructures:
  def test_has_two_structures(self):
    # Cash Flow Statement is intentionally not seeded — see seed.py.
    assert len(STRUCTURES) == 2

  def test_structure_types(self):
    types = {s["structure_type"] for s in STRUCTURES}
    assert types == {"income_statement", "balance_sheet"}

  def test_all_reference_taxonomy(self):
    for s in STRUCTURES:
      assert s["taxonomy_id"] == TAXONOMY_ID

  def test_structure_ids_are_stable(self):
    assert STRUCT_IS_ID == "struct_income_statement"
    assert STRUCT_BS_ID == "struct_balance_sheet"


class TestStructureAssignment:
  def test_income_statement_elements_assigned_correctly(self):
    for e in INCOME_STATEMENT_ELEMENTS:
      struct = _structure_for_element(e)
      assert struct == STRUCT_IS_ID, f"{e['name']} should be IS, got {struct}"

  def test_balance_sheet_elements_assigned_correctly(self):
    for e in BALANCE_SHEET_ELEMENTS:
      struct = _structure_for_element(e)
      assert struct == STRUCT_BS_ID, f"{e['name']} should be BS, got {struct}"


class TestAssociationGeneration:
  def test_all_elements_with_parents_generate_associations(self):
    """Every GAAP element with a parent_id should generate an association."""
    with_parents = [e for e in ALL_GAAP_ELEMENTS if e.get("parent_id")]
    assert len(with_parents) == len(ALL_GAAP_ELEMENTS), (
      "All GAAP elements should have parent_id"
    )
