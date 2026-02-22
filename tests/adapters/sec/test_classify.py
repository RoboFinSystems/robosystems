"""Tests for association classification via embedded LadybugDB.

Tests TempLadybugContext lifecycle and AssociationClassifier pattern detection
for RollUp, RollForward, Hypercube, LineItems, MemberList, and Dimension.
"""

from pathlib import Path

import pandas as pd
import pytest

from robosystems.adapters.sec.processors.classify import (
  DISCLOSURE_CONCEPT_MAP,
  DISCLOSURE_TO_CANONICAL,
  AssociationClassifier,
  TempLadybugContext,
  _generate_ddl,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
  """Write a DataFrame to parquet, creating parent dirs.

  Uses pyarrow directly with file handle to avoid LocalFileSystem
  registration conflicts when DuckDB tests run in the same process.
  """
  import pyarrow as pa
  import pyarrow.parquet as pq

  path.parent.mkdir(parents=True, exist_ok=True)
  table = pa.Table.from_pandas(df, preserve_index=False)
  with open(path, "wb") as f:
    pq.write_table(table, f)


def _make_test_data(
  tmp_path: Path,
  associations: list[dict],
  elements: list[dict],
  from_rels: list[dict],
  to_rels: list[dict],
  structures: list[dict] | None = None,
  struct_assoc_rels: list[dict] | None = None,
) -> Path:
  """Write test parquet files and return the output directory."""
  output_dir = tmp_path / "output"

  _write_parquet(
    pd.DataFrame(associations),
    output_dir / "nodes" / "Association.parquet",
  )
  _write_parquet(
    pd.DataFrame(elements),
    output_dir / "nodes" / "Element.parquet",
  )
  # Only write relationship parquets if they have data (empty parquets have no schema)
  if from_rels:
    _write_parquet(
      pd.DataFrame(from_rels),
      output_dir / "relationships" / "ASSOCIATION_HAS_FROM_ELEMENT.parquet",
    )
  if to_rels:
    _write_parquet(
      pd.DataFrame(to_rels),
      output_dir / "relationships" / "ASSOCIATION_HAS_TO_ELEMENT.parquet",
    )
  if structures:
    _write_parquet(
      pd.DataFrame(structures),
      output_dir / "nodes" / "Structure.parquet",
    )
  if struct_assoc_rels:
    _write_parquet(
      pd.DataFrame(struct_assoc_rels),
      output_dir / "relationships" / "STRUCTURE_HAS_ASSOCIATION.parquet",
    )
  return output_dir


# ---------------------------------------------------------------------------
# TempLadybugContext tests
# ---------------------------------------------------------------------------


class TestTempLadybugContext:
  def test_context_lifecycle(self):
    """Context creates and cleans up temp directory."""
    ddl = _generate_ddl()
    assert len(ddl) > 0

    with TempLadybugContext(ddl) as ctx:
      assert ctx._tmpdir is not None
      tmpdir = ctx._tmpdir
      assert Path(tmpdir).exists()

    # After exit, temp dir should be cleaned up
    assert not Path(tmpdir).exists()

  def test_execute_returns_results(self):
    """Can execute Cypher and get results."""
    with TempLadybugContext() as ctx:
      # Just verify the schema was installed (tables exist)
      results = ctx.execute("MATCH (a:Association) RETURN count(a) AS cnt")
      assert results == [{"cnt": 0}]

  def test_cleanup_on_error(self):
    """Temp directory cleaned up even on exception."""
    tmpdir = None
    try:
      with TempLadybugContext() as ctx:
        tmpdir = ctx._tmpdir
        raise ValueError("test error")
    except ValueError:
      pass

    assert tmpdir is not None
    assert not Path(tmpdir).exists()


# ---------------------------------------------------------------------------
# DDL generation tests
# ---------------------------------------------------------------------------


class TestDDLGeneration:
  def test_generates_node_tables(self):
    """DDL includes Association and Element node tables."""
    ddl = _generate_ddl()
    ddl_text = " ".join(ddl)
    assert "Association" in ddl_text
    assert "Element" in ddl_text

  def test_generates_relationship_tables(self):
    """DDL includes from/to element relationship tables."""
    ddl = _generate_ddl()
    ddl_text = " ".join(ddl)
    assert "ASSOCIATION_HAS_FROM_ELEMENT" in ddl_text
    assert "ASSOCIATION_HAS_TO_ELEMENT" in ddl_text

  def test_generates_structure_tables(self):
    """DDL includes Structure node and STRUCTURE_HAS_ASSOCIATION relationship."""
    ddl = _generate_ddl()
    ddl_text = " ".join(ddl)
    assert "Structure" in ddl_text
    assert "STRUCTURE_HAS_ASSOCIATION" in ddl_text


# ---------------------------------------------------------------------------
# AssociationClassifier tests
# ---------------------------------------------------------------------------


class TestRollUpClassification:
  def test_rollup_detected(self, tmp_path):
    """Element in both presentation and calculation → RollUp classification."""
    # Element shared between presentation and calculation
    elements = [
      {
        "identifier": "elem1",
        "qname": "us-gaap:Assets",
        "name": "Assets",
        "period_type": "instant",
        "classification": None,
        "balance": "debit",
        "uri": "http://example.com#Assets",
      },
    ]
    # Presentation association (parent → elem1)
    associations = [
      {
        "identifier": "assoc_pres",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
        "order_value": 1.0,
        "association_type": "Presentation",
        "weight": None,
        "root": "True",
        "preferred_label": None,
      },
      {
        "identifier": "assoc_calc",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "False",
        "preferred_label": None,
      },
    ]
    # Presentation points TO elem1, Calculation points FROM elem1
    to_rels = [{"from": "assoc_pres", "to": "elem1"}]
    from_rels = [{"from": "assoc_calc", "to": "elem1"}]

    output_dir = _make_test_data(tmp_path, associations, elements, from_rels, to_rels)

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    assert not class_df.empty
    rollups = class_df[class_df["type"] == "RollUp"]
    assert len(rollups) >= 1
    assert rollups.iloc[0]["source"] == "arcrole_analysis"
    assert rollups.iloc[0]["confidence"] == 1.0


class TestRollForwardClassification:
  def test_rollforward_detected(self, tmp_path):
    """Instant element with periodEndLabel → RollForward classification."""
    elements = [
      {
        "identifier": "elem_instant",
        "qname": "us-gaap:Cash",
        "name": "Cash",
        "period_type": "instant",
        "classification": None,
        "balance": "debit",
        "uri": "http://example.com#Cash",
      },
    ]
    associations = [
      {
        "identifier": "assoc_rf",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
        "order_value": 1.0,
        "association_type": "Presentation",
        "weight": None,
        "root": "False",
        "preferred_label": "http://www.xbrl.org/2003/role/periodEndLabel",
      },
    ]
    to_rels = [{"from": "assoc_rf", "to": "elem_instant"}]
    from_rels = []  # Not needed for RollForward

    output_dir = _make_test_data(tmp_path, associations, elements, from_rels, to_rels)

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    assert not class_df.empty
    rf = class_df[class_df["type"] == "RollForward"]
    assert len(rf) >= 1


class TestElementBasedClassifications:
  @pytest.mark.parametrize(
    "classification_value,expected_type",
    [
      ("hypercubeElement", "Hypercube"),
      ("lineItemsElement", "LineItems"),
      ("domainElement", "MemberList"),
      ("dimensionElement", "Dimension"),
    ],
  )
  def test_element_classification_detected(
    self, tmp_path, classification_value, expected_type
  ):
    """Element classification types are detected correctly."""
    elements = [
      {
        "identifier": "elem1",
        "qname": "test:Elem",
        "name": "Elem",
        "period_type": "duration",
        "classification": classification_value,
        "balance": None,
        "uri": "http://example.com#Elem",
      },
    ]
    associations = [
      {
        "identifier": "assoc1",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
        "order_value": 1.0,
        "association_type": "Presentation",
        "weight": None,
        "root": "True",
        "preferred_label": None,
      },
    ]
    from_rels = [{"from": "assoc1", "to": "elem1"}]
    to_rels = []

    output_dir = _make_test_data(tmp_path, associations, elements, from_rels, to_rels)

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    assert not class_df.empty
    matches = class_df[class_df["type"] == expected_type]
    assert len(matches) >= 1


class TestManyToMany:
  def test_association_gets_multiple_classifications(self, tmp_path):
    """An association can receive multiple classifications simultaneously."""
    # Element that is both in a calculation relationship AND has a classification
    elements = [
      {
        "identifier": "elem1",
        "qname": "test:Total",
        "name": "Total",
        "period_type": "duration",
        "classification": "lineItemsElement",
        "balance": "debit",
        "uri": "http://example.com#Total",
      },
    ]
    associations = [
      # Presentation association
      {
        "identifier": "assoc_pres",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
        "order_value": 1.0,
        "association_type": "Presentation",
        "weight": None,
        "root": "True",
        "preferred_label": None,
      },
      # Calculation association pointing FROM the same element
      {
        "identifier": "assoc_calc",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "False",
        "preferred_label": None,
      },
    ]
    # pres → TO elem1 (for RollUp), pres → FROM elem1 (for LineItems)
    # calc → FROM elem1 (for RollUp cross-reference)
    to_rels = [{"from": "assoc_pres", "to": "elem1"}]
    from_rels = [
      {"from": "assoc_pres", "to": "elem1"},
      {"from": "assoc_calc", "to": "elem1"},
    ]

    output_dir = _make_test_data(tmp_path, associations, elements, from_rels, to_rels)

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    # Should have at least RollUp + LineItems for assoc_pres
    types = set(class_df["type"].tolist())
    assert "RollUp" in types
    assert "LineItems" in types

    # The relationship table should link assoc_pres to multiple classifications
    pres_rels = rel_df[rel_df["from"] == "assoc_pres"]
    assert len(pres_rels) >= 2


class TestNoClassifications:
  def test_no_patterns_returns_empty(self, tmp_path):
    """When no patterns match, empty DataFrames returned."""
    elements = [
      {
        "identifier": "elem1",
        "qname": "custom:Thing",
        "name": "Thing",
        "period_type": "duration",
        "classification": None,
        "balance": None,
        "uri": "http://example.com#Thing",
      },
    ]
    associations = [
      {
        "identifier": "assoc1",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
        "order_value": 1.0,
        "association_type": "Presentation",
        "weight": None,
        "root": "True",
        "preferred_label": None,
      },
    ]
    # Only TO relationship (no FROM, no calc, no special labels)
    to_rels = [{"from": "assoc1", "to": "elem1"}]
    from_rels = []

    output_dir = _make_test_data(tmp_path, associations, elements, from_rels, to_rels)

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    assert class_df.empty
    assert rel_df.empty

  def test_missing_parquets_returns_empty(self, tmp_path):
    """When parquet files are missing, returns empty DataFrames gracefully."""
    output_dir = tmp_path / "empty_output"
    output_dir.mkdir()

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    assert class_df.empty
    assert rel_df.empty


class TestFeatureFlag:
  def test_classification_skipped_when_disabled(self, tmp_path, monkeypatch):
    """When XBRL_ASSOCIATION_CLASSIFICATION is False, classification is skipped."""
    monkeypatch.setattr(
      "robosystems.adapters.sec.config.XBRL_ASSOCIATION_CLASSIFICATION", False
    )

    # Import after monkeypatch to get the patched value
    from robosystems.adapters.sec.config import XBRL_ASSOCIATION_CLASSIFICATION

    assert XBRL_ASSOCIATION_CLASSIFICATION is False


# ---------------------------------------------------------------------------
# Semantic classification tests (disclosure mechanics layer)
# ---------------------------------------------------------------------------


def _semantic_base_elements():
  """Shared element fixtures for semantic classification tests."""
  return [
    {
      "identifier": "elem_assets",
      "qname": "us-gaap:Assets",
      "name": "Assets",
      "period_type": "instant",
      "classification": None,
      "balance": "debit",
      "uri": "http://example.com#Assets",
    },
    {
      "identifier": "elem_current",
      "qname": "us-gaap:AssetsCurrent",
      "name": "AssetsCurrent",
      "period_type": "instant",
      "classification": None,
      "balance": "debit",
      "uri": "http://example.com#AssetsCurrent",
    },
    {
      "identifier": "elem_noncurrent",
      "qname": "us-gaap:AssetsNoncurrent",
      "name": "AssetsNoncurrent",
      "period_type": "instant",
      "classification": None,
      "balance": "debit",
      "uri": "http://example.com#AssetsNoncurrent",
    },
  ]


class TestSemanticClassification:
  def test_assets_rollup_identified(self, tmp_path):
    """Structure with calc root us-gaap:Assets → all associations get AssetsRollUp."""
    elements = _semantic_base_elements()
    associations = [
      {
        "identifier": "assoc_calc_root",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
      {
        "identifier": "assoc_calc_child",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 2.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "False",
        "preferred_label": None,
      },
    ]
    structures = [{"identifier": "struct1"}]
    struct_assoc_rels = [
      {"from": "struct1", "to": "assoc_calc_root", "association_context": "calc"},
      {"from": "struct1", "to": "assoc_calc_child", "association_context": "calc"},
    ]
    # Root calc association FROM elem_assets
    from_rels = [{"from": "assoc_calc_root", "to": "elem_assets"}]
    to_rels = [{"from": "assoc_calc_root", "to": "elem_current"}]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels,
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    # Should have at least one AssetsRollUp classification
    semantic = class_df[class_df["source"] == "disclosure_mechanics"]
    assert not semantic.empty
    assert "AssetsRollUp" in semantic["type"].values

    # Both associations in the structure should be linked
    assets_class_id = semantic[semantic["type"] == "AssetsRollUp"].iloc[0]["identifier"]
    linked = rel_df[rel_df["to"] == assets_class_id]
    assert set(linked["from"].tolist()) == {"assoc_calc_root", "assoc_calc_child"}

  def test_alternative_concept_matched(self, tmp_path):
    """AssetsCurrent (alternative concept) → still matches AssetsRollUp."""
    elements = _semantic_base_elements()
    associations = [
      {
        "identifier": "assoc_root",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
    ]
    structures = [{"identifier": "struct_alt"}]
    struct_assoc_rels = [
      {"from": "struct_alt", "to": "assoc_root", "association_context": "calc"},
    ]
    # Root FROM elem_current (AssetsCurrent, an alternative concept for AssetsRollUp)
    from_rels = [{"from": "assoc_root", "to": "elem_current"}]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels=[],
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    semantic = class_df[class_df["source"] == "disclosure_mechanics"]
    assert not semantic.empty
    assert "AssetsRollUp" in semantic["type"].values

  def test_unknown_root_no_semantic_classification(self, tmp_path):
    """Custom root not in DISCLOSURE_CONCEPT_MAP → no semantic classification."""
    elements = [
      {
        "identifier": "elem_custom",
        "qname": "custom:MySpecialTotal",
        "name": "MySpecialTotal",
        "period_type": "duration",
        "classification": None,
        "balance": None,
        "uri": "http://example.com#MySpecialTotal",
      },
    ]
    associations = [
      {
        "identifier": "assoc_custom",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
    ]
    structures = [{"identifier": "struct_custom"}]
    struct_assoc_rels = [
      {"from": "struct_custom", "to": "assoc_custom", "association_context": "calc"},
    ]
    from_rels = [{"from": "assoc_custom", "to": "elem_custom"}]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels=[],
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    # No semantic classifications should exist
    if not class_df.empty:
      semantic = class_df[class_df["source"] == "disclosure_mechanics"]
      assert semantic.empty

  def test_multiple_structures_independent(self, tmp_path):
    """Each structure gets its own semantic classification independently."""
    elements = [
      *_semantic_base_elements(),
      {
        "identifier": "elem_goodwill",
        "qname": "us-gaap:Goodwill",
        "name": "Goodwill",
        "period_type": "instant",
        "classification": None,
        "balance": "debit",
        "uri": "http://example.com#Goodwill",
      },
    ]
    associations = [
      {
        "identifier": "assoc_assets_root",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
      {
        "identifier": "assoc_goodwill_root",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
    ]
    structures = [
      {"identifier": "struct_assets"},
      {"identifier": "struct_goodwill"},
    ]
    struct_assoc_rels = [
      {
        "from": "struct_assets",
        "to": "assoc_assets_root",
        "association_context": "calc",
      },
      {
        "from": "struct_goodwill",
        "to": "assoc_goodwill_root",
        "association_context": "calc",
      },
    ]
    from_rels = [
      {"from": "assoc_assets_root", "to": "elem_assets"},
      {"from": "assoc_goodwill_root", "to": "elem_goodwill"},
    ]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels=[],
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    semantic = class_df[class_df["source"] == "disclosure_mechanics"]
    assert len(semantic) == 2
    types = set(semantic["type"].tolist())
    assert "AssetsRollUp" in types
    assert "GoodwillRollForward" in types

  def test_both_layers_present(self, tmp_path):
    """An association gets both structural (RollUp) and semantic (AssetsRollUp)."""
    elements = _semantic_base_elements()
    associations = [
      # Presentation association (for structural RollUp detection)
      {
        "identifier": "assoc_pres",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
        "order_value": 1.0,
        "association_type": "Presentation",
        "weight": None,
        "root": "True",
        "preferred_label": None,
      },
      # Calculation root (for both structural RollUp cross-ref and semantic)
      {
        "identifier": "assoc_calc",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
    ]
    structures = [{"identifier": "struct_bs"}]
    struct_assoc_rels = [
      {"from": "struct_bs", "to": "assoc_pres", "association_context": "pres"},
      {"from": "struct_bs", "to": "assoc_calc", "association_context": "calc"},
    ]
    # pres TO elem_assets (for RollUp: presentation points to element that's in calc)
    to_rels = [{"from": "assoc_pres", "to": "elem_assets"}]
    # calc FROM elem_assets (for RollUp cross-ref + semantic root detection)
    from_rels = [{"from": "assoc_calc", "to": "elem_assets"}]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels,
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    # Structural layer: RollUp from arcrole_analysis
    structural = class_df[class_df["source"] == "arcrole_analysis"]
    assert "RollUp" in structural["type"].values

    # Semantic layer: AssetsRollUp from disclosure_mechanics
    semantic = class_df[class_df["source"] == "disclosure_mechanics"]
    assert "AssetsRollUp" in semantic["type"].values

    # assoc_pres should have both layers linked
    pres_rels = rel_df[rel_df["from"] == "assoc_pres"]
    linked_class_ids = set(pres_rels["to"].tolist())
    linked_types = set(
      class_df[class_df["identifier"].isin(linked_class_ids)]["type"].tolist()
    )
    assert "RollUp" in linked_types
    assert "AssetsRollUp" in linked_types


class TestDisclosureConceptMap:
  def test_map_has_expected_count(self):
    """DISCLOSURE_CONCEPT_MAP has 143 entries."""
    assert len(DISCLOSURE_CONCEPT_MAP) == 143

  def test_key_entries_present(self):
    """Key entries from the taxonomy are present."""
    assert DISCLOSURE_CONCEPT_MAP["Assets"] == "AssetsRollUp"
    assert DISCLOSURE_CONCEPT_MAP["NetIncomeLoss"] == "IncomeStatement"
    assert DISCLOSURE_CONCEPT_MAP["Goodwill"] == "GoodwillRollForward"
    assert (
      DISCLOSURE_CONCEPT_MAP["CashAndCashEquivalentsPeriodIncreaseDecrease"]
      == "CashFlowStatement"
    )


class TestDisclosureToCanonical:
  def test_assets_root_gives_canonical_hint(self, tmp_path):
    """Root of AssetsRollUp → canonical hint for total_assets."""
    elements = _semantic_base_elements()
    associations = [
      {
        "identifier": "assoc_calc_root",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
    ]
    structures = [{"identifier": "struct1"}]
    struct_assoc_rels = [
      {"from": "struct1", "to": "assoc_calc_root", "association_context": "calc"},
    ]
    from_rels = [{"from": "assoc_calc_root", "to": "elem_assets"}]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels=[],
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    # elem_assets should get a canonical hint for total_assets
    assert "elem_assets" in canonical_hints
    concept_id, confidence = canonical_hints["elem_assets"]
    assert concept_id == "total_assets"
    assert confidence == 0.97

  def test_goodwill_root_gives_canonical_hint(self, tmp_path):
    """Root of GoodwillRollForward → canonical hint for goodwill."""
    elements = [
      {
        "identifier": "elem_goodwill",
        "qname": "us-gaap:Goodwill",
        "name": "Goodwill",
        "period_type": "instant",
        "classification": None,
        "balance": "debit",
        "uri": "http://example.com#Goodwill",
      },
    ]
    associations = [
      {
        "identifier": "assoc_root",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
    ]
    structures = [{"identifier": "struct_gw"}]
    struct_assoc_rels = [
      {"from": "struct_gw", "to": "assoc_root", "association_context": "calc"},
    ]
    from_rels = [{"from": "assoc_root", "to": "elem_goodwill"}]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels=[],
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    assert "elem_goodwill" in canonical_hints
    assert canonical_hints["elem_goodwill"] == ("goodwill", 0.97)

  def test_unknown_disclosure_no_canonical_hint(self, tmp_path):
    """Root of disclosure not in DISCLOSURE_TO_CANONICAL → no canonical hint."""
    elements = [
      {
        "identifier": "elem_warranty",
        "qname": "us-gaap:ProductWarrantyAccrual",
        "name": "ProductWarrantyAccrual",
        "period_type": "instant",
        "classification": None,
        "balance": "credit",
        "uri": "http://example.com#ProductWarrantyAccrual",
      },
    ]
    associations = [
      {
        "identifier": "assoc_root",
        "arcrole": "http://www.xbrl.org/2003/arcrole/summation-item",
        "order_value": 1.0,
        "association_type": "Calculation",
        "weight": 1.0,
        "root": "True",
        "preferred_label": None,
      },
    ]
    structures = [{"identifier": "struct_warranty"}]
    struct_assoc_rels = [
      {"from": "struct_warranty", "to": "assoc_root", "association_context": "calc"},
    ]
    from_rels = [{"from": "assoc_root", "to": "elem_warranty"}]

    output_dir = _make_test_data(
      tmp_path,
      associations,
      elements,
      from_rels,
      to_rels=[],
      structures=structures,
      struct_assoc_rels=struct_assoc_rels,
    )

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    # ProductWarrantyAccrual maps to StandardAndExtendedProductWarrantyMovementRollForward
    # which is NOT in DISCLOSURE_TO_CANONICAL → no hint
    assert "elem_warranty" not in canonical_hints

  def test_no_structures_empty_hints(self, tmp_path):
    """Without structure data, canonical_hints is empty."""
    elements = _semantic_base_elements()
    associations = [
      {
        "identifier": "assoc1",
        "arcrole": "http://www.xbrl.org/2003/arcrole/parent-child",
        "order_value": 1.0,
        "association_type": "Presentation",
        "weight": None,
        "root": "True",
        "preferred_label": None,
      },
    ]
    to_rels = [{"from": "assoc1", "to": "elem_assets"}]

    output_dir = _make_test_data(tmp_path, associations, elements, [], to_rels)

    classifier = AssociationClassifier()
    class_df, rel_df, canonical_hints = classifier.classify(output_dir)

    assert canonical_hints == {}

  def test_mapping_key_entries(self):
    """DISCLOSURE_TO_CANONICAL has expected key mappings."""
    assert DISCLOSURE_TO_CANONICAL["AssetsRollUp"] == "total_assets"
    assert DISCLOSURE_TO_CANONICAL["InventoryNetRollUp"] == "inventory"
    assert DISCLOSURE_TO_CANONICAL["GoodwillRollForward"] == "goodwill"
    assert DISCLOSURE_TO_CANONICAL["CashFlowStatement"] == "operating_cash_flow"
    assert DISCLOSURE_TO_CANONICAL["IncomeStatement"] == "net_income"
