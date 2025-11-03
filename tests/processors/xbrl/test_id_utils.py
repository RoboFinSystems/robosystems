from robosystems.processors.xbrl.id_utils import (
  create_element_id,
  create_label_id,
  create_taxonomy_id,
  create_reference_id,
  create_report_id,
  create_fact_id,
  create_entity_id,
  create_period_id,
  create_unit_id,
  create_factset_id,
  create_dimension_id,
  create_structure_id,
)


class TestElementID:
  def test_create_element_id_deterministic(self):
    uri = "http://example.com/element#Assets"
    id1 = create_element_id(uri)
    id2 = create_element_id(uri)

    assert id1 == id2

  def test_create_element_id_different_uris(self):
    uri1 = "http://example.com/element#Assets"
    uri2 = "http://example.com/element#Liabilities"

    id1 = create_element_id(uri1)
    id2 = create_element_id(uri2)

    assert id1 != id2

  def test_create_element_id_returns_string(self):
    uri = "http://example.com/element#Assets"
    result = create_element_id(uri)

    assert isinstance(result, str)
    assert len(result) > 0


class TestLabelID:
  def test_create_label_id_deterministic(self):
    value = "Total Assets"
    label_type = "label"
    language = "en-US"

    id1 = create_label_id(value, label_type, language)
    id2 = create_label_id(value, label_type, language)

    assert id1 == id2

  def test_create_label_id_different_values(self):
    label_type = "label"
    language = "en-US"

    id1 = create_label_id("Total Assets", label_type, language)
    id2 = create_label_id("Total Liabilities", label_type, language)

    assert id1 != id2

  def test_create_label_id_different_types(self):
    value = "Total Assets"
    language = "en-US"

    id1 = create_label_id(value, "label", language)
    id2 = create_label_id(value, "documentation", language)

    assert id1 != id2

  def test_create_label_id_different_languages(self):
    value = "Total Assets"
    label_type = "label"

    id1 = create_label_id(value, label_type, "en-US")
    id2 = create_label_id(value, label_type, "es-ES")

    assert id1 != id2


class TestTaxonomyID:
  def test_create_taxonomy_id_deterministic(self):
    uri = "http://fasb.org/us-gaap/2023"

    id1 = create_taxonomy_id(uri)
    id2 = create_taxonomy_id(uri)

    assert id1 == id2

  def test_create_taxonomy_id_different_uris(self):
    id1 = create_taxonomy_id("http://fasb.org/us-gaap/2023")
    id2 = create_taxonomy_id("http://fasb.org/us-gaap/2024")

    assert id1 != id2


class TestReferenceID:
  def test_create_reference_id_deterministic(self):
    value = "Topic 220"
    ref_type = "standard"

    id1 = create_reference_id(value, ref_type)
    id2 = create_reference_id(value, ref_type)

    assert id1 == id2

  def test_create_reference_id_different_values(self):
    ref_type = "standard"

    id1 = create_reference_id("Topic 220", ref_type)
    id2 = create_reference_id("Topic 230", ref_type)

    assert id1 != id2

  def test_create_reference_id_different_types(self):
    value = "Topic 220"

    id1 = create_reference_id(value, "standard")
    id2 = create_reference_id(value, "regulation")

    assert id1 != id2


class TestReportID:
  def test_create_report_id_deterministic(self):
    uri = (
      "https://sec.gov/Archives/edgar/data/320193/000032019323000077/aapl-20230930.xml"
    )

    id1 = create_report_id(uri)
    id2 = create_report_id(uri)

    assert id1 == id2

  def test_create_report_id_different_reports(self):
    id1 = create_report_id(
      "https://sec.gov/Archives/edgar/data/320193/000032019323000077/aapl-20230930.xml"
    )
    id2 = create_report_id(
      "https://sec.gov/Archives/edgar/data/320193/000032019323000078/aapl-20231231.xml"
    )

    assert id1 != id2


class TestFactID:
  def test_create_fact_id_deterministic(self):
    fact_uri = "fact://aapl-20230930/Assets/instant"

    id1 = create_fact_id(fact_uri)
    id2 = create_fact_id(fact_uri)

    assert id1 == id2

  def test_create_fact_id_different_facts(self):
    id1 = create_fact_id("fact://aapl-20230930/Assets/instant")
    id2 = create_fact_id("fact://aapl-20230930/Liabilities/instant")

    assert id1 != id2


class TestEntityID:
  def test_create_entity_id_deterministic(self):
    entity_uri = "http://www.sec.gov/CIK0000320193"

    id1 = create_entity_id(entity_uri)
    id2 = create_entity_id(entity_uri)

    assert id1 == id2

  def test_create_entity_id_different_entities(self):
    id1 = create_entity_id("http://www.sec.gov/CIK0000320193")
    id2 = create_entity_id("http://www.sec.gov/CIK0001318605")

    assert id1 != id2


class TestPeriodID:
  def test_create_period_id_deterministic(self):
    period_uri = "period://2023-09-30/instant"

    id1 = create_period_id(period_uri)
    id2 = create_period_id(period_uri)

    assert id1 == id2

  def test_create_period_id_different_periods(self):
    id1 = create_period_id("period://2023-09-30/instant")
    id2 = create_period_id("period://2023-12-31/instant")

    assert id1 != id2


class TestUnitID:
  def test_create_unit_id_deterministic(self):
    unit_uri = "unit://usd"

    id1 = create_unit_id(unit_uri)
    id2 = create_unit_id(unit_uri)

    assert id1 == id2

  def test_create_unit_id_different_units(self):
    id1 = create_unit_id("unit://usd")
    id2 = create_unit_id("unit://shares")

    assert id1 != id2


class TestFactSetID:
  def test_create_factset_id_deterministic(self):
    factset_uri = "factset://aapl-20230930"

    id1 = create_factset_id(factset_uri)
    id2 = create_factset_id(factset_uri)

    assert id1 == id2

  def test_create_factset_id_different_sets(self):
    id1 = create_factset_id("factset://aapl-20230930")
    id2 = create_factset_id("factset://msft-20230930")

    assert id1 != id2


class TestDimensionID:
  def test_create_dimension_id_deterministic(self):
    dimension_uri = "dimension://segment/geographic"

    id1 = create_dimension_id(dimension_uri)
    id2 = create_dimension_id(dimension_uri)

    assert id1 == id2

  def test_create_dimension_id_different_dimensions(self):
    id1 = create_dimension_id("dimension://segment/geographic")
    id2 = create_dimension_id("dimension://segment/product")

    assert id1 != id2


class TestStructureID:
  def test_create_structure_id_deterministic(self):
    structure_uri = "structure://presentation/balance-sheet"

    id1 = create_structure_id(structure_uri)
    id2 = create_structure_id(structure_uri)

    assert id1 == id2

  def test_create_structure_id_different_structures(self):
    id1 = create_structure_id("structure://presentation/balance-sheet")
    id2 = create_structure_id("structure://presentation/income-statement")

    assert id1 != id2


class TestIDConsistencyAcrossFunctions:
  def test_same_namespace_different_content(self):
    elem_id1 = create_element_id("test1")
    elem_id2 = create_element_id("test2")

    assert elem_id1 != elem_id2

  def test_different_namespaces_same_content(self):
    content = "test_content"

    elem_id = create_element_id(content)
    fact_id = create_fact_id(content)
    entity_id = create_entity_id(content)

    assert elem_id != fact_id
    assert fact_id != entity_id
    assert elem_id != entity_id
