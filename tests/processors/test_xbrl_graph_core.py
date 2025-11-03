"""
Comprehensive tests for core XBRL Graph Processor methods.

Tests the critical data extraction logic including:
- Fact processing (make_fact)
- Unit processing (make_units)
- Period processing (make_period)
- Element processing (make_element, make_element_labels)
"""

import pytest
import tempfile
import shutil
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from robosystems.processors.xbrl_graph import XBRLGraphProcessor


@pytest.fixture
def temp_dir():
  """Create temporary directory for test outputs."""
  temp = tempfile.mkdtemp()
  yield temp
  shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def mock_schema_config():
  """Standard schema configuration for tests."""
  return {
    "nodes": {
      "Fact": {"properties": [{"name": "identifier", "type": "STRING"}]},
      "Unit": {"properties": [{"name": "identifier", "type": "STRING"}]},
      "Period": {"properties": [{"name": "identifier", "type": "STRING"}]},
      "Element": {"properties": [{"name": "identifier", "type": "STRING"}]},
      "Label": {"properties": [{"name": "identifier", "type": "STRING"}]},
    },
    "relationships": {
      "FACT_HAS_UNIT": {"from": "Fact", "to": "Unit"},
      "FACT_HAS_PERIOD": {"from": "Fact", "to": "Period"},
      "FACT_HAS_ELEMENT": {"from": "Fact", "to": "Element"},
    },
  }


@pytest.fixture
def processor(temp_dir, mock_schema_config):
  """Create initialized processor for testing."""
  with (
    patch(
      "robosystems.processors.xbrl_graph.XBRLSchemaAdapter"
    ) as mock_schema_adapter_class,
    patch(
      "robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator"
    ) as mock_config_gen_class,
    patch(
      "robosystems.processors.xbrl_graph.DataFrameManager"
    ) as mock_df_manager_class,
  ):
    mock_schema_instance = MagicMock()
    mock_config_gen_instance = MagicMock()
    mock_df_manager_instance = MagicMock()

    mock_schema_adapter_class.return_value = mock_schema_instance
    mock_config_gen_class.return_value = mock_config_gen_instance
    mock_df_manager_class.return_value = mock_df_manager_instance

    mock_schema_builder = MagicMock()
    mock_schema = MagicMock()
    mock_node = MagicMock()
    mock_node.name = "Fact"
    mock_schema.nodes = [mock_node]
    mock_schema.relationships = []
    mock_schema_builder.schema = mock_schema
    mock_schema_instance.schema_builder = mock_schema_builder

    def mock_process_dataframe(table_name, data):
      """Mock schema adapter to convert data dict to DataFrame."""
      if isinstance(data, dict):
        return pd.DataFrame([data])
      return data

    mock_schema_instance.process_dataframe_for_schema.side_effect = (
      mock_process_dataframe
    )

    dataframes = {
      "facts_df": pd.DataFrame(
        columns=[
          "identifier",
          "uri",
          "value",
          "numeric_value",
          "fact_type",
          "decimals",
          "value_type",
          "content_type",
        ]
      ),
      "units_df": pd.DataFrame(
        columns=[
          "identifier",
          "measure",
          "value",
          "uri",
          "numerator_uri",
          "denominator_uri",
        ]
      ),
      "periods_df": pd.DataFrame(
        columns=[
          "identifier",
          "start_date",
          "end_date",
          "instant",
          "period_type",
          "fiscal_year",
          "fiscal_quarter",
          "days_in_period",
          "is_ytd",
        ]
      ),
      "elements_df": pd.DataFrame(
        columns=[
          "identifier",
          "qname",
          "name",
          "namespace",
          "type",
          "is_abstract",
          "substitution_group",
          "period_type",
          "balance_type",
        ]
      ),
      "labels_df": pd.DataFrame(columns=["identifier", "type", "language", "value"]),
      "references_df": pd.DataFrame(columns=["identifier", "role", "name", "value"]),
      "entities_df": pd.DataFrame(
        columns=[
          "identifier",
          "uri",
          "name",
          "scheme",
          "is_parent",
          "parent_entity_id",
          "entity_type",
        ]
      ),
      "fact_units_df": pd.DataFrame(columns=["from", "to", "unit_context"]),
      "fact_periods_df": pd.DataFrame(columns=["from", "to", "period_context"]),
      "fact_elements_df": pd.DataFrame(columns=["from", "to", "element_context"]),
      "fact_entities_df": pd.DataFrame(columns=["from", "to", "entity_context"]),
      "report_facts_df": pd.DataFrame(columns=["from", "to", "fact_context"]),
      "element_labels_df": pd.DataFrame(columns=["from", "to", "label_context"]),
    }
    mock_df_manager_instance.initialize_all_dataframes.return_value = dataframes

    proc = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test_entity",
      output_dir=temp_dir,
      schema_config=mock_schema_config,
    )

    proc.entity_data = {"identifier": "entity123"}
    proc.report_data = {"identifier": "report123", "uri": "file:///test.xml"}
    proc.report_factset_id = "factset123"

    return proc


class TestMakeFact:
  """Test fact processing logic."""

  def test_make_fact_numeric_with_decimals(self, processor):
    """Test processing numeric fact with decimal scaling."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "1000000"
      xfact.decimals = "-6"  # Millions
      xfact.unit = MagicMock()
      xfact.context = MagicMock()

      processor.make_fact(xfact)

      assert len(processor.facts_df) == 1
      fact = processor.facts_df.iloc[0]

      assert fact["value"] == "1000000"
      assert fact["numeric_value"] == 1.0  # 1000000 * 10^-6 = 1.0
      assert fact["fact_type"] == "Numeric"
      assert fact["decimals"] == "-6"

  def test_make_fact_numeric_without_decimals(self, processor):
    """Test processing numeric fact without decimals."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "42.5"
      xfact.decimals = None
      xfact.unit = MagicMock()
      xfact.context = MagicMock()

      processor.make_fact(xfact)

      assert len(processor.facts_df) == 1
      fact = processor.facts_df.iloc[0]

      assert fact["numeric_value"] == 42.5
      assert fact["decimals"] is None

  def test_make_fact_non_numeric(self, processor):
    """Test processing non-numeric (text) fact."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "Some text content"
      xfact.unit = None
      xfact.context = MagicMock()

      processor.make_fact(xfact)

      assert len(processor.facts_df) == 1
      fact = processor.facts_df.iloc[0]

      assert fact["value"] == "Some text content"
      assert fact["numeric_value"] is None
      assert fact["fact_type"] == "Nonnumeric"
      assert fact["decimals"] is None

  def test_make_fact_duplicate_prevention(self, processor):
    """Test that duplicate facts are not created."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "100"
      xfact.unit = MagicMock()
      xfact.context = MagicMock()

      processor.make_fact(xfact)
      assert len(processor.facts_df) == 1

      processor.make_fact(xfact)
      assert len(processor.facts_df) == 1

  def test_make_fact_with_html_content(self, processor):
    """Test fact with HTML content that should be externalized."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      processor.textblock_externalizer.should_externalize = MagicMock(return_value=True)
      processor.textblock_externalizer.queue_value_for_s3 = MagicMock(
        return_value={
          "url": "https://cdn.example.com/fact.html",
          "value_type": "external",
          "content_type": "text/html",
        }
      )

      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "<div>Large HTML content</div>" * 1000
      xfact.unit = None
      xfact.context = MagicMock()

      processor.make_fact(xfact)

      assert len(processor.facts_df) == 1
      fact = processor.facts_df.iloc[0]

      assert fact["value"] == "https://cdn.example.com/fact.html"
      assert fact["value_type"] == "external"
      assert fact["content_type"] == "text/html"

  def test_make_fact_externalization_failure(self, processor):
    """Test fallback when externalization fails."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      processor.textblock_externalizer.should_externalize = MagicMock(return_value=True)
      processor.textblock_externalizer.queue_value_for_s3 = MagicMock(return_value=None)

      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "<div>Large HTML content</div>" * 1000
      xfact.unit = None
      xfact.context = MagicMock()

      processor.make_fact(xfact)

      assert len(processor.facts_df) == 1
      fact = processor.facts_df.iloc[0]

      assert "<div>" in fact["value"]
      assert fact["value_type"] == "inline"

  def test_make_fact_invalid_numeric_value(self, processor):
    """Test handling of invalid numeric values."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "not-a-number"
      xfact.decimals = "0"
      xfact.unit = MagicMock()
      xfact.context = MagicMock()

      processor.make_fact(xfact)

      assert len(processor.facts_df) == 1
      fact = processor.facts_df.iloc[0]

      assert fact["numeric_value"] is None
      assert fact["fact_type"] == "Numeric"

  def test_make_fact_creates_relationships(self, processor):
    """Test that fact creates proper relationships."""
    with (
      patch.object(processor, "make_units"),
      patch.object(processor, "make_period"),
      patch.object(processor, "make_entity_from_context"),
      patch.object(processor, "make_concept"),
    ):
      xfact = MagicMock()
      xfact.md5sum.value = "abc123"
      xfact.value = "100"
      xfact.unit = None
      xfact.context = MagicMock()

      processor.make_fact(xfact)

      assert len(processor.report_facts_df) == 1
      assert processor.report_facts_df.iloc[0]["from"] == "report123"


class TestMakeUnits:
  """Test unit processing logic."""

  def test_make_units_single_measure_usd(self, processor):
    """Test processing single measure unit (USD)."""
    xfact = MagicMock()
    xfact.md5sum.value = "fact123"
    xfact.unit.isSingleMeasure = True

    qname_mock = MagicMock()
    qname_mock.__str__ = lambda self: "iso4217:USD"
    qname_mock.__repr__ = lambda self: "iso4217:USD"

    xfact.unit.measures = [[qname_mock]]
    xfact.unit.nsmap = {"iso4217": "http://www.xbrl.org/2003/iso4217"}
    xfact.unit.elementNamespaceURI = "http://www.xbrl.org/2003/iso4217"

    fact_data = {"identifier": "fact123"}

    processor.make_units(fact_data, xfact)

    assert len(processor.units_df) == 1
    unit = processor.units_df.iloc[0]

    assert unit["measure"] == "iso4217:USD"
    assert unit["value"] == "USD"
    assert "iso4217" in unit["uri"]
    assert unit["numerator_uri"] is None
    assert unit["denominator_uri"] is None

  def test_make_units_duplicate_prevention(self, processor):
    """Test that duplicate units are not created."""
    xfact = MagicMock()
    xfact.md5sum.value = "fact123"
    xfact.unit.isSingleMeasure = True
    xfact.unit.measures = [[("iso4217", "USD")]]
    xfact.unit.nsmap = {"iso4217": "http://www.xbrl.org/2003/iso4217"}
    xfact.unit.elementNamespaceURI = "http://www.xbrl.org/2003/iso4217"

    fact_data1 = {"identifier": "fact123"}
    fact_data2 = {"identifier": "fact456"}

    processor.make_units(fact_data1, xfact)
    assert len(processor.units_df) == 1

    processor.make_units(fact_data2, xfact)
    assert len(processor.units_df) == 1

  def test_make_units_divide_measures(self, processor):
    """Test processing divide unit (e.g., USD per share)."""
    xfact = MagicMock()
    xfact.md5sum.value = "fact123"
    xfact.unit.isSingleMeasure = False
    xfact.unit.isDivide = True

    qname_numerator = MagicMock()
    qname_numerator.__str__ = lambda self: "iso4217:USD"
    qname_denominator = MagicMock()
    qname_denominator.__str__ = lambda self: "xbrli:shares"

    xfact.unit.measures = [[qname_numerator], [qname_denominator]]
    xfact.unit.nsmap = {
      "iso4217": "http://www.xbrl.org/2003/iso4217",
      "xbrli": "http://www.xbrl.org/2003/instance",
    }

    fact_data = {"identifier": "fact123"}

    processor.make_units(fact_data, xfact)

    assert len(processor.units_df) == 1
    unit = processor.units_df.iloc[0]

    assert "USD" in unit["numerator_uri"]
    assert "shares" in unit["denominator_uri"]
    assert unit["measure"] == "iso4217:USD/xbrli:shares"
    assert unit["value"] == "USD/shares"

  def test_make_units_measure_without_prefix(self, processor):
    """Test processing measure without namespace prefix."""
    xfact = MagicMock()
    xfact.md5sum.value = "fact123"
    xfact.unit.isSingleMeasure = True
    xfact.unit.isDivide = False

    qname_mock = MagicMock()
    qname_mock.__str__ = lambda self: "pure"

    xfact.unit.measures = [[qname_mock]]
    xfact.unit.elementNamespaceURI = "http://www.xbrl.org/2003/instance"

    fact_data = {"identifier": "fact123"}

    processor.make_units(fact_data, xfact)

    assert len(processor.units_df) == 1
    unit = processor.units_df.iloc[0]

    assert unit["measure"] == "pure"
    assert unit["value"] == "pure"

  def test_make_units_creates_relationship(self, processor):
    """Test that unit processing creates fact-unit relationship."""
    xfact = MagicMock()
    xfact.md5sum.value = "fact123"
    xfact.unit.isSingleMeasure = True
    xfact.unit.isDivide = False

    qname_mock = MagicMock()
    qname_mock.__str__ = lambda self: "iso4217:USD"

    xfact.unit.measures = [[qname_mock]]
    xfact.unit.nsmap = {"iso4217": "http://www.xbrl.org/2003/iso4217"}
    xfact.unit.elementNamespaceURI = "http://www.xbrl.org/2003/iso4217"

    fact_data = {"identifier": "fact123"}

    processor.make_units(fact_data, xfact)

    assert len(processor.fact_units_df) == 1
    assert processor.fact_units_df.iloc[0]["from"] == "fact123"


class TestMakePeriod:
  """Test period processing logic."""

  def test_make_period_instant(self, processor):
    """Test processing instant period."""
    xfact = MagicMock()
    instant_datetime = datetime(2023, 12, 31, 0, 0, 0)

    xfact.context.isInstantPeriod = True
    xfact.context.isStartEndPeriod = False
    xfact.context.instantDatetime = instant_datetime

    fact_data = {"identifier": "fact123"}

    processor.make_period(fact_data, xfact)

    assert len(processor.periods_df) == 1
    period = processor.periods_df.iloc[0]

    assert period["end_date"] == "2023-12-30"
    assert period["start_date"] is None
    assert period["period_type"] == "instant"
    assert period["fiscal_year"] == 2023
    assert period["fiscal_quarter"] == "Q4"
    assert period["is_annual"]
    assert period["is_quarterly"]
    assert period["days_in_period"] == 0

  def test_make_period_instant_q1(self, processor):
    """Test instant period in Q1."""
    xfact = MagicMock()
    instant_datetime = datetime(2023, 3, 31, 0, 0, 0)

    xfact.context.isInstantPeriod = True
    xfact.context.isStartEndPeriod = False
    xfact.context.instantDatetime = instant_datetime

    fact_data = {"identifier": "fact123"}

    processor.make_period(fact_data, xfact)

    assert len(processor.periods_df) == 1
    period = processor.periods_df.iloc[0]

    assert period["fiscal_quarter"] == "Q1"
    assert not period["is_annual"]
    assert period["is_quarterly"]

  def test_make_period_start_end_quarterly(self, processor):
    """Test processing quarterly duration period."""
    xfact = MagicMock()
    start_datetime = datetime(2023, 7, 1, 0, 0, 0)
    end_datetime = datetime(2023, 10, 1, 0, 0, 0)  # Will subtract 1 day

    xfact.context.isInstantPeriod = False
    xfact.context.isStartEndPeriod = True
    xfact.context.startDatetime = start_datetime
    xfact.context.endDatetime = end_datetime

    fact_data = {"identifier": "fact123"}

    processor.make_period(fact_data, xfact)

    assert len(processor.periods_df) == 1
    period = processor.periods_df.iloc[0]

    assert period["start_date"] == "2023-07-01"
    assert period["end_date"] == "2023-09-30"
    assert period["period_type"] == "quarterly"
    assert period["fiscal_quarter"] == "Q3"
    assert period["days_in_period"] == 92
    assert not period["is_ytd"]

  def test_make_period_start_end_annual(self, processor):
    """Test processing annual duration period."""
    xfact = MagicMock()
    start_datetime = datetime(2023, 1, 1, 0, 0, 0)
    end_datetime = datetime(2024, 1, 1, 0, 0, 0)

    xfact.context.isInstantPeriod = False
    xfact.context.isStartEndPeriod = True
    xfact.context.startDatetime = start_datetime
    xfact.context.endDatetime = end_datetime

    fact_data = {"identifier": "fact123"}

    processor.make_period(fact_data, xfact)

    assert len(processor.periods_df) == 1
    period = processor.periods_df.iloc[0]

    assert period["period_type"] == "annual"
    assert period["fiscal_year"] == 2023
    assert period["days_in_period"] == 365  # 2024-01-01 - 2023-01-01 = 365 days
    assert not period["is_ytd"]

  def test_make_period_start_end_nine_months(self, processor):
    """Test processing nine-month YTD period."""
    xfact = MagicMock()
    start_datetime = datetime(2023, 1, 1, 0, 0, 0)
    end_datetime = datetime(2023, 10, 1, 0, 0, 0)

    xfact.context.isInstantPeriod = False
    xfact.context.isStartEndPeriod = True
    xfact.context.startDatetime = start_datetime
    xfact.context.endDatetime = end_datetime

    fact_data = {"identifier": "fact123"}

    processor.make_period(fact_data, xfact)

    assert len(processor.periods_df) == 1
    period = processor.periods_df.iloc[0]

    assert period["period_type"] == "nine_months"
    assert period["is_ytd"]
    assert period["days_in_period"] == 273

  def test_make_period_duplicate_prevention(self, processor):
    """Test that duplicate periods are not created."""
    xfact = MagicMock()
    instant_datetime = datetime(2023, 12, 31, 0, 0, 0)

    xfact.context.isInstantPeriod = True
    xfact.context.isStartEndPeriod = False
    xfact.context.instantDatetime = instant_datetime

    fact_data1 = {"identifier": "fact123"}
    fact_data2 = {"identifier": "fact456"}

    processor.make_period(fact_data1, xfact)
    assert len(processor.periods_df) == 1

    processor.make_period(fact_data2, xfact)
    assert len(processor.periods_df) == 1

  def test_make_period_creates_relationship(self, processor):
    """Test that period processing creates fact-period relationship."""
    xfact = MagicMock()
    instant_datetime = datetime(2023, 12, 31, 0, 0, 0)

    xfact.context.isInstantPeriod = True
    xfact.context.isStartEndPeriod = False
    xfact.context.instantDatetime = instant_datetime

    fact_data = {"identifier": "fact123"}

    processor.make_period(fact_data, xfact)

    assert len(processor.fact_periods_df) == 1
    assert processor.fact_periods_df.iloc[0]["from"] == "fact123"


class TestMakeElement:
  """Test element processing logic."""

  def test_make_element_basic(self, processor):
    """Test basic element creation."""
    with (
      patch.object(processor, "make_element_labels"),
      patch.object(processor, "make_element_references"),
    ):
      xconcept = MagicMock()
      xconcept.document.targetNamespace = "http://example.com/2023"
      xconcept.name = "Revenue"
      xconcept.qname = "ex:Revenue"
      xconcept.periodType = "duration"
      xconcept.niceType = "monetaryItemType"
      xconcept.balance = "credit"
      xconcept.isAbstract = False
      xconcept.isDimensionItem = False
      xconcept.isDomainMember = False
      xconcept.isHypercubeItem = False
      xconcept.isInteger = False
      xconcept.isNumeric = True
      xconcept.isShares = False
      xconcept.isFraction = False
      xconcept.isTextBlock = False

      processor.make_element(xconcept)

      assert len(processor.elements_df) == 1
      element = processor.elements_df.iloc[0]

      assert element["name"] == "Revenue"
      assert element["qname"] == "ex:Revenue"
      assert "Revenue" in element["uri"]
      assert element["period_type"] == "duration"
      assert element["type"] == "monetaryItemType"
      assert element["balance"] == "credit"
      assert element["is_numeric"]
      assert not element["is_abstract"]

  def test_make_element_abstract(self, processor):
    """Test abstract element creation."""
    with (
      patch.object(processor, "make_element_labels"),
      patch.object(processor, "make_element_references"),
    ):
      xconcept = MagicMock()
      xconcept.document.targetNamespace = "http://example.com/2023"
      xconcept.name = "AssetsAbstract"
      xconcept.qname = "ex:AssetsAbstract"
      xconcept.periodType = "instant"
      xconcept.niceType = "stringItemType"
      xconcept.balance = None
      xconcept.isAbstract = True
      xconcept.isDimensionItem = False
      xconcept.isDomainMember = False
      xconcept.isHypercubeItem = False
      xconcept.isInteger = False
      xconcept.isNumeric = False
      xconcept.isShares = False
      xconcept.isFraction = False
      xconcept.isTextBlock = False

      processor.make_element(xconcept)

      element = processor.elements_df.iloc[0]
      assert element["is_abstract"]

  def test_make_element_duplicate_prevention(self, processor):
    """Test that duplicate elements are not created."""
    with (
      patch.object(processor, "make_element_labels"),
      patch.object(processor, "make_element_references"),
    ):
      xconcept = MagicMock()
      xconcept.document.targetNamespace = "http://example.com/2023"
      xconcept.name = "Revenue"
      xconcept.qname = "ex:Revenue"
      xconcept.periodType = "duration"
      xconcept.niceType = "monetaryItemType"
      xconcept.balance = "credit"
      xconcept.isAbstract = False
      xconcept.isDimensionItem = False
      xconcept.isDomainMember = False
      xconcept.isHypercubeItem = False
      xconcept.isInteger = False
      xconcept.isNumeric = True
      xconcept.isShares = False
      xconcept.isFraction = False
      xconcept.isTextBlock = False

      processor.make_element(xconcept)
      assert len(processor.elements_df) == 1
      assert len(processor.processed_elements) == 1

      result = processor.make_element(xconcept)
      assert len(processor.elements_df) == 1
      assert len(processor.processed_elements) == 1
      assert result["name"] == "Revenue"

  def test_make_element_textblock(self, processor):
    """Test textblock element creation."""
    with (
      patch.object(processor, "make_element_labels"),
      patch.object(processor, "make_element_references"),
    ):
      xconcept = MagicMock()
      xconcept.document.targetNamespace = "http://example.com/2023"
      xconcept.name = "SignificantAccountingPoliciesTextBlock"
      xconcept.qname = "us-gaap:SignificantAccountingPoliciesTextBlock"
      xconcept.periodType = "duration"
      xconcept.niceType = "textBlockItemType"
      xconcept.balance = None
      xconcept.isAbstract = False
      xconcept.isDimensionItem = False
      xconcept.isDomainMember = False
      xconcept.isHypercubeItem = False
      xconcept.isInteger = False
      xconcept.isNumeric = False
      xconcept.isShares = False
      xconcept.isFraction = False
      xconcept.isTextBlock = True

      processor.make_element(xconcept)

      element = processor.elements_df.iloc[0]
      assert element["is_textblock"]


class TestMakeElementClassification:
  """Test element classification logic."""

  def test_make_element_classification_dimension(self, processor):
    """Test classification of dimension element."""
    xconcept = MagicMock()

    subgrp_qname = MagicMock()
    subgrp_qname.__str__ = lambda self: "xbrldt:hypercubeItem"
    subgrp_qname.localName = "hypercubeItem"
    subgrp_qname.namespaceURI = "http://xbrl.org/2006/xbrldt"
    xconcept.substitutionGroupQname = subgrp_qname

    type_qname = MagicMock()
    type_qname.localName = "domainItemType"
    type_qname.namespaceURI = "http://www.xbrl.org/2003/instance"
    xconcept.typeQname = type_qname

    xconcept.periodType = "instant"
    xconcept.abstract = "true"

    element_data = {
      "uri": "http://example.com#test",
      "substitution_group": None,
      "item_type": None,
      "classification": None,
    }

    result = processor.make_element_classification(element_data, xconcept)

    assert result["classification"] == "dimensionElement"
    assert "xbrldt" in result["substitution_group"]

  def test_make_element_classification_domain(self, processor):
    """Test classification of domain element."""
    xconcept = MagicMock()

    subgrp_qname = MagicMock()
    subgrp_qname.__str__ = lambda self: "xbrli:item"
    subgrp_qname.localName = "item"
    subgrp_qname.namespaceURI = "http://www.xbrl.org/2003/instance"
    xconcept.substitutionGroupQname = subgrp_qname

    type_qname = MagicMock()
    type_qname.localName = "domainItemType"
    type_qname.namespaceURI = "http://www.xbrl.org/2003/instance"
    xconcept.typeQname = type_qname

    xconcept.periodType = "duration"
    xconcept.abstract = "true"
    xconcept.nillable = "true"
    xconcept.name = "TestDomain"

    element_data = {
      "uri": "http://example.com#test",
      "substitution_group": None,
      "item_type": None,
      "classification": None,
    }

    result = processor.make_element_classification(element_data, xconcept)

    assert result["classification"] == "domainElement"


class TestMakeElementLabels:
  """Test element label processing."""

  def test_make_element_labels_standard(self, processor):
    """Test processing standard label."""
    processor.arelle_cntlr = MagicMock()

    label_obj = MagicMock()
    label_obj.xmlLang = "en-US"
    label_obj.role = "http://www.xbrl.org/2003/role/label"
    label_obj.text = "Total Revenue"

    rel = MagicMock()
    rel.toModelObject = label_obj

    rel_set = MagicMock()
    rel_set.fromModelObject.return_value = [rel]
    processor.arelle_cntlr.relationshipSet.return_value = rel_set

    xconcept = MagicMock()
    element_data = {"identifier": "elem123", "uri": "http://example.com#Revenue"}

    processor.make_element_labels(element_data, xconcept)

    assert len(processor.labels_df) == 1
    label = processor.labels_df.iloc[0]

    assert label["value"] == "Total Revenue"
    assert label["language"] == "en-US"
    assert "label" in label["type"]

    assert len(processor.element_labels_df) == 1
    assert processor.element_labels_df.iloc[0]["from"] == "elem123"

  def test_make_element_labels_multiple(self, processor):
    """Test processing multiple labels for one element."""
    processor.arelle_cntlr = MagicMock()

    label_obj1 = MagicMock()
    label_obj1.xmlLang = "en-US"
    label_obj1.role = "http://www.xbrl.org/2003/role/label"
    label_obj1.text = "Revenue"

    label_obj2 = MagicMock()
    label_obj2.xmlLang = "en-US"
    label_obj2.role = "http://www.xbrl.org/2003/role/verboseLabel"
    label_obj2.text = "Total Revenue from Operations"

    rel1 = MagicMock()
    rel1.toModelObject = label_obj1
    rel2 = MagicMock()
    rel2.toModelObject = label_obj2

    rel_set = MagicMock()
    rel_set.fromModelObject.return_value = [rel1, rel2]
    processor.arelle_cntlr.relationshipSet.return_value = rel_set

    xconcept = MagicMock()
    element_data = {"identifier": "elem123", "uri": "http://example.com#Revenue"}

    processor.make_element_labels(element_data, xconcept)

    assert len(processor.labels_df) == 2
    assert len(processor.element_labels_df) == 2


class TestSafeConcatIntegration:
  """Test safe_concat method integration with actual processing."""

  def test_safe_concat_maintains_dtypes(self, processor):
    """Test that safe_concat maintains dtype consistency."""
    df1 = pd.DataFrame({"identifier": ["id1"], "value": [100]})
    df2 = pd.DataFrame({"identifier": ["id2"], "value": [200]})

    result = processor.safe_concat(df1, df2)

    assert len(result) == 2
    assert list(result["identifier"]) == ["id1", "id2"]
    assert result["value"].dtype == "int64"

  def test_safe_concat_handles_mixed_types(self, processor):
    """Test that safe_concat handles mixed column types."""
    df1 = pd.DataFrame({"identifier": ["id1"], "value": [100]})
    df2 = pd.DataFrame({"identifier": ["id2"], "value": ["text"]})

    result = processor.safe_concat(df1, df2)

    assert len(result) == 2
    assert result["value"].dtype == "object"
