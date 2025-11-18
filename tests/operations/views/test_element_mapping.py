import pandas as pd
import pytest
from robosystems.models.api.views import (
  AggregationMethod,
  ElementAssociation,
  MappingStructure,
)
from robosystems.operations.views.element_mapping import (
  apply_element_mapping,
  _aggregate_values,
)


class TestApplyElementMapping:
  @pytest.fixture
  def sample_coa_facts(self):
    return pd.DataFrame(
      {
        "element_id": [
          "qb:CheckingAccount1",
          "qb:CheckingAccount2",
          "qb:SavingsAccount1",
          "qb:CheckingAccount1",
          "qb:CheckingAccount2",
        ],
        "element_name": [
          "Checking 1",
          "Checking 2",
          "Savings 1",
          "Checking 1",
          "Checking 2",
        ],
        "numeric_value": [10000, 5000, 3000, 12000, 6000],
        "period_end": [
          "2024-12-31",
          "2024-12-31",
          "2024-12-31",
          "2023-12-31",
          "2023-12-31",
        ],
        "entity_id": ["ACME", "ACME", "ACME", "ACME", "ACME"],
      }
    )

  @pytest.fixture
  def cash_mapping_structure(self):
    return MappingStructure(
      identifier="mapping_001",
      name="CoA to US-GAAP Mapping",
      description="Map Chart of Accounts to US-GAAP taxonomy",
      taxonomy_uri="qb:taxonomy",
      target_taxonomy_uri="us-gaap:taxonomy",
      associations=[
        ElementAssociation(
          identifier="assoc_001",
          source_element="qb:CheckingAccount1",
          target_element="us-gaap:CashAndCashEquivalents",
          aggregation_method=AggregationMethod.SUM,
          weight=1.0,
          formula=None,
          order_value=1.0,
        ),
        ElementAssociation(
          identifier="assoc_002",
          source_element="qb:CheckingAccount2",
          target_element="us-gaap:CashAndCashEquivalents",
          aggregation_method=AggregationMethod.SUM,
          weight=1.0,
          formula=None,
          order_value=2.0,
        ),
        ElementAssociation(
          identifier="assoc_003",
          source_element="qb:SavingsAccount1",
          target_element="us-gaap:CashAndCashEquivalents",
          aggregation_method=AggregationMethod.SUM,
          weight=1.0,
          formula=None,
          order_value=3.0,
        ),
      ],
    )

  def test_apply_sum_aggregation(self, sample_coa_facts, cash_mapping_structure):
    result = apply_element_mapping(sample_coa_facts, cash_mapping_structure)

    assert not result.empty
    assert len(result) == 2
    assert all(result["element_id"] == "us-gaap:CashAndCashEquivalents")

    period_2024 = result[result["period_end"] == "2024-12-31"]
    assert len(period_2024) == 1
    assert period_2024.iloc[0]["numeric_value"] == 18000

    period_2023 = result[result["period_end"] == "2023-12-31"]
    assert len(period_2023) == 1
    assert period_2023.iloc[0]["numeric_value"] == 18000

  def test_apply_weighted_average_aggregation(self, sample_coa_facts):
    weighted_mapping = MappingStructure(
      identifier="mapping_002",
      name="Weighted Average Mapping",
      description=None,
      taxonomy_uri=None,
      target_taxonomy_uri=None,
      associations=[
        ElementAssociation(
          identifier="assoc_001",
          source_element="qb:CheckingAccount1",
          target_element="us-gaap:WeightedAverage",
          aggregation_method=AggregationMethod.WEIGHTED_AVERAGE,
          weight=2.0,
          formula=None,
          order_value=1.0,
        ),
        ElementAssociation(
          identifier="assoc_002",
          source_element="qb:CheckingAccount2",
          target_element="us-gaap:WeightedAverage",
          aggregation_method=AggregationMethod.WEIGHTED_AVERAGE,
          weight=1.0,
          formula=None,
          order_value=2.0,
        ),
      ],
    )

    result = apply_element_mapping(sample_coa_facts, weighted_mapping)

    assert not result.empty
    period_2024 = result[result["period_end"] == "2024-12-31"]
    assert len(period_2024) == 1
    expected_value = (10000 * 2.0 + 5000 * 1.0) / (2.0 + 1.0)
    assert period_2024.iloc[0]["numeric_value"] == pytest.approx(expected_value)

  def test_apply_average_aggregation(self, sample_coa_facts):
    avg_mapping = MappingStructure(
      identifier="mapping_003",
      name="Average Mapping",
      description=None,
      taxonomy_uri=None,
      target_taxonomy_uri=None,
      associations=[
        ElementAssociation(
          identifier="assoc_001",
          source_element="qb:CheckingAccount1",
          target_element="us-gaap:Average",
          aggregation_method=AggregationMethod.AVERAGE,
          weight=1.0,
          formula=None,
          order_value=1.0,
        ),
        ElementAssociation(
          identifier="assoc_002",
          source_element="qb:CheckingAccount2",
          target_element="us-gaap:Average",
          aggregation_method=AggregationMethod.AVERAGE,
          weight=1.0,
          formula=None,
          order_value=2.0,
        ),
      ],
    )

    result = apply_element_mapping(sample_coa_facts, avg_mapping)

    period_2024 = result[result["period_end"] == "2024-12-31"]
    assert period_2024.iloc[0]["numeric_value"] == 7500.0

  def test_apply_first_aggregation(self, sample_coa_facts):
    first_mapping = MappingStructure(
      identifier="mapping_004",
      name="First Mapping",
      description=None,
      taxonomy_uri=None,
      target_taxonomy_uri=None,
      associations=[
        ElementAssociation(
          identifier="assoc_001",
          source_element="qb:CheckingAccount1",
          target_element="us-gaap:First",
          aggregation_method=AggregationMethod.FIRST,
          weight=1.0,
          formula=None,
          order_value=1.0,
        ),
        ElementAssociation(
          identifier="assoc_002",
          source_element="qb:CheckingAccount2",
          target_element="us-gaap:First",
          aggregation_method=AggregationMethod.FIRST,
          weight=1.0,
          formula=None,
          order_value=2.0,
        ),
      ],
    )

    result = apply_element_mapping(sample_coa_facts, first_mapping)

    period_2024 = result[result["period_end"] == "2024-12-31"]
    assert period_2024.iloc[0]["numeric_value"] in [10000, 5000]

  def test_apply_last_aggregation(self, sample_coa_facts):
    last_mapping = MappingStructure(
      identifier="mapping_005",
      name="Last Mapping",
      description=None,
      taxonomy_uri=None,
      target_taxonomy_uri=None,
      associations=[
        ElementAssociation(
          identifier="assoc_001",
          source_element="qb:CheckingAccount1",
          target_element="us-gaap:Last",
          aggregation_method=AggregationMethod.LAST,
          weight=1.0,
          formula=None,
          order_value=1.0,
        ),
        ElementAssociation(
          identifier="assoc_002",
          source_element="qb:CheckingAccount2",
          target_element="us-gaap:Last",
          aggregation_method=AggregationMethod.LAST,
          weight=1.0,
          formula=None,
          order_value=2.0,
        ),
      ],
    )

    result = apply_element_mapping(sample_coa_facts, last_mapping)

    period_2024 = result[result["period_end"] == "2024-12-31"]
    assert period_2024.iloc[0]["numeric_value"] in [10000, 5000]

  def test_empty_fact_data(self, cash_mapping_structure):
    empty_df = pd.DataFrame()
    result = apply_element_mapping(empty_df, cash_mapping_structure)

    assert result.empty

  def test_empty_mapping_associations(self, sample_coa_facts):
    empty_mapping = MappingStructure(
      identifier="mapping_006",
      name="Empty Mapping",
      description=None,
      taxonomy_uri=None,
      target_taxonomy_uri=None,
      associations=[],
    )

    result = apply_element_mapping(sample_coa_facts, empty_mapping)

    assert len(result) == len(sample_coa_facts)
    assert result.equals(sample_coa_facts)

  def test_no_matching_source_elements(self, sample_coa_facts):
    non_matching_mapping = MappingStructure(
      identifier="mapping_007",
      name="Non-Matching Mapping",
      description=None,
      taxonomy_uri=None,
      target_taxonomy_uri=None,
      associations=[
        ElementAssociation(
          identifier="assoc_001",
          source_element="qb:NonExistent1",
          target_element="us-gaap:Something",
          aggregation_method=AggregationMethod.SUM,
          weight=1.0,
          formula=None,
          order_value=1.0,
        ),
      ],
    )

    result = apply_element_mapping(sample_coa_facts, non_matching_mapping)

    assert len(result) == len(sample_coa_facts)

  def test_multiple_target_elements(self, sample_coa_facts):
    multi_target_mapping = MappingStructure(
      identifier="mapping_008",
      name="Multi-Target Mapping",
      description=None,
      taxonomy_uri=None,
      target_taxonomy_uri=None,
      associations=[
        ElementAssociation(
          identifier="assoc_001",
          source_element="qb:CheckingAccount1",
          target_element="us-gaap:Cash",
          aggregation_method=AggregationMethod.SUM,
          weight=1.0,
          formula=None,
          order_value=1.0,
        ),
        ElementAssociation(
          identifier="assoc_002",
          source_element="qb:CheckingAccount2",
          target_element="us-gaap:Cash",
          aggregation_method=AggregationMethod.SUM,
          weight=1.0,
          formula=None,
          order_value=2.0,
        ),
        ElementAssociation(
          identifier="assoc_003",
          source_element="qb:SavingsAccount1",
          target_element="us-gaap:ShortTermInvestments",
          aggregation_method=AggregationMethod.SUM,
          weight=1.0,
          formula=None,
          order_value=3.0,
        ),
      ],
    )

    result = apply_element_mapping(sample_coa_facts, multi_target_mapping)

    assert not result.empty
    unique_elements = result["element_id"].unique()
    assert "us-gaap:Cash" in unique_elements
    assert "us-gaap:ShortTermInvestments" in unique_elements


class TestAggregateValues:
  @pytest.fixture
  def sample_facts_df(self):
    return pd.DataFrame(
      {
        "element_id": ["qb:Account1", "qb:Account2", "qb:Account3"],
        "numeric_value": [100, 200, 300],
      }
    )

  @pytest.fixture
  def sample_associations(self):
    return [
      ElementAssociation(
        identifier="assoc_001",
        source_element="qb:Account1",
        target_element="us-gaap:Total",
        aggregation_method=AggregationMethod.SUM,
        weight=1.0,
        formula=None,
        order_value=1.0,
      ),
      ElementAssociation(
        identifier="assoc_002",
        source_element="qb:Account2",
        target_element="us-gaap:Total",
        aggregation_method=AggregationMethod.SUM,
        weight=2.0,
        formula=None,
        order_value=2.0,
      ),
      ElementAssociation(
        identifier="assoc_003",
        source_element="qb:Account3",
        target_element="us-gaap:Total",
        aggregation_method=AggregationMethod.SUM,
        weight=3.0,
        formula=None,
        order_value=3.0,
      ),
    ]

  def test_aggregate_sum(self, sample_facts_df, sample_associations):
    result = _aggregate_values(
      sample_facts_df, sample_associations, AggregationMethod.SUM, "numeric_value"
    )
    assert result == 600

  def test_aggregate_average(self, sample_facts_df, sample_associations):
    result = _aggregate_values(
      sample_facts_df, sample_associations, AggregationMethod.AVERAGE, "numeric_value"
    )
    assert result == 200

  def test_aggregate_weighted_average(self, sample_facts_df, sample_associations):
    result = _aggregate_values(
      sample_facts_df,
      sample_associations,
      AggregationMethod.WEIGHTED_AVERAGE,
      "numeric_value",
    )
    expected = (100 * 1.0 + 200 * 2.0 + 300 * 3.0) / (1.0 + 2.0 + 3.0)
    assert result == pytest.approx(expected)

  def test_aggregate_first(self, sample_facts_df, sample_associations):
    result = _aggregate_values(
      sample_facts_df, sample_associations, AggregationMethod.FIRST, "numeric_value"
    )
    assert result == 100

  def test_aggregate_last(self, sample_facts_df, sample_associations):
    result = _aggregate_values(
      sample_facts_df, sample_associations, AggregationMethod.LAST, "numeric_value"
    )
    assert result == 300

  def test_aggregate_calculated(self, sample_facts_df, sample_associations):
    result = _aggregate_values(
      sample_facts_df,
      sample_associations,
      AggregationMethod.CALCULATED,
      "numeric_value",
    )
    assert result == 600

  def test_aggregate_weighted_average_zero_weight(self, sample_associations):
    zero_weight_df = pd.DataFrame(
      {
        "element_id": ["qb:Account1", "qb:Account2"],
        "numeric_value": [100, 200],
      }
    )

    zero_weight_associations = [
      ElementAssociation(
        identifier="assoc_001",
        source_element="qb:Account1",
        target_element="us-gaap:Total",
        aggregation_method=AggregationMethod.WEIGHTED_AVERAGE,
        weight=0.0,
        formula=None,
        order_value=1.0,
      ),
      ElementAssociation(
        identifier="assoc_002",
        source_element="qb:Account2",
        target_element="us-gaap:Total",
        aggregation_method=AggregationMethod.WEIGHTED_AVERAGE,
        weight=0.0,
        formula=None,
        order_value=2.0,
      ),
    ]

    result = _aggregate_values(
      zero_weight_df,
      zero_weight_associations,
      AggregationMethod.WEIGHTED_AVERAGE,
      "numeric_value",
    )
    assert result == 0.0
