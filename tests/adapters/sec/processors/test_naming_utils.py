import pandas as pd

from robosystems.adapters.sec.processors.ids import (
  camel_to_snake,
  convert_schema_name_to_filename,
  make_plural,
  safe_concat,
)


class TestCamelToSnake:
  def test_simple_pascal_case(self):
    assert camel_to_snake("EntityReport") == "entity_report"

  def test_single_word(self):
    assert camel_to_snake("Entity") == "entity"

  def test_multiple_words(self):
    assert camel_to_snake("FactDimension") == "fact_dimension"

  def test_abbreviation(self):
    assert camel_to_snake("HTTPSConnection") == "https_connection"

  def test_already_lowercase(self):
    assert camel_to_snake("entity") == "entity"

  def test_with_numbers(self):
    assert camel_to_snake("Element2023") == "element2023"

  def test_consecutive_capitals(self):
    assert camel_to_snake("XMLParser") == "xml_parser"

  def test_camelcase_with_lowercase_start(self):
    assert camel_to_snake("entityReport") == "entity_report"


class TestMakePlural:
  def test_regular_word(self):
    assert make_plural("fact") == "facts"

  def test_word_ending_in_y(self):
    assert make_plural("entity") == "entities"

  def test_word_ending_in_s(self):
    assert make_plural("class") == "classes"

  def test_word_ending_in_x(self):
    assert make_plural("box") == "boxes"

  def test_word_ending_in_z(self):
    assert make_plural("buzz") == "buzzes"

  def test_word_ending_in_ch(self):
    assert make_plural("branch") == "branches"

  def test_word_ending_in_sh(self):
    assert make_plural("brush") == "brushes"

  def test_taxonomy(self):
    assert make_plural("taxonomy") == "taxonomies"

  def test_element(self):
    assert make_plural("element") == "elements"

  def test_reference(self):
    assert make_plural("reference") == "references"


class TestConvertSchemaNameToFilename:
  def test_simple_name(self):
    assert convert_schema_name_to_filename("Entity") == "Entity.parquet"

  def test_pascal_case(self):
    assert convert_schema_name_to_filename("FactDimension") == "FactDimension.parquet"

  def test_uppercase_with_underscores(self):
    assert (
      convert_schema_name_to_filename("FACT_HAS_DIMENSION")
      == "FACT_HAS_DIMENSION.parquet"
    )

  def test_lowercase(self):
    assert convert_schema_name_to_filename("element") == "element.parquet"

  def test_with_numbers(self):
    assert convert_schema_name_to_filename("Report2023") == "Report2023.parquet"


class TestSafeConcat:
  def test_concat_two_non_empty_dataframes(self):
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame({"a": [5, 6], "b": [7, 8]})

    result = safe_concat(df1, df2)

    assert len(result) == 4
    assert list(result["a"]) == [1, 2, 5, 6]

  def test_concat_with_empty_new_df(self):
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame()

    result = safe_concat(df1, df2)

    pd.testing.assert_frame_equal(result, df1)

  def test_concat_with_empty_existing_df(self):
    df1 = pd.DataFrame()
    df2 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    result = safe_concat(df1, df2)

    pd.testing.assert_frame_equal(result, df2)

  def test_concat_both_empty(self):
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()

    result = safe_concat(df1, df2)

    assert result.empty

  def test_concat_with_dtype_mismatch_to_object(self):
    df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pd.DataFrame({"a": [3, 4], "b": ["z", "w"]})

    df2["a"] = df2["a"].astype(str)

    result = safe_concat(df1, df2)

    assert len(result) == 4
    assert result["a"].dtype == "object"

  def test_concat_with_numeric_dtypes(self):
    df1 = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
    df2 = pd.DataFrame({"a": [5, 6], "b": [7.5, 8.5]})

    result = safe_concat(df1, df2)

    assert len(result) == 4
    assert result["a"].dtype == "int64"
    assert result["b"].dtype == "float64"

  def test_concat_preserves_columns(self):
    df1 = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
    df2 = pd.DataFrame({"a": [4], "b": [5], "c": [6]})

    result = safe_concat(df1, df2)

    assert list(result.columns) == ["a", "b", "c"]

  def test_concat_resets_index(self):
    df1 = pd.DataFrame({"a": [1, 2]}, index=[10, 20])
    df2 = pd.DataFrame({"a": [3, 4]}, index=[30, 40])

    result = safe_concat(df1, df2)

    assert list(result.index) == [0, 1, 2, 3]

  def test_concat_with_object_and_int(self):
    df1 = pd.DataFrame({"a": ["x", "y"]})
    df2 = pd.DataFrame({"a": [1, 2]})

    result = safe_concat(df1, df2)

    assert result["a"].dtype == "object"
    assert len(result) == 4

  def test_concat_handles_missing_columns_gracefully(self):
    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [3, 4], "b": [5, 6]})

    result = safe_concat(df1, df2)

    assert len(result) == 4
    assert "a" in result.columns
    assert "b" in result.columns
