import pytest
import re

from robosystems.middleware.graph.types import (
  is_subgraph_id,
  parse_graph_id,
  construct_subgraph_id,
  SUBGRAPH_NAME_PATTERN,
  GraphTypeRegistry,
)


class TestIsSubgraphId:
  @pytest.mark.unit
  def test_valid_subgraph_ids(self):
    valid_subgraph_ids = [
      "kg1234567890abcdef_dev",
      "kg1234567890abcdef_staging",
      "kg1234567890abcdef_prod",
      "kg1234567890abcdef_test",
      "kg123_a",
      "kg1_dev",
      "kg12345678_production",
      "kg1234567890abcdef_A1B2C3",
      "kg1234567890abcdef_12345",
      "kgabc123_test",
    ]

    for graph_id in valid_subgraph_ids:
      assert is_subgraph_id(graph_id) is True, (
        f"Expected {graph_id} to be a subgraph ID"
      )

  @pytest.mark.unit
  def test_parent_graph_ids_not_subgraphs(self):
    parent_graph_ids = [
      "kg1234567890abcdef",
      "kg123456",
      "kgabc",
      "kg1",
      "kgabcdef12345678",
    ]

    for graph_id in parent_graph_ids:
      assert is_subgraph_id(graph_id) is False, (
        f"Expected {graph_id} to NOT be a subgraph ID"
      )

  @pytest.mark.unit
  def test_shared_repositories_not_subgraphs(self):
    for repo in GraphTypeRegistry.SHARED_REPOSITORIES:
      assert is_subgraph_id(repo) is False, (
        f"Shared repository {repo} should not be treated as subgraph"
      )

  @pytest.mark.unit
  def test_empty_and_special_cases(self):
    assert is_subgraph_id("") is False
    assert is_subgraph_id("_") is False
    assert is_subgraph_id("kg_") is True
    assert is_subgraph_id("_dev") is False

  @pytest.mark.unit
  def test_multiple_underscores(self):
    assert is_subgraph_id("kg123_dev_test") is True
    assert is_subgraph_id("kg123_a_b_c") is True
    assert is_subgraph_id("kg_a_b") is True

  @pytest.mark.unit
  def test_case_sensitivity(self):
    assert is_subgraph_id("kg123_DEV") is True
    assert is_subgraph_id("kg123_Dev") is True
    assert is_subgraph_id("kg123_dev") is True


class TestParseGraphId:
  @pytest.mark.unit
  def test_parse_valid_subgraph_ids(self):
    test_cases = [
      ("kg1234567890abcdef_dev", "kg1234567890abcdef", "dev"),
      ("kg1234567890abcdef_staging", "kg1234567890abcdef", "staging"),
      ("kg123_prod", "kg123", "prod"),
      ("kg1_test", "kg1", "test"),
      ("kg1234567890abcdef_A1B2C3", "kg1234567890abcdef", "A1B2C3"),
      ("kg1234567890abcdef_12345", "kg1234567890abcdef", "12345"),
    ]

    for graph_id, expected_parent, expected_subgraph in test_cases:
      parent_id, subgraph_name = parse_graph_id(graph_id)
      assert parent_id == expected_parent, f"Parent ID mismatch for {graph_id}"
      assert subgraph_name == expected_subgraph, (
        f"Subgraph name mismatch for {graph_id}"
      )

  @pytest.mark.unit
  def test_parse_parent_graph_ids(self):
    parent_ids = [
      "kg1234567890abcdef",
      "kg123456",
      "kgabc",
      "kg1",
    ]

    for graph_id in parent_ids:
      parent_id, subgraph_name = parse_graph_id(graph_id)
      assert parent_id == graph_id
      assert subgraph_name is None

  @pytest.mark.unit
  def test_parse_shared_repositories(self):
    for repo in GraphTypeRegistry.SHARED_REPOSITORIES:
      parent_id, subgraph_name = parse_graph_id(repo)
      assert parent_id == repo
      assert subgraph_name is None

  @pytest.mark.unit
  def test_parse_multiple_underscores(self):
    test_cases = [
      ("kg123_dev_test", "kg123", "dev_test"),
      ("kg123_a_b_c", "kg123", "a_b_c"),
      ("kg1234567890abcdef_my_long_name", "kg1234567890abcdef", "my_long_name"),
    ]

    for graph_id, expected_parent, expected_subgraph in test_cases:
      parent_id, subgraph_name = parse_graph_id(graph_id)
      assert parent_id == expected_parent
      assert subgraph_name == expected_subgraph

  @pytest.mark.unit
  def test_parse_edge_cases(self):
    assert parse_graph_id("") == ("", None)
    assert parse_graph_id("kg_") == ("kg", "")

  @pytest.mark.unit
  def test_parse_preserves_case(self):
    parent_id, subgraph_name = parse_graph_id("kg123_DEV")
    assert subgraph_name == "DEV"

    parent_id, subgraph_name = parse_graph_id("kg123_Dev")
    assert subgraph_name == "Dev"

  @pytest.mark.unit
  def test_parse_numeric_subgraph_names(self):
    parent_id, subgraph_name = parse_graph_id("kg123_12345")
    assert parent_id == "kg123"
    assert subgraph_name == "12345"

  @pytest.mark.unit
  def test_parse_mixed_alphanumeric(self):
    parent_id, subgraph_name = parse_graph_id("kg1234567890abcdef_A1B2C3")
    assert parent_id == "kg1234567890abcdef"
    assert subgraph_name == "A1B2C3"

  @pytest.mark.unit
  def test_parse_very_long_parent_id(self):
    long_parent = "kg" + ("a" * 50)
    graph_id = f"{long_parent}_dev"
    parent_id, subgraph_name = parse_graph_id(graph_id)
    assert parent_id == long_parent
    assert subgraph_name == "dev"


class TestConstructSubgraphId:
  @pytest.mark.unit
  def test_construct_valid_subgraph_ids(self):
    test_cases = [
      ("kg1234567890abcdef", "dev", "kg1234567890abcdef_dev"),
      ("kg1234567890abcdef", "staging", "kg1234567890abcdef_staging"),
      ("kg123", "prod", "kg123_prod"),
      ("kg1", "test", "kg1_test"),
      ("kg1234567890abcdef", "A1B2C3", "kg1234567890abcdef_A1B2C3"),
      ("kg1234567890abcdef", "12345", "kg1234567890abcdef_12345"),
    ]

    for parent_id, subgraph_name, expected in test_cases:
      result = construct_subgraph_id(parent_id, subgraph_name)
      assert result == expected

  @pytest.mark.unit
  def test_construct_empty_parent_id_error(self):
    with pytest.raises(ValueError, match="parent_graph_id cannot be empty"):
      construct_subgraph_id("", "dev")

  @pytest.mark.unit
  def test_construct_empty_subgraph_name_error(self):
    with pytest.raises(ValueError, match="subgraph_name cannot be empty"):
      construct_subgraph_id("kg123", "")

  @pytest.mark.unit
  def test_construct_parent_with_underscore_error(self):
    with pytest.raises(ValueError, match="parent_graph_id cannot contain underscore"):
      construct_subgraph_id("kg123_test", "dev")

    with pytest.raises(ValueError, match="parent_graph_id cannot contain underscore"):
      construct_subgraph_id("kg_123", "staging")

  @pytest.mark.unit
  def test_construct_invalid_subgraph_name_special_chars(self):
    invalid_names = [
      "dev-test",
      "dev.test",
      "dev test",
      "dev@prod",
      "dev#1",
      "dev!",
      "dev$test",
      "dev%",
      "dev&test",
      "dev*",
      "dev+test",
      "dev=test",
      "dev[test]",
      "dev{test}",
      "dev|test",
      "dev\\test",
      "dev/test",
      "dev:test",
      "dev;test",
      "dev'test",
      'dev"test',
      "dev<test>",
      "dev,test",
      "dev?",
    ]

    for invalid_name in invalid_names:
      with pytest.raises(ValueError, match="subgraph_name must be alphanumeric"):
        construct_subgraph_id("kg123", invalid_name)

  @pytest.mark.unit
  def test_construct_subgraph_name_too_long(self):
    too_long = "a" * 21
    with pytest.raises(ValueError, match="subgraph_name must be alphanumeric"):
      construct_subgraph_id("kg123", too_long)

  @pytest.mark.unit
  def test_construct_max_length_subgraph_name(self):
    max_length_name = "a" * 20
    result = construct_subgraph_id("kg123", max_length_name)
    assert result == f"kg123_{max_length_name}"

  @pytest.mark.unit
  def test_construct_min_length_subgraph_name(self):
    result = construct_subgraph_id("kg123", "a")
    assert result == "kg123_a"

  @pytest.mark.unit
  def test_construct_numeric_only_subgraph_name(self):
    result = construct_subgraph_id("kg123", "12345")
    assert result == "kg123_12345"

  @pytest.mark.unit
  def test_construct_mixed_case_subgraph_name(self):
    result = construct_subgraph_id("kg123", "DevTest")
    assert result == "kg123_DevTest"

  @pytest.mark.unit
  def test_construct_uppercase_subgraph_name(self):
    result = construct_subgraph_id("kg123", "PRODUCTION")
    assert result == "kg123_PRODUCTION"

  @pytest.mark.unit
  def test_construct_roundtrip_with_parse(self):
    parent = "kg1234567890abcdef"
    subgraph = "staging"

    constructed = construct_subgraph_id(parent, subgraph)
    parsed_parent, parsed_subgraph = parse_graph_id(constructed)

    assert parsed_parent == parent
    assert parsed_subgraph == subgraph

  @pytest.mark.unit
  def test_construct_very_long_parent_id(self):
    long_parent = "kg" + ("a" * 100)
    result = construct_subgraph_id(long_parent, "dev")
    assert result == f"{long_parent}_dev"


class TestSubgraphNamePattern:
  @pytest.mark.unit
  def test_pattern_matches_valid_names(self):
    valid_names = [
      "dev",
      "staging",
      "prod",
      "test",
      "a",
      "A",
      "1",
      "12345",
      "A1B2C3",
      "DevTest",
      "PRODUCTION",
      "test123",
      "a" * 20,
    ]

    pattern = re.compile(SUBGRAPH_NAME_PATTERN)
    for name in valid_names:
      assert pattern.match(name), f"Pattern should match valid name: {name}"

  @pytest.mark.unit
  def test_pattern_rejects_invalid_names(self):
    invalid_names = [
      "",
      "a" * 21,
      "dev-test",
      "dev.test",
      "dev test",
      "dev@prod",
      "dev_test",
    ]

    pattern = re.compile(SUBGRAPH_NAME_PATTERN)
    for name in invalid_names:
      assert not pattern.match(name), f"Pattern should reject invalid name: {name}"

  @pytest.mark.unit
  def test_pattern_length_boundaries(self):
    pattern = re.compile(SUBGRAPH_NAME_PATTERN)

    assert pattern.match("a")
    assert pattern.match("a" * 20)
    assert not pattern.match("")
    assert not pattern.match("a" * 21)


class TestHelperFunctionIntegration:
  @pytest.mark.unit
  def test_construct_then_parse_identity(self):
    test_cases = [
      ("kg1234567890abcdef", "dev"),
      ("kg123", "staging"),
      ("kg1", "prod"),
      ("kgabc", "test123"),
      ("kg1234567890abcdef", "A1B2C3"),
    ]

    for parent, subgraph in test_cases:
      constructed = construct_subgraph_id(parent, subgraph)
      assert is_subgraph_id(constructed)

      parsed_parent, parsed_subgraph = parse_graph_id(constructed)
      assert parsed_parent == parent
      assert parsed_subgraph == subgraph

  @pytest.mark.unit
  def test_parse_then_validate_subgraph_name(self):
    valid_subgraph_ids = [
      "kg123_dev",
      "kg1234567890abcdef_staging",
      "kg1_A1B2C3",
    ]

    pattern = re.compile(SUBGRAPH_NAME_PATTERN)
    for graph_id in valid_subgraph_ids:
      parent_id, subgraph_name = parse_graph_id(graph_id)
      assert subgraph_name is not None
      assert pattern.match(subgraph_name)

  @pytest.mark.unit
  def test_is_subgraph_implies_parse_success(self):
    test_ids = [
      "kg123_dev",
      "kg1234567890abcdef_staging",
      "kg1_prod",
      "kgabc_test123",
    ]

    for graph_id in test_ids:
      if is_subgraph_id(graph_id):
        parent_id, subgraph_name = parse_graph_id(graph_id)
        assert parent_id is not None
        assert subgraph_name is not None
        assert parent_id != graph_id

  @pytest.mark.unit
  def test_shared_repos_consistent_across_helpers(self):
    for repo in GraphTypeRegistry.SHARED_REPOSITORIES:
      assert not is_subgraph_id(repo)

      parent_id, subgraph_name = parse_graph_id(repo)
      assert parent_id == repo
      assert subgraph_name is None


class TestErrorMessages:
  @pytest.mark.unit
  def test_construct_error_message_empty_parent(self):
    try:
      construct_subgraph_id("", "dev")
      assert False, "Should have raised ValueError"
    except ValueError as e:
      assert "parent_graph_id cannot be empty" in str(e)

  @pytest.mark.unit
  def test_construct_error_message_empty_subgraph(self):
    try:
      construct_subgraph_id("kg123", "")
      assert False, "Should have raised ValueError"
    except ValueError as e:
      assert "subgraph_name cannot be empty" in str(e)

  @pytest.mark.unit
  def test_construct_error_message_underscore_in_parent(self):
    try:
      construct_subgraph_id("kg123_test", "dev")
      assert False, "Should have raised ValueError"
    except ValueError as e:
      assert "parent_graph_id cannot contain underscore" in str(e)
      assert "kg123_test" in str(e)

  @pytest.mark.unit
  def test_construct_error_message_invalid_subgraph_name(self):
    try:
      construct_subgraph_id("kg123", "dev-test")
      assert False, "Should have raised ValueError"
    except ValueError as e:
      assert "subgraph_name must be alphanumeric" in str(e)
      assert "dev-test" in str(e)
