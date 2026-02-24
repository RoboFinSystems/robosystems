"""Graph API utils tests."""

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.utils import (
    validate_database_name,
    validate_query_parameters,
)


class TestValidateDatabaseName:
    """Tests for validate_database_name."""

    def test_valid_name(self):
        assert validate_database_name("kg123abc") == "kg123abc"

    def test_valid_name_with_underscore(self):
        assert validate_database_name("my_graph") == "my_graph"

    def test_valid_name_with_hyphen(self):
        assert validate_database_name("my-graph") == "my-graph"

    def test_empty_name_rejected(self):
        with pytest.raises(HTTPException) as exc:
            validate_database_name("")
        assert exc.value.status_code == 400

    def test_path_traversal_dot_dot(self):
        with pytest.raises(HTTPException) as exc:
            validate_database_name("../etc/passwd")
        assert exc.value.status_code == 400

    def test_path_traversal_slash(self):
        with pytest.raises(HTTPException) as exc:
            validate_database_name("graph/id")
        assert exc.value.status_code == 400

    def test_path_traversal_backslash(self):
        with pytest.raises(HTTPException) as exc:
            validate_database_name("graph\\id")
        assert exc.value.status_code == 400

    def test_null_byte(self):
        with pytest.raises(HTTPException) as exc:
            validate_database_name("graph\x00id")
        assert exc.value.status_code == 400

    def test_special_chars_rejected(self):
        with pytest.raises(HTTPException) as exc:
            validate_database_name("graph@id!")
        assert exc.value.status_code == 400

    def test_too_long_name(self):
        with pytest.raises(HTTPException) as exc:
            validate_database_name("a" * 65)
        assert exc.value.status_code == 400

    def test_max_length_name(self):
        assert validate_database_name("a" * 64) == "a" * 64


class TestValidateQueryParameters:
    """Tests for validate_query_parameters."""

    def test_none_params_ok(self):
        validate_query_parameters(None)

    def test_empty_dict_ok(self):
        validate_query_parameters({})

    def test_valid_string_param(self):
        validate_query_parameters({"name": "test"})

    def test_valid_int_param(self):
        validate_query_parameters({"count": 42})

    def test_valid_float_param(self):
        validate_query_parameters({"rate": 3.14})

    def test_valid_bool_param(self):
        validate_query_parameters({"active": True})

    def test_valid_none_param(self):
        validate_query_parameters({"optional": None})

    def test_valid_list_param(self):
        validate_query_parameters({"ids": ["a", "b", "c"]})

    def test_valid_dict_param(self):
        validate_query_parameters({"nested": {"key": "value"}})

    def test_too_many_params(self):
        params = {f"param_{i}": "val" for i in range(51)}
        with pytest.raises(HTTPException) as exc:
            validate_query_parameters(params)
        assert exc.value.status_code == 400

    def test_invalid_param_name(self):
        with pytest.raises(HTTPException) as exc:
            validate_query_parameters({"invalid name!": "val"})
        assert exc.value.status_code == 400

    def test_string_too_long(self):
        with pytest.raises(HTTPException) as exc:
            validate_query_parameters({"text": "x" * 10001})
        assert exc.value.status_code == 400

    def test_list_too_many_elements(self):
        with pytest.raises(HTTPException) as exc:
            validate_query_parameters({"ids": ["x"] * 1001})
        assert exc.value.status_code == 400

    def test_number_too_large(self):
        with pytest.raises(HTTPException) as exc:
            validate_query_parameters({"big": 1e16})
        assert exc.value.status_code == 400

    def test_nested_param_name_with_dots(self):
        validate_query_parameters({"entity.name": "test"})

    def test_dict_too_many_keys(self):
        big_dict = {f"k{i}": "v" for i in range(101)}
        with pytest.raises(HTTPException) as exc:
            validate_query_parameters({"nested": big_dict})
        assert exc.value.status_code == 400
