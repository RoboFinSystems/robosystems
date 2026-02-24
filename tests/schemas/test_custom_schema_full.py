"""Full custom schema parser and manager tests."""

import json

import pytest

from robosystems.schemas.custom import (
    CustomSchemaDefinition,
    CustomSchemaManager,
    CustomSchemaParser,
    SchemaFormat,
)


@pytest.fixture
def parser():
    return CustomSchemaParser()


@pytest.fixture
def manager():
    return CustomSchemaManager()


def _minimal_schema():
    return {
        "name": "TestSchema",
        "nodes": [
            {
                "name": "Person",
                "properties": [
                    {"name": "id", "type": "STRING", "is_primary_key": True},
                    {"name": "name", "type": "STRING"},
                ],
            }
        ],
        "relationships": [],
    }


def _schema_with_relationship():
    return {
        "name": "TestSchema",
        "nodes": [
            {
                "name": "Person",
                "properties": [
                    {"name": "id", "type": "STRING", "is_primary_key": True},
                ],
            },
            {
                "name": "Company",
                "properties": [
                    {"name": "id", "type": "STRING", "is_primary_key": True},
                ],
            },
        ],
        "relationships": [
            {
                "name": "WORKS_AT",
                "from_node": "Person",
                "to_node": "Company",
                "properties": [
                    {"name": "since", "type": "DATE"},
                ],
            }
        ],
    }


class TestParseFormats:
    """Tests for parsing different input formats."""

    def test_parse_dict(self, parser):
        schema = parser.parse(_minimal_schema(), format=SchemaFormat.DICT)
        assert schema.name == "TestSchema"
        assert len(schema.nodes) == 1

    def test_parse_json_string(self, parser):
        json_str = json.dumps(_minimal_schema())
        schema = parser.parse(json_str, format=SchemaFormat.JSON)
        assert schema.name == "TestSchema"

    def test_parse_json_dict(self, parser):
        schema = parser.parse(_minimal_schema(), format=SchemaFormat.JSON)
        assert schema.name == "TestSchema"

    def test_parse_yaml_string(self, parser):
        yaml_str = """
name: TestSchema
nodes:
  - name: Person
    properties:
      - name: id
        type: STRING
        is_primary_key: true
relationships: []
"""
        schema = parser.parse(yaml_str, format=SchemaFormat.YAML)
        assert schema.name == "TestSchema"

    def test_parse_yaml_dict(self, parser):
        schema = parser.parse(_minimal_schema(), format=SchemaFormat.YAML)
        assert schema.name == "TestSchema"

    def test_dict_format_rejects_string(self, parser):
        with pytest.raises(ValueError, match="dictionary input"):
            parser.parse("not a dict", format=SchemaFormat.DICT)

    def test_json_format_rejects_invalid(self, parser):
        with pytest.raises(ValueError, match="string or dict"):
            parser.parse(42, format=SchemaFormat.JSON)

    def test_yaml_format_rejects_invalid(self, parser):
        with pytest.raises(ValueError, match="string or dict"):
            parser.parse(42, format=SchemaFormat.YAML)

    def test_yaml_non_dict_result(self, parser):
        with pytest.raises(ValueError, match="dictionary"):
            parser.parse("just a string", format=SchemaFormat.YAML)


class TestSchemaStructureValidation:
    """Tests for schema structure validation."""

    def test_missing_name(self, parser):
        with pytest.raises(ValueError, match="name"):
            parser.parse({"nodes": []}, format=SchemaFormat.DICT)

    def test_nodes_not_list(self, parser):
        with pytest.raises(ValueError, match="nodes"):
            parser.parse(
                {"name": "test", "nodes": "not a list"},
                format=SchemaFormat.DICT,
            )

    def test_relationships_not_list(self, parser):
        with pytest.raises(ValueError, match="relationships"):
            parser.parse(
                {"name": "test", "relationships": "not a list"},
                format=SchemaFormat.DICT,
            )


class TestNodeBuilding:
    """Tests for _build_node."""

    def test_node_missing_name(self, parser):
        with pytest.raises(ValueError, match="name"):
            parser._build_node({"properties": []})

    def test_node_missing_properties(self, parser):
        with pytest.raises(ValueError, match="properties"):
            parser._build_node({"name": "Test"})

    def test_node_no_primary_key(self, parser):
        with pytest.raises(ValueError, match="primary key"):
            parser._build_node(
                {
                    "name": "Test",
                    "properties": [{"name": "name", "type": "STRING"}],
                }
            )

    def test_reserved_node_name(self, parser):
        with pytest.raises(ValueError, match="reserved"):
            parser._build_node(
                {
                    "name": "SystemConfig",
                    "properties": [
                        {"name": "id", "type": "STRING", "is_primary_key": True}
                    ],
                }
            )

    def test_node_with_description(self, parser):
        node = parser._build_node(
            {
                "name": "Test",
                "description": "A test node",
                "properties": [
                    {"name": "id", "type": "STRING", "is_primary_key": True}
                ],
            }
        )
        assert node.description == "A test node"


class TestPropertyBuilding:
    """Tests for _build_property."""

    def test_missing_name(self, parser):
        with pytest.raises(ValueError, match="name"):
            parser._build_property({"type": "STRING"})

    def test_missing_type(self, parser):
        with pytest.raises(ValueError, match="type"):
            parser._build_property({"name": "test"})

    def test_invalid_type(self, parser):
        with pytest.raises(ValueError, match="Invalid type"):
            parser._build_property({"name": "test", "type": "BADTYPE"})

    def test_all_valid_base_types(self, parser):
        for valid_type in ["STRING", "INT64", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "UUID"]:
            prop = parser._build_property({"name": "test", "type": valid_type})
            assert prop.type == valid_type

    def test_nullable_default(self, parser):
        prop = parser._build_property({"name": "test", "type": "STRING"})
        assert prop.nullable is True

    def test_not_nullable(self, parser):
        prop = parser._build_property(
            {"name": "test", "type": "STRING", "nullable": False}
        )
        assert prop.nullable is False


class TestRelationshipBuilding:
    """Tests for _build_relationship."""

    def test_relationship_with_properties(self, parser):
        schema_dict = _schema_with_relationship()
        schema = parser.parse(schema_dict, format=SchemaFormat.DICT)
        rel = schema.relationships[0]
        assert rel.name == "WORKS_AT"
        assert rel.from_node == "Person"
        assert rel.to_node == "Company"
        assert len(rel.properties) == 1

    def test_relationship_missing_name(self, parser):
        from robosystems.schemas.models import Schema

        with pytest.raises(ValueError, match="name"):
            parser._build_relationship(
                {"from_node": "A", "to_node": "B"}, Schema(name="test")
            )

    def test_relationship_missing_from_node(self, parser):
        from robosystems.schemas.models import Schema

        with pytest.raises(ValueError, match="from_node"):
            parser._build_relationship(
                {"name": "REL", "to_node": "B"}, Schema(name="test")
            )

    def test_relationship_missing_to_node(self, parser):
        from robosystems.schemas.models import Schema

        with pytest.raises(ValueError, match="to_node"):
            parser._build_relationship(
                {"name": "REL", "from_node": "A"}, Schema(name="test")
            )

    def test_reserved_relationship_name(self, parser):
        from robosystems.schemas.models import Schema

        with pytest.raises(ValueError, match="reserved"):
            parser._build_relationship(
                {"name": "SYSTEM_OWNS", "from_node": "A", "to_node": "B"},
                Schema(name="test"),
            )


class TestCompleteSchemaValidation:
    """Tests for _validate_complete_schema."""

    def test_duplicate_node_names(self, parser):
        with pytest.raises(ValueError, match="Duplicate node"):
            parser.parse(
                {
                    "name": "test",
                    "nodes": [
                        {
                            "name": "Person",
                            "properties": [
                                {"name": "id", "type": "STRING", "is_primary_key": True}
                            ],
                        },
                        {
                            "name": "Person",
                            "properties": [
                                {"name": "id", "type": "STRING", "is_primary_key": True}
                            ],
                        },
                    ],
                },
                format=SchemaFormat.DICT,
            )

    def test_duplicate_relationship_names(self, parser):
        with pytest.raises(ValueError, match="Duplicate relationship"):
            parser.parse(
                {
                    "name": "test",
                    "nodes": [
                        {
                            "name": "A",
                            "properties": [
                                {"name": "id", "type": "STRING", "is_primary_key": True}
                            ],
                        },
                        {
                            "name": "B",
                            "properties": [
                                {"name": "id", "type": "STRING", "is_primary_key": True}
                            ],
                        },
                    ],
                    "relationships": [
                        {"name": "REL", "from_node": "A", "to_node": "B"},
                        {"name": "REL", "from_node": "B", "to_node": "A"},
                    ],
                },
                format=SchemaFormat.DICT,
            )

    def test_unknown_from_node(self, parser):
        with pytest.raises(ValueError, match="unknown from_node"):
            parser.parse(
                {
                    "name": "test",
                    "nodes": [
                        {
                            "name": "A",
                            "properties": [
                                {"name": "id", "type": "STRING", "is_primary_key": True}
                            ],
                        },
                    ],
                    "relationships": [
                        {"name": "REL", "from_node": "Missing", "to_node": "A"},
                    ],
                },
                format=SchemaFormat.DICT,
            )

    def test_unknown_to_node(self, parser):
        with pytest.raises(ValueError, match="unknown to_node"):
            parser.parse(
                {
                    "name": "test",
                    "nodes": [
                        {
                            "name": "A",
                            "properties": [
                                {"name": "id", "type": "STRING", "is_primary_key": True}
                            ],
                        },
                    ],
                    "relationships": [
                        {"name": "REL", "from_node": "A", "to_node": "Missing"},
                    ],
                },
                format=SchemaFormat.DICT,
            )

    def test_wildcard_nodes_allowed(self, parser):
        schema = parser.parse(
            {
                "name": "test",
                "nodes": [
                    {
                        "name": "A",
                        "properties": [
                            {"name": "id", "type": "STRING", "is_primary_key": True}
                        ],
                    },
                ],
                "relationships": [
                    {"name": "REL", "from_node": "*", "to_node": "*"},
                ],
            },
            format=SchemaFormat.DICT,
        )
        assert schema.relationships[0].from_node == "*"


class TestCustomSchemaManager:
    """Tests for CustomSchemaManager."""

    def test_create_from_json(self, manager):
        json_str = json.dumps(_minimal_schema())
        schema = manager.create_from_json(json_str)
        assert schema.name == "TestSchema"

    def test_create_from_dict(self, manager):
        schema = manager.create_from_dict(_minimal_schema())
        assert schema.name == "TestSchema"

    def test_create_from_yaml(self, manager):
        yaml_str = """
name: TestSchema
nodes:
  - name: Person
    properties:
      - name: id
        type: STRING
        is_primary_key: true
relationships: []
"""
        schema = manager.create_from_yaml(yaml_str)
        assert schema.name == "TestSchema"

    def test_validate_json_schema_valid(self, manager):
        json_str = json.dumps(_minimal_schema())
        result = manager.validate_json_schema(json_str)
        assert result["valid"] is True
        assert result["stats"]["nodes"] == 1

    def test_validate_json_schema_invalid(self, manager):
        result = manager.validate_json_schema('{"invalid": true}')
        assert result["valid"] is False
        assert "error_type" in result

    def test_merge_with_base(self, manager):
        user_schema = manager.create_from_dict(_minimal_schema())
        merged = manager.merge_with_base(user_schema)
        assert "(Extended)" in merged.name
        # Should have base nodes + user nodes
        node_names = [n.name for n in merged.nodes]
        assert "Person" in node_names
