import pytest
from unittest.mock import Mock
from sqlalchemy.orm import Session

from robosystems.operations.graph.table_service import TableService, infer_table_type
from robosystems.models.iam import GraphTable


class TestInferTableType:
    def test_relationship_table_screaming_snake_case(self):
        assert infer_table_type("PERSON_WORKS_FOR_COMPANY") == "relationship"
        assert infer_table_type("COMPANY_SPONSORS_PROJECT") == "relationship"
        assert infer_table_type("HAS_ORDER") == "relationship"
        assert infer_table_type("FILED_BY") == "relationship"

    def test_node_table_pascal_case(self):
        assert infer_table_type("Person") == "node"
        assert infer_table_type("Company") == "node"
        assert infer_table_type("Project") == "node"
        assert infer_table_type("Entity") == "node"

    def test_node_table_mixed_case(self):
        assert infer_table_type("GraphMetadata") == "node"
        assert infer_table_type("UserAccount") == "node"

    def test_node_table_lowercase(self):
        assert infer_table_type("person") == "node"
        assert infer_table_type("company") == "node"

    def test_single_word_uppercase_is_node(self):
        assert infer_table_type("ENTITY") == "node"
        assert infer_table_type("PERSON") == "node"

    def test_uppercase_with_underscore_is_relationship(self):
        assert infer_table_type("A_B") == "relationship"
        assert infer_table_type("HAS_PARENT") == "relationship"

    def test_edge_cases(self):
        assert infer_table_type("person_data") == "node"
        assert infer_table_type("PersonData") == "node"
        assert infer_table_type("PERSON_HAS_DATA") == "relationship"


class TestTableServiceCreateTablesFromSchema:
    @pytest.fixture
    def mock_session(self):
        session = Mock(spec=Session)
        session.commit = Mock()
        session.add = Mock()
        return session

    @pytest.fixture
    def table_service(self, mock_session):
        return TableService(mock_session)

    def test_create_node_tables_only(self, table_service, mock_session):
        schema_ddl = """
        CREATE NODE TABLE Company(
            identifier STRING,
            name STRING,
            industry STRING,
            PRIMARY KEY(identifier)
        );
        CREATE NODE TABLE Person(
            identifier STRING,
            name STRING,
            age INT64,
            PRIMARY KEY(identifier)
        );
        """

        GraphTable.get_by_name = Mock(return_value=None)
        GraphTable.create = Mock(side_effect=lambda **kwargs: Mock(**kwargs))

        graph_id = "kg123"
        user_id = "user123"

        result = table_service.create_tables_from_schema(graph_id, user_id, schema_ddl)

        assert len(result) == 2
        assert GraphTable.create.call_count == 2

        create_calls = GraphTable.create.call_args_list
        assert create_calls[0][1]["table_name"] == "Company"
        assert create_calls[0][1]["table_type"] == "node"
        assert create_calls[1][1]["table_name"] == "Person"
        assert create_calls[1][1]["table_type"] == "node"

    def test_create_relationship_tables(self, table_service, mock_session):
        schema_ddl = """
        CREATE NODE TABLE Company(identifier STRING, PRIMARY KEY(identifier));
        CREATE NODE TABLE Person(identifier STRING, PRIMARY KEY(identifier));
        CREATE REL TABLE PERSON_WORKS_FOR_COMPANY(FROM Person TO Company, role STRING);
        """

        GraphTable.get_by_name = Mock(return_value=None)
        GraphTable.create = Mock(side_effect=lambda **kwargs: Mock(**kwargs))

        graph_id = "kg123"
        user_id = "user123"

        result = table_service.create_tables_from_schema(graph_id, user_id, schema_ddl)

        assert len(result) == 3

        create_calls = GraphTable.create.call_args_list
        relationship_call = [
            call for call in create_calls if call[1]["table_type"] == "relationship"
        ][0]

        assert relationship_call[1]["table_name"] == "PERSON_WORKS_FOR_COMPANY"
        assert relationship_call[1]["table_type"] == "relationship"
        assert relationship_call[1]["target_node_type"] is None
        assert relationship_call[1]["schema_json"] == {
            "name": "PERSON_WORKS_FOR_COMPANY",
            "properties": [],
        }

    def test_skip_existing_tables(self, table_service, mock_session):
        schema_ddl = """
        CREATE NODE TABLE Company(identifier STRING, PRIMARY KEY(identifier));
        CREATE NODE TABLE Person(identifier STRING, PRIMARY KEY(identifier));
        """

        existing_company = Mock(id="table1", table_name="Company")
        GraphTable.get_by_name = Mock(
            side_effect=lambda gid, name, session: (
                existing_company if name == "Company" else None
            )
        )
        GraphTable.create = Mock(side_effect=lambda **kwargs: Mock(**kwargs))

        graph_id = "kg123"
        user_id = "user123"

        result = table_service.create_tables_from_schema(graph_id, user_id, schema_ddl)

        assert len(result) == 2
        assert GraphTable.create.call_count == 1
        assert result[0] == existing_company

    def test_empty_schema_returns_empty_list(self, table_service, mock_session):
        schema_ddl = ""

        with pytest.raises(ValueError, match="Schema DDL cannot be empty"):
            table_service.create_tables_from_schema("kg123", "user123", schema_ddl)

    def test_mixed_nodes_and_relationships(self, table_service, mock_session):
        schema_ddl = """
        CREATE NODE TABLE Company(identifier STRING, PRIMARY KEY(identifier));
        CREATE NODE TABLE Project(identifier STRING, PRIMARY KEY(identifier));
        CREATE NODE TABLE Person(identifier STRING, PRIMARY KEY(identifier));
        CREATE REL TABLE PERSON_WORKS_FOR_COMPANY(FROM Person TO Company);
        CREATE REL TABLE PERSON_WORKS_ON_PROJECT(FROM Person TO Project);
        CREATE REL TABLE COMPANY_SPONSORS_PROJECT(FROM Company TO Project);
        """

        GraphTable.get_by_name = Mock(return_value=None)
        GraphTable.create = Mock(side_effect=lambda **kwargs: Mock(**kwargs))

        graph_id = "kg123"
        user_id = "user123"

        result = table_service.create_tables_from_schema(graph_id, user_id, schema_ddl)

        assert len(result) == 6

        node_tables = [
            call
            for call in GraphTable.create.call_args_list
            if call[1]["table_type"] == "node"
        ]
        relationship_tables = [
            call
            for call in GraphTable.create.call_args_list
            if call[1]["table_type"] == "relationship"
        ]

        assert len(node_tables) == 3
        assert len(relationship_tables) == 3


class TestTableServiceS3Pattern:
    @pytest.fixture
    def mock_session(self):
        return Mock(spec=Session)

    @pytest.fixture
    def table_service(self, mock_session):
        return TableService(mock_session)

    def test_get_s3_pattern_for_table(self, table_service, monkeypatch):
        monkeypatch.setenv("AWS_S3_BUCKET", "test-bucket")
        from robosystems.config import env

        env.AWS_S3_BUCKET = "test-bucket"

        pattern = table_service.get_s3_pattern_for_table(
            graph_id="kg123", table_name="Company", user_id="user456"
        )

        expected = (
            "s3://test-bucket/user-staging/user456/kg123/Company/**/*.parquet"
        )
        assert pattern == expected
