import pytest

from robosystems.schemas.parser import (
  parse_cypher_schema,
  parse_relationship_types,
  NodeType,
)


class TestParseCypherSchema:
  def test_parse_single_node_type(self):
    ddl = "CREATE NODE TABLE Customer(name STRING, sector STRING, PRIMARY KEY(name));"
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 1
    assert node_types[0].name == "Customer"
    assert len(node_types[0].properties) == 2
    assert node_types[0].properties[0] == {"name": "name", "type": "STRING"}
    assert node_types[0].properties[1] == {"name": "sector", "type": "STRING"}

  def test_parse_multiple_node_types(self):
    ddl = """
        CREATE NODE TABLE Customer(name STRING, sector STRING, PRIMARY KEY(name));
        CREATE NODE TABLE Order(id INT64, amount DOUBLE, PRIMARY KEY(id));
        """
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 2
    assert node_types[0].name == "Customer"
    assert node_types[1].name == "Order"
    assert len(node_types[1].properties) == 2
    assert node_types[1].properties[0] == {"name": "id", "type": "INT64"}
    assert node_types[1].properties[1] == {"name": "amount", "type": "DOUBLE"}

  def test_parse_node_with_array_type(self):
    ddl = "CREATE NODE TABLE Product(id INT64, tags STRING[], PRIMARY KEY(id));"
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 1
    assert node_types[0].name == "Product"
    assert len(node_types[0].properties) == 2
    assert node_types[0].properties[1] == {"name": "tags", "type": "STRING[]"}

  def test_parse_node_with_multiple_constraints(self):
    ddl = """
        CREATE NODE TABLE Entity(
            id INT64,
            name STRING,
            PRIMARY KEY(id),
            UNIQUE(name)
        );
        """
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 1
    assert node_types[0].name == "Entity"
    assert len(node_types[0].properties) == 2
    assert node_types[0].properties[0] == {"name": "id", "type": "INT64"}
    assert node_types[0].properties[1] == {"name": "name", "type": "STRING"}

  def test_parse_node_with_no_properties(self):
    ddl = "CREATE NODE TABLE EmptyNode(PRIMARY KEY(id));"
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 0

  def test_parse_case_insensitive(self):
    ddl = "create node table Customer(name string, sector string, primary key(name));"
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 1
    assert node_types[0].name == "Customer"
    assert len(node_types[0].properties) == 2

  def test_parse_multiline_with_whitespace(self):
    ddl = """
        CREATE NODE TABLE Customer(
            name STRING,
            sector STRING,
            revenue DOUBLE,
            PRIMARY KEY(name)
        );
        """
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 1
    assert node_types[0].name == "Customer"
    assert len(node_types[0].properties) == 3
    assert node_types[0].properties[2] == {"name": "revenue", "type": "DOUBLE"}

  def test_parse_complex_types(self):
    ddl = """
        CREATE NODE TABLE DataNode(
            id INT64,
            name STRING,
            value DOUBLE,
            active BOOLEAN,
            created_at DATE,
            tags STRING[],
            PRIMARY KEY(id)
        );
        """
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 1
    assert len(node_types[0].properties) == 6
    assert node_types[0].properties[0] == {"name": "id", "type": "INT64"}
    assert node_types[0].properties[1] == {"name": "name", "type": "STRING"}
    assert node_types[0].properties[2] == {"name": "value", "type": "DOUBLE"}
    assert node_types[0].properties[3] == {"name": "active", "type": "BOOLEAN"}
    assert node_types[0].properties[4] == {"name": "created_at", "type": "DATE"}
    assert node_types[0].properties[5] == {"name": "tags", "type": "STRING[]"}

  def test_empty_ddl_raises_error(self):
    with pytest.raises(ValueError, match="Schema DDL cannot be empty"):
      parse_cypher_schema("")

  def test_whitespace_only_ddl_raises_error(self):
    with pytest.raises(ValueError, match="Schema DDL cannot be empty"):
      parse_cypher_schema("   \n\t  ")

  def test_unbalanced_parentheses_raises_error(self):
    ddl = "CREATE NODE TABLE Bad(name STRING, PRIMARY KEY(name);"
    with pytest.raises(ValueError, match="Unclosed parentheses"):
      parse_cypher_schema(ddl)

  def test_extra_closing_parenthesis_raises_error(self):
    ddl = "CREATE NODE TABLE Bad(name STRING));"
    with pytest.raises(ValueError, match="Unbalanced parentheses"):
      parse_cypher_schema(ddl)

  def test_malformed_property_definition(self):
    ddl = "CREATE NODE TABLE Bad(InvalidProperty, PRIMARY KEY(id));"
    node_types = parse_cypher_schema(ddl)
    assert len(node_types) == 0

  def test_node_type_to_dict(self):
    node_type = NodeType(
      name="Customer", properties=[{"name": "name", "type": "STRING"}]
    )
    result = node_type.to_dict()

    assert result == {
      "name": "Customer",
      "properties": [{"name": "name", "type": "STRING"}],
    }

  def test_parse_with_foreign_key_constraint(self):
    ddl = """
        CREATE NODE TABLE Order(
            id INT64,
            customer_id INT64,
            PRIMARY KEY(id),
            FOREIGN KEY(customer_id) REFERENCES Customer(id)
        );
        """
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 1
    assert len(node_types[0].properties) == 2
    assert node_types[0].properties[0] == {"name": "id", "type": "INT64"}
    assert node_types[0].properties[1] == {"name": "customer_id", "type": "INT64"}

  def test_parse_mixed_content(self):
    ddl = """
        CREATE NODE TABLE Customer(name STRING, PRIMARY KEY(name));
        -- This is a comment
        CREATE NODE TABLE Order(id INT64, PRIMARY KEY(id));
        """
    node_types = parse_cypher_schema(ddl)
    assert len(node_types) == 2

  def test_parse_real_world_schema(self):
    ddl = """
        CREATE NODE TABLE Entity(
            cik STRING,
            entity_name STRING,
            sic_code STRING,
            fiscal_year_end STRING,
            state_of_incorporation STRING,
            business_address STRING,
            mailing_address STRING,
            PRIMARY KEY(cik)
        );
        CREATE NODE TABLE Filing(
            accession_number STRING,
            filing_date DATE,
            form_type STRING,
            file_url STRING,
            PRIMARY KEY(accession_number)
        );
        CREATE NODE TABLE Fact(
            fact_id STRING,
            tag STRING,
            value DOUBLE,
            unit STRING,
            fiscal_period STRING,
            fiscal_year INT64,
            PRIMARY KEY(fact_id)
        );
        """
    node_types = parse_cypher_schema(ddl)

    assert len(node_types) == 3
    assert node_types[0].name == "Entity"
    assert len(node_types[0].properties) == 7
    assert node_types[1].name == "Filing"
    assert len(node_types[1].properties) == 4
    assert node_types[2].name == "Fact"
    assert len(node_types[2].properties) == 6


class TestParseRelationshipTypes:
  def test_parse_single_relationship(self):
    ddl = "CREATE REL TABLE HAS_ORDER(FROM Customer TO Order);"
    rel_types = parse_relationship_types(ddl)

    assert len(rel_types) == 1
    assert rel_types[0] == "HAS_ORDER"

  def test_parse_multiple_relationships(self):
    ddl = """
        CREATE REL TABLE HAS_ORDER(FROM Customer TO Order);
        CREATE REL TABLE CONTAINS(FROM Order TO Product);
        """
    rel_types = parse_relationship_types(ddl)

    assert len(rel_types) == 2
    assert rel_types[0] == "HAS_ORDER"
    assert rel_types[1] == "CONTAINS"

  def test_parse_relationship_with_properties(self):
    ddl = (
      "CREATE REL TABLE PURCHASED(FROM Customer TO Product, date DATE, amount DOUBLE);"
    )
    rel_types = parse_relationship_types(ddl)

    assert len(rel_types) == 1
    assert rel_types[0] == "PURCHASED"

  def test_parse_case_insensitive_relationship(self):
    ddl = "create rel table has_order(from Customer to Order);"
    rel_types = parse_relationship_types(ddl)

    assert len(rel_types) == 1
    assert rel_types[0] == "has_order"

  def test_parse_no_relationships(self):
    ddl = "CREATE NODE TABLE Customer(name STRING, PRIMARY KEY(name));"
    rel_types = parse_relationship_types(ddl)

    assert len(rel_types) == 0

  def test_parse_empty_ddl(self):
    rel_types = parse_relationship_types("")
    assert len(rel_types) == 0

  def test_parse_mixed_nodes_and_relationships(self):
    ddl = """
        CREATE NODE TABLE Customer(name STRING, PRIMARY KEY(name));
        CREATE REL TABLE HAS_ORDER(FROM Customer TO Order);
        CREATE NODE TABLE Order(id INT64, PRIMARY KEY(id));
        CREATE REL TABLE CONTAINS(FROM Order TO Product);
        """
    rel_types = parse_relationship_types(ddl)

    assert len(rel_types) == 2
    assert rel_types[0] == "HAS_ORDER"
    assert rel_types[1] == "CONTAINS"

  def test_parse_real_world_relationships(self):
    ddl = """
        CREATE REL TABLE FILED_BY(FROM Filing TO Entity);
        CREATE REL TABLE HAS_FACT(FROM Filing TO Fact);
        CREATE REL TABLE BELONGS_TO(FROM Fact TO Entity);
        """
    rel_types = parse_relationship_types(ddl)

    assert len(rel_types) == 3
    assert rel_types[0] == "FILED_BY"
    assert rel_types[1] == "HAS_FACT"
    assert rel_types[2] == "BELONGS_TO"
