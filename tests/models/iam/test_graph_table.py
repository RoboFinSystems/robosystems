# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportOperatorIssue=false, reportOptionalMemberAccess=false
import pytest

from robosystems.models.iam.graph_table import GraphTable
from robosystems.models.iam import Graph


@pytest.mark.unit
class TestGraphTableModel:
  def test_create_graph_table(self, db_session, sample_graph):
    schema_json = {
      "columns": [
        {"name": "id", "type": "int64"},
        {"name": "name", "type": "string"},
      ]
    }

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    assert table.id is not None
    assert table.graph_id == sample_graph.graph_id
    assert table.table_name == "customers"
    assert table.table_type == "staging"
    assert table.schema_json == schema_json
    assert table.row_count == 0
    assert table.file_count == 0
    assert table.total_size_bytes == 0
    assert table.created_at is not None
    assert table.updated_at is not None

  def test_create_graph_table_with_target_node_type(self, db_session, sample_graph):
    schema_json = {"columns": []}

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="orders",
      table_type="staging",
      schema_json=schema_json,
      target_node_type="Order",
      session=db_session,
    )

    assert table.target_node_type == "Order"

  def test_get_by_name(self, db_session, sample_graph):
    schema_json = {"columns": []}

    GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    found = GraphTable.get_by_name(sample_graph.graph_id, "customers", db_session)

    assert found is not None
    assert found.table_name == "customers"
    assert found.graph_id == sample_graph.graph_id

  def test_get_by_name_not_found(self, db_session, sample_graph):
    found = GraphTable.get_by_name(sample_graph.graph_id, "nonexistent", db_session)
    assert found is None

  def test_get_by_name_different_graph(self, db_session, sample_graph):
    schema_json = {"columns": []}

    GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    found = GraphTable.get_by_name("different_graph_id", "customers", db_session)
    assert found is None

  def test_get_all_for_graph(self, db_session, sample_graph):
    schema_json = {"columns": []}

    GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="orders",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    tables = GraphTable.get_all_for_graph(sample_graph.graph_id, db_session)

    assert len(tables) == 2
    assert {t.table_name for t in tables} == {"customers", "orders"}

  def test_get_all_for_graph_empty(self, db_session, sample_graph):
    tables = GraphTable.get_all_for_graph(sample_graph.graph_id, db_session)
    assert len(tables) == 0

  def test_get_by_id(self, db_session, sample_graph):
    schema_json = {"columns": []}

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    found = GraphTable.get_by_id(table.id, db_session)

    assert found is not None
    assert found.id == table.id
    assert found.table_name == "customers"

  def test_get_by_id_not_found(self, db_session):
    found = GraphTable.get_by_id("nonexistent_id", db_session)
    assert found is None

  def test_update_stats_all_fields(self, db_session, sample_graph):
    schema_json = {"columns": []}

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    original_updated_at = table.updated_at

    table.update_stats(
      session=db_session, row_count=100, file_count=5, total_size_bytes=1024000
    )

    assert table.row_count == 100
    assert table.file_count == 5
    assert table.total_size_bytes == 1024000
    assert table.updated_at > original_updated_at

  def test_update_stats_partial_fields(self, db_session, sample_graph):
    schema_json = {"columns": []}

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    table.update_stats(session=db_session, row_count=50)

    assert table.row_count == 50
    assert table.file_count == 0
    assert table.total_size_bytes == 0

  def test_update_stats_incremental(self, db_session, sample_graph):
    schema_json = {"columns": []}

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    table.update_stats(session=db_session, row_count=100, file_count=2)
    table.update_stats(session=db_session, row_count=150, file_count=3)

    assert table.row_count == 150
    assert table.file_count == 3

  def test_unique_constraint_graph_id_table_name(self, db_session, sample_graph):
    schema_json = {"columns": []}

    GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    with pytest.raises(Exception):
      GraphTable.create(
        graph_id=sample_graph.graph_id,
        table_name="customers",
        table_type="staging",
        schema_json=schema_json,
        session=db_session,
      )

  def test_repr_format(self, db_session, sample_graph):
    schema_json = {"columns": []}

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    repr_str = repr(table)
    assert "GraphTable" in repr_str
    assert table.id in repr_str
    assert sample_graph.graph_id in repr_str  # type: ignore[operator]
    assert "customers" in repr_str
    assert "staging" in repr_str

  def test_jsonb_schema_storage(self, db_session, sample_graph):
    complex_schema = {
      "columns": [
        {"name": "id", "type": "int64", "nullable": False, "primary_key": True},
        {
          "name": "metadata",
          "type": "json",
          "nullable": True,
          "default": {},
        },
        {"name": "tags", "type": "array", "element_type": "string"},
      ],
      "indexes": [{"columns": ["id"], "unique": True}],
    }

    table = GraphTable.create(
      graph_id=sample_graph.graph_id,
      table_name="complex_table",
      table_type="staging",
      schema_json=complex_schema,
      session=db_session,
    )

    found = GraphTable.get_by_id(table.id, db_session)
    assert found.schema_json == complex_schema
    assert isinstance(found.schema_json, dict)
    assert len(found.schema_json["columns"]) == 3

  def test_multiple_graphs_same_table_name(self, db_session, test_user, test_org):
    from robosystems.models.iam import GraphUser
    from robosystems.config.graph_tier import GraphTier

    graph1 = Graph.create(
      graph_id="graph1",
      graph_name="Graph 1",
      graph_type="entity",
      graph_tier=GraphTier.LADYBUG_STANDARD,
      org_id=test_org.id,
      session=db_session,
    )
    GraphUser.create(user_id=test_user.id, graph_id=graph1.graph_id, session=db_session)

    graph2 = Graph.create(
      graph_id="graph2",
      graph_name="Graph 2",
      graph_type="entity",
      graph_tier=GraphTier.LADYBUG_STANDARD,
      org_id=test_org.id,
      session=db_session,
    )
    GraphUser.create(user_id=test_user.id, graph_id=graph2.graph_id, session=db_session)

    schema_json = {"columns": []}

    table1 = GraphTable.create(
      graph_id=graph1.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    table2 = GraphTable.create(
      graph_id=graph2.graph_id,
      table_name="customers",
      table_type="staging",
      schema_json=schema_json,
      session=db_session,
    )

    assert table1.id != table2.id
    assert table1.graph_id != table2.graph_id
    assert table1.table_name == table2.table_name
