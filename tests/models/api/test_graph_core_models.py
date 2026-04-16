"""Unit tests for graph core API models (creation, metadata, tier, subgraphs)."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.core import (
  CreateGraphRequest,
  CreateGraphResponse,
  GraphMetadata,
  InitialEntityData,
)
from robosystems.models.api.graphs.subgraphs import (
  CreateSubgraphRequest,
  DeleteSubgraphRequest,
  ListSubgraphsResponse,
  SubgraphQuotaResponse,
  SubgraphType,
)
from robosystems.models.api.graphs.tier import (
  AvailableGraphTiersResponse,
  GraphCapacityResponse,
  GraphTierBackup,
  GraphTierCopyOperations,
  GraphTierInfo,
  GraphTierInstance,
  GraphTierLimits,
  TierCapacity,
)


@pytest.mark.unit
class TestGraphMetadata:
  def test_minimal_metadata(self):
    model = GraphMetadata(graph_name="My Graph")
    assert model.graph_name == "My Graph"
    assert model.description is None
    assert model.schema_extensions == []
    assert model.tags == []

  def test_full_metadata(self):
    model = GraphMetadata(
      graph_name="Acme Corp",
      description="Main graph for Acme",
      schema_extensions=["roboledger", "roboinvestor"],
      tags=["production", "accounting"],
    )
    assert model.description == "Main graph for Acme"
    assert len(model.schema_extensions) == 2
    assert len(model.tags) == 2

  def test_graph_name_required(self):
    with pytest.raises(ValidationError):
      GraphMetadata()  # type: ignore[call-arg]


@pytest.mark.unit
class TestInitialEntityData:
  def test_minimal_entity(self):
    model = InitialEntityData(name="Test Co", uri="https://test.co")
    assert model.name == "Test Co"
    assert model.uri == "https://test.co"
    assert model.cik is None

  def test_full_entity_data(self):
    model = InitialEntityData(
      name="Apple Inc.",
      uri="https://apple.com",
      cik="0000320193",
      sic="3571",
      sic_description="Electronic Computers",
      category="Technology",
      state_of_incorporation="California",
      fiscal_year_end="0930",
      ein="94-2404110",
    )
    assert model.ein == "94-2404110"

  def test_name_required(self):
    with pytest.raises(ValidationError):
      InitialEntityData(uri="https://test.co")  # type: ignore[call-arg]

  def test_uri_required(self):
    with pytest.raises(ValidationError):
      InitialEntityData(name="Test")  # type: ignore[call-arg]

  def test_name_min_length(self):
    with pytest.raises(ValidationError):
      InitialEntityData(name="", uri="https://test.co")

  def test_name_max_length(self):
    with pytest.raises(ValidationError):
      InitialEntityData(name="x" * 256, uri="https://test.co")

  def test_uri_min_length(self):
    with pytest.raises(ValidationError):
      InitialEntityData(name="Test", uri="")


@pytest.mark.unit
class TestCreateGraphRequest:
  def test_minimal_entity_graph(self):
    model = CreateGraphRequest(
      metadata=GraphMetadata(graph_name="Test Graph"),
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
    )
    assert model.instance_tier == "ladybug-standard"
    assert model.custom_schema is None
    assert model.initial_entity is not None
    assert model.create_entity is True
    assert model.tags == []

  def test_entity_graph_requires_initial_entity(self):
    with pytest.raises(ValidationError) as exc_info:
      CreateGraphRequest(metadata=GraphMetadata(graph_name="Test Graph"))
    errors = exc_info.value.errors()
    assert any("initial_entity" in str(e["msg"]) for e in errors)

  def test_generic_graph_allows_no_initial_entity(self):
    from robosystems.models.api.graphs.schema import CustomSchemaDefinition

    model = CreateGraphRequest(
      metadata=GraphMetadata(graph_name="Custom Graph"),
      custom_schema=CustomSchemaDefinition(
        name="test",
        version="1.0.0",
        description=None,
        extends=None,
        nodes=[],
        relationships=[],
      ),
    )
    assert model.initial_entity is None
    assert model.custom_schema is not None

  def test_all_valid_tiers(self):
    for tier in ["ladybug-standard", "ladybug-large", "ladybug-xlarge"]:
      model = CreateGraphRequest(
        metadata=GraphMetadata(graph_name="Test"),
        instance_tier=tier,
        initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      )
      assert model.instance_tier == tier

  def test_invalid_tier_rejected(self):
    with pytest.raises(ValidationError) as exc_info:
      CreateGraphRequest(
        metadata=GraphMetadata(graph_name="Test"),
        instance_tier="invalid-tier",
        initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("instance_tier",) for e in errors)

  def test_with_initial_entity(self):
    model = CreateGraphRequest(
      metadata=GraphMetadata(graph_name="Acme Corp"),
      initial_entity=InitialEntityData(name="Acme Corp", uri="https://acme.com"),
    )
    assert model.initial_entity is not None
    assert model.initial_entity.name == "Acme Corp"

  def test_create_entity_default_true(self):
    model = CreateGraphRequest(
      metadata=GraphMetadata(graph_name="Test"),
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
    )
    assert model.create_entity is True

  def test_create_entity_false(self):
    model = CreateGraphRequest(
      metadata=GraphMetadata(graph_name="Test"),
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      create_entity=False,
    )
    assert model.create_entity is False

  def test_tags_max_length(self):
    model = CreateGraphRequest(
      metadata=GraphMetadata(graph_name="Test"),
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      tags=["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"],
    )
    assert len(model.tags) == 10

  def test_json_round_trip(self):
    model = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Test Graph",
        schema_extensions=["roboledger"],
      ),
      instance_tier="ladybug-large",
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      tags=["test"],
    )
    json_str = model.model_dump_json()
    restored = CreateGraphRequest.model_validate_json(json_str)
    assert restored.metadata.graph_name == "Test Graph"
    assert restored.instance_tier == "ladybug-large"


@pytest.mark.unit
class TestCreateGraphResponse:
  def test_valid_response(self):
    model = CreateGraphResponse(
      graph_id="kg1234567890abcdef",
      status="created",
      message="Graph created successfully",
    )
    assert model.graph_id == "kg1234567890abcdef"
    assert model.status == "created"


@pytest.mark.unit
class TestCreateSubgraphRequest:
  def test_valid_request(self):
    model = CreateSubgraphRequest(
      name="dev",
      display_name="Development Environment",
    )
    assert model.name == "dev"
    assert model.display_name == "Development Environment"
    assert model.subgraph_type == SubgraphType.STATIC

  def test_name_normalized_to_lowercase(self):
    model = CreateSubgraphRequest(
      name="DevTest",
      display_name="Dev Test",
    )
    assert model.name == "devtest"

  def test_alphanumeric_name_only(self):
    with pytest.raises(ValidationError) as exc_info:
      CreateSubgraphRequest(
        name="dev-test",
        display_name="Dev Test",
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_name_with_underscores_rejected(self):
    with pytest.raises(ValidationError):
      CreateSubgraphRequest(
        name="dev_test",
        display_name="Dev Test",
      )

  def test_name_with_spaces_rejected(self):
    with pytest.raises(ValidationError):
      CreateSubgraphRequest(
        name="dev test",
        display_name="Dev Test",
      )

  def test_name_max_length(self):
    model = CreateSubgraphRequest(
      name="a" * 20,
      display_name="Test",
    )
    assert len(model.name) == 20

  def test_name_exceeds_max_length(self):
    with pytest.raises(ValidationError):
      CreateSubgraphRequest(
        name="a" * 21,
        display_name="Test",
      )

  def test_name_empty_rejected(self):
    with pytest.raises(ValidationError):
      CreateSubgraphRequest(
        name="",
        display_name="Test",
      )

  def test_display_name_required(self):
    with pytest.raises(ValidationError):
      CreateSubgraphRequest(name="dev")  # type: ignore[call-arg]

  def test_description_optional(self):
    model = CreateSubgraphRequest(
      name="dev",
      display_name="Dev",
      description="Development environment",
    )
    assert model.description == "Development environment"

  def test_description_max_length(self):
    with pytest.raises(ValidationError):
      CreateSubgraphRequest(
        name="dev",
        display_name="Dev",
        description="x" * 501,
      )

  def test_fork_parent_default_false(self):
    model = CreateSubgraphRequest(
      name="dev",
      display_name="Dev",
    )
    assert model.fork_parent is False

  def test_fork_parent_true(self):
    model = CreateSubgraphRequest(
      name="fork1",
      display_name="Fork",
      fork_parent=True,
    )
    assert model.fork_parent is True

  def test_numeric_name_allowed(self):
    model = CreateSubgraphRequest(
      name="123",
      display_name="Numeric",
    )
    assert model.name == "123"

  def test_mixed_alphanumeric_allowed(self):
    model = CreateSubgraphRequest(
      name="test1abc2",
      display_name="Mixed",
    )
    assert model.name == "test1abc2"


@pytest.mark.unit
class TestSubgraphType:
  def test_enum_values(self):
    assert SubgraphType.STATIC == "static"
    assert SubgraphType.TEMPORAL == "temporal"
    assert SubgraphType.VERSIONED == "versioned"
    assert SubgraphType.MEMORY == "memory"


@pytest.mark.unit
class TestDeleteSubgraphRequest:
  def test_defaults(self):
    model = DeleteSubgraphRequest()
    assert model.force is False
    assert model.backup_first is True
    assert model.backup_location is None

  def test_force_delete(self):
    model = DeleteSubgraphRequest(force=True, backup_first=False)
    assert model.force is True
    assert model.backup_first is False


@pytest.mark.unit
class TestSubgraphQuotaResponse:
  def test_valid_response(self):
    model = SubgraphQuotaResponse(
      parent_graph_id="kg1234567890abcdef",
      tier="ladybug-large",
      current_count=3,
      max_allowed=10,
      remaining=7,
    )
    assert model.remaining == 7

  def test_ge_zero_constraint(self):
    with pytest.raises(ValidationError):
      SubgraphQuotaResponse(
        parent_graph_id="kg123",
        tier="ladybug-large",
        current_count=-1,
      )


@pytest.mark.unit
class TestListSubgraphsResponse:
  def test_valid_response(self):
    model = ListSubgraphsResponse(
      parent_graph_id="kg1234567890abcdef",
      parent_graph_name="Test Graph",
      parent_graph_tier="ladybug-large",
      subgraphs_enabled=True,
      subgraph_count=0,
      subgraphs=[],
    )
    assert model.subgraph_count == 0
    assert model.subgraphs == []

  def test_subgraph_count_ge_zero(self):
    with pytest.raises(ValidationError):
      ListSubgraphsResponse(
        parent_graph_id="kg123",
        parent_graph_name="Test",
        parent_graph_tier="ladybug-large",
        subgraphs_enabled=True,
        subgraph_count=-1,
        subgraphs=[],
      )


@pytest.mark.unit
class TestGraphTierModels:
  def test_graph_tier_copy_operations(self):
    model = GraphTierCopyOperations(
      max_file_size_gb=1.0,
      timeout_seconds=900,
      concurrent_operations=1,
      max_files_per_operation=100,
      daily_copy_operations=25,
    )
    assert model.max_file_size_gb == 1.0

  def test_graph_tier_backup(self):
    model = GraphTierBackup(
      max_backup_size_gb=10,
      backup_retention_days=7,
      max_backups_per_day=2,
    )
    assert model.backup_retention_days == 7

  def test_graph_tier_instance(self):
    model = GraphTierInstance(
      type="r7g.large",
      memory_mb=16384,
      is_multitenant=True,
    )
    assert model.is_multitenant is True

  def test_graph_tier_limits(self):
    copy_ops = GraphTierCopyOperations(
      max_file_size_gb=1.0,
      timeout_seconds=900,
      concurrent_operations=1,
      max_files_per_operation=100,
      daily_copy_operations=25,
    )
    backup = GraphTierBackup(
      max_backup_size_gb=10,
      backup_retention_days=7,
      max_backups_per_day=2,
    )
    model = GraphTierLimits(
      monthly_credits=8000,
      max_subgraphs=None,
      copy_operations=copy_ops,
      backup=backup,
    )
    assert model.monthly_credits == 8000
    assert model.max_subgraphs is None

  def test_graph_tier_info(self):
    copy_ops = GraphTierCopyOperations(
      max_file_size_gb=1.0,
      timeout_seconds=900,
      concurrent_operations=1,
      max_files_per_operation=100,
      daily_copy_operations=25,
    )
    backup = GraphTierBackup(
      max_backup_size_gb=10,
      backup_retention_days=7,
      max_backups_per_day=2,
    )
    limits = GraphTierLimits(
      monthly_credits=8000,
      max_subgraphs=None,
      copy_operations=copy_ops,
      backup=backup,
    )
    instance = GraphTierInstance(
      type="r7g.large",
      memory_mb=16384,
      is_multitenant=True,
    )
    model = GraphTierInfo(
      tier="ladybug-standard",
      name="ladybug-standard",
      display_name="LadybugDB Standard",
      description="Multi-tenant entry tier",
      backend="ladybug",
      enabled=True,
      max_subgraphs=None,
      monthly_credits=8000,
      api_rate_multiplier=1.0,
      features=["Shared infrastructure", "Basic support"],
      instance=instance,
      limits=limits,
    )
    assert model.tier == "ladybug-standard"
    assert model.monthly_price is None

  def test_tier_capacity(self):
    model = TierCapacity(
      tier="ladybug-standard",
      display_name="LadybugDB Standard",
      status="ready",
      message="Ready to provision",
    )
    assert model.status == "ready"

  def test_available_graph_tiers_response(self):
    model = AvailableGraphTiersResponse(tiers=[])
    assert model.tiers == []

  def test_graph_capacity_response(self):
    capacity = TierCapacity(
      tier="ladybug-standard",
      display_name="LadybugDB Standard",
      status="ready",
      message="Ready",
    )
    model = GraphCapacityResponse(tiers=[capacity])
    assert len(model.tiers) == 1
