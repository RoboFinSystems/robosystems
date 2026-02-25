"""Unit tests for user management API models."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.user import (
  AccountInfo,
  APIKeyInfo,
  APIKeysResponse,
  CreateAPIKeyRequest,
  CreateAPIKeyResponse,
  GraphInfo,
  UpdateAPIKeyRequest,
  UpdatePasswordRequest,
  UpdateUserRequest,
  UserGraphsResponse,
  UserResponse,
)


@pytest.mark.unit
class TestUpdateUserRequest:
  def test_all_optional(self):
    model = UpdateUserRequest()
    assert model.name is None
    assert model.email is None

  def test_name_only(self):
    model = UpdateUserRequest(name="New Name")
    assert model.name == "New Name"

  def test_email_only(self):
    model = UpdateUserRequest(email="new@example.com")
    assert model.email == "new@example.com"

  def test_name_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      UpdateUserRequest(name="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_name_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      UpdateUserRequest(name="x" * 101)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_invalid_email(self):
    with pytest.raises(ValidationError) as exc_info:
      UpdateUserRequest(email="not-an-email")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("email",) for e in errors)


@pytest.mark.unit
class TestUpdatePasswordRequest:
  def test_valid_request(self):
    model = UpdatePasswordRequest(
      current_password="OldP@ssw0rd!",
      new_password="N3wP@ssw0rd!",
      confirm_password="N3wP@ssw0rd!",
    )
    assert model.current_password == "OldP@ssw0rd!"
    assert model.new_password == "N3wP@ssw0rd!"

  def test_password_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      UpdatePasswordRequest(
        current_password="short",
        new_password="short",
        confirm_password="short",
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("current_password",) for e in errors)

  def test_all_fields_required(self):
    with pytest.raises(ValidationError):
      UpdatePasswordRequest(
        current_password="OldP@ssw0rd!",
        new_password="N3wP@ssw0rd!",
      )  # type: ignore[call-arg]


@pytest.mark.unit
class TestCreateAPIKeyRequest:
  def test_valid_request(self):
    model = CreateAPIKeyRequest(name="My API Key")
    assert model.name == "My API Key"
    assert model.description is None
    assert model.expires_at is None

  def test_with_all_fields(self):
    model = CreateAPIKeyRequest(
      name="Production Key",
      description="Used for CI/CD pipeline",
      expires_at="2025-12-31T23:59:59Z",
    )
    assert model.description == "Used for CI/CD pipeline"
    assert model.expires_at == "2025-12-31T23:59:59Z"

  def test_name_required(self):
    with pytest.raises(ValidationError):
      CreateAPIKeyRequest()  # type: ignore[call-arg]

  def test_name_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      CreateAPIKeyRequest(name="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_name_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      CreateAPIKeyRequest(name="x" * 101)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_description_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      CreateAPIKeyRequest(name="Key", description="x" * 501)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("description",) for e in errors)


@pytest.mark.unit
class TestUpdateAPIKeyRequest:
  def test_all_optional(self):
    model = UpdateAPIKeyRequest()
    assert model.name is None
    assert model.description is None

  def test_name_only(self):
    model = UpdateAPIKeyRequest(name="Updated Key")
    assert model.name == "Updated Key"

  def test_name_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      UpdateAPIKeyRequest(name="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_description_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      UpdateAPIKeyRequest(description="x" * 501)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("description",) for e in errors)


@pytest.mark.unit
class TestAccountInfo:
  def test_valid_account(self):
    model = AccountInfo(
      provider="github",
      provider_type="oauth",
      provider_account_id="12345",
    )
    assert model.provider == "github"
    assert model.provider_type == "oauth"


@pytest.mark.unit
class TestUserResponse:
  def test_minimal_response(self):
    model = UserResponse(id="user-123")
    assert model.id == "user-123"
    assert model.name is None
    assert model.email is None
    assert model.accounts == []

  def test_full_response(self):
    account = AccountInfo(
      provider="github",
      provider_type="oauth",
      provider_account_id="12345",
    )
    model = UserResponse(
      id="user-123",
      name="John Doe",
      email="john@example.com",
      accounts=[account],
    )
    assert len(model.accounts) == 1
    assert model.accounts[0].provider == "github"


@pytest.mark.unit
class TestGraphInfo:
  def test_minimal_graph(self):
    model = GraphInfo(
      graphId="kg1234567890abcdef",
      graphName="My Graph",
      role="admin",
      isSelected=True,
      createdAt="2024-01-15T10:00:00Z",
    )
    assert model.graphId == "kg1234567890abcdef"
    assert model.isRepository is False
    assert model.repositoryType is None
    assert model.schemaExtensions == []
    assert model.isSubgraph is False
    assert model.parentGraphId is None
    assert model.graphType == "entity"
    assert model.status == "active"

  def test_repository_graph(self):
    model = GraphInfo(
      graphId="sec",
      graphName="SEC EDGAR Filings",
      role="read",
      isSelected=False,
      createdAt="2024-01-01T00:00:00Z",
      isRepository=True,
      repositoryType="sec",
      graphType="repository",
    )
    assert model.isRepository is True
    assert model.repositoryType == "sec"

  def test_subgraph(self):
    model = GraphInfo(
      graphId="kg1234567890abcdef_dev",
      graphName="My Graph - Dev",
      role="member",
      isSelected=False,
      createdAt="2024-02-01T00:00:00Z",
      isSubgraph=True,
      parentGraphId="kg1234567890abcdef",
    )
    assert model.isSubgraph is True
    assert model.parentGraphId == "kg1234567890abcdef"

  def test_model_dump(self):
    model = GraphInfo(
      graphId="kg123",
      graphName="Test",
      role="admin",
      isSelected=True,
      createdAt="2024-01-01T00:00:00Z",
    )
    dumped = model.model_dump()
    assert "graphId" in dumped
    assert "graphName" in dumped
    assert dumped["status"] == "active"


@pytest.mark.unit
class TestUserGraphsResponse:
  def test_empty_graphs(self):
    model = UserGraphsResponse(graphs=[], selectedGraphId=None)
    assert model.graphs == []
    assert model.selectedGraphId is None

  def test_with_graphs(self):
    graph = GraphInfo(
      graphId="kg123",
      graphName="Test",
      role="admin",
      isSelected=True,
      createdAt="2024-01-01T00:00:00Z",
    )
    model = UserGraphsResponse(
      graphs=[graph],
      selectedGraphId="kg123",
    )
    assert len(model.graphs) == 1
    assert model.selectedGraphId == "kg123"


@pytest.mark.unit
class TestAPIKeyInfo:
  def test_valid_info(self):
    model = APIKeyInfo(
      id="key_123",
      name="My Key",
      prefix="rs_abc",
      is_active=True,
      created_at="2024-01-01T00:00:00Z",
    )
    assert model.is_active is True
    assert model.description is None
    assert model.last_used_at is None
    assert model.expires_at is None


@pytest.mark.unit
class TestCreateAPIKeyResponse:
  def test_valid_response(self):
    key_info = APIKeyInfo(
      id="key_123",
      name="My Key",
      prefix="rs_abc",
      is_active=True,
      created_at="2024-01-01T00:00:00Z",
    )
    model = CreateAPIKeyResponse(
      api_key=key_info,
      key="rs_abcdef1234567890",
    )
    assert model.key == "rs_abcdef1234567890"


@pytest.mark.unit
class TestAPIKeysResponse:
  def test_empty_list(self):
    model = APIKeysResponse(api_keys=[])
    assert model.api_keys == []

  def test_with_keys(self):
    key_info = APIKeyInfo(
      id="key_1",
      name="Key 1",
      prefix="rs_abc",
      is_active=True,
      created_at="2024-01-01T00:00:00Z",
    )
    model = APIKeysResponse(api_keys=[key_info])
    assert len(model.api_keys) == 1
